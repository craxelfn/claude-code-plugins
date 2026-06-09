"""Bundle orchestrator: DAG, state, run loop. Public surface = ``run()``.

This module owns:
  - ``run()`` — the public entry point.
  - ``resolve_plan()`` — classifies bundle names into (in-plan, extra-plan)
    and topo-sorts the in-plan DAG.
  - ``_execute_node()`` — the per-step dispatch with timing wrapping +
    try/except for module-side errors (returns ``RunStep.failed`` on
    any module exception).
  - ``_skip_dependents()`` / ``_abort_remaining()`` — two-phase cascade
    on failure (B Option B audit-completeness: every plan node gets
    exactly one state row per run, even if the run halted early).
  - ``_bootstrap_spark()`` — sentinel-typed Spark session bootstrapper.

Modules ``runtime`` / ``registry`` / ``state`` / ``errors`` are imports.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from graphlib import TopologicalSorter
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal

from oracle_ai_data_platform_fusion_bundle import extractors
from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog
from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle
from oracle_ai_data_platform_fusion_bundle.schema.plan_resolver import (
    resolve_dry_run_plan,
)
from oracle_ai_data_platform_fusion_bundle.schema.run_summary import (
    PlanNode,
    PrereqNode,
)

from . import registry, state
from .merge_sql import (
    build_explicit_when_matched_clause,
    build_explicit_when_not_matched_clause,
)
from .state import SchemaReconcileResult
from .errors import (
    IncrementalCursorMissingError,
    IncrementalTargetMissingError,
    MissingDependencyError,
    OrchestratorConfigError,
    SchemaEvolutionTypeConflictError,
    StateReadFailedError,
    UnsupportedModeError,
    WatermarkMonotonicityError,
)
from .registry import (
    BRONZE_EXTRACTS,
    GOLD_MARTS,
    KNOWN_DEFERRED_DIMS,
    KNOWN_DEFERRED_MARTS,
    SILVER_DIMS,
    BronzeExtractSpec,
    DeferredSpec,
    GoldMartSpec,
    SilverDimSpec,
    _VALID_LAYERS,
    _layer_for_spec,
    _resolve_bronze,
    _resolve_dim,
    _resolve_mart,
    _resolve_target_table,
    _resolve_watermark_source,
)
from .runtime import (
    ExternalDep,
    RunStep,
    RunSummary,
    WATERMARK_SAFETY_WINDOW,
    _new_run_id,
    _preflight_external_deps,
    _resolve_password,
    _resolve_safety_window,
    _safe_write_state_row,
    _utc_now,
    _VALID_MODES,
    BRONZE_AUDIT_COLUMNS,
    enrich_bronze_audit_cols,
    load_bundle,
)

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterable

    from pyspark.sql import DataFrame, SparkSession


# Re-export errors module-level so __init__ acts as the public face
from .errors import (  # noqa: E402  (re-export at module level)
    BronzeSchemaProbeError,
    BundleLoadError,
    BundleVersionMismatchError,
    CredentialResolutionError,
    MultipleNaturalKeyError,
    MultipleUpstreamWatermarkError,
    OrchestratorRuntimeError,
    PrerequisiteError,
    WatermarkMonotonicityError,
)


Spec = BronzeExtractSpec | SilverDimSpec | GoldMartSpec | DeferredSpec


# ---------------------------------------------------------------------------
# resolve_plan — classify bundle names + topo-sort
# ---------------------------------------------------------------------------


def resolve_plan(
    bundle: Bundle,
    datasets: list[str] | None,
    layers: list[str] | None,
    *,
    paths: TablePaths,
) -> tuple[list[Spec], tuple[ExternalDep, ...]]:
    """Engine-side wrapper around :func:`schema.plan_resolver.resolve_dry_run_plan`.

    P1.5ε-fix9 moved the classification + filter + topo-sort logic to the
    neutral ``schema.plan_resolver`` so the dispatch package can render
    dry-run plans without importing the orchestrator. The engine-side
    wrapper here reconstructs ``Spec`` instances + ``ExternalDep`` from
    the DTOs the schema resolver returns — the rest of the orchestrator
    (per-step dispatch, watermark capture, MERGE generation) needs spec
    objects with their ``builder`` callables, not neutral DTOs.

    The deferred-name branch is load-bearing: deferred names
    (``dim_org``, ``ar_aging``, ``hcm_worker_assignments``, …) are NOT
    in ``BRONZE_EXTRACTS`` / ``SILVER_DIMS`` / ``GOLD_MARTS``, so a
    naive dict lookup would ``KeyError`` on them.
    """
    plan_nodes, prereq_nodes = resolve_dry_run_plan(
        bundle, paths,
        datasets=datasets, layers=layers,
    )
    spec_plan: list[Spec] = []
    for node in plan_nodes:
        if node.status == "deferred":
            spec_plan.append(DeferredSpec(
                dataset_id=node.dataset_id,
                layer=node.layer,
                reason=node.reason or "",
            ))
        elif node.layer == "bronze":
            spec_plan.append(BRONZE_EXTRACTS[node.dataset_id])
        elif node.layer == "silver":
            spec_plan.append(SILVER_DIMS[node.dataset_id])
        elif node.layer == "gold":
            spec_plan.append(GOLD_MARTS[node.dataset_id])
        else:  # pragma: no cover — defensive (resolver validates layers)
            raise OrchestratorRuntimeError(
                f"PlanNode.layer={node.layer!r} not in {{bronze, silver, gold}}"
            )
    extra_deps = tuple(
        ExternalDep(
            dataset_id=pn.dataset_id, layer=pn.layer,
            consumer=pn.consumer, table_path=pn.table_path,
        ) for pn in prereq_nodes
    )
    return spec_plan, extra_deps


# ---------------------------------------------------------------------------
# P1.17 — bronze MERGE helpers
# ---------------------------------------------------------------------------


def _to_bicc_iso(wm: datetime) -> str:
    """Format a UTC ``datetime`` as the ISO-8601 string BICC's
    ``fusion.initial.extract-date`` option accepts (e.g.
    ``"2026-04-01T00:00:00Z"``). Pure function — trivially unit-testable.
    """
    return wm.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _natural_key_join_sql(
    natural_key: "str | tuple[str, ...]",
    *,
    target_alias: str = "target",
    src_alias: str = "src",
) -> str:
    """Build the MERGE ON predicate for a single- or multi-column natural key.

    Uses Spark's NULL-safe equality operator ``<=>`` instead of ``=`` so
    composite keys with NULL components (e.g. ``gl_period_balances`` on
    ``BalanceTranslatedFlag`` — see LIMITS.md P1.17-L8) still match
    NULL-vs-NULL rows. The operator is identical to ``=`` for non-NULL
    values; the NULL-safety is the only behavioral difference.

    Single-column key → ``target.k <=> src.k``.
    Composite tuple → ``target.k1 <=> src.k1 AND target.k2 <=> src.k2 AND ...``.
    Empty string / empty tuple raises — caller must validate the spec
    has a populated natural_key before invoking MERGE.
    """
    if isinstance(natural_key, str):
        if not natural_key:
            raise ValueError(
                "natural_key is empty — cannot construct MERGE ON predicate. "
                "Populate spec.natural_key / PvoEntry.natural_key before MERGE."
            )
        cols: tuple[str, ...] = (natural_key,)
    else:
        if len(natural_key) == 0:
            raise ValueError(
                "natural_key is empty tuple — cannot construct MERGE ON "
                "predicate. Populate spec.natural_key / PvoEntry.natural_key "
                "before MERGE."
            )
        cols = tuple(natural_key)
    return " AND ".join(
        f"{target_alias}.{c} <=> {src_alias}.{c}" for c in cols
    )


def _payload_diff_predicate_sql(
    data_columns: "Iterable[str]",
    *,
    target_alias: str = "target",
    src_alias: str = "src",
) -> str | None:
    """Build the P1.17e payload-diff predicate for a bronze MERGE's WHEN MATCHED clause.

    Bronze incremental MERGE under V1 used unconditional
    ``WHEN MATCHED THEN UPDATE SET *``, which rewrites every matched row's
    ``_extract_ts`` on every cycle. For PVOs flagged ``incremental_capable=False``
    (full re-extract every cycle — ``gl_period_balances``, ``gl_coa``,
    ``ap_aging_periods``), the rewritten ``_extract_ts`` propagates downstream:
    silver/gold's ``WHERE bronze_extract_ts > <prior_silver_watermark>`` source
    predicate matches every row, forcing silver/gold MERGE to run unconditionally
    even when nothing materially changed. See LIMITS.md §P1.17-L7.

    This helper builds the predicate that gates the UPDATE: an OR-joined
    ``IS DISTINCT FROM`` clause across every non-audit DATA column. When no
    payload column has changed for a matched row, the predicate evaluates
    ``false``, the UPDATE is suppressed, ``_extract_ts`` is NOT rewritten,
    and downstream silver/gold MERGE source filters match zero rows.

    Why ``IS DISTINCT FROM`` instead of ``<>``: Spark's ``<>`` is NULL-unsafe
    (``NULL <> NULL`` → NULL, treated as false in a WHEN clause). Bronze data
    often carries NULLs in optional columns (e.g., ``gl_period_balances``'s
    ``BalanceTranslatedFlag`` per LIMITS.md §P1.17-L8). ``IS DISTINCT FROM``
    is the NULL-safe inequality: ``NULL IS DISTINCT FROM NULL`` → false;
    ``NULL IS DISTINCT FROM 1`` → true. Mirrors the NULL-safe ``<=>`` used
    in :func:`_natural_key_join_sql` — the two helpers have a coherent
    NULL-handling story.

    Why audit columns are excluded: ``_extract_ts`` and ``_run_id`` carry
    this run's literal values, which always differ from the prior run's
    literals — including them in the diff would force every cycle's UPDATE,
    defeating the whole optimization. ``_source_pvo`` and ``_watermark_used``
    are similarly cycle-constant or cycle-distinct and contribute nothing
    useful to a diff. The four are excluded by symbolic reference to
    :data:`BRONZE_AUDIT_COLUMNS`.

    Natural-key columns are included in the predicate even though, on a
    matched row (where the ON predicate matched), the natural-key columns
    are by construction NULL-safe-equal between target and src. The
    ``target.k IS DISTINCT FROM src.k`` clause evaluates ``false`` for those
    columns; their inclusion is harmless and keeps this helper decoupled
    from :class:`BronzeExtractSpec` (it doesn't need to know the natural key).

    Args:
        data_columns: An iterable of bronze schema column names — typically
            ``df.schema.names`` of the source DataFrame after audit-column
            enrichment.
        target_alias: SQL alias of the MERGE target. Defaults to ``"target"``.
        src_alias: SQL alias of the MERGE source. Defaults to ``"src"``.

    Returns:
        The OR-joined ``IS DISTINCT FROM`` predicate, or ``None`` if no data
        column remains after excluding :data:`BRONZE_AUDIT_COLUMNS`. ``None``
        signals the caller to fall back to the V1 unconditional ``UPDATE SET *``
        shape — defensive against a malformed bronze schema that wouldn't
        reach this code in practice.

    Examples:
        >>> _payload_diff_predicate_sql(["SEGMENT1", "VENDORID", "_extract_ts"])
        'target.SEGMENT1 IS DISTINCT FROM src.SEGMENT1 OR target.VENDORID IS DISTINCT FROM src.VENDORID'
        >>> _payload_diff_predicate_sql(["_extract_ts", "_source_pvo"])
        # → None  (all columns are audit; caller falls back to V1 shape)
    """
    # Preserve source order from the input iterable; do NOT sort. Source order
    # is deterministic per (extractor, PVO) and makes golden-snapshot SQL tests
    # trivially stable. Sorting would risk nondeterminism if a future Spark
    # version changes column-iteration semantics.
    data_cols = [c for c in data_columns if c not in BRONZE_AUDIT_COLUMNS]
    if not data_cols:
        return None
    return " OR ".join(
        f"{target_alias}.{c} IS DISTINCT FROM {src_alias}.{c}" for c in data_cols
    )


# ---------------------------------------------------------------------------
# _execute_node — per-step dispatch with try/except + timing wrapping
# ---------------------------------------------------------------------------


def _execute_node(
    node: Spec,
    spark: "SparkSession",
    paths: TablePaths,
    bundle: Bundle,
    run_id: str,
    mode: str,
    *,
    effective_schemas: dict[str, str],
    plan_hash: str | None = None,
    plan_snapshot: str | None = None,
) -> RunStep:
    """Dispatch a single plan node and return a ``RunStep``.

    Branches:
      - ``BronzeExtractSpec`` → ``extract_pvo`` → enrich audit cols → write Delta.
      - ``SilverDimSpec`` / ``GoldMartSpec`` → ``node.builder(spark, paths=paths, run_id=run_id)``.
      - ``DeferredSpec`` → ``RunStep.deferred(...)`` (no-op).

    Module-dispatch exceptions (BICC down, Spark AnalysisException, vault
    permission denied, builder raised) are caught and surfaced as
    ``RunStep.failed``. Orchestrator-internal logic errors (unknown spec
    type) propagate uncaught as ``TypeError`` — those are real bugs.

    P1.5α-fix19: ``effective_schemas`` is the per-PVO resolved BICC
    offering schema (override / catalog / discovered). Threaded in from
    ``preflight_bronze_schemas`` so the real bronze dispatch uses the
    SAME schema preflight validated. Without this, overrides + auto-
    discovery would be cosmetic-only.

    P1.5β.1: resolves ``prior_watermark`` via
    :func:`_resolve_watermark_source` + :func:`state.read_last_watermark`
    for EVERY node regardless of ``mode`` — the resolver/read is not
    mode-gated, only the eventual ``extract_pvo(watermark=...)``
    threading is (and that stays unwired in this PR per the
    ``NotImplementedError`` gate). Bronze closures capture
    ``extract_started_at - WATERMARK_SAFETY_WINDOW`` into
    ``RunStep.last_watermark``; silver/gold leave it ``None`` (capture
    deferred to P1.17). The monotonicity check (bronze only) compares
    the captured cursor to ``prior_watermark`` and raises
    :class:`WatermarkMonotonicityError` on regression.
    """
    t0 = perf_counter()
    # P1.5β.1: resolve + read the prior watermark for this node BEFORE
    # the build dispatches. The resolver is layer-aware: bronze reads
    # its own state row; silver/gold read the upstream bronze's row;
    # parameter-driven specs (dim_calendar) return None. The read is
    # soft — a Spark/metastore failure logs a structured WARN with the
    # ``watermark_read_soft_failed`` marker and returns None. NAMED
    # ``prior_watermark`` (NOT ``resolved``) to avoid collision with
    # the ``resolved_password`` SecretStr in the bronze branch.
    _wm_source = _resolve_watermark_source(node)
    prior_watermark = (
        state.read_last_watermark(spark, paths, *_wm_source)
        if _wm_source is not None
        else None
    )
    try:
        if isinstance(node, BronzeExtractSpec):
            pvo = fusion_catalog.get(node.pvo_id)
            target = paths.bronze(pvo.bronze_table_name)
            # Credential resolution at dispatch (preflight already verified
            # resolvability; this call should always succeed). Local
            # named ``resolved_password`` (not ``resolved``) to keep it
            # distinct from any watermark-related ``resolved`` in scope —
            # the bronze closure threads both a SecretStr and a datetime,
            # so the rename is a defensive hygiene measure.
            resolved_password = _resolve_password(bundle.fusion.password)
            # P1.5α-fix19: preflight resolved schema via override → catalog →
            # auto-discovery. Use the SAME value here — without this, override
            # + auto-discovery would be cosmetic (preflight passes, real run
            # crashes with DATA_ACCESS_LAYER_0031 on the same PVO). KeyError
            # if dataset_id missing = orchestrator bug, fail loudly.
            effective_schema = effective_schemas[node.dataset_id]

            # P1.17 — resolve per-run safety window (bundle override or default).
            safety_window = _resolve_safety_window(bundle)

            def _do_bronze() -> tuple[int, datetime | None]:
                # P1.5β.1: capture orchestrator wall clock immediately
                # before BICC extract. ``extract_started_at`` is the
                # un-windowed audit instant (stamped as ``_extract_ts``
                # on every row); ``persisted_cursor`` subtracts the
                # safety window to absorb AIDP-vs-Fusion clock skew
                # and lands on ``RunStep.last_watermark``. Each retry
                # re-evaluates both — that's correct, a successful
                # retry's cursor reflects the moment IT extracted, not
                # the failed attempt's wall clock.
                extract_started_at = datetime.now(timezone.utc)
                persisted_cursor = extract_started_at - safety_window

                # P1.17 B5 + B6b — three-condition gate on threading
                # the prior watermark to BICC:
                #   1. prior_watermark must be non-None (fresh tenant
                #      → full extract; bronze degenerates cleanly).
                #   2. PVO must support `fusion.initial.extract-date`
                #      (`incremental_capable=True`). Three PVOs today
                #      carry False: gl_period_balances, gl_coa,
                #      ap_aging_periods — BICC's cursor filter is
                #      not respected for these. See LIMITS.md P1.17-L2.
                #   3. orchestrator mode must be "incremental".
                bicc_watermark = (
                    _to_bicc_iso(prior_watermark) if (
                        prior_watermark is not None
                        and pvo.incremental_capable
                        and mode == "incremental"
                    ) else None
                )

                df = extractors.bicc.extract_pvo(
                    spark,
                    pvo,
                    fusion_service_url=bundle.fusion.service_url,
                    username=bundle.fusion.username,
                    password=resolved_password.get_secret_value(),  # SOLE unwrap site
                    fusion_external_storage=bundle.fusion.external_storage,
                    schema=effective_schema,  # P1.5α-fix19 dispatch contract
                    watermark=bicc_watermark,  # P1.17 — None for seed/fresh/non-capable
                )
                df = enrich_bronze_audit_cols(
                    df,
                    source_pvo=pvo.datastore,
                    run_id=run_id,
                    # P1.17 — when BICC actually consumed a cursor (the three-
                    # condition gate above produced a non-None `bicc_watermark`),
                    # stamp every bronze row's `_watermark_used` with the same
                    # raw datetime that fed `_to_bicc_iso`. Without this, the
                    # bronze audit column says NULL while BICC's filter saw a
                    # real cursor — SOX traceability for "what window produced
                    # this row" is broken. β.1's "stays NULL" comment applied
                    # under the NotImplementedError gate (BICC never received a
                    # watermark in β.1); P1.17 removes the gate and wires the
                    # audit column per runtime.enrich_bronze_audit_cols' docstring.
                    watermark=prior_watermark if bicc_watermark is not None else None,
                    extract_ts=extract_started_at,  # audit literal == this run's instant
                )

                # P1.17 B6 — cache the source DataFrame: we count it
                # for the empty-delta gate AND read it again for the
                # MERGE source. Without caching, MERGE re-executes the
                # full BICC extract under the hood.
                df.cache()
                try:
                    source_delta_count = df.count()

                    if mode == "seed":
                        # Phase α path — full overwrite; target count == source count.
                        # overwriteSchema=true matches CLAUDE.md's "CREATE OR
                        # REPLACE for seed mode" invariant.
                        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
                        materialized_count = source_delta_count
                    else:
                        # P1.17 incremental — MERGE INTO. Ensures target
                        # exists BEFORE branching on source_delta_count
                        # so the empty-source short-circuit's
                        # `spark.table(target).count()` query never hits
                        # TABLE_OR_VIEW_NOT_FOUND on a fresh tenant
                        # whose first incremental run extracts zero
                        # rows (B6c).
                        state._ensure_target_table_exists(spark, target, df.schema)
                        # P1.17d — reconcile target schema with source BEFORE
                        # the MERGE. Auto-ALTERs in any source-wider columns;
                        # returns the column list to use for explicit-list
                        # MERGE when target-wider columns exist (preserves
                        # them by exclusion from UPDATE/INSERT). Raises
                        # SchemaEvolutionTypeConflictError on incompatible
                        # type drift. See LIMITS.md §P1.17-L6 (resolved).
                        reconcile = state._ensure_target_schema_for_merge(
                            spark, target, df.schema.names, df.schema,
                        )
                        if source_delta_count == 0:
                            # P1.17 short-circuit: empty source → no MERGE.
                            # Avoids a wasted MERGE plan AND prevents
                            # _extract_ts being touched on any existing rows.
                            materialized_count = spark.table(target).count()
                        else:
                            df.createOrReplaceTempView("_p117_bronze_src")
                            # B6 + B6d — payload-diff-gated UPDATE shipped per
                            # P1.17e. The WHEN MATCHED AND (...) predicate
                            # suppresses no-op UPDATEs when the bronze
                            # re-extract carries the same payload (the
                            # incremental_capable=False steady state on
                            # gl_period_balances / gl_coa / ap_aging_periods),
                            # which prevents _extract_ts from being rewritten
                            # and breaks the downstream silver/gold
                            # propagation chain. See LIMITS.md §P1.17-L7
                            # (resolved). The NULL-safe `<=>` join predicate
                            # handles composite keys with NULL components
                            # (LIMITS.md P1.17-L8 — gl_period_balances).
                            #
                            # P1.17d — when target-wider columns exist (target
                            # carries cols the source lacks), switch from
                            # `UPDATE SET *` / `INSERT *` (which would NULL
                            # those target-only cols on UPDATE or trip Spark
                            # AnalysisException on INSERT) to explicit-column-
                            # list MERGE over (common ∪ source_only). The
                            # payload-diff predicate continues to gate the
                            # explicit UPDATE — both optimizations compose.
                            natural_key_join = _natural_key_join_sql(pvo.natural_key)
                            payload_diff = _payload_diff_predicate_sql(df.schema.names)
                            if reconcile.target_only_columns:
                                merge_cols = (
                                    reconcile.common_columns + reconcile.source_only_columns
                                )
                                when_matched_clause = build_explicit_when_matched_clause(
                                    merge_cols, payload_diff=payload_diff,
                                )
                                when_not_matched_clause = build_explicit_when_not_matched_clause(
                                    merge_cols,
                                )
                            elif payload_diff is not None:
                                when_matched_clause = (
                                    f"WHEN MATCHED AND ({payload_diff}) THEN UPDATE SET *"
                                )
                                when_not_matched_clause = "WHEN NOT MATCHED THEN INSERT *"
                            else:
                                # Defensive — bronze schema has no non-audit
                                # columns (shouldn't reach this path in
                                # practice; V1 unconditional shape preserves
                                # today's behavior under this edge).
                                when_matched_clause = "WHEN MATCHED THEN UPDATE SET *"
                                when_not_matched_clause = "WHEN NOT MATCHED THEN INSERT *"
                            spark.sql(f"""
                                MERGE INTO {target} AS target
                                USING _p117_bronze_src AS src
                                ON {natural_key_join}
                                {when_matched_clause}
                                {when_not_matched_clause}
                            """)
                            materialized_count = spark.table(target).count()
                finally:
                    df.unpersist()

                # B5 empty-delta gate uses SOURCE count (BICC delta size),
                # NOT materialized count (which under MERGE includes
                # existing rows — would falsely advance the cursor on
                # an empty delta against a non-empty target).
                new_wm = persisted_cursor if source_delta_count > 0 else prior_watermark

                # State row's `row_count` column carries the materialized
                # (target) count — operators reading
                # `fusion_bundle_state.row_count` for a bronze row see
                # the table's current size, matching Phase α audit
                # semantics exactly.
                return (materialized_count, new_wm)

            # P1.5α-fix20: transient infra hiccups (OCI Object Storage 5xx,
            # Spark executor loss, BICC connection reset) shouldn't waste a
            # multi-PVO pipeline. Permanent bugs (schema-not-found, auth, Delta
            # merge errors) skip retry entirely and fail fast — preserves the
            # cascade-vs-abort contract by NOT masking real bugs.
            from .retry import run_with_retry
            row_count, new_wm = run_with_retry(_do_bronze, dataset_id=node.dataset_id)
            # P1.5β.1 monotonicity check (bronze only). Under the
            # orchestrator-wall-clock contract, time moves forward and
            # the cursor strictly increases; this check is defensive —
            # it fires only on clock-jumping VMs (NTP correction,
            # suspend/resume warp) larger than the safety window, OR
            # if a future change reintroduces a non-wall-clock cursor.
            # Empty-delta runs (where ``new_wm == prior_watermark``)
            # pass trivially.
            if (
                prior_watermark is not None
                and new_wm is not None
                and new_wm < prior_watermark
            ):
                raise WatermarkMonotonicityError(
                    prior=prior_watermark,
                    new=new_wm,
                    dataset_id=node.dataset_id,
                )
            return RunStep.success(
                node, run_id, mode,
                row_count=row_count,
                duration_seconds=perf_counter() - t0,
                watermark_used=prior_watermark,  # in-memory audit only (B0)
                last_watermark=new_wm,
                plan_hash=plan_hash,
                plan_snapshot=plan_snapshot,
            )

        if isinstance(node, (SilverDimSpec, GoldMartSpec)):
            from .retry import run_with_retry

            # P1.17 C5 + B4 — TWO-READ shape:
            #   READ #1 (prior_watermark above): upstream-bronze cursor —
            #     `(depends_on_bronze[0], "bronze")`. Used for
            #     RunStep.watermark_used in-memory audit only (β.1 B0).
            #     NOT passed to the silver/gold builder.
            #   READ #2 (here):                  layer-local cursor —
            #     `(node.dataset_id, layer)`. Threaded to the builder's
            #     `watermark` kwarg per B8a. Filters the MERGE source
            #     predicate `WHERE bronze_extract_ts > <watermark>`.
            #
            # `dim_calendar` skips READ #2 (no source watermark, resolver
            # returned None for READ #1 already).
            _own_layer = _layer_for_spec(node)
            if _wm_source is None:
                own_layer_wm: datetime | None = None
            else:
                own_layer_wm = state.read_last_watermark(
                    spark, paths, node.dataset_id, _own_layer,
                )

            # P1.17 B4 / B3b — for marts flagged `incremental_capable=False`
            # the builder ignores `refresh_mode` and always emits seed-shape
            # SQL. Orchestrator still passes the kwargs for signature
            # symmetry, but downgrades the mode signal to "seed" so the
            # builder's branch logic never sees "incremental" for an
            # exempt mart.
            effective_refresh_mode = (
                mode if (
                    mode == "incremental"
                    and getattr(node, "incremental_capable", True)
                ) else "seed"
            )

            # P1.17 C5a — builder dispatch differs by spec class:
            #   - dim_calendar (no upstream bronze) → no refresh_mode /
            #     watermark kwargs (Invariant 3).
            #   - all other silver/gold → (refresh_mode, watermark)
            #     kwargs added.
            def _do_silver_gold() -> int:
                if _wm_source is None:
                    df = node.builder(spark, paths=paths, run_id=run_id)
                else:
                    df = node.builder(
                        spark, paths=paths, run_id=run_id,
                        refresh_mode=effective_refresh_mode,
                        watermark=own_layer_wm,
                    )
                return df.count()

            row_count = run_with_retry(_do_silver_gold, dataset_id=node.dataset_id)

            # P1.17 B8 + C5 — silver/gold `last_watermark` capture in
            # BOTH seed AND incremental modes (except dim_calendar — no
            # `bronze_extract_ts` column). Seed-mode capture is what
            # populates the FIRST incremental run's READ #2 (own_layer_wm)
            # — without it, the first incremental run sees
            # `own_layer_wm = None` and trips B4b's preflight.
            new_wm: datetime | None
            if _wm_source is not None:
                target_table = _resolve_target_table(node, paths)
                wm_row = spark.sql(
                    f"SELECT MAX(bronze_extract_ts) AS wm FROM {target_table}"
                ).first()
                new_wm = wm_row["wm"] if wm_row is not None else None
            else:
                new_wm = None  # dim_calendar — no source watermark

            return RunStep.success(
                node, run_id, mode,
                row_count=row_count,
                duration_seconds=perf_counter() - t0,
                watermark_used=prior_watermark,  # upstream-bronze, in-memory audit (B0)
                last_watermark=new_wm,           # layer-local, persisted (B8)
                plan_hash=plan_hash,
                plan_snapshot=plan_snapshot,
            )

        if isinstance(node, DeferredSpec):
            return RunStep.deferred(
                node, run_id, mode, error_message=node.reason,
                plan_hash=plan_hash,
                plan_snapshot=plan_snapshot,
            )

    except Exception as exc:
        # Module-dispatch error. Record as failed; the run loop cascades.
        return RunStep.failed(
            node, run_id, mode,
            exc=exc,
            duration_seconds=perf_counter() - t0,
            plan_hash=plan_hash,
            plan_snapshot=plan_snapshot,
        )

    # Unknown spec type — orchestrator wiring bug, not a data event.
    raise TypeError(f"unknown node type: {type(node).__name__}")


# ---------------------------------------------------------------------------
# _skip_dependents + _abort_remaining (two-phase cascade)
# ---------------------------------------------------------------------------


def _skip_dependents(
    plan: list[Spec],
    failed_node: Spec,
    run_id: str,
    mode: str,
    steps: list[RunStep],
    spark: "SparkSession",
    paths: TablePaths,
    *,
    plan_hash: str | None = None,
    plan_snapshot: str | None = None,
) -> None:
    """Phase 1: emit ``RunStep.skipped_cascade`` for every transitive
    downstream of ``failed_node`` still in the plan.

    No-op-safe for nodes with zero downstreams (e.g. a failing gold leaf).
    Tolerates the empty-downstream case without raising — important
    because §4.4's cascade unconditionally runs ``_skip_dependents`` then
    ``_abort_remaining`` without a pre-check.
    """
    failed_id = failed_node.dataset_id
    already_done = {s.dataset_id for s in steps}

    # Walk the plan forward, marking transitive descendants.
    # A node is a descendant if any of its bronze/silver deps is the
    # failed node OR a node already marked as cascade-skipped here.
    cascade_set = {failed_id}
    for node in plan:
        if node.dataset_id in already_done:
            continue
        deps: tuple[str, ...] = ()
        if isinstance(node, SilverDimSpec):
            deps = node.depends_on_bronze
        elif isinstance(node, GoldMartSpec):
            deps = node.depends_on_bronze + node.depends_on_silver
        if any(d in cascade_set for d in deps):
            cascade_set.add(node.dataset_id)
            step = RunStep.skipped_cascade(
                node, run_id, mode, upstream_dataset_id=failed_id,
                plan_hash=plan_hash,
                plan_snapshot=plan_snapshot,
            )
            steps.append(step)
            _safe_write_state_row(spark, paths, step)


def _abort_remaining(
    plan: list[Spec],
    failed_node: Spec,
    run_id: str,
    mode: str,
    steps: list[RunStep],
    spark: "SparkSession",
    paths: TablePaths,
    *,
    plan_hash: str | None = None,
    plan_snapshot: str | None = None,
) -> None:
    """Phase 2: emit ``RunStep.skipped_aborted`` for every plan node not
    yet in ``steps`` (independent branches + unattempted leaves).

    Closes the audit-completeness gap (pre-2026-05-17 ``break`` left
    independent-branch rows missing). Every plan node receives exactly
    one state row per run.
    """
    failed_id = failed_node.dataset_id
    already_done = {s.dataset_id for s in steps}
    for node in plan:
        if node.dataset_id in already_done:
            continue
        # Deferred specs never get abort-marked — they get their own
        # 'deferred' status row during dispatch. But the run halted
        # before dispatching this one, so emit the abort row instead.
        step = RunStep.skipped_aborted(
            node, run_id, mode, failed_dataset_id=failed_id,
            plan_hash=plan_hash,
            plan_snapshot=plan_snapshot,
        )
        steps.append(step)
        _safe_write_state_row(spark, paths, step)


# ---------------------------------------------------------------------------
# Spark bootstrap (overridable)
# ---------------------------------------------------------------------------


def _bootstrap_spark() -> "SparkSession":
    """Construct (or get) a SparkSession. Callers can pass ``spark=...`` to
    ``run()`` to inject their own (notebook session uses the AIDP-injected
    one); standalone laptop callers fall through to ``builder.getOrCreate``.
    """
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]

    return SparkSession.builder.appName("aidp-fusion-bundle-orchestrator").getOrCreate()


def _effective_bundle_scope(bundle: "Any") -> set[str]:
    """Compute the cross-layer scope the resolver should treat as roots.

    Phase 9 contract: ``bundle.datasets[]`` is the operator's high-level
    intent list. It can reference bronze / silver / gold ids (D-1
    auto-pulls transitive deps). Two legacy bundle fields —
    ``bundle.dimensions.build`` and ``bundle.gold.marts`` — pre-date the
    cross-layer ``datasets[]`` contract; when the YAML actually carries
    those blocks they fold into the scope so old bundles keep working.

    **Presence-aware**: the Pydantic schema ships non-empty defaults for
    ``dimensions.build`` (``dim_supplier``, ``dim_account``,
    ``dim_calendar``, ``dim_org``) and ``gold.marts`` (``ar_aging``,
    ``ap_aging``, ``gl_balance``, ``po_backlog``). A Phase 9 bundle that
    omits the blocks entirely would otherwise have those default ids
    smuggled into the scope. The check uses ``bundle.model_fields_set``
    — Pydantic's "fields the constructor was given" record — to fold
    only when the YAML actually authored the block. An author who
    explicitly writes ``dimensions: { build: [] }`` (or a non-empty
    list) marks ``dimensions`` as set and the inner ``build`` list is
    honored regardless of contents.

    Disabled datasets (``DatasetSpec.enabled = False``) are excluded —
    same contract the legacy resolver honored.

    Returns the SET of declared root ids. The resolver consumes this
    as ``bundle_scope=`` and:
      * Uses it as the implicit root set when no CLI ``--datasets``
        filter is given (so a no-filter run executes only declared
        roots + D-1 deps, NOT every pack node).
      * Validates CLI ``--datasets`` is a subset of it; ids outside
        the scope raise ``AIDPF-1043 CLI_DATASET_OUTSIDE_BUNDLE_SCOPE``.
    """
    scope: set[str] = set()
    for ds in getattr(bundle, "datasets", []) or []:
        if getattr(ds, "enabled", True):
            scope.add(ds.id)
    bundle_fields_set = getattr(bundle, "model_fields_set", set()) or set()
    # ``dimensions.build`` only folds when the YAML carries a
    # ``dimensions:`` block. Without this guard, Pydantic's non-empty
    # ``DimensionsSpec.build`` default would smuggle dim_supplier /
    # dim_account / dim_calendar / dim_org into the scope of every
    # Phase 9 bundle that omits the block.
    if "dimensions" in bundle_fields_set:
        dims = getattr(bundle, "dimensions", None)
        if dims is not None:
            for name in getattr(dims, "build", None) or []:
                scope.add(str(name))
    # Same guard for ``gold.marts`` (defaults to
    # ar_aging / ap_aging / gl_balance / po_backlog).
    if "gold" in bundle_fields_set:
        gold = getattr(bundle, "gold", None)
        if gold is not None:
            for name in getattr(gold, "marts", None) or []:
                scope.add(str(name))
    return scope


# ---------------------------------------------------------------------------
# Public API — run()
# ---------------------------------------------------------------------------


def run(
    bundle_path: Path,
    *,
    spark: "SparkSession | None" = None,
    mode: str = "seed",
    datasets: list[str] | None = None,
    layers: list[str] | None = None,
    dry_run: bool = False,
    resume_run_id: str | None = None,
    # Phase 9 — legacy `execution_backend` kwarg retained for backwards
    # compatibility with callers (tests, programmatic uses) that pass it
    # explicitly; the value is IGNORED. The only execution path is
    # content-pack now (v1 modules deleted). See ADR-0022.
    execution_backend: str = "content-pack",
    resolved_pack: "Any | None" = None,
    tenant_profile: "Any | None" = None,
    # Phase 3c — runtime drift gate bypass (dev/sandbox; hidden flag).
    force_fingerprint_skip: bool = False,
    # Phase 9 — opt-out of D-1 implicit-transitive-include in the plan
    # resolver. When True, declared roots must include every transitive
    # dep explicitly; missing deps raise AIDPF-1042.
    strict_scope: bool = False,
    # Phase 5 Step 5 — shared run_id contract retained for resume
    # semantics. Private contract; the CLI never passes this directly.
    _forced_run_id: str | None = None,
) -> RunSummary:
    """Materialize bronze + silver + gold per the bundle.yaml plan.

    Args:
        bundle_path: path to ``bundle.yaml``.
        spark: optional pre-existing SparkSession (notebook callers pass
            the AIDP-injected one; standalone callers leave None to use
            ``_bootstrap_spark``).
        mode: ``"seed"`` (Phase α — full overwrite per layer) or
            ``"incremental"`` (P1.17 — bronze MERGE + row-level
            silver/gold MERGE; exempt marts `supplier_spend`,
            `ap_aging`, `dim_calendar` always run seed-shape).
        datasets: ``--datasets`` CSV filter, classified across registries.
        layers: ``--layers`` filter, e.g. ``["gold"]``.
        dry_run: skip execution; return ``RunSummary.empty(..., plan=...)``
            with the would-run plan and extra-plan prereqs populated.
        resume_run_id: when set, resume the named run_id from its
            checkpoint. Reads ``fusion_bundle_state``, skips datasets
            whose latest terminal row is ``success`` or
            ``resumed_skipped``, re-attempts the rest under the
            original ``run_id``. Bundle drift raises
            ``ResumeBundleMismatchError``; unknown / non-resumable
            runs raise ``ResumeRunNotFoundError`` /
            ``ResumeRunNotResumableError``.

    Returns:
        ``RunSummary`` with one ``RunStep`` per plan node (or empty for
        dry-run / empty-bundle paths).

    Raises:
        UnsupportedModeError: mode not in ``{"seed", "incremental"}``.
        IncrementalCursorMissingError: ``mode="incremental"`` requested
            but one or more silver/gold nodes lack a prior cursor in
            ``fusion_bundle_state``. Run ``--mode seed`` first.
        BundleLoadError: any bundle.yaml load failure.
        CredentialResolutionError: ``bundle.fusion.password`` unresolvable.
        MissingDependencyError: typo in datasets/dims/marts.
        PrerequisiteError: extra-plan dependency missing on disk.
        ResumeRunNotFoundError: ``resume_run_id`` has no rows in
            ``fusion_bundle_state``.
        ResumeRunNotResumableError: ``resume_run_id`` exists but
            lacks ``plan_hash`` or ``plan_snapshot`` (legacy row or
            partially-migrated write path).
        ResumeBundleMismatchError: stored vs current plan hash diverge.
    """
    # 0. Mode validation (§4.4c) — runs BEFORE any I/O.
    if mode not in _VALID_MODES:
        raise UnsupportedModeError(
            f"mode={mode!r} is not supported. Valid modes: "
            f"{sorted(_VALID_MODES)}. "
            f"(The retired alias 'full' is now called 'seed'.)"
        )
    # P1.17 — the β.1 NotImplementedError gate is gone. `mode="incremental"`
    # now dispatches the bronze MERGE + silver/gold MERGE pipeline; the
    # write-strategy / state-contract pieces shipped together to keep the
    # destructive-write blast radius contained.

    # Phase 9 — single execution path is content-pack. Phase 5's
    # `execution_backend` kwarg is retained for backwards compatibility
    # with programmatic callers but its value is effectively ignored:
    # content-pack is the only dispatcher (see ADR-0022). The kwarg
    # gating below preserves the legacy-python entry for tests that
    # exercise the v1 dispatch path until that test surface is
    # rewritten (Phase 9 follow-up — see Step 8 in the plan).
    if execution_backend == "content-pack":
        return _phase5_top_level_dispatch(
            bundle_path=bundle_path,
            spark=spark,
            mode=mode,
            datasets=datasets,
            layers=layers,
            dry_run=dry_run,
            resume_run_id=resume_run_id,
            resolved_pack=resolved_pack,
            tenant_profile=tenant_profile,
            force_fingerprint_skip=force_fingerprint_skip,
            strict_scope=strict_scope,
        )

    # Phase 9 — v1 main loop deleted (ADR-0022). Reaching this point
    # means execution_backend != "content-pack" was passed, which is
    # not a supported value anymore.
    raise OrchestratorConfigError(
        f"execution_backend={execution_backend!r} is not supported; "
        f"the v1 dispatch path was removed in Phase 9 (ADR-0022). "
        f"Use the default content-pack backend."
    )


# ---------------------------------------------------------------------------
# Phase 5 — top-level dispatcher (scope-split, shared run_id, gates)
# ---------------------------------------------------------------------------


def _phase5_top_level_dispatch(
    *,
    bundle_path: "Path",
    spark: "SparkSession | None",
    mode: str,
    datasets: list[str] | None,
    layers: list[str] | None,
    resume_run_id: str | None,
    resolved_pack: "Any | None",
    tenant_profile: "Any | None",
    force_fingerprint_skip: bool,
    dry_run: bool = False,
    strict_scope: bool = False,
) -> RunSummary:
    """Single-path top-level dispatcher (Phase 9, ADR-0022).

    Bronze + silver + gold all dispatch through the content-pack runner
    (``_run_content_pack_backend``). The Phase 5 scope-split + recursive
    bronze-via-legacy-python dance is gone — bronze is now a first-class
    layer in ``pack.bronze`` and ``resolve_content_pack_plan`` walks all
    three layers uniformly.

    Sequence:

    1. Load bundle + validate ``contentPack`` block present (AIDPF-1031
       / AIDPF-1030).
    2. Resolve resume context (when ``resume_run_id`` is supplied).
    3. Dry-run path: emit the content-pack plan + return.
    4. Mint a single shared ``run_id`` (or adopt the resume id).
    5. Run the Fusion PVO drift gate (AIDPF-2072) when bronze nodes
       are in scope — fires BEFORE any state write.
    6. Delegate to ``_run_content_pack_backend`` with the full
       ``(datasets, layers)`` filter.
    """
    from datetime import datetime as _dt, timezone as _tz
    from ..schema.bundle import (
        AIDPF_1030_PROFILE_MISSING,
        AIDPF_1031_CONTENT_PACK_MISSING,
        load_bundle as _load_bundle_v2,
    )

    bundle, paths = _load_bundle_v2(bundle_path)

    if bundle.content_pack is None:
        raise OrchestratorConfigError(
            f"{AIDPF_1031_CONTENT_PACK_MISSING}: bundle.yaml has no "
            f"`contentPack:` block; the content-pack backend "
            f"requires it. Add the `contentPack:` block to bundle.yaml."
        )
    if bundle.content_pack.profile is None:
        raise OrchestratorConfigError(
            f"{AIDPF_1030_PROFILE_MISSING}: bundle.yaml's "
            f"contentPack.profile field is missing."
        )

    # Resume context resolution — read fusion_bundle_state to:
    #   1. Reject unknown run_ids via ResumeRunNotFoundError.
    #   2. Reconstruct (datasets, layers) when a bare --resume is supplied.
    #   3. Surface succeeded nodes so the per-node loop emits
    #      resumed_skip instead of re-dispatching.
    # Dry-run skips state I/O.
    resume_context = None
    if resume_run_id is not None and not dry_run:
        from . import state_phase2 as _state_phase2
        from .resume import check_identity_drift, reconstruct_resume_scope

        spark = spark or _bootstrap_spark()
        state.ensure_state_table(spark, paths)
        _state_phase2.ensure_state_columns_v2(spark, paths)
        resume_context = state.read_content_pack_resumable_state(
            spark, paths, resume_run_id,
        )

        if resume_context.bronze_plan_snapshot is not None:
            from oracle_ai_data_platform_fusion_bundle import __version__ as _pv
            check_identity_drift(
                resume_context.bronze_plan_snapshot,
                bundle=bundle, paths=paths, plugin_version=_pv,
                run_id=resume_context.run_id,
            )

        if datasets is None and layers is None:
            if resume_context.bronze_plan_snapshot is not None:
                datasets, layers = reconstruct_resume_scope(
                    resume_context.bronze_plan_snapshot,
                )
            else:
                datasets = list(resume_context.scope_datasets)
                layers = list(resume_context.scope_layers)

    # Dry-run — emit the would-run plan and return.
    if dry_run:
        plan_nodes = _build_content_pack_dry_run_plan(
            resolved_pack=resolved_pack,
            datasets=datasets,
            layers=layers,
            strict_scope=strict_scope,
            bundle_scope=_effective_bundle_scope(bundle),
        )
        return RunSummary.empty(
            bundle_project=bundle.project, mode=mode, plan=plan_nodes,
        )

    # Mint the shared run_id.
    if resume_context is not None:
        shared_run_id = resume_context.run_id
    elif resume_run_id is not None:
        shared_run_id = resume_run_id
    else:
        shared_run_id = _new_run_id()

    started_at = _dt.now(_tz.utc)

    # Fusion PVO drift gate (AIDPF-2072). Fires BEFORE state writes
    # when bronze nodes are in scope.
    #
    # Phase 9 review fix: enumerate in-scope bronze ids from the
    # RESOLVED PLAN, not from the raw (datasets, layers) filter. D-1
    # implicit-transitive-include adds bronze deps for silver/gold
    # roots — e.g. ``--datasets supplier_spend`` or ``--layers gold``
    # still executes ``ap_invoices`` + ``erp_suppliers``. Computing the
    # gate scope from raw filters missed those transitive bronze
    # extracts, letting Fusion column drift slip past the AIDPF-2072
    # gate and surface as opaque downstream failures.
    bundle_scope = _effective_bundle_scope(bundle)
    in_scope_bronze: set[str] = set()
    if resolved_pack is not None:
        try:
            from .content_pack_plan_resolver import resolve_content_pack_plan
            gate_plan = resolve_content_pack_plan(
                resolved_pack,
                datasets=datasets, layers=layers,
                strict_scope=strict_scope,
                bundle_scope=bundle_scope,
            )
            in_scope_bronze = {n.id for n in gate_plan if n.layer == "bronze"}
        except Exception:  # noqa: BLE001 — resolver failures surface
            # again from _run_content_pack_backend; the gate just
            # degrades to "no bronze in scope" here.
            in_scope_bronze = set()
        # Legacy bronze.yaml fallback: a pack that hasn't migrated to
        # per-file bronze/<id>.yaml carries its bronze ids only in
        # pack.bronze_yaml. Resolver returns them as part of the plan
        # already (resolve_content_pack_plan walks pack.bronze), so the
        # set above is complete; this loop is belt-and-braces.
        bronze_yaml = getattr(resolved_pack, "bronze_yaml", None) or {}
        legacy_ids = {
            str(ds["id"]) for ds in bronze_yaml.get("datasets", []) or []
            if isinstance(ds, dict) and "id" in ds
        }
        if legacy_ids:
            # Apply the same filter shape as the resolver would have.
            if datasets is not None:
                legacy_ids &= set(datasets)
            if layers is not None and "bronze" not in {l.lower() for l in layers}:
                legacy_ids = set()
            in_scope_bronze |= legacy_ids

    if in_scope_bronze:
        in_scope_bronze = _filter_resume_succeeded(in_scope_bronze, resume_context)
    if in_scope_bronze:
        gate_step = _phase5_run_fusion_pvo_drift_gate(
            bundle=bundle,
            bundle_path=bundle_path,
            spark=spark,
            bronze_filter=(sorted(in_scope_bronze), None),
            cp_filter=None,
            resolved_pack=resolved_pack,
            tenant_profile=tenant_profile,
            run_id=shared_run_id,
            mode=mode,
        )
        if gate_step is not None:
            return RunSummary(
                run_id=shared_run_id,
                started_at=started_at,
                finished_at=_dt.now(_tz.utc),
                bundle_project=bundle.project,
                mode=mode,
                steps=(gate_step,),
            )

    return _run_content_pack_backend(
        bundle_path=bundle_path,
        spark=spark,
        mode=mode,
        datasets=datasets,
        layers=layers,
        dry_run=False,
        resume_run_id=resume_run_id,
        resolved_pack=resolved_pack,
        tenant_profile=tenant_profile,
        force_fingerprint_skip=force_fingerprint_skip,
        shared_run_id=shared_run_id,
        enable_bronze_readiness_gate=False,
        shared_resume_context=resume_context,
        strict_scope=strict_scope,
    )


def _filter_resume_succeeded(
    bronze_ids: set[str], resume_context: "Any | None",
) -> set[str]:
    """Drop bronze ids whose latest state row is already success."""
    if resume_context is None:
        return bronze_ids
    succeeded = getattr(resume_context, "succeeded", None) or set()
    return {b for b in bronze_ids if b not in succeeded}


# ---------------------------------------------------------------------------
# Phase 5 Step 2d — Fusion PVO drift gate wiring (AIDPF-2072)
# ---------------------------------------------------------------------------


def _struct_type_to_columns_map(
    struct_type: "Any",
) -> dict[str, str]:
    """Flatten a Spark ``StructType`` to ``{col_name_lower: type_string}``.

    Used to feed ``assert_fusion_pvo_compatibility`` which expects the
    live schema in dict form (case-insensitive keys, simple type
    strings). Resilient to test fakes that don't expose
    ``.fields`` — falls back to ``.names`` + ``.dataType`` if needed.
    """
    out: dict[str, str] = {}
    fields = getattr(struct_type, "fields", None)
    if fields is None:
        return out
    for f in fields:
        name = getattr(f, "name", None)
        if name is None:
            continue
        dtype = getattr(f, "dataType", None)
        if dtype is None:
            type_str = ""
        else:
            simple = getattr(dtype, "simpleString", None)
            type_str = simple() if callable(simple) else str(dtype)
        out[name.lower()] = type_str
    return out


def _phase5_run_fusion_pvo_drift_gate(
    *,
    bundle: "Any",
    bundle_path: "Path",
    spark: "SparkSession | None",
    bronze_filter: tuple[list[str] | None, list[str] | None],
    cp_filter: tuple[list[str] | None, list[str] | None] | None,
    resolved_pack: "Any | None",
    tenant_profile: "Any | None",
    run_id: str,
    mode: str,
) -> "RunStep | None":
    """Phase 5 Step 2d — fire the AIDPF-2072 PVO drift gate.

    Runs BEFORE the bronze branch in ``_phase5_top_level_dispatch``.
    Probes the live Fusion PVO schemas via the metadata-only BICC
    primitive ``preflight_bronze_schemas`` (no row transfer), loads the
    pinned per-dataset snapshot if present, then hands the pair off to
    ``assert_fusion_pvo_compatibility``.

    Args:
        bundle: loaded ``Bundle``.
        bundle_path: path to ``bundle.yaml`` (used to resolve the
            snapshot file under ``profiles/``).
        spark: caller-supplied session or ``None``. Bootstrapped if None.
        bronze_filter: ``scope.bronze_filter`` from ``split_run_scope``;
            limits which bronze ids the gate complains about.
        cp_filter: ``scope.cp_filter`` from ``split_run_scope``;
            narrows the silver/gold required-column union.
        resolved_pack: loaded ``ResolvedPack`` or ``None`` (bronze-only
            run — required-column check is skipped).
        tenant_profile: loaded ``TenantProfile`` or ``None``.
        run_id: shared run identifier; threaded into the diagnostic path.
        mode: ``"seed"`` or ``"incremental"`` (carried on the
            ``gate_failed`` RunStep).

    Returns:
        ``None`` when the gate passes (or has nothing to do — empty
        bronze plan, all probes failed and surfaced elsewhere).
        A synthetic :class:`RunStep` with ``status='failed'`` carrying
        AIDPF-2072 when the gate detects drift. The dispatcher consumes
        this and returns a one-step ``RunSummary`` — bronze never runs.

    Notes:
        * The dispatcher-level preflight call is intentionally distinct
          from the legacy bronze path's own preflight inside the
          recursive ``run()``. Both are metadata-only and idempotent;
          the double-probe is wasteful but correct, and lifting the
          preflight result down into the legacy path would require a
          new ``_skip_preflight`` kwarg layered through ``run()``.
          TODO(phase-6): factor the preflight to a single dispatcher-
          owned probe and skip the legacy re-run.
        * A snapshot YAML that's absent OR unparseable degrades the
          gate to missing-column / renamed-column detection only —
          matches the contract in ``fusion_pvo_drift.py``.
        * Failures during preflight itself (BronzeSchemaProbeError,
          credential failures) are NOT caught here — they propagate so
          the operator sees the real probe error, not a synthetic
          gate-failure step that hides the real cause.
    """
    from .fusion_pvo_drift import (
        AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
        FusionPvoDriftError,
        assert_fusion_pvo_compatibility,
    )
    from .builtins.bronze_extract_adapter import probe_bronze_schemas
    from ..schema.bronze_schema_snapshot import (
        BronzeSchemaSnapshotSchemaError,
        load_bronze_schema_snapshot,
        resolve_snapshot_path,
    )

    bundle_inst, paths = load_bundle(bundle_path)

    # Phase 9 — enumerate bronze ids from the resolved pack. Honors both
    # per-file pack.bronze (Phase 9 contract) and the legacy single-file
    # pack.bronze_yaml fallback.
    bronze_node_ids = set(resolved_pack.bronze.keys()) if resolved_pack else set()
    if resolved_pack is not None:
        legacy_bronze = getattr(resolved_pack, "bronze_yaml", None) or {}
        for ds in legacy_bronze.get("datasets", []) or []:
            if isinstance(ds, dict) and "id" in ds:
                bronze_node_ids.add(str(ds["id"]))

    # Narrow to the scope's bronze filter.
    bronze_datasets, bronze_layers = bronze_filter
    if bronze_datasets is not None:
        bronze_node_ids &= set(bronze_datasets)
    if not bronze_node_ids:
        return None

    # Probe live PVO schemas via the bronze adapter (rehomed from the
    # deleted orchestrator/preflight.py). Metadata-only roundtrip — no
    # row transfer. Failures propagate so the operator sees them.
    spark_session = spark or _bootstrap_spark()
    resolved_password = _resolve_password(bundle_inst.fusion.password).get_secret_value()
    live_pvo_schemas = probe_bronze_schemas(
        spark_session,
        pack=resolved_pack,
        bundle=bundle_inst,
        resolved_password=resolved_password,
        dataset_ids=bronze_node_ids,
    )

    # Convert per-PVO ``StructType`` -> ``{col_name_lower: type_string}``.
    live_pvo_columns: dict[str, dict[str, str]] = {}
    for ds_id, struct_type in live_pvo_schemas.items():
        live_pvo_columns[ds_id] = _struct_type_to_columns_map(struct_type)

    # Load the pinned snapshot. Absent / unparseable → degraded mode
    # (None). Matches the Phase 3d graceful-degrade contract.
    schema_snapshot = None
    profile_name = (
        bundle_inst.content_pack.profile if bundle_inst.content_pack else None
    )
    if profile_name is not None:
        try:
            snapshot_path = resolve_snapshot_path(bundle_path, profile_name)
            if snapshot_path.exists():
                schema_snapshot = load_bronze_schema_snapshot(snapshot_path)
        except (BronzeSchemaSnapshotSchemaError, OSError):
            schema_snapshot = None

    diagnostics_root = bundle_path.resolve().parent / ".aidp" / "diagnostics"

    try:
        assert_fusion_pvo_compatibility(
            live_pvo_columns=live_pvo_columns,
            resolved_pack=resolved_pack,
            cp_filter=cp_filter,
            bronze_filter=bronze_filter,
            schema_snapshot=schema_snapshot,
            run_id=run_id,
            diagnostics_root=diagnostics_root,
            tenant_profile=tenant_profile,
        )
    except FusionPvoDriftError as exc:
        return RunStep.gate_failed(
            run_id=run_id,
            mode=mode,
            layer="bronze",
            gate_dataset_id="__fusion_pvo_drift_gate__",
            aidpf_code=AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
            error_message=str(exc),
        )
    return None


# ---------------------------------------------------------------------------
# Phase 5 Step 9b — resume helpers (dispatcher-side narrowing + skip emission)
# ---------------------------------------------------------------------------


def _resolve_scope_bronze_ids(
    bundle: "Any",
    bronze_filter: tuple[list[str] | None, list[str] | None],
) -> set[str]:
    """Return the set of bronze ids covered by ``bronze_filter``.

    ``(None, ["bronze"])`` → every enabled bronze id in the bundle.
    ``(["ap_invoices", "gl_coa"], None)`` → that intersection with
    the enabled set (so a typo dataset never sneaks in).
    """
    datasets, _layers = bronze_filter
    enabled_bronze_ids = {ds.id for ds in bundle.datasets if ds.enabled}
    if datasets is None:
        return enabled_bronze_ids
    return {d for d in datasets if d in enabled_bronze_ids}


def _narrow_bronze_filter_to_reattempt(
    bronze_filter: tuple[list[str] | None, list[str] | None],
    bundle: "Any",
    resume_context: "Any | None",  # state.ResumeContext | None
) -> tuple[list[str] | None, list[str] | None] | None:
    """Return a bronze filter narrowed to bronze ids that still need work.

    No resume → pass the filter through unchanged. With a resume
    context: subtract the ``succeeded`` set from the scope's bronze
    ids and rebuild a positive-list filter. All succeeded → return
    ``None`` (nothing left to dispatch on the bronze branch).
    """
    if resume_context is None:
        return bronze_filter
    scope_ids = _resolve_scope_bronze_ids(bundle, bronze_filter)
    reattempt_ids = sorted(scope_ids - resume_context.succeeded)
    if not reattempt_ids:
        return None
    return (reattempt_ids, None)


def _emit_dispatcher_resumed_skip_for_bronze(
    *,
    bronze_id: str,
    run_id: str,
    mode: str,
    resume_context: "Any",  # state.ResumeContext
    spark: "Any",
    paths: "Any",
) -> "RunStep | None":
    """Emit a ``resumed_skip`` step + soft-write a state row for an
    already-succeeded bronze id.

    Resolves the registry spec (real :class:`BronzeExtractSpec` or
    :class:`DeferredSpec` for deferred datasets) so the existing
    ``RunStep.resumed_skip`` factory can compute the layer + carry
    forward ``row_count`` / ``last_watermark`` from
    ``resume_context``. State-row write is best-effort — a failure
    only loses the audit trail and never raises.

    Returns ``None`` when the id resolves to neither a runnable spec
    nor a deferred entry (shouldn't happen in practice because
    ``_resolve_scope_bronze_ids`` already filtered to enabled
    bundle ids that v1 ``resolve_plan`` accepts).
    """
    from .registry import (
        BRONZE_EXTRACTS,
        DeferredSpec,
        KNOWN_DEFERRED_DATASETS,
    )

    if bronze_id in BRONZE_EXTRACTS:
        spec: "Any" = BRONZE_EXTRACTS[bronze_id]
    elif bronze_id in KNOWN_DEFERRED_DATASETS:
        spec = DeferredSpec(
            dataset_id=bronze_id,
            layer="bronze",
            reason=KNOWN_DEFERRED_DATASETS[bronze_id],
        )
    else:
        return None

    key = (bronze_id, "bronze")
    # CPResumeContext carries ``bronze_plan_snapshot`` (lifted from a
    # v1-shape bronze row, may be None for pure cp-only runs) and no
    # run-level ``plan_hash`` (CP writes per-node hashes). Resumed-skip
    # rows propagate the snapshot for audit-trail continuity; plan_hash
    # stays None — read_content_pack_resumable_state tolerates that.
    step = RunStep.resumed_skip(
        spec, run_id, mode,
        row_count=resume_context.succeeded_row_counts.get(key),
        last_watermark=resume_context.succeeded_last_watermarks.get(key),
        plan_hash=None,
        plan_snapshot=resume_context.bronze_plan_snapshot,
    )
    _safe_write_state_row(spark, paths, step)
    return step



def _build_content_pack_dry_run_plan(
    *,
    resolved_pack: "Any",
    datasets: list[str] | None,
    layers: list[str] | None,
    strict_scope: bool = False,
    bundle_scope: set[str] | None = None,
) -> tuple[Any, ...]:
    """Return a tuple of :class:`PlanNode` for the content-pack dry-run path.

    Phase 9 contract: when ``bundle_scope`` is supplied, the resolver
    treats it as the declared-root ceiling. Without it (callers that
    pre-date the bundle-scope wiring), the resolver falls back to
    "every pack node is a root" — which lies to the operator when the
    bundle declares only a subset. Production callers
    (``_phase5_top_level_dispatch`` dry-run + REST dispatch) must pass
    ``bundle_scope=_effective_bundle_scope(bundle)``.

    The implementation walks ``resolve_content_pack_plan`` (the same
    resolver the runtime uses) so the dry-run plan is byte-equivalent
    to what would actually run — minus the side effects.
    """
    from .content_pack_plan_resolver import resolve_content_pack_plan

    plan = resolve_content_pack_plan(
        resolved_pack, datasets=datasets, layers=layers,
        strict_scope=strict_scope,
        bundle_scope=bundle_scope,
    )
    plan_nodes = tuple(
        PlanNode(
            dataset_id=node.id,
            layer=node.layer,
            status="eligible",
            reason=None,
        )
        for node in plan
    )
    return plan_nodes


# ---------------------------------------------------------------------------
# Phase 5 — pack-driven node discovery
# ---------------------------------------------------------------------------


class PackNodeNotFoundError(OrchestratorRuntimeError):
    """Requested node id is not in the resolved pack's silver/gold maps.

    Raised by :func:`_resolve_node_from_pack` when a caller hands in a
    layer + node_id pair that doesn't exist on the loaded pack. Surfaces
    pack-author mistakes (typo in a YAML id) without conflating with
    the registry-lookup errors raised under the legacy backend.
    """


def _resolve_node_from_pack(
    pack: "Any",  # ResolvedPack — typed without import to avoid load-time cycles
    layer: str,
    node_id: str,
) -> "Any":  # NodeYaml
    """Look up a content-pack node by ``(layer, node_id)`` — Phase 5.

    The orchestrator's per-node dispatch loop (
    :func:`_run_content_pack_backend`) walks ``resolve_content_pack_plan``'s
    output directly — that path already returns ``NodeYaml`` objects.
    This helper exists so that direct callers (tests, future
    integrations, dry-run plan renderers) can ask the pack the same
    question without re-walking the plan resolver: "give me the
    ``NodeYaml`` for silver/dim_supplier".

    Per-node ``implementation.type`` (``sql`` / ``builtin`` /
    ``bronze_extract``) discriminates the runtime path; the dispatch
    itself is inside ``sql_runner.execute_node``.

    Args:
        pack: the resolved content pack (``ResolvedPack``).
        layer: ``"bronze"`` / ``"silver"`` / ``"gold"``.
        node_id: pack-author node id (matches ``NodeYaml.id``).

    Returns:
        The :class:`NodeYaml` for that ``(layer, node_id)``.

    Raises:
        ValueError: ``layer`` not in ``{"bronze", "silver", "gold"}``.
        PackNodeNotFoundError: ``node_id`` is absent from
            ``pack.bronze`` / ``pack.silver`` / ``pack.gold``.
    """
    if layer == "bronze":
        bucket = getattr(pack, "bronze", {})
    elif layer == "silver":
        bucket = getattr(pack, "silver", {})
    elif layer == "gold":
        bucket = getattr(pack, "gold", {})
    else:
        raise ValueError(
            f"_resolve_node_from_pack: layer={layer!r} not in "
            f"{{'bronze', 'silver', 'gold'}}."
        )
    if node_id not in bucket:
        available = sorted(bucket.keys())
        raise PackNodeNotFoundError(
            f"_resolve_node_from_pack: pack has no {layer} node "
            f"{node_id!r}. Available {layer} node ids: {available!r}. "
            f"Check the pack's {layer}/*.yaml files."
        )
    return bucket[node_id]


# ---------------------------------------------------------------------------
# Phase 2 — content-pack execution backend dispatcher
# ---------------------------------------------------------------------------


def _run_content_pack_backend(
    *,
    bundle_path: "Path",
    spark: "SparkSession | None",
    mode: str,
    datasets: "list[str] | None",
    layers: "list[str] | None",
    dry_run: bool,
    resume_run_id: str | None,
    resolved_pack: "Any | None",
    tenant_profile: "Any | None",
    force_fingerprint_skip: bool = False,
    # Phase 5 Step 5 — shared run_id contract. When the top-level
    # dispatcher (the caller) already minted a run_id (because bronze
    # + content-pack must share one), pass it in and the content-pack
    # backend will adopt it instead of minting `cp-<timestamp>-<hex>`.
    shared_run_id: str | None = None,
    # Phase 5 Step 5 — enable the Step 2c bronze readiness gate.
    # Default off so unit tests / direct callers that don't pre-seed
    # bronze tables don't trip on missing tables; the top-level
    # dispatcher in `run()` flips this on for full-medallion invocations.
    enable_bronze_readiness_gate: bool = False,
    # Phase 5 Step 9b — resume support. When the top-level dispatcher
    # read fusion_bundle_state to build a ResumeContext, it threads
    # the snapshot through here so the per-node loop can short-circuit
    # already-succeeded nodes (emit ``resumed_skip`` instead of
    # re-dispatching) and the bronze-readiness gate (above) narrows
    # to the reattempt-only cp_filter. ``None`` outside a resume.
    shared_resume_context: "Any | None" = None,
    # Phase 9 — disable D-1 transitive include in the plan resolver.
    strict_scope: bool = False,
) -> RunSummary:
    """Execute the silver+gold layers via the content-pack runner (PLAN §15 Phase 2).

    Default for v0.3: bronze stays a legacy-python concern. Customers
    opting into ``--execution-backend content-pack`` run a legacy seed
    first to populate bronze, then content-pack against silver+gold.
    Phase 3 migrates bronze; until then this dispatcher does NOT
    invoke the v1 bronze extractor — that keeps the cross-backend
    blast radius contained.

    Args:
        bundle_path: path to ``bundle.yaml``.
        spark: optional pre-existing SparkSession.
        mode: ``"seed"`` or ``"incremental"``.
        datasets / layers: content-pack node-id and layer filters
            (interpreted by :func:`resolve_content_pack_plan`, NOT by
            the legacy registry's :func:`resolve_plan`).
        dry_run: returns an empty RunSummary without dispatching.
        resume_run_id: not supported for content-pack v0.3
            (AIDPF-1032; CLI rejects this earlier; defensive
            re-check below).
        resolved_pack: pre-loaded ``ResolvedPack``. CLI / inline
            passes the laptop-resolved pack; REST notebook passes the
            cluster-side reconstructed pack from
            ``materialize_staged_pack`` + ``load_full_chain``.
        tenant_profile: pre-loaded ``TenantProfile``. Same shape as
            above.

    Returns:
        Standard :class:`RunSummary` with one :class:`RunStep` per
        executed node.

    Raises:
        OrchestratorConfigError: ``--resume`` requested with
            ``execution_backend == 'content-pack'`` (defensive — the
            CLI rejects earlier).
        ValueError: ``resolved_pack`` or ``tenant_profile`` is None.
    """
    # Lazy imports — the content-pack backend's deps don't load on the
    # default v1 path.
    from datetime import datetime as _dt, timezone as _tz
    from uuid import uuid4
    from .content_pack_plan_resolver import resolve_content_pack_plan
    from .sql_runner import execute_node as cp_execute_node
    from .sql_renderer import RunContext as CpRunContext
    from .state_phase2 import ensure_state_columns_v2
    from ..schema.bundle import (
        AIDPF_1032_RESUME_NOT_SUPPORTED,
        load_bundle as _load_bundle_v2,
    )
    from ..schema.tenant_profile import compute_profile_hash

    # Phase 5 Step 9b — AIDPF-1032 resolved. ``--resume`` on the content-
    # pack backend is supported by:
    #   1. Adopting the supplied ``resume_run_id`` as the shared run_id
    #      (the per-node loop's prior-state hydration + plan-hash drift
    #      gate already enforce the resume contract).
    #   2. Falling through to the normal per-node dispatch — nodes whose
    #      latest state row is already ``success`` for this run_id are
    #      idempotent in the §11.9 atomic-commit model; non-success nodes
    #      retry through the same dispatcher path.
    # No bespoke "resume planner" is needed for v0.3 because the content-
    # pack backend's per-node atomicity (each ``execute_node`` is a full
    # preflight → render → drift → execute → quality → state commit) is
    # the resume unit.
    if resolved_pack is None:
        raise ValueError(
            "_run_content_pack_backend: resolved_pack is None. The CLI / "
            "inline path is responsible for loading the pack via "
            "load_full_chain(...) and passing it in. REST dispatch passes "
            "the cluster-side reconstructed pack."
        )
    if tenant_profile is None:
        raise ValueError(
            "_run_content_pack_backend: tenant_profile is None. The CLI / "
            "inline path loads the profile via load_tenant_profile(...); "
            "REST dispatch reconstructs it via load_tenant_profile_from_string."
        )

    bundle, paths = _load_bundle_v2(bundle_path)
    bundle_project = bundle.project

    # Phase 9 review fix: every resolver call from this point on uses
    # the bundle's declared scope as the implicit root set. A no-CLI-
    # filter run executes only bundle-declared roots + D-1 deps,
    # NOT every pack node.
    bundle_scope = _effective_bundle_scope(bundle)

    if dry_run:
        # Phase 5 Step 6 — populate the content-pack dry-run plan so the
        # renderer can show operators which silver/gold nodes would run +
        # how each would be dispatched. Plan resolution is cheap (pure
        # data walk; no Spark / BICC).
        plan_nodes = _build_content_pack_dry_run_plan(
            resolved_pack=resolved_pack,
            datasets=datasets,
            layers=layers,
            strict_scope=strict_scope,
            bundle_scope=bundle_scope,
        )
        return RunSummary.empty(
            bundle_project=bundle_project, mode=mode, plan=plan_nodes,
        )

    spark = spark or _bootstrap_spark()

    # Mint run_id BEFORE the Phase 3c drift gate so the drift artifact
    # + any force-skip audit row + the RunSummary all carry the SAME
    # run_id. Round-2 audit-correlation requirement: lifting this above
    # state.ensure_state_table keeps "gate runs before any state write"
    # AND "one run_id across all Phase 3c artifacts" both true.
    #
    # Phase 5 Step 5 — when the top-level dispatcher minted a shared
    # run_id (so bronze + cp join cleanly on run_id), adopt it instead
    # of minting a `cp-`-prefixed one. The prefix loses meaning once
    # the same run also extracts bronze through the legacy path.
    #
    # Phase 5 Step 9b — also adopt ``resume_run_id`` when supplied so
    # the resumed run writes state rows under the same id as the
    # original failed run (joining cleanly with the prior state).
    # Precedence: explicit shared_run_id > resume_run_id > newly minted.
    if shared_run_id is not None:
        run_id = shared_run_id
    elif resume_run_id is not None:
        run_id = resume_run_id
    else:
        run_id = f"cp-{_dt.now(_tz.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

    # Phase 3c — bronze-schema fingerprint drift gate. Runs BEFORE any
    # Spark write and BEFORE state.ensure_state_table. Returns a
    # `PreflightOutcome`; raises only via the SchemaDriftDetectedError
    # constructor here (the helper itself never raises drift-typed
    # exceptions — that's the CLI-mapping boundary).
    from .preflight_evidence import check_bronze_fingerprint_drift
    from ..schema.errors import SchemaDriftDetectedError

    preflight = check_bronze_fingerprint_drift(
        spark=spark,
        bundle=bundle,
        bundle_path=bundle_path,
        pack=resolved_pack,
        profile=tenant_profile,
        run_id=run_id,
        mode=mode,
        workdir=bundle_path.resolve().parent,
        force_skip=force_fingerprint_skip,
    )
    if preflight.kind == "drift":
        raise SchemaDriftDetectedError(
            run_id=run_id,
            diagnostic_path=preflight.diagnostic_path,  # type: ignore[arg-type]
            summary=preflight.summary,
            prior_fingerprint=preflight.prior_fingerprint,  # type: ignore[arg-type]
            current_fingerprint=preflight.current_fingerprint,  # type: ignore[arg-type]
        )

    # State-table setup + Phase 2 additive migration. ensure_state_table
    # (v1) creates the base table if needed; ensure_state_columns_v2
    # adds the Phase 2 columns + redeploys the latest view with the
    # widened grain.
    state.ensure_state_table(spark, paths)
    ensure_state_columns_v2(spark, paths)

    # Phase 3c — force-skip audit row (after state-table exists; uses
    # the SAME run_id as the rest of the run).
    if preflight.kind == "skip_force_flag":
        state.write_fingerprint_skip_row(
            spark, paths,
            run_id=run_id,
            prior_fingerprint=preflight.prior_fingerprint,  # type: ignore[arg-type]
            current_fingerprint=preflight.current_fingerprint,  # type: ignore[arg-type]
        )

    # Build the run context the renderer needs. ``active_profile_name``
    # is the bundle's contentPack.profile — keyed by the renderer + builtin
    # adapters into pack.pack.profiles for pack-default lookups. Required
    # field (no default); the content-pack backend has already validated
    # that bundle.content_pack and bundle.content_pack.profile exist.
    active_profile_name = bundle.content_pack.profile  # type: ignore[union-attr]
    # Phase 9 review fix: build the source-id → bronze-table map from
    # the resolved pack's bronze nodes, using each node's ``target``
    # (not ``id``). The pack contract permits id != target — the
    # starter pack has gl_journal_lines (id) → gl_journal_headers
    # (target). The pre-fix bundle-based map assumed they're identical
    # and would point silver/gold at catalog.bronze.gl_journal_lines
    # when the bronze extractor actually writes
    # catalog.bronze.gl_journal_headers.
    bronze_table_for_source: dict[str, str] = {
        node_id: paths.bronze(node.target)
        for node_id, node in resolved_pack.bronze.items()
    }
    # Legacy pack.bronze_yaml fallback (pre-Phase-9 packs that haven't
    # migrated to per-file bronze/<id>.yaml).
    legacy_bronze = getattr(resolved_pack, "bronze_yaml", None) or {}
    for ds in legacy_bronze.get("datasets", []) or []:
        if not isinstance(ds, dict):
            continue
        ds_id = ds.get("id")
        if not ds_id or ds_id in bronze_table_for_source:
            continue
        # Legacy YAML carries the bronze table name as "target" or
        # "pvo" depending on pack vintage; fall back to id.
        table_name = ds.get("target") or ds.get("pvo") or ds_id
        bronze_table_for_source[ds_id] = paths.bronze(table_name)
    ctx = CpRunContext(
        catalog=bundle.aidp.catalog,
        bronze_schema=bundle.aidp.bronze_schema,
        silver_schema=bundle.aidp.silver_schema,
        gold_schema=bundle.aidp.gold_schema,
        run_id=run_id,
        active_profile_name=active_profile_name,
        prior_watermark={},
        mode=mode,
        bronze_table_for_source=bronze_table_for_source,
        # Phase 9 — bundle threaded so bronze_extract_adapter can read
        # bundle.fusion.{service_url, username, password,
        # external_storage} + bundle.fusion.schemaOverrides.<id>.
        bundle=bundle,
    )

    profile_hash = compute_profile_hash(tenant_profile)

    plan = resolve_content_pack_plan(
        resolved_pack, datasets=datasets, layers=layers,
        strict_scope=strict_scope,
        bundle_scope=bundle_scope,
    )

    # Phase 5 Step 2c — bronze readiness gate. Verify every in-scope
    # silver/gold node's transitive bronze dependencies exist AND
    # surface every required column BEFORE dispatching any node.
    # When the gate fails, return a RunSummary with the (otherwise
    # empty) plan plus a synthetic gate-failure RunStep so the CLI
    # exits non-zero AND operators see the gap. No silver/gold state
    # rows are written.
    if enable_bronze_readiness_gate and not dry_run:
        from .bronze_readiness import (
            BronzeReadinessGateError,
            AIDPF_2071_BRONZE_READINESS_GATE_FAILED,
            assert_bronze_readiness,
        )
        # On resume, narrow the gate's cp_filter to the reattempt
        # subset of nodes. A succeeded node's bronze dependency that
        # was manually dropped post-success is not the resume's
        # problem; gating over it would block recovery of unrelated
        # silver/gold work. All-succeeded → skip the gate entirely
        # (no node will dispatch this run).
        gate_cp_filter: tuple[list[str] | None, list[str] | None] | None
        if shared_resume_context is not None:
            reattempt_ids = [
                node.id for node in plan
                if node.id not in shared_resume_context.succeeded
            ]
            gate_cp_filter = (reattempt_ids, None) if reattempt_ids else None
        else:
            gate_cp_filter = (datasets, layers)

        if gate_cp_filter is not None:
            try:
                assert_bronze_readiness(
                    spark,
                    resolved_pack=resolved_pack,
                    cp_filter=gate_cp_filter,
                    paths=paths,
                    run_id=run_id,
                    diagnostics_root=(bundle_path.resolve().parent / ".aidp" / "diagnostics"),
                    tenant_profile=tenant_profile,
                )
            except BronzeReadinessGateError as gate_exc:
                gate_step = RunStep.gate_failed(
                    run_id=run_id,
                    mode=mode,
                    layer="silver",
                    gate_dataset_id="__bronze_readiness_gate__",
                    aidpf_code=AIDPF_2071_BRONZE_READINESS_GATE_FAILED,
                    error_message=str(gate_exc),
                )
                gate_now = _dt.now(_tz.utc)
                return RunSummary(
                    run_id=run_id,
                    started_at=gate_now,
                    finished_at=gate_now,
                    bundle_project=bundle_project,
                    mode=mode,  # type: ignore[arg-type]
                    steps=(gate_step,),
                )

    # Per-node execution loop. execute_node writes its own state rows
    # (success + failure paths) and returns a NodeExecutionResult; we
    # translate that into RunStep entries for the RunSummary.
    #
    # Two contracts enforced in this loop (round-13 review findings):
    #
    #   1. Prior state hydration. Before each node's execute_node, we
    #      look up the latest successful primary state row to populate
    #      ctx.prior_watermark[<source_id>] (so {{ watermark_predicate }}
    #      filters the source delta instead of evaluating 1=1 and
    #      scanning the full source) AND prior_plan_hash (so the
    #      AIDPF-4040 drift gate can fire on incremental resume).
    #
    #   2. Cascade-abort on failure. The plan is topologically ordered
    #      (resolve_content_pack_plan sorts silver-then-gold with
    #      explicit silver->silver and silver->gold dependencies
    #      threaded through). When a node returns any non-success
    #      status, downstream nodes that depend on it (directly or
    #      transitively) MUST NOT be dispatched — they'd read stale
    #      pre-existing upstream tables and commit success rows after
    #      the current run's upstream failed. We track failed node IDs
    #      and skip-cascade any dependent.
    started_at = _dt.now(_tz.utc)
    steps: list[RunStep] = []
    failed_node_ids: set[str] = set()
    for node in plan:
        # Phase 5 Step 9b — resume short-circuit. Nodes whose latest
        # terminal state row under this run_id is 'success' (or a
        # carry-forwarded 'resumed_skipped') emit a fresh
        # resumed_skipped step instead of re-dispatching. The
        # ResumeContext is the source of truth — even if the operator
        # manually dropped the node's table between runs, we trust
        # state; the bronze-readiness gate above catches a dropped
        # upstream that a reattempt node actually reads.
        if (
            shared_resume_context is not None
            and node.id in shared_resume_context.succeeded
        ):
            _emit_content_pack_resumed_skip(
                steps=steps,
                spark=spark, paths=paths,
                node=node, run_id=run_id, mode=mode,
                resume_context=shared_resume_context,
                tenant_profile=tenant_profile,
                resolved_pack=resolved_pack,
            )
            continue

        # Cascade-abort check — if any of this node's silver-deps is in
        # failed_node_ids, skip it with a 'cascade' RunStep instead of
        # dispatching to execute_node. Write a best-effort soft state
        # row for the skipped node so the persisted audit trail records
        # the cascade — without this, status/audit readers would still
        # show the node's previous successful run (or no record at all)
        # for the current run_id, violating the v1 audit-completeness
        # invariant.
        cascade_blocking = _find_cascade_blocker(node, failed_node_ids)
        if cascade_blocking:
            _safe_write_content_pack_cascade_skip_row(
                spark=spark,
                paths=paths,
                node=node,
                run_id=run_id,
                mode=mode,
                blocker_id=cascade_blocking,
                tenant_profile=tenant_profile,
                resolved_pack=resolved_pack,
            )
            steps.append(
                RunStep(
                    run_id=run_id,
                    dataset_id=node.id,
                    layer=node.layer,
                    mode=mode,  # type: ignore[arg-type]
                    status="skipped",
                    row_count=None,
                    duration_seconds=0.0,
                    error_message=f"cascade: upstream {cascade_blocking!r} failed",
                    watermark_used=None,
                    last_watermark=None,
                    skip_reason="cascade",
                    plan_hash=None,
                    plan_snapshot=None,
                )
            )
            # The skipped node itself is also part of the failed set so
            # transitive dependents (gold depending on a skipped silver)
            # propagate the skip.
            failed_node_ids.add(node.id)
            continue

        # Prior-state hydration for the drift gate + watermark predicate.
        # ``mode`` is threaded in so the helper can fail closed on
        # incremental reads (round-13/14 review fix) — a state-read
        # failure in incremental mode must NOT silently degrade to
        # seed semantics.
        prior_plan_hash, prior_watermark_for_node = _read_prior_state_for_node(
            spark, paths, node, mode=mode,
        )
        # Build a per-node ctx that carries the prior watermark for the
        # primary source. We rebuild the ctx (instead of mutating
        # ctx.prior_watermark) so it stays a clean immutable dataclass.
        node_ctx = CpRunContext(
            catalog=ctx.catalog,
            bronze_schema=ctx.bronze_schema,
            silver_schema=ctx.silver_schema,
            gold_schema=ctx.gold_schema,
            run_id=ctx.run_id,
            active_profile_name=ctx.active_profile_name,
            prior_watermark=prior_watermark_for_node,
            mode=ctx.mode,
            bronze_table_for_source=ctx.bronze_table_for_source,
            bundle=ctx.bundle,
        )

        node_started = _dt.now(_tz.utc)
        result = cp_execute_node(
            spark,
            node=node,
            pack=resolved_pack,
            profile=tenant_profile,
            ctx=node_ctx,
            paths=paths,
            mode=mode,  # type: ignore[arg-type]
            profile_hash=profile_hash,
            prior_plan_hash=prior_plan_hash,
        )
        node_duration = (_dt.now(_tz.utc) - node_started).total_seconds()
        status: str = "success" if result.status == "success" else "failed"
        if status != "success":
            failed_node_ids.add(node.id)
        steps.append(
            RunStep(
                run_id=run_id,
                dataset_id=node.id,
                layer=node.layer,
                mode=mode,  # type: ignore[arg-type]
                status=status,  # type: ignore[arg-type]
                row_count=result.row_count,
                duration_seconds=node_duration,
                error_message=result.error_message or None,
                watermark_used=None,
                last_watermark=result.output_watermark,
                plan_hash=result.plan_hash or None,
                plan_snapshot=None,
            )
        )

    finished_at = _dt.now(_tz.utc)
    return RunSummary(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        bundle_project=bundle_project,
        mode=mode,
        steps=tuple(steps),
    )


def _safe_write_content_pack_cascade_skip_row(
    *,
    spark: "Any",
    paths: "Any",
    node: "Any",
    run_id: str,
    mode: str,
    blocker_id: str,
    tenant_profile: "Any | None",
    resolved_pack: "Any | None",
) -> None:
    """Best-effort soft state row for a cascade-skipped content-pack node.

    Mirrors sql_runner's _safe_write_failure_row pattern: assemble the
    row dict + call state_phase2.write_state_rows_hard, but wrap the
    write in try/except so a Spark failure here only loses the audit
    trail — never raises. Cursor advancement is preserved (no
    output_watermark on the row); the prior run's last_watermark is
    not touched because we leave the field NULL.

    Carries the upstream blocker id in ``error_message`` so audit
    readers can trace which dep triggered the cascade.
    """
    from datetime import datetime as _dt, timezone as _tz
    from . import state_phase2 as _sp2

    primary_source = _resolve_primary_source_id_for_state_read(node)
    now = _dt.now(_tz.utc)
    pack_id = getattr(getattr(resolved_pack, "pack", None), "id", None)
    pack_version = getattr(getattr(resolved_pack, "pack", None), "version", None)
    tenant = getattr(tenant_profile, "tenant", None)
    fingerprint = getattr(tenant_profile, "bronze_schema_fingerprint", None)

    row = {
        "run_id": run_id,
        "dataset_id": node.id,
        "layer": node.layer,
        "mode": mode,
        "last_watermark": None,
        "last_run_at": now,
        "status": "skipped",
        "row_count": None,
        "error_message": f"cascade: upstream {blocker_id!r} failed",
        "skip_reason": "cascade",
        "duration_seconds": None,
        "plan_hash": None,
        "plan_snapshot": None,
        "pack_id": pack_id,
        "pack_version": pack_version,
        "node_version": None,
        "node_implementation_type": getattr(node.implementation, "type", None),
        "rendered_sql_hash": None,
        "output_schema_hash": None,
        "profile_hash": None,
        "tenant_fingerprint": tenant,
        "fusion_version": None,
        "bronze_schema_fingerprint": fingerprint,
        "source_id": primary_source,
        "source_role": "primary",
        "input_watermark_start": None,
        "input_watermark_end": None,
        "output_watermark": None,
        "consumed_version": None,
        "delta_row_count": None,
    }
    try:
        _sp2.write_state_rows_hard(spark, paths, [row])
    except Exception:  # noqa: BLE001 — diagnostic write is best-effort
        return


def _emit_content_pack_resumed_skip(
    *,
    steps: "list[RunStep]",
    spark: "Any",
    paths: "Any",
    node: "Any",
    run_id: str,
    mode: str,
    resume_context: "Any",
    tenant_profile: "Any | None",
    resolved_pack: "Any | None",
) -> None:
    """Append a ``resumed_skipped`` step + best-effort soft state row.

    Used by ``_run_content_pack_backend``'s per-node loop when a node's
    id is in ``resume_context.succeeded``. The shape mirrors the v1
    resume path (RunStep.resumed_skip + a state row carrying the
    original run's ``plan_hash`` / ``plan_snapshot`` so the resumed
    row's drift-gate metadata is consistent with the prior success
    row).

    Carry-forwarded ``row_count`` / ``last_watermark`` come from
    ``resume_context``'s tuple-keyed dicts so the
    ``fusion_bundle_state_latest`` projection preserves the original
    logical row count and bronze cursor instead of regressing them to
    NULL.

    State write is best-effort (matches the cascade-skip pattern at
    :func:`_safe_write_content_pack_cascade_skip_row`).
    """
    from datetime import datetime as _dt, timezone as _tz
    from . import state_phase2 as _sp2

    key = (node.id, node.layer)
    row_count = resume_context.succeeded_row_counts.get(key)
    last_watermark = resume_context.succeeded_last_watermarks.get(key)
    # CPResumeContext: no run-level plan_hash (CP writes per-node).
    # bronze_plan_snapshot lifted from any v1-shape bronze row; None
    # for pure silver/gold runs. read_content_pack_resumable_state
    # tolerates both fields being NULL on a resumed-skip row.
    plan_hash = None
    plan_snapshot = resume_context.bronze_plan_snapshot

    primary_source = _resolve_primary_source_id_for_state_read(node)
    now = _dt.now(_tz.utc)
    pack_id = getattr(getattr(resolved_pack, "pack", None), "id", None)
    pack_version = getattr(getattr(resolved_pack, "pack", None), "version", None)
    tenant = getattr(tenant_profile, "tenant", None)
    fingerprint = getattr(tenant_profile, "bronze_schema_fingerprint", None)

    steps.append(
        RunStep(
            run_id=run_id,
            dataset_id=node.id,
            layer=node.layer,
            mode=mode,  # type: ignore[arg-type]
            status="resumed_skipped",
            row_count=row_count,
            duration_seconds=0.0,
            error_message=(
                f"resume({run_id!r}): node already succeeded under this "
                f"run_id — carrying forward."
            ),
            watermark_used=None,
            last_watermark=last_watermark,
            skip_reason="resume-skip",
            plan_hash=plan_hash,
            plan_snapshot=plan_snapshot,
        )
    )

    row = {
        "run_id": run_id,
        "dataset_id": node.id,
        "layer": node.layer,
        "mode": mode,
        "last_watermark": last_watermark,
        "last_run_at": now,
        "status": "resumed_skipped",
        "row_count": row_count,
        "error_message": (
            f"resume({run_id!r}): node already succeeded under this "
            f"run_id — carrying forward."
        ),
        "skip_reason": "resume-skip",
        "duration_seconds": None,
        "plan_hash": plan_hash,
        "plan_snapshot": plan_snapshot,
        "pack_id": pack_id,
        "pack_version": pack_version,
        "node_version": None,
        "node_implementation_type": getattr(node.implementation, "type", None),
        "rendered_sql_hash": None,
        "output_schema_hash": None,
        "profile_hash": None,
        "tenant_fingerprint": tenant,
        "fusion_version": None,
        "bronze_schema_fingerprint": fingerprint,
        "source_id": primary_source,
        "source_role": "primary",
        "input_watermark_start": None,
        "input_watermark_end": None,
        "output_watermark": None,
        "consumed_version": None,
        "delta_row_count": None,
    }
    try:
        _sp2.write_state_rows_hard(spark, paths, [row])
    except Exception:  # noqa: BLE001 — diagnostic write is best-effort
        return


def _find_cascade_blocker(node: Any, failed_node_ids: set[str]) -> str | None:
    """Return a failed upstream node id if this node depends on one, else None.

    Walks the node's ``dependsOn.silver`` list (intra-pack dependencies).
    Bronze dependencies are out of scope for cascade — bronze is the
    legacy-python concern in Phase 2 v0.3.
    """
    deps = getattr(node, "depends_on", None)
    if deps is None:
        return None
    silver_deps = getattr(deps, "silver", None) or []
    for dep in silver_deps:
        if dep.id in failed_node_ids:
            return dep.id
    return None


def _read_prior_state_for_node(
    spark: "Any", paths: "Any", node: "Any", *, mode: str,
) -> "tuple[str | None, dict[str, Any]]":
    """Read the latest successful primary state row for a content-pack node.

    Returns ``(prior_plan_hash, prior_watermark_by_source)``.

    Empty result set (no prior successful row exists — the common
    first-run case) yields ``(None, {})`` in both modes:

    * ``prior_plan_hash=None`` makes the AIDPF-4040 drift gate a no-op
      (correct semantics; nothing to drift against).
    * Empty ``prior_watermark`` makes the renderer emit
      ``{{ watermark_predicate }}`` as ``1=1`` (correct semantics for
      seed mode AND first incremental — both legitimately have no prior
      cursor).

    Failure modes differ by mode (round-13/14 review fix — fail closed
    on incremental):

    * ``mode == "seed"`` — Spark-side read failures (table missing on
      first run, transient connection blip) are SWALLOWED and the
      function returns ``(None, {})``. Seed semantics are "full
      rebuild from bronze" — no cursor needed; a benign read failure
      shouldn't fail the run.

    * ``mode == "incremental"`` — Spark-side read failures FAIL the
      run with ``StateReadFailedError``. An incremental run cannot
      proceed without verifying the prior cursor + plan hash, because
      falling through to ``(None, {})`` would silently full-scan the
      source AND skip the AIDPF-4040 drift gate. The reviewer's
      example: metastore/permission/schema error on the latest-view
      read would otherwise let the run commit despite being unable to
      verify state.

    Args:
        spark: live Spark session.
        paths: TablePaths.
        node: validated NodeYaml whose prior state we're reading.
        mode: ``"seed"`` or ``"incremental"`` — drives the
            fail-open / fail-closed decision.

    Returns:
        ``(prior_plan_hash, {source_id: prior_output_watermark})``.

    Raises:
        StateReadFailedError: ``mode == "incremental"`` AND the
            underlying Spark query raised. Carries the original
            exception as ``__cause__``.
    """
    primary_source = _resolve_primary_source_id_for_state_read(node)
    if primary_source is None:
        return None, {}

    try:
        # Read the latest primary-role row for this node from the
        # Phase 2 latest view. The view's grain is (run_id, dataset_id,
        # layer, source_id) so we additionally filter by source_role
        # to disambiguate.
        from . import state as v1_state
        view_path = v1_state._state_latest_view_path(paths)
        df = spark.sql(
            f"SELECT plan_hash, output_watermark, source_id, status "
            f"FROM {view_path} "
            f"WHERE dataset_id = '{node.id}' AND layer = '{node.layer}' "
            f"AND source_role = 'primary' AND status = 'success' "
            f"ORDER BY last_run_at DESC LIMIT 1"
        )
        rows = df.collect()
    except Exception as exc:  # noqa: BLE001 — re-wrap based on mode
        if mode == "incremental":
            # Fail closed — caller cannot verify prior cursor / plan hash.
            # Use the existing StateReadFailedError class (same shape v1
            # preflight uses); operators see a consistent diagnostic
            # regardless of which backend triggered the failure.
            from . import state as v1_state
            raise StateReadFailedError(
                dataset_id=node.id,
                layer=node.layer,
                table_path=v1_state._state_latest_view_path(paths),
                cause=exc,
            ) from exc
        # Seed mode — table-missing on first run is benign; fall through.
        return None, {}

    if not rows:
        return None, {}

    row = rows[0]
    # Spark Row supports both attribute and index access; use index
    # for resilience to fake-Spark tuples used in unit tests.
    try:
        plan_hash = row["plan_hash"]
        output_watermark = row["output_watermark"]
    except (KeyError, TypeError):
        try:
            plan_hash, output_watermark = row[0], row[1]
        except (IndexError, TypeError):
            return None, {}

    prior_watermark = {primary_source: output_watermark} if output_watermark is not None else {}
    return plan_hash, prior_watermark


def _resolve_primary_source_id_for_state_read(node: "Any") -> "str | None":
    """Mirror sql_runner._resolve_primary_source_id (kept private here to
    avoid a cross-module import cycle into the dispatcher)."""
    inc = node.refresh.incremental
    if inc is not None and inc.watermark is not None:
        return inc.watermark.source
    deps = getattr(node, "depends_on", None)
    if deps and deps.bronze:
        return deps.bronze[0].id
    return None


__all__ = [
    "run",
    "resolve_plan",
    "RunStep",
    "RunSummary",
    "ExternalDep",
    # Exception re-exports for `_run_inline`'s catch clause + downstream callers
    "OrchestratorConfigError",
    "BundleLoadError",
    "BundleVersionMismatchError",
    "UnsupportedModeError",
    "MissingDependencyError",
    "PrerequisiteError",
    "CredentialResolutionError",
    "BronzeSchemaProbeError",
    # P1.17 — new config errors
    "IncrementalCursorMissingError",
    "MultipleNaturalKeyError",
    # P1.17c — dropped-target preflight + strict state read
    "IncrementalTargetMissingError",
    "StateReadFailedError",
    # P1.5β.1 runtime errors
    "OrchestratorRuntimeError",
    "WatermarkMonotonicityError",
    "MultipleUpstreamWatermarkError",
    # P1.17e — bronze MERGE payload-diff predicate
    "BRONZE_AUDIT_COLUMNS",
    # P1.17d — schema evolution under MERGE
    "SchemaEvolutionTypeConflictError",
    "SchemaReconcileResult",
]
