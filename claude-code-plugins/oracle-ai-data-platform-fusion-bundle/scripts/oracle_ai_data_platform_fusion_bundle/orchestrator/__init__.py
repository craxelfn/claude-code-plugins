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
from typing import TYPE_CHECKING, Literal

from oracle_ai_data_platform_fusion_bundle import extractors
from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog
from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle

from . import registry, state
from .errors import (
    IncrementalCursorMissingError,
    MissingDependencyError,
    OrchestratorConfigError,
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
    enrich_bronze_audit_cols,
    load_bundle,
)

if TYPE_CHECKING:  # pragma: no cover
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
    """Classify every name from ``bundle.{datasets,dimensions.build,gold.marts}``
    across the six registries and topo-sort the in-plan nodes.

    Args:
        bundle: the parsed bundle.yaml.
        datasets: ``--datasets`` CSV filter, classified by name (across
            BRONZE / SILVER / GOLD namespaces). ``None`` = include all.
        layers: ``--layers`` filter (e.g. ``["gold"]``). ``None`` = include all.
        paths: tenant-aware ``TablePaths`` for external-dep table-path resolution.

    Returns:
        ``(plan, extra_deps)``:
        - ``plan`` — topo-sorted list of Spec instances to dispatch.
        - ``extra_deps`` — tuple of ``ExternalDep`` for in-plan consumers
          whose upstream was filtered out (preflighted on disk before any
          module dispatch by ``_preflight_external_deps``).

    Raises:
        MissingDependencyError: any bundle name unknown to every registry,
            OR an in-plan node depends on a name that doesn't exist anywhere.
    """
    # 1. Resolve every bundle name into a spec (raises MissingDependencyError on typo).
    # P1.5α-fix15: honor DatasetSpec.enabled=false — disabled entries are
    # excluded from all_specs (so downstream consumers see them as "not in
    # the bundle plan"). The id is tracked separately in `disabled_datasets`
    # so the error builders below can emit disabled-specific remediation
    # ("set enabled: true") instead of the misleading generic message
    # ("add it to bundle.datasets" — which is wrong because the entry IS
    # already there, just disabled).
    all_specs: dict[str, Spec] = {}
    disabled_datasets: set[str] = set()
    for ds in bundle.datasets:
        if not ds.enabled:
            disabled_datasets.add(ds.id)
            continue
        all_specs[ds.id] = _resolve_bronze(ds.id)
    for dim_name in bundle.dimensions.build:
        all_specs[dim_name] = _resolve_dim(dim_name)
    for mart_name in bundle.gold.marts:
        all_specs[mart_name] = _resolve_mart(mart_name)

    # 1a. Validate filter inputs BEFORE applying them. Without this guard, a
    #     typoed --datasets / --layers name silently produces an empty plan
    #     and the run exits 0 — an operator would believe a scoped refresh
    #     ran while no table changed. P1.5α-fix12 (post-α blocking bug).
    if datasets is not None:
        unknown_datasets = sorted(set(datasets) - set(all_specs))
        if unknown_datasets:
            # P1.5α-fix15: distinguish disabled-in-bundle from never-declared,
            # so the operator doesn't add a duplicate entry. Two distinct
            # remediations — "set enabled: true" vs "edit bundle.yaml first".
            disabled_in_filter = [
                d for d in unknown_datasets if d in disabled_datasets
            ]
            truly_unknown = [
                d for d in unknown_datasets if d not in disabled_datasets
            ]
            msg_parts: list[str] = []
            if disabled_in_filter:
                msg_parts.append(
                    f"--datasets references disabled name(s): "
                    f"{disabled_in_filter}. "
                    f"Either set `enabled: true` in bundle.datasets for "
                    f"those entries, or remove them from --datasets."
                )
            if truly_unknown:
                msg_parts.append(
                    f"--datasets contains name(s) not in the bundle plan: "
                    f"{truly_unknown}. "
                    f"Available names from bundle.yaml: {sorted(all_specs)}. "
                    f"--datasets is a filter over the bundle's declared "
                    f"datasets / dimensions / marts; to add a new name, edit "
                    f"bundle.yaml first."
                )
            raise MissingDependencyError("\n".join(msg_parts))
    if layers is not None:
        unknown_layers = sorted(set(layers) - _VALID_LAYERS)
        if unknown_layers:
            raise MissingDependencyError(
                f"--layers contains unknown layer(s): {unknown_layers}. "
                f"Valid layers: {sorted(_VALID_LAYERS)}."
            )

    # 2. Determine which names are "in plan" given the (now-validated) filters.
    def _matches_filter(name: str, spec: Spec) -> bool:
        if datasets is not None and name not in datasets:
            return False
        if layers is not None:
            if _layer_for_spec(spec) not in layers:
                return False
        return True

    in_plan_names: set[str] = {
        name for name, spec in all_specs.items() if _matches_filter(name, spec)
    }

    # 3. Identify extra-plan dependencies for in-plan consumers.
    #    Bronze nodes have no dependencies (extractor-only); silver/gold do.
    #    Deferred nodes have no dependencies (no module to run).
    extra_deps_list: list[ExternalDep] = []
    seen_extra: set[tuple[str, str]] = set()  # (dataset_id, layer)

    def _add_extra(dep_name: str, dep_layer: Literal["bronze", "silver", "gold"], consumer: str) -> None:
        key = (dep_name, dep_layer)
        if key in seen_extra:
            return
        # Resolve the table path for the dep.
        if dep_layer == "bronze":
            # bronze table name lives in the catalog
            pvo_id = BRONZE_EXTRACTS[dep_name].pvo_id if dep_name in BRONZE_EXTRACTS else dep_name
            pvo = fusion_catalog.get(pvo_id)
            table_path = paths.bronze(pvo.bronze_table_name)
        elif dep_layer == "silver":
            table_path = paths.silver(dep_name)
        else:
            table_path = paths.gold(dep_name)
        extra_deps_list.append(
            ExternalDep(
                dataset_id=dep_name,
                layer=dep_layer,
                consumer=consumer,
                table_path=table_path,
            )
        )
        seen_extra.add(key)

    def _check_dep_exists_or_raise(dep_name: str, dep_layer: str, consumer: str) -> None:
        """Dep must exist in the corresponding registry OR be deferred — never unknown."""
        if dep_layer == "bronze":
            if dep_name not in BRONZE_EXTRACTS and dep_name not in registry.KNOWN_DEFERRED_DATASETS:
                raise MissingDependencyError(
                    f"Gold/silver consumer {consumer!r} depends on bronze {dep_name!r}, "
                    f"but that name is not in BRONZE_EXTRACTS or KNOWN_DEFERRED_DATASETS. "
                    f"Add the entry to schema/fusion_catalog.py + registry."
                )
        elif dep_layer == "silver":
            if dep_name not in SILVER_DIMS and dep_name not in KNOWN_DEFERRED_DIMS:
                raise MissingDependencyError(
                    f"Gold consumer {consumer!r} depends on silver {dep_name!r}, "
                    f"but that name is not in SILVER_DIMS or KNOWN_DEFERRED_DIMS."
                )

    # P1.5α-fix14: undeclared upstreams must raise, not silently become ExternalDeps.
    # Collect across the whole consumer-loop so one error names every offender —
    # the operator who forgot N upstreams shouldn't have to fix-rerun N times.
    # Distinct from `_check_dep_exists_or_raise` above: that's a registry-consistency
    # check (orchestrator bug if it fires). This check is bundle-declaration —
    # "did the operator opt in via bundle.yaml?" Both must pass.
    #
    # The dep is "declared" iff dep_name appears in all_specs (which is built
    # from bundle.{datasets, dimensions.build, gold.marts}). If the operator
    # declared the upstream and just filtered it out via --datasets/--layers,
    # all_specs still contains it — so case (A) (declared-but-filtered) keeps
    # the ExternalDep path correctly. Case (B) (never declared at all) is the
    # one this check catches.
    # P1.5α-fix15: tuple widened with consumer_layer (4-axis instead of 3-axis)
    # so the error builder below can name the correct bundle.yaml section to
    # remove the consumer from. Derived via _layer_for_spec(all_specs[consumer]) —
    # consumer is always in_plan_names, so always in all_specs.
    undeclared_deps: list[tuple[str, str, str, str]] = []
    # (consumer, consumer_layer, dep_layer, dep_name)

    def _is_declared(dep_name: str) -> bool:
        return dep_name in all_specs

    for name in in_plan_names:
        spec = all_specs[name]
        consumer_layer = _layer_for_spec(spec)
        if isinstance(spec, SilverDimSpec):
            for b in spec.depends_on_bronze:
                _check_dep_exists_or_raise(b, "bronze", name)
                if not _is_declared(b):
                    undeclared_deps.append((name, consumer_layer, "bronze", b))
                    continue
                if b not in in_plan_names:
                    _add_extra(b, "bronze", name)
        elif isinstance(spec, GoldMartSpec):
            for b in spec.depends_on_bronze:
                _check_dep_exists_or_raise(b, "bronze", name)
                if not _is_declared(b):
                    undeclared_deps.append((name, consumer_layer, "bronze", b))
                    continue
                if b not in in_plan_names:
                    _add_extra(b, "bronze", name)
            for s in spec.depends_on_silver:
                _check_dep_exists_or_raise(s, "silver", name)
                if not _is_declared(s):
                    undeclared_deps.append((name, consumer_layer, "silver", s))
                    continue
                if s not in in_plan_names:
                    _add_extra(s, "silver", name)
        # BronzeExtractSpec + DeferredSpec — no upstream dispatch deps

    if undeclared_deps:
        # Consolidated error: one raise listing every undeclared upstream + which
        # bundle.yaml section to act on. Matches the per-layer remediation
        # established by P1.5α-fix12 (--datasets typo guard).
        #
        # P1.5α-fix15: two wording branches — disabled-specific (the upstream
        # IS in bundle.datasets, just disabled — tell the operator to flip the
        # flag) vs generic undeclared (truly missing — tell the operator to
        # add a new entry). The generic message would mislead the operator
        # into adding a duplicate entry; the disabled-specific message points
        # at the actual fix.
        _BUNDLE_SECTION = {
            "bronze": "bundle.datasets",
            "silver": "bundle.dimensions.build",
            "gold":   "bundle.gold.marts",
        }
        lines = [
            f"bundle.yaml is missing {len(undeclared_deps)} upstream "
            f"declaration(s) — refusing to run with undeclared "
            f"dependencies (which would silently rebuild from stale "
            f"on-disk tables or trigger a misleading PrerequisiteError):"
        ]
        for consumer, consumer_layer, dep_layer, dep_name in undeclared_deps:
            if dep_name in disabled_datasets:
                lines.append(
                    f"  • {dep_layer} {dep_name!r} is disabled in bundle.datasets "
                    f"(required by {consumer!r}) — set `enabled: true` "
                    f"or remove {consumer!r} from {_BUNDLE_SECTION[consumer_layer]}"
                )
            else:
                lines.append(
                    f"  • {dep_layer} {dep_name!r} (required by {consumer!r}) — "
                    f"add it to {_BUNDLE_SECTION[dep_layer]}"
                )
        raise MissingDependencyError("\n".join(lines))

    # 4. Topo-sort the in-plan names.
    ts: TopologicalSorter[str] = TopologicalSorter()
    for name in in_plan_names:
        spec = all_specs[name]
        deps_in_plan: set[str] = set()
        if isinstance(spec, SilverDimSpec):
            deps_in_plan.update(d for d in spec.depends_on_bronze if d in in_plan_names)
        elif isinstance(spec, GoldMartSpec):
            deps_in_plan.update(d for d in spec.depends_on_bronze if d in in_plan_names)
            deps_in_plan.update(d for d in spec.depends_on_silver if d in in_plan_names)
        ts.add(name, *deps_in_plan)

    ordered_names = list(ts.static_order())
    plan = [all_specs[name] for name in ordered_names]
    return plan, tuple(extra_deps_list)


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
                    watermark=None,  # _watermark_used column stays NULL (B0 in-memory only)
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
                        if source_delta_count == 0:
                            # P1.17 short-circuit: empty source → no MERGE.
                            # Avoids a wasted MERGE plan AND prevents
                            # _extract_ts being touched on any existing rows.
                            materialized_count = spark.table(target).count()
                        else:
                            df.createOrReplaceTempView("_p117_bronze_src")
                            # B6 + B6d — unconditional UPDATE SET * (payload-diff
                            # predicate DEFERRED to P1.17e). The NULL-safe `<=>`
                            # join predicate handles composite keys with NULL
                            # components (LIMITS.md P1.17-L8 — gl_period_balances).
                            natural_key_join = _natural_key_join_sql(pvo.natural_key)
                            spark.sql(f"""
                                MERGE INTO {target} AS target
                                USING _p117_bronze_src AS src
                                ON {natural_key_join}
                                WHEN MATCHED THEN UPDATE SET *
                                WHEN NOT MATCHED THEN INSERT *
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

    # 1. Load bundle.yaml → (Bundle, TablePaths) via load_bundle (§4.4b).
    bundle, paths = load_bundle(bundle_path)

    # Pre-resume state read for BARE --resume only. When --resume is
    # set without explicit --datasets/--layers, we must read the
    # stored plan_snapshot before resolve_plan to reconstruct the
    # original scope. For --resume WITH explicit filters, the
    # user-supplied filters take precedence and we defer the state
    # read to after the typo-check (so typoed --datasets fails fast
    # with MissingDependencyError, preserving the exit-2 contract).
    resume_context = None
    if resume_run_id is not None and datasets is None and layers is None:
        spark = spark or _bootstrap_spark()
        state.ensure_state_table(spark, paths)
        resume_context = state.read_resumable_state(spark, paths, resume_run_id)
        # Identity-only drift check BEFORE any preflight / BICC call.
        # Drifted fusion.serviceUrl/username here would otherwise send
        # credentials to the wrong endpoint at the bronze preflight step.
        from oracle_ai_data_platform_fusion_bundle import __version__ as _pv
        from .resume import check_identity_drift, reconstruct_resume_scope
        check_identity_drift(
            resume_context.plan_snapshot,
            bundle=bundle, paths=paths, plugin_version=_pv,
            run_id=resume_context.run_id,
        )
        datasets, layers = reconstruct_resume_scope(resume_context.plan_snapshot)

    # 2. Resolve which datasets / dims / marts are in scope + classify
    #    extra-plan deps for the preflight.
    plan, extra_deps = resolve_plan(bundle, datasets, layers, paths=paths)
    if not plan:
        return RunSummary.empty(bundle.project, mode)

    # 3. Dry-run: return the would-run plan + prereqs, no work.
    if dry_run:
        return RunSummary.empty(
            bundle.project, mode,
            plan=tuple(plan), prereqs=extra_deps,
        )

    # 3.5. Credential preflight (B5 + Blocker-5 reorder) — runs BEFORE
    #     _bootstrap_spark so a bad credential never spins up Spark.
    #     Result discarded; we only verify resolvability.
    #
    # Deferred on resume: a drifted bundle whose password reference is
    # broken (missing ${env:...} var or unreachable vault OCID) should
    # still surface ResumeBundleMismatchError — not CredentialResolutionError
    # masking the real issue. On the resume paths we skip the preflight
    # here and run it after the identity drift gate has rendered the
    # right error (if any).
    if resume_run_id is None:
        _resolve_password(bundle.fusion.password)

    # 4. Spark bootstrap (caller-overridable). Idempotent if the bare-
    #    --resume pre-read above already bootstrapped.
    spark = spark or _bootstrap_spark()

    # 5. HARD prerequisite — state table exists + is writeable.
    #    Idempotent if the bare-resume pre-read above already ran it.
    #    Always runs the ALTER TABLE ADD COLUMNS migration
    #    (plan_hash + plan_snapshot) + creates the
    #    fusion_bundle_state_latest VIEW.
    state.ensure_state_table(spark, paths)

    # Deferred state read — for --resume WITH explicit
    # --datasets/--layers (the typo-protected path). We didn't read
    # state in the pre-resume block above because we needed to wait
    # for resolve_plan to catch typos first.
    if resume_run_id is not None and resume_context is None:
        resume_context = state.read_resumable_state(spark, paths, resume_run_id)
        # Identity-only drift check BEFORE preflight unwraps the
        # password + contacts BICC at fusion.serviceUrl.
        from oracle_ai_data_platform_fusion_bundle import __version__ as _pv
        from .resume import check_identity_drift
        check_identity_drift(
            resume_context.plan_snapshot,
            bundle=bundle, paths=paths, plugin_version=_pv,
            run_id=resume_context.run_id,
        )

    # 5.4. Resume credential preflight — deferred from step 3.5 so the
    #      identity drift gate gets first refusal. Now safe to verify
    #      the password resolves before we hand it to preflight.
    if resume_run_id is not None:
        _resolve_password(bundle.fusion.password)

    # 5.5. HARD — every bronze PVO probes cleanly (schema name + PVO existence
    #     + BICC credential at reader layer). Catches the most common class of
    #     "fails 20min into the run" bug in ~1-2s per PVO without writing any
    #     data. See P1.5α-fix17 / orchestrator/preflight.py for the motivation.
    #
    # P1.5α-fix19: preflight now returns PreflightResult carrying both
    # recommendations (operator-facing footer copy) AND effective_schemas
    # (dataset_id → resolved schema). The orchestrator threads
    # effective_schemas into _execute_node so the REAL bronze dispatch
    # uses the same schema preflight validated. Without this threading,
    # overrides + auto-discovery would be cosmetic-only.
    #
    # On resume, narrow the preflight input to bronze nodes NOT in
    # resume_context.succeeded. Re-probing BICC for already-succeeded
    # nodes wastes minutes per node and risks a transient failure on
    # a previously-good node failing the resume.
    from .preflight import preflight_bronze_schemas
    preflight_plan = plan
    if resume_context is not None:
        preflight_plan = [
            n for n in plan
            if n.dataset_id not in resume_context.succeeded
        ]
    preflight_result = preflight_bronze_schemas(
        spark, bundle, preflight_plan,
        resolved_password=_resolve_password(bundle.fusion.password).get_secret_value(),
    )

    # Compute the canonical plan hash + snapshot. Identity combines
    # (fusion.serviceUrl, fusion.externalStorage, fusion.username,
    # aidp.{catalog, bronzeSchema, silverSchema, goldSchema},
    # plugin_version) — see orchestrator/plan_hash.py.
    #
    # On resume, blend `preflight_result.effective_schemas` (for
    # un-succeeded bronze) with `resume_context.succeeded_schemas`
    # (for already-succeeded bronze — pulled from the stored
    # snapshot, not re-probed). Without this, the hash would diverge
    # between original and resume even when nothing materially changed.
    from oracle_ai_data_platform_fusion_bundle import __version__ as _plugin_version
    from .plan_hash import hash_resolved_plan, serialize_plan_snapshot
    blended_schemas: dict[str, str] = dict(preflight_result.effective_schemas)
    if resume_context is not None:
        for ds_id, schema in resume_context.succeeded_schemas.items():
            blended_schemas.setdefault(ds_id, schema)
    plan_hash_value = hash_resolved_plan(
        plan, blended_schemas, mode,
        bundle=bundle, paths=paths, plugin_version=_plugin_version,
    )
    plan_snapshot_value = serialize_plan_snapshot(
        plan, blended_schemas, mode,
        bundle=bundle, paths=paths, plugin_version=_plugin_version,
    )

    # Drift gate — on resume, compare current hash to stored.
    if resume_context is not None and plan_hash_value != resume_context.plan_hash:
        from .errors import ResumeBundleMismatchError
        from .plan_hash import build_current_diagnostics
        from .resume import render_drift_error

        current_identity, current_node_tuples = build_current_diagnostics(
            plan, blended_schemas, mode,
            bundle=bundle, paths=paths, plugin_version=_plugin_version,
        )
        msg = render_drift_error(
            stored_snapshot_json=resume_context.plan_snapshot,
            current_identity=current_identity,
            current_node_tuples=current_node_tuples,
            stored_hash=resume_context.plan_hash,
            current_hash=plan_hash_value,
            run_id=resume_context.run_id,
        )
        raise ResumeBundleMismatchError(msg)

    # 5.7. HARD — extra-plan deps exist on disk. On resume, the
    # reattempt plan's effective extra-deps include succeeded-node
    # tables (they're upstream of un-succeeded silver/gold but not in
    # the reattempt subset). Catches the case where the operator
    # manually dropped a succeeded bronze between runs.
    if resume_context is not None:
        from .resume import compute_reattempt_extra_deps
        effective_extra_deps = compute_reattempt_extra_deps(
            plan, resume_context.succeeded, extra_deps, paths,
        )
    else:
        effective_extra_deps = extra_deps
    _preflight_external_deps(spark, effective_extra_deps)

    # 5.8. P1.17 — incremental cursor preflight. Fails fast at run-level
    # (NOT per-node) before any module dispatch when ``--mode incremental``
    # is asked for but one or more silver/gold nodes lack a prior
    # ``last_watermark`` in fusion_bundle_state. Bronze tolerates a null
    # prior cursor (full extract); silver/gold can't, so we consolidate
    # the missing-cursor list into a single ``IncrementalCursorMissingError``
    # → CLI exit-2 with the full remediation list. Skips ``dim_calendar``
    # + ``incremental_capable=False`` marts (supplier_spend, ap_aging).
    if mode == "incremental":
        from .preflight import _preflight_incremental_cursors
        _preflight_incremental_cursors(spark, plan, paths)

    # 6. Execute plan.
    # On resume, preserve the original run_id so the state-table
    # audit trail (and the medallion `<layer>_run_id` invariant)
    # stays a single continuous record.
    run_id = resume_context.run_id if resume_context is not None else _new_run_id()
    started_at = _utc_now()
    steps: list[RunStep] = []

    for node in plan:
        # Resume short-circuit — succeeded nodes (or carry-forwards
        # from a prior resume) emit a resumed_skip row instead of
        # re-dispatching. The state table is the source of truth —
        # even if a customer manually dropped the node's table between
        # runs, we trust state (the upstream preflight at 5.7 catches
        # dropped tables a downstream reattempt actually reads).
        if resume_context is not None and node.dataset_id in resume_context.succeeded:
            # P1.5β.1: tuple-keyed (dataset_id, layer) lookups —
            # matches the state-table primary-key grain. Without this
            # change, future registry additions that reuse a
            # ``dataset_id`` across layers would silently regress
            # row_count + last_watermark to NULL on the
            # ``fusion_bundle_state_latest`` projection.
            _resume_key = (node.dataset_id, _layer_for_spec(node))
            step = RunStep.resumed_skip(
                node, run_id, mode,
                row_count=resume_context.succeeded_row_counts.get(_resume_key),
                last_watermark=resume_context.succeeded_last_watermarks.get(_resume_key),
                plan_hash=plan_hash_value,
                plan_snapshot=plan_snapshot_value,
            )
        else:
            step = _execute_node(
                node, spark, paths, bundle, run_id, mode,
                effective_schemas=preflight_result.effective_schemas,
                plan_hash=plan_hash_value,
                plan_snapshot=plan_snapshot_value,
            )
        steps.append(step)
        _safe_write_state_row(spark, paths, step)
        if step.status == "failed":
            # Two-phase cascade (Option B audit-completeness):
            # phase 1 = cascade-skip transitive downstream;
            # phase 2 = abort-mark every remaining plan node.
            _skip_dependents(
                plan, node, run_id, mode, steps, spark, paths,
                plan_hash=plan_hash_value, plan_snapshot=plan_snapshot_value,
            )
            _abort_remaining(
                plan, node, run_id, mode, steps, spark, paths,
                plan_hash=plan_hash_value, plan_snapshot=plan_snapshot_value,
            )
            break

    return RunSummary(
        run_id=run_id,
        started_at=started_at,
        finished_at=_utc_now(),
        bundle_project=bundle.project,
        mode=mode,
        steps=tuple(steps),
        # P1.5α-fix19: thread the preflight recommendations into the
        # operator-facing summary so the CLI renders them in the footer.
        recommendations=preflight_result.recommendations,
    )


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
    # P1.5β.1 runtime errors
    "OrchestratorRuntimeError",
    "WatermarkMonotonicityError",
    "MultipleUpstreamWatermarkError",
]
