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
    MissingDependencyError,
    OrchestratorConfigError,
    UnsupportedModeError,
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
)
from .runtime import (
    ExternalDep,
    RunStep,
    RunSummary,
    _new_run_id,
    _preflight_external_deps,
    _resolve_password,
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
    BundleLoadError,
    BundleVersionMismatchError,
    CredentialResolutionError,
    PrerequisiteError,
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
# _execute_node — per-step dispatch with try/except + timing wrapping
# ---------------------------------------------------------------------------


def _execute_node(
    node: Spec,
    spark: "SparkSession",
    paths: TablePaths,
    bundle: Bundle,
    run_id: str,
    mode: str,
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
    """
    t0 = perf_counter()
    try:
        if isinstance(node, BronzeExtractSpec):
            pvo = fusion_catalog.get(node.pvo_id)
            target = paths.bronze(pvo.bronze_table_name)
            # Credential resolution at dispatch (preflight already verified
            # resolvability; this call should always succeed).
            resolved = _resolve_password(bundle.fusion.password)
            df = extractors.bicc.extract_pvo(
                spark,
                pvo,
                fusion_service_url=bundle.fusion.service_url,
                username=bundle.fusion.username,
                password=resolved.get_secret_value(),  # SOLE unwrap site
                fusion_external_storage=bundle.fusion.external_storage,
            )
            df = enrich_bronze_audit_cols(
                df,
                source_pvo=pvo.datastore,
                run_id=run_id,
                watermark=None,  # Phase β fills this for incremental
            )
            # overwriteSchema=true matches CLAUDE.md's "CREATE OR REPLACE for
            # seed mode" invariant — lets a re-run converge even when the prior
            # table has stale audit-column metadata (caught by TC26 live probe
            # with run_id=023482f5: Delta merged _watermark_used into itself).
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
            # Count the materialized table, not the BICC plan (would re-extract).
            row_count = spark.table(target).count()
            return RunStep.success(
                node, run_id, mode,
                row_count=row_count,
                duration_seconds=perf_counter() - t0,
            )

        if isinstance(node, (SilverDimSpec, GoldMartSpec)):
            # B3: thread run_id for the silver_run_id/gold_run_id audit column.
            df = node.builder(spark, paths=paths, run_id=run_id)
            return RunStep.success(
                node, run_id, mode,
                row_count=df.count(),
                duration_seconds=perf_counter() - t0,
            )

        if isinstance(node, DeferredSpec):
            return RunStep.deferred(
                node, run_id, mode, error_message=node.reason,
            )

    except Exception as exc:
        # Module-dispatch error. Record as failed; the run loop cascades.
        return RunStep.failed(
            node, run_id, mode,
            exc=exc,
            duration_seconds=perf_counter() - t0,
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
) -> RunSummary:
    """Materialize bronze + silver + gold per the bundle.yaml plan.

    Args:
        bundle_path: path to ``bundle.yaml``.
        spark: optional pre-existing SparkSession (notebook callers pass
            the AIDP-injected one; standalone callers leave None to use
            ``_bootstrap_spark``).
        mode: ``"seed"`` (Phase α — all that's implemented) or
            ``"incremental"`` (Phase β — raises NotImplementedError).
        datasets: ``--datasets`` CSV filter, classified across registries.
        layers: ``--layers`` filter, e.g. ``["gold"]``.
        dry_run: skip execution; return ``RunSummary.empty(..., plan=...)``
            with the would-run plan and extra-plan prereqs populated.

    Returns:
        ``RunSummary`` with one ``RunStep`` per plan node (or empty for
        dry-run / empty-bundle paths).

    Raises:
        UnsupportedModeError: mode not in ``{"seed", "incremental"}``.
        NotImplementedError: mode == ``"incremental"`` (Phase β).
        BundleLoadError: any bundle.yaml load failure.
        CredentialResolutionError: ``bundle.fusion.password`` unresolvable.
        MissingDependencyError: typo in datasets/dims/marts.
        PrerequisiteError: extra-plan dependency missing on disk.
    """
    # 0. Mode validation (§4.4c) — runs BEFORE any I/O.
    if mode not in _VALID_MODES:
        raise UnsupportedModeError(
            f"mode={mode!r} is not supported. Valid modes: "
            f"{sorted(_VALID_MODES)}. "
            f"(The retired alias 'full' is now called 'seed'.)"
        )
    if mode == "incremental":
        raise NotImplementedError(
            "Incremental mode is P1.5β follow-up; current modules emit "
            "CREATE OR REPLACE only. Use mode='seed' for now."
        )

    # 1. Load bundle.yaml → (Bundle, TablePaths) via load_bundle (§4.4b).
    bundle, paths = load_bundle(bundle_path)

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
    _resolve_password(bundle.fusion.password)

    # 4. Spark bootstrap (caller-overridable).
    spark = spark or _bootstrap_spark()

    # 5. HARD prerequisite — state table exists + is writeable.
    state.ensure_state_table(spark, paths)

    # 5.5. HARD — extra-plan deps exist on disk.
    _preflight_external_deps(spark, extra_deps)

    # 6. Execute plan.
    run_id = _new_run_id()
    started_at = _utc_now()
    steps: list[RunStep] = []

    for node in plan:
        step = _execute_node(node, spark, paths, bundle, run_id, mode)
        steps.append(step)
        _safe_write_state_row(spark, paths, step)
        if step.status == "failed":
            # Two-phase cascade (Option B audit-completeness):
            # phase 1 = cascade-skip transitive downstream;
            # phase 2 = abort-mark every remaining plan node.
            _skip_dependents(plan, node, run_id, mode, steps, spark, paths)
            _abort_remaining(plan, node, run_id, mode, steps, spark, paths)
            break

    return RunSummary(
        run_id=run_id,
        started_at=started_at,
        finished_at=_utc_now(),
        bundle_project=bundle.project,
        mode=mode,
        steps=tuple(steps),
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
]
