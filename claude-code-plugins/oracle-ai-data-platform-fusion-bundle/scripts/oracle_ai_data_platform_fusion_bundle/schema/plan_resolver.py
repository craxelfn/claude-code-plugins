"""Neutral plan resolver for dispatch-side dry-run plan rendering (P1.5ε-fix9).

This module is the behavior half of the data/behavior split applied to
``orchestrator/registry.py``. It classifies bundle names into the six
registry namespaces (BRONZE / SILVER / GOLD × runnable / deferred), applies
``--datasets`` / ``--layers`` filters, walks declared upstreams, topo-sorts
the in-plan DAG, and returns layer-aware DTOs the renderer can consume.

The engine-side ``orchestrator/__init__.py:resolve_plan`` wraps this and
reconstructs ``Spec`` instances for per-step dispatch. The dispatch-side
``dispatch/__init__.py:dispatch_via_rest`` dry-run path consumes the DTOs
directly without ever importing the engine package.

Boundary contract: this module MUST NOT import from ``orchestrator/*``,
``dimensions/*``, ``transforms/*``, or ``extractors/*``. It reads metadata
from ``schema.registry_metadata`` and PVO bronze table names from
``schema.fusion_catalog`` — both neutral schema-namespace modules.
"""

from __future__ import annotations

from graphlib import TopologicalSorter
from typing import TYPE_CHECKING, Final, Literal

from .errors import MissingDependencyError
from .registry_metadata import (
    BRONZE_EXTRACT_METADATA,
    GOLD_MART_METADATA,
    KNOWN_DEFERRED_DATASETS,
    KNOWN_DEFERRED_DIMS,
    KNOWN_DEFERRED_MARTS,
    SILVER_DIM_METADATA,
)
from .run_summary import PlanNode, PrereqNode

if TYPE_CHECKING:  # pragma: no cover
    from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths

    from .bundle import Bundle


# Mirrors ``orchestrator.registry._VALID_LAYERS``. Inlined here so the
# schema-layer resolver does not need to import from orchestrator (which
# would defeat the dispatch package's §4.3 import-boundary).
_VALID_LAYERS: Final[frozenset[str]] = frozenset({"bronze", "silver", "gold"})


# Bundle-section names per layer — operator-facing remediation strings.
_BUNDLE_SECTION: Final[dict[str, str]] = {
    "bronze": "bundle.datasets",
    "silver": "bundle.dimensions.build",
    "gold":   "bundle.gold.marts",
}


def _classify_bronze(name: str) -> tuple[Literal["eligible", "deferred"], str | None]:
    """Classify a bronze name across the (runnable, deferred) namespaces.

    Returns ``(status, reason)``. ``reason`` is the BACKLOG ref for
    deferred names, ``None`` for eligible names. Raises
    :class:`MissingDependencyError` for typos.
    """
    if name in BRONZE_EXTRACT_METADATA:
        return "eligible", None
    if name in KNOWN_DEFERRED_DATASETS:
        return "deferred", KNOWN_DEFERRED_DATASETS[name]
    raise MissingDependencyError(
        f"Unknown dataset {name!r} in datasets[]. "
        f"Known: {sorted(BRONZE_EXTRACT_METADATA)}. "
        f"Deferred: {sorted(KNOWN_DEFERRED_DATASETS)}."
    )


def _classify_dim(name: str) -> tuple[Literal["eligible", "deferred"], str | None]:
    if name in SILVER_DIM_METADATA:
        return "eligible", None
    if name in KNOWN_DEFERRED_DIMS:
        return "deferred", KNOWN_DEFERRED_DIMS[name]
    raise MissingDependencyError(
        f"Unknown dim {name!r} in dimensions.build. "
        f"Known: {sorted(SILVER_DIM_METADATA)}. "
        f"Deferred: {sorted(KNOWN_DEFERRED_DIMS)}."
    )


def _classify_mart(name: str) -> tuple[Literal["eligible", "deferred"], str | None]:
    if name in GOLD_MART_METADATA:
        return "eligible", None
    if name in KNOWN_DEFERRED_MARTS:
        return "deferred", KNOWN_DEFERRED_MARTS[name]
    raise MissingDependencyError(
        f"Unknown mart {name!r} in gold.marts. "
        f"Known: {sorted(GOLD_MART_METADATA)}. "
        f"Deferred: {sorted(KNOWN_DEFERRED_MARTS)}."
    )


def resolve_dry_run_plan(
    bundle: "Bundle",
    paths: "TablePaths",
    *,
    datasets: list[str] | None,
    layers: list[str] | None,
) -> tuple[tuple[PlanNode, ...], tuple[PrereqNode, ...]]:
    """Classify, filter, and topo-sort the bundle plan for dry-run rendering.

    Mirrors the behavior of ``orchestrator.__init__.resolve_plan`` exactly
    but returns neutral DTOs (``PlanNode`` + ``PrereqNode``) and consumes
    only schema-layer metadata — no engine imports.

    Args:
        bundle: the parsed ``bundle.yaml``.
        datasets: ``--datasets`` CSV filter (``None`` = include all).
        layers: ``--layers`` filter (``None`` = include all).
        paths: tenant-aware ``TablePaths`` — drives 3-part table names for
            extra-plan prereqs. Read from
            ``bundle.aidp.{catalog, bronzeSchema, silverSchema, goldSchema}``.

    Returns:
        ``(plan, prereqs)``:
        - ``plan`` — topo-sorted tuple of ``PlanNode``.
        - ``prereqs`` — tuple of ``PrereqNode`` for in-plan consumers
          whose upstream was filtered out by ``--datasets`` / ``--layers``.

    Raises:
        MissingDependencyError: any bundle name unknown to every registry,
            any filter typo, any disabled-but-required dataset, or any
            in-plan consumer with an undeclared upstream.
    """
    # 1. Classify every bundle name across the six namespaces.
    #    P1.5α-fix15: honor DatasetSpec.enabled=false.
    all_classes: dict[str, tuple[Literal["bronze", "silver", "gold"], Literal["eligible", "deferred"], str | None]] = {}
    disabled_datasets: set[str] = set()
    for ds in bundle.datasets:
        if not ds.enabled:
            disabled_datasets.add(ds.id)
            continue
        status, reason = _classify_bronze(ds.id)
        all_classes[ds.id] = ("bronze", status, reason)
    for dim_name in bundle.dimensions.build:
        status, reason = _classify_dim(dim_name)
        all_classes[dim_name] = ("silver", status, reason)
    for mart_name in bundle.gold.marts:
        status, reason = _classify_mart(mart_name)
        all_classes[mart_name] = ("gold", status, reason)

    # 1a. Validate filter inputs BEFORE applying them.
    if datasets is not None:
        unknown_datasets = sorted(set(datasets) - set(all_classes))
        if unknown_datasets:
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
                    f"Available names from bundle.yaml: {sorted(all_classes)}. "
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

    # 2. Determine which names are "in plan" given the validated filters.
    def _matches_filter(name: str, layer: str) -> bool:
        if datasets is not None and name not in datasets:
            return False
        if layers is not None and layer not in layers:
            return False
        return True

    in_plan_names: set[str] = {
        name for name, (layer, _status, _reason) in all_classes.items()
        if _matches_filter(name, layer)
    }

    # 3. Walk upstreams of in-plan consumers + classify each into
    #    in-plan / extra-plan / undeclared / unknown.
    prereqs_list: list[PrereqNode] = []
    seen_prereqs: set[tuple[str, str]] = set()

    def _add_prereq(dep_name: str, dep_layer: Literal["bronze", "silver", "gold"], consumer: str) -> None:
        key = (dep_name, dep_layer)
        if key in seen_prereqs:
            return
        if dep_layer == "bronze":
            # PVO bronze table name lives in the catalog (engine-side
            # resolution at orchestrator/__init__.py:227-229).
            from . import fusion_catalog

            pvo_id = (
                BRONZE_EXTRACT_METADATA[dep_name].pvo_id
                if dep_name in BRONZE_EXTRACT_METADATA
                else dep_name
            )
            pvo = fusion_catalog.get(pvo_id)
            table_path = paths.bronze(pvo.bronze_table_name)
        elif dep_layer == "silver":
            table_path = paths.silver(dep_name)
        else:
            table_path = paths.gold(dep_name)
        prereqs_list.append(
            PrereqNode(
                dataset_id=dep_name,
                layer=dep_layer,
                consumer=consumer,
                table_path=table_path,
            )
        )
        seen_prereqs.add(key)

    def _check_dep_exists_or_raise(dep_name: str, dep_layer: str, consumer: str) -> None:
        """Dep must exist in the corresponding registry OR be deferred — never unknown."""
        if dep_layer == "bronze":
            if (
                dep_name not in BRONZE_EXTRACT_METADATA
                and dep_name not in KNOWN_DEFERRED_DATASETS
            ):
                raise MissingDependencyError(
                    f"Gold/silver consumer {consumer!r} depends on bronze {dep_name!r}, "
                    f"but that name is not in BRONZE_EXTRACTS or KNOWN_DEFERRED_DATASETS. "
                    f"Add the entry to schema/fusion_catalog.py + registry."
                )
        elif dep_layer == "silver":
            if (
                dep_name not in SILVER_DIM_METADATA
                and dep_name not in KNOWN_DEFERRED_DIMS
            ):
                raise MissingDependencyError(
                    f"Gold consumer {consumer!r} depends on silver {dep_name!r}, "
                    f"but that name is not in SILVER_DIMS or KNOWN_DEFERRED_DIMS."
                )

    # P1.5α-fix14: undeclared upstreams must raise, not silently become prereqs.
    undeclared_deps: list[tuple[str, str, str, str]] = []
    # (consumer, consumer_layer, dep_layer, dep_name)

    def _is_declared(dep_name: str) -> bool:
        return dep_name in all_classes

    for name in in_plan_names:
        consumer_layer, status, _reason = all_classes[name]
        # Deferred nodes have no module to dispatch and no upstream
        # dependencies to walk.
        if status == "deferred":
            continue
        if consumer_layer == "silver":
            md = SILVER_DIM_METADATA[name]
            for b in md.depends_on_bronze:
                _check_dep_exists_or_raise(b, "bronze", name)
                if not _is_declared(b):
                    undeclared_deps.append((name, consumer_layer, "bronze", b))
                    continue
                if b not in in_plan_names:
                    _add_prereq(b, "bronze", name)
        elif consumer_layer == "gold":
            md_g = GOLD_MART_METADATA[name]
            for b in md_g.depends_on_bronze:
                _check_dep_exists_or_raise(b, "bronze", name)
                if not _is_declared(b):
                    undeclared_deps.append((name, consumer_layer, "bronze", b))
                    continue
                if b not in in_plan_names:
                    _add_prereq(b, "bronze", name)
            for s in md_g.depends_on_silver:
                _check_dep_exists_or_raise(s, "silver", name)
                if not _is_declared(s):
                    undeclared_deps.append((name, consumer_layer, "silver", s))
                    continue
                if s not in in_plan_names:
                    _add_prereq(s, "silver", name)
        # bronze in-plan nodes have no upstream

    if undeclared_deps:
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

    # 4. Topo-sort the in-plan names. Only eligible nodes carry
    #    dependency edges; deferred nodes have no module to dispatch.
    ts: TopologicalSorter[str] = TopologicalSorter()
    for name in in_plan_names:
        consumer_layer, status, _reason = all_classes[name]
        deps_in_plan: set[str] = set()
        if status == "eligible":
            if consumer_layer == "silver":
                deps_in_plan.update(
                    d for d in SILVER_DIM_METADATA[name].depends_on_bronze
                    if d in in_plan_names
                )
            elif consumer_layer == "gold":
                deps_in_plan.update(
                    d for d in GOLD_MART_METADATA[name].depends_on_bronze
                    if d in in_plan_names
                )
                deps_in_plan.update(
                    d for d in GOLD_MART_METADATA[name].depends_on_silver
                    if d in in_plan_names
                )
        ts.add(name, *deps_in_plan)

    ordered_names = list(ts.static_order())
    plan_nodes = tuple(
        PlanNode(
            dataset_id=name,
            layer=all_classes[name][0],
            status="deferred" if all_classes[name][1] == "deferred" else "eligible",
            reason=all_classes[name][2],
        )
        for name in ordered_names
    )
    return plan_nodes, tuple(prereqs_list)


__all__ = ["resolve_dry_run_plan"]
