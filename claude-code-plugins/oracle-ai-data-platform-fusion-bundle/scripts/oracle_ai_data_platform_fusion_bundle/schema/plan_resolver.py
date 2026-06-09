"""Neutral plan resolver for dispatch-side dry-run plan rendering.

Phase 9 (ADR-0022): walks a ``ResolvedPack`` instead of the
registry-metadata maps. Bronze + silver + gold are all sourced from
the resolved content pack; node ``dependsOn`` edges drive prerequisite
discovery and topological sort.

The engine-side ``orchestrator.run`` calls
:func:`resolve_content_pack_plan` for runtime dispatch. The
dispatch-side ``dispatch.dispatch_via_rest`` dry-run path consumes
the DTOs (``PlanNode`` + ``PrereqNode``) this module produces.

Boundary contract: this module MUST NOT import from ``orchestrator/*``,
``dimensions/*``, ``transforms/*``, or ``extractors/*``. The pack is
loaded by the caller (``commands/run.py`` for both the inline and the
REST paths) and passed in — this preserves the §4.3 import boundary
that ``tests/unit/dispatch/test_imports.py`` enforces.
"""

from __future__ import annotations

from graphlib import TopologicalSorter
from typing import TYPE_CHECKING, Final, Literal

from .errors import MissingDependencyError
from .run_summary import PlanNode, PrereqNode

if TYPE_CHECKING:  # pragma: no cover
    from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths

    from .bundle import Bundle
    from .medallion_pack import NodeYaml, ResolvedPack


# Mirrors ``orchestrator.registry._VALID_LAYERS``. Inlined here so the
# schema-layer resolver does not need to import from orchestrator
# (which would defeat the §4.3 import-boundary).
_VALID_LAYERS: Final[frozenset[str]] = frozenset({"bronze", "silver", "gold"})

# Mirrors orchestrator.content_pack_plan_resolver's strict-scope error
# code. Inlined here for the same boundary reason.
AIDPF_1042_STRICT_SCOPE_MISSING_DEPENDENCY: Final[str] = "AIDPF-1042"

# Bundle-section names per layer — operator-facing remediation strings.
_BUNDLE_SECTION: Final[dict[str, str]] = {
    "bronze": "bundle.datasets",
    "silver": "bundle.dimensions.build",
    "gold":   "bundle.gold.marts",
}


def _bronze_ids_from_pack(pack: "ResolvedPack") -> set[str]:
    """Collect bronze node ids from the resolved pack.

    Honors both Phase 9 per-file ``pack.bronze`` and the legacy
    single-file ``pack.bronze_yaml`` (back-compat fallback).
    """
    ids: set[str] = set(pack.bronze.keys())
    bronze_yaml = getattr(pack, "bronze_yaml", None) or {}
    for ds in bronze_yaml.get("datasets", []) or []:
        if isinstance(ds, dict) and "id" in ds:
            ids.add(str(ds["id"]))
    return ids


def _node_layer(pack: "ResolvedPack", name: str) -> str | None:
    """Identify which layer a name belongs to in the resolved pack."""
    if name in pack.bronze:
        return "bronze"
    if name in pack.silver:
        return "silver"
    if name in pack.gold:
        return "gold"
    # Legacy bronze.yaml fallback.
    bronze_yaml = getattr(pack, "bronze_yaml", None) or {}
    for ds in bronze_yaml.get("datasets", []) or []:
        if isinstance(ds, dict) and ds.get("id") == name:
            return "bronze"
    return None


def _node_depends_on(
    pack: "ResolvedPack", layer: str, name: str,
) -> tuple[list[str], list[str]]:
    """Return (bronze_dep_ids, silver_dep_ids) for a pack node.

    Bronze nodes never have dependsOn entries. Silver nodes depend on
    bronze; gold nodes depend on bronze + silver.
    """
    bucket: dict[str, "NodeYaml"] | None = None
    if layer == "silver":
        bucket = pack.silver
    elif layer == "gold":
        bucket = pack.gold
    if not bucket or name not in bucket:
        return [], []
    node = bucket[name]
    deps = getattr(node, "depends_on", None)
    if deps is None:
        return [], []
    bronze_ids = [src.id for src in getattr(deps, "bronze", []) or []]
    silver_ids = [src.id for src in getattr(deps, "silver", []) or []]
    return bronze_ids, silver_ids


def _bronze_target(pack: "ResolvedPack", node_id: str) -> str:
    """Return the bronze node's ``target`` table name (single-segment).

    Honors per-file ``pack.bronze`` first; falls back to the legacy
    ``pack.bronze_yaml`` form (which carries the table name as the
    dataset's ``target`` or ``pvo`` key) — or finally to ``node_id``.
    """
    if node_id in pack.bronze:
        return pack.bronze[node_id].target
    bronze_yaml = getattr(pack, "bronze_yaml", None) or {}
    for ds in bronze_yaml.get("datasets", []) or []:
        if isinstance(ds, dict) and ds.get("id") == node_id:
            return str(ds.get("target") or ds.get("pvo") or node_id)
    return node_id


def resolve_dry_run_plan(
    pack: "ResolvedPack",
    bundle: "Bundle",
    paths: "TablePaths",
    *,
    datasets: list[str] | None,
    layers: list[str] | None,
    strict_scope: bool = False,
) -> tuple[tuple[PlanNode, ...], tuple[PrereqNode, ...]]:
    """Classify, filter, and topo-sort the pack plan for dry-run rendering.

    Phase 9: walks ``pack.bronze ∪ pack.silver ∪ pack.gold`` instead of
    the registry-metadata maps. Honors ``bundle.datasets[]`` /
    ``bundle.dimensions.build`` / ``bundle.gold.marts`` as the
    operator's declared scope; unknown ids in the bundle raise
    ``MissingDependencyError``. ``--datasets`` / ``--layers`` filter
    the resulting plan; in-plan consumers whose upstream is filtered
    out emit ``PrereqNode`` entries.

    Phase 9 D-1 + strict-scope:

    * ``strict_scope=False`` (default — matches the inline resolver's
      D-1 implicit-transitive-include): a consumer whose upstream is
      not in ``bundle.datasets[]`` / ``bundle.dimensions.build`` /
      ``bundle.gold.marts`` auto-includes that upstream in the plan
      (operator declared intent, resolver fills in deps).
    * ``strict_scope=True``: a consumer whose upstream is not declared
      in the bundle raises ``MissingDependencyError``. Matches the
      inline resolver's ``AIDPF-1042`` contract — operators get the
      same opt-out semantics whether they ``--inline`` or REST-
      dispatch.

    Args:
        pack: the resolved content pack.
        bundle: the parsed ``bundle.yaml``.
        paths: tenant-aware ``TablePaths`` — drives 3-part table names
            for extra-plan prereqs.
        datasets: ``--datasets`` CSV filter (``None`` = include all).
        layers: ``--layers`` filter (``None`` = include all).
        strict_scope: when True, undeclared upstreams raise; when
            False (default), D-1 auto-includes them.

    Returns:
        ``(plan, prereqs)``:
        - ``plan`` — topo-sorted tuple of ``PlanNode``.
        - ``prereqs`` — tuple of ``PrereqNode`` for in-plan consumers
          whose upstream was filtered out by the filters.

    Raises:
        MissingDependencyError: any bundle name unknown to the pack,
            any filter typo, any disabled-but-required dataset, or any
            in-plan consumer with an undeclared upstream.
    """
    bronze_ids_in_pack = _bronze_ids_from_pack(pack)

    # 1. Classify every bundle name against the pack. Honor
    #    DatasetSpec.enabled=false (P1.5α-fix15).
    all_classes: dict[
        str,
        tuple[Literal["bronze", "silver", "gold"], Literal["eligible"], str | None],
    ] = {}
    disabled_datasets: set[str] = set()
    for ds in bundle.datasets:
        if not ds.enabled:
            disabled_datasets.add(ds.id)
            continue
        layer = _node_layer(pack, ds.id)
        if layer is None:
            raise MissingDependencyError(
                f"Unknown dataset {ds.id!r} in bundle.datasets. "
                f"Known pack ids: bronze={sorted(bronze_ids_in_pack)!r}, "
                f"silver={sorted(pack.silver)!r}, "
                f"gold={sorted(pack.gold)!r}."
            )
        all_classes[ds.id] = (layer, "eligible", None)
    for dim_name in bundle.dimensions.build:
        if dim_name in pack.silver:
            all_classes[dim_name] = ("silver", "eligible", None)
        else:
            raise MissingDependencyError(
                f"Unknown dim {dim_name!r} in bundle.dimensions.build. "
                f"Known silver ids: {sorted(pack.silver)!r}."
            )
    for mart_name in bundle.gold.marts:
        if mart_name in pack.gold:
            all_classes[mart_name] = ("gold", "eligible", None)
        else:
            raise MissingDependencyError(
                f"Unknown mart {mart_name!r} in bundle.gold.marts. "
                f"Known gold ids: {sorted(pack.gold)!r}."
            )

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
                    f"datasets / dimensions / marts; to add a new name, "
                    f"edit bundle.yaml first."
                )
            raise MissingDependencyError("\n".join(msg_parts))
    if layers is not None:
        unknown_layers = sorted(set(layers) - _VALID_LAYERS)
        if unknown_layers:
            raise MissingDependencyError(
                f"--layers contains unknown layer(s): {unknown_layers}. "
                f"Valid layers: {sorted(_VALID_LAYERS)}."
            )

    # 2. Determine which names are "in plan" given the filters.
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
    #    in-plan / extra-plan / undeclared.
    prereqs_list: list[PrereqNode] = []
    seen_prereqs: set[tuple[str, str]] = set()

    def _add_prereq(
        dep_name: str,
        dep_layer: Literal["bronze", "silver", "gold"],
        consumer: str,
    ) -> None:
        key = (dep_name, dep_layer)
        if key in seen_prereqs:
            return
        if dep_layer == "bronze":
            table_path = paths.bronze(_bronze_target(pack, dep_name))
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

    def _check_dep_exists_or_raise(
        dep_name: str, dep_layer: str, consumer: str,
    ) -> None:
        """Dep must exist in the pack — never unknown."""
        if dep_layer == "bronze":
            if dep_name not in bronze_ids_in_pack:
                raise MissingDependencyError(
                    f"Gold/silver consumer {consumer!r} depends on bronze "
                    f"{dep_name!r}, but that name is not in the pack's bronze "
                    f"layer. Add a content_packs/<pack>/bronze/<id>.yaml or "
                    f"a legacy bronze.yaml entry for it."
                )
        elif dep_layer == "silver":
            if dep_name not in pack.silver:
                raise MissingDependencyError(
                    f"Gold consumer {consumer!r} depends on silver "
                    f"{dep_name!r}, but that name is not in the pack's silver "
                    f"layer."
                )

    undeclared_deps: list[tuple[str, str, str, str]] = []
    # Phase 9 D-1 (strict_scope=False): undeclared upstreams auto-
    # include into the plan rather than raising. Tracked so the
    # topo-sort below sees them as in-plan members.
    auto_included: set[str] = set()

    def _is_declared(dep_name: str) -> bool:
        return dep_name in all_classes

    def _record_undeclared(
        consumer: str, consumer_layer: str, dep_layer: str, dep_name: str,
    ) -> None:
        if strict_scope:
            undeclared_deps.append(
                (consumer, consumer_layer, dep_layer, dep_name)
            )
        else:
            # D-1 auto-include: add to all_classes + in_plan_names
            # so the topo-sort + plan output picks it up.
            all_classes[dep_name] = (
                dep_layer,  # type: ignore[assignment]
                "eligible",
                None,
            )
            in_plan_names.add(dep_name)
            auto_included.add(dep_name)

    for name in list(in_plan_names):
        consumer_layer, _status, _reason = all_classes[name]
        if consumer_layer == "bronze":
            continue
        bronze_deps, silver_deps = _node_depends_on(
            pack, consumer_layer, name,
        )
        for b in bronze_deps:
            _check_dep_exists_or_raise(b, "bronze", name)
            if not _is_declared(b):
                _record_undeclared(name, consumer_layer, "bronze", b)
                continue
            if b not in in_plan_names:
                _add_prereq(b, "bronze", name)
        if consumer_layer == "gold":
            for s in silver_deps:
                _check_dep_exists_or_raise(s, "silver", name)
                if not _is_declared(s):
                    _record_undeclared(name, consumer_layer, "silver", s)
                    continue
                if s not in in_plan_names:
                    _add_prereq(s, "silver", name)

    # D-1 transitive closure: auto-included silver nodes themselves
    # have bronze deps that may also be undeclared. Walk them so a
    # gold-only bundle pulls the full bronze→silver→gold chain.
    if not strict_scope and auto_included:
        pending = list(auto_included)
        while pending:
            current = pending.pop()
            current_layer, _, _ = all_classes[current]
            if current_layer not in ("silver", "gold"):
                continue
            b_deps, s_deps = _node_depends_on(pack, current_layer, current)
            for b in b_deps:
                _check_dep_exists_or_raise(b, "bronze", current)
                if not _is_declared(b):
                    all_classes[b] = ("bronze", "eligible", None)
                    in_plan_names.add(b)
                    auto_included.add(b)
                    pending.append(b)
            if current_layer == "gold":
                for s in s_deps:
                    _check_dep_exists_or_raise(s, "silver", current)
                    if not _is_declared(s):
                        all_classes[s] = ("silver", "eligible", None)
                        in_plan_names.add(s)
                        auto_included.add(s)
                        pending.append(s)

    if undeclared_deps:
        lines = [
            f"{AIDPF_1042_STRICT_SCOPE_MISSING_DEPENDENCY}: "
            f"--strict-scope requires every transitive dep be declared; "
            f"bundle.yaml is missing {len(undeclared_deps)} upstream "
            f"declaration(s):"
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
        consumer_layer, _status, _reason = all_classes[name]
        deps_in_plan: set[str] = set()
        if consumer_layer == "silver":
            bronze_deps, _ = _node_depends_on(pack, "silver", name)
            deps_in_plan.update(d for d in bronze_deps if d in in_plan_names)
        elif consumer_layer == "gold":
            bronze_deps, silver_deps = _node_depends_on(pack, "gold", name)
            deps_in_plan.update(d for d in bronze_deps if d in in_plan_names)
            deps_in_plan.update(d for d in silver_deps if d in in_plan_names)
        ts.add(name, *deps_in_plan)

    ordered_names = list(ts.static_order())
    plan_nodes = tuple(
        PlanNode(
            dataset_id=name,
            layer=all_classes[name][0],
            status="eligible",
            reason=None,
        )
        for name in ordered_names
    )
    return plan_nodes, tuple(prereqs_list)


__all__ = ["resolve_dry_run_plan"]
