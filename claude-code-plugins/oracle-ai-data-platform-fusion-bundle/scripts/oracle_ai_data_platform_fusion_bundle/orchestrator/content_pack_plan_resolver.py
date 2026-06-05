"""Content-pack DAG plan resolver (Phase 2 Step 12d).

The v1 ``resolve_plan`` (``orchestrator/__init__.py:118``) returns
registry ``Spec`` objects from ``BRONZE_EXTRACTS`` / ``SILVER_DIMS`` /
``GOLD_MARTS`` — it has no awareness of content-pack-declared nodes.
Phase 2 adds a parallel resolver that walks a ``ResolvedPack`` and
produces a topologically-sorted list of ``NodeYaml`` objects for the
content-pack backend to execute.

This module is invoked ONLY when ``execution_backend == 'content-pack'``;
the legacy backend continues to use the v1 ``resolve_plan``.

References:

* PLAN §11 (medallion correctness invariants)
* PLAN §11.10 (multi-source primary/lookup)
* Step 12d acceptance: a fixture node not in the legacy registry MUST
  still execute under the content-pack backend.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..schema.medallion_pack import NodeYaml

    from .content_pack import ResolvedPack


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_1034_UNKNOWN_DATASET_FILTER = "AIDPF-1034"
"""Content-pack ``--datasets`` references a node id not in the pack."""


class UnknownDatasetFilterError(Exception):
    """`--datasets <id>` references a node id absent from the content pack."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_content_pack_plan(
    pack: "ResolvedPack",
    *,
    datasets: list[str] | None = None,
    layers: list[str] | None = None,
) -> list["NodeYaml"]:
    """Build a topologically-ordered list of nodes to execute.

    Walks the merged ``ResolvedPack``'s silver + gold sections, builds
    the dependency graph from each node's ``dependsOn.silver`` references
    (silver -> silver / gold -> silver), applies ``--datasets`` and
    ``--layers`` filters, and returns nodes in topological order.

    Args:
        pack: assembled ResolvedPack from :func:`load_full_chain`.
        datasets: optional list of node ids to filter to. Each id must
            be one of the content-pack node ids (e.g. ``dim_thing``);
            unknown ids raise :class:`UnknownDatasetFilterError` with
            AIDPF-1034.
        layers: optional list of layer names to filter to (``silver`` /
            ``gold``). Other values are silently ignored.

    Returns:
        List of ``NodeYaml`` objects in dependency order — every node's
        dependencies precede it in the list.

    Raises:
        UnknownDatasetFilterError: AIDPF-1034 — ``datasets`` references
            an id not in the pack.
    """
    # Build the candidate node set first.
    all_nodes: dict[str, "NodeYaml"] = {}
    for node_id, node in pack.silver.items():
        all_nodes[node_id] = node
    for node_id, node in pack.gold.items():
        all_nodes[node_id] = node

    # Apply layer filter first (cheap structural test).
    if layers is not None:
        layer_set = {l.strip().lower() for l in layers}
        all_nodes = {
            nid: n for nid, n in all_nodes.items() if n.layer in layer_set
        }

    # Apply dataset filter — validate every id is known.
    if datasets is not None:
        unknown = [d for d in datasets if d not in all_nodes]
        if unknown:
            raise UnknownDatasetFilterError(
                f"{AIDPF_1034_UNKNOWN_DATASET_FILTER}: --datasets references "
                f"node id(s) not in content pack: {unknown!r}. Available: "
                f"{sorted(all_nodes.keys())!r}."
            )
        all_nodes = {nid: n for nid, n in all_nodes.items() if nid in datasets}

    # Topological sort by intra-pack silver dependencies. Bronze deps
    # come from outside the pack (the orchestrator handles those via
    # the existing bronze extract path).
    ordered: list["NodeYaml"] = []
    visited: set[str] = set()
    in_progress: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visited:
            return
        if node_id in in_progress:
            raise ValueError(
                f"resolve_content_pack_plan: dependency cycle detected at {node_id!r}."
            )
        in_progress.add(node_id)
        node = all_nodes.get(node_id)
        if node is not None:
            deps = getattr(node, "depends_on", None)
            silver_deps = getattr(deps, "silver", None) if deps else None
            if silver_deps:
                for dep in silver_deps:
                    # Only follow deps within our filtered set; an
                    # unfiltered silver dep is handled by the
                    # orchestrator's per-layer ordering at run time.
                    if dep.id in all_nodes:
                        visit(dep.id)
            ordered.append(node)
        in_progress.remove(node_id)
        visited.add(node_id)

    # Sort layer-then-id for deterministic ordering when there are no
    # explicit silver->silver deps (gold is naturally after silver).
    for layer_priority in ("silver", "gold"):
        for node_id in sorted(all_nodes.keys()):
            if all_nodes[node_id].layer == layer_priority:
                visit(node_id)

    return ordered
