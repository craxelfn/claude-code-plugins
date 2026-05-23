"""Resume helpers — pure functions used when ``--resume`` is set.

Three pure functions used by ``orchestrator.run`` when ``--resume`` is set:

  * :func:`reconstruct_resume_scope` — derive the ``datasets`` / ``layers``
    filter from the stored ``plan_snapshot`` so bare ``--resume <run_id>``
    (no CLI filters) re-resolves the original scope.
  * :func:`render_drift_error` — produce the operator-facing
    ``ResumeBundleMismatchError`` message: identity diff first, dataset
    diff second, hash echo last.
  * :func:`compute_reattempt_extra_deps` — augment external-dep preflight
    so reattempted downstream nodes catch a manually-dropped upstream
    table BEFORE dispatch (clean exit-2, not a mid-flight crash).

These live in a dedicated module to keep ``orchestrator/__init__.py``
focused on the main dispatch loop. They are pure (no Spark, no I/O) so
unit-testable without fixtures.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Mapping, Sequence
    from typing import Any

    from .runtime import ExternalDep


def reconstruct_resume_scope(
    plan_snapshot: str,
) -> tuple[list[str], list[str]]:
    """Parse ``plan_snapshot`` JSON and return
    ``(datasets, layers)`` reproducing the original run's scope.

    Both lists are deduplicated and sorted for determinism. Callers
    only consult them when the CLI didn't supply explicit
    ``--datasets`` / ``--layers``; an explicit filter wins (and gets
    checked by the hash compare for divergence).

    Raises:
        ResumeRunNotResumableError: if the snapshot is unparseable.
            (``read_resumable_state`` should have already rejected
            this case; defense-in-depth here so a corrupt snapshot
            doesn't crash with an opaque JSON error.)
    """
    from .errors import ResumeRunNotResumableError

    try:
        snapshot = json.loads(plan_snapshot)
    except (ValueError, TypeError) as exc:  # pragma: no cover
        raise ResumeRunNotResumableError(
            f"plan_snapshot is not valid JSON: {exc!r}. Re-run from scratch."
        ) from exc

    nodes = snapshot.get("nodes", [])
    datasets = sorted({n["dataset_id"] for n in nodes})
    layers = sorted({n["layer"] for n in nodes})
    return datasets, layers


def compute_reattempt_extra_deps(
    plan: "Sequence[Any]",
    succeeded: "frozenset[str]",
    original_extra_deps: "tuple[ExternalDep, ...]",
    paths: "Any",  # TablePaths
) -> "tuple[ExternalDep, ...]":
    """Build the external-dep set the preflight should check on a
    resumed run.

    Returns a tuple combining (both filtered to reattempt-only
    consumers):
      * Out-of-scope upstreams from ``original_extra_deps`` whose
        ``consumer`` is in the reattempt plan. A succeeded-only
        consumer doesn't read its upstream on the resume (it's a
        no-op carry-forward), so preflighting its upstream would
        fail a no-op resume on an unrelated dropped table.
      * Succeeded-node tables that any reattempt-plan node (i.e.
        in ``plan`` and NOT in ``succeeded``) reads from. These are
        "implicit external deps" on resume — they were dispatched on
        the original run (so they exist on disk under normal
        circumstances), but the resume isn't re-dispatching them, so
        the preflight must check they still exist before any
        downstream-only reattempt fires.

    All-succeeded resume (``reattempt_ids`` empty) returns ``()`` —
    nothing is going to dispatch, so nothing needs preflighting. A
    missing upstream on a no-op resume is irrelevant to the resume's
    correctness; the operator would learn about it on the next non-
    resume run.

    Catches the failure mode where an operator manually drops a
    succeeded bronze table between runs and then resumes — without
    this augmentation, the un-succeeded silver/gold would dispatch
    and crash mid-flight as a ``failed`` row, instead of failing
    cleanly pre-dispatch as ``PrerequisiteError``.

    Implementation notes:
      * Walks reattempt-plan nodes' ``depends_on_bronze`` /
        ``depends_on_silver`` lists.
      * Skips deps that are themselves in the reattempt plan (they'll
        be re-dispatched; not external).
      * Skips deps that aren't in ``succeeded`` (they're either out
        of scope — already in ``original_extra_deps`` — or deferred
        with no on-disk table).
      * Deduplicates via ``(dataset_id, layer)``.
    """
    from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog

    from .registry import (
        BRONZE_EXTRACTS,
        GoldMartSpec,
        SilverDimSpec,
    )
    from .runtime import ExternalDep

    reattempt_ids = {n.dataset_id for n in plan if n.dataset_id not in succeeded}

    # All-succeeded resume → nothing to preflight. A no-op resume
    # must not fail on a dropped upstream that no reattempt node
    # reads from.
    if not reattempt_ids:
        return ()

    # Filter the original extra-deps to those consumed by a reattempt
    # node. A succeeded-only consumer is a no-op carry-forward and
    # doesn't actually read its upstream on the resume.
    filtered_original = tuple(
        d for d in original_extra_deps if d.consumer in reattempt_ids
    )
    seen: set[tuple[str, str]] = {
        (d.dataset_id, d.layer) for d in filtered_original
    }
    result: list[ExternalDep] = list(filtered_original)

    def _resolve_table_path(dep_name: str, dep_layer: str) -> str:
        if dep_layer == "bronze":
            pvo_id = (
                BRONZE_EXTRACTS[dep_name].pvo_id
                if dep_name in BRONZE_EXTRACTS
                else dep_name
            )
            pvo = fusion_catalog.get(pvo_id)
            return paths.bronze(pvo.bronze_table_name)
        if dep_layer == "silver":
            return paths.silver(dep_name)
        return paths.gold(dep_name)

    def _add(dep_name: str, dep_layer: str, consumer: str) -> None:
        key = (dep_name, dep_layer)
        if key in seen:
            return
        if dep_name in reattempt_ids:
            return
        if dep_name not in succeeded:
            # Dep is neither succeeded nor in reattempt plan — it's
            # not a meaningful "implicit external dep" we need to
            # preflight (it's either out of original scope entirely,
            # in which case it's already in `original_extra_deps`,
            # or it's a deferred node which has no table to check).
            return
        table_path = _resolve_table_path(dep_name, dep_layer)
        result.append(
            ExternalDep(
                dataset_id=dep_name,
                layer=dep_layer,  # type: ignore[arg-type]
                consumer=consumer,
                table_path=table_path,
            )
        )
        seen.add(key)

    for node in plan:
        if node.dataset_id not in reattempt_ids:
            continue
        if isinstance(node, SilverDimSpec):
            for b in node.depends_on_bronze:
                _add(b, "bronze", node.dataset_id)
        elif isinstance(node, GoldMartSpec):
            for b in node.depends_on_bronze:
                _add(b, "bronze", node.dataset_id)
            for s in node.depends_on_silver:
                _add(s, "silver", node.dataset_id)

    return tuple(result)


def render_drift_error(
    stored_snapshot_json: str,
    current_identity: "Mapping[str, str]",
    current_node_tuples: "Sequence[Mapping[str, str]]",
    stored_hash: str,
    current_hash: str,
    run_id: str,
) -> str:
    """Build the operator-facing ``ResumeBundleMismatchError`` message.

    Three sections, in order:
      1. **Identity diff** — one line per changed field, named explicitly
         (``aidp.silverSchema: "silver_v1" → "silver_v2"``).
      2. **Dataset diff** — added / removed dataset_ids, and for nodes
         present on both sides with diverging
         ``(layer, mode, effective_schema)``, name the per-field delta.
      3. **Hash echo** — ``stored_hash`` + ``current_hash`` truncated to
         12 hex chars for readability (collision-resistant enough for
         operator correlation; full hashes are in the state-table row).

    All three render even if a given section finds zero differences —
    catches the (unlikely) case where the canonical-payload code path
    differs from the hash compute path (would manifest as "hash mismatch
    but nothing diffs"). In that case we still print the hashes so the
    operator has something to file a bug with.
    """
    snapshot = json.loads(stored_snapshot_json)
    stored_identity = snapshot.get("identity", {})
    stored_nodes = snapshot.get("nodes", [])

    lines: list[str] = [
        f"--resume: bundle drift detected against run_id={run_id!r}. "
        f"Either re-run from scratch with the current bundle, or revert "
        f"the bundle to match the original run.",
        "",
    ]

    # 1. Identity diff.
    identity_changes: list[str] = []
    all_identity_keys = sorted(set(stored_identity) | set(current_identity))
    for key in all_identity_keys:
        old = stored_identity.get(key)
        new = current_identity.get(key)
        if old != new:
            identity_changes.append(f"  {key}: {old!r} → {new!r}")
    if identity_changes:
        lines.append("Identity changes:")
        lines.extend(identity_changes)
        lines.append("")

    # 2. Dataset diff.
    stored_by_id = {n["dataset_id"]: n for n in stored_nodes}
    current_by_id = {n["dataset_id"]: n for n in current_node_tuples}
    added = sorted(set(current_by_id) - set(stored_by_id))
    removed = sorted(set(stored_by_id) - set(current_by_id))
    common = sorted(set(stored_by_id) & set(current_by_id))

    per_dataset_changes: list[str] = []
    for ds_id in common:
        s = stored_by_id[ds_id]
        c = current_by_id[ds_id]
        deltas: list[str] = []
        for field in ("layer", "mode", "effective_schema"):
            if s.get(field) != c.get(field):
                deltas.append(f"{field}: {s.get(field)!r} → {c.get(field)!r}")
        if deltas:
            per_dataset_changes.append(
                f"  {ds_id}: " + ", ".join(deltas)
            )

    if added or removed or per_dataset_changes:
        lines.append("Dataset changes:")
        if added:
            lines.append(f"  added:   {added}")
        if removed:
            lines.append(f"  removed: {removed}")
        if per_dataset_changes:
            lines.append("  per-dataset deltas:")
            lines.extend(per_dataset_changes)
        lines.append("")

    # 3. Hash echo.
    lines.append(
        f"Hashes: stored={stored_hash[:12]}…  current={current_hash[:12]}…  "
        f"(full hashes in fusion_bundle_state.plan_hash)"
    )

    return "\n".join(lines)


__all__ = [
    "reconstruct_resume_scope",
    "compute_reattempt_extra_deps",
    "render_drift_error",
]
