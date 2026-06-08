"""Run-scope split for the default-flipped content-pack backend (Phase 5 Step 2b).

Today's ``--execution-backend content-pack`` path is silver/gold only —
the content-pack runner does NOT extract bronze, expecting it to be
pre-seeded. Phase 5 flips the default backend, so a no-flag
``aidp-fusion-bundle run --mode seed`` must materialise bronze AND
silver+gold end-to-end. This module classifies a CLI invocation's
``(--datasets, --layers)`` filter pair into two sub-filters:

* **bronze_filter**: ``(datasets, layers)`` passed to the legacy bronze
  helper. ``None`` when bronze is out of scope.
* **cp_filter**: ``(datasets, layers)`` passed to the content-pack
  helper. ``None`` when silver/gold are out of scope.

Both helpers receive the SAME shared ``run_id`` (minted upstream by the
top-level dispatcher) so bronze + silver + gold state rows and audit
columns join cleanly on ``run_id`` — preserving the SOX audit invariant
the v1 monolithic path had.

References:

* PLAN §15 Phase 5 Step 2b — Option A "top-level scope split".
* CLAUDE.md "v1 + v2 coexistence" — bronze extraction is still legacy-
  python through Phase 5; bronze migration is deferred to a later
  phase.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .errors import OrchestratorConfigError

if TYPE_CHECKING:  # pragma: no cover
    from ..schema.bundle import Bundle
    from .content_pack import ResolvedPack


# ---------------------------------------------------------------------------
# AIDPF error code
# ---------------------------------------------------------------------------

AIDPF_1035_SCOPE_SPLIT_REJECTED = "AIDPF-1035"
"""Pre-resolver scope split rejected the ``(--datasets, --layers)`` filter.

Raised by :func:`split_run_scope` when:

* ``--datasets`` references an id present in NEITHER the bundle's v1
  bronze spec list NOR the content-pack ``pack.silver`` /
  ``pack.gold`` node ids.
* The effective scope resolves to no bronze AND no silver/gold work.
* ``--datasets`` / ``--layers`` combinations are semantically
  unsatisfiable (e.g. ``--datasets dim_supplier --layers bronze``).
"""


class ScopeSplitError(OrchestratorConfigError):
    """``(--datasets, --layers)`` filter cannot be classified — AIDPF-1035."""


# ---------------------------------------------------------------------------
# RunScope dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunScope:
    """Two-way classification of a run's ``(--datasets, --layers)`` filter.

    Attributes:
        bronze_filter: ``(datasets, layers)`` tuple to pass to the legacy
            bronze helper, or ``None`` if bronze is out of scope.
        cp_filter: ``(datasets, layers)`` tuple to pass to the
            content-pack runner, or ``None`` if silver/gold are out of
            scope.
    """

    bronze_filter: tuple[list[str] | None, list[str] | None] | None
    cp_filter: tuple[list[str] | None, list[str] | None] | None

    @property
    def is_empty(self) -> bool:
        """Both branches absent — operator filtered everything out."""
        return self.bronze_filter is None and self.cp_filter is None


# ---------------------------------------------------------------------------
# split_run_scope — the classifier
# ---------------------------------------------------------------------------


def split_run_scope(
    *,
    bronze_ids: "set[str]",
    silver_ids: "set[str]",
    gold_ids: "set[str]",
    datasets: list[str] | None,
    layers: list[str] | None,
) -> RunScope:
    """Classify a CLI ``(--datasets, --layers)`` filter into bronze + cp scopes.

    Inputs are the THREE id sets the orchestrator can see:

    * ``bronze_ids`` — bundle's v1 bronze spec ids (available without
      loading the content pack).
    * ``silver_ids`` — content-pack ``resolved_pack.silver.keys()``.
    * ``gold_ids`` — content-pack ``resolved_pack.gold.keys()``.

    Either set may be empty: a bundle without a content pack passes
    empty silver/gold sets; a bundle with a pack but no v1 datasets
    passes an empty bronze set. The classifier handles all three.

    The decision matrix (PLAN §15 Phase 5 Step 2b):

    * **No filters** (datasets None + layers None): both ``bronze_filter``
      and ``cp_filter`` non-None (full medallion run).
    * **--layers bronze**: ``bronze_filter=(None, ["bronze"])``,
      ``cp_filter=None``.
    * **--layers silver / gold / silver,gold**: ``bronze_filter=None``,
      ``cp_filter=(None, layers)`` (today's content-pack-only behaviour;
      bronze assumed pre-seeded).
    * **--layers bronze,silver,gold**: BOTH non-None.
    * **--datasets <bronze_id>**: routes to ``bronze_filter``,
      ``cp_filter=None``.
    * **--datasets <silver_id|gold_id>**: routes to ``cp_filter``,
      ``bronze_filter=None``.
    * **--datasets mixed (bronze + silver/gold)**: each id routes to
      the matching filter (one set of bronze ids, one set of cp ids).
    * **--datasets <unknown_id>**: AIDPF-1035.
    * **--datasets <silver_id> --layers bronze** (semantically
      unsatisfiable): AIDPF-1035.
    * **Empty effective scope** (e.g. layers narrowed away everything):
      AIDPF-1035.

    Args:
        bronze_ids: bundle's v1 bronze spec ids.
        silver_ids: pack's silver node ids (empty for pack-less bundles).
        gold_ids: pack's gold node ids (empty for pack-less bundles).
        datasets: ``--datasets`` CSV (None for no filter).
        layers: ``--layers`` CSV (None for no filter).

    Returns:
        A :class:`RunScope`.

    Raises:
        ScopeSplitError: AIDPF-1035 — see decision matrix.
    """
    layer_set = set(layers) if layers else None

    if layer_set is not None:
        unknown_layers = layer_set - {"bronze", "silver", "gold"}
        if unknown_layers:
            raise ScopeSplitError(
                f"{AIDPF_1035_SCOPE_SPLIT_REJECTED}: --layers contains "
                f"unknown values {sorted(unknown_layers)!r}. Valid values: "
                f"{{'bronze', 'silver', 'gold'}}."
            )

    if datasets is not None:
        # Classify each dataset id by EXACT layer (bronze / silver / gold).
        # Per-layer classification (not lumped "cp") is load-bearing: the
        # cp_filter we emit must carry the narrowed CP layer list, else
        # ``resolve_content_pack_plan`` sees ``layers=None`` and runs nodes
        # from layers the operator excluded (e.g. --datasets dim_supplier
        # --layers gold would otherwise run the silver dim_supplier node).
        bronze_in_filter: list[str] = []
        silver_in_filter: list[str] = []
        gold_in_filter: list[str] = []
        unknown: list[str] = []
        for ds_id in datasets:
            in_bronze = ds_id in bronze_ids
            in_silver = ds_id in silver_ids
            in_gold = ds_id in gold_ids
            if in_silver and not in_bronze:
                silver_in_filter.append(ds_id)
            elif in_gold and not in_bronze:
                gold_in_filter.append(ds_id)
            elif in_bronze and not (in_silver or in_gold):
                bronze_in_filter.append(ds_id)
            elif in_bronze and (in_silver or in_gold):
                # Shouldn't happen in practice (registry ids are unique
                # across layers). Prefer the higher layer because that's
                # where the user-visible mart lives.
                if in_silver:
                    silver_in_filter.append(ds_id)
                else:
                    gold_in_filter.append(ds_id)
            else:
                unknown.append(ds_id)

        if unknown:
            raise ScopeSplitError(
                f"{AIDPF_1035_SCOPE_SPLIT_REJECTED}: --datasets references "
                f"unknown id(s) {unknown!r}. None of these are in the "
                f"bundle's bronze spec list ({sorted(bronze_ids)!r}) or "
                f"the content-pack's silver/gold node ids "
                f"({sorted(silver_ids | gold_ids)!r}). Check for typos."
            )

        # Now apply --layers constraints, if present, to the classified ids.
        if layer_set is not None:
            if bronze_in_filter and "bronze" not in layer_set:
                raise ScopeSplitError(
                    f"{AIDPF_1035_SCOPE_SPLIT_REJECTED}: --datasets includes "
                    f"bronze id(s) {bronze_in_filter!r} but --layers="
                    f"{sorted(layer_set)!r} excludes bronze. The filter "
                    f"combination is semantically unsatisfiable."
                )
            if silver_in_filter and "silver" not in layer_set:
                raise ScopeSplitError(
                    f"{AIDPF_1035_SCOPE_SPLIT_REJECTED}: --datasets includes "
                    f"silver id(s) {silver_in_filter!r} but --layers="
                    f"{sorted(layer_set)!r} excludes silver. The filter "
                    f"combination is semantically unsatisfiable."
                )
            if gold_in_filter and "gold" not in layer_set:
                raise ScopeSplitError(
                    f"{AIDPF_1035_SCOPE_SPLIT_REJECTED}: --datasets includes "
                    f"gold id(s) {gold_in_filter!r} but --layers="
                    f"{sorted(layer_set)!r} excludes gold. The filter "
                    f"combination is semantically unsatisfiable."
                )

        cp_in_filter = silver_in_filter + gold_in_filter
        # Narrow CP layers to those actually represented in the dataset
        # filter (in medallion order). Defense-in-depth: the resolver
        # ALSO enforces the layer filter, so a future code path that
        # bypasses the per-layer dataset routing still can't cross
        # layers the operator excluded.
        cp_layers_present: list[str] = []
        if silver_in_filter:
            cp_layers_present.append("silver")
        if gold_in_filter:
            cp_layers_present.append("gold")

        bronze_filter = (
            (bronze_in_filter, None) if bronze_in_filter else None
        )
        cp_filter = (cp_in_filter, cp_layers_present) if cp_in_filter else None
        scope = RunScope(bronze_filter=bronze_filter, cp_filter=cp_filter)
        if scope.is_empty:
            raise ScopeSplitError(
                f"{AIDPF_1035_SCOPE_SPLIT_REJECTED}: --datasets "
                f"{datasets!r} resolves to an empty effective scope — no "
                f"bronze, silver, or gold work would run."
            )
        return scope

    # No --datasets: classify by --layers only.
    bronze_filter: tuple[list[str] | None, list[str] | None] | None = None
    cp_filter: tuple[list[str] | None, list[str] | None] | None = None

    if layer_set is None:
        # No filters at all — full medallion run.
        bronze_filter = (None, ["bronze"])
        cp_filter = (None, ["silver", "gold"])
    else:
        if "bronze" in layer_set:
            bronze_filter = (None, ["bronze"])
        # Preserve medallion order (silver before gold) in the emitted
        # filter list — matches the no-filter default below and makes
        # the dispatcher's downstream comparisons / log lines stable.
        cp_layers = [layer for layer in ("silver", "gold") if layer in layer_set]
        if cp_layers:
            cp_filter = (None, cp_layers)

    scope = RunScope(bronze_filter=bronze_filter, cp_filter=cp_filter)
    if scope.is_empty:
        raise ScopeSplitError(
            f"{AIDPF_1035_SCOPE_SPLIT_REJECTED}: --layers "
            f"{sorted(layer_set) if layer_set else 'None'!r} resolves to "
            f"an empty effective scope."
        )
    return scope


def split_run_scope_from_bundle(
    bundle: "Bundle",
    resolved_pack: "ResolvedPack | None",
    *,
    datasets: list[str] | None,
    layers: list[str] | None,
) -> RunScope:
    """Wrapper that pulls the three id sets from a Bundle + ResolvedPack.

    The Bundle's v1 bronze spec list is always available without
    loading the content pack. ``resolved_pack`` may be ``None`` for
    pack-less bundles — in that case ``silver_ids`` and ``gold_ids``
    are empty.
    """
    bronze_ids = {ds.id for ds in bundle.datasets if ds.enabled}
    silver_ids: set[str] = set()
    gold_ids: set[str] = set()
    if resolved_pack is not None:
        silver_ids = set(resolved_pack.silver.keys())
        gold_ids = set(resolved_pack.gold.keys())
    return split_run_scope(
        bronze_ids=bronze_ids,
        silver_ids=silver_ids,
        gold_ids=gold_ids,
        datasets=datasets,
        layers=layers,
    )


__all__ = [
    "AIDPF_1035_SCOPE_SPLIT_REJECTED",
    "RunScope",
    "ScopeSplitError",
    "split_run_scope",
    "split_run_scope_from_bundle",
]
