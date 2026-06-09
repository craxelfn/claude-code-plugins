"""Spec dataclasses + the four registries that drive the orchestrator's DAG.

Three runnable-spec types (BronzeExtractSpec, SilverDimSpec, GoldMartSpec)
plus a DeferredSpec for names in KNOWN_DEFERRED_* registries (modules/
extractors not yet shipped). Each spec carries the minimum dispatch info
its layer needs.

Three "what's shipped" dicts (``BRONZE_EXTRACTS``, ``SILVER_DIMS``,
``GOLD_MARTS``) keyed by ``dataset_id`` / ``mart_id`` / ``dim_id``.

Three "what's promised but not shipped" dicts (``KNOWN_DEFERRED_DATASETS``,
``KNOWN_DEFERRED_DIMS``, ``KNOWN_DEFERRED_MARTS``) mapping the same kind of
identifier to a BACKLOG ref. The resolvers consult both — known names
return a runnable spec; deferred names return a ``DeferredSpec`` whose
state-row carries ``status='deferred'`` + the BACKLOG ref.

All six registries share a single namespace (no name collisions across
them) — enforced by ``test_no_name_collisions_across_registries`` in §8.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal

from oracle_ai_data_platform_fusion_bundle.dimensions import dim_calendar
from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import (
    CATALOG,
    PvoKind,
)
from oracle_ai_data_platform_fusion_bundle.schema.registry_metadata import (
    BRONZE_EXTRACT_METADATA,
    GOLD_MART_METADATA,
    KNOWN_DEFERRED_DATASETS,
    KNOWN_DEFERRED_DIMS,
    KNOWN_DEFERRED_MARTS,
    SILVER_DIM_METADATA,
)

# Phase 9 — v1 silver/gold modules deleted. Their entries are
# pruned from _SILVER_BUILDERS / _GOLD_BUILDERS below; only
# dim_calendar (genuine Python builtin, ADR-0011) remains.

from .errors import MissingDependencyError, MultipleUpstreamWatermarkError

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame

    from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths


# Closed set of layer values. DeferredSpec.__post_init__ + _layer_for_spec
# in runtime.py both reference this; widening here widens both call sites
# (and the state-schema CHECK constraint).
_VALID_LAYERS: Final[frozenset[str]] = frozenset({"bronze", "silver", "gold"})


# ---------------------------------------------------------------------------
# Spec dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BronzeExtractSpec:
    """Maps a bundle.datasets[].id to a curated PVO. Bronze write target is
    ``paths.bronze(pvo.bronze_table_name)`` — the orchestrator composes the
    3-part name at dispatch via ``TablePaths.bronze(...)``. Per §4.8a, the
    table-name segment lives in the catalog (``PvoEntry.bronze_table_name``);
    catalog + bronze_schema come from ``bundle.yaml.aidp.*``.
    """

    dataset_id: str
    """The customer-facing identifier (matches ``DatasetSpec.id`` in bundle.yaml)."""

    pvo_id: str
    """Looked up via ``schema.fusion_catalog.get(pvo_id)``. May differ from
    ``dataset_id`` if a future bundle adds aliases; today they always match.
    """


@dataclass(frozen=True)
class SilverDimSpec:
    """A silver dimension's dispatch info: builder callable + upstream bronze
    dataset_ids for topo-sort. Modules accept ``run_id: str | None = None``
    keyword-only for the SOX-trail audit column (§3.5a).

    ``natural_key`` (P1.17) is the **silver-side projected** column name
    used by the incremental MERGE ``ON`` predicate (``target.<natural_key>
    = src.<natural_key>``). Empty string for ``dim_calendar`` (exempt;
    parameter-driven, no source watermark).
    """

    dataset_id: str
    builder: Callable[..., "DataFrame"]
    depends_on_bronze: tuple[str, ...]
    natural_key: "str | tuple[str, ...]" = ""


@dataclass(frozen=True)
class GoldMartSpec:
    """A gold mart's dispatch info. Same shape as ``SilverDimSpec`` plus
    ``depends_on_silver`` for the second topo-sort edge.

    ``natural_key`` (P1.17) is the **gold-side projected** column(s) used
    by the incremental MERGE ``ON`` predicate. Composite keys stored as
    a tuple. Unused for marts where ``incremental_capable=False``
    (recorded for completeness + P1.17b consumption).

    ``incremental_capable`` (P1.17 B3b + B2): when ``False`` the mart
    always emits seed-shape ``CREATE OR REPLACE TABLE`` regardless of
    orchestrator mode. Mirrors :attr:`PvoEntry.incremental_capable` for
    bronze — same name, same semantics, different layer.

    V1-exempt marts:
      * ``supplier_spend`` — aggregate over a grain that mixes mutable
        dim + fact attributes (``approval_status``); partial-MERGE would
        leave both old and new aggregate rows. Aggregate-MERGE pattern
        deferred to P1.17b.
      * ``ap_aging`` — bucket assignments are ``CURRENT_DATE()``-anchored,
        so rows age daily independent of bronze deltas. Full-recompute
        keeps buckets accurate.
    """

    dataset_id: str
    builder: Callable[..., "DataFrame"]
    depends_on_bronze: tuple[str, ...]
    depends_on_silver: tuple[str, ...]
    natural_key: "str | tuple[str, ...]" = ""
    incremental_capable: bool = True


@dataclass(frozen=True)
class DeferredSpec:
    """A name that resolved through ``KNOWN_DEFERRED_*`` — module/extractor
    not yet shipped. The bundle.yaml references it legitimately (per schema
    defaults or customer intent); the orchestrator emits a
    ``RunStep(status='deferred')`` instead of crashing.

    ``layer`` is REQUIRED so the state-row write at ``status='deferred'``
    satisfies ``fusion_bundle_state.layer STRING NOT NULL`` (§3.2).
    Validated at construction via ``__post_init__`` — stdlib
    ``@dataclass(frozen=True)`` does NOT enforce ``Literal`` at runtime.
    """

    dataset_id: str
    layer: Literal["bronze", "silver", "gold"]
    reason: str  # BACKLOG ref from KNOWN_DEFERRED_*

    def __post_init__(self) -> None:
        if self.layer not in _VALID_LAYERS:
            raise ValueError(
                f"DeferredSpec.layer={self.layer!r} is not valid; "
                f"must be one of {sorted(_VALID_LAYERS)}."
            )


# ---------------------------------------------------------------------------
# Shipped registries — runnable today
# ---------------------------------------------------------------------------

# Runnable registries — derived from ``schema.registry_metadata`` (the
# neutral half of the data/behavior split) joined with the engine-side
# builder callables. Adding a new shipped module means: (1) extend the
# matching map in ``schema/registry_metadata.py``, (2) wire its builder
# into ``_SILVER_BUILDERS`` / ``_GOLD_BUILDERS`` below, and (3) update
# the snapshot test in ``tests/unit/schema/test_registry_metadata.py``.
#
# Bronze extracts are generic in *shape* (no build() function required), but
# NOT every catalog entry is wireable here. Two constraints apply:
#   (a) The catalog entry's PvoKind must be EXTRACT_PVO (BICC bulk-extract).
#       Entries with kind=PvoKind.SAAS_BATCH (e.g. hcm_worker_assignments)
#       require an unshipped extractor and go in KNOWN_DEFERRED_DATASETS;
#       entries with kind=PvoKind.OTBI are refused entirely.
#   (b) The §8 invariant lint asserts every EXTRACT_PVO catalog entry is in
#       BRONZE_EXTRACTS OR KNOWN_DEFERRED_DATASETS — catches drift at import.

BRONZE_EXTRACTS: dict[str, BronzeExtractSpec] = {
    name: BronzeExtractSpec(dataset_id=md.dataset_id, pvo_id=md.pvo_id)
    for name, md in BRONZE_EXTRACT_METADATA.items()
}


_SILVER_BUILDERS: dict[str, Callable[..., "DataFrame"]] = {
    # Phase 9 — dim_supplier / dim_account migrated to SQL templates
    # under content_packs/<pack-id>/silver/. Only dim_calendar (true
    # Python builtin, ADR-0011) remains.
    "dim_calendar": dim_calendar.build,
}

# Phase 9 — SILVER_DIMS retains only nodes whose builder is still
# present (dim_calendar). Other entries from SILVER_DIM_METADATA are
# skipped so the dict isn't entries-without-builders.
SILVER_DIMS: dict[str, SilverDimSpec] = {
    name: SilverDimSpec(
        dataset_id=md.dataset_id,
        builder=_SILVER_BUILDERS[name],
        depends_on_bronze=md.depends_on_bronze,
        natural_key=md.natural_key,
    )
    for name, md in SILVER_DIM_METADATA.items()
    if name in _SILVER_BUILDERS
}


_GOLD_BUILDERS: dict[str, Callable[..., "DataFrame"]] = {
    # Phase 9 — every v1 gold mart deleted. All gold nodes ship as
    # SQL templates under content_packs/<pack-id>/gold/.
}

GOLD_MARTS: dict[str, GoldMartSpec] = {}


# ---------------------------------------------------------------------------
# Deferred registries — re-exported from schema.registry_metadata so
# orchestrator-side importers see them at the original location.
# ---------------------------------------------------------------------------
#
# KNOWN_DEFERRED_DATASETS / KNOWN_DEFERRED_DIMS / KNOWN_DEFERRED_MARTS are
# imported above and re-exported via __all__ at the bottom of this module.


# ---------------------------------------------------------------------------
# Resolvers — single-namespace classification across {known, deferred}
# ---------------------------------------------------------------------------


def _resolve_bronze(name: str) -> BronzeExtractSpec | DeferredSpec:
    if name in BRONZE_EXTRACTS:
        return BRONZE_EXTRACTS[name]
    if name in KNOWN_DEFERRED_DATASETS:
        return DeferredSpec(name, layer="bronze", reason=KNOWN_DEFERRED_DATASETS[name])
    raise MissingDependencyError(
        f"Unknown dataset {name!r} in datasets[]. "
        f"Known: {sorted(BRONZE_EXTRACTS)}. Deferred: {sorted(KNOWN_DEFERRED_DATASETS)}."
    )


def _resolve_dim(name: str) -> SilverDimSpec | DeferredSpec:
    if name in SILVER_DIMS:
        return SILVER_DIMS[name]
    if name in KNOWN_DEFERRED_DIMS:
        return DeferredSpec(name, layer="silver", reason=KNOWN_DEFERRED_DIMS[name])
    raise MissingDependencyError(
        f"Unknown dim {name!r} in dimensions.build. "
        f"Known: {sorted(SILVER_DIMS)}. Deferred: {sorted(KNOWN_DEFERRED_DIMS)}."
    )


def _resolve_mart(name: str) -> GoldMartSpec | DeferredSpec:
    if name in GOLD_MARTS:
        return GOLD_MARTS[name]
    if name in KNOWN_DEFERRED_MARTS:
        return DeferredSpec(name, layer="gold", reason=KNOWN_DEFERRED_MARTS[name])
    raise MissingDependencyError(
        f"Unknown mart {name!r} in gold.marts. "
        f"Known: {sorted(GOLD_MARTS)}. Deferred: {sorted(KNOWN_DEFERRED_MARTS)}."
    )


# ---------------------------------------------------------------------------
# Layer-from-spec helper (single source of truth for the mapping)
# ---------------------------------------------------------------------------


def _resolve_target_table(spec: object, paths: "TablePaths") -> str:
    """Return the 3-part Delta target table identifier for ``spec``.

    Single source of truth for the spec → ``catalog.schema.table`` mapping
    that several P1.17 sites need: post-build ``MAX(bronze_extract_ts)``
    capture (C5/B4), the bronze MERGE target (C4), and any future
    inspection (β.1 inlines the equivalent expression at two
    ``__init__.py`` sites).

    For :class:`BronzeExtractSpec`, the bronze table name lives on the
    PVO entry (``PvoEntry.bronze_table_name``); for silver/gold the
    table name equals ``spec.dataset_id``.

    :class:`DeferredSpec` raises — deferred specs never materialize a
    Delta target and the caller has no meaningful path to render.
    """
    from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog

    if isinstance(spec, BronzeExtractSpec):
        return paths.bronze(fusion_catalog.get(spec.pvo_id).bronze_table_name)
    if isinstance(spec, SilverDimSpec):
        return paths.silver(spec.dataset_id)
    if isinstance(spec, GoldMartSpec):
        return paths.gold(spec.dataset_id)
    if isinstance(spec, DeferredSpec):
        raise TypeError(
            f"_resolve_target_table: deferred spec {spec.dataset_id!r} has no "
            "materialized target table to resolve."
        )
    raise TypeError(
        f"_resolve_target_table: unknown spec type {type(spec).__name__}"
    )


def _layer_for_spec(spec: object) -> Literal["bronze", "silver", "gold"]:
    """Single source of truth for spec → layer mapping. Used by every
    ``RunStep`` factory (in runtime.py) except ``.deferred`` (which reads
    ``spec.layer`` directly, since DeferredSpec carries it as a field).
    """
    if isinstance(spec, BronzeExtractSpec):
        return "bronze"
    if isinstance(spec, SilverDimSpec):
        return "silver"
    if isinstance(spec, GoldMartSpec):
        return "gold"
    if isinstance(spec, DeferredSpec):
        return spec.layer
    raise TypeError(f"unknown spec type: {type(spec).__name__}")


def _resolve_watermark_source(spec: object) -> "tuple[str, str] | None":
    """Map a spec to the ``(dataset_id, layer)`` whose ``fusion_bundle_state``
    row carries the watermark to read for that spec.

    Required because Phase α writes one state row per
    ``(dataset_id, layer)`` and silver/gold ``dataset_id`` values
    (e.g. ``dim_supplier``, ``gl_balance``) **do not match** their
    upstream bronze ids (e.g. ``erp_suppliers``,
    ``gl_period_balances``). A naive ``(node.dataset_id, "bronze")``
    lookup for ``dim_supplier`` would return ``None`` every run.

    Contract:
      * :class:`BronzeExtractSpec` → ``(spec.dataset_id, "bronze")``
        (reads its own state row).
      * :class:`SilverDimSpec` / :class:`GoldMartSpec` with no bronze
        upstream (today only ``dim_calendar``) → ``None``. Parameter-
        driven specs have no source watermark to track.
      * :class:`SilverDimSpec` / :class:`GoldMartSpec` with exactly one
        bronze upstream → ``(depends_on_bronze[0], "bronze")``.
      * :class:`SilverDimSpec` / :class:`GoldMartSpec` with two or more
        bronze upstreams → raises
        :class:`MultipleUpstreamWatermarkError`. No shipped mart hits
        this today; P1.17 picks the multi-upstream policy (min, max,
        or explicit per-upstream) once it actually consumes the cursor.
      * :class:`DeferredSpec` → ``None``. Deferred specs never
        dispatch, so they neither read nor advance watermarks.

    Pure function over the spec — no Spark, no paths, no side effects.
    Trivially unit-testable.
    """
    if isinstance(spec, BronzeExtractSpec):
        return (spec.dataset_id, "bronze")
    if isinstance(spec, (SilverDimSpec, GoldMartSpec)):
        upstreams = spec.depends_on_bronze
        if len(upstreams) == 0:
            return None
        if len(upstreams) == 1:
            return (upstreams[0], "bronze")
        raise MultipleUpstreamWatermarkError(
            f"spec {spec.dataset_id!r} has {len(upstreams)} bronze upstreams "
            f"({list(upstreams)!r}); multi-upstream watermark policy is not "
            f"shipped yet (P1.17). Either declare a single upstream or wait "
            f"for the P1.17 resolver to pick min/max/per-upstream semantics."
        )
    if isinstance(spec, DeferredSpec):
        return None
    raise TypeError(
        f"_resolve_watermark_source: unknown spec type {type(spec).__name__}"
    )


__all__ = [
    "BronzeExtractSpec",
    "SilverDimSpec",
    "GoldMartSpec",
    "DeferredSpec",
    "BRONZE_EXTRACTS",
    "SILVER_DIMS",
    "GOLD_MARTS",
    "KNOWN_DEFERRED_DATASETS",
    "KNOWN_DEFERRED_DIMS",
    "KNOWN_DEFERRED_MARTS",
    "_resolve_bronze",
    "_resolve_dim",
    "_resolve_mart",
    "_layer_for_spec",
    "_resolve_target_table",
    "_resolve_watermark_source",
    "_VALID_LAYERS",
]
