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

from oracle_ai_data_platform_fusion_bundle.dimensions import (
    dim_account,
    dim_calendar,
    dim_supplier,
)
from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import (
    CATALOG,
    PvoKind,
)
from oracle_ai_data_platform_fusion_bundle.transforms.gold import (
    ap_aging,
    gl_balance,
    supplier_spend,
)

from .errors import MissingDependencyError

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame


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
    """

    dataset_id: str
    builder: Callable[..., "DataFrame"]
    depends_on_bronze: tuple[str, ...]


@dataclass(frozen=True)
class GoldMartSpec:
    """A gold mart's dispatch info. Same shape as ``SilverDimSpec`` plus
    ``depends_on_silver`` for the second topo-sort edge.
    """

    dataset_id: str
    builder: Callable[..., "DataFrame"]
    depends_on_bronze: tuple[str, ...]
    depends_on_silver: tuple[str, ...]


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

# Hardcoded registries — append a new entry when shipping a new module.
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
    "erp_suppliers":      BronzeExtractSpec("erp_suppliers",      "erp_suppliers"),
    "ap_invoices":        BronzeExtractSpec("ap_invoices",        "ap_invoices"),
    "ap_payments":        BronzeExtractSpec("ap_payments",        "ap_payments"),
    "ar_invoices":        BronzeExtractSpec("ar_invoices",        "ar_invoices"),
    "ar_receipts":        BronzeExtractSpec("ar_receipts",        "ar_receipts"),
    "gl_coa":             BronzeExtractSpec("gl_coa",             "gl_coa"),
    "gl_journal_lines":   BronzeExtractSpec("gl_journal_lines",   "gl_journal_lines"),
    "gl_period_balances": BronzeExtractSpec("gl_period_balances", "gl_period_balances"),
    "po_orders":          BronzeExtractSpec("po_orders",          "po_orders"),
    "po_receipts":        BronzeExtractSpec("po_receipts",        "po_receipts"),
    "scm_items":          BronzeExtractSpec("scm_items",          "scm_items"),
}

SILVER_DIMS: dict[str, SilverDimSpec] = {
    "dim_supplier": SilverDimSpec(
        "dim_supplier",
        builder=dim_supplier.build,
        depends_on_bronze=("erp_suppliers",),
    ),
    "dim_account": SilverDimSpec(
        "dim_account",
        builder=dim_account.build,
        depends_on_bronze=("gl_coa",),
    ),
    "dim_calendar": SilverDimSpec(
        "dim_calendar",
        builder=dim_calendar.build,
        depends_on_bronze=(),  # calendar is parameter-driven, not bronze-driven
    ),
}

GOLD_MARTS: dict[str, GoldMartSpec] = {
    "supplier_spend": GoldMartSpec(
        "supplier_spend",
        builder=supplier_spend.build,
        depends_on_bronze=("ap_invoices",),
        depends_on_silver=("dim_supplier",),
    ),
    "gl_balance": GoldMartSpec(
        "gl_balance",
        builder=gl_balance.build,
        depends_on_bronze=("gl_period_balances",),
        depends_on_silver=("dim_account",),
    ),
    "ap_aging": GoldMartSpec(
        "ap_aging",
        builder=ap_aging.build,
        depends_on_bronze=("ap_invoices",),
        depends_on_silver=("dim_supplier",),
    ),
}


# ---------------------------------------------------------------------------
# Deferred registries — names that resolve to DeferredSpec
# ---------------------------------------------------------------------------

KNOWN_DEFERRED_DATASETS: dict[str, str] = {
    # Catalog entries with kind != PvoKind.EXTRACT_PVO (extractor not shipped).
    # The §8 catalog↔registry invariant lint catches drift.
    "hcm_worker_assignments": "BACKLOG P2.11 — saas-batch REST extractor (kind=SAAS_BATCH), not BICC",
    "ap_aging_periods": (
        "BACKLOG P1.10b — bronze for AgingPeriodHeader bucket configs; "
        "gold ap_aging mart computed downstream from ap_invoices + ap_payments + bucket configs"
    ),
}

KNOWN_DEFERRED_DIMS: dict[str, str] = {
    "dim_org":  "P1.7 — HCM org dim, blocked on customer HCM pod (P3.8)",
    "dim_item": "P1.6 — inventory item dim, no shipped consumer yet",
}

KNOWN_DEFERRED_MARTS: dict[str, str] = {
    "ar_aging":   "P1.10 — accounts-receivable aging gold mart, not yet shipped",
    "po_backlog": "P1.11 — open POs by supplier × due date, not yet shipped",
}


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
    "_VALID_LAYERS",
]
