"""P1.5ε-fix9 — boundary lock + snapshot tests for schema.registry_metadata.

These tests use **independent assertions** (not derivation comparisons) so a
future contributor who pulls non-runnable catalog entries
(``hcm_worker_assignments``, ``ap_aging_periods``) into the runnable
metadata maps fails loudly. After P1.5ε-fix9 Step 2, ``orchestrator.registry``
is derived from this module — so a tautological "metadata == registry"
assertion would catch nothing useful.

Boundary lock: ``import schema.registry_metadata`` MUST NOT pull
``orchestrator/*`` / ``dimensions/*`` / ``transforms/*`` / ``extractors/*``
into ``sys.modules``. Locked via a subprocess so the assertion is
order-independent of other tests in the session.
"""

from __future__ import annotations

import dataclasses
import json
import subprocess
import sys
import textwrap

import pytest

from oracle_ai_data_platform_fusion_bundle.schema.registry_metadata import (
    BRONZE_EXTRACT_METADATA,
    GOLD_MART_METADATA,
    KNOWN_DEFERRED_DATASETS,
    KNOWN_DEFERRED_DIMS,
    KNOWN_DEFERRED_MARTS,
    SILVER_DIM_METADATA,
    BronzeExtractMetadata,
    GoldMartMetadata,
    SilverDimMetadata,
)


def test_metadata_module_imports_without_engine_side_effects() -> None:
    """Hard boundary lock — importing schema.registry_metadata in a fresh
    subprocess MUST NOT pull engine packages into sys.modules. If it does,
    the dispatch package's §4.3 import-boundary is broken and
    tests/unit/dispatch/test_imports.py will go red downstream.
    """
    spec = textwrap.dedent("""
        import json, sys
        import oracle_ai_data_platform_fusion_bundle.schema.registry_metadata  # noqa: F401
        print(json.dumps(sorted(sys.modules.keys())))
    """)
    result = subprocess.run(
        [sys.executable, "-c", spec],
        check=True, capture_output=True, text=True,
    )
    loaded = set(json.loads(result.stdout))
    forbidden_prefixes = (
        "oracle_ai_data_platform_fusion_bundle.orchestrator",
        "oracle_ai_data_platform_fusion_bundle.dimensions",
        "oracle_ai_data_platform_fusion_bundle.transforms",
        "oracle_ai_data_platform_fusion_bundle.extractors",
    )
    leaked = {m for m in loaded if m.startswith(forbidden_prefixes)}
    assert not leaked, (
        f"schema.registry_metadata leaked engine imports into sys.modules: {leaked}"
    )


def test_bronze_metadata_runnable_entries_only() -> None:
    """Independent presence/absence check — catches catalog-projection mistake
    where a contributor copies fusion_catalog.CATALOG entries (which include
    not-yet-runnable PVO kinds) into BRONZE_EXTRACT_METADATA.
    """
    assert "hcm_worker_assignments" not in BRONZE_EXTRACT_METADATA
    assert "hcm_worker_assignments" in KNOWN_DEFERRED_DATASETS

    assert "ap_aging_periods" not in BRONZE_EXTRACT_METADATA
    assert "ap_aging_periods" in KNOWN_DEFERRED_DATASETS


def test_bronze_metadata_expected_key_set() -> None:
    """Snapshot test — adding a new extractor requires deliberately updating
    BOTH the metadata map AND this expected list. Prevents accidental
    drift between metadata and the runnable orchestrator registry.
    """
    expected = [
        "ap_invoices",
        "ap_payments",
        "ar_invoices",
        "ar_receipts",
        "erp_suppliers",
        "gl_coa",
        "gl_journal_lines",
        "gl_period_balances",
        "po_orders",
        "po_receipts",
        "scm_items",
    ]
    assert sorted(BRONZE_EXTRACT_METADATA.keys()) == expected


def test_silver_metadata_expected_key_set() -> None:
    assert sorted(SILVER_DIM_METADATA.keys()) == [
        "dim_account", "dim_calendar", "dim_supplier",
    ]


def test_gold_metadata_expected_key_set() -> None:
    assert sorted(GOLD_MART_METADATA.keys()) == [
        "ap_aging", "gl_balance", "supplier_spend",
    ]


def test_known_deferred_names_expected() -> None:
    """Snapshot tests for the three deferred maps — the reason strings are
    operator-facing so we snapshot the full dicts. A typo in a reason string
    fails this test loudly.
    """
    assert KNOWN_DEFERRED_DATASETS == {
        "hcm_worker_assignments": "BACKLOG P2.11 — saas-batch REST extractor (kind=SAAS_BATCH), not BICC",
        "ap_aging_periods": (
            "BACKLOG P1.10b — bronze for AgingPeriodHeader bucket configs; "
            "gold ap_aging mart computed downstream from ap_invoices + ap_payments + bucket configs"
        ),
    }
    assert KNOWN_DEFERRED_DIMS == {
        "dim_org":  "P1.7 — HCM org dim, blocked on customer HCM pod (P3.8)",
        "dim_item": "P1.6 — inventory item dim, no shipped consumer yet",
    }
    assert KNOWN_DEFERRED_MARTS == {
        "ar_aging":   "P1.10 — accounts-receivable aging gold mart, not yet shipped",
        "po_backlog": "P1.11 — open POs by supplier × due date, not yet shipped",
    }


def test_metadata_dataclasses_are_frozen() -> None:
    """All three metadata dataclasses must be frozen. Defends against a
    future contributor adding mutable ``Callable`` fields that would drag
    engine modules back into the schema-layer import graph.
    """
    bronze = BronzeExtractMetadata(dataset_id="x", pvo_id="x")
    silver = SilverDimMetadata(dataset_id="x", depends_on_bronze=())
    gold = GoldMartMetadata(
        dataset_id="x", depends_on_bronze=(), depends_on_silver=(),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        bronze.dataset_id = "y"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        silver.dataset_id = "y"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        gold.dataset_id = "y"  # type: ignore[misc]
