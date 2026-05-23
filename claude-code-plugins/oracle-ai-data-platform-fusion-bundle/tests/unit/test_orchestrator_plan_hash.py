"""Unit tests for P1.5α-fix21 ``orchestrator.plan_hash``.

Covers the resume drift gate's primitives:
  * Hash stability — same plan + identity → same hash.
  * Sort-stability — reordered plan → same hash.
  * Plan-shape sensitivity — effective_schema flip → hash flips.
  * Identity sensitivity — every one of the 8 identity fields flips
    the hash independently. Catches the failure mode where adding a
    new identity field misses an existing surface (e.g. the snapshot
    serializer drops a field while the hash includes it).
  * Snapshot shape — top-level ``{"identity": {...}, "nodes": [...]}``
    with stable JSON serialization.

No Spark, no I/O. Pure tests against pre-built bundle/paths/plan
fixtures.
"""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
from oracle_ai_data_platform_fusion_bundle.orchestrator import registry
from oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash import (
    build_current_diagnostics,
    hash_resolved_plan,
    serialize_plan_snapshot,
)
from oracle_ai_data_platform_fusion_bundle.schema.bundle import (
    AidpRefs,
    Bundle,
    DatasetSpec,
    DimensionsSpec,
    FusionConn,
    GoldSpec,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_bundle(
    *,
    service_url: str = "https://pod-a.example.com",
    external_storage: str = "oci://bucket@ns/path",
    username: str = "alice",
) -> Bundle:
    """Minimal Bundle with overridable Fusion identity fields."""
    return Bundle(
        apiVersion="aidp-fusion-bundle/v1",
        project="test-bundle",
        fusion=FusionConn(
            serviceUrl=service_url,
            username=username,
            password="literal-pw",
            externalStorage=external_storage,
        ),
        aidp=AidpRefs(),
        datasets=[
            DatasetSpec(id="ap_invoices"),
            DatasetSpec(id="erp_suppliers"),
        ],
        dimensions=DimensionsSpec(build=["dim_supplier"]),
        gold=GoldSpec(marts=["supplier_spend"]),
    )


def _make_paths(
    *,
    catalog: str = "fusion_catalog",
    bronze_schema: str = "bronze",
    silver_schema: str = "silver",
    gold_schema: str = "gold",
) -> TablePaths:
    return TablePaths(
        catalog=catalog,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
    )


def _make_plan():
    """Three-node plan: one bronze, one silver, one gold. The bronze
    node gets an effective_schema (BronzeExtractSpec); silver/gold
    normalize to the empty string per the plan_hash contract.
    """
    return [
        registry.BRONZE_EXTRACTS["ap_invoices"],
        registry.SILVER_DIMS["dim_supplier"],
        registry.GOLD_MARTS["supplier_spend"],
    ]


def _hash(
    plan=None,
    *,
    effective_schemas: dict[str, str] | None = None,
    bundle: Bundle | None = None,
    paths: TablePaths | None = None,
    plugin_version: str = "0.1.0a0",
    mode: str = "seed",
) -> str:
    return hash_resolved_plan(
        plan if plan is not None else _make_plan(),
        effective_schemas if effective_schemas is not None else {"ap_invoices": "Financial"},
        mode,
        bundle=bundle if bundle is not None else _make_bundle(),
        paths=paths if paths is not None else _make_paths(),
        plugin_version=plugin_version,
    )


# ---------------------------------------------------------------------------
# Hash stability
# ---------------------------------------------------------------------------


def test_identical_inputs_hash_identical() -> None:
    h1 = _hash()
    h2 = _hash()
    assert h1 == h2


def test_hash_is_64_hex_chars() -> None:
    h = _hash()
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)


def test_plan_order_does_not_affect_hash() -> None:
    plan = _make_plan()
    forward = _hash(plan=plan)
    reversed_plan = list(reversed(plan))
    backward = _hash(plan=reversed_plan)
    assert forward == backward, (
        "Plan-order changes must not flip the hash; "
        "the canonical payload sorts by dataset_id."
    )


# ---------------------------------------------------------------------------
# Plan-shape sensitivity
# ---------------------------------------------------------------------------


def test_effective_schema_change_flips_hash() -> None:
    """Auto-discovered schema flips between runs → hash flips →
    ResumeBundleMismatchError fires."""
    base = _hash(effective_schemas={"ap_invoices": "Financial"})
    flipped = _hash(effective_schemas={"ap_invoices": "FinancialV2"})
    assert base != flipped


def test_mode_change_flips_hash() -> None:
    base = _hash(mode="seed")
    incremental = _hash(mode="incremental")
    assert base != incremental


def test_plan_node_added_flips_hash() -> None:
    smaller = [registry.BRONZE_EXTRACTS["ap_invoices"]]
    larger = [
        registry.BRONZE_EXTRACTS["ap_invoices"],
        registry.BRONZE_EXTRACTS["erp_suppliers"],
    ]
    assert _hash(plan=smaller, effective_schemas={"ap_invoices": "Financial"}) != _hash(
        plan=larger,
        effective_schemas={"ap_invoices": "Financial", "erp_suppliers": "Financial"},
    )


# ---------------------------------------------------------------------------
# Identity sensitivity — one test per field
# ---------------------------------------------------------------------------


def test_identity_drift_service_url() -> None:
    b1 = _make_bundle(service_url="https://pod-a.example.com")
    b2 = _make_bundle(service_url="https://pod-b.example.com")
    assert _hash(bundle=b1) != _hash(bundle=b2)


def test_identity_drift_external_storage() -> None:
    b1 = _make_bundle(external_storage="oci://bucket-a@ns/path")
    b2 = _make_bundle(external_storage="oci://bucket-b@ns/path")
    assert _hash(bundle=b1) != _hash(bundle=b2)


def test_identity_drift_username() -> None:
    """Mixed-authorization guard: same plan, different Fusion
    principal → hash flips."""
    b1 = _make_bundle(username="alice@oracle")
    b2 = _make_bundle(username="bob@oracle")
    assert _hash(bundle=b1) != _hash(bundle=b2)


def test_identity_drift_aidp_catalog() -> None:
    p1 = _make_paths(catalog="fusion_catalog")
    p2 = _make_paths(catalog="fusion_catalog_v2")
    assert _hash(paths=p1) != _hash(paths=p2)


def test_identity_drift_aidp_bronze_schema() -> None:
    p1 = _make_paths(bronze_schema="bronze")
    p2 = _make_paths(bronze_schema="bronze_v2")
    assert _hash(paths=p1) != _hash(paths=p2)


def test_identity_drift_aidp_silver_schema() -> None:
    """High-risk drift case: customer leaves bronze_schema alone so
    state table is still found, but switches silver_schema. Resume
    would skip succeeded bronze and write reattempted silver into a
    different physical schema if this didn't flip the hash."""
    p1 = _make_paths(silver_schema="silver")
    p2 = _make_paths(silver_schema="silver_v2")
    assert _hash(paths=p1) != _hash(paths=p2)


def test_identity_drift_aidp_gold_schema() -> None:
    """Same high-risk-drift logic as silver_schema."""
    p1 = _make_paths(gold_schema="gold")
    p2 = _make_paths(gold_schema="gold_v2")
    assert _hash(paths=p1) != _hash(paths=p2)


def test_identity_drift_plugin_version() -> None:
    """Same plan + identity, bumped plugin version → hash flips.
    Guards against silently mixing two transform-SQL versions under
    one run_id."""
    base = _hash(plugin_version="0.1.0a0")
    bumped = _hash(plugin_version="0.1.1a0")
    assert base != bumped


# ---------------------------------------------------------------------------
# Snapshot shape
# ---------------------------------------------------------------------------


def test_snapshot_is_canonical_json_with_identity_and_nodes() -> None:
    snap_str = serialize_plan_snapshot(
        _make_plan(),
        {"ap_invoices": "Financial"},
        "seed",
        bundle=_make_bundle(),
        paths=_make_paths(),
        plugin_version="0.1.0a0",
    )
    parsed = json.loads(snap_str)
    assert set(parsed.keys()) == {"identity", "nodes"}
    # Identity has all 8 fields with the spelled-out keys.
    assert set(parsed["identity"].keys()) == {
        "fusion.serviceUrl", "fusion.externalStorage", "fusion.username",
        "aidp.catalog", "aidp.bronzeSchema", "aidp.silverSchema",
        "aidp.goldSchema", "plugin_version",
    }
    # Nodes are sorted by dataset_id, one dict per node.
    node_ids = [n["dataset_id"] for n in parsed["nodes"]]
    assert node_ids == sorted(node_ids)
    # Each node has the four canonical fields.
    for node in parsed["nodes"]:
        assert set(node.keys()) == {"dataset_id", "layer", "mode", "effective_schema"}


def test_snapshot_is_byte_stable() -> None:
    """Two snapshots of identical inputs are byte-identical — JSON
    keys sorted, no whitespace variance."""
    args = dict(
        plan=_make_plan(),
        effective_schemas={"ap_invoices": "Financial"},
        mode="seed",
        bundle=_make_bundle(),
        paths=_make_paths(),
        plugin_version="0.1.0a0",
    )
    s1 = serialize_plan_snapshot(**args)
    s2 = serialize_plan_snapshot(**args)
    assert s1 == s2


def test_snapshot_bronze_node_carries_effective_schema() -> None:
    """Bronze nodes' ``effective_schema`` is the post-preflight value,
    not raw schemaOverrides. Silver/gold nodes normalize to ""."""
    snap_str = serialize_plan_snapshot(
        _make_plan(),
        {"ap_invoices": "FinancialV2"},
        "seed",
        bundle=_make_bundle(),
        paths=_make_paths(),
        plugin_version="0.1.0a0",
    )
    parsed = json.loads(snap_str)
    by_id = {n["dataset_id"]: n for n in parsed["nodes"]}
    assert by_id["ap_invoices"]["effective_schema"] == "FinancialV2"
    assert by_id["dim_supplier"]["effective_schema"] == ""
    assert by_id["supplier_spend"]["effective_schema"] == ""


def test_build_current_diagnostics_returns_identity_and_node_tuples() -> None:
    """Diagnostics helper returns the two-part shape the drift
    renderer needs."""
    identity, nodes = build_current_diagnostics(
        _make_plan(),
        {"ap_invoices": "Financial"},
        "seed",
        bundle=_make_bundle(),
        paths=_make_paths(),
        plugin_version="0.1.0a0",
    )
    assert "fusion.serviceUrl" in identity
    assert "fusion.username" in identity
    assert len(nodes) == 3
    assert {n["dataset_id"] for n in nodes} == {
        "ap_invoices", "dim_supplier", "supplier_spend",
    }
