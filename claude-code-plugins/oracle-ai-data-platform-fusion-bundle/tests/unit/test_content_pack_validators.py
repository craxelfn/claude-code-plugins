"""Unit tests for static content validators (Step 6).

One test per error code surfaced by the validators in
orchestrator/content_pack_validators.py.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack_validators import (
    AIDPF_2003_SQL_FILE_MISSING,
    AIDPF_2040_DAG_CYCLE,
    AIDPF_2041_UNRESOLVED_DEPENDENCY,
    AIDPF_5002_UNKNOWN_TEMPLATE_VAR,
    AIDPF_5003_UNDECLARED_VARIATION_POINT,
    AIDPF_7001_DASHBOARD_MISSING_NODE,
    AIDPF_7003_DASHBOARD_TYPE_MISMATCH,
    validate_dag,
    validate_dashboard_requires,
    validate_pack_full,
    validate_sql_paths,
    validate_template_variables,
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False))


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _make_base_pack(root: Path, *, with_sql: bool = False) -> Path:
    pack_root = root / "fusion-finance-starter"
    pack_root.mkdir(parents=True, exist_ok=True)
    _write_yaml(
        pack_root / "pack.yaml",
        {
            "id": "fusion-finance-starter",
            "version": "0.1.0",
            "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
            "columnAliases": {
                "supplier_natural_key": {
                    "appliesTo": "bronze.erp_suppliers",
                    "required": True,
                    "candidates": ["SEGMENT1"],
                }
            },
        },
    )
    _write_yaml(
        pack_root / "bronze.yaml",
        {
            "datasets": [
                {"id": "erp_suppliers", "extractor": "bicc", "pvo": "SupplierExtractPVO", "target": "erp_suppliers"},
            ]
        },
    )

    impl: dict
    if with_sql:
        impl = {"type": "sql", "sql": "silver/dim_supplier.sql"}
        _write_file(
            pack_root / "silver" / "dim_supplier.sql",
            "SELECT {{ column.supplier_natural_key }} FROM {{ catalog }}.{{ bronze_schema }}.erp_suppliers",
        )
    else:
        impl = {
            "type": "python_legacy",
            "callable": "pkg.dim_supplier:build",
            "deprecated": False,
            "migrationTarget": "silver/dim_supplier.sql",
        }

    _write_yaml(
        pack_root / "silver" / "dim_supplier.yaml",
        {
            "id": "dim_supplier",
            "layer": "silver",
            "implementation": impl,
            "target": "dim_supplier",
            "dependsOn": {
                "bronze": [{"id": "erp_suppliers", "watermark": {"column": "_extract_ts"}}]
            },
            "refresh": {
                "seed": {"strategy": "replace"},
                "incremental": {
                    "strategy": "merge",
                    "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
                    "naturalKey": ["supplier_number"],
                },
            },
            "outputSchema": {
                "columns": [
                    {"name": "supplier_key", "type": "bigint", "nullable": False, "pii": "none"},
                ]
            },
        },
    )
    return pack_root


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_sql_file_missing_for_sql_node(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path, with_sql=True)
    # Remove the SQL file the node references.
    (pack_root / "silver" / "dim_supplier.sql").unlink()
    pack = load_pack(pack_root)
    errors = validate_sql_paths(pack)
    assert any(e.code == AIDPF_2003_SQL_FILE_MISSING for e in errors)


def test_sql_path_validator_skips_python_legacy(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path, with_sql=False)
    pack = load_pack(pack_root)
    errors = validate_sql_paths(pack)
    # python_legacy is exempt; no AIDPF-2003.
    assert errors == []


def test_unknown_template_variable(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path, with_sql=True)
    _write_file(
        pack_root / "silver" / "dim_supplier.sql",
        "SELECT {{ frobnicate }} FROM x",
    )
    pack = load_pack(pack_root)
    errors = validate_template_variables(pack)
    assert any(e.code == AIDPF_5002_UNKNOWN_TEMPLATE_VAR for e in errors)


def test_variation_point_reference_undeclared(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path, with_sql=True)
    _write_file(
        pack_root / "silver" / "dim_supplier.sql",
        "SELECT {{ column.undeclared_alias }} FROM x",
    )
    pack = load_pack(pack_root)
    errors = validate_template_variables(pack)
    assert any(e.code == AIDPF_5003_UNDECLARED_VARIATION_POINT for e in errors)


def test_template_var_validator_accepts_known(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path, with_sql=True)
    # Base fixture uses `{{ column.supplier_natural_key }}` which is declared.
    pack = load_pack(pack_root)
    errors = validate_template_variables(pack)
    assert errors == []


def test_dag_unresolved_dependency(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path)
    # Add a gold node that depends on a non-existent silver node.
    _write_yaml(
        pack_root / "gold" / "supplier_spend.yaml",
        {
            "id": "supplier_spend",
            "layer": "gold",
            "implementation": {
                "type": "python_legacy",
                "callable": "pkg.supplier_spend:build",
                "deprecated": False,
                "migrationTarget": "gold/supplier_spend.sql",
            },
            "target": "supplier_spend",
            "dependsOn": {
                "silver": [{"id": "dim_nonexistent"}],
            },
            "refresh": {"seed": {"strategy": "replace"}},
            "outputSchema": {
                "columns": [
                    {"name": "supplier_number", "type": "string", "nullable": False, "pii": "low"},
                ]
            },
        },
    )
    pack = load_pack(pack_root)
    errors = validate_dag(pack)
    assert any(e.code == AIDPF_2041_UNRESOLVED_DEPENDENCY for e in errors)


def test_dag_cycle_detected(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path)
    # Two silver nodes that depend on each other; introduces a cycle.
    _write_yaml(
        pack_root / "silver" / "node_a.yaml",
        {
            "id": "node_a",
            "layer": "silver",
            "implementation": {
                "type": "python_legacy",
                "callable": "pkg.node_a:build",
                "deprecated": False,
                "migrationTarget": "silver/node_a.sql",
            },
            "target": "node_a",
            "dependsOn": {"silver": [{"id": "node_b"}]},
            "refresh": {"seed": {"strategy": "replace"}},
            "outputSchema": {
                "columns": [{"name": "k", "type": "string", "nullable": False, "pii": "none"}]
            },
        },
    )
    _write_yaml(
        pack_root / "silver" / "node_b.yaml",
        {
            "id": "node_b",
            "layer": "silver",
            "implementation": {
                "type": "python_legacy",
                "callable": "pkg.node_b:build",
                "deprecated": False,
                "migrationTarget": "silver/node_b.sql",
            },
            "target": "node_b",
            "dependsOn": {"silver": [{"id": "node_a"}]},
            "refresh": {"seed": {"strategy": "replace"}},
            "outputSchema": {
                "columns": [{"name": "k", "type": "string", "nullable": False, "pii": "none"}]
            },
        },
    )
    pack = load_pack(pack_root)
    errors = validate_dag(pack)
    assert any(e.code == AIDPF_2040_DAG_CYCLE for e in errors)


def test_dashboard_requires_missing_gold_node(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path)
    _write_yaml(
        pack_root / "dashboards" / "executive_cfo.yaml",
        {
            "id": "executive_cfo",
            "title": "CFO",
            "version": "0.1.0",
            "delivery": {
                "type": "oac-snapshot",
                "barObject": "dashboards/executive-cfo.bar",
                "oac": {
                    "projectName": "CFO",
                    "folderPath": "/Shared",
                    "connectionName": "aidp-fusion-gold",
                },
            },
            "requires": {
                "pack": {"id": "fusion-finance-starter", "minVersion": "0.1.0"},
                "tables": ["gold.nonexistent_table"],
            },
        },
    )
    pack = load_pack(pack_root)
    dashboard = pack.dashboards["executive_cfo"]
    errors = validate_dashboard_requires(pack, dashboard)
    assert any(e.code == AIDPF_7001_DASHBOARD_MISSING_NODE for e in errors)


def test_dashboard_column_type_mismatch(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path)
    _write_yaml(
        pack_root / "gold" / "gl_balance.yaml",
        {
            "id": "gl_balance",
            "layer": "gold",
            "implementation": {
                "type": "python_legacy",
                "callable": "pkg.gl_balance:build",
                "deprecated": False,
                "migrationTarget": "gold/gl_balance.sql",
            },
            "target": "gl_balance",
            "refresh": {"seed": {"strategy": "replace"}},
            "outputSchema": {
                "columns": [
                    {"name": "ledger_id", "type": "bigint", "nullable": False, "pii": "none"},
                ]
            },
        },
    )
    _write_yaml(
        pack_root / "dashboards" / "executive_cfo.yaml",
        {
            "id": "executive_cfo",
            "title": "CFO",
            "version": "0.1.0",
            "delivery": {
                "type": "oac-snapshot",
                "barObject": "dashboards/executive-cfo.bar",
                "oac": {
                    "projectName": "CFO",
                    "folderPath": "/Shared",
                    "connectionName": "aidp-fusion-gold",
                },
            },
            "requires": {
                "pack": {"id": "fusion-finance-starter", "minVersion": "0.1.0"},
                "tables": ["gold.gl_balance"],
                "columns": {
                    "gold.gl_balance": [
                        # Dashboard declares ledger_id as string; pack has bigint.
                        {"name": "ledger_id", "type": "string"},
                    ]
                },
            },
        },
    )
    pack = load_pack(pack_root)
    dashboard = pack.dashboards["executive_cfo"]
    errors = validate_dashboard_requires(pack, dashboard)
    assert any(e.code == AIDPF_7003_DASHBOARD_TYPE_MISMATCH for e in errors)


def test_validate_pack_full_aggregates_errors(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path, with_sql=True)
    # Break the SQL file to surface AIDPF-2003 + AIDPF-5002.
    (pack_root / "silver" / "dim_supplier.sql").unlink()
    pack = load_pack(pack_root)
    report = validate_pack_full(pack)
    assert not report.ok
    codes = {e.code for e in report.errors}
    assert AIDPF_2003_SQL_FILE_MISSING in codes
