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
    AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE,
    AIDPF_7005_ALLOWED_COLUMNS_NOT_REQUIRED,
    AIDPF_8002_PII_HIGH_DASHBOARD_EXPOSURE,
    validate_dag,
    validate_dashboard_requires,
    validate_dashboard_security_and_compat,
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
            "type": "builtin",
            "callable": "pkg.dim_supplier:build",
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


def test_sql_path_validator_skips_non_sql_impl(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path, with_sql=False)
    pack = load_pack(pack_root)
    errors = validate_sql_paths(pack)
    # Non-sql implementation (builtin) is exempt; no AIDPF-2003.
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
                "type": "builtin",
                "callable": "pkg.supplier_spend:build",
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
                "type": "builtin",
                "callable": "pkg.node_a:build",
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
                "type": "builtin",
                "callable": "pkg.node_b:build",
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


def test_dashboard_requires_columns_typo_table_surfaces_AIDPF_7001(tmp_path: Path) -> None:
    """A typo confined to requires.columns must NOT slip through silently.

    Regression test for Finding 5 — previously validate_dashboard_requires
    silently skipped `requires.columns` table keys not in gold_by_target,
    assuming they were already reported from `requires.tables`. False when
    the typo only appears in `requires.columns` (correct gold table in
    `requires.tables`, typo'd table in `requires.columns`).
    """
    pack_root = _make_base_pack(tmp_path)
    _write_yaml(
        pack_root / "gold" / "gl_balance.yaml",
        {
            "id": "gl_balance",
            "layer": "gold",
            "implementation": {
                "type": "builtin",
                "callable": "pkg.gl_balance:build",
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
                # requires.tables is correct ...
                "tables": ["gold.gl_balance"],
                # ... but requires.columns has a typo (gl_balnace) that
                # references no gold node.
                "columns": {
                    "gold.gl_balnace": [{"name": "ledger_id", "type": "bigint"}]
                },
            },
        },
    )
    pack = load_pack(pack_root)
    dashboard = pack.dashboards["executive_cfo"]
    errors = validate_dashboard_requires(pack, dashboard)
    # The typo'd key must surface as AIDPF-7001, NOT be silently swallowed.
    assert any(
        e.code == AIDPF_7001_DASHBOARD_MISSING_NODE and "gl_balnace" in e.message
        for e in errors
    ), f"expected AIDPF-7001 for typo'd gold.gl_balnace, got: {errors!r}"


def test_dashboard_column_type_mismatch(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path)
    _write_yaml(
        pack_root / "gold" / "gl_balance.yaml",
        {
            "id": "gl_balance",
            "layer": "gold",
            "implementation": {
                "type": "builtin",
                "callable": "pkg.gl_balance:build",
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


# ---------------------------------------------------------------------------
# Dashboard security + compat validator (AIDPF-7004, AIDPF-7005, AIDPF-8002)
# ---------------------------------------------------------------------------


def _add_gold_node(
    pack_root: Path, *, target: str, pii_columns: dict[str, str]
) -> None:
    """Write a gold/<target>.yaml stub with the given (column → pii) outputSchema."""
    cols = [
        {"name": name, "type": "string", "nullable": True, "pii": pii}
        for name, pii in pii_columns.items()
    ]
    _write_yaml(
        pack_root / "gold" / f"{target}.yaml",
        {
            "id": target,
            "layer": "gold",
            "implementation": {
                "type": "builtin",
                "callable": f"pkg.{target}:build",
            },
            "target": target,
            "refresh": {"seed": {"strategy": "replace"}},
            "outputSchema": {"columns": cols},
        },
    )


def _write_dashboard(
    pack_root: Path,
    *,
    dash_id: str,
    requires_pack: dict,
    tables: list[str],
    required_columns: dict[str, list[dict]],
    allowed_columns: dict[str, list[str]] | None = None,
) -> None:
    """Write a dashboards/<id>.yaml with the given references."""
    data: dict = {
        "id": dash_id,
        "title": dash_id,
        "version": "0.1.0",
        "delivery": {
            "type": "oac-snapshot",
            "barObject": f"dashboards/{dash_id}.bar",
            "oac": {
                "projectName": dash_id,
                "folderPath": "/Shared",
                "connectionName": "aidp-fusion-gold",
            },
        },
        "requires": {
            "pack": requires_pack,
            "tables": tables,
            "columns": required_columns,
        },
    }
    if allowed_columns is not None:
        data["security"] = {"allowedColumns": allowed_columns}
    _write_yaml(pack_root / "dashboards" / f"{dash_id}.yaml", data)


def test_dashboard_pack_id_mismatch_rejected(tmp_path: Path) -> None:
    """AIDPF-7004 fires when dashboard's requires.pack.id != active pack id."""
    pack_root = _make_base_pack(tmp_path)
    _add_gold_node(pack_root, target="gl_balance", pii_columns={"ledger_id": "none"})
    _write_dashboard(
        pack_root,
        dash_id="cfo",
        requires_pack={"id": "some-other-pack", "minVersion": "0.1.0"},
        tables=["gold.gl_balance"],
        required_columns={"gold.gl_balance": [{"name": "ledger_id", "type": "string"}]},
    )
    pack = load_pack(pack_root)
    errors = validate_dashboard_security_and_compat(pack, pack.dashboards["cfo"])
    assert any(e.code == AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE for e in errors)


def test_dashboard_pack_min_version_violation_rejected(tmp_path: Path) -> None:
    """AIDPF-7004 fires when active pack version < requires.pack.minVersion."""
    pack_root = _make_base_pack(tmp_path)
    _add_gold_node(pack_root, target="gl_balance", pii_columns={"ledger_id": "none"})
    _write_dashboard(
        pack_root,
        dash_id="cfo",
        requires_pack={"id": "fusion-finance-starter", "minVersion": "9.9.9"},
        tables=["gold.gl_balance"],
        required_columns={"gold.gl_balance": [{"name": "ledger_id", "type": "string"}]},
    )
    pack = load_pack(pack_root)
    errors = validate_dashboard_security_and_compat(pack, pack.dashboards["cfo"])
    assert any(e.code == AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE for e in errors)


def test_dashboard_pack_max_version_violation_rejected(tmp_path: Path) -> None:
    """AIDPF-7004 fires when active pack version > requires.pack.maxVersion."""
    pack_root = _make_base_pack(tmp_path)
    _add_gold_node(pack_root, target="gl_balance", pii_columns={"ledger_id": "none"})
    _write_dashboard(
        pack_root,
        dash_id="cfo",
        requires_pack={
            "id": "fusion-finance-starter",
            "minVersion": "0.0.1",
            "maxVersion": "0.0.5",  # active pack is 0.1.0; exceeds max
        },
        tables=["gold.gl_balance"],
        required_columns={"gold.gl_balance": [{"name": "ledger_id", "type": "string"}]},
    )
    pack = load_pack(pack_root)
    errors = validate_dashboard_security_and_compat(pack, pack.dashboards["cfo"])
    assert any(e.code == AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE for e in errors)


def test_dashboard_pack_compat_passes_when_within_range(tmp_path: Path) -> None:
    """No AIDPF-7004 when active pack falls within [minVersion, maxVersion]."""
    pack_root = _make_base_pack(tmp_path)
    _add_gold_node(pack_root, target="gl_balance", pii_columns={"ledger_id": "none"})
    _write_dashboard(
        pack_root,
        dash_id="cfo",
        requires_pack={
            "id": "fusion-finance-starter",
            "minVersion": "0.1.0",
            "maxVersion": "1.0.0",
        },
        tables=["gold.gl_balance"],
        required_columns={"gold.gl_balance": [{"name": "ledger_id", "type": "string"}]},
    )
    pack = load_pack(pack_root)
    errors = validate_dashboard_security_and_compat(pack, pack.dashboards["cfo"])
    assert not [e for e in errors if e.code == AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE]


def test_dashboard_pii_high_in_required_columns_rejected(tmp_path: Path) -> None:
    """AIDPF-8002 fires when requires.columns includes a pii: high gold column."""
    pack_root = _make_base_pack(tmp_path)
    _add_gold_node(
        pack_root,
        target="ap_aging",
        pii_columns={"supplier_number": "low", "tax_id": "high"},
    )
    _write_dashboard(
        pack_root,
        dash_id="payables",
        requires_pack={"id": "fusion-finance-starter", "minVersion": "0.1.0"},
        tables=["gold.ap_aging"],
        required_columns={
            "gold.ap_aging": [
                {"name": "supplier_number", "type": "string"},
                {"name": "tax_id", "type": "string"},  # pii: high
            ],
        },
    )
    pack = load_pack(pack_root)
    errors = validate_dashboard_security_and_compat(pack, pack.dashboards["payables"])
    assert any(
        e.code == AIDPF_8002_PII_HIGH_DASHBOARD_EXPOSURE and "tax_id" in e.message
        for e in errors
    )


def test_dashboard_pii_high_in_allowed_columns_rejected(tmp_path: Path) -> None:
    """AIDPF-8002 fires when security.allowedColumns includes a pii: high column."""
    pack_root = _make_base_pack(tmp_path)
    _add_gold_node(
        pack_root,
        target="ap_aging",
        pii_columns={"supplier_number": "low", "tax_id": "high"},
    )
    _write_dashboard(
        pack_root,
        dash_id="payables",
        requires_pack={"id": "fusion-finance-starter", "minVersion": "0.1.0"},
        tables=["gold.ap_aging"],
        required_columns={
            "gold.ap_aging": [
                {"name": "supplier_number", "type": "string"},
                {"name": "tax_id", "type": "string"},
            ],
        },
        allowed_columns={"gold.ap_aging": ["supplier_number", "tax_id"]},
    )
    pack = load_pack(pack_root)
    errors = validate_dashboard_security_and_compat(pack, pack.dashboards["payables"])
    pii_errors = [e for e in errors if e.code == AIDPF_8002_PII_HIGH_DASHBOARD_EXPOSURE]
    assert pii_errors, f"expected AIDPF-8002 errors, got: {errors!r}"
    assert any("tax_id" in e.message for e in pii_errors)


def test_dashboard_allowed_columns_not_required_rejected(tmp_path: Path) -> None:
    """AIDPF-7005 fires when allowedColumns has entries missing from requires.columns."""
    pack_root = _make_base_pack(tmp_path)
    _add_gold_node(
        pack_root,
        target="ap_aging",
        pii_columns={"supplier_number": "low", "open_amount": "none"},
    )
    _write_dashboard(
        pack_root,
        dash_id="payables",
        requires_pack={"id": "fusion-finance-starter", "minVersion": "0.1.0"},
        tables=["gold.ap_aging"],
        required_columns={
            "gold.ap_aging": [{"name": "supplier_number", "type": "string"}]
        },
        # allowedColumns mentions open_amount, but it's NOT in required_columns.
        allowed_columns={"gold.ap_aging": ["supplier_number", "open_amount"]},
    )
    pack = load_pack(pack_root)
    errors = validate_dashboard_security_and_compat(pack, pack.dashboards["payables"])
    assert any(
        e.code == AIDPF_7005_ALLOWED_COLUMNS_NOT_REQUIRED and "open_amount" in e.message
        for e in errors
    )


def test_dashboard_security_validator_clean_when_no_pii_high(tmp_path: Path) -> None:
    """Validator passes when no high-PII column is referenced and pack matches."""
    pack_root = _make_base_pack(tmp_path)
    _add_gold_node(
        pack_root,
        target="ap_aging",
        pii_columns={"supplier_number": "low", "open_amount": "none"},
    )
    _write_dashboard(
        pack_root,
        dash_id="payables",
        requires_pack={"id": "fusion-finance-starter", "minVersion": "0.1.0"},
        tables=["gold.ap_aging"],
        required_columns={
            "gold.ap_aging": [
                {"name": "supplier_number", "type": "string"},
                {"name": "open_amount", "type": "string"},
            ]
        },
        allowed_columns={"gold.ap_aging": ["supplier_number", "open_amount"]},
    )
    pack = load_pack(pack_root)
    errors = validate_dashboard_security_and_compat(pack, pack.dashboards["payables"])
    assert errors == []


def test_validate_pack_full_aggregates_errors(tmp_path: Path) -> None:
    pack_root = _make_base_pack(tmp_path, with_sql=True)
    # Break the SQL file to surface AIDPF-2003 + AIDPF-5002.
    (pack_root / "silver" / "dim_supplier.sql").unlink()
    pack = load_pack(pack_root)
    report = validate_pack_full(pack)
    assert not report.ok
    codes = {e.code for e in report.errors}
    assert AIDPF_2003_SQL_FILE_MISSING in codes
