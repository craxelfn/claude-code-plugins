"""Unit tests for ``transforms/gold/supplier_spend.py``.

Same testing pattern as ``test_dim_supplier.py``: target the SQL string
output of the pure builders. The Spark wrapper :func:`build` and the dim
helper it calls (:func:`id_populated_pct`) are not unit-tested directly —
they delegate to ``spark.sql`` / ``spark.table`` and follow the same
"compose vs execute" split established by ``extractors.bicc``.
"""

from __future__ import annotations

import re

from oracle_ai_data_platform_fusion_bundle.transforms.gold import supplier_spend
from oracle_ai_data_platform_fusion_bundle.transforms.gold.supplier_spend import (
    DEFAULT_JOIN_THRESHOLD,
    SOURCE_BRONZE_TABLE,
    SOURCE_SILVER_DIM,
    TARGET_GOLD_TABLE,
    build_join_form_sql,
    build_spend_only_form_sql,
    build_supplier_spend_sql,
)


class TestConstants:
    def test_source_bronze_table_three_part(self) -> None:
        assert SOURCE_BRONZE_TABLE == "fusion_catalog.bronze.ap_invoices"

    def test_source_silver_dim_three_part(self) -> None:
        assert SOURCE_SILVER_DIM == "fusion_catalog.silver.dim_supplier"

    def test_target_gold_table_three_part(self) -> None:
        assert TARGET_GOLD_TABLE == "fusion_catalog.gold.supplier_spend"

    def test_join_threshold_in_unit_interval(self) -> None:
        assert 0.0 <= DEFAULT_JOIN_THRESHOLD <= 1.0


class TestJoinForm:
    def test_uses_create_or_replace_delta(self) -> None:
        sql = build_join_form_sql()
        assert "CREATE OR REPLACE TABLE" in sql
        assert "USING DELTA" in sql

    def test_joins_dim_supplier_on_vendor_id(self) -> None:
        sql = build_join_form_sql()
        # Dim supplier uses BIGINT vendor_id; ap_invoices uses ApInvoicesVendorId.
        # The JOIN must explicitly cast the bronze side to BIGINT to match.
        assert re.search(
            r"JOIN\s+fusion_catalog\.silver\.dim_supplier\s+\w+\s+ON\s+\w+\.vendor_id\s*=\s*CAST\(\w+\.ApInvoicesVendorId\s+AS\s+BIGINT\)",
            sql,
        ), "join form must JOIN dim_supplier on vendor_id = CAST(ApInvoicesVendorId AS BIGINT)"

    def test_carries_dim_attributes(self) -> None:
        """Join form should carry supplier_number / supplier_name / business_relationship."""
        sql = build_join_form_sql()
        for col in ("supplier_number", "supplier_name", "business_relationship"):
            assert f"AS {col}" in sql, f"join form missing dim attribute: {col}"

    def test_aggregates_per_vendor_and_status(self) -> None:
        sql = build_join_form_sql()
        # Grain = (vendor_id, approval_status); GROUP BY must include both
        assert "GROUP BY" in sql
        assert "ApInvoicesApprovalStatus" in sql
        # And the canonical aggregate set
        assert "COUNT(*)" in sql
        assert "AS invoice_count" in sql
        assert "AS total_invoice_amount" in sql
        assert "AS total_paid" in sql
        assert "AS last_invoice_date" in sql

    def test_filters_null_vendor_id(self) -> None:
        sql = build_join_form_sql()
        assert "WHERE inv.ApInvoicesVendorId IS NOT NULL" in sql

    def test_emits_gold_built_at(self) -> None:
        sql = build_join_form_sql()
        assert "current_timestamp()" in sql
        assert "AS gold_built_at" in sql


class TestSpendOnlyForm:
    def test_uses_create_or_replace_delta(self) -> None:
        sql = build_spend_only_form_sql()
        assert "CREATE OR REPLACE TABLE" in sql
        assert "USING DELTA" in sql

    def test_does_not_join_dim(self) -> None:
        """Fallback form must NOT join silver.dim_supplier."""
        sql = build_spend_only_form_sql()
        assert "silver.dim_supplier" not in sql
        # And no JOIN keyword on the dim
        assert "JOIN" not in sql.upper().split("FROM")[1].split("WHERE")[0]

    def test_emits_null_dim_columns(self) -> None:
        """Fallback fills supplier_number / supplier_name / business_relationship as NULL."""
        sql = build_spend_only_form_sql()
        for col in ("supplier_number", "supplier_name", "business_relationship"):
            assert f"AS {col}" in sql, f"fallback missing column placeholder: {col}"
            # Must be CAST(NULL AS STRING) — schema parity with join form
        assert sql.count("CAST(NULL AS STRING)") == 3

    def test_aggregates_per_vendor_and_status(self) -> None:
        sql = build_spend_only_form_sql()
        assert "GROUP BY" in sql
        assert "ApInvoicesVendorId" in sql
        assert "ApInvoicesApprovalStatus" in sql
        assert "COUNT(*)" in sql
        for col in ("invoice_count", "total_invoice_amount", "total_paid", "last_invoice_date"):
            assert f"AS {col}" in sql

    def test_filters_null_vendor_id(self) -> None:
        sql = build_spend_only_form_sql()
        assert "WHERE inv.ApInvoicesVendorId IS NOT NULL" in sql


class TestPicker:
    def test_use_join_true_returns_join_sql(self) -> None:
        join_sql = build_join_form_sql()
        picked = build_supplier_spend_sql(use_join_form=True)
        assert picked == join_sql

    def test_use_join_false_returns_spend_only_sql(self) -> None:
        fallback_sql = build_spend_only_form_sql()
        picked = build_supplier_spend_sql(use_join_form=False)
        assert picked == fallback_sql

    def test_custom_table_names_propagate_through_picker(self) -> None:
        sql = build_supplier_spend_sql(
            use_join_form=True,
            bronze_invoices="x.bronze.ap",
            silver_dim="x.silver.dim",
            gold_table="x.gold.spend",
        )
        assert "FROM x.bronze.ap" in sql
        assert "JOIN x.silver.dim" in sql
        assert "CREATE OR REPLACE TABLE x.gold.spend" in sql
        # No bleed-through of defaults
        assert "fusion_catalog" not in sql


class TestSchemaParity:
    """Both forms must produce the same column set so downstream consumers are agnostic."""

    def test_same_column_aliases_in_both_forms(self) -> None:
        # Only count aliases that end the column expression (followed by comma
        # or newline). Avoids picking up type names from CAST(... AS STRING).
        pattern = r"\bAS (\w+)(?=\s*(?:,|\n))"
        join_aliases = set(re.findall(pattern, build_join_form_sql()))
        spend_aliases = set(re.findall(pattern, build_spend_only_form_sql()))
        # Both must produce identical column sets:
        assert join_aliases == spend_aliases, (
            f"schema mismatch — join only: {join_aliases - spend_aliases}, "
            f"spend-only only: {spend_aliases - join_aliases}"
        )
        # Sanity: must include the canonical 10-column gold mart shape
        expected = {
            "vendor_id",
            "supplier_number",
            "supplier_name",
            "business_relationship",
            "approval_status",
            "invoice_count",
            "total_invoice_amount",
            "total_paid",
            "last_invoice_date",
            "gold_built_at",
        }
        assert expected.issubset(join_aliases), f"join form missing columns: {expected - join_aliases}"


class TestModuleExports:
    def test_all_includes_public_surface(self) -> None:
        expected = {
            "SOURCE_BRONZE_TABLE",
            "SOURCE_SILVER_DIM",
            "TARGET_GOLD_TABLE",
            "DEFAULT_JOIN_THRESHOLD",
            "build",
            "build_join_form_sql",
            "build_spend_only_form_sql",
            "build_supplier_spend_sql",
        }
        assert expected.issubset(set(supplier_spend.__all__))
