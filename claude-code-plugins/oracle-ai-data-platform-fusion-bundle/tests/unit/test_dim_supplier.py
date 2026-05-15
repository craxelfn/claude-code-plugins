"""Unit tests for ``dimensions/dim_supplier.py``.

All tests target the SQL builder + module-level constants. The Spark wrappers
``build()`` and ``id_populated_pct()`` are not unit-tested directly — they
delegate to ``spark.sql`` / ``spark.table`` and follow the same precedent as
``extractors.bicc.extract_pvo``: split "compose the spec" (testable) from
"execute against Spark" (not testable in unit since pyspark isn't a project
dependency).
"""

from __future__ import annotations

import re

from oracle_ai_data_platform_fusion_bundle.dimensions import dim_supplier
from oracle_ai_data_platform_fusion_bundle.dimensions.dim_supplier import (
    SOURCE_BRONZE_TABLE,
    TARGET_SILVER_TABLE,
    build_dim_supplier_sql,
)
from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import get


class TestConstants:
    def test_source_bronze_table_matches_catalog(self) -> None:
        assert SOURCE_BRONZE_TABLE == get("erp_suppliers").bronze_table

    def test_target_silver_table_three_part(self) -> None:
        assert TARGET_SILVER_TABLE == "fusion_catalog.silver.dim_supplier"


class TestSqlBuilder:
    def test_uses_create_or_replace_delta(self) -> None:
        sql = build_dim_supplier_sql()
        assert "CREATE OR REPLACE TABLE" in sql
        assert "USING DELTA" in sql

    def test_dedupes_on_segment1_desc_extract_ts(self) -> None:
        sql = build_dim_supplier_sql()
        assert re.search(
            r"ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION BY\s+SEGMENT1\s+ORDER BY\s+_extract_ts\s+DESC\s*\)",
            sql,
        ), "dedupe window must partition by SEGMENT1 + order by _extract_ts DESC"

    def test_filters_null_segment1(self) -> None:
        assert "WHERE SEGMENT1 IS NOT NULL" in build_dim_supplier_sql()

    def test_keeps_only_rn_1(self) -> None:
        assert "WHERE _rn = 1" in build_dim_supplier_sql()

    def test_nullifs_zero_for_each_id_column(self) -> None:
        sql = build_dim_supplier_sql()
        for col in ("VENDORID", "PARTYID", "PARENTVENDORID", "PARENTPARTYID"):
            assert f"NULLIF(CAST({col}" in sql, f"missing NULLIF(CAST({col} ...), 0)"
            # And the trailing ", 0)" must be on the same defensive call
            assert re.search(rf"NULLIF\(CAST\({col}\s+AS BIGINT\),\s*0\)", sql), (
                f"NULLIF(CAST({col} AS BIGINT), 0) shape required"
            )

    def test_supplier_name_coalesce_chain(self) -> None:
        """eseb-test live probe (2026-05-07): no name col is 100% pop; coalesce chain mandatory."""
        sql = build_dim_supplier_sql()
        # Order matters: AlternateNamePartyName is the highest-populated signal we found.
        # If a future probe finds a better column, update both the SQL and this test.
        # Use .*? with DOTALL since COALESCE args contain nested NULLIF(...) parens.
        assert re.search(
            r"COALESCE\(.*?AlternateNamePartyName.*?AliasPartyName.*?TaxReportingName.*?\)\s*AS supplier_name",
            sql,
            flags=re.DOTALL,
        ), "supplier_name coalesce chain must try AlternateNamePartyName → AliasPartyName → TaxReportingName"

    def test_carries_audit_columns_from_bronze(self) -> None:
        sql = build_dim_supplier_sql()
        assert "_extract_ts" in sql and "AS bronze_extract_ts" in sql
        assert "_source_pvo" in sql and "AS bronze_source_pvo" in sql

    def test_emits_silver_built_at(self) -> None:
        assert "current_timestamp()" in build_dim_supplier_sql()
        assert "AS silver_built_at" in build_dim_supplier_sql()

    def test_uses_custom_table_names_when_provided(self) -> None:
        sql = build_dim_supplier_sql(
            bronze_table="my_bronze.erp_suppliers",
            silver_table="my_silver.dim_supplier",
        )
        assert "CREATE OR REPLACE TABLE my_silver.dim_supplier" in sql
        assert "FROM my_bronze.erp_suppliers" in sql
        # And no bleed-through of the defaults:
        assert "fusion_catalog.bronze.erp_suppliers" not in sql
        assert "fusion_catalog.silver.dim_supplier" not in sql

    def test_default_table_names_when_called_without_args(self) -> None:
        sql = build_dim_supplier_sql()
        assert SOURCE_BRONZE_TABLE in sql
        assert TARGET_SILVER_TABLE in sql

    def test_supplier_key_uses_monotonically_increasing_id(self) -> None:
        """Surrogate key contract: rebuild-fresh, not hash-stable. Downstream marts must join on supplier_number."""
        assert "monotonically_increasing_id()" in build_dim_supplier_sql()
        assert "AS supplier_key" in build_dim_supplier_sql()


class TestModuleExports:
    def test_all_includes_public_surface(self) -> None:
        expected = {
            "SOURCE_BRONZE_TABLE",
            "TARGET_SILVER_TABLE",
            "build",
            "build_dim_supplier_sql",
            "id_populated_pct",
        }
        assert expected.issubset(set(dim_supplier.__all__))


class TestPathsThreading:
    """P1.5b — tenant-aware table-path resolution.

    Defaults must reproduce the pre-refactor literal values byte-for-byte;
    a ``TablePaths`` override flows through to the SQL; explicit per-table
    kwargs win over both.
    """

    def test_paths_none_matches_pre_refactor_defaults(self) -> None:
        sql = build_dim_supplier_sql()
        assert "fusion_catalog.bronze.erp_suppliers" in sql
        assert "fusion_catalog.silver.dim_supplier"  in sql

    def test_paths_threading_replaces_catalog(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_dim_supplier_sql(paths=TablePaths(catalog="my_lake"))
        assert "my_lake.bronze.erp_suppliers" in sql
        assert "my_lake.silver.dim_supplier"  in sql
        assert "fusion_catalog" not in sql

    def test_explicit_table_kwarg_wins_over_paths(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_dim_supplier_sql(
            paths=TablePaths(catalog="my_lake"),
            bronze_table="explicit.thing.X",
            silver_table="explicit.thing.Y",
        )
        assert "explicit.thing.X" in sql
        assert "explicit.thing.Y" in sql
        assert "my_lake.bronze.erp_suppliers" not in sql
        assert "my_lake.silver.dim_supplier"  not in sql
