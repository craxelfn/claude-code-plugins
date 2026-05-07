"""Unit tests for ``dimensions/dim_account.py``.

Same testing pattern as ``test_dim_supplier.py`` / ``test_dim_calendar.py``:
target the SQL string output of the pure builder. The Spark wrapper
:func:`build` isn't unit-tested directly — same "compose vs execute" split
as ``extractors.bicc.extract_pvo``.
"""

from __future__ import annotations

import re

from oracle_ai_data_platform_fusion_bundle.dimensions import dim_account
from oracle_ai_data_platform_fusion_bundle.dimensions.dim_account import (
    SOURCE_BRONZE_TABLE,
    TARGET_SILVER_TABLE,
    build_dim_account_sql,
)
from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import get


class TestConstants:
    def test_source_bronze_table_matches_catalog(self) -> None:
        assert SOURCE_BRONZE_TABLE == get("gl_coa").bronze_table

    def test_target_silver_table_three_part(self) -> None:
        assert TARGET_SILVER_TABLE == "fusion_catalog.silver.dim_account"


class TestSqlBuilder:
    def test_uses_create_or_replace_delta(self) -> None:
        sql = build_dim_account_sql()
        assert "CREATE OR REPLACE TABLE" in sql
        assert "USING DELTA" in sql

    def test_dedupes_on_ccid_desc_extract_ts(self) -> None:
        """Dedupe window: PARTITION BY CodeCombinationCodeCombinationId ORDER BY _extract_ts DESC."""
        sql = build_dim_account_sql()
        assert re.search(
            r"ROW_NUMBER\(\)\s+OVER\s*\(\s*"
            r"PARTITION BY\s+CodeCombinationCodeCombinationId\s+"
            r"ORDER BY\s+_extract_ts\s+DESC\s*\)",
            sql,
        ), "dedupe window must partition by CodeCombinationCodeCombinationId + order by _extract_ts DESC"

    def test_filters_null_ccid(self) -> None:
        """NULL CCID rows are dropped — they'd never join anyway."""
        sql = build_dim_account_sql()
        assert "WHERE CodeCombinationCodeCombinationId IS NOT NULL" in sql

    def test_keeps_only_rn_1(self) -> None:
        assert "WHERE _rn = 1" in build_dim_account_sql()

    def test_account_id_cast_to_bigint(self) -> None:
        """natural CCID is decimal(18,0) on bronze; we cast to BIGINT for downstream joins."""
        sql = build_dim_account_sql()
        assert re.search(
            r"CAST\(CodeCombinationCodeCombinationId\s+AS\s+BIGINT\)\s+AS\s+account_id",
            sql,
        ), "account_id must be CAST(CodeCombinationCodeCombinationId AS BIGINT)"

    def test_chart_of_accounts_id_cast_to_bigint(self) -> None:
        sql = build_dim_account_sql()
        assert re.search(
            r"CAST\(CodeCombinationChartOfAccountsId\s+AS\s+BIGINT\)\s+AS\s+chart_of_accounts_id",
            sql,
        )

    def test_code_combination_concat_ws_six_segments(self) -> None:
        """code_combination = SEGMENT1.SEGMENT2.…SEGMENT6 via CONCAT_WS('.')."""
        sql = build_dim_account_sql()
        # CONCAT_WS('.', S1, S2, S3, S4, S5, S6)
        assert re.search(
            r"CONCAT_WS\(\s*'\.'\s*,\s*"
            r"CodeCombinationSegment1\s*,\s*CodeCombinationSegment2\s*,\s*"
            r"CodeCombinationSegment3\s*,\s*CodeCombinationSegment4\s*,\s*"
            r"CodeCombinationSegment5\s*,\s*CodeCombinationSegment6",
            sql,
        ), "code_combination must CONCAT_WS('.') over the first 6 segments"

    def test_six_segments_named_aliases(self) -> None:
        """The 6 standard segment columns are projected with semantic names."""
        sql = build_dim_account_sql()
        for src, alias in [
            ("CodeCombinationSegment1", "company"),
            ("CodeCombinationSegment2", "cost_center"),
            ("CodeCombinationSegment3", "account"),
            ("CodeCombinationSegment4", "subaccount"),
            ("CodeCombinationSegment5", "product"),
            ("CodeCombinationSegment6", "intercompany"),
        ]:
            assert re.search(rf"{src}\s+AS\s+{alias}", sql), (
                f"missing segment alias mapping: {src} → {alias}"
            )

    def test_flag_and_classification_columns(self) -> None:
        """Account type, enabled, summary, postable, financial category — all projected."""
        sql = build_dim_account_sql()
        for src, alias in [
            ("CodeCombinationAccountType",                  "account_type"),
            ("CodeCombinationEnabledFlag",                  "enabled_flag"),
            ("CodeCombinationSummaryFlag",                  "summary_flag"),
            ("CodeCombinationDetailPostingAllowedFlag",     "detail_posting_allowed_flag"),
            ("CodeCombinationFinancialCategory",            "financial_category"),
        ]:
            assert re.search(rf"{src}\s+AS\s+{alias}", sql), (
                f"missing flag/classification: {src} → {alias}"
            )

    def test_date_columns_pass_through_natively(self) -> None:
        """StartDateActive/EndDateActive are already DATE on bronze — no CAST needed."""
        sql = build_dim_account_sql()
        for src, alias in [
            ("CodeCombinationStartDateActive", "start_date_active"),
            ("CodeCombinationEndDateActive",   "end_date_active"),
        ]:
            assert re.search(rf"{src}\s+AS\s+{alias}", sql), (
                f"missing date column: {src} → {alias}"
            )
            # And no spurious CAST around them — bronze types are already correct
            assert f"CAST({src}" not in sql, (
                f"{src} should NOT need CAST — bronze type is already DATE"
            )

    def test_carries_audit_columns_from_bronze(self) -> None:
        sql = build_dim_account_sql()
        assert "_extract_ts" in sql and "AS bronze_extract_ts" in sql
        assert "_source_pvo" in sql and "AS bronze_source_pvo" in sql

    def test_emits_silver_built_at(self) -> None:
        sql = build_dim_account_sql()
        assert "current_timestamp()" in sql
        assert "AS silver_built_at" in sql

    def test_surrogate_key_uses_monotonically_increasing_id(self) -> None:
        """Surrogate is non-stable across rebuilds; downstream joins on account_id."""
        sql = build_dim_account_sql()
        assert "monotonically_increasing_id()" in sql
        assert "AS account_key" in sql

    def test_uses_custom_table_names_when_provided(self) -> None:
        sql = build_dim_account_sql(
            bronze_table="my_bronze.gl_coa",
            silver_table="my_silver.dim_account",
        )
        assert "CREATE OR REPLACE TABLE my_silver.dim_account" in sql
        assert "FROM my_bronze.gl_coa" in sql
        # No bleed-through of defaults
        assert "fusion_catalog.bronze.gl_coa" not in sql
        assert "fusion_catalog.silver.dim_account" not in sql

    def test_default_table_names_when_called_without_args(self) -> None:
        sql = build_dim_account_sql()
        assert SOURCE_BRONZE_TABLE in sql
        assert TARGET_SILVER_TABLE in sql

    def test_does_not_filter_by_enabled_or_summary_flag(self) -> None:
        """The dim exposes all CoAs; filtering is the consumer's choice.

        Required by BACKLOG: "hierarchy attributes" — the dim must surface
        summary accounts too (with summary_flag='Y'), not silently drop them.
        """
        sql = build_dim_account_sql()
        # The only WHERE clauses should be (a) NULL CCID filter, (b) _rn = 1
        assert "ENABLEDFLAG = 'Y'" not in sql
        assert "EnabledFlag = 'Y'" not in sql
        assert "SUMMARYFLAG = 'N'" not in sql
        assert "SummaryFlag = 'N'" not in sql


class TestEmptyCoaEdgeCase:
    """Required by BACKLOG P1.3: covers the empty-CoA edge case."""

    def test_sql_does_not_assume_nonzero_rows(self) -> None:
        """The dedupe + WHERE structure works on 0-row bronze without exception.

        The SQL generates a CTAS that runs even when the inner SELECT yields
        0 rows; the resulting silver table is empty but exists. We can't
        execute Spark in unit tests, but we can assert no patterns require
        non-empty input (e.g., no LIMIT 1, no ORDER BY at the outer level
        that would NPE on empty, no aggregate without GROUP BY).
        """
        sql = build_dim_account_sql()
        # No outer-level aggregation that would silently drop empty results
        assert "GROUP BY" not in sql, (
            "dim_account is a row-preserving transform — GROUP BY here would "
            "be a bug (likely silently aggregates away empty CoAs)"
        )
        # No outer LIMIT that would mask shape issues
        assert "LIMIT" not in sql.upper()


class TestModuleExports:
    def test_all_includes_public_surface(self) -> None:
        expected = {
            "SOURCE_BRONZE_TABLE",
            "TARGET_SILVER_TABLE",
            "build",
            "build_dim_account_sql",
        }
        assert expected.issubset(set(dim_account.__all__))
