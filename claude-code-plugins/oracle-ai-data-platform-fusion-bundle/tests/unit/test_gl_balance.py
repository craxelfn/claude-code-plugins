"""Unit tests for ``transforms/gold/gl_balance.py``.

Same testing pattern as ``test_supplier_spend.py``: target the SQL string
output of the pure builder. The Spark wrapper :func:`build` isn't unit-tested
directly — it delegates to ``spark.sql`` / ``spark.table``.

Single-form contract — no `dim_calendar` join
---------------------------------------------

These tests lock in the **financial-correctness invariant** for the mart
(LEFT JOIN preserves every balance row) and the **grain-correctness
invariant** (no `dim_calendar` join because that dim is daily-grain;
period context comes from the fact's `period_year`/`period_num` columns).
"""

from __future__ import annotations

import re

from oracle_ai_data_platform_fusion_bundle.transforms.gold import gl_balance
from oracle_ai_data_platform_fusion_bundle.transforms.gold.gl_balance import (
    SOURCE_BRONZE_TABLE,
    SOURCE_SILVER_DIM,
    TARGET_GOLD_TABLE,
    build_gl_balance_sql,
)


class TestConstants:
    def test_source_bronze_table_three_part(self) -> None:
        assert SOURCE_BRONZE_TABLE == "fusion_catalog.bronze.gl_period_balances"

    def test_source_silver_dim_three_part(self) -> None:
        assert SOURCE_SILVER_DIM == "fusion_catalog.silver.dim_account"

    def test_target_gold_table_three_part(self) -> None:
        assert TARGET_GOLD_TABLE == "fusion_catalog.gold.gl_balance"


class TestSqlBuilder:
    def test_uses_create_or_replace_delta(self) -> None:
        sql = build_gl_balance_sql()
        assert "CREATE OR REPLACE TABLE" in sql
        assert "USING DELTA" in sql

    def test_uses_left_join_not_inner(self) -> None:
        """Financial-correctness invariant: balance rows are NEVER dropped.

        An INNER JOIN would silently drop balances whose CCID isn't in
        ``dim_account`` — under-reporting the balance sheet for any account
        the dim doesn't contain (recently-added segments, summary accounts).
        """
        sql = build_gl_balance_sql()
        assert "LEFT JOIN" in sql, (
            "gl_balance MUST use LEFT JOIN — INNER JOIN drops balances for "
            "accounts missing from the dim, under-reporting the balance sheet"
        )
        # bronze.gl_period_balances LEFT JOIN silver.dim_account (fact on left)
        assert re.search(
            r"FROM\s+\S*gl_period_balances\s+\w+\s+LEFT\s+JOIN\s+\S*dim_account",
            sql,
            flags=re.IGNORECASE,
        ), "gl_period_balances must be the LEFT (preserved) side of the join"

    def test_grain_uses_fact_account_id(self) -> None:
        """The grain MUST be the fact's CCID, not the dim's account_id.

        Selecting `da.account_id` would lose balance rows that didn't match
        (NULL collapse). Selecting `CAST(b.BalanceCodeCombinationId AS BIGINT)`
        preserves them — same financial-correctness reasoning as
        ``supplier_spend``.
        """
        sql = build_gl_balance_sql()
        assert re.search(
            r"CAST\(b\.BalanceCodeCombinationId\s+AS\s+BIGINT\)\s+AS\s+account_id",
            sql,
        ), "account_id alias must come from b.BalanceCodeCombinationId, not da.account_id"

    def test_dim_account_attributes_pulled_via_join(self) -> None:
        """code_combination / account_type / segment columns come from dim.

        NULL where the fact's CCID isn't in the dim — LEFT-JOIN behavior;
        the financial number stays accurate.
        """
        sql = build_gl_balance_sql()
        for col in ("code_combination", "account_type", "company", "cost_center"):
            assert re.search(rf"da\.{col}\s+AS\s+{col}", sql), (
                f"{col} must be selected from the dim alias `da`"
            )

    def test_no_dim_calendar_join(self) -> None:
        """Grain-correctness invariant: no `dim_calendar` join.

        ``dim_calendar`` is daily-grain; ``gl_period_balances`` is period-grain.
        Joining them naturally requires a derived first-of-period date and
        accepts period x day blowup. Instead we surface period_year/period_num/
        period_name from the fact directly. Don't reintroduce the dim_calendar
        join — see module docstring.
        """
        sql = build_gl_balance_sql()
        assert "dim_calendar" not in sql, (
            "gl_balance must not join dim_calendar — grain mismatch (daily vs period). "
            "Use period_year/period_num from the fact instead."
        )

    def test_period_context_from_fact(self) -> None:
        """period_year, period_num, period_name surface directly from the fact."""
        sql = build_gl_balance_sql()
        assert re.search(
            r"CAST\(b\.BalancePeriodYear\s+AS\s+BIGINT\)\s+AS\s+period_year",
            sql,
        ), "period_year must come from b.BalancePeriodYear cast to BIGINT"
        assert re.search(
            r"CAST\(b\.BalancePeriodNum\s+AS\s+BIGINT\)\s+AS\s+period_num",
            sql,
        ), "period_num must come from b.BalancePeriodNum cast to BIGINT"
        assert re.search(
            r"b\.BalancePeriodName\s+AS\s+period_name",
            sql,
        ), "period_name must come from b.BalancePeriodName (raw, not normalized)"

    def test_actual_flag_filter(self) -> None:
        """v0.2.0 filters to actual_flag='A' — no budget, no encumbrance."""
        sql = build_gl_balance_sql()
        assert re.search(
            r"WHERE[\s\S]*BalanceActualFlag\s*=\s*'A'",
            sql,
        ), "v0.2.0 must filter to BalanceActualFlag = 'A'"

    def test_filters_null_ccid(self) -> None:
        """Null-CCID rows can't join the dim and aren't meaningful — drop them."""
        sql = build_gl_balance_sql()
        assert "BalanceCodeCombinationId IS NOT NULL" in sql

    def test_translated_flag_surfaced_not_filtered(self) -> None:
        """translated_flag is exposed as a column but NOT filtered.

        Multi-currency aggregation rules belong to the consumer, not the mart.
        """
        sql = build_gl_balance_sql()
        # surfaced
        assert re.search(
            r"b\.BalanceTranslatedFlag\s+AS\s+translated_flag",
            sql,
        ), "translated_flag must be surfaced as a column"
        # not filtered (no `BalanceTranslatedFlag = ...` predicate)
        assert not re.search(
            r"BalanceTranslatedFlag\s*=\s*'",
            sql,
        ), "translated_flag must NOT be filtered — consumers pick reporting vs entered"

    def test_amount_casts_decimal_28_2(self) -> None:
        """Source is decimal(38,30); output cast to DECIMAL(28,2) for cents granularity."""
        sql = build_gl_balance_sql()
        for col in (
            "BalanceBeginBalanceDr",
            "BalanceBeginBalanceCr",
            "BalancePeriodNetDr",
            "BalancePeriodNetCr",
        ):
            assert re.search(
                rf"CAST\(b\.{col}\s+AS\s+DECIMAL\(28,\s*2\)\)",
                sql,
            ), f"{col} must be cast to DECIMAL(28,2) for output"

    def test_closing_balance_formula(self) -> None:
        """closing = COALESCE(begin_dr,0) - COALESCE(begin_cr,0)
                   + COALESCE(period_net_dr,0) - COALESCE(period_net_cr,0).

        Standard Fusion accounting form (signed; account-type sign-flip is a
        consumer concern). Wrapped in ROUND(..., 2). Each cast is wrapped in
        COALESCE(..., 0) to prevent NULL propagation when the source has any
        NULL component (verified live 2026-05-09: ~20% of sample rows had at
        least one NULL component).
        """
        sql = build_gl_balance_sql()
        # Approximate match — whitespace-tolerant
        formula_pattern = (
            r"ROUND\(\s*"
            r"COALESCE\(\s*CAST\(b\.BalanceBeginBalanceDr\s+AS\s+DECIMAL\(28,\s*2\)\),\s*0\s*\)\s*"
            r"-\s*COALESCE\(\s*CAST\(b\.BalanceBeginBalanceCr\s+AS\s+DECIMAL\(28,\s*2\)\),\s*0\s*\)\s*"
            r"\+\s*COALESCE\(\s*CAST\(b\.BalancePeriodNetDr\s+AS\s+DECIMAL\(28,\s*2\)\),\s*0\s*\)\s*"
            r"-\s*COALESCE\(\s*CAST\(b\.BalancePeriodNetCr\s+AS\s+DECIMAL\(28,\s*2\)\),\s*0\s*\),\s*"
            r"2\s*\)\s+AS\s+closing_balance"
        )
        assert re.search(formula_pattern, sql), (
            "closing_balance must be ROUND(COALESCE(begin_dr,0) - COALESCE(begin_cr,0) "
            "+ COALESCE(period_net_dr,0) - COALESCE(period_net_cr,0), 2) — "
            "without COALESCE, NULL propagation nullifies closing_balance whenever "
            "any source component is NULL"
        )

    def test_surfaced_amount_columns_not_coalesced(self) -> None:
        """The individual surfaced amount columns must NOT be COALESCE'd.

        Consumers need to distinguish "no data" (NULL) from "zero balance" (0).
        Only the computed ``closing_balance`` is COALESCE'd (to prevent NULL
        propagation in the sum); the surfaced columns pass through as-is.
        """
        sql = build_gl_balance_sql()
        for col in ("begin_balance_dr", "begin_balance_cr", "period_net_dr", "period_net_cr"):
            # The SELECT alias for each column must come directly from CAST(...)
            # without a COALESCE wrapper.
            assert re.search(
                rf"CAST\(b\.\w+\s+AS\s+DECIMAL\(28,\s*2\)\)\s+AS\s+{col}\b",
                sql,
            ), f"surfaced column {col} must come from a bare CAST without COALESCE"

    def test_emits_gold_built_at(self) -> None:
        sql = build_gl_balance_sql()
        assert "current_timestamp()" in sql
        assert "AS gold_built_at" in sql

    def test_custom_table_names_propagate(self) -> None:
        sql = build_gl_balance_sql(
            bronze_balances="x.bronze.bal",
            silver_dim="x.silver.dim",
            gold_table="x.gold.bal",
        )
        assert "FROM x.bronze.bal" in sql
        assert "LEFT JOIN x.silver.dim" in sql
        assert "CREATE OR REPLACE TABLE x.gold.bal" in sql
        assert "fusion_catalog" not in sql

    def test_default_table_names(self) -> None:
        sql = build_gl_balance_sql()
        assert SOURCE_BRONZE_TABLE in sql
        assert SOURCE_SILVER_DIM in sql
        assert TARGET_GOLD_TABLE in sql


class TestNoSilentBalanceDropContract:
    """Regression tests guarding against re-introducing INNER JOIN."""

    def test_no_inner_join_present(self) -> None:
        sql = build_gl_balance_sql()
        assert "INNER JOIN" not in sql.upper()
        # Strip the one LEFT JOIN we want; assert no bare JOIN snuck in
        stripped = sql.replace("LEFT JOIN", "")
        assert "JOIN" not in stripped, (
            "gl_balance should have exactly one JOIN, and it must be LEFT JOIN. "
            "Bare JOIN defaults to INNER in Spark, which drops balance rows."
        )


class TestModuleExports:
    def test_all_includes_public_surface(self) -> None:
        expected = {
            "SOURCE_BRONZE_TABLE",
            "SOURCE_SILVER_DIM",
            "TARGET_GOLD_TABLE",
            "build",
            "build_gl_balance_sql",
        }
        assert expected.issubset(set(gl_balance.__all__))

    def test_listed_in_gold_package(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.transforms import gold
        assert "gl_balance" in gold.__all__
        assert hasattr(gold, "gl_balance")
