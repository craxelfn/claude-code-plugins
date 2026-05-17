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
    DEFAULT_ACTUAL_FLAG_FILTER,
    DEFAULT_COA_SEGMENT_MAP,
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

        P2.18 refactor split: bronze ``gl_period_balances`` lives inside the
        ``balances`` CTE, and the outer SELECT joins ``balances`` to
        ``dim_account`` with the fact (CTE) on the LEFT side.
        """
        sql = build_gl_balance_sql()
        assert "LEFT JOIN" in sql, (
            "gl_balance MUST use LEFT JOIN — INNER JOIN drops balances for "
            "accounts missing from the dim, under-reporting the balance sheet"
        )
        # Bronze gl_period_balances is the source of the CTE
        assert re.search(
            r"FROM\s+\S*gl_period_balances\s+\w+",
            sql,
            flags=re.IGNORECASE,
        ), "gl_period_balances must be referenced (now inside the balances CTE)"
        # Outer SELECT: balances LEFT JOIN dim_account, CTE on the LEFT (preserved)
        assert re.search(
            r"FROM\s+balances\s+\w+\s+LEFT\s+JOIN\s+\S*dim_account",
            sql,
            flags=re.IGNORECASE,
        ), (
            "the balances CTE must be the LEFT (preserved) side of the dim join — "
            "every balance row must reach the output regardless of dim membership"
        )

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
        the financial number stays accurate. Plugin-portability note:
        ``company`` / ``cost_center`` etc. are now sourced from positional
        ``da.segment_NN`` columns (always emitted by ``dim_account``)
        rather than from optional semantic aliases — see the
        ``coa_segment_map`` knob.
        """
        sql = build_gl_balance_sql()
        # Non-segment dim attributes still pulled by name
        for col in ("code_combination", "account_type"):
            assert re.search(rf"da\.{col}\s+AS\s+{col}", sql), (
                f"{col} must be selected from the dim alias `da`"
            )
        # Segment-backed output columns now come from da.segment_NN positions
        for pos, alias in [(1, "company"), (2, "cost_center")]:
            assert re.search(rf"da\.segment_{pos:02d}\s+AS\s+{alias}", sql), (
                f"{alias} must be sourced from da.segment_{pos:02d} (positional, "
                "always emitted by dim_account regardless of tenant COA shape)"
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
        consumer concern). Wrapped in ROUND(..., 2). Each term is wrapped in
        COALESCE(..., 0) to prevent NULL propagation when the source has any
        NULL component (verified live 2026-05-09: ~20% of sample rows had at
        least one NULL component).

        P2.18 refactor: the four DECIMAL(28, 2) casts are hoisted into the
        ``balances`` CTE (one cast per row, not two). The outer SELECT's
        ``closing_balance`` formula references the CTE columns
        ``b.begin_balance_dr``, etc. The ``COALESCE(..., 0)`` wrap stays on
        each term — the cast hoist must NOT eliminate the COALESCE, or NULL
        propagation re-emerges.
        """
        sql = build_gl_balance_sql()
        # Whitespace-tolerant match on the new (post-P2.18) shape
        formula_pattern = (
            r"ROUND\(\s*"
            r"COALESCE\(\s*b\.begin_balance_dr,\s*0\s*\)\s*"
            r"-\s*COALESCE\(\s*b\.begin_balance_cr,\s*0\s*\)\s*"
            r"\+\s*COALESCE\(\s*b\.period_net_dr,\s*0\s*\)\s*"
            r"-\s*COALESCE\(\s*b\.period_net_cr,\s*0\s*\),\s*"
            r"2\s*\)\s+AS\s+closing_balance"
        )
        assert re.search(formula_pattern, sql), (
            "closing_balance must be ROUND(COALESCE(b.begin_balance_dr, 0) - "
            "COALESCE(b.begin_balance_cr, 0) + COALESCE(b.period_net_dr, 0) - "
            "COALESCE(b.period_net_cr, 0), 2) — without COALESCE on every term, "
            "NULL propagation nullifies closing_balance whenever any source "
            "component is NULL. The cast hoist into the CTE must NOT drop the "
            "outer COALESCE — these are separate invariants."
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
            "DEFAULT_ACTUAL_FLAG_FILTER",
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


class TestActualFlagFilterKnob:
    """Plugin-portability (round-6 review): a hardcoded ``BalanceActualFlag = 'A'``
    filter would silently drop balance rows on tenants whose data is
    predominantly encumbrance (``'E'``) or budget (``'B'``). The knob
    accepts each documented Fusion value, ``None`` to disable, and
    rejects everything else.
    """

    def test_default_is_actuals(self) -> None:
        """Backward-compatible default — same shape as pre-knob versions."""
        assert DEFAULT_ACTUAL_FLAG_FILTER == "A"
        sql = build_gl_balance_sql()
        assert "BalanceActualFlag = 'A'" in sql

    def test_encumbrance_override(self) -> None:
        sql = build_gl_balance_sql(actual_flag_filter="E")
        assert "BalanceActualFlag = 'E'" in sql
        assert "BalanceActualFlag = 'A'" not in sql

    def test_budget_override(self) -> None:
        sql = build_gl_balance_sql(actual_flag_filter="B")
        assert "BalanceActualFlag = 'B'" in sql

    def test_none_disables_filter(self) -> None:
        """``None`` removes the flag filter entirely — consumer slices on
        ``actual_flag`` themselves. CCID-NOT-NULL filter remains.
        """
        sql = build_gl_balance_sql(actual_flag_filter=None)
        assert "BalanceActualFlag = " not in sql
        assert "BalanceCodeCombinationId IS NOT NULL" in sql

    def test_invalid_filter_rejected(self) -> None:
        import pytest
        with pytest.raises(ValueError, match="actual_flag_filter"):
            build_gl_balance_sql(actual_flag_filter="X")

    def test_ccid_filter_still_present_under_default(self) -> None:
        sql = build_gl_balance_sql()
        assert "BalanceCodeCombinationId IS NOT NULL" in sql


class TestCoaSegmentMapKnob:
    """Plugin-portability: gl_balance reads positional ``segment_NN`` columns
    from ``silver.dim_account`` (always emitted) rather than the dim's
    optional semantic aliases (which are now tenant-configurable in
    dim_account and may not exist on non-conventional COA designs).
    The ``coa_segment_map`` knob controls which positions become which
    output column names.
    """

    def test_default_map_is_fusion_conventional(self) -> None:
        assert dict(DEFAULT_COA_SEGMENT_MAP) == {
            1: "company",
            2: "cost_center",
            3: "natural_account",
            4: "subaccount",
            5: "product",
            6: "intercompany",
        }

    def test_default_emits_canonical_six_segments(self) -> None:
        """Backwards-compat: the default produces the same output column
        names gl_balance had pre-refactor, so dashboards keep working.
        """
        sql = build_gl_balance_sql()
        for pos, alias in DEFAULT_COA_SEGMENT_MAP.items():
            assert re.search(rf"da\.segment_{pos:02d}\s+AS\s+{alias}", sql), (
                f"default map must emit da.segment_{pos:02d} AS {alias}"
            )

    def test_does_not_read_optional_dim_aliases_by_default(self) -> None:
        """The whole point of this refactor: do NOT read ``da.company`` /
        ``da.cost_center`` / ``da.account`` etc. anymore — those are
        optional in dim_account now, and a non-conventional COA tenant
        wouldn't have them.
        """
        sql = build_gl_balance_sql()
        for forbidden in (
            r"da\.company\b",
            r"da\.cost_center\b",
            r"da\.account\b(?!_)",   # avoid matching da.account_type / da.account_key
            r"da\.subaccount\b",
            r"da\.product\b",
            r"da\.intercompany\b",
        ):
            assert not re.search(forbidden, sql), (
                f"gl_balance must NOT read {forbidden!r} from dim_account — "
                "those aliases are tenant-optional. Use da.segment_NN instead."
            )

    def test_custom_map_relabels_segments(self) -> None:
        """A tenant whose COA puts natural account at segment 5 passes
        a custom map. Output column name follows the map value; backing
        position follows the map key.
        """
        sql = build_gl_balance_sql(
            coa_segment_map={
                1: "company",
                2: "department",      # tenant calls segment 2 "department"
                5: "natural_account", # natural account at position 5
            },
        )
        assert re.search(r"da\.segment_01\s+AS\s+company", sql)
        assert re.search(r"da\.segment_02\s+AS\s+department", sql)
        assert re.search(r"da\.segment_05\s+AS\s+natural_account", sql)
        # Default-only aliases must NOT appear under the custom map
        assert not re.search(r"AS\s+cost_center\b", sql)
        assert not re.search(r"AS\s+subaccount\b", sql)

    def test_empty_map_omits_segment_columns(self) -> None:
        """Caller can produce a segment-less mart and let consumers read
        ``segment_NN`` directly from ``silver.dim_account``.
        """
        sql = build_gl_balance_sql(coa_segment_map={})
        assert not re.search(r"da\.segment_\d{2}\s+AS", sql)
        # Non-segment dim attributes still present
        assert "da.code_combination" in sql
        assert "da.account_type" in sql

    def test_invalid_position_rejected(self) -> None:
        import pytest
        for bad_pos in (0, -1, 31, 100):
            with pytest.raises(ValueError, match=r"out of range"):
                build_gl_balance_sql(coa_segment_map={bad_pos: "x"})

    def test_invalid_alias_rejected(self) -> None:
        import pytest
        with pytest.raises(ValueError, match=r"valid SQL identifier"):
            build_gl_balance_sql(coa_segment_map={1: "drop table; --"})

    def test_digit_leading_alias_rejected(self) -> None:
        """Unquoted SQL identifiers cannot start with a digit. Caught at
        config-validation rather than as a cryptic Spark parse error.
        """
        import pytest
        for bad in ("123company", "9account", "0_x"):
            with pytest.raises(ValueError, match=r"valid SQL identifier"):
                build_gl_balance_sql(coa_segment_map={1: bad})

    def test_non_string_alias_rejected(self) -> None:
        import pytest
        with pytest.raises(ValueError, match=r"valid SQL identifier"):
            build_gl_balance_sql(coa_segment_map={1: 42})  # type: ignore[dict-item]

    def test_duplicate_alias_rejected(self) -> None:
        import pytest
        with pytest.raises(ValueError, match=r"duplicated"):
            build_gl_balance_sql(coa_segment_map={1: "x", 2: "x"})

    def test_map_in_exports(self) -> None:
        import oracle_ai_data_platform_fusion_bundle.transforms.gold.gl_balance as mod
        assert "DEFAULT_COA_SEGMENT_MAP" in mod.__all__


class TestPathsThreading:
    """P1.5b — tenant-aware table-path resolution."""

    def test_paths_none_matches_pre_refactor_defaults(self) -> None:
        sql = build_gl_balance_sql()
        assert "fusion_catalog.bronze.gl_period_balances" in sql
        assert "fusion_catalog.silver.dim_account"        in sql
        assert "fusion_catalog.gold.gl_balance"           in sql

    def test_paths_threading_replaces_catalog(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_gl_balance_sql(paths=TablePaths(catalog="my_lake"))
        assert "my_lake.bronze.gl_period_balances" in sql
        assert "my_lake.silver.dim_account"        in sql
        assert "my_lake.gold.gl_balance"           in sql
        assert "fusion_catalog" not in sql

    def test_explicit_table_kwarg_wins_over_paths(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_gl_balance_sql(
            paths=TablePaths(catalog="my_lake"),
            bronze_balances="explicit.bronze.X",
            silver_dim="explicit.silver.Y",
            gold_table="explicit.gold.Z",
        )
        assert "explicit.bronze.X" in sql
        assert "explicit.silver.Y" in sql
        assert "explicit.gold.Z"   in sql
        assert "my_lake" not in sql
