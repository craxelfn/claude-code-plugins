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
    DEFAULT_SEMANTIC_SEGMENT_MAP,
    MAX_FUSION_SEGMENTS,
    SOURCE_BRONZE_TABLE,
    TARGET_SILVER_TABLE,
    build_dim_account_sql,
)
from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import get


class TestConstants:
    def test_source_bronze_table_matches_catalog(self) -> None:
        # Post §4.8a (2026-05-17): catalog declares only the bare table name;
        # the 3-part path is composed via DEFAULT_PATHS.bronze(name).
        from oracle_ai_data_platform_fusion_bundle.config.paths import DEFAULT_PATHS
        assert SOURCE_BRONZE_TABLE == DEFAULT_PATHS.bronze(get("gl_coa").bronze_table_name)

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
            "DEFAULT_SEMANTIC_SEGMENT_MAP",
            "MAX_FUSION_SEGMENTS",
            "SOURCE_BRONZE_TABLE",
            "TARGET_SILVER_TABLE",
            "build",
            "build_dim_account_sql",
            "detect_active_segments",
        }
        assert expected.issubset(set(dim_account.__all__))


class TestPositionalSegments:
    """Plugin-portability — every Fusion COA value lives in a tenant-agnostic
    ``segment_NN`` column, regardless of the customer's segment count or
    semantic naming. ``CodeCombinationSegment1..30`` always exist on the
    BICC extract; we surface all of them by default so no segment data
    can ever be truncated downstream.
    """

    def test_default_n_segments_is_fusion_max(self) -> None:
        """30 = Fusion's maximum; default avoids any tenant-specific
        truncation. Sparse tenants get NULL columns, not lost data.
        """
        assert MAX_FUSION_SEGMENTS == 30

    def test_all_30_positional_segments_emitted_by_default(self) -> None:
        sql = build_dim_account_sql()
        for i in range(1, 31):
            assert re.search(
                rf"CodeCombinationSegment{i}\b[^\n]*AS\s+segment_{i:02d}\b",
                sql,
            ), f"missing positional column segment_{i:02d} ← CodeCombinationSegment{i}"

    def test_n_segments_param_truncates_emission(self) -> None:
        """A tenant on a 4-segment COA can call with ``n_segments=4`` to
        avoid the 26 NULL columns. segment_01..segment_04 emitted;
        segment_05..segment_30 absent.
        """
        sql = build_dim_account_sql(n_segments=4, semantic_segment_map={})
        for i in range(1, 5):
            assert f"AS segment_{i:02d}" in sql
        for i in range(5, 31):
            assert f"AS segment_{i:02d}" not in sql

    def test_n_segments_out_of_range_rejected(self) -> None:
        import pytest
        for bad in (0, -1, 31, 100):
            with pytest.raises(ValueError, match=r"n_segments"):
                build_dim_account_sql(n_segments=bad)


class TestSemanticSegmentMap:
    """The semantic aliases (``company``, ``cost_center``, etc.) are
    tenant-configurable. Default mapping matches the Fusion-conventional
    six-segment ordering so saasfademo1 and similar pods reproduce the
    pre-refactor shape exactly; tenants whose COA differs override.
    """

    def test_default_map_is_fusion_conventional(self) -> None:
        assert dict(DEFAULT_SEMANTIC_SEGMENT_MAP) == {
            1: "company",
            2: "cost_center",
            3: "account",
            4: "subaccount",
            5: "product",
            6: "intercompany",
        }

    def test_custom_map_replaces_defaults(self) -> None:
        """A tenant whose segment 2 is "department" and segment 3 is "region"
        passes a custom map; only the configured aliases appear.
        """
        sql = build_dim_account_sql(
            semantic_segment_map={1: "ledger_id", 2: "department", 3: "region"},
        )
        for alias in ("ledger_id", "department", "region"):
            assert f"AS {alias}" in sql
        # Default Fusion aliases must NOT appear when overridden
        for alias in ("cost_center", "subaccount", "intercompany"):
            assert f"AS {alias}" not in sql

    def test_empty_map_suppresses_semantic_aliases(self) -> None:
        """Caller can request a strictly positional dim by passing ``{}``.

        Useful for tenants whose COA layout is so different from the Fusion
        convention that no default aliases make sense — they read
        ``segment_NN`` directly and let consumers/dashboards label. Use
        word-boundary assertions because "account" is a substring of
        "account_id" / "account_type" / "account_key" (all retained).
        """
        sql = build_dim_account_sql(semantic_segment_map={})
        for alias in ("company", "cost_center", "subaccount",
                      "product", "intercompany"):
            assert not re.search(rf"AS\s+{alias}\b(?!_)", sql), (
                f"semantic alias {alias!r} must be absent when "
                "semantic_segment_map={}"
            )
        # "account" is special — the bare alias is gone but compound
        # column names (account_type, account_id, account_key) remain.
        assert not re.search(r"AS\s+account\b(?!_)", sql)
        # Positional columns still present
        assert "AS segment_01" in sql

    def test_map_position_out_of_range_rejected(self) -> None:
        import pytest
        with pytest.raises(ValueError, match=r"out of range"):
            build_dim_account_sql(
                n_segments=4,
                semantic_segment_map={5: "product"},  # position 5 > n_segments=4
            )

    def test_map_invalid_alias_rejected(self) -> None:
        """SQL identifiers — protect against injection / parse errors."""
        import pytest
        with pytest.raises(ValueError, match=r"valid SQL identifier"):
            build_dim_account_sql(
                semantic_segment_map={1: "company; DROP TABLE x;--"},
            )

    def test_map_digit_leading_alias_rejected(self) -> None:
        """SQL identifiers cannot start with a digit (unquoted). The old
        ``.isalnum()`` check accepted ``"123abc"`` because it's alphanumeric,
        but Spark's SQL parser would reject it with a cryptic error. The
        strict regex catches it at config-validation time.
        """
        import pytest
        for bad in ("123abc", "5company", "42", "0_segment"):
            with pytest.raises(ValueError, match=r"valid SQL identifier"):
                build_dim_account_sql(semantic_segment_map={1: bad})

    def test_map_non_string_alias_rejected(self) -> None:
        """A misconfigured tenant config (e.g. YAML that resolves the value
        to int 1 instead of "company") fails at validation with a clear
        error rather than producing malformed SQL.
        """
        import pytest
        with pytest.raises(ValueError, match=r"valid SQL identifier"):
            build_dim_account_sql(semantic_segment_map={1: 42})  # type: ignore[dict-item]

    def test_map_underscore_and_letter_leading_accepted(self) -> None:
        """The two valid leading characters are letters (A-Za-z) and
        underscore (_). Both must continue to work.
        """
        sql = build_dim_account_sql(
            semantic_segment_map={1: "_private_seg", 2: "Company_Code"},
        )
        assert "AS _private_seg" in sql
        assert "AS Company_Code" in sql

    def test_map_duplicate_alias_rejected(self) -> None:
        import pytest
        with pytest.raises(ValueError, match=r"duplicated"):
            build_dim_account_sql(
                semantic_segment_map={1: "company", 2: "company"},
            )


class TestCodeCombinationCovers30Segments:
    """``code_combination`` is built from CONCAT_WS across all configured
    segments, not just the six conventional ones. ``CONCAT_WS`` skips
    NULL inputs by definition, so sparse tenants get clean dotted keys
    with no trailing dots.
    """

    def test_concat_ws_covers_all_n_segments(self) -> None:
        sql = build_dim_account_sql()
        for i in range(1, 31):
            assert f"CodeCombinationSegment{i}" in sql, (
                f"CONCAT_WS missing CodeCombinationSegment{i}"
            )

    def test_concat_ws_respects_n_segments(self) -> None:
        sql = build_dim_account_sql(n_segments=4, semantic_segment_map={})
        # Find the CONCAT_WS line and assert it doesn't list segments 5..30
        concat_match = re.search(r"CONCAT_WS\('\.',(.+?)\)", sql, flags=re.DOTALL)
        assert concat_match is not None
        concat_args = concat_match.group(1)
        for i in range(1, 5):
            assert f"CodeCombinationSegment{i}" in concat_args
        for i in range(5, 31):
            assert f"CodeCombinationSegment{i}" not in concat_args


class TestPathsThreading:
    """P1.5b — tenant-aware table-path resolution."""

    def test_paths_none_matches_pre_refactor_defaults(self) -> None:
        sql = build_dim_account_sql()
        assert "fusion_catalog.bronze.gl_coa"     in sql
        assert "fusion_catalog.silver.dim_account" in sql

    def test_paths_threading_replaces_catalog(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_dim_account_sql(paths=TablePaths(catalog="my_lake"))
        assert "my_lake.bronze.gl_coa"     in sql
        assert "my_lake.silver.dim_account" in sql
        assert "fusion_catalog" not in sql

    def test_explicit_table_kwarg_wins_over_paths(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_dim_account_sql(
            paths=TablePaths(catalog="my_lake"),
            bronze_table="explicit.thing.X",
            silver_table="explicit.thing.Y",
        )
        assert "explicit.thing.X" in sql
        assert "explicit.thing.Y" in sql
        assert "my_lake.bronze.gl_coa"      not in sql
        assert "my_lake.silver.dim_account" not in sql
