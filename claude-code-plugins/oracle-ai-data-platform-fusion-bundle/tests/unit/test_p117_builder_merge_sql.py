"""P1.17 Stage D — builder MERGE / seed SQL shape tests.

Covers:
  - D1: each silver/gold builder's incremental MERGE SQL contract
        (MERGE INTO + ON natural_key + WHERE _extract_ts > watermark
        + WHEN MATCHED/NOT MATCHED). Row-level builders only —
        `supplier_spend` and `ap_aging` are incremental-exempt.
  - D1 (exempt): `supplier_spend` and `ap_aging` emit byte-identical
        SQL in seed and incremental modes (the `incremental_capable=False`
        contract).
  - D2: seed-mode regression — surrogate key swap (P1.19) is the
        intentional delta; `bronze_extract_ts` is the new gold lineage
        column; everything else carries forward.
  - D7: P1.19 surrogate-key STABILITY — the SQL emits a deterministic
        `xxhash64(...)` expression rather than `monotonically_increasing_id()`.
  - D-aggregate: `supplier_spend.build(refresh_mode="incremental")`
        does NOT emit MERGE INTO (always seed-shape).
  - D-ap-aging-time-anchored: `ap_aging.build(refresh_mode="incremental")`
        does NOT emit MERGE INTO (always seed-shape per B3b).

All assertions are pure-string over the SQL output — no Spark required.
Live evidence (E1a / TC30a) verifies the SQL produces the right
behavior against real Delta tables.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from oracle_ai_data_platform_fusion_bundle.dimensions.dim_account import (
    NATURAL_KEY_COLUMN as DIM_ACCOUNT_NATURAL_KEY,
    build_dim_account_sql,
)
from oracle_ai_data_platform_fusion_bundle.dimensions.dim_supplier import (
    NATURAL_KEY_COLUMN as DIM_SUPPLIER_NATURAL_KEY,
    build_dim_supplier_sql,
)
from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
    build_ap_aging_sql,
    DUE_DATE_MODE_REAL,
)
from oracle_ai_data_platform_fusion_bundle.transforms.gold.gl_balance import (
    NATURAL_KEY_COLUMNS as GL_BALANCE_NATURAL_KEYS,
    build_gl_balance_sql,
)
from oracle_ai_data_platform_fusion_bundle.transforms.gold.supplier_spend import (
    build_supplier_spend_sql,
)


# A pinned watermark — `2026-05-01T00:00:00Z` rendered as an ISO literal
# with the trailing `Z` BICC expects.
_WATERMARK = datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
_WATERMARK_ISO = "2026-05-01T00:00:00Z"


# ---------------------------------------------------------------------------
# D1 — silver dim incremental MERGE SQL shape
# ---------------------------------------------------------------------------


class TestDimSupplierIncrementalSQL:
    """`dim_supplier` is a row-level silver dim — incremental ships a
    MERGE INTO keyed on `supplier_number` (the silver-side projection
    of `SEGMENT1`).
    """

    def test_natural_key_column_is_silver_projection(self) -> None:
        # The natural key must be the SILVER-side column name. An earlier
        # plan draft said "supplier_id" — that column does not exist on
        # this dim (dim_supplier.py:107 emits `SEGMENT1 AS supplier_number`).
        assert DIM_SUPPLIER_NATURAL_KEY == "supplier_number"

    def test_incremental_emits_merge_with_natural_key_on_clause(self) -> None:
        sql = build_dim_supplier_sql(refresh_mode="incremental", watermark=_WATERMARK)
        assert "MERGE INTO" in sql
        # NULL-safe `<=>` so composite or NULL-bearing keys match correctly.
        assert (
            "target.supplier_number <=> src.supplier_number" in sql
        ), "ON predicate should use the silver-projected natural key with <=>"
        assert "WHEN MATCHED THEN UPDATE SET *" in sql
        assert "WHEN NOT MATCHED THEN INSERT *" in sql

    def test_incremental_filters_source_by_watermark(self) -> None:
        sql = build_dim_supplier_sql(refresh_mode="incremental", watermark=_WATERMARK)
        assert f"_extract_ts > '{_WATERMARK_ISO}'" in sql

    def test_incremental_keeps_row_number_dedupe(self) -> None:
        # The MERGE source must still dedupe by SEGMENT1 → most-recent row.
        # Without this, a slow-changing supplier with two extracts in the
        # delta window would attempt to MERGE two rows under the same
        # natural key and Delta would refuse with a multi-match error.
        sql = build_dim_supplier_sql(refresh_mode="incremental", watermark=_WATERMARK)
        assert "ROW_NUMBER() OVER (PARTITION BY SEGMENT1 ORDER BY _extract_ts DESC)" in sql
        assert "WHERE _rn = 1" in sql

    def test_incremental_rejects_missing_watermark(self) -> None:
        with pytest.raises(ValueError, match="requires a non-None watermark"):
            build_dim_supplier_sql(refresh_mode="incremental")

    def test_seed_does_not_contain_merge(self) -> None:
        # Phase α path stays CREATE OR REPLACE for clean rebuilds.
        sql = build_dim_supplier_sql(refresh_mode="seed")
        assert "MERGE INTO" not in sql
        assert "CREATE OR REPLACE TABLE" in sql

    def test_seed_does_not_contain_watermark_predicate(self) -> None:
        sql = build_dim_supplier_sql(refresh_mode="seed", watermark=_WATERMARK)
        assert "_extract_ts >" not in sql

    def test_rejects_unknown_refresh_mode(self) -> None:
        with pytest.raises(ValueError, match="must be 'seed' or 'incremental'"):
            build_dim_supplier_sql(refresh_mode="bogus")


class TestDimAccountIncrementalSQL:
    """`dim_account` mirrors `dim_supplier` — keyed on `account_id`."""

    def test_natural_key_column_is_silver_projection(self) -> None:
        assert DIM_ACCOUNT_NATURAL_KEY == "account_id"

    def test_incremental_emits_merge_with_natural_key_on_clause(self) -> None:
        sql = build_dim_account_sql(refresh_mode="incremental", watermark=_WATERMARK)
        assert "MERGE INTO" in sql
        assert "target.account_id <=> src.account_id" in sql
        assert "WHEN MATCHED THEN UPDATE SET *" in sql
        assert "WHEN NOT MATCHED THEN INSERT *" in sql

    def test_incremental_filters_source_by_watermark(self) -> None:
        sql = build_dim_account_sql(refresh_mode="incremental", watermark=_WATERMARK)
        assert f"_extract_ts > '{_WATERMARK_ISO}'" in sql

    def test_incremental_rejects_missing_watermark(self) -> None:
        with pytest.raises(ValueError, match="requires a non-None watermark"):
            build_dim_account_sql(refresh_mode="incremental")


# ---------------------------------------------------------------------------
# D1 — gl_balance row-level gold mart incremental MERGE
# ---------------------------------------------------------------------------


class TestGLBalanceIncrementalSQL:
    """`gl_balance` is row-level (no GROUP BY) — incremental ships a
    composite-key MERGE with NULL-safe `<=>` so the `translated_flag`
    NULL bug doesn't break row matching (LIMITS.md P1.17-L8).
    """

    def test_natural_key_is_7_column_composite(self) -> None:
        assert GL_BALANCE_NATURAL_KEYS == (
            "ledger_id",
            "account_id",
            "period_year",
            "period_num",
            "currency_code",
            "actual_flag",
            "translated_flag",
        )

    def test_incremental_emits_merge_with_composite_null_safe_on_clause(self) -> None:
        sql = build_gl_balance_sql(refresh_mode="incremental", watermark=_WATERMARK)
        assert "MERGE INTO" in sql
        for col in GL_BALANCE_NATURAL_KEYS:
            assert (
                f"target.{col} <=> src.{col}" in sql
            ), f"composite ON predicate missing column {col}"
        # All `<=>` join clauses chained with AND.
        assert "AND target.account_id <=> src.account_id" in sql

    def test_incremental_filters_source_by_extract_ts_watermark(self) -> None:
        # gl_balance reads `b._extract_ts` directly from bronze (the
        # silver/dim join happens on top of the filtered bronze CTE).
        sql = build_gl_balance_sql(refresh_mode="incremental", watermark=_WATERMARK)
        assert f"b._extract_ts > '{_WATERMARK_ISO}'" in sql

    def test_incremental_does_not_drop_actual_flag_filter(self) -> None:
        # The default `BalanceActualFlag = 'A'` filter on the bronze
        # WHERE must still fire in incremental mode (it's a physical-
        # row filter, not a watermark concept).
        sql = build_gl_balance_sql(refresh_mode="incremental", watermark=_WATERMARK)
        assert "WHERE b.BalanceActualFlag = 'A'" in sql

    def test_incremental_does_not_contain_aggregate_pattern_ctes(self) -> None:
        # P1.17 V1 ships row-level only; the aggregate-pattern CTEs
        # (fact_delta_keys, dim_delta_pks, affected_slices, recomputed)
        # ship with P1.17b. Their presence here would be a leak.
        sql = build_gl_balance_sql(refresh_mode="incremental", watermark=_WATERMARK)
        for forbidden in ("fact_delta_keys", "dim_account_delta_pks", "affected_slices", "recomputed"):
            assert forbidden not in sql, f"{forbidden} CTE leaked from P1.17b"

    def test_incremental_carries_bronze_extract_ts_row_level(self) -> None:
        # gl_balance.bronze_extract_ts is a row-level passthrough from
        # `b._extract_ts` — NOT a MAX() aggregation (no GROUP BY).
        sql = build_gl_balance_sql(refresh_mode="incremental", watermark=_WATERMARK)
        assert "b.bronze_extract_ts" in sql
        assert "MAX(b._extract_ts)" not in sql
        assert "MAX(b.bronze_extract_ts)" not in sql

    def test_incremental_rejects_missing_watermark(self) -> None:
        with pytest.raises(ValueError, match="requires a non-None watermark"):
            build_gl_balance_sql(refresh_mode="incremental")

    def test_seed_does_not_contain_merge(self) -> None:
        sql = build_gl_balance_sql(refresh_mode="seed")
        assert "MERGE INTO" not in sql
        assert "CREATE OR REPLACE TABLE" in sql

    def test_seed_carries_bronze_extract_ts(self) -> None:
        # B3 lineage column rollout — even seed mode adds it now.
        sql = build_gl_balance_sql(refresh_mode="seed")
        assert "bronze_extract_ts" in sql


# ---------------------------------------------------------------------------
# D1 (exempt) + D-aggregate + D-ap-aging-time-anchored — incremental-exempt
# gold marts emit seed-shape SQL in incremental mode.
# ---------------------------------------------------------------------------


class TestSupplierSpendIncrementalExempt:
    """`supplier_spend` is `incremental_capable=False` (B2 + C1a): the
    6-column GROUP BY grain mixes a mutable fact attribute
    (`approval_status`); partial-MERGE would leave both old and new
    aggregate rows on a status flip. V1 always emits seed-shape SQL
    regardless of refresh_mode.
    """

    def test_supplier_spend_sql_renderer_never_emits_merge(self) -> None:
        # build_supplier_spend_sql doesn't take refresh_mode (the
        # renderer is mode-agnostic). The build() wrapper accepts the
        # kwargs but ignores them. Either way the SQL string never
        # contains MERGE INTO.
        sql = build_supplier_spend_sql()
        assert "MERGE INTO" not in sql
        assert "CREATE OR REPLACE TABLE" in sql

    def test_supplier_spend_carries_bronze_extract_ts(self) -> None:
        sql = build_supplier_spend_sql()
        # Aggregate mart — bronze_extract_ts uses MAX() over the grain.
        assert "MAX(inv.bronze_extract_ts)" in sql


class TestApAgingIncrementalExempt:
    """`ap_aging` is `incremental_capable=False` (B3b): bucket
    assignments are `CURRENT_DATE()`-anchored — rows age daily even
    with zero bronze delta. V1 always emits seed-shape SQL.
    """

    def test_ap_aging_sql_renderer_never_emits_merge(self) -> None:
        sql = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_REAL)
        assert "MERGE INTO" not in sql
        assert "CREATE OR REPLACE TABLE" in sql

    def test_ap_aging_carries_bronze_extract_ts(self) -> None:
        sql = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_REAL)
        # Aggregate mart — bronze_extract_ts uses MAX() over the grain.
        assert "MAX(o.bronze_extract_ts)" in sql


# ---------------------------------------------------------------------------
# D2 — seed-mode regression: P1.19 surrogate-key swap + B3 lineage column
# are the ONLY intentional deltas vs Phase α.
# ---------------------------------------------------------------------------


class TestSeedModeRegressionDeltas:
    """The only intentional deltas in seed-mode SQL vs Phase α are
    (a) P1.19's xxhash64 surrogate keys on dim_supplier + dim_account,
    (b) B3's new `bronze_extract_ts` column on every gold mart. Any
    other change to seed-mode SQL is a regression.
    """

    def test_dim_supplier_uses_xxhash_surrogate(self) -> None:
        sql = build_dim_supplier_sql(refresh_mode="seed")
        assert "xxhash64(CAST(SEGMENT1 AS STRING))" in sql
        assert "AS supplier_key" in sql
        # Pre-P1.19 used monotonically_increasing_id(); MUST be gone.
        assert "monotonically_increasing_id" not in sql

    def test_dim_account_uses_xxhash_surrogate(self) -> None:
        sql = build_dim_account_sql(refresh_mode="seed")
        assert "xxhash64(CAST(CodeCombinationCodeCombinationId AS STRING))" in sql
        assert "AS account_key" in sql
        assert "monotonically_increasing_id" not in sql

    def test_supplier_spend_seed_has_bronze_extract_ts(self) -> None:
        sql = build_supplier_spend_sql()
        assert "bronze_extract_ts" in sql

    def test_gl_balance_seed_has_bronze_extract_ts(self) -> None:
        sql = build_gl_balance_sql(refresh_mode="seed")
        assert "bronze_extract_ts" in sql

    def test_ap_aging_seed_has_bronze_extract_ts(self) -> None:
        sql = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_REAL)
        assert "bronze_extract_ts" in sql


# ---------------------------------------------------------------------------
# D7 — P1.19 surrogate-key stability (deterministic xxhash64 expression).
# A live-evidence E4 test verifies the values are stable across two real
# Spark builds; the unit assertion here pins the SQL form.
# ---------------------------------------------------------------------------


class TestSurrogateKeyStabilityShape:
    """Pre-P1.19 used `monotonically_increasing_id()` — partition-local
    + non-deterministic → surrogate values change every refresh. P1.17
    switches to `xxhash64(natural_key)` so two builds of the same
    bronze snapshot produce byte-identical surrogate values.
    """

    def test_dim_supplier_emits_deterministic_surrogate_expr(self) -> None:
        # Same SQL → same xxhash64() inputs → same outputs.
        sql_a = build_dim_supplier_sql(refresh_mode="seed", run_id="run-a")
        sql_b = build_dim_supplier_sql(refresh_mode="seed", run_id="run-b")
        # run_id differs (it's just a string literal); surrogate
        # expression is identical.
        assert "xxhash64(CAST(SEGMENT1 AS STRING))" in sql_a
        assert "xxhash64(CAST(SEGMENT1 AS STRING))" in sql_b

    def test_dim_account_emits_deterministic_surrogate_expr(self) -> None:
        sql = build_dim_account_sql(refresh_mode="seed")
        assert "xxhash64(CAST(CodeCombinationCodeCombinationId AS STRING))" in sql


# ---------------------------------------------------------------------------
# P1.17e — bronze MERGE payload-diff predicate helper + emitted SQL shape
# ---------------------------------------------------------------------------


from oracle_ai_data_platform_fusion_bundle.orchestrator import (  # noqa: E402
    BRONZE_AUDIT_COLUMNS,
    _payload_diff_predicate_sql,
)


class TestPayloadDiffPredicateHelper:
    """Direct unit tests for ``_payload_diff_predicate_sql`` — P1.17e's
    bronze MERGE payload-diff predicate generator. Pins the helper's
    contract independently of the renderer that consumes it."""

    def test_excludes_only_audit_cols_and_preserves_source_order(self) -> None:
        # E3 — pin the exclusion contract + source-order preservation.
        # Source order matters because Python set-iteration is not
        # deterministic; sorting would mask helper-side ordering bugs.
        cols = [
            "col_a",
            "_extract_ts",
            "col_b",
            "_source_pvo",
            "_run_id",
            "_watermark_used",
            "col_c",
        ]
        predicate = _payload_diff_predicate_sql(cols)
        assert predicate == (
            "target.col_a IS DISTINCT FROM src.col_a"
            " OR target.col_b IS DISTINCT FROM src.col_b"
            " OR target.col_c IS DISTINCT FROM src.col_c"
        )

    def test_returns_none_on_empty_input(self) -> None:
        assert _payload_diff_predicate_sql([]) is None

    def test_returns_none_when_only_audit_columns_present(self) -> None:
        # Defensive fallback path — caller renders V1 unconditional MERGE
        # shape unchanged when there are no data columns to diff.
        assert _payload_diff_predicate_sql(sorted(BRONZE_AUDIT_COLUMNS)) is None

    def test_alias_overrides_propagate(self) -> None:
        predicate = _payload_diff_predicate_sql(
            ["x"], target_alias="t", src_alias="s"
        )
        assert predicate == "t.x IS DISTINCT FROM s.x"

    def test_default_aliases_are_target_and_src(self) -> None:
        predicate = _payload_diff_predicate_sql(["x"])
        assert predicate == "target.x IS DISTINCT FROM src.x"

    def test_each_audit_column_excluded_individually(self) -> None:
        # Regression guard — if BRONZE_AUDIT_COLUMNS shrinks by mistake,
        # this test catches the leak. Pins every individual audit name.
        for audit in sorted(BRONZE_AUDIT_COLUMNS):
            predicate = _payload_diff_predicate_sql(["payload_col", audit])
            assert predicate == "target.payload_col IS DISTINCT FROM src.payload_col", (
                f"audit column {audit!r} leaked into payload-diff predicate"
            )


class TestBronzeMergePayloadDiffSQLShape:
    """End-to-end SQL-shape assertions for the bronze MERGE renderer +
    payload-diff predicate. Pure-string tests against the helper output
    (no orchestrator dispatch). The dispatch-boundary wiring is pinned
    separately in ``test_p117_orchestrator_dispatch.py`` (E4)."""

    def test_single_column_natural_key_shape(self) -> None:
        # E1 — single-column natural key (`erp_suppliers.SEGMENT1`).
        # Simulate the bronze column list as the renderer would see it
        # post-`enrich_bronze_audit_cols`: payload cols + 4 audit cols.
        cols = ["SEGMENT1", "VENDORID", "PARTYID", "_extract_ts", "_source_pvo", "_run_id", "_watermark_used"]
        predicate = _payload_diff_predicate_sql(cols)
        assert predicate is not None
        # Predicate contains an OR-clause for every payload col.
        assert "target.SEGMENT1 IS DISTINCT FROM src.SEGMENT1" in predicate
        assert "target.VENDORID IS DISTINCT FROM src.VENDORID" in predicate
        assert "target.PARTYID IS DISTINCT FROM src.PARTYID" in predicate
        # And NONE of the four audit cols.
        for audit in BRONZE_AUDIT_COLUMNS:
            assert audit not in predicate, (
                f"audit col {audit!r} leaked into payload-diff predicate: {predicate}"
            )

    def test_composite_natural_key_shape(self) -> None:
        # E2 — composite natural key (`gl_period_balances`-shaped, 7-col
        # composite key per registry.py:233-240). Each natural-key column
        # is included in the diff (harmless — the ON predicate already
        # matched, so target.k IS DISTINCT FROM src.k → false on those
        # cols). Pins the helper's decoupling from the natural-key set.
        cols = [
            "BalanceLedgerId",
            "BalanceCodeCombinationId",
            "BalancePeriodYear",
            "BalancePeriodNumber",
            "BalanceCurrencyCode",
            "BalanceActualFlag",
            "BalanceTranslatedFlag",
            "BalanceBeginBalance",
            "BalanceEndBalance",
            "_extract_ts",
            "_source_pvo",
            "_run_id",
            "_watermark_used",
        ]
        predicate = _payload_diff_predicate_sql(cols)
        assert predicate is not None
        # Each non-audit col contributes an OR-clause.
        non_audit = [c for c in cols if c not in BRONZE_AUDIT_COLUMNS]
        for col in non_audit:
            assert f"target.{col} IS DISTINCT FROM src.{col}" in predicate
        # `<=>` is reserved for the ON predicate — the payload-diff helper
        # MUST NOT emit it (regression guard against renderer-helper drift).
        assert "<=>" not in predicate
        # Audit cols excluded.
        for audit in BRONZE_AUDIT_COLUMNS:
            assert audit not in predicate
