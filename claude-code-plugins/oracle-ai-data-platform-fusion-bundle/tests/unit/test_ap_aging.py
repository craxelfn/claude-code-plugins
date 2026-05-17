"""Unit tests for ``transforms/gold/ap_aging.py``.

Same testing convention as ``test_gl_balance.py`` / ``test_supplier_spend.py``:
target the SQL string output of the pure builder. The Spark wrapper
:func:`build` and the schema-introspecting :func:`detect_ap_aging_params`
aren't unit-tested directly (they delegate to ``spark.table`` / ``spark.sql``);
they're exercised by TC24 live evidence on ``fusion_bundle_dev``.

These tests lock in the invariants from the reviewer rounds:

1. **Currency in grain** — ``currency_code`` is a key column, not a slicer
   (gl_balance precedent + the per-pod fork TC23 documented).
2. **Open-amount filter is invariant ``<> 0``** — never downgraded to ``> 0``,
   even on pods with zero credits. Reviewer Blocker #1.
3. **``due_date_mode`` is a public parameter** — both ``"real"`` and ``"proxy"``
   branches ship; the orchestrator selects per-tenant. Reviewer Blocker #2.
4. **No silent NET-30 under the canonical ``ap_aging`` name** — NET-30
   fallback applies only to residual NULLs in real mode (and only when at
   least one of ``terms_date_col``/``due_date_col`` is set); proxy mode buckets
   by ``invoice_date`` under a different table name. Reviewer Blocker #3.
5. **NULL ``invoice_date`` doesn't silently bucket into ``91+``** — default
   policy drops; ``unknown_bucket`` policy adds an explicit branch. Should-fix.
6. **As-of date is injectable** — defaults to ``CURRENT_DATE()`` but tests
   can pin a literal date for deterministic bucket assertions.
"""

from __future__ import annotations

import re

from oracle_ai_data_platform_fusion_bundle.transforms.gold import ap_aging
from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
    CANCELLED_KIND_FLAG,
    DEFAULT_REAL_MODE_GATE_THRESHOLD,
    DUE_DATE_MODE_AUTO,
    DUE_DATE_MODE_PROXY,
    DUE_DATE_MODE_REAL,
    NULL_INVOICE_DATE_POLICY_UNKNOWN_BUCKET,
    SOURCE_BRONZE_TABLE,
    SOURCE_SILVER_DIM,
    TARGET_GOLD_TABLE_PROXY,
    TARGET_GOLD_TABLE_REAL,
    build_ap_aging_sql,
    decide_due_date_mode,
)


class TestConstants:
    def test_source_bronze_table_three_part(self) -> None:
        assert SOURCE_BRONZE_TABLE == "fusion_catalog.bronze.ap_invoices"

    def test_source_silver_dim_three_part(self) -> None:
        assert SOURCE_SILVER_DIM == "fusion_catalog.silver.dim_supplier"

    def test_target_tables_both_present(self) -> None:
        """Both modes' target tables are exported as constants.

        The orchestrator/probe selects ``due_date_mode`` per tenant; consumers
        querying the gold layer need both names available. Reviewer Blocker #2.
        """
        assert TARGET_GOLD_TABLE_REAL  == "fusion_catalog.gold.ap_aging"
        assert TARGET_GOLD_TABLE_PROXY == "fusion_catalog.gold.ap_outstanding_by_invoice_age"
        assert TARGET_GOLD_TABLE_REAL != TARGET_GOLD_TABLE_PROXY


class TestModuleExports:
    def test_public_api_exported(self) -> None:
        for name in (
            "SOURCE_BRONZE_TABLE", "SOURCE_SILVER_DIM",
            "TARGET_GOLD_TABLE_REAL", "TARGET_GOLD_TABLE_PROXY",
            "DUE_DATE_MODE_REAL", "DUE_DATE_MODE_PROXY",
            "build", "build_ap_aging_sql", "detect_ap_aging_params",
        ):
            assert name in ap_aging.__all__, f"{name} must be in __all__"


class TestSqlShapeRealMode:
    def test_create_or_replace_delta_real(self) -> None:
        sql = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_REAL)
        assert "CREATE OR REPLACE TABLE" in sql
        assert "USING DELTA" in sql
        assert TARGET_GOLD_TABLE_REAL in sql

    def test_left_join_to_dim_supplier(self) -> None:
        """Financial-correctness invariant: every open invoice row is preserved
        even if its vendor isn't in ``silver.dim_supplier``. Same reasoning as
        ``gl_balance``/``supplier_spend``.
        """
        sql = build_ap_aging_sql()
        assert re.search(
            r"FROM\s+open_invoices\s+\w+\s+LEFT\s+JOIN\s+\S*dim_supplier",
            sql, flags=re.IGNORECASE,
        ), "open_invoices must be the LEFT (preserved) side of the join"

    def test_open_balance_filter_is_invariant_neq_zero(self) -> None:
        """Reviewer Blocker #1: filter is ALWAYS ``<> 0``, never ``> 0``.

        Preserves credit memos / overpayment offsets as negative open balances.
        On pods with zero credits today, ``<> 0`` reduces to ``> 0`` semantics
        anyway — but the filter must not be downgraded based on a single
        tenant's measurement.
        """
        sql = build_ap_aging_sql()
        assert re.search(r"<>\s*0", sql), "open-amount filter must use `<> 0`"
        assert "> 0" not in sql.replace("<> 0", ""), (
            "no `> 0` filter on open_amount — that would silently drop credits"
        )

    def test_null_vendor_filter(self) -> None:
        sql = build_ap_aging_sql()
        assert "ApInvoicesVendorId IS NOT NULL" in sql

    def test_decimal_28_2_precision(self) -> None:
        """Amounts use DECIMAL(28, 2) — cents granularity, financial standard."""
        sql = build_ap_aging_sql()
        assert "DECIMAL(28, 2)" in sql
        # Source is decimal(38, 30) — make sure we don't accidentally carry that
        assert "DECIMAL(38, 30)" not in sql

    def test_amount_paid_coalesce(self) -> None:
        """NULL ApInvoicesAmountPaid must be treated as 0, not NULL.

        Without ``COALESCE``, a single NULL nullifies the entire open_amount
        (NULL propagation in arithmetic) — would drop the row from the
        ``<> 0`` filter and produce wrong totals.
        """
        sql = build_ap_aging_sql()
        assert re.search(
            r"COALESCE\(\s*inv\.ApInvoicesAmountPaid,\s*0\s*\)",
            sql,
        ), "amount_paid must be COALESCE'd to 0 to avoid NULL propagation"


class TestCurrencyInGrain:
    def test_currency_code_in_projection(self) -> None:
        sql = build_ap_aging_sql()
        assert re.search(r"o\.currency_code\s+AS\s+currency_code", sql), (
            "currency_code must be a projected key column"
        )

    def test_currency_code_in_group_by(self) -> None:
        """Reviewer Blocker #1 from earlier round: currency in grain.

        Without currency in GROUP BY, the mart would sum USD + EUR + JPY,
        which is meaningless. Same lesson as TC23 documented for gl_balance.
        """
        sql = build_ap_aging_sql()
        group_by_clause = sql[sql.upper().rindex("GROUP BY"):]
        assert "o.currency_code" in group_by_clause, (
            "currency_code must appear in GROUP BY — see TC23 cross-currency lesson"
        )

    def test_currency_uppercased(self) -> None:
        """Normalize currency codes via UPPER() — consistent grain across tenants."""
        sql = build_ap_aging_sql()
        assert re.search(
            r"UPPER\(\s*CAST\(\s*inv\.ApInvoicesInvoiceCurrencyCode\s+AS\s+STRING\s*\)\s*\)",
            sql,
        )


class TestDueDateModeContract:
    """Reviewer Blocker #2: ``due_date_mode`` is a public parameter; both
    modes ship. Locks the public surface so a refactor can't silently
    remove one mode."""

    def test_real_mode_targets_canonical_table(self) -> None:
        sql = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_REAL)
        assert TARGET_GOLD_TABLE_REAL in sql
        assert TARGET_GOLD_TABLE_PROXY not in sql

    def test_proxy_mode_targets_proxy_table(self) -> None:
        sql = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_PROXY)
        assert TARGET_GOLD_TABLE_PROXY in sql
        assert TARGET_GOLD_TABLE_REAL not in sql

    def test_real_and_proxy_outputs_differ(self) -> None:
        """The two modes must produce distinct SQL (different bucketing input
        and different real-mode-only aggregates)."""
        real_sql  = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_REAL)
        proxy_sql = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_PROXY)
        assert real_sql != proxy_sql
        assert "net30_fallback_count" in real_sql
        assert "net30_fallback_count" not in proxy_sql

    def test_invalid_due_date_mode_rejected(self) -> None:
        import pytest
        with pytest.raises(ValueError, match="due_date_mode"):
            build_ap_aging_sql(due_date_mode="nonsense")


class TestRealModeDueDateLogic:
    def test_due_date_coalesce_chain(self) -> None:
        """Real-mode due_date is COALESCE(TermsDate, DueDate, invoice_date + 30).

        NET-30 fallback applies ONLY to residual NULLs after both real
        columns are exhausted. Per reviewer Blocker #3: not a uniform
        replacement under the canonical name.
        """
        sql = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_REAL)
        assert re.search(
            r"COALESCE\(\s*\n\s*CAST\(inv\.ApInvoicesTermsDate\s+AS\s+DATE\),\s*\n\s*"
            r"CAST\(inv\.ApInvoicesDueDate\s+AS\s+DATE\),\s*\n\s*"
            r"DATE_ADD\(CAST\(inv\.ApInvoicesInvoiceDate\s+AS\s+DATE\),\s*30\)",
            sql,
        )

    def test_due_date_source_provenance_aggregates(self) -> None:
        """Real-mode emits per-source counts (terms_date / due_date /
        net30_fallback) so live evidence can report the share that fell
        back to NET-30 — drives the orchestrator's gate threshold tuning.
        """
        sql = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_REAL)
        assert "net30_fallback_count" in sql
        assert "terms_date_count"     in sql
        assert "due_date_count"       in sql

    def test_real_mode_requires_at_least_one_due_date_col(self) -> None:
        """Cannot ship real mode if both real columns are absent — that would
        collapse to a uniform NET-30 fallback, which is what reviewer
        Blocker #3 specifically forbids under the canonical name.
        """
        import pytest
        with pytest.raises(ValueError, match="at least one of"):
            build_ap_aging_sql(
                due_date_mode=DUE_DATE_MODE_REAL,
                terms_date_col=None,
                due_date_col=None,
            )


class TestProxyModeBucketing:
    def test_proxy_bucket_uses_invoice_date(self) -> None:
        """Proxy mode buckets by ``DATEDIFF(as_of, invoice_date)`` — no
        due_date concept. Mart name (``ap_outstanding_by_invoice_age``)
        and ``bucket_basis = 'invoice_date'`` audit column tell consumers
        the semantics.
        """
        sql = build_ap_aging_sql(
            due_date_mode=DUE_DATE_MODE_PROXY,
            terms_date_col=None,
            due_date_col=None,
        )
        assert re.search(r"DATEDIFF\([^,]+,\s*o\.invoice_date\)", sql)
        assert "'invoice_date'" in sql  # bucket_basis literal
        # proxy-mode-only checks: no real-mode-only columns
        assert "net30_fallback_count" not in sql
        assert "terms_date_count"     not in sql


class TestAgingBucketBoundaries:
    """Locks the canonical bucket edges: 0 / 30 / 60 / 90."""

    def test_five_buckets_default(self) -> None:
        sql = build_ap_aging_sql()
        for bucket in ("'current'", "'1-30'", "'31-60'", "'61-90'", "'91+'"):
            assert bucket in sql, f"missing aging bucket {bucket}"

    def test_unknown_date_bucket_only_when_policy_enabled(self) -> None:
        """Default (policy='drop') has 5 buckets; ``unknown_bucket`` policy
        adds a 6th ``unknown_date`` branch and drops the IS-NOT-NULL filter.
        """
        default_sql = build_ap_aging_sql()
        assert "'unknown_date'" not in default_sql
        assert "ApInvoicesInvoiceDate IS NOT NULL" in default_sql

        ub_sql = build_ap_aging_sql(
            null_invoice_date_policy=NULL_INVOICE_DATE_POLICY_UNKNOWN_BUCKET,
        )
        assert "'unknown_date'" in ub_sql
        assert "ApInvoicesInvoiceDate IS NOT NULL" not in ub_sql


class TestAsOfDateContract:
    def test_default_is_current_date(self) -> None:
        sql = build_ap_aging_sql()
        assert "CURRENT_DATE()" in sql

    def test_injectable_literal_substitutes(self) -> None:
        """Tests pin a literal date so bucket assertions are deterministic."""
        sql = build_ap_aging_sql(as_of_date_expr="DATE'2026-05-10'")
        assert "DATE'2026-05-10'" in sql
        # the DATEDIFF must use the injected expression, not CURRENT_DATE()
        assert re.search(r"DATEDIFF\(\s*DATE'2026-05-10'", sql)
        # and the as_of_date output column must too
        assert re.search(r"CAST\(DATE'2026-05-10'\s+AS\s+DATE\)", sql)


class TestSchemaVariantKnobs:
    """Plugin-portability: the module supports the Fusion AP schema
    variants observed across tenants without code changes — only kwargs.
    """

    def test_due_date_col_absent_on_tenant(self) -> None:
        """The TC24 demo pod (saasfademo1) has TermsDate but no DueDate.

        With ``due_date_col=None``, the SQL must not reference
        ``ApInvoicesDueDate`` anywhere (Spark would reject parse).
        """
        sql = build_ap_aging_sql(
            due_date_mode=DUE_DATE_MODE_REAL,
            due_date_col=None,
        )
        assert "ApInvoicesDueDate" not in sql, (
            "due_date_col=None must remove all references to ApInvoicesDueDate"
        )
        assert "ApInvoicesTermsDate" in sql

    def test_cancelled_kind_flag_uses_y_filter(self) -> None:
        """Tenants with ``ApInvoicesCancelledFlag`` filter on != 'Y',
        not on NULL. The mart must support both variants.
        """
        sql = build_ap_aging_sql(
            cancelled_col="ApInvoicesCancelledFlag",
            cancelled_kind=CANCELLED_KIND_FLAG,
        )
        assert "ApInvoicesCancelledFlag" in sql
        assert "<> 'Y'" in sql

    def test_cancelled_col_none_skips_filter(self) -> None:
        """Tenants with no cancelled column should skip the cancelled filter
        entirely (don't fabricate a column reference that would fail parse).
        """
        sql = build_ap_aging_sql(cancelled_col=None)
        assert "ApInvoicesCancelledDate" not in sql
        assert "ApInvoicesCancelledFlag" not in sql

    def test_no_invoice_type_column_referenced(self) -> None:
        """The mart neither projects nor filters on invoice-type.

        Reviewer Blocker (round 4): an earlier version hardcoded
        ``ApInvoicesInvoiceTypeLookupCode`` in the CTE, which would fail
        Spark analysis on tenants whose column is named
        ``ApInvoicesInvoiceType`` instead. Since the mart doesn't use
        invoice-type for output or filtering (the probe surfaces it for
        information only), the right fix is to remove the reference
        entirely. If invoice-type aggregates are needed later, add an
        ``invoice_type_col: str | None`` knob.
        """
        for variant in ("real", "proxy"):
            kwargs = {"due_date_mode": variant}
            if variant == "proxy":
                kwargs.update(terms_date_col=None, due_date_col=None)
            sql = build_ap_aging_sql(**kwargs)  # type: ignore[arg-type]
            assert "ApInvoicesInvoiceTypeLookupCode" not in sql, (
                f"{variant} mode SQL must not reference "
                f"ApInvoicesInvoiceTypeLookupCode (tenant-variant column)"
            )
            assert "ApInvoicesInvoiceType" not in sql, (
                f"{variant} mode SQL must not reference any "
                f"ApInvoicesInvoiceType column"
            )


class TestMaxDaysColumnNamingByMode:
    """Reviewer Blocker (round 4): in proxy mode, the days expression is
    days-since-invoice (``DATEDIFF(as_of, invoice_date)``), not days-past-
    due. Labeling that ``max_days_past_due`` reintroduces the semantic
    confusion the mart-name gate exists to prevent. Each mode emits a
    self-documenting column name:

    * real  → ``max_days_past_due``
    * proxy → ``max_days_outstanding``
    """

    def test_real_mode_emits_max_days_past_due(self) -> None:
        sql = build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_REAL)
        assert "AS max_days_past_due" in sql
        assert "AS max_days_outstanding" not in sql

    def test_proxy_mode_emits_max_days_outstanding(self) -> None:
        sql = build_ap_aging_sql(
            due_date_mode=DUE_DATE_MODE_PROXY,
            terms_date_col=None,
            due_date_col=None,
        )
        assert "AS max_days_outstanding" in sql
        assert "AS max_days_past_due" not in sql, (
            "proxy mode must NOT label invoice-age as 'days past due' — "
            "that reintroduces the semantic confusion the mart-name gate "
            "exists to prevent"
        )


class TestDecideDueDateMode:
    """Reviewer Blocker (round 5): the public ``build()`` path defaulted to
    ``due_date_mode='real'`` and auto-detect only handled column *presence*,
    not coverage. On a tenant where ``TermsDate`` exists but is sparsely
    populated, calling ``ap_aging.build(spark)`` would silently ship
    ``gold.ap_aging`` with majority NET-30 fallback — exactly what reviewer
    Blocker #3 forbids.

    The fix: a pure ``decide_due_date_mode()`` function exercises the 80%
    coalesced-coverage gate; the public ``build()`` path defaults to
    ``due_date_mode='auto'`` which probes coverage and routes via this
    function. Unit-test the decision logic without Spark; the Spark-side
    coverage measurement is exercised by TC24 live evidence.
    """

    def test_both_cols_none_returns_proxy(self) -> None:
        """If neither real-date column exists, there's nothing to coalesce —
        proxy mode is the only honest answer regardless of coverage.
        """
        assert decide_due_date_mode(
            terms_date_col=None, due_date_col=None, coalesced_frac=None,
        ) == DUE_DATE_MODE_PROXY

    def test_coverage_at_threshold_picks_real(self) -> None:
        """The gate is ``>=`` 0.80 (PLAN §3.2); exactly 0.80 stays in real
        mode. Below threshold routes to proxy.
        """
        assert decide_due_date_mode(
            terms_date_col="ApInvoicesTermsDate", due_date_col=None,
            coalesced_frac=DEFAULT_REAL_MODE_GATE_THRESHOLD,
        ) == DUE_DATE_MODE_REAL

    def test_coverage_just_below_threshold_picks_proxy(self) -> None:
        """A tenant with TermsDate at 79% coverage must NOT publish
        ``gold.ap_aging`` — the resulting mart would be 21% NET-30 fallback,
        which is fake aging under the canonical name (reviewer Blocker #3).
        """
        assert decide_due_date_mode(
            terms_date_col="ApInvoicesTermsDate", due_date_col=None,
            coalesced_frac=0.7999,
        ) == DUE_DATE_MODE_PROXY

    def test_full_coverage_picks_real(self) -> None:
        """This pod's case: TermsDate populated 100% → real mode."""
        assert decide_due_date_mode(
            terms_date_col="ApInvoicesTermsDate", due_date_col=None,
            coalesced_frac=1.0,
        ) == DUE_DATE_MODE_REAL

    def test_zero_coverage_picks_proxy(self) -> None:
        """If both columns exist but are entirely empty, proxy mode is correct
        (effectively the same as both columns being absent)."""
        assert decide_due_date_mode(
            terms_date_col="ApInvoicesTermsDate",
            due_date_col="ApInvoicesDueDate",
            coalesced_frac=0.0,
        ) == DUE_DATE_MODE_PROXY

    def test_custom_threshold_respected(self) -> None:
        """Deployments wanting a stricter gate can raise the threshold via
        the ``gate_threshold`` parameter; the decision honors it.
        """
        assert decide_due_date_mode(
            terms_date_col="ApInvoicesTermsDate", due_date_col=None,
            coalesced_frac=0.85, gate_threshold=0.90,
        ) == DUE_DATE_MODE_PROXY  # 0.85 < 0.90 custom gate

    def test_missing_coverage_defaults_to_real_when_cols_set(self) -> None:
        """``coalesced_frac=None`` + cols set = empty open-invoice population
        (nothing to measure coverage against). Ship empty canonical
        ``gold.ap_aging`` rather than relabeling to the proxy mart —
        proxy mode is the answer for low data quality on existing rows,
        not for "no rows at all". Tomorrow's data lands in the right
        shape and consumers don't have to re-pivot.
        """
        assert decide_due_date_mode(
            terms_date_col="ApInvoicesTermsDate", due_date_col=None,
            coalesced_frac=None,
        ) == DUE_DATE_MODE_REAL

    def test_out_of_range_fraction_rejected(self) -> None:
        import pytest
        with pytest.raises(ValueError, match=r"\[0\.0, 1\.0\]"):
            decide_due_date_mode(
                terms_date_col="ApInvoicesTermsDate", due_date_col=None,
                coalesced_frac=1.5,
            )

    def test_build_ap_aging_sql_rejects_auto_mode(self) -> None:
        """The SQL builder only handles concrete modes. ``"auto"`` is a
        ``build()``-level concept (it requires Spark to measure coverage); the
        SQL builder shouldn't accept it.
        """
        import pytest
        with pytest.raises(ValueError, match="due_date_mode"):
            build_ap_aging_sql(due_date_mode=DUE_DATE_MODE_AUTO)


class TestCurrencyDetectionAndGate:
    """Round-6 plugin-portability fix: currency must be detected (canonical
    + alias) and absence hard-gates the build with a clear error. Earlier
    versions hardcoded ``ApInvoicesInvoiceCurrencyCode`` and would fail late
    inside Spark on tenants with the ``ApInvoicesCurrencyCode`` alias.

    We exercise the detect helper's contract directly via a minimal fake
    spark — the helper only needs ``spark.table(name).schema`` to return an
    iterable of objects with a ``.name`` attribute.
    """

    @staticmethod
    def _fake_spark(cols: list[str]):
        """Minimal duck-typed Spark substitute for ``detect_ap_aging_params``.

        Returns a stub whose ``.table(name).schema`` is a list of objects with
        ``.name`` attributes. We don't need Spark itself; we only need the
        detect helper's read interface.
        """
        class _Field:
            def __init__(self, name: str): self.name = name
        fields = [_Field(c) for c in cols]
        class _Table:
            schema = fields
        class _Spark:
            def table(self, _name: str): return _Table()
        return _Spark()

    def test_detects_canonical_currency_col(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
            detect_ap_aging_params,
        )
        spark = self._fake_spark([
            "ApInvoicesVendorId", "ApInvoicesInvoiceDate",
            "ApInvoicesInvoiceCurrencyCode",
        ])
        detected = detect_ap_aging_params(spark)
        assert detected["currency_col"] == "ApInvoicesInvoiceCurrencyCode"

    def test_detects_alias_currency_col(self) -> None:
        """Tenants using the ``ApInvoicesCurrencyCode`` alias instead of the
        canonical ``ApInvoicesInvoiceCurrencyCode`` are equally supported.
        """
        from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
            detect_ap_aging_params,
        )
        spark = self._fake_spark([
            "ApInvoicesVendorId", "ApInvoicesInvoiceDate",
            "ApInvoicesCurrencyCode",   # alias, no canonical
        ])
        detected = detect_ap_aging_params(spark)
        assert detected["currency_col"] == "ApInvoicesCurrencyCode"

    def test_canonical_wins_when_both_present(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
            detect_ap_aging_params,
        )
        spark = self._fake_spark([
            "ApInvoicesVendorId", "ApInvoicesInvoiceDate",
            "ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode",
        ])
        detected = detect_ap_aging_params(spark)
        assert detected["currency_col"] == "ApInvoicesInvoiceCurrencyCode"

    def test_neither_present_returns_none(self) -> None:
        """Caller (the build path) is responsible for hard-gating; the
        detect helper just reports None."""
        from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
            detect_ap_aging_params,
        )
        spark = self._fake_spark([
            "ApInvoicesVendorId", "ApInvoicesInvoiceDate",  # no currency
        ])
        detected = detect_ap_aging_params(spark)
        assert detected["currency_col"] is None


class TestCancelledAliasDetection:
    """Cancelled-flag variant detection: Fusion AP extracts emit one of
    three column names depending on the BICC packaging — all three are
    auto-detected so a default-args ``build(spark)`` works on any pod.

    Priority order: ``ApInvoicesCancelledDate`` (canonical) →
    ``ApInvoicesCancelDate`` (alias without the "led" suffix) →
    ``ApInvoicesCancelledFlag`` (Y/N boolean variant). The first two are
    date columns (kind="date"); the third is a Y/N flag (kind="flag").
    """

    @staticmethod
    def _fake_spark(cols: list[str]):
        class _Field:
            def __init__(self, name: str): self.name = name
        fields = [_Field(c) for c in cols]
        class _Table:
            schema = fields
        class _Spark:
            def table(self, _name: str): return _Table()
        return _Spark()

    def test_cancel_date_alias_detected(self) -> None:
        """A tenant whose extract emits ``ApInvoicesCancelDate`` (no "led")
        is still detected. Without this, the mart would silently include
        cancelled invoices in aging on those tenants.
        """
        from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
            CANCELLED_KIND_DATE,
            detect_ap_aging_params,
        )
        spark = self._fake_spark([
            "ApInvoicesVendorId", "ApInvoicesInvoiceDate",
            "ApInvoicesInvoiceCurrencyCode", "ApInvoicesCancelDate",
        ])
        detected = detect_ap_aging_params(spark)
        assert detected["cancelled_col"]  == "ApInvoicesCancelDate"
        assert detected["cancelled_kind"] == CANCELLED_KIND_DATE

    def test_canonical_cancelled_date_wins_over_alias(self) -> None:
        """When both alias and canonical exist, canonical wins to keep
        behavior predictable across mixed-schema tenants.
        """
        from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
            detect_ap_aging_params,
        )
        spark = self._fake_spark([
            "ApInvoicesVendorId", "ApInvoicesInvoiceDate",
            "ApInvoicesInvoiceCurrencyCode",
            "ApInvoicesCancelledDate", "ApInvoicesCancelDate",
        ])
        detected = detect_ap_aging_params(spark)
        assert detected["cancelled_col"] == "ApInvoicesCancelledDate"

    def test_flag_variant_used_only_when_neither_date_present(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
            CANCELLED_KIND_FLAG,
            detect_ap_aging_params,
        )
        spark = self._fake_spark([
            "ApInvoicesVendorId", "ApInvoicesInvoiceDate",
            "ApInvoicesInvoiceCurrencyCode", "ApInvoicesCancelledFlag",
        ])
        detected = detect_ap_aging_params(spark)
        assert detected["cancelled_col"]  == "ApInvoicesCancelledFlag"
        assert detected["cancelled_kind"] == CANCELLED_KIND_FLAG


class TestCoverageMeasurementAnsiSafety:
    """Plugin-portability / ANSI Spark safety: the coverage probe used
    ``SUM(...) / COUNT(*)`` directly, which faults under ANSI mode when
    the WHERE clause yields zero rows. The query now uses
    ``NULLIF(COUNT(*), 0)`` for the divisor; an empty population returns
    a NULL fraction, which the build path translates to "no measurement
    available" → default to real mode (the empty-data semantics).
    """

    def test_coverage_query_uses_nullif_count_divisor(self) -> None:
        """We can't run Spark in unit tests, but we can read the SQL the
        Spark-side helper would emit by exercising it through the
        ``_measure_due_date_coverage`` SQL composition. Build a minimal
        fake spark stub and capture the SQL string.
        """
        from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
            _measure_due_date_coverage,
        )
        captured: dict[str, str] = {}

        class _Row:
            def __getitem__(self, k: str) -> int: return 0

        class _DF:
            def collect(self) -> list[_Row]: return [_Row()]

        class _Spark:
            def sql(self, q: str) -> _DF:
                captured["sql"] = q
                return _DF()

        _measure_due_date_coverage(
            _Spark(),  # type: ignore[arg-type]
            bronze_table="fusion_catalog.bronze.ap_invoices",
            terms_date_col="ApInvoicesTermsDate",
            due_date_col=None,
            cancelled_col=None,
            cancelled_kind="date",
            null_invoice_date_policy="drop",
        )
        assert "NULLIF(COUNT(*), 0)" in captured["sql"], (
            "coverage probe MUST use NULLIF in the divisor to be ANSI-safe; "
            "raw COUNT(*) faults under strict ANSI Spark when the WHERE "
            "clause yields zero rows"
        )

    def test_empty_population_returns_none(self) -> None:
        """When the open-invoice population is empty, the measurement
        returns None (not 0.0). 0.0 would route the auto-mode router to
        proxy mode under the 80% gate, silently relabeling an empty
        canonical mart to the proxy variant. None preserves "no data"
        semantics so :func:`decide_due_date_mode` can default to real.
        """
        from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
            _measure_due_date_coverage,
        )

        class _EmptyRow:
            def __getitem__(self, k: str):
                # COUNT(*) over zero rows is 0; SUM/NULLIF yields NULL
                return {"open_n": 0, "coalesced_frac": None}[k]

        class _DF:
            def collect(self) -> list: return [_EmptyRow()]

        class _Spark:
            def sql(self, q: str): return _DF()

        result = _measure_due_date_coverage(
            _Spark(),  # type: ignore[arg-type]
            bronze_table="fusion_catalog.bronze.ap_invoices",
            terms_date_col="ApInvoicesTermsDate",
            due_date_col=None,
            cancelled_col=None,
            cancelled_kind="date",
            null_invoice_date_policy="drop",
        )
        assert result is None, (
            "empty open-invoice population must return None, not 0.0 — "
            "otherwise auto-mode routing relabels empty canonical mart to "
            "proxy under the 80% gate"
        )

    def test_no_real_cols_returns_none(self) -> None:
        """When neither real-date column is configured, the measurement
        is a no-op (None). The caller routes directly to proxy via
        decide_due_date_mode without needing the coverage number.
        """
        from oracle_ai_data_platform_fusion_bundle.transforms.gold.ap_aging import (
            _measure_due_date_coverage,
        )

        class _Spark:
            def sql(self, q: str): raise AssertionError("must not query Spark")

        result = _measure_due_date_coverage(
            _Spark(),  # type: ignore[arg-type]
            bronze_table="fusion_catalog.bronze.ap_invoices",
            terms_date_col=None,
            due_date_col=None,
            cancelled_col=None,
            cancelled_kind="date",
            null_invoice_date_policy="drop",
        )
        assert result is None


class TestPathsThreading:
    """P1.5b — tenant-aware table-path resolution.

    Tests A/B/C are the standard threading triple (matches every other mart).
    Tests D/E pin concrete-mode pure-SQL targets.
    Tests F/G use a fake-Spark stub to exercise the critical ordering
    invariant — gold_table must be resolved AFTER the auto-router resolves
    due_date_mode. Without this ordering, a tenant with
    100% due-date coverage would silently land at the proxy mart's path.
    """

    # --- A/B/C: standard threading triple --------------------------------------

    def test_paths_none_matches_pre_refactor_defaults(self) -> None:
        sql = build_ap_aging_sql()  # default: due_date_mode='real'
        assert "fusion_catalog.bronze.ap_invoices"  in sql
        assert "fusion_catalog.silver.dim_supplier" in sql
        assert "fusion_catalog.gold.ap_aging"       in sql

    def test_paths_threading_replaces_catalog(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_ap_aging_sql(paths=TablePaths(catalog="my_lake"))
        assert "my_lake.bronze.ap_invoices"  in sql
        assert "my_lake.silver.dim_supplier" in sql
        assert "my_lake.gold.ap_aging"       in sql
        assert "fusion_catalog" not in sql

    def test_explicit_table_kwarg_wins_over_paths(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_ap_aging_sql(
            paths=TablePaths(catalog="my_lake"),
            bronze_table="explicit.bronze.X",
            silver_dim="explicit.silver.Y",
            gold_table="explicit.gold.Z",
        )
        assert "explicit.bronze.X" in sql
        assert "explicit.silver.Y" in sql
        assert "explicit.gold.Z"   in sql
        assert "my_lake" not in sql

    # --- D/E: concrete-mode pure-SQL targets -----------------------------------

    def test_paths_threading_real_mode_under_custom_catalog(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_ap_aging_sql(
            paths=TablePaths(catalog="my_lake"),
            due_date_mode=DUE_DATE_MODE_REAL,
        )
        assert "my_lake.gold.ap_aging" in sql
        assert "my_lake.gold.ap_outstanding_by_invoice_age" not in sql

    def test_paths_threading_proxy_mode_under_custom_catalog(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_ap_aging_sql(
            paths=TablePaths(catalog="my_lake"),
            due_date_mode=DUE_DATE_MODE_PROXY,
            terms_date_col=None,
            due_date_col=None,
        )
        assert "my_lake.gold.ap_outstanding_by_invoice_age" in sql
        assert "my_lake.gold.ap_aging" not in sql.replace(
            "my_lake.gold.ap_outstanding_by_invoice_age", ""
        )

    # --- F/G: build()-level — auto-router ordering invariant -------------------

    @staticmethod
    def _fake_spark_for_build(coverage_frac: float, cols: list[str]) -> object:
        """Fake-Spark stub that:
        * Returns a fake schema (for detect_ap_aging_params) over ``cols``.
        * Captures every ``spark.sql(q)`` call to ``captured_sqls``.
        * Returns predetermined rows for the coverage probe + a degenerate
          row for the eventual CREATE-OR-REPLACE.
        """
        captured_sqls: list[str] = []

        class _Field:
            def __init__(self, name: str): self.name = name

        fields = [_Field(c) for c in cols]

        class _Table:
            schema = fields

        class _Row:
            def __getitem__(self, k: str):
                # Coverage probe expects open_n + coalesced_frac.
                return {"open_n": 100, "coalesced_frac": coverage_frac}[k]

        class _DF:
            def collect(self) -> list: return [_Row()]
            def show(self, *a, **kw) -> None: pass

        class _Spark:
            captured = captured_sqls

            def sql(self, q: str) -> _DF:
                captured_sqls.append(q)
                return _DF()

            def table(self, name: str) -> _Table:
                return _Table()

        return _Spark()

    def test_build_auto_mode_above_threshold_resolves_real_table_under_custom_paths(self) -> None:
        """F: coverage = 0.95 ≥ 0.80 gate → auto router lands on REAL mode →
        CREATE-OR-REPLACE must target ``my_lake.gold.ap_aging``.

        This test catches the regression where ``gold_table`` is resolved
        BEFORE the auto-router runs (which would land on proxy because
        ``due_date_mode == 'auto' != 'real'``).
        """
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        from oracle_ai_data_platform_fusion_bundle.transforms.gold import ap_aging

        spark = self._fake_spark_for_build(
            coverage_frac=0.95,
            cols=[
                "ApInvoicesVendorId", "ApInvoicesInvoiceDate",
                "ApInvoicesInvoiceAmount", "ApInvoicesAmountPaid",
                "ApInvoicesInvoiceCurrencyCode",
                "ApInvoicesTermsDate",
                "ApInvoicesCancelledDate",
            ],
        )
        ap_aging.build(spark, paths=TablePaths(catalog="my_lake"))  # type: ignore[arg-type]

        # Find the CREATE OR REPLACE SQL among captured calls.
        create_sqls = [q for q in spark.captured if "CREATE OR REPLACE" in q]  # type: ignore[attr-defined]
        assert create_sqls, "build() must issue a CREATE OR REPLACE TABLE"
        assert "my_lake.gold.ap_aging" in create_sqls[0]
        assert "my_lake.gold.ap_outstanding_by_invoice_age" not in create_sqls[0]

    def test_build_auto_mode_below_threshold_resolves_proxy_table_under_custom_paths(self) -> None:
        """G: coverage = 0.10 < 0.80 gate → auto router lands on PROXY mode →
        CREATE-OR-REPLACE must target ``my_lake.gold.ap_outstanding_by_invoice_age``.
        """
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        from oracle_ai_data_platform_fusion_bundle.transforms.gold import ap_aging

        spark = self._fake_spark_for_build(
            coverage_frac=0.10,
            cols=[
                "ApInvoicesVendorId", "ApInvoicesInvoiceDate",
                "ApInvoicesInvoiceAmount", "ApInvoicesAmountPaid",
                "ApInvoicesInvoiceCurrencyCode",
                "ApInvoicesTermsDate",
                "ApInvoicesCancelledDate",
            ],
        )
        ap_aging.build(spark, paths=TablePaths(catalog="my_lake"))  # type: ignore[arg-type]

        create_sqls = [q for q in spark.captured if "CREATE OR REPLACE" in q]  # type: ignore[attr-defined]
        assert create_sqls, "build() must issue a CREATE OR REPLACE TABLE"
        assert "my_lake.gold.ap_outstanding_by_invoice_age" in create_sqls[0]
        # Real table name must NOT appear (substring check after stripping the proxy table).
        stripped = create_sqls[0].replace("my_lake.gold.ap_outstanding_by_invoice_age", "")
        assert "my_lake.gold.ap_aging" not in stripped
