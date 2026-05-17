"""Unit tests for ``transforms/gold/supplier_spend.py``.

Same testing pattern as ``test_dim_supplier.py``: target the SQL string
output of the pure builder. The Spark wrapper :func:`build` isn't unit-
tested directly — it delegates to ``spark.sql`` / ``spark.table`` and
follows the same "compose vs execute" split established by ``extractors.bicc``.

Single-form contract — no picker, no INNER JOIN
-----------------------------------------------

These tests lock in the **financial-correctness invariant** for the mart:
the LEFT JOIN preserves every invoice (no silent drops). Earlier prototypes
had two SQL forms (INNER JOIN + spend-only fallback) selected by a picker;
that design could understate spend whenever an invoice's vendor wasn't in
the dim. Tests below assert the unified LEFT-JOIN shape stays in place.
"""

from __future__ import annotations

import re

from oracle_ai_data_platform_fusion_bundle.transforms.gold import supplier_spend
from oracle_ai_data_platform_fusion_bundle.transforms.gold.supplier_spend import (
    DEFAULT_CURRENCY_COL,
    KNOWN_CURRENCY_COL_ALIASES,
    SOURCE_BRONZE_TABLE,
    SOURCE_SILVER_DIM,
    TARGET_GOLD_TABLE,
    build_supplier_spend_sql,
    detect_currency_col,
)


class TestConstants:
    def test_source_bronze_table_three_part(self) -> None:
        assert SOURCE_BRONZE_TABLE == "fusion_catalog.bronze.ap_invoices"

    def test_source_silver_dim_three_part(self) -> None:
        assert SOURCE_SILVER_DIM == "fusion_catalog.silver.dim_supplier"

    def test_target_gold_table_three_part(self) -> None:
        assert TARGET_GOLD_TABLE == "fusion_catalog.gold.supplier_spend"


class TestSqlBuilder:
    def test_uses_create_or_replace_delta(self) -> None:
        sql = build_supplier_spend_sql()
        assert "CREATE OR REPLACE TABLE" in sql
        assert "USING DELTA" in sql

    def test_uses_left_join_not_inner(self) -> None:
        """Financial-correctness invariant: invoices are NEVER dropped.

        The mart consumes a CFO dashboard. An INNER JOIN would silently drop
        invoices whose vendor isn't in dim_supplier — understating spend.
        We require a LEFT JOIN with the invoice table on the left.

        P2.19 refactor split: bronze ``ap_invoices`` lives inside the
        ``invoices`` CTE, and the outer SELECT joins ``invoices`` to
        ``dim_supplier`` with the fact (CTE) on the LEFT side.
        """
        sql = build_supplier_spend_sql()
        assert "LEFT JOIN" in sql, (
            "supplier_spend MUST use LEFT JOIN — INNER JOIN drops invoices for "
            "vendors missing from the dim, understating spend"
        )
        # Bronze ap_invoices is the source of the CTE
        assert re.search(
            r"FROM\s+\S*ap_invoices\s+\w+",
            sql,
            flags=re.IGNORECASE,
        ), "ap_invoices must be referenced (now inside the invoices CTE)"
        # Outer SELECT: invoices LEFT JOIN dim_supplier, CTE on the LEFT (preserved)
        assert re.search(
            r"FROM\s+invoices\s+\w+\s+LEFT\s+JOIN\s+\S*dim_supplier",
            sql,
            flags=re.IGNORECASE,
        ), (
            "the invoices CTE must be the LEFT (preserved) side of the dim join — "
            "every invoice dollar must reach the output regardless of dim membership"
        )

    def test_grouping_uses_invoice_vendor_id(self) -> None:
        """The grain MUST be the invoice's claim of vendor, not the dim's.

        Grouping on `ds.vendor_id` would lose the invoice rows that didn't
        match the dim (NULL vendor_id collapse). The CTE projects
        `CAST(inv.ApInvoicesVendorId AS BIGINT) AS vendor_id` once; outer
        SELECT, JOIN, and GROUP BY all reference `inv.vendor_id`. The
        invariant is preserved because the CTE alias `inv` is the invoice
        side — `inv.vendor_id` is the invoice's vendor claim.
        """
        sql = build_supplier_spend_sql()
        # The invoices CTE projects the BIGINT vendor_id from the invoice side
        assert re.search(
            r"CAST\(inv\.ApInvoicesVendorId\s+AS\s+BIGINT\)\s+AS\s+vendor_id",
            sql,
        ), (
            "the invoices CTE must project CAST(inv.ApInvoicesVendorId AS BIGINT) "
            "AS vendor_id — invoice-side vendor id, not dim-side"
        )
        # The GROUP BY must include the CTE's invoice-side vendor_id
        group_by_clause = sql[sql.upper().rindex("GROUP BY"):]
        assert re.search(
            r"\binv\.vendor_id\b",
            group_by_clause,
        ), (
            "GROUP BY must reference inv.vendor_id (invoice CTE side) — "
            "grouping on ds.vendor_id would collapse unmatched invoices into a "
            "single NULL bucket"
        )

    def test_select_vendor_id_from_invoice_side(self) -> None:
        """`vendor_id` in the output is the invoice's claim — not `ds.vendor_id`."""
        sql = build_supplier_spend_sql()
        # The SELECT list's vendor_id alias must come from the invoice
        assert re.search(
            r"CAST\(inv\.ApInvoicesVendorId\s+AS\s+BIGINT\)\s+AS\s+vendor_id",
            sql,
        ), "vendor_id alias must come from inv.ApInvoicesVendorId, not ds.vendor_id"

    def test_dim_attributes_pulled_via_join(self) -> None:
        """supplier_number / supplier_name / business_relationship come from dim.

        They'll be NULL where the invoice's vendor isn't in the dim — that's
        the LEFT JOIN behavior, and the financial number stays accurate.
        """
        sql = build_supplier_spend_sql()
        for col in ("supplier_number", "supplier_name", "business_relationship"):
            assert re.search(rf"ds\.{col}\s+AS\s+{col}", sql), (
                f"{col} must be selected from the dim alias `ds`"
            )

    def test_aggregates_per_vendor_and_status(self) -> None:
        sql = build_supplier_spend_sql()
        assert "GROUP BY" in sql
        assert "ApInvoicesApprovalStatus" in sql
        # Canonical aggregate set
        assert "COUNT(*)" in sql
        assert "AS invoice_count" in sql
        assert "AS total_invoice_amount" in sql
        assert "AS total_paid" in sql
        assert "AS last_invoice_date" in sql

    def test_filters_null_invoice_vendor_id(self) -> None:
        """We do drop invoices with NO vendor at all — they have no place in spend."""
        sql = build_supplier_spend_sql()
        assert "WHERE inv.ApInvoicesVendorId IS NOT NULL" in sql

    def test_emits_gold_built_at(self) -> None:
        sql = build_supplier_spend_sql()
        assert "current_timestamp()" in sql
        assert "AS gold_built_at" in sql

    def test_custom_table_names_propagate(self) -> None:
        sql = build_supplier_spend_sql(
            bronze_invoices="x.bronze.ap",
            silver_dim="x.silver.dim",
            gold_table="x.gold.spend",
        )
        assert "FROM x.bronze.ap" in sql
        assert "LEFT JOIN x.silver.dim" in sql
        assert "CREATE OR REPLACE TABLE x.gold.spend" in sql
        # No bleed-through of defaults
        assert "fusion_catalog" not in sql

    def test_default_table_names(self) -> None:
        sql = build_supplier_spend_sql()
        assert SOURCE_BRONZE_TABLE in sql
        assert SOURCE_SILVER_DIM in sql
        assert TARGET_GOLD_TABLE in sql


class TestNoSilentInvoiceDropContract:
    """Regression tests guarding against re-introducing the INNER JOIN bug."""

    def test_no_inner_join_present(self) -> None:
        """If anyone re-introduces an INNER JOIN, this test must catch it."""
        sql = build_supplier_spend_sql()
        # `INNER JOIN` shouldn't appear; nor should the bare `JOIN` keyword
        # without LEFT/RIGHT modifier (Spark SQL defaults bare JOIN to INNER).
        assert "INNER JOIN" not in sql.upper()
        # Strip the LEFT JOIN we explicitly want, then check no other JOINs
        # snuck in:
        stripped = sql.replace("LEFT JOIN", "")
        assert "JOIN" not in stripped, (
            "supplier_spend should have exactly one JOIN, and it must be LEFT JOIN. "
            "Bare JOIN defaults to INNER in Spark, which can drop invoices."
        )

    def test_no_picker_or_threshold_present(self) -> None:
        """The unified form has no use_join_form parameter — it's now obsolete."""
        import inspect
        sig = inspect.signature(build_supplier_spend_sql)
        assert "use_join_form" not in sig.parameters, (
            "Picker logic has been removed; supplier_spend now uses a single "
            "LEFT JOIN form. If you're re-introducing two-form selection, "
            "please measure invoice→dim coverage (not dim's internal "
            "completeness) and document the rationale in the module docstring."
        )


class TestModuleExports:
    def test_all_includes_public_surface(self) -> None:
        expected = {
            "SOURCE_BRONZE_TABLE",
            "SOURCE_SILVER_DIM",
            "TARGET_GOLD_TABLE",
            "build",
            "build_supplier_spend_sql",
        }
        assert expected.issubset(set(supplier_spend.__all__))

    def test_obsolete_picker_symbols_are_removed(self) -> None:
        """The two-form era's exports must be gone — keep the public surface tight."""
        gone = {
            "DEFAULT_JOIN_THRESHOLD",
            "build_join_form_sql",
            "build_spend_only_form_sql",
        }
        for name in gone:
            assert name not in supplier_spend.__all__, (
                f"{name} was part of the picker design; it's gone now. "
                f"Update __all__ if you're re-introducing the picker."
            )
            assert not hasattr(supplier_spend, name), (
                f"{name} still defined as a module attribute — clean it up."
            )


class TestCurrencyInGrain:
    """Round-6 plugin-portability fix: currency_code in grain.

    Without it, ``total_invoice_amount`` and ``total_paid`` sum across
    currencies — meaningless on any multi-currency pod. ``saasfademo1``
    has 12 distinct currencies on AP invoices (USD/GBP/EUR/CNY/JPY/AUD/
    INR/CHF/AED/PLN/TRY/MXN as of 2026-05-10). Same lesson TC23 documented
    for gl_balance and reviewer Blocker #1 enforced for ap_aging.
    """

    def test_default_currency_col_is_canonical(self) -> None:
        assert DEFAULT_CURRENCY_COL == "ApInvoicesInvoiceCurrencyCode"

    def test_currency_code_projected_uppercased(self) -> None:
        sql = build_supplier_spend_sql()
        assert re.search(
            r"UPPER\(\s*CAST\(\s*inv\.ApInvoicesInvoiceCurrencyCode\s+AS\s+STRING\s*\)\s*\)\s+AS\s+currency_code",
            sql,
        ), "currency_code must be UPPER'd and projected as a grain key"

    def test_currency_code_in_group_by(self) -> None:
        """Without GROUP BY currency, amounts would still aggregate across
        currencies even if the column is projected. Both must be present.

        P2.19 refactor: the ``UPPER(CAST(...))`` derivation lives in the
        ``invoices`` CTE; outer SELECT and GROUP BY reference
        ``inv.currency_code``. Currency-in-grain is the invariant —
        GROUP BY must contain a currency-discriminating column.
        """
        sql = build_supplier_spend_sql()
        # The CTE projects the UPPER+CAST derivation exactly once
        assert re.search(
            r"UPPER\(\s*CAST\(\s*inv\.ApInvoicesInvoiceCurrencyCode\s+AS\s+STRING\s*\)\s*\)\s+AS\s+currency_code",
            sql,
        ), (
            "the invoices CTE must project UPPER(CAST(inv.{currency_col} AS STRING)) "
            "AS currency_code — single derivation, used by both projection and GROUP BY"
        )
        # GROUP BY references the CTE's currency_code column
        group_by_clause = sql[sql.upper().rindex("GROUP BY"):]
        assert re.search(
            r"\binv\.currency_code\b",
            group_by_clause,
        ), (
            "GROUP BY must reference inv.currency_code — without it, amounts "
            "would sum across currencies (meaningless on multi-currency tenants)"
        )

    def test_currency_col_override_threads_through(self) -> None:
        """Tenants with an aliased currency column (e.g.
        ``ApInvoicesCurrencyCode``) override the default; the override
        must reach both the SELECT projection and the GROUP BY.
        """
        sql = build_supplier_spend_sql(currency_col="ApInvoicesCurrencyCode")
        assert "ApInvoicesCurrencyCode" in sql
        assert "ApInvoicesInvoiceCurrencyCode" not in sql, (
            "override must REPLACE the default column reference, not coexist"
        )

    def test_currency_in_module_exports(self) -> None:
        assert "DEFAULT_CURRENCY_COL" in supplier_spend.__all__


class TestCurrencyDetection:
    """Plugin-portability: ``build(spark)`` must detect the currency column
    alias automatically — same contract ``ap_aging.build`` honors. A tenant
    using the ``ApInvoicesCurrencyCode`` alias instead of canonical
    ``ApInvoicesInvoiceCurrencyCode`` should NOT fail Spark analysis from
    a default-args call.
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

    def test_known_aliases_include_both_variants(self) -> None:
        assert "ApInvoicesInvoiceCurrencyCode" in KNOWN_CURRENCY_COL_ALIASES
        assert "ApInvoicesCurrencyCode"        in KNOWN_CURRENCY_COL_ALIASES

    def test_detects_canonical(self) -> None:
        spark = self._fake_spark(["ApInvoicesInvoiceCurrencyCode", "other_col"])
        assert detect_currency_col(spark) == "ApInvoicesInvoiceCurrencyCode"

    def test_detects_alias(self) -> None:
        """Tenants using only the alias variant — detect picks it up."""
        spark = self._fake_spark(["ApInvoicesCurrencyCode", "other_col"])
        assert detect_currency_col(spark) == "ApInvoicesCurrencyCode"

    def test_canonical_wins_when_both_present(self) -> None:
        spark = self._fake_spark([
            "ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode",
        ])
        assert detect_currency_col(spark) == "ApInvoicesInvoiceCurrencyCode"

    def test_returns_none_when_neither_present(self) -> None:
        """Caller (the build path) is responsible for hard-gating; the detect
        helper just reports None."""
        spark = self._fake_spark(["ApInvoicesVendorId", "ApInvoicesInvoiceAmount"])
        assert detect_currency_col(spark) is None

    def test_detect_in_module_exports(self) -> None:
        for name in ("KNOWN_CURRENCY_COL_ALIASES", "detect_currency_col"):
            assert name in supplier_spend.__all__


class TestAmountPaidCoalesce:
    """Plugin-portability / NULL-propagation fix: a group of invoices where
    every ``ApInvoicesAmountPaid`` is NULL would have produced ``total_paid =
    NULL`` without ``COALESCE`` (Spark SUM propagates NULL through CAST). Same
    pattern ap_aging and gl_balance use.
    """

    def test_amount_paid_coalesced(self) -> None:
        sql = build_supplier_spend_sql()
        assert re.search(
            r"SUM\(\s*COALESCE\(\s*CAST\(\s*inv\.ApInvoicesAmountPaid\s+AS\s+DECIMAL\(20,\s*2\)\s*\)\s*,\s*0\s*\)\s*\)",
            sql,
        ), "total_paid SUM must COALESCE NULL AmountPaid to 0 to avoid NULL propagation"

    def test_invoice_amount_also_coalesced(self) -> None:
        """Symmetric protection for total_invoice_amount — NULL invoice amounts
        are extremely rare but the COALESCE makes the aggregation robust.
        """
        sql = build_supplier_spend_sql()
        assert re.search(
            r"SUM\(\s*COALESCE\(\s*CAST\(\s*inv\.ApInvoicesInvoiceAmount\s+AS\s+DECIMAL\(20,\s*2\)\s*\)\s*,\s*0\s*\)\s*\)",
            sql,
        )


class TestPathsThreading:
    """P1.5b — tenant-aware table-path resolution."""

    def test_paths_none_matches_pre_refactor_defaults(self) -> None:
        sql = build_supplier_spend_sql()
        assert "fusion_catalog.bronze.ap_invoices"  in sql
        assert "fusion_catalog.silver.dim_supplier" in sql
        assert "fusion_catalog.gold.supplier_spend" in sql

    def test_paths_threading_replaces_catalog(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_supplier_spend_sql(paths=TablePaths(catalog="my_lake"))
        assert "my_lake.bronze.ap_invoices"  in sql
        assert "my_lake.silver.dim_supplier" in sql
        assert "my_lake.gold.supplier_spend" in sql
        assert "fusion_catalog" not in sql

    def test_explicit_table_kwarg_wins_over_paths(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
        sql = build_supplier_spend_sql(
            paths=TablePaths(catalog="my_lake"),
            bronze_invoices="explicit.bronze.X",
            silver_dim="explicit.silver.Y",
            gold_table="explicit.gold.Z",
        )
        assert "explicit.bronze.X" in sql
        assert "explicit.silver.Y" in sql
        assert "explicit.gold.Z"   in sql
        assert "my_lake" not in sql
