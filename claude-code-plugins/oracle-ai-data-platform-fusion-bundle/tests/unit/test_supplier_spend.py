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
    SOURCE_BRONZE_TABLE,
    SOURCE_SILVER_DIM,
    TARGET_GOLD_TABLE,
    build_supplier_spend_sql,
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
        """
        sql = build_supplier_spend_sql()
        assert "LEFT JOIN" in sql, (
            "supplier_spend MUST use LEFT JOIN — INNER JOIN drops invoices for "
            "vendors missing from the dim, understating spend"
        )
        # And it must be `bronze.ap_invoices LEFT JOIN silver.dim_supplier`,
        # not the other way around — the invoice side must be the preserved (left) side.
        assert re.search(
            r"FROM\s+\S*ap_invoices\s+\w+\s+LEFT\s+JOIN\s+\S*dim_supplier",
            sql,
            flags=re.IGNORECASE,
        ), "ap_invoices must be the LEFT (preserved) side of the join"

    def test_grouping_uses_invoice_vendor_id(self) -> None:
        """The grain MUST be the invoice's claim of vendor, not the dim's.

        Grouping on `ds.vendor_id` would lose the invoice rows that didn't
        match the dim (NULL vendor_id collapse). Grouping on
        `CAST(inv.ApInvoicesVendorId AS BIGINT)` preserves them and produces
        a stable per-vendor grain regardless of dim membership.
        """
        sql = build_supplier_spend_sql()
        # The GROUP BY must include the invoice's vendor_id expression
        assert re.search(
            r"GROUP BY[\s\S]*CAST\(inv\.ApInvoicesVendorId\s+AS\s+BIGINT\)",
            sql,
        ), "GROUP BY must use CAST(inv.ApInvoicesVendorId AS BIGINT) as the vendor_id key"

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
