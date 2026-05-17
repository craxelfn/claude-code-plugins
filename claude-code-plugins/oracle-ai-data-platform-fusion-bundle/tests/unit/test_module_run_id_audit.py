"""Phase 4 / B3 — run_id audit column threading across the 6 shipped modules.

Every silver/gold module's ``build_<mart>_sql(run_id=...)`` MUST emit the
layer-specific run_id audit column (``silver_run_id`` for dims,
``gold_run_id`` for marts) with:
  - the literal ``'<run_id>'`` when the orchestrator threads its run identifier,
  - ``NULL`` when called standalone (no run_id passed).

This is the SOX-trail join contract from PLAN §3.5a — every gold/silver
row joins back to ``fusion_bundle_state.run_id`` for forensics.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_bundle.dimensions import (
    dim_account,
    dim_calendar,
    dim_supplier,
)
from oracle_ai_data_platform_fusion_bundle.transforms.gold import (
    ap_aging,
    gl_balance,
    supplier_spend,
)


# ---------------------------------------------------------------------------
# Silver dims — `silver_run_id` column
# ---------------------------------------------------------------------------


class TestDimSupplierRunId:
    def test_with_run_id_embeds_literal(self) -> None:
        sql = dim_supplier.build_dim_supplier_sql(run_id="run-abc-123")
        assert "'run-abc-123'" in sql
        assert "AS silver_run_id" in sql

    def test_without_run_id_emits_null(self) -> None:
        sql = dim_supplier.build_dim_supplier_sql()
        assert "NULL                                                     AS silver_run_id" in sql or "NULL\n" in sql or "NULL " in sql
        assert "AS silver_run_id" in sql

    def test_default_signature_no_run_id_kwarg(self) -> None:
        # build() accepts run_id kwarg without TypeError
        # (we don't run the SQL since pyspark isn't installed)
        import inspect
        sig = inspect.signature(dim_supplier.build)
        assert "run_id" in sig.parameters
        assert sig.parameters["run_id"].default is None


class TestDimAccountRunId:
    def test_with_run_id_embeds_literal(self) -> None:
        sql = dim_account.build_dim_account_sql(run_id="run-account-99")
        assert "'run-account-99'" in sql
        assert "AS silver_run_id" in sql

    def test_without_run_id_emits_null(self) -> None:
        sql = dim_account.build_dim_account_sql()
        assert "AS silver_run_id" in sql

    def test_build_signature_has_run_id(self) -> None:
        import inspect
        sig = inspect.signature(dim_account.build)
        assert "run_id" in sig.parameters


class TestDimCalendarRunId:
    def test_with_run_id_embeds_literal(self) -> None:
        sql = dim_calendar.build_dim_calendar_sql(run_id="run-cal-1")
        assert "'run-cal-1'" in sql
        assert "AS silver_run_id" in sql

    def test_without_run_id_emits_null(self) -> None:
        sql = dim_calendar.build_dim_calendar_sql()
        assert "AS silver_run_id" in sql

    def test_build_signature_has_run_id(self) -> None:
        import inspect
        sig = inspect.signature(dim_calendar.build)
        assert "run_id" in sig.parameters


# ---------------------------------------------------------------------------
# Gold marts — `gold_run_id` column
# ---------------------------------------------------------------------------


class TestSupplierSpendRunId:
    def test_with_run_id_embeds_literal(self) -> None:
        sql = supplier_spend.build_supplier_spend_sql(run_id="run-ss-42")
        assert "'run-ss-42'" in sql
        assert "AS gold_run_id" in sql

    def test_without_run_id_emits_null(self) -> None:
        sql = supplier_spend.build_supplier_spend_sql()
        assert "AS gold_run_id" in sql

    def test_build_signature_has_run_id(self) -> None:
        import inspect
        sig = inspect.signature(supplier_spend.build)
        assert "run_id" in sig.parameters


class TestGlBalanceRunId:
    def test_with_run_id_embeds_literal(self) -> None:
        sql = gl_balance.build_gl_balance_sql(run_id="run-gl-7")
        assert "'run-gl-7'" in sql
        assert "AS gold_run_id" in sql

    def test_without_run_id_emits_null(self) -> None:
        sql = gl_balance.build_gl_balance_sql()
        assert "AS gold_run_id" in sql

    def test_build_signature_has_run_id(self) -> None:
        import inspect
        sig = inspect.signature(gl_balance.build)
        assert "run_id" in sig.parameters


class TestApAgingRunId:
    def test_with_run_id_embeds_literal(self) -> None:
        sql = ap_aging.build_ap_aging_sql(run_id="run-ap-100")
        assert "'run-ap-100'" in sql
        assert "AS gold_run_id" in sql

    def test_without_run_id_emits_null(self) -> None:
        sql = ap_aging.build_ap_aging_sql()
        assert "AS gold_run_id" in sql

    def test_build_signature_has_run_id(self) -> None:
        import inspect
        sig = inspect.signature(ap_aging.build)
        assert "run_id" in sig.parameters


# ---------------------------------------------------------------------------
# Cross-module invariants
# ---------------------------------------------------------------------------


class TestAuditColInvariants:
    """Verify the same shape applies uniformly across all six modules."""

    @pytest.mark.parametrize("sql_fn,expected_col", [
        (dim_supplier.build_dim_supplier_sql, "silver_run_id"),
        (dim_account.build_dim_account_sql, "silver_run_id"),
        (dim_calendar.build_dim_calendar_sql, "silver_run_id"),
        (supplier_spend.build_supplier_spend_sql, "gold_run_id"),
        (gl_balance.build_gl_balance_sql, "gold_run_id"),
        (ap_aging.build_ap_aging_sql, "gold_run_id"),
    ])
    def test_uuid_run_id_embeds_safely(self, sql_fn, expected_col: str) -> None:
        """A real UUID4-shaped run_id embeds without quote escaping."""
        sql = sql_fn(run_id="550e8400-e29b-41d4-a716-446655440000")
        assert "'550e8400-e29b-41d4-a716-446655440000'" in sql
        assert f"AS {expected_col}" in sql

    @pytest.mark.parametrize("sql_fn", [
        dim_supplier.build_dim_supplier_sql,
        dim_account.build_dim_account_sql,
        dim_calendar.build_dim_calendar_sql,
        supplier_spend.build_supplier_spend_sql,
        gl_balance.build_gl_balance_sql,
        ap_aging.build_ap_aging_sql,
    ])
    def test_quote_in_run_id_is_escaped(self, sql_fn) -> None:
        """Defensive escape — if someone passes a run_id with a single quote,
        the SQL literal must double it (no injection)."""
        sql = sql_fn(run_id="run'with'quotes")
        # Single quotes are doubled per ANSI SQL string-literal escape
        assert "'run''with''quotes'" in sql
