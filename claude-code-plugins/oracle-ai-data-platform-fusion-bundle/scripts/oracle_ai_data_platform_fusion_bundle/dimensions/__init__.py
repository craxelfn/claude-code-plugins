"""Conformed dimensions package.

Phase 9 (ADR-0022) deleted the v1 dim_supplier + dim_account modules —
silver dim_supplier / dim_account now ship as SQL templates under
``content_packs/<pack-id>/silver/`` and dispatch via
``orchestrator.builtins.sql_runner.execute_node``.

The remaining module ``dim_calendar`` is the only true Python builtin
(ADR-0011): its date-math generator is parameter-driven, not table-
driven, so a SQL template can't express it.
"""

from . import dim_calendar

__all__ = ["dim_calendar"]
