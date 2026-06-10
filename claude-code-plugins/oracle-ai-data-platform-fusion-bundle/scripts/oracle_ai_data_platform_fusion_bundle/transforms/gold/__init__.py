"""Gold-layer business marts (Phase 9: now empty).

The v1 modules supplier_spend, gl_balance, ap_aging were deleted in
Phase 9 (ADR-0022) — all gold marts now ship as SQL templates under
``content_packs/<pack-id>/gold/`` and dispatch via
``orchestrator.sql_runner.execute_node``.

This package directory is retained as a stable import target; the
content pack is the new authoring surface.
"""

__all__: list[str] = []
