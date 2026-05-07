"""Conformed dimensions (dim_supplier, dim_account, dim_calendar, dim_item, dim_org).

Phase 2 (v0.2.0) wires these one by one:

* :mod:`dim_supplier` — productized from TC8's prototype.
* :mod:`dim_calendar` — system-generated; required by ``gold.gl_balance`` and
  ``gold.po_backlog``.
* ``dim_account`` (P1.3), ``dim_item`` (P1.6) — pending.
* ``dim_org`` (P1.7) — deferred until customer HCM pod is available (P3.8).

Each module follows the same pattern:

* module-level constants for source/target table names (and defaults like
  date ranges for ``dim_calendar``)
* a pure ``build_<dim>_sql(...) -> str`` SQL builder (unit-testable)
* a ``build(spark, ...) -> DataFrame`` Spark wrapper (executes the SQL)
* optional helpers (e.g. :func:`dim_supplier.id_populated_pct`) feeding gold-layer decisions
"""

from . import dim_calendar, dim_supplier

__all__ = ["dim_calendar", "dim_supplier"]
