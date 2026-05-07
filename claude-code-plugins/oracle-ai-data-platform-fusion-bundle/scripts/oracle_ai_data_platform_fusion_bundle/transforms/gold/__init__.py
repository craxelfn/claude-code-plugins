"""Gold-layer business marts.

Each mart is a small module exporting a ``build(spark, ...) → DataFrame``
entry point and a pure ``build_<mart>_sql(...) → str`` builder for unit tests.

Phase 2 deliverables:

* :mod:`supplier_spend` — Supplier × approval-status spend mart (P1.2). First
  gold mart shipped; sets the pattern the rest copy.
* ``gl_balance`` (P1.8), ``ap_aging`` (P1.9), ``ar_aging`` (P1.10),
  ``po_backlog`` (P1.11) — pending.

Per-pod data shape varies — production pods have populated supplier
identifiers (``vendor_id`` 100%); demo pods like eseb-test have them all
NULL. Marts that join on supplier IDs use
:func:`...dimensions.dim_supplier.id_populated_pct` to pick the canonical
JOIN form (production) vs a spend-only fallback (demo).
"""

from . import supplier_spend

__all__ = ["supplier_spend"]
