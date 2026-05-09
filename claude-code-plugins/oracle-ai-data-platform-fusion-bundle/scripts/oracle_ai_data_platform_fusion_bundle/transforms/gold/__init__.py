"""Gold-layer business marts.

Each mart is a small module exporting a ``build(spark, ...) → DataFrame``
entry point and a pure ``build_<mart>_sql(...) → str`` builder for unit tests.

Phase 2 deliverables:

* :mod:`supplier_spend` — Supplier x approval-status spend mart (P1.2). First
  gold mart shipped; sets the pattern the rest copy.
* :mod:`gl_balance` — GL period balances by (account, period, ledger,
  currency) (P1.8). Single LEFT-JOIN form to ``silver.dim_account``; no
  ``dim_calendar`` join because that dim is daily-grain (period-context comes
  from the fact's ``period_year`` / ``period_num`` columns).
* ``ap_aging`` (P1.9), ``ar_aging`` (P1.10), ``po_backlog`` (P1.11) — pending.

Per-pod data shape varies — production pods have populated supplier
identifiers (``vendor_id`` 100%); demo pods like eseb-test have them all
NULL. Marts that join on supplier IDs (``supplier_spend``) use a single
**invoice-preserving LEFT JOIN** to ``silver.dim_supplier``: every
invoice dollar appears in the output regardless of dim membership, with
dim attributes pulled through where matched and NULL otherwise. The
former two-form picker (INNER JOIN vs spend-only fallback chosen by
``id_populated_pct >= 0.5``) was removed pre-PR for financial
correctness — the INNER form would silently drop invoices for vendors
missing from the dim, understating spend.
``dim_supplier.id_populated_pct`` remains as a runtime diagnostic but is
no longer load-bearing for path selection.
"""

from . import gl_balance, supplier_spend

__all__ = ["gl_balance", "supplier_spend"]
