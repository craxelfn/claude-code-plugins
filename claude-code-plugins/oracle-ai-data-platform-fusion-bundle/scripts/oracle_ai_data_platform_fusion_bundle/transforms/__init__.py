"""Bronze → silver → gold transforms (Phase 2 deliverable).

* :mod:`gold` — business marts (P1.2: ``supplier_spend``; P1.8-P1.11 pending)
* Future: ``silver`` namespace for typing/projection helpers shared across
  silver dim builds (extracted in P1.12 once duplication appears).
"""

from . import gold

__all__ = ["gold"]
