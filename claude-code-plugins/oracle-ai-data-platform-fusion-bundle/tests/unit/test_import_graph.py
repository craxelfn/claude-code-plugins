"""P1.17d D2b — import-graph smoke test.

Catches future regressions where someone moves the explicit-column-list
MERGE clause helpers (or any other cross-module helper) back into
``orchestrator/__init__.py`` and reintroduces the circular import.

The cycle this guards against:

  orchestrator/__init__.py
    └─ imports registry
       └─ imports dim_supplier / dim_account / gl_balance
          └─ (if they import from orchestrator/__init__.py)
             ── orchestrator/__init__.py is still initializing
                ── ImportError or partial-module binding

The neutral module ``orchestrator/merge_sql.py`` breaks the cycle by
not importing from ``__init__.py`` or ``registry.py`` itself.

This test passes when:
  - ``from oracle_ai_data_platform_fusion_bundle.orchestrator import run``
    completes without ImportError.
  - Each silver/gold builder module can be imported directly.
  - The merge_sql helpers are importable from the neutral module.
"""
from __future__ import annotations


def test_orchestrator_run_imports_without_cycle() -> None:
    # The public entry point — drives the full __init__.py initialization
    # chain through registry → builders.
    from oracle_ai_data_platform_fusion_bundle.orchestrator import run  # noqa: F401


def test_silver_builders_import_standalone() -> None:
    # If a builder tries to import a P1.17d helper from
    # `orchestrator/__init__.py` (instead of `merge_sql.py`), THIS
    # import would fail at builder-module-load time with an ImportError.
    from oracle_ai_data_platform_fusion_bundle.dimensions import (  # noqa: F401
        dim_supplier as _ds,
    )
    from oracle_ai_data_platform_fusion_bundle.dimensions import (  # noqa: F401
        dim_account as _da,
    )
    from oracle_ai_data_platform_fusion_bundle.dimensions import (  # noqa: F401
        dim_calendar as _dc,
    )


def test_gold_builders_import_standalone() -> None:
    from oracle_ai_data_platform_fusion_bundle.transforms.gold import (  # noqa: F401
        gl_balance as _gl,
    )
    from oracle_ai_data_platform_fusion_bundle.transforms.gold import (  # noqa: F401
        supplier_spend as _ss,
    )
    from oracle_ai_data_platform_fusion_bundle.transforms.gold import (  # noqa: F401
        ap_aging as _ap,
    )


def test_merge_sql_helpers_importable_from_neutral_module() -> None:
    from oracle_ai_data_platform_fusion_bundle.orchestrator.merge_sql import (
        build_explicit_when_matched_clause,
        build_explicit_when_not_matched_clause,
    )

    # Smoke-call to confirm they're callable functions, not partial
    # bindings from a half-initialized module.
    assert callable(build_explicit_when_matched_clause)
    assert callable(build_explicit_when_not_matched_clause)
