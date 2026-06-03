"""P1.5ε §Step 6b — dispatch import-boundary regression test.

Asserts the §4.3 separation: ``import dispatch`` MUST NOT pull
``orchestrator/*``, ``extractors/*``, ``dimensions/*``, or ``transforms/*``
into ``sys.modules``. Crossing this boundary means:

- The dispatch package can never accidentally couple to engine internals.
- A future MCP / Airflow dispatcher can slot in as a sibling under
  ``dispatch/`` without code surgery.
- P1.17's incremental MERGE rework can ship without touching dispatch.

Permitted schema-level imports (these are the explicit cross-boundary
modules):

- ``schema.bundle`` (AidpConfig, EnvSpec, Bundle, load_bundle)
- ``schema.errors`` (OrchestratorConfigError + cross-boundary subclasses)
- ``schema.refs`` (env-var rendering)
- ``schema.run_summary`` (RunStep, RunSummary, PlanNode, marker serializers)
"""

from __future__ import annotations

import subprocess
import sys


FORBIDDEN_PREFIXES = (
    "oracle_ai_data_platform_fusion_bundle.orchestrator",
    "oracle_ai_data_platform_fusion_bundle.extractors",
    "oracle_ai_data_platform_fusion_bundle.dimensions",
    "oracle_ai_data_platform_fusion_bundle.transforms",
)


def _modules_loaded_by(import_spec: str) -> set[str]:
    """Run a fresh Python subprocess that imports ``import_spec`` and emits
    the set of ``oracle_ai_data_platform_fusion_bundle.*`` modules in
    ``sys.modules`` afterwards. Using a subprocess guarantees we don't
    pollute the test runner's import graph (which already has the whole
    orchestrator loaded)."""
    code = (
        f"import sys\n"
        f"{import_spec}\n"
        "for m in sorted(sys.modules):\n"
        "    if m.startswith('oracle_ai_data_platform_fusion_bundle'):\n"
        "        print(m)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    return set(proc.stdout.split())


def test_import_dispatch_package_does_not_pull_engine() -> None:
    """``import dispatch`` is the entry point most consumers go through."""
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_bundle import dispatch"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, (
        f"dispatch package leaked engine imports into sys.modules: {leaks}. "
        "Check dispatch/__init__.py + submodules for stray "
        "`from ..orchestrator import` lines."
    )


def test_import_dispatch_rest_client_does_not_pull_engine() -> None:
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_bundle.dispatch.rest_client "
        "import AidpRestClient"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, f"rest_client leaked: {leaks}"


def test_import_dispatch_notebook_builder_does_not_pull_engine() -> None:
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_bundle.dispatch.notebook_builder "
        "import build_notebook"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, f"notebook_builder leaked: {leaks}"


def test_import_dispatch_preflight_does_not_pull_engine() -> None:
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_bundle.dispatch.preflight "
        "import run_local_preflight, run_remote_preflight"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, f"preflight leaked: {leaks}"


def test_import_dispatch_wheel_builder_does_not_pull_engine() -> None:
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_bundle.dispatch.wheel_builder "
        "import build_wheel"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, f"wheel_builder leaked: {leaks}"


def test_schema_imports_are_permitted() -> None:
    """Sanity check — the schema-level imports the dispatch package
    relies on are reachable from the same clean subprocess. If this test
    fails the boundary tests above are likely false-passing because the
    schema packages themselves got renamed."""
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_bundle import dispatch\n"
        "from oracle_ai_data_platform_fusion_bundle.schema import "
        "bundle, errors, refs, run_summary"
    )
    expected = {
        "oracle_ai_data_platform_fusion_bundle.schema.bundle",
        "oracle_ai_data_platform_fusion_bundle.schema.errors",
        "oracle_ai_data_platform_fusion_bundle.schema.refs",
        "oracle_ai_data_platform_fusion_bundle.schema.run_summary",
    }
    assert expected.issubset(loaded), (
        f"expected schema modules not loaded: missing={expected - loaded}"
    )
