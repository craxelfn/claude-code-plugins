"""Phase 5 — ``python_legacy`` rendered_sql_hash includes source-file SHA-256.

The plan-hash drift gate (AIDPF-4040) compares ``rendered_sql_hash``
across runs to detect changes that would silently alter the transform.
For ``type: sql`` nodes the rendered SQL text is the hash input — any
template edit flips the hash. For ``type: builtin`` (today: the
dim_calendar adapter) the plugin VERSION constant is the floor signal:
plugin upgrades move both the code and the version together, so the
version alone is a sufficient drift signal.

``type: python_legacy`` is different — the callable lives in an
EXTERNAL module (the v1 ``dimensions/*.py`` / ``transforms/gold/*.py``
files, or a future customer-shipped module) that the customer may
edit independently of the plugin / adapter. The pre-fix hash
substitute used only ``<callable_id>:<adapter_version>``, which let a
code-only legacy edit (no spec change, no adapter VERSION bump) evade
drift detection.

This test asserts the post-fix substitute incorporates a SHA-256 of
the callable's source file: editing the fixture changes the hash,
mirroring how a SQL-template edit would flip the hash on a
``type: sql`` node.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_runner import (
    _builtin_rendered_sql_hash_substitute,
    _python_legacy_rendered_sql_hash_substitute,
)


_FIXTURE_MODULE_V1 = """\
\"\"\"Fixture v1 — for python_legacy hash test.\"\"\"

def build(spark, **kwargs):
    return spark.table("fixture_v1")
"""

_FIXTURE_MODULE_V2 = """\
\"\"\"Fixture v2 — same callable name, different body.\"\"\"

def build(spark, **kwargs):
    # CHANGED: now reads from a different table.
    return spark.table("fixture_v2")
"""


@pytest.fixture
def fixture_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Create a temp module on sys.path and return ``(module_path, load_fn)``.

    ``load_fn(source_text)`` overwrites the module's source and
    re-imports it from disk so a fresh ``inspect.getsourcefile``
    points to the new bytes.
    """
    pkg_root = tmp_path / "_python_legacy_hash_fixture_pkg"
    pkg_root.mkdir()
    (pkg_root / "__init__.py").write_text("", encoding="utf-8")
    module_path = pkg_root / "fixture_mod.py"
    monkeypatch.syspath_prepend(str(tmp_path))

    def load(source_text: str):
        module_path.write_text(source_text, encoding="utf-8")
        mod_name = "_python_legacy_hash_fixture_pkg.fixture_mod"
        sys.modules.pop(mod_name, None)
        return importlib.import_module(mod_name)

    return module_path, load


class TestPythonLegacyHashIncludesSource:
    def test_source_edit_changes_hash(self, fixture_module) -> None:
        """Editing the fixture's source bytes flips the rendered_sql_hash.
        Without this, AIDPF-4040 drift detection misses code-only
        changes to v1 transforms."""
        _module_path, load = fixture_module

        v1_module = load(_FIXTURE_MODULE_V1)
        v1_hash = _python_legacy_rendered_sql_hash_substitute(
            callable_id="_python_legacy_hash_fixture_pkg.fixture_mod:build",
            version="1.0.0",
            resolved_callable=v1_module.build,
        )

        v2_module = load(_FIXTURE_MODULE_V2)
        v2_hash = _python_legacy_rendered_sql_hash_substitute(
            callable_id="_python_legacy_hash_fixture_pkg.fixture_mod:build",
            version="1.0.0",  # unchanged: prove source alone is enough
            resolved_callable=v2_module.build,
        )

        assert v1_hash != v2_hash, (
            "rendered_sql_hash did NOT flip when the legacy transform's "
            "source file changed — AIDPF-4040 would miss code-only edits "
            "to the v1 transform"
        )

    def test_same_source_same_hash(self, fixture_module) -> None:
        """Determinism check — same source + same version + same id →
        same hash on a second computation."""
        _module_path, load = fixture_module
        module = load(_FIXTURE_MODULE_V1)
        h1 = _python_legacy_rendered_sql_hash_substitute(
            callable_id="x:build",
            version="1.0.0",
            resolved_callable=module.build,
        )
        # Re-import the same source.
        module2 = load(_FIXTURE_MODULE_V1)
        h2 = _python_legacy_rendered_sql_hash_substitute(
            callable_id="x:build",
            version="1.0.0",
            resolved_callable=module2.build,
        )
        assert h1 == h2

    def test_version_bump_changes_hash(self, fixture_module) -> None:
        """VERSION constant remains a floor signal — bumping it alone
        (without source change) still flips the hash."""
        _module_path, load = fixture_module
        module = load(_FIXTURE_MODULE_V1)
        h_old = _python_legacy_rendered_sql_hash_substitute(
            callable_id="x:build",
            version="1.0.0",
            resolved_callable=module.build,
        )
        h_new = _python_legacy_rendered_sql_hash_substitute(
            callable_id="x:build",
            version="1.1.0",
            resolved_callable=module.build,
        )
        assert h_old != h_new

    def test_distinct_from_builtin_substitute(self, fixture_module) -> None:
        """Verify the python_legacy substitute does NOT produce the same
        hash as the builtin substitute for the same callable_id +
        version. Without source-file hashing they would collide."""
        _module_path, load = fixture_module
        module = load(_FIXTURE_MODULE_V1)
        legacy_hash = _python_legacy_rendered_sql_hash_substitute(
            callable_id="x:build",
            version="1.0.0",
            resolved_callable=module.build,
        )
        builtin_hash = _builtin_rendered_sql_hash_substitute("x:build", "1.0.0")
        assert legacy_hash != builtin_hash
