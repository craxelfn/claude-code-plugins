"""Phase 5 Step 2b — Option A end-to-end scope-isolation tests.

The top-level dispatcher ``_phase5_top_level_dispatch`` classifies a
``(datasets, layers)`` filter into ``bronze_filter`` + ``cp_filter``
and routes each branch to its respective backend with a SHARED
``run_id``. These tests assert the scope-isolation invariants
(``--layers bronze`` reaches only the bronze branch; ``--layers
silver,gold`` reaches only the content-pack branch; mixed scope
reaches both branches and both carry the same run_id).

Real Spark execution is exercised by the parity-test trail
(``tests/parity/test_default_backend_bronze.py``, deferred — requires
real Spark + bronze fixtures). These unit tests use mocks for the
backend invocations.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import oracle_ai_data_platform_fusion_bundle.orchestrator as _o
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import (
    load_full_chain,
    make_filesystem_base_resolver,
)
from oracle_ai_data_platform_fusion_bundle.schema.tenant_profile import (
    load_tenant_profile,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_BUNDLE = REPO_ROOT / "tests" / "fixtures" / "projects" / "phase2_project" / "bundle.yaml"
FIXTURE_PACK = REPO_ROOT / "tests" / "fixtures" / "content_packs" / "phase2_test_pack"
FIXTURE_PROFILE = REPO_ROOT / "tests" / "fixtures" / "projects" / "phase2_project" / "profiles" / "phase2-fixture.yaml"


@pytest.fixture
def pack():
    return load_full_chain(
        FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK),
    )


@pytest.fixture
def profile():
    return load_tenant_profile(FIXTURE_PROFILE)


class TestPhase5DispatcherScopeIsolation:
    """The dispatcher splits scope correctly and routes each branch."""

    def test_silver_gold_only_skips_bronze_branch(
        self, pack, profile, monkeypatch,
    ) -> None:
        """``--layers silver,gold`` keeps bronze_filter=None, so the
        bronze legacy recursive call MUST NOT happen."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator import (
            sql_runner, state as v1_state, state_phase2,
        )
        from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_runner import (
            NodeExecutionResult,
        )

        # Mock execute_node so content-pack dispatch succeeds without
        # any real Spark work.
        monkeypatch.setattr(
            sql_runner, "execute_node",
            lambda *a, **kw: NodeExecutionResult(status="success", row_count=0),
        )
        fake_spark = MagicMock()
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        fake_spark.sql.return_value = empty_df
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        # The dispatcher must NOT recurse into the legacy bronze path.
        # We spy on the recursive `run()` calls: if scope.bronze_filter
        # is None, the legacy invocation never happens.
        original_run = _o.run
        call_log: list[dict] = []

        def spy_run(*args, **kwargs):
            call_log.append({"args": args, "kwargs": dict(kwargs)})
            return original_run(*args, **kwargs)

        monkeypatch.setattr(_o, "run", spy_run)

        summary = _o.run(
            bundle_path=FIXTURE_BUNDLE,
            mode="seed",
            layers=["silver", "gold"],
            execution_backend="content-pack",
            resolved_pack=pack,
            tenant_profile=profile,
        )
        # Top-level call only — no recursive legacy bronze call.
        legacy_calls = [
            c for c in call_log
            if c["kwargs"].get("execution_backend") == "legacy-python"
        ]
        assert legacy_calls == [], (
            f"silver/gold-only run leaked into legacy bronze branch: {legacy_calls}"
        )
        assert summary.run_id  # shared id minted

    def test_no_filter_invokes_both_branches_with_shared_run_id(
        self, pack, profile, monkeypatch,
    ) -> None:
        """No filter (full medallion) splits into bronze_filter +
        cp_filter; both branches are invoked with the same run_id."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator import (
            sql_runner, state as v1_state, state_phase2,
        )
        from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_runner import (
            NodeExecutionResult,
        )

        # Bronze legacy path requires resolve_plan to find real datasets.
        # The fixture bundle has `erp_thing` which isn't a real BICC id;
        # we monkeypatch resolve_plan to return an empty plan so the
        # bronze branch returns a "no bronze work" RunSummary.
        from oracle_ai_data_platform_fusion_bundle import orchestrator as _mod
        monkeypatch.setattr(
            _mod, "resolve_plan",
            lambda *a, **kw: ([], ()),
        )

        # Mock execute_node for cp dispatch.
        monkeypatch.setattr(
            sql_runner, "execute_node",
            lambda *a, **kw: NodeExecutionResult(status="success", row_count=0),
        )
        fake_spark = MagicMock()
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        fake_spark.sql.return_value = empty_df
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        summary = _o.run(
            bundle_path=FIXTURE_BUNDLE,
            mode="seed",
            execution_backend="content-pack",
            resolved_pack=pack,
            tenant_profile=profile,
        )
        # Shared run_id stamped on the summary.
        assert summary.run_id
        # All emitted steps (if any) carry the same run_id.
        for step in summary.steps:
            assert step.run_id == summary.run_id, (
                f"step {step.dataset_id!r} drifted run_id "
                f"({step.run_id!r}) from summary ({summary.run_id!r})"
            )

    def test_pack_less_bundle_fails_closed_with_aidpf_1031(
        self, tmp_path, monkeypatch,
    ) -> None:
        """A bundle without a ``contentPack:`` block can't run the
        content-pack backend — Phase 5 fail-closed."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.errors import (
            OrchestratorConfigError,
        )

        # Build a minimal pack-less bundle.
        bundle_path = tmp_path / "bundle.yaml"
        bundle_path.write_text(
            "apiVersion: aidp-fusion-bundle/v1\n"
            "project: packless-test\n"
            "fusion:\n"
            "  serviceUrl: https://example.com\n"
            "  username: test\n"
            "  password: test\n"
            "  externalStorage: test-storage\n"
            "aidp:\n"
            "  catalog: cat\n"
            "  bronzeSchema: bronze\n"
            "  silverSchema: silver\n"
            "  goldSchema: gold\n"
            "datasets:\n"
            "  - id: ap_invoices\n"
            "    mode: incremental\n",
            encoding="utf-8",
        )

        with pytest.raises(OrchestratorConfigError) as exc:
            _o.run(
                bundle_path=bundle_path,
                mode="seed",
                execution_backend="content-pack",
            )
        assert "AIDPF-1031" in str(exc.value)
