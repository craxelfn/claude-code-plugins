"""Unit tests for ``orchestrator/sql_runner.py``::``execute_node`` (Phase 2 Step 11).

The most important tests here lock the **render-then-gate ordering
invariant**: render runs exactly once BEFORE the plan-hash drift gate,
and a preflight/render/drift failure must never invoke Spark writes.
"""

from __future__ import annotations

import pathlib
from unittest.mock import MagicMock, patch

import pytest
import yaml

from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_renderer import (
    RunContext,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_runner import (
    AIDPF_4040_PLAN_HASH_DRIFT,
    AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT,
    NodeExecutionResult,
    _assert_materialized_matches_declared,
    execute_node,
)
from oracle_ai_data_platform_fusion_bundle.schema.tenant_profile import (
    load_tenant_profile_from_string,
)


# ---------------------------------------------------------------------------
# Fixture pack builder + helpers
# ---------------------------------------------------------------------------


PACK_YAML = """
id: phase2-runner-test
version: 1.0.0
description: Phase 2 sql_runner.execute_node test pack
compatibility:
  pluginMinVersion: 0.3.0
"""

NODE_YAML = """
id: dim_thing
layer: silver
implementation:
  type: sql
  sql: silver/dim_thing.sql
target: dim_thing
outputSchema:
  columns:
    - name: thing_id
      type: string
      nullable: false
      pii: none
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
      watermark:
        column: _extract_ts
requiredColumns:
  erp_thing:
    - SEGMENT1
    - _extract_ts
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    naturalKey: [thing_id]
    watermark:
      source: erp_thing
      column: _extract_ts
"""

PROFILE_YAML = """
schemaVersion: 1
tenant: phase2-tenant
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:abc"
resolved:
  column: {}
  semantic: {}
profile: {}
"""

SIMPLE_SQL = "SELECT 1 AS thing_id"


def _build_pack(tmp_path: pathlib.Path, sql: str = SIMPLE_SQL):
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True, exist_ok=True)
    (pack_root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    silver = pack_root / "silver"
    silver.mkdir()
    (silver / "dim_thing.yaml").write_text(NODE_YAML, encoding="utf-8")
    (silver / "dim_thing.sql").write_text(sql, encoding="utf-8")
    return load_pack(pack_root)


def _ctx(mode: str = "seed") -> RunContext:
    return RunContext(
        catalog="cat",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="run-phase2",
        active_profile_name="finance-default",
        prior_watermark={},
        mode=mode,
        bronze_table_for_source={"erp_thing": "cat.bronze.erp_thing"},
    )


def _profile():
    return load_tenant_profile_from_string(PROFILE_YAML)


def _paths() -> MagicMock:
    """MagicMock paths whose .bronze/.silver/.gold return string identifiers.

    The Phase 9 follow-up made ``paths`` REQUIRED at every
    ``_build_target_identifier`` call site (``sql_runner.py:290`` /
    ``:537`` / ``:910`` / ``:1078``), so test fixtures can no longer
    pass a bare ``MagicMock()`` — the helper would return a Mock object
    instead of a string, and downstream ``f"... FROM {target}"`` SQL
    composition would produce ``"... FROM <MagicMock name='...'>``.
    Use this helper to keep tests Spark-free while still emitting real
    identifier strings.
    """
    paths = MagicMock()
    paths.bronze.side_effect = lambda t: f"cat.bronze.{t}"
    paths.silver.side_effect = lambda t: f"cat.silver.{t}"
    paths.gold.side_effect = lambda t: f"cat.gold.{t}"
    return paths


def _fake_spark_seed_happy_path(target_row_count: int = 5) -> MagicMock:
    """Fake Spark that lets execute_node complete the full seed-mode
    happy path: preflight DESCRIBE returns required cols; CREATE OR
    REPLACE succeeds; quality DataFrame supports .count(); materialised
    schema DESCRIBE matches declared; max(watermark) returns NULL.
    """
    spark = MagicMock()

    # Default .sql(...) handler. Different statements need different
    # DataFrames; we route based on the SQL text.
    def sql_side_effect(stmt: str, *args, **kwargs):
        df = MagicMock(name="default-df")
        if "DESCRIBE TABLE cat.bronze.erp_thing" in stmt:
            # Preflight DESCRIBE returns the required columns.
            df.collect.return_value = [
                ("SEGMENT1", "string", None),
                ("_extract_ts", "timestamp", None),
            ]
            return df
        if stmt.startswith("CREATE OR REPLACE TABLE"):
            # Strategy executor — no return value needed.
            return df
        if "SELECT COUNT(*)" in stmt:
            df.collect.return_value = [(target_row_count,)]
            return df
        if "DESCRIBE TABLE" in stmt:
            # Materialised-schema assertion DESCRIBE — must match declared.
            df.collect.return_value = [
                ("thing_id", "string", None),
            ]
            return df
        if "SELECT MAX" in stmt:
            df.collect.return_value = [(None,)]
            return df
        df.collect.return_value = []
        return df

    spark.sql.side_effect = sql_side_effect

    # spark.table(target) -> target_df with .count()
    target_df = MagicMock(name="target_df")
    target_df.count.return_value = target_row_count
    spark.table.return_value = target_df

    # spark.createDataFrame for the state-row write.
    state_df = MagicMock(name="state_df")
    spark.createDataFrame.return_value = state_df

    return spark


# ---------------------------------------------------------------------------
# Happy path — seed mode end-to-end
# ---------------------------------------------------------------------------


class TestSeedHappyPath:
    def test_full_flow_returns_success(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        assert isinstance(result, NodeExecutionResult)
        assert result.status == "success", result.error_message
        assert result.row_count == 5
        assert result.plan_hash  # non-empty

    def test_success_uses_atomic_batch_state_write(self, tmp_path: pathlib.Path) -> None:
        """Exactly ONE createDataFrame call for the success state rows —
        Step 10's atomic batch contract."""
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        # Single createDataFrame call for the state-row batch.
        assert spark.createDataFrame.call_count == 1


# ---------------------------------------------------------------------------
# Render-then-gate ordering — preflight blocks BEFORE render
# ---------------------------------------------------------------------------


class TestRenderThenGateOrdering:
    """These tests lock the Step 11 ordering invariant. A preflight or
    render failure MUST happen before any Spark write occurs."""

    def test_preflight_blocked_does_not_call_renderer(self, tmp_path: pathlib.Path, monkeypatch) -> None:
        pack = _build_pack(tmp_path)
        # Preflight will fail: DESCRIBE returns a column set missing SEGMENT1.
        spark = MagicMock()

        def sql_side_effect(stmt: str, *args, **kwargs):
            df = MagicMock()
            if "DESCRIBE TABLE" in stmt:
                df.collect.return_value = [("_extract_ts", "timestamp", None)]
                # SEGMENT1 is missing -> preflight blocks.
                return df
            df.collect.return_value = []
            return df

        spark.sql.side_effect = sql_side_effect

        # Patch render_node_sql in sql_runner's namespace to detect calls.
        from oracle_ai_data_platform_fusion_bundle.orchestrator import sql_runner

        render_mock = MagicMock(side_effect=AssertionError("render must not be called"))
        monkeypatch.setattr(sql_runner, "render_node_sql", render_mock)

        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        assert result.status == "preflight_blocked"
        render_mock.assert_not_called()

    def test_render_failed_does_not_call_strategy_executor(self, tmp_path: pathlib.Path, monkeypatch) -> None:
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()

        # Patch render_node_sql to raise.
        from oracle_ai_data_platform_fusion_bundle.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_renderer import (
            SqlRendererError,
        )

        monkeypatch.setattr(
            sql_runner, "render_node_sql",
            MagicMock(side_effect=SqlRendererError("simulated render failure")),
        )

        strategy_mock = MagicMock()
        monkeypatch.setattr(sql_runner, "execute_strategy", strategy_mock)

        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        assert result.status == "render_failed"
        strategy_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Plan-hash drift gate — incremental mode only
# ---------------------------------------------------------------------------


class TestPlanHashDriftGate:
    def test_incremental_with_matching_prior_hash_proceeds(
        self, tmp_path: pathlib.Path
    ) -> None:
        """When prior_plan_hash matches the freshly-computed expected
        hash, execution proceeds."""
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        # First run: compute the expected hash to use as prior_plan_hash.
        first = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        assert first.status == "success"

        # Second run with the SAME inputs in incremental mode + matching prior_hash.
        # We need a fresh fake spark because the first run consumed its side_effects.
        spark2 = _fake_spark_seed_happy_path()
        second = execute_node(
            spark2,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("incremental"),
            paths=_paths(),
            mode="incremental",
            profile_hash="profile-h",
            prior_plan_hash=first.plan_hash,
        )
        # Drift gate didn't block — proceeds to execution.
        assert second.status == "success"

    def test_incremental_with_mismatched_prior_hash_blocks_resume(
        self, tmp_path: pathlib.Path
    ) -> None:
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()

        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("incremental"),
            paths=_paths(),
            mode="incremental",
            profile_hash="profile-h",
            prior_plan_hash="some-stale-hash-from-previous-yaml-version",
        )
        assert result.status == "resume_drift_blocked"
        assert AIDPF_4040_PLAN_HASH_DRIFT in result.error_message

    def test_seed_mode_skips_drift_gate(self, tmp_path: pathlib.Path) -> None:
        """Seed mode has no prior state to compare against — drift gate
        is skipped regardless of prior_plan_hash."""
        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
            prior_plan_hash="this-would-cause-drift-but-seed-skips",
        )
        assert result.status == "success"


# ---------------------------------------------------------------------------
# Materialised-schema assertion (Step 11 sub-step 8)
# ---------------------------------------------------------------------------


class TestMaterialisedSchemaAssertion:
    def test_matching_schema_returns_hash(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.schema.medallion_pack import NodeYaml

        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [("thing_id", "string", None)]
        spark.sql.return_value = df
        h = _assert_materialized_matches_declared(spark, "cat.silver.dim_thing", node)
        assert isinstance(h, str)
        assert len(h) == 64  # sha256 hex length

    def test_extra_materialised_column_raises_4070(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_runner import (
            MaterializedSchemaDriftError,
        )
        from oracle_ai_data_platform_fusion_bundle.schema.medallion_pack import NodeYaml

        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [
            ("thing_id", "string", None),
            ("extra_column", "string", None),
        ]
        spark.sql.return_value = df
        with pytest.raises(MaterializedSchemaDriftError) as exc_info:
            _assert_materialized_matches_declared(spark, "cat.silver.dim_thing", node)
        assert AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT in str(exc_info.value)

    def test_type_mismatch_raises_4070(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_runner import (
            MaterializedSchemaDriftError,
        )
        from oracle_ai_data_platform_fusion_bundle.schema.medallion_pack import NodeYaml

        node = NodeYaml.model_validate(yaml.safe_load(NODE_YAML))
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = [("thing_id", "bigint", None)]  # declared string
        spark.sql.return_value = df
        with pytest.raises(MaterializedSchemaDriftError):
            _assert_materialized_matches_declared(spark, "cat.silver.dim_thing", node)


# ---------------------------------------------------------------------------
# State-commit failure — preserves prior watermark
# ---------------------------------------------------------------------------


class TestStateCommitFailure:
    def test_state_commit_failure_returns_state_commit_failed(self, tmp_path: pathlib.Path) -> None:
        from oracle_ai_data_platform_fusion_bundle.orchestrator import state_phase2

        pack = _build_pack(tmp_path)
        spark = _fake_spark_seed_happy_path()
        # Make state write fail.
        spark.createDataFrame.return_value.write.format.return_value.mode.return_value.option.return_value.saveAsTable.side_effect = RuntimeError(
            "simulated state-table write failure"
        )

        result = execute_node(
            spark,
            node=pack.silver["dim_thing"],
            pack=pack,
            profile=_profile(),
            ctx=_ctx("seed"),
            paths=_paths(),
            mode="seed",
            profile_hash="profile-h",
        )
        assert result.status == "state_commit_failed"
        assert "state_commit_failed" in result.error_message
