"""Unit tests for ``orchestrator/node_preflight.py`` (Phase 2 Step 7).

Tests verify the **ordering invariant**: preflight does NOT render SQL.
This is what enables Step 11's execute_node assertion that the renderer
is never invoked when preflight blocks.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import yaml

from oracle_ai_data_platform_fusion_bundle.orchestrator.node_preflight import (
    AIDPF_2042_REQUIRED_COLUMN_MISSING,
    AIDPF_2043_WATERMARK_COLUMN_MISSING,
    AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF,
    PreflightError,
    PreflightReport,
    preflight_node,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_renderer import RunContext
from oracle_ai_data_platform_fusion_bundle.schema.medallion_pack import NodeYaml


NODE_YAML_REQUIRED_COLS = """
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
    - VENDORID
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


def _load_node(yaml_text: str = NODE_YAML_REQUIRED_COLS) -> NodeYaml:
    return NodeYaml.model_validate(yaml.safe_load(yaml_text))


def _ctx() -> RunContext:
    return RunContext(
        catalog="cat",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="r",
        active_profile_name="finance-default",
        bronze_table_for_source={"erp_thing": "cat.bronze.erp_thing"},
    )


def _fake_describe_spark(columns: list[str]) -> MagicMock:
    """Fake Spark whose DESCRIBE TABLE returns Row-like tuples for ``columns``."""
    spark = MagicMock()
    df = MagicMock()
    df.collect.return_value = [(c, "string", None) for c in columns]
    spark.sql.return_value = df
    return spark


def _pack(alias_keys: tuple[str, ...] = ()) -> MagicMock:
    """Minimal ResolvedPack-shaped mock.

    Only the fields preflight reads need to be real: ``pack.column_aliases``
    is used by `_resolve_required_column_entry`'s key-existence check.
    Defaults to an empty alias map for literal-only tests.
    """
    m = MagicMock()
    m.pack.column_aliases = {k: MagicMock() for k in alias_keys}
    return m


def _profile(resolved_column: dict[str, str] | None = None) -> MagicMock:
    """Minimal TenantProfile-shaped mock with a real dict at ``resolved.column``."""
    m = MagicMock()
    m.resolved.column = dict(resolved_column or {})
    return m


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestPreflightHappyPath:
    def test_all_required_columns_present(self) -> None:
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID", "_extract_ts"])
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert report.ok
        assert report.errors == ()

    def test_returns_preflight_report(self) -> None:
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID", "_extract_ts"])
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert isinstance(report, PreflightReport)


# ---------------------------------------------------------------------------
# Required column missing
# ---------------------------------------------------------------------------


class TestRequiredColumnMissing:
    def test_missing_required_column_raises_2042(self) -> None:
        spark = _fake_describe_spark(["VENDORID", "_extract_ts"])  # SEGMENT1 missing
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert not report.ok
        codes = [e.code for e in report.errors]
        assert AIDPF_2042_REQUIRED_COLUMN_MISSING in codes
        # Message names the column.
        assert any("SEGMENT1" in e.message for e in report.errors)

    def test_required_column_pascalcase_live_uppercase_pack_passes(self) -> None:
        # Pack declares SEGMENT1 / VENDORID (UPPERCASE); live tenant
        # (saasfademo1 D1 evidence) emits PascalCase. Spark resolves the
        # SQL case-insensitively at query time, so preflight must not
        # over-reject what the engine would accept. Regression for
        # docs/v2-phase-4-live-defects.md D1 Layer A.
        spark = _fake_describe_spark(["Segment1", "VendorId", "_extract_ts"])
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert report.ok, [e.message for e in report.errors]

    def test_unknown_source_id_yields_2042(self) -> None:
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID", "_extract_ts"])
        ctx = RunContext(
            catalog="cat",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            run_id="r",
            active_profile_name="finance-default",
            bronze_table_for_source={},  # NO entry for erp_thing
        )
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=ctx)
        assert any(e.code == AIDPF_2042_REQUIRED_COLUMN_MISSING for e in report.errors)


# ---------------------------------------------------------------------------
# requiredColumns `$column.<key>` reference resolution (AIDPF-2046)
# ---------------------------------------------------------------------------


NODE_YAML_WITH_REFS = """
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
    - $column.supplier_natural_key
    - PARTYID
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


class TestRequiredColumnRefResolution:
    """B-2 layer: `$column.<key>` references resolve through pack aliases + profile."""

    def test_column_ref_resolves_through_profile(self) -> None:
        # Pack declares the alias; profile pins it to "Segment1" (PascalCase
        # — same saasfademo1 evidence). Live bronze has Segment1. Should pass.
        spark = _fake_describe_spark(["Segment1", "PARTYID", "_extract_ts"])
        node = _load_node(NODE_YAML_WITH_REFS)
        pack = _pack(alias_keys=("supplier_natural_key",))
        profile = _profile(resolved_column={"supplier_natural_key": "Segment1"})
        report = preflight_node(spark, node, pack=pack, profile=profile, ctx=_ctx())
        assert report.ok, [e.message for e in report.errors]

    def test_column_ref_resolves_with_word_different_tenant(self) -> None:
        # Hypothetical overlay-driven tenant where the natural key column is
        # actually "SupplierNumber" — not just case-different. The skill's
        # overlay adds SupplierNumber to the candidate list; bootstrap pins
        # it; preflight must follow the profile, not the pack's literal.
        spark = _fake_describe_spark(["SupplierNumber", "PARTYID", "_extract_ts"])
        node = _load_node(NODE_YAML_WITH_REFS)
        pack = _pack(alias_keys=("supplier_natural_key",))
        profile = _profile(resolved_column={"supplier_natural_key": "SupplierNumber"})
        report = preflight_node(spark, node, pack=pack, profile=profile, ctx=_ctx())
        assert report.ok, [e.message for e in report.errors]

    def test_column_ref_unknown_alias_key_raises_2046(self) -> None:
        # Pack doesn't declare the alias (typo in node YAML or stale ref).
        spark = _fake_describe_spark(["Segment1", "PARTYID", "_extract_ts"])
        node = _load_node(NODE_YAML_WITH_REFS)
        pack = _pack(alias_keys=())  # no aliases declared
        profile = _profile(resolved_column={})
        report = preflight_node(spark, node, pack=pack, profile=profile, ctx=_ctx())
        codes = [e.code for e in report.errors]
        assert AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF in codes
        # Message names the unresolved key.
        assert any("supplier_natural_key" in e.message for e in report.errors)

    def test_column_ref_alias_declared_but_profile_unpinned_raises_2046(self) -> None:
        # Bootstrap was never run (or alias was added post-bootstrap).
        spark = _fake_describe_spark(["Segment1", "PARTYID", "_extract_ts"])
        node = _load_node(NODE_YAML_WITH_REFS)
        pack = _pack(alias_keys=("supplier_natural_key",))
        profile = _profile(resolved_column={})  # alias known but unpinned
        report = preflight_node(spark, node, pack=pack, profile=profile, ctx=_ctx())
        codes = [e.code for e in report.errors]
        assert AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF in codes
        # Message hints at re-running bootstrap.
        assert any("bootstrap" in e.message for e in report.errors)

    def test_literal_entry_still_works_alongside_refs(self) -> None:
        # Backward-compat: the YAML mixes a ref ($column.X) with a literal
        # (PARTYID). Both must pass when present in live bronze.
        spark = _fake_describe_spark(["Segment1", "PARTYID", "_extract_ts"])
        node = _load_node(NODE_YAML_WITH_REFS)
        pack = _pack(alias_keys=("supplier_natural_key",))
        profile = _profile(resolved_column={"supplier_natural_key": "Segment1"})
        report = preflight_node(spark, node, pack=pack, profile=profile, ctx=_ctx())
        assert report.ok, [e.message for e in report.errors]


# ---------------------------------------------------------------------------
# Watermark column missing (AIDPF-2043)
# ---------------------------------------------------------------------------


class TestWatermarkColumnMissing:
    def test_watermark_column_absent_raises_2043(self) -> None:
        # DESCRIBE returns required cols but not _extract_ts.
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID"])
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        codes = [e.code for e in report.errors]
        assert AIDPF_2043_WATERMARK_COLUMN_MISSING in codes

    def test_watermark_column_case_insensitive(self) -> None:
        # Live bronze names the watermark column in a different case than
        # the pack literal. Spark would resolve it; preflight must too.
        # Regression for docs/v2-phase-4-live-defects.md D1 Layer A.
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID", "_Extract_Ts"])
        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert all(e.code != AIDPF_2043_WATERMARK_COLUMN_MISSING for e in report.errors), \
            [e.message for e in report.errors]


# ---------------------------------------------------------------------------
# CRITICAL: preflight never renders SQL
# ---------------------------------------------------------------------------


class TestPreflightDoesNotRender:
    """Locks the Step 11 ordering invariant: preflight runs BEFORE render,
    and a preflight failure must never trigger the renderer.

    If a future change accidentally invokes the renderer inside preflight,
    Step 11's render-then-gate ordering tests would start passing for the
    wrong reason. This test catches that regression."""

    def test_renderer_not_called_on_preflight_blocked(self, monkeypatch) -> None:
        spark = _fake_describe_spark(["VENDORID"])  # SEGMENT1 missing → preflight blocks

        # Patch the renderer so any accidental invocation raises.
        from oracle_ai_data_platform_fusion_bundle.orchestrator import sql_renderer

        renderer_mock = MagicMock(side_effect=AssertionError(
            "render_node_sql MUST NOT be called from preflight_node"
        ))
        monkeypatch.setattr(sql_renderer, "render_node_sql", renderer_mock)

        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        # Preflight blocked, renderer mock never invoked.
        assert not report.ok
        renderer_mock.assert_not_called()

    def test_renderer_not_called_on_preflight_success_either(self, monkeypatch) -> None:
        """Even on the happy path preflight doesn't render — render happens
        in execute_node Step 3, AFTER preflight returns ok."""
        spark = _fake_describe_spark(["SEGMENT1", "VENDORID", "_extract_ts"])

        from oracle_ai_data_platform_fusion_bundle.orchestrator import sql_renderer
        renderer_mock = MagicMock(side_effect=AssertionError(
            "render_node_sql MUST NOT be called from preflight_node"
        ))
        monkeypatch.setattr(sql_renderer, "render_node_sql", renderer_mock)

        report = preflight_node(spark, _load_node(), pack=_pack(), profile=_profile(), ctx=_ctx())
        assert report.ok
        renderer_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Non-merge-strategy nodes skip watermark check
# ---------------------------------------------------------------------------


class TestNonMergeStrategySkipsWatermarkCheck:
    def test_seed_only_node_skips_watermark_check(self) -> None:
        seed_only_yaml = """
id: replace_only
layer: silver
implementation:
  type: sql
  sql: silver/replace_only.sql
target: replace_only
outputSchema:
  columns:
    - name: x
      type: string
      nullable: false
      pii: none
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
refresh:
  seed:
    strategy: replace
"""
        node = _load_node(seed_only_yaml)
        spark = _fake_describe_spark(["x"])  # no _extract_ts but seed-only node doesn't need it
        report = preflight_node(spark, node, pack=_pack(), profile=_profile(), ctx=_ctx())
        assert report.ok  # No watermark check because there's no incremental.merge.
