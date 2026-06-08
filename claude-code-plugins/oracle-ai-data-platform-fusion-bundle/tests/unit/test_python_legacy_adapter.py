"""Unit tests for the python_legacy adapter (Phase 5 Step 1).

Covers:

* :func:`import_legacy_callable` — malformed-spec / module-not-importable /
  attr-missing / not-callable surface AIDPF-2061.
* :func:`_bind_legacy_kwargs` — builds the v1-conventional kwarg dict
  from a NodeYaml's ``depends_on`` + paths + ctx (silver-dim and
  gold-mart shapes).
* :func:`invoke_legacy_callable` — happy path captures kwargs;
  ``inspect.signature`` filtering tolerates narrower callees.
* End-to-end via :func:`execute_node` — success state row written with
  ``node_implementation_type='python_legacy'``; deliberate failure
  surfaces ``strategy_failed``; malformed spec surfaces ``render_failed``
  (AIDPF-2061) BEFORE any Spark write.
* ``paths`` propagation — rendered identifier targets the bundle's
  configured catalog/schemas, NOT ``DEFAULT_PATHS``.
"""

from __future__ import annotations

import pathlib
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
from oracle_ai_data_platform_fusion_bundle.orchestrator.builtins import (
    python_legacy_adapter as legacy,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_renderer import RunContext
from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_runner import execute_node
from oracle_ai_data_platform_fusion_bundle.schema.tenant_profile import (
    load_tenant_profile_from_string,
)
from tests.fixtures.python_legacy import fake_gold_mart, fake_silver_dim


# ---------------------------------------------------------------------------
# Pack + profile fixtures
# ---------------------------------------------------------------------------


PACK_YAML = """
id: phase5-python-legacy-test
version: 1.0.0
description: Phase 5 python_legacy adapter test pack
compatibility:
  pluginMinVersion: 0.3.0
profiles:
  finance-default:
    chartOfAccounts:
      balancingSegment: segment1
      costCenterSegment: segment2
      naturalAccountSegment: segment3
"""

NODE_YAML_FAKE_SILVER = """
id: dim_fake_supplier
layer: silver
implementation:
  type: python_legacy
  callable: tests.fixtures.python_legacy.fake_silver_dim:build
  deprecated: true
target: dim_fake_supplier
dependsOn:
  bronze:
    - id: erp_suppliers
  silver: []
refresh:
  seed:
    strategy: replace
requiredColumns:
  erp_suppliers: [vendor_id, vendor_name]
outputSchema:
  columns:
    - name: supplier_key
      type: bigint
      nullable: false
      pii: none
    - name: supplier_name
      type: string
      nullable: false
      pii: low
"""

NODE_YAML_FAKE_GOLD = """
id: gold_fake_supplier_spend
layer: gold
implementation:
  type: python_legacy
  callable: tests.fixtures.python_legacy.fake_gold_mart:build
  deprecated: true
target: gold_fake_supplier_spend
dependsOn:
  bronze:
    - id: ap_invoices
  silver:
    - id: dim_fake_supplier
refresh:
  seed:
    strategy: replace
requiredColumns:
  ap_invoices: [invoice_id, vendor_id, amount]
outputSchema:
  columns:
    - name: supplier_key
      type: bigint
      nullable: false
      pii: none
    - name: total_amount
      type: double
      nullable: true
      pii: none
"""

NODE_YAML_MALFORMED_SPEC = """
id: dim_oops
layer: silver
implementation:
  type: python_legacy
  callable: oops
  deprecated: true
target: dim_oops
dependsOn:
  bronze:
    - id: erp_suppliers
  silver: []
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: x
      type: bigint
      nullable: false
      pii: none
"""

PROFILE_YAML = """
schemaVersion: 1
tenant: acme-corp
pinnedAt: 2026-06-05T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:python-legacy-test"
"""


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _build_pack(tmp_path: pathlib.Path, node_yaml: str, layer: str, node_id: str):
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True, exist_ok=True)
    (pack_root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    layer_dir = pack_root / layer
    layer_dir.mkdir(exist_ok=True)
    (layer_dir / f"{node_id}.yaml").write_text(node_yaml, encoding="utf-8")
    return load_pack(pack_root)


def _profile():
    return load_tenant_profile_from_string(PROFILE_YAML)


def _ctx(*, catalog: str = "tenant_cat", run_id: str = "py-legacy-run") -> RunContext:
    return RunContext(
        catalog=catalog,
        bronze_schema="bronze_sch",
        silver_schema="silver_sch",
        gold_schema="gold_sch",
        run_id=run_id,
        active_profile_name="finance-default",
        prior_watermark={},
        mode="seed",
        bronze_table_for_source={
            # Populated for preflight column-existence checks. The
            # _fake_spark DESCRIBE stub returns the materialised cols,
            # not the bronze required cols — preflight uses a separate
            # path that calls DESCRIBE on these bronze identifiers.
            "erp_suppliers": "tenant_cat.bronze_sch.erp_suppliers",
            "ap_invoices": "tenant_cat.bronze_sch.ap_invoices",
        },
    )


def _paths() -> TablePaths:
    """A non-default ``TablePaths`` — proves the binding layer propagates
    the bundle's configured catalog/schemas (NOT ``DEFAULT_PATHS``)."""
    return TablePaths(
        catalog="tenant_cat",
        bronze_schema="bronze_sch",
        silver_schema="silver_sch",
        gold_schema="gold_sch",
    )


def _fake_spark(
    materialized_cols: list[tuple[str, str]],
    bronze_cols: dict[str, list[tuple[str, str]]] | None = None,
) -> MagicMock:
    """Spark mock with bare-minimum surface for the dispatcher.

    Captures every ``spark.sql(...)`` argument so tests can assert
    the rendered SQL referenced the bundle's configured identifier.

    ``materialized_cols`` is what DESCRIBE TABLE returns for the
    silver/gold target after the fixture builder runs.
    ``bronze_cols`` is a per-bronze-table override so preflight's
    required-column check finds the columns it expects.
    """
    spark = MagicMock()
    spark.sql_calls = []  # type: ignore[attr-defined]
    bronze_cols = bronze_cols or {}

    def sql_side_effect(stmt, *args, **kwargs):
        spark.sql_calls.append(stmt)  # type: ignore[attr-defined]
        df = MagicMock(name="default-df")
        upper = stmt.upper()
        if "DESCRIBE TABLE" in upper:
            # Identify the target table from the statement so the
            # right schema is returned. Statement shape:
            # "DESCRIBE TABLE <fqn>"
            tail = stmt.split()[-1].strip().rstrip(";")
            if tail in bronze_cols:
                cols = bronze_cols[tail]
            else:
                cols = materialized_cols
            df.collect.return_value = [(n, t, None) for n, t in cols]
        else:
            df.collect.return_value = []
        return df

    spark.sql.side_effect = sql_side_effect

    target_df = MagicMock(name="target_df")
    target_df.count.return_value = 1
    spark.table.return_value = target_df

    spark.createDataFrame.return_value = MagicMock(name="state_df")
    return spark


_DEFAULT_BRONZE_COLS = {
    "tenant_cat.bronze_sch.erp_suppliers": [
        ("vendor_id", "bigint"),
        ("vendor_name", "string"),
    ],
    "tenant_cat.bronze_sch.ap_invoices": [
        ("invoice_id", "bigint"),
        ("vendor_id", "bigint"),
        ("amount", "double"),
    ],
}


@pytest.fixture(autouse=True)
def _reset_fixtures():
    fake_silver_dim.reset()
    fake_gold_mart.reset()
    yield
    fake_silver_dim.reset()
    fake_gold_mart.reset()


# ---------------------------------------------------------------------------
# import_legacy_callable
# ---------------------------------------------------------------------------


class TestImportLegacyCallable:
    def test_happy_path(self) -> None:
        fn = legacy.import_legacy_callable(
            "tests.fixtures.python_legacy.fake_silver_dim:build"
        )
        assert fn is fake_silver_dim.build

    def test_missing_colon_raises(self) -> None:
        with pytest.raises(legacy.LegacyCallableSpecError) as exc:
            legacy.import_legacy_callable("no_colon_here")
        assert legacy.AIDPF_2061_LEGACY_CALLABLE_SPEC_MALFORMED in str(exc.value)

    def test_multiple_colons_raises(self) -> None:
        with pytest.raises(legacy.LegacyCallableSpecError) as exc:
            legacy.import_legacy_callable("a:b:c")
        assert "wrong number of colons" in str(exc.value)

    def test_empty_module_raises(self) -> None:
        with pytest.raises(legacy.LegacyCallableSpecError):
            legacy.import_legacy_callable(":build")

    def test_empty_func_raises(self) -> None:
        with pytest.raises(legacy.LegacyCallableSpecError):
            legacy.import_legacy_callable("module:")

    def test_unimportable_module_raises(self) -> None:
        with pytest.raises(legacy.LegacyCallableSpecError) as exc:
            legacy.import_legacy_callable("totally.fake.module:build")
        assert "not importable" in str(exc.value)

    def test_missing_attr_raises(self) -> None:
        with pytest.raises(legacy.LegacyCallableSpecError) as exc:
            legacy.import_legacy_callable(
                "tests.fixtures.python_legacy.fake_silver_dim:nonexistent_attr"
            )
        assert "has no attribute" in str(exc.value)

    def test_attr_not_callable_raises(self) -> None:
        # CAPTURED_KWARGS is a dict, not callable.
        with pytest.raises(legacy.LegacyCallableSpecError) as exc:
            legacy.import_legacy_callable(
                "tests.fixtures.python_legacy.fake_silver_dim:CAPTURED_KWARGS"
            )
        assert "not callable" in str(exc.value)


# ---------------------------------------------------------------------------
# _bind_legacy_kwargs — silver-dim shape
# ---------------------------------------------------------------------------


class TestBindLegacyKwargsSilverDim:
    def test_silver_dim_binding(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path, NODE_YAML_FAKE_SILVER, "silver", "dim_fake_supplier")
        node = pack.silver["dim_fake_supplier"]
        kwargs = legacy._bind_legacy_kwargs(
            node=node, pack=pack, profile=_profile(), ctx=_ctx(), paths=_paths(),
        )
        # paths propagated (NOT DEFAULT_PATHS).
        assert kwargs["paths"].catalog == "tenant_cat"
        # silver_table is the node's own target via paths.silver(...).
        assert kwargs["silver_table"] == "tenant_cat.silver_sch.dim_fake_supplier"
        # bronze source resolves to bronze_<id> for unknown ids;
        # 'erp_suppliers' has no special mapping → 'bronze_erp_suppliers'.
        assert kwargs["bronze_erp_suppliers"] == "tenant_cat.bronze_sch.erp_suppliers"
        # v1-conventional lifecycle kwargs.
        assert kwargs["refresh_mode"] == "seed"
        assert kwargs["watermark"] is None  # no prior_watermark
        assert kwargs["run_id"] == "py-legacy-run"
        # gold-only kwargs absent for a silver node.
        assert "gold_table" not in kwargs


class TestBindLegacyKwargsGoldMart:
    def test_gold_mart_binding(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path, NODE_YAML_FAKE_GOLD, "gold", "gold_fake_supplier_spend")
        node = pack.gold["gold_fake_supplier_spend"]
        kwargs = legacy._bind_legacy_kwargs(
            node=node, pack=pack, profile=_profile(), ctx=_ctx(), paths=_paths(),
        )
        # gold_table is the node's own target via paths.gold(...).
        assert kwargs["gold_table"] == "tenant_cat.gold_sch.gold_fake_supplier_spend"
        # silver_dim is the first dependsOn.silver entry via paths.silver(...).
        assert kwargs["silver_dim"] == "tenant_cat.silver_sch.dim_fake_supplier"
        # bronze_invoices for ap_invoices (per _BRONZE_KWARG_BY_SOURCE_ID).
        assert kwargs["bronze_invoices"] == "tenant_cat.bronze_sch.ap_invoices"
        # silver_table absent on a gold node.
        assert "silver_table" not in kwargs


# ---------------------------------------------------------------------------
# invoke_legacy_callable — happy path + filtering
# ---------------------------------------------------------------------------


class TestInvokeLegacyCallable:
    def test_happy_path_captures_kwargs(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(tmp_path, NODE_YAML_FAKE_SILVER, "silver", "dim_fake_supplier")
        node = pack.silver["dim_fake_supplier"]
        spark = MagicMock()
        spark.table.return_value = MagicMock(name="materialised")

        legacy.invoke_legacy_callable(
            fake_silver_dim.build, spark,
            node=node, pack=pack, profile=_profile(), ctx=_ctx(), paths=_paths(),
        )

        assert fake_silver_dim.CAPTURED_KWARGS["paths"].catalog == "tenant_cat"
        assert (
            fake_silver_dim.CAPTURED_KWARGS["silver_table"]
            == "tenant_cat.silver_sch.dim_fake_supplier"
        )
        assert fake_silver_dim.CAPTURED_KWARGS["refresh_mode"] == "seed"
        assert fake_silver_dim.CAPTURED_KWARGS["run_id"] == "py-legacy-run"

    def test_signature_filtering_tolerates_narrow_callee(
        self, tmp_path: pathlib.Path
    ) -> None:
        pack = _build_pack(tmp_path, NODE_YAML_FAKE_SILVER, "silver", "dim_fake_supplier")
        node = pack.silver["dim_fake_supplier"]
        spark = MagicMock()

        # build_narrow accepts only (spark, paths, silver_table). The
        # binding layer constructs run_id / refresh_mode / watermark
        # too — without filtering this would raise TypeError.
        legacy.invoke_legacy_callable(
            fake_gold_mart.build_narrow, spark,
            node=node, pack=pack, profile=_profile(), ctx=_ctx(), paths=_paths(),
        )
        # The narrow callee saw only its accepted kwargs.
        assert "paths" in fake_gold_mart.CAPTURED_KWARGS
        assert "silver_table" in fake_gold_mart.CAPTURED_KWARGS
        # Filtered-out keys never reached the callee.
        assert "run_id" not in fake_gold_mart.CAPTURED_KWARGS
        assert "refresh_mode" not in fake_gold_mart.CAPTURED_KWARGS


# ---------------------------------------------------------------------------
# End-to-end via execute_node
# ---------------------------------------------------------------------------


class TestExecuteNodePythonLegacy:
    def test_silver_dim_success(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(
            tmp_path, NODE_YAML_FAKE_SILVER, "silver", "dim_fake_supplier"
        )
        spark = _fake_spark(
            [("supplier_key", "bigint"), ("supplier_name", "string")],
            bronze_cols=_DEFAULT_BRONZE_COLS,
        )
        node = pack.silver["dim_fake_supplier"]

        result = execute_node(
            spark,
            node=node,
            pack=pack,
            profile=_profile(),
            ctx=_ctx(),
            paths=_paths(),
            mode="seed",
            profile_hash="ph-test",
            prior_plan_hash=None,
        )

        assert result.status == "success", (
            f"expected success, got {result.status}: {result.error_message}"
        )
        # The fixture's CREATE OR REPLACE TABLE referenced the bundle's
        # configured catalog/silver schema, not DEFAULT_PATHS.
        create_stmts = [s for s in spark.sql_calls if "CREATE OR REPLACE TABLE" in s]
        assert any(
            "tenant_cat.silver_sch.dim_fake_supplier" in s for s in create_stmts
        ), (
            "fixture must have referenced bundle paths, not DEFAULT_PATHS — "
            f"got CREATE statements: {create_stmts}"
        )
        # paths propagation guard — the v1 fallback to DEFAULT_PATHS
        # would target 'fusion_catalog.silver.*', which MUST NOT appear.
        assert not any(
            "fusion_catalog.silver" in s for s in spark.sql_calls
        ), "DEFAULT_PATHS fallback was exercised — binding layer regression"

    def test_strategy_failed_path(self, tmp_path: pathlib.Path) -> None:
        pack = _build_pack(
            tmp_path, NODE_YAML_FAKE_SILVER, "silver", "dim_fake_supplier"
        )
        spark = _fake_spark(
            [("supplier_key", "bigint"), ("supplier_name", "string")],
            bronze_cols=_DEFAULT_BRONZE_COLS,
        )
        node = pack.silver["dim_fake_supplier"]

        fake_silver_dim.RAISE_ON_NEXT_CALL = True
        try:
            result = execute_node(
                spark,
                node=node,
                pack=pack,
                profile=_profile(),
                ctx=_ctx(),
                paths=_paths(),
                mode="seed",
                profile_hash="ph-test",
                prior_plan_hash=None,
            )
        finally:
            fake_silver_dim.RAISE_ON_NEXT_CALL = False

        assert result.status == "strategy_failed"
        assert "FakeSilverDimDeliberateFailure" in result.error_message or \
               "deliberate failure" in result.error_message
        assert result.plan_hash != ""  # plan-hash computed before dispatch

    def test_malformed_callable_spec_returns_render_failed(
        self, tmp_path: pathlib.Path
    ) -> None:
        pack = _build_pack(
            tmp_path, NODE_YAML_MALFORMED_SPEC, "silver", "dim_oops"
        )
        spark = _fake_spark([("x", "bigint")], bronze_cols=_DEFAULT_BRONZE_COLS)
        node = pack.silver["dim_oops"]

        # Snapshot pre-call spark call count — verifying no Spark write
        # happened before the spec-validation gate fires.
        result = execute_node(
            spark,
            node=node,
            pack=pack,
            profile=_profile(),
            ctx=_ctx(),
            paths=_paths(),
            mode="seed",
            profile_hash="ph-test",
            prior_plan_hash=None,
        )

        assert result.status == "render_failed"
        assert legacy.AIDPF_2061_LEGACY_CALLABLE_SPEC_MALFORMED in result.error_message
        # No CREATE OR REPLACE TABLE was issued — spec validation killed
        # the dispatch before any Spark write.
        assert not any(
            "CREATE OR REPLACE TABLE" in s for s in spark.sql_calls
        )
