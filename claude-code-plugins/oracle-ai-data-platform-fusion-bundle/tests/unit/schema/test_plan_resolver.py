"""Phase 9 (ADR-0022): schema.plan_resolver walks ResolvedPack.

Replaces the v1 tests that exercised the resolver against
``BRONZE_EXTRACT_METADATA`` / ``SILVER_DIM_METADATA`` /
``GOLD_MART_METADATA``. The resolver now consumes a ``ResolvedPack``
(loaded by the caller — ``commands/run.py`` for both inline and REST
dispatch paths) and walks ``pack.bronze ∪ pack.silver ∪ pack.gold``
plus each node's ``dependsOn`` edges.
"""

from __future__ import annotations

import pathlib

import pytest

from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
from oracle_ai_data_platform_fusion_bundle.schema.errors import (
    MissingDependencyError,
)
from oracle_ai_data_platform_fusion_bundle.schema.plan_resolver import (
    resolve_dry_run_plan,
)


PACK_YAML = """\
id: plan-resolver-test-pack
version: 0.1.0
compatibility:
  pluginMinVersion: 0.1.0
"""


def _bronze_yaml(node_id: str) -> str:
    return f"""\
id: {node_id}
layer: bronze
implementation:
  type: bronze_extract
  datastore: {node_id.upper()}_PVO
  biccSchema: Financial
target: {node_id}
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - {{ name: ID, type: long, nullable: false, pii: none }}
    - {{ name: _extract_ts, type: timestamp, nullable: false, pii: none }}
    - {{ name: _source_pvo, type: string, nullable: false, pii: none }}
    - {{ name: _run_id, type: string, nullable: false, pii: none }}
    - {{ name: _watermark_used, type: timestamp, nullable: true, pii: none }}
"""


DIM_SUPPLIER = """\
id: dim_supplier
layer: silver
implementation:
  type: sql
  sql: silver/dim_supplier.sql
target: dim_supplier
dependsOn:
  bronze:
    - id: erp_suppliers
  silver: []
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: supplier_id
      type: long
      nullable: false
      pii: none
"""


SUPPLIER_SPEND = """\
id: supplier_spend
layer: gold
implementation:
  type: sql
  sql: gold/supplier_spend.sql
target: supplier_spend
dependsOn:
  bronze:
    - id: ap_invoices
  silver:
    - id: dim_supplier
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: supplier_id
      type: long
      nullable: false
      pii: none
"""


_BUNDLE_BASE = """\
apiVersion: aidp-fusion-bundle/v1
project: plan-resolver-test
fusion:
  serviceUrl: https://example.com
  username: u
  password: p
  externalStorage: x
aidp:
  catalog: fusion_catalog
  bronzeSchema: bronze
  silverSchema: silver
  goldSchema: gold
"""


def _bundle(extra: str):
    from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle
    import yaml as _yaml
    return Bundle.model_validate(_yaml.safe_load(_BUNDLE_BASE + extra))


@pytest.fixture
def pack(tmp_path: pathlib.Path):
    from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import (
        load_pack,
    )

    root = tmp_path / "pack"
    root.mkdir()
    (root / "pack.yaml").write_text(PACK_YAML)
    (root / "bronze").mkdir()
    (root / "bronze" / "erp_suppliers.yaml").write_text(_bronze_yaml("erp_suppliers"))
    (root / "bronze" / "ap_invoices.yaml").write_text(_bronze_yaml("ap_invoices"))
    (root / "silver").mkdir()
    (root / "silver" / "dim_supplier.yaml").write_text(DIM_SUPPLIER)
    (root / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_id")
    (root / "gold").mkdir()
    (root / "gold" / "supplier_spend.yaml").write_text(SUPPLIER_SPEND)
    (root / "gold" / "supplier_spend.sql").write_text("SELECT 1 AS supplier_id")
    return load_pack(root)


@pytest.fixture
def bundle():
    return _bundle(
        """\
datasets:
  - id: erp_suppliers
  - id: ap_invoices
dimensions:
  build:
    - dim_supplier
gold:
  marts:
    - supplier_spend
"""
    )


@pytest.fixture
def paths():
    return TablePaths(
        catalog="fusion_catalog",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
    )


class TestResolveDryRunPlan:
    def test_basic_topo_sort(self, pack, bundle, paths):
        plan, prereqs = resolve_dry_run_plan(
            pack, bundle, paths, datasets=None, layers=None,
        )
        ids = [n.dataset_id for n in plan]
        assert set(ids) == {
            "erp_suppliers", "ap_invoices", "dim_supplier", "supplier_spend",
        }
        # bronze deps before silver consumers; silver before gold.
        assert ids.index("erp_suppliers") < ids.index("dim_supplier")
        assert ids.index("dim_supplier") < ids.index("supplier_spend")
        assert prereqs == ()

    def test_layers_silver_filter_creates_bronze_prereq(self, pack, bundle, paths):
        plan, prereqs = resolve_dry_run_plan(
            pack, bundle, paths, datasets=None, layers=["silver"],
        )
        plan_ids = [n.dataset_id for n in plan]
        assert plan_ids == ["dim_supplier"]
        prereq_ids = sorted(p.dataset_id for p in prereqs)
        assert prereq_ids == ["erp_suppliers"]

    def test_datasets_filter_silvers_become_prereqs(self, pack, bundle, paths):
        plan, prereqs = resolve_dry_run_plan(
            pack, bundle, paths, datasets=["supplier_spend"], layers=None,
        )
        plan_ids = [n.dataset_id for n in plan]
        assert plan_ids == ["supplier_spend"]
        prereq_ids = sorted(p.dataset_id for p in prereqs)
        assert prereq_ids == ["ap_invoices", "dim_supplier"]

    def test_unknown_dataset_in_bundle_raises(self, pack, paths):
        b = _bundle(
            """\
datasets:
  - id: totally_unknown
dimensions:
  build: []
gold:
  marts: []
"""
        )
        with pytest.raises(MissingDependencyError, match="totally_unknown"):
            resolve_dry_run_plan(pack, b, paths, datasets=None, layers=None)

    def test_typoed_datasets_filter_raises(self, pack, bundle, paths):
        with pytest.raises(MissingDependencyError, match="dim_typo"):
            resolve_dry_run_plan(
                pack, bundle, paths, datasets=["dim_typo"], layers=None,
            )

    def test_typoed_layers_filter_raises(self, pack, bundle, paths):
        with pytest.raises(MissingDependencyError, match="unknown_layer"):
            resolve_dry_run_plan(
                pack, bundle, paths, datasets=None, layers=["unknown_layer"],
            )

    def test_disabled_dataset_excluded(self, pack, paths):
        # Override Pydantic defaults — empty dimensions/gold to keep
        # the test scoped to bronze enable/disable behavior.
        b = _bundle(
            """\
datasets:
  - id: erp_suppliers
  - id: ap_invoices
    enabled: false
dimensions:
  build: []
gold:
  marts: []
"""
        )
        plan, _ = resolve_dry_run_plan(
            pack, b, paths, datasets=None, layers=None,
        )
        ids = {n.dataset_id for n in plan}
        assert "ap_invoices" not in ids
        assert "erp_suppliers" in ids

    def test_undeclared_bronze_upstream_raises(self, pack, paths):
        b = _bundle(
            """\
datasets: []
dimensions:
  build:
    - dim_supplier
gold:
  marts: []
"""
        )
        with pytest.raises(MissingDependencyError, match="erp_suppliers"):
            resolve_dry_run_plan(pack, b, paths, datasets=None, layers=None)

    def test_unknown_silver_in_bundle_raises(self, pack, paths):
        b = _bundle(
            """\
datasets: []
dimensions:
  build:
    - dim_does_not_exist
gold:
  marts: []
"""
        )
        with pytest.raises(MissingDependencyError, match="dim_does_not_exist"):
            resolve_dry_run_plan(pack, b, paths, datasets=None, layers=None)

    def test_unknown_gold_in_bundle_raises(self, pack, paths):
        b = _bundle(
            """\
datasets: []
dimensions:
  build: []
gold:
  marts:
    - mart_does_not_exist
"""
        )
        with pytest.raises(MissingDependencyError, match="mart_does_not_exist"):
            resolve_dry_run_plan(pack, b, paths, datasets=None, layers=None)

    def test_resolve_dry_run_plan_uses_custom_table_paths(self, pack, bundle):
        custom = TablePaths(
            catalog="custom_cat",
            bronze_schema="custom_bronze",
            silver_schema="custom_silver",
            gold_schema="custom_gold",
        )
        _, prereqs = resolve_dry_run_plan(
            pack, bundle, custom, datasets=["supplier_spend"], layers=None,
        )
        prereq_paths = {p.table_path for p in prereqs}
        assert any("custom_cat" in p for p in prereq_paths)
        assert any("custom_bronze" in p for p in prereq_paths)
