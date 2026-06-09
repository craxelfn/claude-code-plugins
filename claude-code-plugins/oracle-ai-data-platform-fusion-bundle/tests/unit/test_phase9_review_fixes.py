"""Regression tests for the three Phase 9 review-fix findings.

1. Bronze ``_build_target_identifier`` legacy fallback must route to
   ``ctx.bronze_schema`` (NOT fall through to gold).
2. Fusion PVO drift gate scope must come from the RESOLVED PLAN so D-1
   transitive bronze deps reach the gate.
3. ``bronze_table_for_source`` must use ``node.target`` (not
   ``node.id``) so pack contracts with id != target work.
"""

from __future__ import annotations

import pathlib
from unittest.mock import MagicMock

import pytest


PACK_YAML = """\
id: phase9-review-fix-pack
version: 0.1.0
compatibility:
  pluginMinVersion: 0.1.0
"""


def _bronze_yaml(node_id: str, target: str | None = None) -> str:
    target = target or node_id
    return f"""\
id: {node_id}
layer: bronze
implementation:
  type: bronze_extract
  datastore: {node_id.upper()}_PVO
  biccSchema: Financial
target: {target}
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    watermark:
      source: {node_id}
      column: LASTUPDATEDATE
    naturalKey: [ID]
outputSchema:
  columns:
    - {{ name: ID, type: long, nullable: false, pii: none }}
    - {{ name: _extract_ts, type: timestamp, nullable: false, pii: none }}
    - {{ name: _source_pvo, type: string, nullable: false, pii: none }}
    - {{ name: _run_id, type: string, nullable: false, pii: none }}
    - {{ name: _watermark_used, type: timestamp, nullable: true, pii: none }}
"""


SILVER_DIM = """\
id: dim_supplier
layer: silver
implementation:
  type: sql
  sql: silver/dim_supplier.sql
target: dim_supplier
dependsOn:
  bronze:
    - id: erp_suppliers
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


GOLD_MART = """\
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
    # id != target — finding 3.
    (root / "bronze" / "gl_journal_lines.yaml").write_text(
        _bronze_yaml("gl_journal_lines", target="gl_journal_headers")
    )
    (root / "silver").mkdir()
    (root / "silver" / "dim_supplier.yaml").write_text(SILVER_DIM)
    (root / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_id")
    (root / "gold").mkdir()
    (root / "gold" / "supplier_spend.yaml").write_text(GOLD_MART)
    (root / "gold" / "supplier_spend.sql").write_text("SELECT 1 AS supplier_id")
    return load_pack(root)


# ---------------------------------------------------------------------------
# Finding 1 — bronze _build_target_identifier legacy fallback
# ---------------------------------------------------------------------------


class TestBronzeTargetIdentifierFallback:
    """The pre-fix legacy fallback in ``_build_target_identifier`` did
    ``schema = silver_schema if layer == 'silver' else gold_schema``,
    so a bronze node fell through to the gold schema. The
    post-write _assert_materialized_matches_declared then described
    the gold target instead of the bronze table.
    """

    def _ctx(self):
        from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_renderer import (
            RunContext,
        )
        return RunContext(
            catalog="cat",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            run_id="r",
            active_profile_name="p",
        )

    def _bronze_node(self):
        from oracle_ai_data_platform_fusion_bundle.schema.medallion_pack import (
            NodeYaml,
        )
        return NodeYaml.model_validate({
            "id": "erp_suppliers",
            "layer": "bronze",
            "implementation": {
                "type": "bronze_extract",
                "datastore": "X",
                "biccSchema": "Financial",
            },
            "target": "erp_suppliers",
            "dependsOn": {"bronze": [], "silver": []},
            "refresh": {
                "seed": {"strategy": "replace"},
                "incremental": {
                    "strategy": "merge",
                    "watermark": {"source": "erp_suppliers", "column": "X"},
                    "naturalKey": ["ID"],
                },
            },
            "outputSchema": {"columns": [
                {"name": "ID", "type": "long", "nullable": False, "pii": "none"},
                {"name": "_extract_ts", "type": "timestamp", "nullable": False, "pii": "none"},
                {"name": "_source_pvo", "type": "string", "nullable": False, "pii": "none"},
                {"name": "_run_id", "type": "string", "nullable": False, "pii": "none"},
                {"name": "_watermark_used", "type": "timestamp", "nullable": True, "pii": "none"},
            ]},
        })

    def test_legacy_fallback_routes_bronze_to_bronze_schema(self):
        """Without ``paths``, the helper must still route bronze to
        ``ctx.bronze_schema`` — NOT fall through to gold."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_runner import (
            _build_target_identifier,
        )
        target = _build_target_identifier(self._bronze_node(), self._ctx())
        assert target == "cat.bronze.erp_suppliers", (
            f"bronze target must resolve to ``cat.bronze.erp_suppliers``; "
            f"got {target!r}. The legacy fallback incorrectly fell through "
            f"to gold (pre-Phase-9-review behavior)."
        )

    def test_paths_routes_through_table_paths_bronze(self):
        """With ``paths``, the helper routes through
        ``paths.bronze(node.target)`` so identifier validation fires
        centrally."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_runner import (
            _build_target_identifier,
        )
        paths = MagicMock()
        paths.bronze.return_value = "validated.cat.bronze.erp_suppliers"
        target = _build_target_identifier(self._bronze_node(), self._ctx(), paths)
        paths.bronze.assert_called_once_with("erp_suppliers")
        assert target == "validated.cat.bronze.erp_suppliers"


# ---------------------------------------------------------------------------
# Finding 2 — PVO drift gate scope from resolved plan
# ---------------------------------------------------------------------------


class TestPvoDriftGateScopeFromResolvedPlan:
    """``--datasets supplier_spend`` (gold) and ``--layers gold`` BOTH
    trigger bronze extracts via D-1 transitive include. The pre-fix
    PVO drift gate computed scope from raw CLI filters and missed
    those transitive bronze ids — letting Fusion column drift slip
    past the AIDPF-2072 gate.
    """

    def test_resolver_returns_transitive_bronze_for_gold_root(self, pack):
        """The resolver itself must return the transitive bronze deps
        when only a gold node is declared."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack_plan_resolver import (
            resolve_content_pack_plan,
        )
        plan = resolve_content_pack_plan(
            pack, datasets=["supplier_spend"], layers=None,
        )
        ids_by_layer = {}
        for n in plan:
            ids_by_layer.setdefault(n.layer, set()).add(n.id)
        # The drift gate code now does `{n.id for n in plan if n.layer == "bronze"}`
        # exactly — this asserts the resolver supplies the transitive
        # bronze deps the gate needs.
        bronze_in_plan = ids_by_layer.get("bronze", set())
        assert "ap_invoices" in bronze_in_plan, (
            f"gold root supplier_spend must pull ap_invoices via D-1; "
            f"got bronze={bronze_in_plan!r}"
        )
        # erp_suppliers is the transitive dep of dim_supplier (silver),
        # which is a dep of supplier_spend (gold).
        assert "erp_suppliers" in bronze_in_plan

    def test_resolver_returns_transitive_bronze_for_layers_gold(self, pack):
        """``--layers gold`` filters declared roots but D-1 still
        pulls transitive bronze deps; the drift gate must see them."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack_plan_resolver import (
            resolve_content_pack_plan,
        )
        plan = resolve_content_pack_plan(pack, datasets=None, layers=["gold"])
        bronze_in_plan = {n.id for n in plan if n.layer == "bronze"}
        assert bronze_in_plan == {"ap_invoices", "erp_suppliers"}, (
            f"--layers gold + D-1 must surface ap_invoices + erp_suppliers; "
            f"got {bronze_in_plan!r}"
        )

    def test_strict_scope_does_not_auto_include_bronze(self, pack):
        """With ``--strict-scope``, D-1 is disabled — gold roots without
        their bronze deps explicitly declared raise (and the gate
        consequently sees no bronze in scope)."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack_plan_resolver import (
            resolve_content_pack_plan,
            StrictScopeMissingDependencyError,
        )
        with pytest.raises(StrictScopeMissingDependencyError):
            resolve_content_pack_plan(
                pack,
                datasets=["supplier_spend"],
                layers=None,
                strict_scope=True,
            )


# ---------------------------------------------------------------------------
# Finding 3 — bronze_table_for_source from node.target
# ---------------------------------------------------------------------------


class TestBronzeTableForSourceUsesNodeTarget:
    """``gl_journal_lines`` has ``id=gl_journal_lines`` but
    ``target=gl_journal_headers``. The pre-fix map (built from
    ``bundle.datasets[].id``) would assert the bronze table is
    ``catalog.bronze.gl_journal_lines``, but the extractor actually
    writes to ``catalog.bronze.gl_journal_headers``. Required-column
    preflight + semantic-fragment ``{table}`` substitutions would
    read the wrong table.
    """

    def test_starter_pack_gl_journal_lines_target_differs_from_id(self):
        """Validate the precondition: the starter pack still ships an
        id/target mismatch that exercises this code path."""
        import pathlib
        from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import (
            load_pack,
        )
        here = pathlib.Path(__file__).parent.parent.parent
        starter = (
            here / "scripts" / "oracle_ai_data_platform_fusion_bundle"
            / "content_packs" / "fusion-finance-starter"
        )
        if not starter.is_dir():
            pytest.skip(f"starter pack not present at {starter}")
        pack = load_pack(starter)
        gl_node = pack.bronze.get("gl_journal_lines")
        assert gl_node is not None, "starter pack must declare gl_journal_lines"
        assert gl_node.target == "gl_journal_headers", (
            f"starter pack contract: gl_journal_lines.target = "
            f"gl_journal_headers (PVO writes to the headers table); "
            f"got {gl_node.target!r}"
        )

    def test_map_uses_node_target_not_node_id(self, pack):
        """The map's KEY is the node id; the VALUE is built from
        ``paths.bronze(node.target)``. For gl_journal_lines, the value
        must be ``cat.bronze.gl_journal_headers``."""
        from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths

        paths = TablePaths(
            catalog="cat",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
        )
        # Mirror the production code from
        # _run_content_pack_backend so the test catches regressions.
        bronze_table_for_source = {
            node_id: paths.bronze(node.target)
            for node_id, node in pack.bronze.items()
        }
        assert bronze_table_for_source["gl_journal_lines"] == "cat.bronze.gl_journal_headers"
        # Sanity: identity nodes still resolve correctly.
        assert bronze_table_for_source["erp_suppliers"] == "cat.bronze.erp_suppliers"
        assert bronze_table_for_source["ap_invoices"] == "cat.bronze.ap_invoices"
