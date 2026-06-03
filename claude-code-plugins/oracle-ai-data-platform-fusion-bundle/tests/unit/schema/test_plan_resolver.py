"""P1.5ε-fix9 — relocated TestResolvePlan cases now exercising
``schema.plan_resolver.resolve_dry_run_plan`` directly.

These cases were moved from ``tests/unit/test_orchestrator_run.py::TestResolvePlan``.
The schema-level resolver is the new source of truth for the
classification + filter + topo-sort behavior; the engine-side
``orchestrator.resolve_plan`` wrapper just reconstructs ``Spec``
instances + ``ExternalDep`` from the DTOs the resolver returns.

Mechanical adaptations vs. the original cases:
- ``orchestrator.resolve_plan(bundle, datasets, layers, paths=paths)`` →
  ``resolve_dry_run_plan(bundle, paths, datasets=datasets, layers=layers)``.
- ``BronzeExtractSpec`` / ``SilverDimSpec`` / ``GoldMartSpec`` /
  ``DeferredSpec`` assertions → ``PlanNode`` assertions
  (read ``.dataset_id`` / ``.layer`` / ``.status`` / ``.reason``).
- ``ExternalDep`` assertions → ``PrereqNode`` (same 4-field shape).

One new case lives here that isn't a relocation:
``test_resolve_dry_run_plan_uses_custom_table_paths`` — reviewer round 1
blocking lock for the ``paths: TablePaths`` signature requirement.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from oracle_ai_data_platform_fusion_bundle.schema import registry_metadata
from oracle_ai_data_platform_fusion_bundle.schema.bundle import load_bundle
from oracle_ai_data_platform_fusion_bundle.schema.errors import (
    MissingDependencyError,
)
from oracle_ai_data_platform_fusion_bundle.schema.plan_resolver import (
    resolve_dry_run_plan,
)
from oracle_ai_data_platform_fusion_bundle.schema.run_summary import (
    PlanNode,
    PrereqNode,
)


_MIN_BUNDLE = """
apiVersion: aidp-fusion-bundle/v1
project: test-plan-resolver
fusion:
  serviceUrl: https://example.com
  username: u
  password: literal-password
  externalStorage: oci://bucket@ns/path
datasets:
  - id: erp_suppliers
    mode: full
  - id: ap_invoices
    mode: full
  - id: gl_coa
    mode: full
  - id: gl_period_balances
    mode: full
dimensions:
  build:
    - dim_supplier
    - dim_account
    - dim_calendar
gold:
  marts:
    - supplier_spend
    - gl_balance
    - ap_aging
"""


def _bundle_file(tmp_path: Path, content: str = _MIN_BUNDLE) -> Path:
    p = tmp_path / "bundle.yaml"
    p.write_text(content, encoding="utf-8")
    return p


class TestResolveDryRunPlan:
    # ----------------------- happy-path topo sort -----------------------

    def test_basic_topo_sort(self, tmp_path: Path) -> None:
        bundle, paths = load_bundle(_bundle_file(tmp_path))
        plan, prereqs = resolve_dry_run_plan(
            bundle, paths, datasets=None, layers=None,
        )
        plan_names = [n.dataset_id for n in plan]
        assert plan_names.index("erp_suppliers") < plan_names.index("dim_supplier")
        assert plan_names.index("dim_supplier") < plan_names.index("supplier_spend")
        assert plan_names.index("ap_invoices") < plan_names.index("supplier_spend")
        assert plan_names.index("ap_invoices") < plan_names.index("ap_aging")
        assert prereqs == ()
        # Type check: every entry is a PlanNode
        assert all(isinstance(n, PlanNode) for n in plan)

    def test_layer_filter_creates_extra_deps(self, tmp_path: Path) -> None:
        bundle, paths = load_bundle(_bundle_file(tmp_path))
        plan, prereqs = resolve_dry_run_plan(
            bundle, paths, datasets=None, layers=["gold"],
        )
        plan_names = {n.dataset_id for n in plan}
        assert plan_names == {"supplier_spend", "gl_balance", "ap_aging"}
        dep_ids = {(d.dataset_id, d.layer) for d in prereqs}
        assert ("ap_invoices", "bronze") in dep_ids
        assert ("dim_supplier", "silver") in dep_ids
        assert ("gl_period_balances", "bronze") in dep_ids
        assert ("dim_account", "silver") in dep_ids
        assert all(isinstance(d, PrereqNode) for d in prereqs)

    def test_datasets_filter_targets_specific_names(self, tmp_path: Path) -> None:
        bundle, paths = load_bundle(_bundle_file(tmp_path))
        plan, prereqs = resolve_dry_run_plan(
            bundle, paths, datasets=["dim_supplier"], layers=None,
        )
        plan_names = [n.dataset_id for n in plan]
        assert plan_names == ["dim_supplier"]
        assert any(
            d.dataset_id == "erp_suppliers" and d.layer == "bronze"
            for d in prereqs
        )

    # ----------------------- typo / missing-name paths -----------------------

    def test_typo_in_dim_raises_missing_dependency(self, tmp_path: Path) -> None:
        bad = _MIN_BUNDLE.replace("dim_supplier", "dim_typo")
        bundle, paths = load_bundle(_bundle_file(tmp_path, bad))
        with pytest.raises(MissingDependencyError, match="dim_typo"):
            resolve_dry_run_plan(
                bundle, paths, datasets=None, layers=None,
            )

    def test_inplan_consumer_with_unknown_dependency_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        """Reviewer round 4 correction: patch
        ``schema.registry_metadata.GOLD_MART_METADATA`` (the resolver's
        upstream-walk reads here), NOT the engine-side runtime registry.
        """
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: registry-inconsistency-test
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets:
  - id: erp_suppliers
    mode: full
dimensions:
  build:
    - dim_supplier
gold:
  marts:
    - supplier_spend
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))

        real_md = registry_metadata.GOLD_MART_METADATA["supplier_spend"]
        bogus_md = registry_metadata.GoldMartMetadata(
            dataset_id=real_md.dataset_id,
            depends_on_bronze=("nonexistent_pvo",),
            depends_on_silver=real_md.depends_on_silver,
            natural_key=real_md.natural_key,
            incremental_capable=real_md.incremental_capable,
        )

        with patch.dict(
            registry_metadata.GOLD_MART_METADATA,
            {"supplier_spend": bogus_md},
        ):
            with pytest.raises(MissingDependencyError) as exc_info:
                resolve_dry_run_plan(
                    bundle, paths, datasets=None, layers=None,
                )

        msg = str(exc_info.value)
        assert "nonexistent_pvo" in msg, (
            f"MissingDependencyError must name the absent dependency; got: {msg!r}"
        )
        # Locks that the registry-consistency check fires BEFORE any
        # PrereqNode is fabricated for the bad name. If the check
        # disappeared, the upstream-walk would silently create a
        # PrereqNode for ``nonexistent_pvo`` and the run would proceed.
        for line in msg.splitlines():
            assert "PrereqNode" not in line

    def test_deferred_dim_resolves_to_plan_node(self, tmp_path: Path) -> None:
        bundle_with_deferred = _MIN_BUNDLE.replace(
            "    - dim_supplier", "    - dim_supplier\n    - dim_org",
        )
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_with_deferred))
        plan, _ = resolve_dry_run_plan(
            bundle, paths, datasets=None, layers=None,
        )
        dim_org_node = next(n for n in plan if n.dataset_id == "dim_org")
        assert dim_org_node.layer == "silver"
        assert dim_org_node.status == "deferred"
        assert dim_org_node.reason is not None
        assert "P1.7" in dim_org_node.reason

    # ----------------------- filter-typo guardrails -----------------------

    def test_typoed_datasets_filter_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        bundle, paths = load_bundle(_bundle_file(tmp_path))
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve_dry_run_plan(
                bundle, paths, datasets=["ap_invoies"], layers=None,
            )
        msg = str(exc_info.value)
        assert "ap_invoies" in msg
        assert "ap_invoices" in msg

    def test_typoed_datasets_filter_with_mixed_valid_and_invalid(
        self, tmp_path: Path,
    ) -> None:
        bundle, paths = load_bundle(_bundle_file(tmp_path))
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve_dry_run_plan(
                bundle, paths,
                datasets=["dim_supplier", "bogus_name_1", "bogus_name_2"],
                layers=None,
            )
        msg = str(exc_info.value)
        assert "bogus_name_1" in msg and "bogus_name_2" in msg

    def test_typoed_layers_filter_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        bundle, paths = load_bundle(_bundle_file(tmp_path))
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve_dry_run_plan(
                bundle, paths, datasets=None, layers=["gols"],
            )
        msg = str(exc_info.value)
        assert "gols" in msg
        for valid in ("bronze", "silver", "gold"):
            assert valid in msg

    # ----------------------- undeclared-upstream paths -----------------------

    def test_undeclared_bronze_upstream_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: fix14-undeclared-bronze
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets: []
dimensions:
  build: []
gold:
  marts: [ap_aging]
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve_dry_run_plan(
                bundle, paths, datasets=None, layers=None,
            )
        msg = str(exc_info.value)
        assert "ap_aging" in msg
        assert "ap_invoices" in msg
        assert "bundle.datasets" in msg

    def test_declared_bronze_filtered_out_becomes_prereq_node(
        self, tmp_path: Path,
    ) -> None:
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: fix14-declared-but-filtered
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets:
  - id: ap_invoices
    mode: full
dimensions:
  build: [dim_supplier, dim_calendar]
gold:
  marts: [ap_aging]
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        plan, prereqs = resolve_dry_run_plan(
            bundle, paths, datasets=None, layers=["gold"],
        )
        plan_names = {n.dataset_id for n in plan}
        assert plan_names == {"ap_aging"}
        dep_keys = {(d.dataset_id, d.layer) for d in prereqs}
        assert ("ap_invoices", "bronze") in dep_keys

    def test_multiple_undeclared_upstreams_accumulated_in_one_error(
        self, tmp_path: Path,
    ) -> None:
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: fix14-multi-undeclared
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets: []
dimensions:
  build: []
gold:
  marts: [supplier_spend]
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve_dry_run_plan(
                bundle, paths, datasets=None, layers=None,
            )
        msg = str(exc_info.value)
        assert "ap_invoices" in msg
        assert "dim_supplier" in msg
        assert "bundle.datasets" in msg
        assert "bundle.dimensions.build" in msg

    def test_undeclared_bronze_upstream_for_silver_dim_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: fix14-undeclared-bronze-for-silver-dim
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets: []
dimensions:
  build: [dim_supplier]
gold:
  marts: []
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve_dry_run_plan(
                bundle, paths, datasets=None, layers=None,
            )
        msg = str(exc_info.value)
        assert "dim_supplier" in msg
        assert "erp_suppliers" in msg
        assert "bundle.datasets" in msg

    def test_undeclared_silver_upstream_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: fix14-undeclared-silver
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets:
  - id: ap_invoices
    mode: full
  - id: erp_suppliers
    mode: full
dimensions:
  build: []
gold:
  marts: [supplier_spend]
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve_dry_run_plan(
                bundle, paths, datasets=None, layers=None,
            )
        msg = str(exc_info.value)
        assert "supplier_spend" in msg
        assert "dim_supplier" in msg
        assert "bundle.dimensions.build" in msg
        assert "bundle.datasets" not in msg

    # ----------------------- disabled-dataset paths -----------------------

    def test_disabled_dataset_excluded_from_plan(self, tmp_path: Path) -> None:
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: fix15-happy-path-disabled-excluded
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets:
  - id: erp_suppliers
  - id: ap_invoices
    enabled: false
dimensions:
  build: []
gold:
  marts: []
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        plan, prereqs = resolve_dry_run_plan(
            bundle, paths, datasets=None, layers=None,
        )
        plan_names = {n.dataset_id for n in plan}
        assert plan_names == {"erp_suppliers"}
        assert prereqs == ()
        assert not any(d.dataset_id == "ap_invoices" for d in prereqs)

    def test_disabled_dataset_with_gold_consumer_raises_disabled_specific_error(
        self, tmp_path: Path,
    ) -> None:
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: fix15-gold-consumer-disabled-bronze
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets:
  - id: erp_suppliers
  - id: ap_invoices
    enabled: false
dimensions:
  build: [dim_supplier]
gold:
  marts: [supplier_spend]
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve_dry_run_plan(
                bundle, paths, datasets=None, layers=None,
            )
        msg = str(exc_info.value)
        assert "ap_invoices" in msg
        assert "supplier_spend" in msg
        assert "disabled" in msg
        assert "enabled: true" in msg
        assert "bundle.gold.marts" in msg
        assert "add it to bundle.datasets" not in msg

    def test_disabled_dataset_with_silver_consumer_raises_disabled_specific_error(
        self, tmp_path: Path,
    ) -> None:
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: fix15-silver-consumer-disabled-bronze
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets:
  - id: erp_suppliers
    enabled: false
dimensions:
  build: [dim_supplier]
gold:
  marts: []
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve_dry_run_plan(
                bundle, paths, datasets=None, layers=None,
            )
        msg = str(exc_info.value)
        assert "erp_suppliers" in msg
        assert "dim_supplier" in msg
        assert "disabled" in msg
        assert "enabled: true" in msg
        assert "bundle.dimensions.build" in msg
        assert "bundle.gold.marts" not in msg

    def test_datasets_filter_with_disabled_id_raises_disabled_specific_error(
        self, tmp_path: Path,
    ) -> None:
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: fix15-filter-input-disabled
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets:
  - id: ap_invoices
    enabled: false
dimensions:
  build: []
gold:
  marts: []
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            resolve_dry_run_plan(
                bundle, paths, datasets=["ap_invoices"], layers=None,
            )
        msg = str(exc_info.value)
        assert "ap_invoices" in msg
        assert "disabled" in msg
        assert "enabled: true" in msg
        assert "not in the bundle plan" not in msg

    # ----------------------- reviewer round 1 blocking lock -----------------------

    def test_resolve_dry_run_plan_uses_custom_table_paths(
        self, tmp_path: Path,
    ) -> None:
        """Locks the ``paths: TablePaths`` positional requirement. With
        non-default catalog + bronze/silver schemas, every PrereqNode's
        ``table_path`` must reflect the custom values — otherwise a future
        refactor that drops the ``paths`` arg and resolves table names from
        a hardcoded default would silently break tenant-aware dispatch.
        """
        custom_bundle = """
apiVersion: aidp-fusion-bundle/v1
project: tenant-aware-paths-test
aidp:
  catalog: custom_cat
  bronzeSchema: bz_custom
  silverSchema: sv_custom
  goldSchema: gd_custom
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets:
  - id: ap_invoices
    mode: full
  - id: erp_suppliers
    mode: full
dimensions:
  build: [dim_supplier, dim_calendar]
gold:
  marts: [ap_aging]
"""
        bundle, paths = load_bundle(_bundle_file(tmp_path, custom_bundle))
        # --layers=gold filters bronze + silver out → both surface as
        # PrereqNodes with custom-prefixed table paths.
        _plan, prereqs = resolve_dry_run_plan(
            bundle, paths, datasets=None, layers=["gold"],
        )
        bronze_prereqs = [d for d in prereqs if d.layer == "bronze"]
        silver_prereqs = [d for d in prereqs if d.layer == "silver"]
        assert bronze_prereqs, "expected at least one bronze PrereqNode"
        assert silver_prereqs, "expected at least one silver PrereqNode"
        for d in bronze_prereqs:
            assert d.table_path.startswith("custom_cat.bz_custom."), (
                f"bronze prereq must use custom catalog+schema; got {d.table_path!r}"
            )
        for d in silver_prereqs:
            assert d.table_path.startswith("custom_cat.sv_custom."), (
                f"silver prereq must use custom catalog+schema; got {d.table_path!r}"
            )
