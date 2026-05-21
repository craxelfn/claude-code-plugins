"""Unit tests for ``orchestrator.__init__`` — resolve_plan + the run loop.

Covers:
  - resolve_plan: topo-sort + extra-plan dep classification + filter behavior.
  - run(): mode validation order (Blocker-5 — zero Spark/state on mode error),
    dry_run early return, credential preflight ordering, empty-bundle path,
    cascade behavior (single failed bronze cascades to silver+gold AND
    abort-marks independent branches), failing-gold-leaf halt + abort-mark.
  - _execute_node: bronze branch (mocked extractor), deferred branch.
  - _skip_dependents + _abort_remaining: invariants.

Uses fake-Spark stubs (no PySpark dependency) — the orchestrator never
touches Spark beyond the methods stubbed below.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_bundle import orchestrator
from oracle_ai_data_platform_fusion_bundle.orchestrator import registry
from oracle_ai_data_platform_fusion_bundle.orchestrator.errors import (
    CredentialResolutionError,
    MissingDependencyError,
    PrerequisiteError,
    UnsupportedModeError,
)


# ---------------------------------------------------------------------------
# Bundle fixtures
# ---------------------------------------------------------------------------

_MIN_BUNDLE = """
apiVersion: aidp-fusion-bundle/v1
project: test-orchestrator
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


# ---------------------------------------------------------------------------
# Fake Spark — minimal stub for orchestrator interactions
# ---------------------------------------------------------------------------


class _FakeDataFrame:
    def __init__(self, row_count: int = 100) -> None:
        self._row_count = row_count
        self.write = MagicMock()
        self.write.format.return_value = self.write
        self.write.mode.return_value = self.write
        self.write.saveAsTable.return_value = None

    def count(self) -> int:
        return self._row_count

    def withColumn(self, *args, **kwargs) -> "_FakeDataFrame":
        return self


class _FakeCatalog:
    def __init__(self, existing_tables: set[str] | None = None) -> None:
        self._existing = existing_tables or set()

    def tableExists(self, path: str) -> bool:
        return path in self._existing


class _FakeSpark:
    """Just enough Spark for the orchestrator to dispatch. The run loop calls
    ``spark.table(target).count()`` on the bronze branch and
    ``state.ensure_state_table``/``write_state_row`` SQL — stubbed to no-ops.
    """

    def __init__(self, existing_tables: set[str] | None = None) -> None:
        self.catalog = _FakeCatalog(existing_tables)
        self.sql_calls: list[str] = []

    def sql(self, query: str) -> "_FakeDataFrame":
        self.sql_calls.append(query)
        return _FakeDataFrame(0)

    def table(self, name: str) -> "_FakeDataFrame":
        return _FakeDataFrame(100)


# ---------------------------------------------------------------------------
# Mode validation (§4.4c — Blocker-5 zero-side-effects)
# ---------------------------------------------------------------------------


class TestModeValidation:
    def test_mode_full_raises_before_any_io(self, tmp_path: Path) -> None:
        """`mode='full'` must raise UnsupportedModeError BEFORE load_bundle
        touches the filesystem (the load-bearing reorder assertion).

        The message must include the retired-alias hint — that's the
        operator's on-screen breadcrumb explaining the rename. And the
        error must also be a ValueError for back-compat with callers that
        catch ValueError (the P1.5α-fix6 marker pattern's multi-inheritance
        contract).
        """
        with patch("oracle_ai_data_platform_fusion_bundle.orchestrator.load_bundle") as mock_load:
            with pytest.raises(UnsupportedModeError, match="full") as exc_info:
                orchestrator.run(Path("/nonexistent/bundle.yaml"), mode="full")
            mock_load.assert_not_called()
        # Retired-alias hint must survive future message rewrites
        assert "retired" in str(exc_info.value), (
            "UnsupportedModeError message must mention 'retired' so the "
            "operator sees an on-screen breadcrumb for the rename. "
            "Don't strip the hint when rewriting the error format."
        )
        # P1.5α-fix6 marker pattern: multi-inherits ValueError for back-compat
        assert isinstance(exc_info.value, ValueError), (
            "UnsupportedModeError must also be a ValueError — callers that "
            "catch ValueError (legacy code, third-party harnesses) must "
            "still trap mode validation errors."
        )

    def test_mode_typo_raises_before_any_io(self) -> None:
        with patch("oracle_ai_data_platform_fusion_bundle.orchestrator.load_bundle") as mock_load:
            with pytest.raises(UnsupportedModeError):
                orchestrator.run(Path("/nope"), mode="seeed")
            mock_load.assert_not_called()

    def test_mode_incremental_raises_not_implemented(self) -> None:
        with patch("oracle_ai_data_platform_fusion_bundle.orchestrator.load_bundle") as mock_load:
            with pytest.raises(NotImplementedError, match="P1.5β"):
                orchestrator.run(Path("/nope"), mode="incremental")
            mock_load.assert_not_called()

    def test_mode_seed_passes_guard(self, tmp_path: Path) -> None:
        # Seed should pass the guard and proceed to load_bundle (which then fails
        # because the path doesn't exist).
        with pytest.raises(orchestrator.BundleLoadError):
            orchestrator.run(tmp_path / "nope.yaml", mode="seed")


# ---------------------------------------------------------------------------
# resolve_plan
# ---------------------------------------------------------------------------


class TestResolvePlan:
    def test_basic_topo_sort(self, tmp_path: Path) -> None:
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path))
        plan, extra_deps = orchestrator.resolve_plan(bundle, None, None, paths=paths)
        # Every bronze comes before any silver that depends on it.
        plan_names = [s.dataset_id for s in plan]
        assert plan_names.index("erp_suppliers") < plan_names.index("dim_supplier")
        assert plan_names.index("dim_supplier") < plan_names.index("supplier_spend")
        # ap_invoices is a dep of supplier_spend AND ap_aging
        assert plan_names.index("ap_invoices") < plan_names.index("supplier_spend")
        assert plan_names.index("ap_invoices") < plan_names.index("ap_aging")
        assert extra_deps == ()  # nothing filtered out

    def test_layer_filter_creates_extra_deps(self, tmp_path: Path) -> None:
        """With `layers=['gold']`, bronze + silver get filtered out; gold
        marts list their upstream as extra-plan deps for the preflight."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path))
        plan, extra_deps = orchestrator.resolve_plan(bundle, None, ["gold"], paths=paths)
        plan_names = {s.dataset_id for s in plan}
        # Only gold marts in plan
        assert plan_names == {"supplier_spend", "gl_balance", "ap_aging"}
        # Extras include the consumed bronze + silver
        dep_ids = {(d.dataset_id, d.layer) for d in extra_deps}
        assert ("ap_invoices", "bronze") in dep_ids
        assert ("dim_supplier", "silver") in dep_ids
        assert ("gl_period_balances", "bronze") in dep_ids
        assert ("dim_account", "silver") in dep_ids

    def test_datasets_filter_targets_specific_names(self, tmp_path: Path) -> None:
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path))
        plan, extra_deps = orchestrator.resolve_plan(
            bundle, ["dim_supplier"], None, paths=paths,
        )
        plan_names = [s.dataset_id for s in plan]
        assert plan_names == ["dim_supplier"]
        # erp_suppliers is a bronze dep, now extra-plan
        assert any(d.dataset_id == "erp_suppliers" and d.layer == "bronze" for d in extra_deps)

    def test_typo_in_dim_raises_missing_dependency(self, tmp_path: Path) -> None:
        """Branch A: bundle.yaml typo → unknown REQUESTED name. The resolver
        looks up ``dim_typo`` in SILVER_DIMS / KNOWN_DEFERRED_DIMS and finds
        nothing, raising MissingDependencyError at the bundle-name-→-spec
        boundary. Distinct from Branch B (registry-inconsistency,
        ``_check_dep_exists_or_raise``)."""
        bad = _MIN_BUNDLE.replace("dim_supplier", "dim_typo")
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bad))
        with pytest.raises(MissingDependencyError, match="dim_typo"):
            orchestrator.resolve_plan(bundle, None, None, paths=paths)

    def test_inplan_consumer_with_unknown_dependency_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        """Branch B (registry-inconsistency guardrail at
        ``_check_dep_exists_or_raise``, __init__.py:168): a valid in-plan
        consumer (gold mart present in bundle.gold.marts AND in GOLD_MARTS)
        whose ``depends_on_bronze`` references a name absent from
        BRONZE_EXTRACTS + KNOWN_DEFERRED_DATASETS must raise
        MissingDependencyError naming the missing dep — NOT
        PrerequisiteError (which would imply the bad reference leaked
        through to disk-state-checking) and NOT KeyError / bare ValueError
        (which would imply the check was bypassed).

        Future-proofs against a contributor adding a GoldMartSpec with a
        typo or stale name in its depends_on_bronze and getting a
        misleading error class downstream — or worse, a refactor that
        builds extras lazily and silently constructs a malformed
        ExternalDep.
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle

        # Bundle whose gold.marts references a real mart name —
        # consumer-side resolution succeeds; only the dependency is bogus.
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

        # Replace the real supplier_spend GoldMartSpec with one whose
        # depends_on_bronze points at a name NOT in BRONZE_EXTRACTS or
        # KNOWN_DEFERRED_DATASETS. dataset_id stays valid (still in
        # bundle.gold.marts and in GOLD_MARTS dict key).
        real_spec = registry.GOLD_MARTS["supplier_spend"]
        bogus_spec = registry.GoldMartSpec(
            dataset_id=real_spec.dataset_id,
            builder=real_spec.builder,
            depends_on_bronze=("nonexistent_pvo",),  # ← the bad dependency
            depends_on_silver=real_spec.depends_on_silver,
        )

        with patch.dict(
            registry.GOLD_MARTS,
            {"supplier_spend": bogus_spec},
        ):
            with pytest.raises(MissingDependencyError) as exc_info:
                orchestrator.resolve_plan(bundle, None, None, paths=paths)

        msg = str(exc_info.value)
        assert "nonexistent_pvo" in msg, (
            f"MissingDependencyError must name the absent dependency; got: {msg!r}"
        )
        # Load-bearing: must be MissingDependencyError, NOT PrerequisiteError.
        # PrerequisiteError would mean the bad reference leaked into the extras
        # list and got tableExists-checked at preflight time — the wrong error
        # class for the registry-inconsistency root cause.
        assert not isinstance(exc_info.value, PrerequisiteError), (
            "registry inconsistency must raise MissingDependencyError, "
            "NOT PrerequisiteError — preflight is for disk state, not "
            "registry coherence"
        )

    def test_deferred_dim_resolves_to_deferred_spec(self, tmp_path: Path) -> None:
        bundle_with_deferred = _MIN_BUNDLE.replace(
            "    - dim_supplier", "    - dim_supplier\n    - dim_org",
        )
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_with_deferred))
        plan, _ = orchestrator.resolve_plan(bundle, None, None, paths=paths)
        dim_org_spec = next(s for s in plan if s.dataset_id == "dim_org")
        assert isinstance(dim_org_spec, registry.DeferredSpec)
        assert dim_org_spec.layer == "silver"

    def test_typoed_datasets_filter_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        """P1.5α-fix12 — silent-empty-plan guardrail.

        A typoed ``--datasets`` value (here ``ap_invoies``, a real typo of
        ``ap_invoices``) must hard-fail with MissingDependencyError —
        not silently produce an empty plan + exit 0, which would let an
        operator believe a scoped refresh ran while no table changed.

        The error message must name the unknown filter value AND list the
        available bundle names so the operator can self-correct.
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path))

        with pytest.raises(MissingDependencyError) as exc_info:
            orchestrator.resolve_plan(
                bundle, ["ap_invoies"], None, paths=paths,
            )
        msg = str(exc_info.value)
        # Typoed name surfaced
        assert "ap_invoies" in msg, (
            f"error must name the unknown --datasets value; got: {msg!r}"
        )
        # Available names listed for self-correction (at least one real bundle
        # name appears — e.g., the actual ap_invoices is in _MIN_BUNDLE)
        assert "ap_invoices" in msg, (
            f"error must list available bundle names for self-correction; "
            f"got: {msg!r}"
        )

    def test_typoed_datasets_filter_with_mixed_valid_and_invalid(
        self, tmp_path: Path,
    ) -> None:
        """Mixed valid/invalid --datasets list: presence of even one valid
        name does NOT excuse the invalid one. All unknown names are listed.
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path))

        with pytest.raises(MissingDependencyError) as exc_info:
            orchestrator.resolve_plan(
                bundle,
                ["dim_supplier", "bogus_name_1", "bogus_name_2"],
                None,
                paths=paths,
            )
        msg = str(exc_info.value)
        assert "bogus_name_1" in msg and "bogus_name_2" in msg, (
            f"all unknown filter names must surface; got: {msg!r}"
        )

    def test_typoed_layers_filter_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        """P1.5α-fix12 — same guardrail for the layers= filter.

        A typoed ``--layers`` value (here ``gols``, typo of ``gold``) must
        hard-fail with MissingDependencyError naming the offender and
        listing the valid layer enum.
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path))

        with pytest.raises(MissingDependencyError) as exc_info:
            orchestrator.resolve_plan(
                bundle, None, ["gols"], paths=paths,
            )
        msg = str(exc_info.value)
        assert "gols" in msg, (
            f"error must name the unknown --layers value; got: {msg!r}"
        )
        # Valid layer enum surfaced for self-correction
        for valid in ("bronze", "silver", "gold"):
            assert valid in msg, (
                f"error must list valid layers; missing {valid!r} in: {msg!r}"
            )

    # ----------------------------------------------------------------------
    # P1.5α-fix14 — undeclared upstreams must raise, not silently become
    # ExternalDeps. The check distinguishes:
    #   (A) declared-but-filtered  → legitimate ExternalDep (case A preserved)
    #   (B) never declared at all  → MissingDependencyError naming the
    #       offender(s) + which bundle.yaml section to add to
    # ----------------------------------------------------------------------

    def test_undeclared_bronze_upstream_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        """Bundle declares a gold mart whose bronze upstream is missing from
        ``bundle.datasets`` → ``MissingDependencyError`` naming the consumer,
        the upstream name, AND ``bundle.datasets`` as the section to add it to.

        Distinct from ``test_typoed_datasets_filter_raises_missing_dependency``
        (P1.5α-fix12 — covers filter-input typos, not bundle omission) and
        from ``test_inplan_consumer_with_unknown_dependency_raises_missing_dependency``
        (P1.5α-fix14 fires on a name that IS in the registry but missing from
        the bundle — registry vs bundle-declaration are orthogonal contracts).
        """
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
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            orchestrator.resolve_plan(bundle, None, None, paths=paths)
        msg = str(exc_info.value)
        # Names the gold consumer
        assert "ap_aging" in msg, f"error must name consumer; got {msg!r}"
        # Names the bronze upstream that ap_aging depends on (ap_invoices)
        assert "ap_invoices" in msg, (
            f"error must name the undeclared bronze upstream; got {msg!r}"
        )
        # Remediation: tells operator WHERE in bundle.yaml to add it
        assert "bundle.datasets" in msg, (
            f"error must point at the bundle.yaml section to fix; got {msg!r}"
        )

    def test_declared_bronze_filtered_out_becomes_external_dep(
        self, tmp_path: Path,
    ) -> None:
        """Regression for case (A): operator DECLARED the upstream in
        bundle.yaml then filtered it out via --layers/--datasets. This is the
        legitimate ExternalDep path — fix14 must NOT break it.

        Bundle declares ap_invoices + ap_aging + a stub silver dim, then we
        filter to --layers=['gold']. ap_invoices stays declared but gets
        filtered out of in_plan_names → becomes a legitimate ExternalDep,
        no error raised.
        """
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
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        # --layers=['gold'] filters bronze + silver out. ap_invoices is
        # declared (case A) — must become an ExternalDep, NOT raise.
        plan, extra_deps = orchestrator.resolve_plan(
            bundle, None, ["gold"], paths=paths,
        )
        plan_names = {s.dataset_id for s in plan}
        assert plan_names == {"ap_aging"}, (
            f"only gold mart should be in plan; got {plan_names}"
        )
        dep_keys = {(d.dataset_id, d.layer) for d in extra_deps}
        assert ("ap_invoices", "bronze") in dep_keys, (
            f"declared-but-filtered ap_invoices must become ExternalDep; "
            f"got {dep_keys}"
        )

    def test_multiple_undeclared_upstreams_accumulated_in_one_error(
        self, tmp_path: Path,
    ) -> None:
        """Operator who forgot multiple upstreams shouldn't have to
        fix-rerun-fix-rerun. Single MissingDependencyError lists every
        offender. Validates the accumulate-vs-fail-fast decision documented
        in plan.md.

        Bundle: supplier_spend depends on bronze ap_invoices + silver
        dim_supplier; declare NEITHER bronze nor silver → one error names
        BOTH undeclared upstreams.
        """
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
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            orchestrator.resolve_plan(bundle, None, None, paths=paths)
        msg = str(exc_info.value)
        # Both undeclared upstreams must appear in the SAME error message
        assert "ap_invoices" in msg, (
            f"first undeclared upstream must be named; got {msg!r}"
        )
        assert "dim_supplier" in msg, (
            f"second undeclared upstream must be named; got {msg!r}"
        )
        # Each line carries the right remediation section
        assert "bundle.datasets" in msg
        assert "bundle.dimensions.build" in msg

    def test_undeclared_bronze_upstream_for_silver_dim_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        """Reviewer catch #2: ``test_undeclared_bronze_upstream_raises_missing_dependency``
        covers a GoldMartSpec.depends_on_bronze miss, but ``resolve_plan`` walks
        ``SilverDimSpec.depends_on_bronze`` at ``__init__.py:227`` SEPARATELY.
        Without this test, a future refactor that breaks ONLY the
        SilverDim → bronze branch could let ``dim_supplier``'s undeclared
        ``erp_suppliers`` upstream silently coerce into an ``ExternalDep``
        while the gold-side tests still pass.

        Bundle: declare ``dim_supplier`` in ``dimensions.build``, omit its
        bronze upstream ``erp_suppliers`` from ``datasets``. No gold marts —
        isolates the silver branch.
        """
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
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            orchestrator.resolve_plan(bundle, None, None, paths=paths)
        msg = str(exc_info.value)
        # Names the silver consumer
        assert "dim_supplier" in msg, (
            f"error must name the silver consumer; got {msg!r}"
        )
        # Names the undeclared BRONZE upstream the silver depends on
        assert "erp_suppliers" in msg, (
            f"error must name the undeclared bronze upstream of the silver dim; got {msg!r}"
        )
        # Remediation: bronze section, not silver/gold
        assert "bundle.datasets" in msg, (
            f"error must point at bundle.datasets (the bronze section); got {msg!r}"
        )

    def test_undeclared_silver_upstream_raises_missing_dependency(
        self, tmp_path: Path,
    ) -> None:
        """Reviewer catch: the bug is symmetric across SilverDimSpec.depends_on_bronze
        AND GoldMartSpec.depends_on_silver — without this test, an
        implementation could plug the bronze hole at line 213/218 but leave
        the silver path at line 222 still silently coercing dim_supplier into
        an ExternalDep against a stale Delta table.

        Bundle declares bronze deps (so the bronze undeclared check does NOT
        fire — isolating the silver-only failure), but omits the silver dim
        the gold mart depends on. supplier_spend depends on bronze
        ap_invoices + bronze erp_suppliers + silver dim_supplier; declare
        the bronzes but NOT the silver.
        """
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
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            orchestrator.resolve_plan(bundle, None, None, paths=paths)
        msg = str(exc_info.value)
        # Names the gold consumer
        assert "supplier_spend" in msg, f"error must name consumer; got {msg!r}"
        # Names the undeclared SILVER upstream
        assert "dim_supplier" in msg, (
            f"error must name the undeclared silver upstream; got {msg!r}"
        )
        # Remediation: tells operator WHERE in bundle.yaml to add it
        assert "bundle.dimensions.build" in msg, (
            f"error must point at the silver section of bundle.yaml; got {msg!r}"
        )
        # Bronze deps are declared — so the message should NOT also flag them
        # (this verifies the check only fires on truly undeclared, not on
        # the entire dependency set).
        assert "bundle.datasets" not in msg, (
            f"declared bronze deps must not appear in the error; got {msg!r}"
        )

    # ----------------------------------------------------------------------
    # P1.5α-fix15 — honor DatasetSpec.enabled=false + disabled-specific
    # wording on the consumer-upstream (fix14) AND filter-input (fix12) paths.
    # All four tests use fully-explicit minimal YAML to suppress schema
    # defaults that would otherwise inject undeclared upstreams and pollute
    # the error-message assertions.
    # ----------------------------------------------------------------------

    def test_disabled_dataset_excluded_from_plan(self, tmp_path: Path) -> None:
        """Happy path: a `enabled: false` dataset is absent from BOTH the
        plan and extra_deps (locks the contract that disabled-without-
        consumers does NOT silently become an ExternalDep).

        Explicit empty `dimensions.build` and `gold.marts` suppress the
        4-dim + 4-mart defaults — otherwise they'd inject undeclared
        consumers and crash the test for unrelated reasons.
        """
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
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        plan, extra_deps = orchestrator.resolve_plan(
            bundle, None, None, paths=paths,
        )
        plan_names = {s.dataset_id for s in plan}
        assert plan_names == {"erp_suppliers"}, (
            f"only enabled bronze must be in plan; got {plan_names}"
        )
        assert extra_deps == (), (
            f"disabled-without-consumers must not become an ExternalDep; "
            f"got extra_deps={extra_deps}"
        )
        # Defensive: disabled id absent from BOTH paths
        assert not any(d.dataset_id == "ap_invoices" for d in extra_deps), (
            "disabled bronze must NOT surface via extra_deps"
        )

    def test_disabled_dataset_with_gold_consumer_raises_disabled_specific_error(
        self, tmp_path: Path,
    ) -> None:
        """Gold-consumer path — `_BUNDLE_SECTION[gold] = bundle.gold.marts`.

        Asserts disabled-specific wording (set enabled: true; remove from
        bundle.gold.marts) is emitted instead of the generic
        "add to bundle.datasets" message (which would mislead the operator
        into adding a duplicate entry — ap_invoices IS already declared,
        just disabled).
        """
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
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            orchestrator.resolve_plan(bundle, None, None, paths=paths)
        msg = str(exc_info.value)
        # Names the disabled dep + the consumer
        assert "ap_invoices" in msg
        assert "supplier_spend" in msg
        # Disabled-specific wording present
        assert "disabled" in msg, (
            f"error must contain 'disabled' for the disabled-state path; got {msg!r}"
        )
        assert "enabled: true" in msg, (
            f"error must point at the `enabled: true` remediation; got {msg!r}"
        )
        # Correct consumer-layer section (gold → bundle.gold.marts)
        assert "bundle.gold.marts" in msg, (
            f"error must point at bundle.gold.marts for a gold consumer; got {msg!r}"
        )
        # NOT the misleading generic remediation (would send operator to add
        # a duplicate entry, since ap_invoices IS already in bundle.datasets)
        assert "add it to bundle.datasets" not in msg, (
            f"disabled wording must NOT contain misleading "
            f"'add it to bundle.datasets'; got {msg!r}"
        )

    def test_disabled_dataset_with_silver_consumer_raises_disabled_specific_error(
        self, tmp_path: Path,
    ) -> None:
        """Silver-consumer path — `_BUNDLE_SECTION[silver] = bundle.dimensions.build`.

        Without consumer_layer derivation in undeclared_deps, this test fails
        because the wrong section would be named (bundle.gold.marts for a
        silver consumer). Locks the contract that the wording uses the
        consumer's layer, not a hardcoded mapping.
        """
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
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            orchestrator.resolve_plan(bundle, None, None, paths=paths)
        msg = str(exc_info.value)
        # Names the disabled bronze dep + the silver consumer
        assert "erp_suppliers" in msg
        assert "dim_supplier" in msg
        # Disabled-specific wording
        assert "disabled" in msg
        assert "enabled: true" in msg
        # CORRECT section: silver consumer → bundle.dimensions.build,
        # NOT bundle.gold.marts (would dead-end the operator).
        assert "bundle.dimensions.build" in msg, (
            f"silver-consumer remediation must point at bundle.dimensions.build; "
            f"got {msg!r}"
        )
        assert "bundle.gold.marts" not in msg, (
            f"silver consumer must NOT be told to remove from bundle.gold.marts; "
            f"got {msg!r}"
        )

    def test_datasets_filter_with_disabled_id_raises_disabled_specific_error(
        self, tmp_path: Path,
    ) -> None:
        """Filter-input path — `--datasets ap_invoices` where ap_invoices is
        disabled in bundle.datasets. fix12's generic message would say "not
        in the bundle plan, edit bundle.yaml first" — but the entry IS in
        the bundle.

        Locks fix12's new branch that consults disabled_datasets BEFORE the
        generic unknown-dataset error.
        """
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
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        with pytest.raises(MissingDependencyError) as exc_info:
            orchestrator.resolve_plan(
                bundle, datasets=["ap_invoices"], layers=None, paths=paths,
            )
        msg = str(exc_info.value)
        # Names the disabled filter-input id
        assert "ap_invoices" in msg
        # Disabled-specific wording present
        assert "disabled" in msg, (
            f"error must contain 'disabled' on the filter-input path; got {msg!r}"
        )
        assert "enabled: true" in msg, (
            f"error must point at `enabled: true` remediation; got {msg!r}"
        )
        # Generic remediation must NOT be the ONLY message — would send the
        # operator to "edit bundle.yaml" which they already did (it's disabled).
        # We allow "not in the bundle plan" to appear if there's ALSO a truly-
        # unknown name in the filter, but here there's only the disabled one.
        assert "not in the bundle plan" not in msg, (
            f"pure-disabled filter must NOT use the generic "
            f"'not in the bundle plan' wording; got {msg!r}"
        )


# ---------------------------------------------------------------------------
# run() — dry_run + empty bundle + credential preflight ordering
# ---------------------------------------------------------------------------


class TestRunDryRun:
    def test_dry_run_returns_plan_and_prereqs_without_spark(self, tmp_path: Path) -> None:
        """dry_run=True returns RunSummary.empty(...) with plan+prereqs;
        never touches Spark or credentials."""
        with patch("oracle_ai_data_platform_fusion_bundle.orchestrator._bootstrap_spark") as mock_spark, \
             patch("oracle_ai_data_platform_fusion_bundle.orchestrator._resolve_password") as mock_cred:
            summary = orchestrator.run(_bundle_file(tmp_path), mode="seed", dry_run=True)
        mock_spark.assert_not_called()
        mock_cred.assert_not_called()
        assert summary.steps == ()
        assert summary.plan is not None and len(summary.plan) > 0
        assert summary.run_id.startswith("empty-")


class TestRunCredentialPreflight:
    def test_credential_failure_exits_before_spark(self, tmp_path: Path) -> None:
        """Blocker-5: _resolve_password runs BEFORE _bootstrap_spark.
        Vault failure surfaces as CredentialResolutionError with zero
        Spark / state calls."""
        bad = _MIN_BUNDLE.replace("literal-password", "${vault:ocid1.bogus}")
        fp = _bundle_file(tmp_path, bad)
        with patch("oracle_ai_data_platform_fusion_bundle.orchestrator._bootstrap_spark") as mock_spark, \
             patch("oracle_ai_data_platform_fusion_bundle.orchestrator.state.ensure_state_table") as mock_state:
            # Stub aidputils.secrets.get to raise
            import sys
            fake_module = type(sys)("aidputils")
            fake_secrets = type(sys)("aidputils.secrets")
            def _raise(ocid: str) -> str:
                raise RuntimeError("403 vault denied")
            fake_secrets.get = _raise
            fake_module.secrets = fake_secrets
            with patch.dict(sys.modules, {"aidputils": fake_module, "aidputils.secrets": fake_secrets}):
                with pytest.raises(CredentialResolutionError):
                    orchestrator.run(fp, mode="seed")
        # The load-bearing assertion: zero Spark + zero state calls
        mock_spark.assert_not_called()
        mock_state.assert_not_called()


class TestRunEmptyBundle:
    def test_empty_plan_returns_empty_summary(self, tmp_path: Path) -> None:
        """Bundle with no datasets/dims/marts produces an empty RunSummary."""
        empty_bundle = """
apiVersion: aidp-fusion-bundle/v1
project: empty
fusion:
  serviceUrl: https://x
  username: u
  password: p
  externalStorage: oci://b@n/p
datasets: []
dimensions:
  build: []
gold:
  marts: []
"""
        with patch("oracle_ai_data_platform_fusion_bundle.orchestrator._bootstrap_spark") as mock_spark:
            summary = orchestrator.run(_bundle_file(tmp_path, empty_bundle), mode="seed")
        assert summary.steps == ()
        assert summary.run_id.startswith("empty-")
        mock_spark.assert_not_called()


# ---------------------------------------------------------------------------
# Layer-filter preflight (P1.5α-fix4)
# ---------------------------------------------------------------------------


class TestLayerFilterPreflight:
    """Run-loop integration of ``layers=`` filter + extra-plan preflight.

    Exercises the full ``orchestrator.run(..., layers=['gold'])`` path:
    ``resolve_plan`` classifies bronze/silver as extra-plan deps,
    ``_preflight_external_deps`` checks each via
    ``spark.catalog.tableExists``, and the dispatch loop runs only the
    gold marts. The two branches (all deps present / one missing) get
    separate tests so a regression in either branch is identifiable
    from the failure line alone.
    """

    def test_layers_gold_with_prereqs_present_dispatches_only_gold(
        self, tmp_path: Path,
    ) -> None:
        """Happy path: when all bronze+silver tables exist on disk, a
        ``layers=['gold']`` run preflight-passes and dispatches only the
        gold marts. Bronze/silver builders are NEVER called — that's the
        iterating-on-gold-SQL workflow the layer-filter contract promises.
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle

        bundle_path = _bundle_file(tmp_path)  # _MIN_BUNDLE: 4 bronze + 3 silver + 3 gold
        # Seed the fake catalog with every extra-plan dep table path the
        # preflight will check. resolve_plan is the source of truth for
        # that list; query it first, then build the fake spark from it.
        bundle_obj, paths = load_bundle(bundle_path)
        _, extra_deps = orchestrator.resolve_plan(
            bundle_obj, None, ["gold"], paths=paths,
        )
        existing = {d.table_path for d in extra_deps}
        fake_spark = _FakeSpark(existing_tables=existing)

        bronze_calls: list[str] = []
        silver_calls: list[str] = []
        gold_calls: list[str] = []

        def fake_extract(spark, pvo, **kwargs):
            bronze_calls.append(pvo.id)
            return _FakeDataFrame(10)

        def fake_silver_builder(spark, **kwargs):
            silver_calls.append("silver")
            return _FakeDataFrame(5)

        def fake_gold_builder(spark, **kwargs):
            gold_calls.append("gold")
            return _FakeDataFrame(3)

        def fake_enrich(df, *, source_pvo, run_id, watermark):
            return df

        with patch(
            "oracle_ai_data_platform_fusion_bundle.extractors.bicc.extract_pvo",
            side_effect=fake_extract,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.enrich_bronze_audit_cols",
            side_effect=fake_enrich,
        ), patch.dict(
            registry.SILVER_DIMS,
            {k: type(v)(v.dataset_id, fake_silver_builder, v.depends_on_bronze) for k, v in registry.SILVER_DIMS.items()},
        ), patch.dict(
            registry.GOLD_MARTS,
            {k: type(v)(v.dataset_id, fake_gold_builder, v.depends_on_bronze, v.depends_on_silver) for k, v in registry.GOLD_MARTS.items()},
        ):
            summary = orchestrator.run(
                bundle_path, spark=fake_spark, mode="seed", layers=["gold"],
            )

        # Only gold marts in the RunSummary
        plan_ids = {s.dataset_id for s in summary.steps}
        assert plan_ids == {"supplier_spend", "gl_balance", "ap_aging"}, (
            f"layers=['gold'] must dispatch only gold marts; got {plan_ids}"
        )
        assert all(s.status == "success" for s in summary.steps)

        # Load-bearing: bronze + silver builders NEVER invoked
        assert bronze_calls == [], (
            f"bronze extractor must not be called under layers=['gold']; "
            f"got {bronze_calls}"
        )
        assert silver_calls == [], (
            f"silver builder must not be called under layers=['gold']; "
            f"got {silver_calls}"
        )
        # Exactly 3 gold builders called (one per mart)
        assert len(gold_calls) == 3, (
            f"all 3 gold marts must dispatch; got {len(gold_calls)} calls"
        )

    def test_layers_gold_with_missing_prereq_raises_prerequisite_error(
        self, tmp_path: Path,
    ) -> None:
        """Failure path: when an extra-plan dep table doesn't exist on disk,
        ``_preflight_external_deps`` raises ``PrerequisiteError`` with the
        missing table list + redirect hint. NO module dispatch happens —
        same zero-side-effects contract as the credential/mode/state-table
        failures.
        """
        bundle_path = _bundle_file(tmp_path)
        # Empty catalog — every tableExists() returns False, so the first
        # dep check fails and preflight raises.
        fake_spark = _FakeSpark()

        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator._execute_node",
        ) as mock_execute:
            with pytest.raises(PrerequisiteError) as exc_info:
                orchestrator.run(
                    bundle_path, spark=fake_spark, mode="seed", layers=["gold"],
                )
        # Zero dispatch attempts
        mock_execute.assert_not_called()

        msg = str(exc_info.value)
        assert "Extra-plan dependencies missing on disk" in msg, (
            f"message must lead with the contract statement; got: {msg!r}"
        )
        # At least one missing bronze dep must surface by name
        assert any(
            bronze_id in msg
            for bronze_id in ("ap_invoices", "gl_period_balances", "erp_suppliers")
        ), f"message must name at least one missing bronze dep; got: {msg!r}"
        # Redirect hint — operator needs a way out
        assert "--datasets" in msg or "--layers" in msg, (
            f"message must include redirect hint pointing at the CLI knob; "
            f"got: {msg!r}"
        )


# ---------------------------------------------------------------------------
# Cascade + abort-remaining (Option B audit-completeness)
# ---------------------------------------------------------------------------


class TestRunCascadeAndAbort:
    def test_failed_bronze_cascades_to_skipped_silver_and_gold(
        self, tmp_path: Path,
    ) -> None:
        """ap_invoices fails → dim_supplier + supplier_spend + ap_aging
        cascade-skipped; gl_* branch abort-skipped (no failure
        cascade-relationship, but the run halted)."""
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: cascade-test
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
  - id: gl_coa
    mode: full
  - id: gl_period_balances
    mode: full
dimensions:
  build:
    - dim_supplier
    - dim_account
gold:
  marts:
    - supplier_spend
    - ap_aging
    - gl_balance
"""
        fake_spark = _FakeSpark()
        # Stub extract_pvo to raise only for ap_invoices
        def fake_extract(spark, pvo, **kwargs):
            if pvo.id == "ap_invoices":
                raise RuntimeError("BICC 503")
            return _FakeDataFrame(10)
        # Stub builder to return a fake DF
        def fake_builder(spark, **kwargs):
            return _FakeDataFrame(5)

        # Mock enrich_bronze_audit_cols to a no-op since pyspark is not
        # installed locally (the real function imports pyspark.sql.functions).
        # The cascade test cares about step status + skip_reason, not the
        # actual audit-column shape.
        def fake_enrich(df, *, source_pvo, run_id, watermark):
            return df

        # P1.5α-fix3: verify the run loop persists state through the SOFT
        # wrapper (_safe_write_state_row), not directly via state.write_state_row.
        # ``wraps=`` lets the wrapper delegate to the (also-patched)
        # state.write_state_row so we can count both layers independently.
        from oracle_ai_data_platform_fusion_bundle.orchestrator import runtime as runtime_mod

        with patch("oracle_ai_data_platform_fusion_bundle.extractors.bicc.extract_pvo", side_effect=fake_extract), \
             patch("oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas"), \
             patch("oracle_ai_data_platform_fusion_bundle.orchestrator.enrich_bronze_audit_cols", side_effect=fake_enrich), \
             patch("oracle_ai_data_platform_fusion_bundle.orchestrator.state.write_state_row") as mock_write_state_row, \
             patch(
                 "oracle_ai_data_platform_fusion_bundle.orchestrator._safe_write_state_row",
                 wraps=runtime_mod._safe_write_state_row,
             ) as mock_safe_write, \
             patch.dict(
                 registry.SILVER_DIMS,
                 {k: type(v)(v.dataset_id, fake_builder, v.depends_on_bronze) for k, v in registry.SILVER_DIMS.items()},
             ), \
             patch.dict(
                 registry.GOLD_MARTS,
                 {k: type(v)(v.dataset_id, fake_builder, v.depends_on_bronze, v.depends_on_silver) for k, v in registry.GOLD_MARTS.items()},
             ):
            summary = orchestrator.run(_bundle_file(tmp_path, bundle_yaml), spark=fake_spark, mode="seed")

        # Every plan node has one row (audit-completeness)
        step_ids = {s.dataset_id for s in summary.steps}
        assert step_ids == {
            "ap_invoices", "erp_suppliers", "gl_coa", "gl_period_balances",
            "dim_supplier", "dim_account",
            "supplier_spend", "ap_aging", "gl_balance",
        }
        # ap_invoices failed
        ap_step = next(s for s in summary.steps if s.dataset_id == "ap_invoices")
        assert ap_step.status == "failed"
        assert "BICC 503" in ap_step.error_message  # type: ignore[operator]
        # Direct transitive descendants of ap_invoices → cascade-skipped.
        # supplier_spend depends_on_bronze=ap_invoices; ap_aging depends_on_bronze=ap_invoices.
        # dim_supplier does NOT depend on ap_invoices (it depends on erp_suppliers),
        # so it's classified as aborted, not cascade.
        for cascade_id in ("supplier_spend", "ap_aging"):
            s = next(s for s in summary.steps if s.dataset_id == cascade_id)
            assert s.status == "skipped", f"{cascade_id} should be skipped"
            assert s.skip_reason == "cascade", f"{cascade_id} should be cascade-skipped, got {s.skip_reason}"
        # Independent-branch nodes get abort-marked (gl_* chain + dim_supplier
        # which depends on erp_suppliers, not ap_invoices). The exact set
        # depends on topo-sort order — assert the predicate, not specific names.
        for abort_id in ("gl_balance",):
            s = next(s for s in summary.steps if s.dataset_id == abort_id)
            assert s.status == "skipped"
            assert s.skip_reason == "aborted", f"{abort_id} should be abort-skipped, got {s.skip_reason}"
        # Audit-completeness: every status is one of success / failed / skipped
        # and every plan node has exactly one row.
        assert sum(1 for s in summary.steps if s.status == "failed") == 1
        assert all(s.status in {"success", "failed", "skipped"} for s in summary.steps)

        # P1.5α-fix3: the run loop persists every step through the SOFT wrapper.
        # Calling state.write_state_row directly would bypass the WARN-on-failure
        # contract and re-introduce halt-on-transient-flake.
        assert mock_safe_write.call_count == len(summary.steps), (
            f"_safe_write_state_row must be called once per RunStep; got "
            f"{mock_safe_write.call_count} calls for {len(summary.steps)} steps"
        )
        # All wrapper calls succeeded under happy-path mocks → the underlying
        # state.write_state_row was called the same number of times.
        assert mock_write_state_row.call_count == len(summary.steps), (
            f"when wrapper succeeds, the underlying state.write_state_row "
            f"should be called the same number of times; got "
            f"{mock_write_state_row.call_count} for {len(summary.steps)} steps"
        )


# ---------------------------------------------------------------------------
# State-table failure semantics (P1.5α-fix3)
# ---------------------------------------------------------------------------


class TestStateWriteFailureSemantics:
    """Two-layer state-table contract:
      - Layer 1 (HARD): ``state.ensure_state_table`` failure halts the run
        BEFORE any module dispatch.
      - Layer 2 (SOFT): per-step ``state.write_state_row`` failures get
        WARN-logged via ``_safe_write_state_row`` and the loop continues.
    """

    def test_state_write_failure_logged_and_continues(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """SOFT per-step write: one row raising does NOT abort the run.
        ``_safe_write_state_row`` catches, logs WARN with the four required
        fields (dataset_id, layer, status, exc), returns False, and the loop
        continues. The in-memory ``RunStep`` sequence is unaffected because
        cascade decisions read ``step.status``, never the persisted row.
        """
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: state-write-soft-test
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
  marts: []
"""
        fake_spark = _FakeSpark()

        def fake_extract(spark, pvo, **kwargs):
            return _FakeDataFrame(10)

        def fake_builder(spark, **kwargs):
            return _FakeDataFrame(5)

        def fake_enrich(df, *, source_pvo, run_id, watermark):
            return df

        # state.write_state_row raises on the SECOND call (i.e. mid-run), not
        # the first — verifies that earlier writes succeed AND later writes
        # are still attempted regardless of the mid-run failure.
        write_calls: list[str] = []

        def flaky_write(spark, paths, step):
            write_calls.append(step.dataset_id)
            if len(write_calls) == 2:
                raise OSError("transient Delta write failure")

        with caplog.at_level(
            logging.WARNING,
            logger="oracle_ai_data_platform_fusion_bundle.orchestrator.runtime",
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.extractors.bicc.extract_pvo",
            side_effect=fake_extract,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.enrich_bronze_audit_cols",
            side_effect=fake_enrich,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.state.write_state_row",
            side_effect=flaky_write,
        ), patch.dict(
            registry.SILVER_DIMS,
            {k: type(v)(v.dataset_id, fake_builder, v.depends_on_bronze) for k, v in registry.SILVER_DIMS.items()},
        ):
            summary = orchestrator.run(
                _bundle_file(tmp_path, bundle_yaml), spark=fake_spark, mode="seed",
            )

        # The loop completed: both steps in the in-memory summary
        assert {s.dataset_id for s in summary.steps} == {"erp_suppliers", "dim_supplier"}
        assert all(s.status == "success" for s in summary.steps), (
            "cascade reads step.status, not state-table — both should still be 'success'"
        )

        # write_state_row was attempted for EVERY step (the wrapper retries
        # each row independently, never gives up after one failure)
        assert len(write_calls) == len(summary.steps), (
            f"_safe_write_state_row must attempt every per-step write; "
            f"got {len(write_calls)} attempts for {len(summary.steps)} steps"
        )

        # Exactly one WARN log emitted with the four required fields
        warn_records = [
            r for r in caplog.records
            if r.levelno == logging.WARNING and "state-write failed" in r.getMessage()
        ]
        assert len(warn_records) == 1, (
            f"expected exactly 1 WARN, got {len(warn_records)}; messages: "
            f"{[r.getMessage() for r in warn_records]}"
        )
        msg = warn_records[0].getMessage()
        for required_field in ("dataset_id=", "layer=", "status=", "exc="):
            assert required_field in msg, (
                f"WARN log missing required field {required_field!r}; got: {msg!r}"
            )
        assert "transient Delta write failure" in msg, (
            "WARN must surface the underlying exception for operator triage"
        )

    def test_ensure_state_table_failure_halts_run_before_dispatch(
        self, tmp_path: Path,
    ) -> None:
        """HARD ``ensure_state_table``: structural failures (wrong catalog,
        missing schema, DDL/DML grant misconfig, vault OCID unreachable for
        Delta-path credentials) halt the run BEFORE any module dispatch —
        no bronze extract burns Fusion-side load on a tenant whose state
        table is structurally inaccessible.
        """
        fake_spark = _FakeSpark()

        # Patch _execute_node to detect any dispatch attempt — the test
        # fails loud if even one node was attempted before the
        # ensure_state_table PermissionError propagated.
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.state.ensure_state_table",
            side_effect=PermissionError("Delta DDL denied: aidp.bronzeSchema misconfig"),
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator._execute_node",
        ) as mock_execute:
            with pytest.raises(PermissionError, match="Delta DDL denied"):
                orchestrator.run(
                    _bundle_file(tmp_path), spark=fake_spark, mode="seed",
                )
        # Load-bearing assertion: zero dispatch attempts
        mock_execute.assert_not_called()
