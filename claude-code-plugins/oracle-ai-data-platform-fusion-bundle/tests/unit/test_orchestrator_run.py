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
    def __init__(self, row_count: int = 100, *, rows: list | None = None) -> None:
        self._row_count = row_count
        self._rows = rows or []
        self.write = MagicMock()
        self.write.format.return_value = self.write
        self.write.mode.return_value = self.write
        self.write.option.return_value = self.write
        self.write.saveAsTable.return_value = None
        # Minimal schema-like object so P1.17's _ensure_target_table_exists
        # can render CREATE TABLE on the incremental + fresh-tenant path.
        self.schema = MagicMock()
        self.schema.fields = []

    def count(self) -> int:
        return self._row_count

    def withColumn(self, *args, **kwargs) -> "_FakeDataFrame":
        return self

    def collect(self) -> list:
        return self._rows

    def first(self):
        # P1.17 silver/gold capture path: spark.sql("SELECT MAX(...)").first().
        # Returns None on an empty result, matching real Spark semantics.
        return self._rows[0] if self._rows else None

    # P1.17 bronze MERGE path uses cache + temp views; the seed-path
    # tests in this file never hit incremental, but defining these
    # as no-ops insulates any future test that switches modes.
    def cache(self) -> "_FakeDataFrame":
        return self

    def unpersist(self) -> None:
        return None

    def createOrReplaceTempView(self, _name: str) -> None:
        return None


class _FakeCatalog:
    def __init__(self, existing_tables: set[str] | None = None) -> None:
        self._existing = existing_tables or set()

    def tableExists(self, path: str) -> bool:
        return path in self._existing


class _FakeSpark:
    """Just enough Spark for the orchestrator to dispatch. The run loop calls
    ``spark.table(target).count()`` on the bronze branch and
    ``state.ensure_state_table``/``write_state_row`` SQL — stubbed to no-ops.

    ``DESCRIBE TABLE`` is recognized and returns an empty result (so
    ``ensure_state_table``'s schema-aware migration sees a "no
    existing columns" view and emits the ALTER TABLE — harmless
    against the fake).
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

    def test_mode_incremental_passes_guard_after_p117_gate_removal(self) -> None:
        """P1.17 (C9) removed the ``NotImplementedError`` gate that β.1
        held. The new contract: ``mode="incremental"`` now passes the
        mode-validation guard and proceeds to ``load_bundle`` (which
        raises ``BundleLoadError`` for the bogus path here). The β.1
        D7 ``test_mode_incremental_raises_not_implemented`` was deleted
        atomically with this gate removal — keeping it would have kept
        the gate.
        """
        with pytest.raises(orchestrator.BundleLoadError):
            orchestrator.run(Path("/nope"), mode="incremental")

    def test_mode_seed_passes_guard(self, tmp_path: Path) -> None:
        # Seed should pass the guard and proceed to load_bundle (which then fails
        # because the path doesn't exist).
        with pytest.raises(orchestrator.BundleLoadError):
            orchestrator.run(tmp_path / "nope.yaml", mode="seed")


# ---------------------------------------------------------------------------
# resolve_plan
# ---------------------------------------------------------------------------


class TestResolvePlan:
    """P1.5ε-fix9 — TestResolvePlan shrank to two engine-side smokes after
    the bulk of behavior tests relocated to
    ``tests/unit/schema/test_plan_resolver.py``. The remaining tests lock
    the engine-side wrapper contract: ``resolve_plan`` still returns
    ``Spec`` instances + ``ExternalDep``, and the deferred-name branch
    reconstructs ``DeferredSpec`` instead of KeyError-ing on a registry
    miss.
    """

    def test_resolve_plan_back_compat_returns_specs_and_external_deps(
        self, tmp_path: Path,
    ) -> None:
        """Engine-side import surface lock — anything that did
        ``from oracle_ai_data_platform_fusion_bundle.orchestrator import resolve_plan``
        keeps working unchanged. Returns a topo-sorted list of Spec
        instances + ExternalDep prereqs, same as pre-fix9.
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import (
            ExternalDep,
            load_bundle,
        )
        bundle, paths = load_bundle(_bundle_file(tmp_path))
        plan, extra_deps = orchestrator.resolve_plan(
            bundle, None, ["gold"], paths=paths,
        )
        plan_names = {s.dataset_id for s in plan}
        assert plan_names == {"supplier_spend", "gl_balance", "ap_aging"}
        # Plan entries are Spec instances (not PlanNode) — the engine
        # consumes ``spec.builder`` per step.
        for s in plan:
            assert isinstance(s, (registry.GoldMartSpec, registry.SilverDimSpec, registry.BronzeExtractSpec, registry.DeferredSpec))
        # Prereqs are ExternalDep — engine-side preflight calls
        # ``spark.catalog.tableExists`` on the ``table_path``.
        for d in extra_deps:
            assert isinstance(d, ExternalDep)

    def test_resolve_plan_wrapper_returns_deferred_spec_for_deferred_names(
        self, tmp_path: Path,
    ) -> None:
        """Reviewer round 1 blocking lock — deferred names like
        ``dim_org`` / ``ar_aging`` / ``hcm_worker_assignments`` are NOT in
        ``BRONZE_EXTRACTS`` / ``SILVER_DIMS`` / ``GOLD_MARTS``. A naive
        registry-dict lookup in the wrapper would KeyError. The wrapper
        must branch on ``PlanNode.status == "deferred"`` and reconstruct
        ``DeferredSpec`` from the PlanNode fields.
        """
        bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: fix9-deferred-reconstruction
fusion:
  serviceUrl: https://x
  username: u
  password: literal-pw
  externalStorage: oci://b@n/p
datasets:
  - id: hcm_worker_assignments
dimensions:
  build:
    - dim_org
gold:
  marts:
    - ar_aging
"""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import (
            load_bundle,
        )
        bundle, paths = load_bundle(_bundle_file(tmp_path, bundle_yaml))
        plan, _ = orchestrator.resolve_plan(
            bundle, None, None, paths=paths,
        )
        deferred = {s.dataset_id: s for s in plan if isinstance(s, registry.DeferredSpec)}
        assert set(deferred) == {"hcm_worker_assignments", "dim_org", "ar_aging"}
        assert deferred["hcm_worker_assignments"].layer == "bronze"
        assert deferred["dim_org"].layer == "silver"
        assert deferred["ar_aging"].layer == "gold"
        # Reason strings are operator-facing — must be preserved verbatim
        # through the PlanNode→DeferredSpec hop.
        assert "P2.11" in deferred["hcm_worker_assignments"].reason
        assert "P1.7" in deferred["dim_org"].reason
        assert "P1.10" in deferred["ar_aging"].reason


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

    def test_inline_dry_run_returns_plan_nodes_only(self, tmp_path: Path) -> None:
        """P1.5ε-fix9 reviewer round 1 blocking lock — the --inline
        dry-run path must coerce engine Specs → PlanNode AND ExternalDep
        → PrereqNode BEFORE building RunSummary.empty, so the renderer
        sees the same DTO types regardless of execution surface
        (--inline or REST dispatch). After Step 6's
        ``_layer_for_spec`` lazy-import fallback removal in
        ``_render_summary``, any spec that leaks through would break
        plan rendering.
        """
        from oracle_ai_data_platform_fusion_bundle.schema.run_summary import (
            PlanNode,
            PrereqNode,
        )

        with patch("oracle_ai_data_platform_fusion_bundle.orchestrator._bootstrap_spark"), \
             patch("oracle_ai_data_platform_fusion_bundle.orchestrator._resolve_password"):
            summary = orchestrator.run(
                _bundle_file(tmp_path), mode="seed", dry_run=True,
                layers=["gold"],  # forces silver/bronze into prereqs
            )
        assert summary.plan is not None
        assert isinstance(summary.plan, tuple)
        assert all(isinstance(n, PlanNode) for n in summary.plan), (
            f"every plan entry must be a PlanNode; got "
            f"{[type(n).__name__ for n in summary.plan]}"
        )
        assert summary.prereqs is not None
        assert isinstance(summary.prereqs, tuple)
        assert all(isinstance(n, PrereqNode) for n in summary.prereqs), (
            f"every prereq entry must be a PrereqNode; got "
            f"{[type(n).__name__ for n in summary.prereqs]}"
        )
        # Render-summary round-trip: must not raise on a tuple of
        # PlanNode/PrereqNode (catches a regression where a spec leaks
        # through and node.layer lookup fails).
        from io import StringIO

        from rich.console import Console

        from oracle_ai_data_platform_fusion_bundle.commands.run import (
            _render_summary,
        )
        _render_summary(Console(file=StringIO()), summary)


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

        def fake_enrich(df, *, source_pvo, run_id, watermark, extract_ts):
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
        def fake_enrich(df, *, source_pvo, run_id, watermark, extract_ts):
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

        def fake_enrich(df, *, source_pvo, run_id, watermark, extract_ts):
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


# ---------------------------------------------------------------------------
# P1.5α-fix19 — PreflightResult propagation + dispatch-contract threading.
# These tests prove the resolved schema (override / catalog / discovered)
# flows ALL the way through to the real bronze extract_pvo call — not just
# preflight. Without these, the override + auto-discovery features would be
# cosmetic-only (preflight passes, real run still crashes with the same
# DATA_ACCESS_LAYER_0031).
# ---------------------------------------------------------------------------


class TestFix19PreflightThreading:
    def test_run_threads_preflight_recommendations_into_run_summary(
        self, tmp_path: Path,
    ) -> None:
        """PreflightResult.recommendations → RunSummary.recommendations.
        Propagation contract — without this, a refactor could drop the
        recommendations silently (preflight emits them, RunSummary never
        carries them, CLI never renders them).
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator.preflight import (
            PreflightResult,
        )
        from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog

        # Cover every bronze in _MIN_BUNDLE's plan so the dispatch lookup
        # never raises KeyError on effective_schemas[node.dataset_id].
        effective_schemas = {
            "erp_suppliers": fusion_catalog.get("erp_suppliers").schema,
            "ap_invoices": fusion_catalog.get("ap_invoices").schema,
            "gl_coa": fusion_catalog.get("gl_coa").schema,
            "gl_period_balances": fusion_catalog.get("gl_period_balances").schema,
        }
        stub_result = PreflightResult(
            recommendations=("recommendation A", "recommendation B"),
            effective_schemas=effective_schemas,
        )

        fake_spark = _FakeSpark()
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
            return_value=stub_result,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.extractors.bicc.extract_pvo",
            return_value=_FakeDataFrame(10),
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.enrich_bronze_audit_cols",
            side_effect=lambda df, **kw: df,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.state.write_state_row",
        ), patch.dict(
            registry.SILVER_DIMS,
            {k: type(v)(v.dataset_id, lambda *a, **k: _FakeDataFrame(5), v.depends_on_bronze) for k, v in registry.SILVER_DIMS.items()},
        ), patch.dict(
            registry.GOLD_MARTS,
            {k: type(v)(v.dataset_id, lambda *a, **k: _FakeDataFrame(3), v.depends_on_bronze, v.depends_on_silver) for k, v in registry.GOLD_MARTS.items()},
        ):
            summary = orchestrator.run(
                _bundle_file(tmp_path), spark=fake_spark, mode="seed",
            )

        # Exact match — tuple type, order preserved
        assert summary.recommendations == ("recommendation A", "recommendation B")

    def test_override_value_used_at_bronze_dispatch_not_just_preflight(
        self, tmp_path: Path,
    ) -> None:
        """fix19 dispatch contract — the resolved schema must flow to the
        real extract_pvo call, not just preflight. Without this, override is
        cosmetic theatre."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.preflight import (
            PreflightResult,
        )

        # Preflight (mocked) returns the override values for every bronze
        effective_schemas = {
            "erp_suppliers": "OVERRIDE_ERP",
            "ap_invoices": "OVERRIDE_AP",
            "gl_coa": "OVERRIDE_COA",
            "gl_period_balances": "OVERRIDE_GLPB",
        }
        stub_result = PreflightResult(
            recommendations=(),
            effective_schemas=effective_schemas,
        )

        # Capture extract_pvo's schema kwarg per call
        recorded_schemas: dict[str, str] = {}

        def recording_extract(spark, pvo, **kwargs):
            recorded_schemas[pvo.id] = kwargs.get("schema")
            return _FakeDataFrame(10)

        fake_spark = _FakeSpark()
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
            return_value=stub_result,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.extractors.bicc.extract_pvo",
            side_effect=recording_extract,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.enrich_bronze_audit_cols",
            side_effect=lambda df, **kw: df,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.state.write_state_row",
        ), patch.dict(
            registry.SILVER_DIMS,
            {k: type(v)(v.dataset_id, lambda *a, **k: _FakeDataFrame(5), v.depends_on_bronze) for k, v in registry.SILVER_DIMS.items()},
        ), patch.dict(
            registry.GOLD_MARTS,
            {k: type(v)(v.dataset_id, lambda *a, **k: _FakeDataFrame(3), v.depends_on_bronze, v.depends_on_silver) for k, v in registry.GOLD_MARTS.items()},
        ):
            orchestrator.run(
                _bundle_file(tmp_path), spark=fake_spark, mode="seed",
            )

        # Every bronze got the override value, NOT the catalog default
        assert recorded_schemas["erp_suppliers"] == "OVERRIDE_ERP", (
            f"override schema must reach real bronze dispatch; "
            f"got {recorded_schemas.get('erp_suppliers')!r}"
        )
        assert recorded_schemas["ap_invoices"] == "OVERRIDE_AP"
        assert recorded_schemas["gl_coa"] == "OVERRIDE_COA"
        assert recorded_schemas["gl_period_balances"] == "OVERRIDE_GLPB"

    def test_auto_discovered_schema_used_at_bronze_dispatch_not_just_preflight(
        self, tmp_path: Path,
    ) -> None:
        """Same as the override test but the resolved schema came from
        auto-discovery rather than override. Both paths must flow to the
        real BICC pull."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.preflight import (
            PreflightResult,
        )

        # Simulate preflight ran auto-discovery and resolved every bronze
        # to a "DiscoveredXxx" schema (not the catalog default).
        effective_schemas = {
            "erp_suppliers": "DiscoveredERP",
            "ap_invoices": "DiscoveredAP",
            "gl_coa": "DiscoveredCOA",
            "gl_period_balances": "DiscoveredGLPB",
        }
        stub_result = PreflightResult(
            recommendations=(
                "consider adding schemaOverrides.erp_suppliers: DiscoveredERP to bundle.yaml",
            ),
            effective_schemas=effective_schemas,
        )

        recorded_schemas: dict[str, str] = {}

        def recording_extract(spark, pvo, **kwargs):
            recorded_schemas[pvo.id] = kwargs.get("schema")
            return _FakeDataFrame(10)

        fake_spark = _FakeSpark()
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
            return_value=stub_result,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.extractors.bicc.extract_pvo",
            side_effect=recording_extract,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.enrich_bronze_audit_cols",
            side_effect=lambda df, **kw: df,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.state.write_state_row",
        ), patch.dict(
            registry.SILVER_DIMS,
            {k: type(v)(v.dataset_id, lambda *a, **k: _FakeDataFrame(5), v.depends_on_bronze) for k, v in registry.SILVER_DIMS.items()},
        ), patch.dict(
            registry.GOLD_MARTS,
            {k: type(v)(v.dataset_id, lambda *a, **k: _FakeDataFrame(3), v.depends_on_bronze, v.depends_on_silver) for k, v in registry.GOLD_MARTS.items()},
        ):
            summary = orchestrator.run(
                _bundle_file(tmp_path), spark=fake_spark, mode="seed",
            )

        # Discovered schemas reach real dispatch (NOT the catalog defaults)
        assert recorded_schemas["erp_suppliers"] == "DiscoveredERP"
        assert recorded_schemas["ap_invoices"] == "DiscoveredAP"
        assert recorded_schemas["gl_coa"] == "DiscoveredCOA"
        assert recorded_schemas["gl_period_balances"] == "DiscoveredGLPB"
        # AND the recommendation made it through to the summary footer
        assert summary.recommendations == (
            "consider adding schemaOverrides.erp_suppliers: DiscoveredERP to bundle.yaml",
        )
