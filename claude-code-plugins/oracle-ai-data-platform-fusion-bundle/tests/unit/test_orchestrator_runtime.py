"""Unit tests for orchestrator.runtime + orchestrator.registry + orchestrator.state.

Covers:
  - RunStep factory shapes (success, failed, skipped_cascade,
    skipped_aborted, deferred) including the structured skip_reason
    discriminator (B1.1).
  - RunSummary.empty() classmethod + counter properties.
  - _resolve_password — vault success/failure, env success/missing,
    literal-warn-once-per-run (R3).
  - _render_env_vars — recursion, missing-var raises BundleLoadError.
  - load_bundle — five failure modes + version-specific routing.
  - DeferredSpec.__post_init__ — Literal validation enforced.
  - Resolver functions — known / deferred / typo (MissingDependencyError).
  - _layer_for_spec — single source of truth for spec→layer mapping.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import SecretStr

from oracle_ai_data_platform_fusion_bundle.orchestrator import registry, runtime
from oracle_ai_data_platform_fusion_bundle.orchestrator.errors import (
    BundleLoadError,
    BundleVersionMismatchError,
    CredentialResolutionError,
    MissingDependencyError,
    OrchestratorConfigError,
    PrerequisiteError,
    UnsupportedModeError,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

_VALID_BUNDLE_YAML = """
apiVersion: aidp-fusion-bundle/v1
project: test-bundle
fusion:
  serviceUrl: https://example.com
  username: u
  password: literal-password
  externalStorage: oci://bucket@ns/path
datasets:
  - id: erp_suppliers
    mode: full
"""


def _write_yaml(tmp_path: Path, content: str, name: str = "bundle.yaml") -> Path:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# RunStep + RunSummary
# ---------------------------------------------------------------------------


class TestRunStepFactories:
    def test_success_bronze_derives_layer_from_spec(self) -> None:
        spec = registry.BRONZE_EXTRACTS["ap_invoices"]
        step = runtime.RunStep.success(
            spec, "run-1", "seed", row_count=100, duration_seconds=2.5
        )
        assert step.layer == "bronze"
        assert step.status == "success"
        assert step.row_count == 100
        assert step.duration_seconds == 2.5
        assert step.error_message is None
        assert step.skip_reason is None
        assert step.dataset_id == "ap_invoices"

    def test_success_silver_derives_layer(self) -> None:
        spec = registry.SILVER_DIMS["dim_supplier"]
        step = runtime.RunStep.success(
            spec, "run-1", "seed", row_count=10, duration_seconds=1.0
        )
        assert step.layer == "silver"

    def test_success_gold_derives_layer(self) -> None:
        spec = registry.GOLD_MARTS["supplier_spend"]
        step = runtime.RunStep.success(
            spec, "run-1", "seed", row_count=10, duration_seconds=1.0
        )
        assert step.layer == "gold"

    def test_failed_carries_repr_exc(self) -> None:
        spec = registry.BRONZE_EXTRACTS["ap_invoices"]
        step = runtime.RunStep.failed(
            spec, "run-1", "seed",
            exc=RuntimeError("BICC 503"),
            duration_seconds=3.0,
        )
        assert step.status == "failed"
        assert step.row_count is None
        assert "BICC 503" in step.error_message  # type: ignore[operator]
        assert "RuntimeError" in step.error_message  # type: ignore[operator]
        assert step.skip_reason is None

    def test_skipped_cascade_uses_template_and_marks_reason(self) -> None:
        spec = registry.SILVER_DIMS["dim_supplier"]
        step = runtime.RunStep.skipped_cascade(
            spec, "run-1", "seed", upstream_dataset_id="ap_invoices"
        )
        assert step.status == "skipped"
        assert step.skip_reason == "cascade"
        assert "ap_invoices" in step.error_message  # type: ignore[operator]
        assert step.error_message.startswith("cascade:")  # type: ignore[union-attr]
        assert step.duration_seconds == 0.0

    def test_skipped_aborted_uses_template_and_marks_reason(self) -> None:
        spec = registry.GOLD_MARTS["gl_balance"]
        step = runtime.RunStep.skipped_aborted(
            spec, "run-1", "seed", failed_dataset_id="ap_invoices"
        )
        assert step.status == "skipped"
        assert step.skip_reason == "aborted"
        assert "ap_invoices" in step.error_message  # type: ignore[operator]
        assert step.error_message.startswith("aborted:")  # type: ignore[union-attr]
        assert step.duration_seconds == 0.0

    def test_deferred_reads_spec_layer(self) -> None:
        spec = registry.DeferredSpec("dim_org", layer="silver", reason="P1.7")
        step = runtime.RunStep.deferred(
            spec, "run-1", "seed", error_message="P1.7 — HCM org dim"
        )
        assert step.layer == "silver"
        assert step.status == "deferred"
        assert step.row_count is None
        assert step.error_message == "P1.7 — HCM org dim"
        assert step.skip_reason is None
        assert step.duration_seconds == 0.0

    # ---- P1.5α-fix21: resumed_skip factory + frozen-instance contract ----

    def test_resumed_skip_bronze(self) -> None:
        spec = registry.BRONZE_EXTRACTS["ap_invoices"]
        step = runtime.RunStep.resumed_skip(spec, "run-1", "seed")
        assert step.layer == "bronze"
        assert step.status == "resumed_skipped"
        assert step.skip_reason == "resume-skip"
        assert step.row_count is None
        assert step.duration_seconds == 0.0
        # Error message names the original run_id for audit traceability.
        assert "run-1" in (step.error_message or "")

    def test_resumed_skip_silver_and_gold_derive_layer(self) -> None:
        s_silver = runtime.RunStep.resumed_skip(
            registry.SILVER_DIMS["dim_supplier"], "run-1", "seed",
        )
        assert s_silver.layer == "silver"
        s_gold = runtime.RunStep.resumed_skip(
            registry.GOLD_MARTS["supplier_spend"], "run-1", "seed",
        )
        assert s_gold.layer == "gold"

    def test_resumed_skip_deferred_reads_spec_layer(self) -> None:
        spec = registry.DeferredSpec("dim_org", layer="silver", reason="P1.7")
        step = runtime.RunStep.resumed_skip(spec, "run-1", "seed")
        assert step.layer == "silver"
        assert step.status == "resumed_skipped"

    def test_all_factories_accept_plan_hash_and_snapshot_kwargs(self) -> None:
        """P1.5α-fix21: every factory threads `plan_hash` + `plan_snapshot`
        into the constructed RunStep via kwargs (NOT post-construction
        assignment — RunStep is frozen)."""
        spec = registry.BRONZE_EXTRACTS["ap_invoices"]
        h, snap = "hash-XYZ", '{"identity":{},"nodes":[]}'
        for step in (
            runtime.RunStep.success(spec, "r", "seed", row_count=1, duration_seconds=0.1, plan_hash=h, plan_snapshot=snap),
            runtime.RunStep.failed(spec, "r", "seed", exc=Exception("x"), duration_seconds=0.1, plan_hash=h, plan_snapshot=snap),
            runtime.RunStep.skipped_cascade(spec, "r", "seed", upstream_dataset_id="up", plan_hash=h, plan_snapshot=snap),
            runtime.RunStep.skipped_aborted(spec, "r", "seed", failed_dataset_id="fl", plan_hash=h, plan_snapshot=snap),
            runtime.RunStep.deferred(registry.DeferredSpec("d", layer="bronze", reason="r"), "r", "seed", error_message="x", plan_hash=h, plan_snapshot=snap),
            runtime.RunStep.resumed_skip(spec, "r", "seed", plan_hash=h, plan_snapshot=snap),
        ):
            assert step.plan_hash == h
            assert step.plan_snapshot == snap

    def test_runstep_remains_frozen(self) -> None:
        """Pin the @dataclass(frozen=True) contract — direct
        attribute assignment must raise FrozenInstanceError. Catches a
        regression that silently relaxes the immutability and lets the
        orchestrator mutate a step post-construction (which would
        bypass the factory-kwarg threading)."""
        import dataclasses
        spec = registry.BRONZE_EXTRACTS["ap_invoices"]
        step = runtime.RunStep.success(
            spec, "r", "seed", row_count=1, duration_seconds=0.1,
        )
        assert dataclasses.is_dataclass(step)
        assert step.__dataclass_params__.frozen is True
        with pytest.raises(dataclasses.FrozenInstanceError):
            step.plan_hash = "mutated"  # type: ignore[misc]


class TestRunSummary:
    def test_counter_properties(self) -> None:
        spec_b = registry.BRONZE_EXTRACTS["ap_invoices"]
        spec_s = registry.SILVER_DIMS["dim_supplier"]
        steps = (
            runtime.RunStep.success(spec_b, "r1", "seed", row_count=1, duration_seconds=0.1),
            runtime.RunStep.failed(spec_b, "r1", "seed", exc=Exception("boom"), duration_seconds=0.1),
            runtime.RunStep.skipped_cascade(spec_s, "r1", "seed", upstream_dataset_id="ap_invoices"),
            runtime.RunStep.deferred(
                registry.DeferredSpec("dim_org", layer="silver", reason="x"),
                "r1", "seed", error_message="x",
            ),
        )
        from datetime import datetime, timezone
        summary = runtime.RunSummary(
            run_id="r1",
            started_at=datetime.now(tz=timezone.utc),
            finished_at=datetime.now(tz=timezone.utc),
            bundle_project="p",
            mode="seed",
            steps=steps,
        )
        assert summary.succeeded == 1
        assert summary.failed == 1
        assert summary.skipped == 1
        assert summary.deferred == 1
        assert summary.succeeded + summary.failed + summary.skipped + summary.deferred == len(steps)

    def test_resumed_skipped_counter(self) -> None:
        """P1.5α-fix21: resumed_skipped is its own counter — distinct
        from `skipped` (which counts cascade + abort skips only)."""
        spec = registry.BRONZE_EXTRACTS["ap_invoices"]
        steps = (
            runtime.RunStep.success(spec, "r", "seed", row_count=1, duration_seconds=0.1),
            runtime.RunStep.resumed_skip(spec, "r", "seed"),
            runtime.RunStep.resumed_skip(spec, "r", "seed"),
        )
        from datetime import datetime, timezone
        summary = runtime.RunSummary(
            run_id="r", started_at=datetime.now(tz=timezone.utc),
            finished_at=datetime.now(tz=timezone.utc),
            bundle_project="p", mode="seed", steps=steps,
        )
        assert summary.resumed_skipped == 2
        # `skipped` is independent — counts only cascade/abort.
        assert summary.skipped == 0

    def test_empty_bundle_path(self) -> None:
        s = runtime.RunSummary.empty("p", "seed")
        assert s.steps == ()
        assert s.plan is None
        assert s.prereqs is None
        assert s.run_id.startswith("empty-")
        assert s.mode == "seed"

    def test_empty_dry_run_path_carries_plan_and_prereqs(self) -> None:
        s = runtime.RunSummary.empty(
            "p", "seed",
            plan=("dim_supplier", "supplier_spend"),
            prereqs=("ap_invoices",),
        )
        assert s.plan == ("dim_supplier", "supplier_spend")
        assert s.prereqs == ("ap_invoices",)
        assert s.steps == ()


# ---------------------------------------------------------------------------
# DeferredSpec __post_init__ validation
# ---------------------------------------------------------------------------


class TestDeferredSpecValidation:
    def test_valid_layers_construct(self) -> None:
        for layer in ("bronze", "silver", "gold"):
            spec = registry.DeferredSpec("x", layer=layer, reason="r")  # type: ignore[arg-type]
            assert spec.layer == layer

    def test_invalid_layer_typo_raises(self) -> None:
        with pytest.raises(ValueError, match="sliver"):
            registry.DeferredSpec("x", layer="sliver", reason="r")  # type: ignore[arg-type]

    def test_invalid_layer_empty_raises(self) -> None:
        with pytest.raises(ValueError):
            registry.DeferredSpec("x", layer="", reason="r")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Resolver functions
# ---------------------------------------------------------------------------


class TestResolvers:
    def test_resolve_bronze_known(self) -> None:
        spec = registry._resolve_bronze("ap_invoices")
        assert isinstance(spec, registry.BronzeExtractSpec)
        assert spec.dataset_id == "ap_invoices"

    def test_resolve_bronze_deferred(self) -> None:
        spec = registry._resolve_bronze("hcm_worker_assignments")
        assert isinstance(spec, registry.DeferredSpec)
        assert spec.layer == "bronze"
        assert "P2.11" in spec.reason or "saas-batch" in spec.reason.lower()

    def test_resolve_bronze_typo_raises_missing_dependency(self) -> None:
        with pytest.raises(MissingDependencyError) as ei:
            registry._resolve_bronze("nonexistent_dataset")
        msg = str(ei.value)
        assert "nonexistent_dataset" in msg
        # Inherits from OrchestratorConfigError so the CLI's except clause catches it
        assert isinstance(ei.value, OrchestratorConfigError)

    def test_resolve_dim_known(self) -> None:
        spec = registry._resolve_dim("dim_supplier")
        assert isinstance(spec, registry.SilverDimSpec)

    def test_resolve_dim_deferred(self) -> None:
        spec = registry._resolve_dim("dim_org")
        assert isinstance(spec, registry.DeferredSpec)
        assert spec.layer == "silver"

    def test_resolve_dim_typo_raises_missing_dependency(self) -> None:
        with pytest.raises(MissingDependencyError):
            registry._resolve_dim("dim_typo")

    def test_resolve_mart_known(self) -> None:
        spec = registry._resolve_mart("ap_aging")
        assert isinstance(spec, registry.GoldMartSpec)

    def test_resolve_mart_deferred(self) -> None:
        spec = registry._resolve_mart("ar_aging")
        assert isinstance(spec, registry.DeferredSpec)
        assert spec.layer == "gold"

    def test_resolve_mart_typo_raises_missing_dependency(self) -> None:
        with pytest.raises(MissingDependencyError):
            registry._resolve_mart("mart_typo")


# ---------------------------------------------------------------------------
# _layer_for_spec — single source of truth
# ---------------------------------------------------------------------------


class TestLayerForSpec:
    def test_bronze(self) -> None:
        assert registry._layer_for_spec(registry.BRONZE_EXTRACTS["ap_invoices"]) == "bronze"

    def test_silver(self) -> None:
        assert registry._layer_for_spec(registry.SILVER_DIMS["dim_supplier"]) == "silver"

    def test_gold(self) -> None:
        assert registry._layer_for_spec(registry.GOLD_MARTS["supplier_spend"]) == "gold"

    def test_deferred_reads_spec_layer(self) -> None:
        spec = registry.DeferredSpec("x", layer="bronze", reason="r")
        assert registry._layer_for_spec(spec) == "bronze"

    def test_unknown_spec_type_raises_typeerror(self) -> None:
        class FakeSpec:
            dataset_id = "x"
        with pytest.raises(TypeError, match="unknown spec type"):
            registry._layer_for_spec(FakeSpec())


# ---------------------------------------------------------------------------
# Registry invariants (§8 lints)
# ---------------------------------------------------------------------------


class TestRegistryInvariants:
    def test_no_name_collisions_across_registries(self) -> None:
        """B1.1 + Option 4 fix — every registry key must be unique across the
        union of all six registries. ``resolve_plan`` treats names as a
        single namespace; collisions cause ambiguous dispatch.
        """
        registries = {
            "BRONZE_EXTRACTS": set(registry.BRONZE_EXTRACTS),
            "SILVER_DIMS": set(registry.SILVER_DIMS),
            "GOLD_MARTS": set(registry.GOLD_MARTS),
            "KNOWN_DEFERRED_DATASETS": set(registry.KNOWN_DEFERRED_DATASETS),
            "KNOWN_DEFERRED_DIMS": set(registry.KNOWN_DEFERRED_DIMS),
            "KNOWN_DEFERRED_MARTS": set(registry.KNOWN_DEFERRED_MARTS),
        }
        names = list(registries.items())
        for i, (name_a, set_a) in enumerate(names):
            for name_b, set_b in names[i + 1 :]:
                shared = set_a & set_b
                assert not shared, (
                    f"name collision between {name_a} and {name_b}: {shared}. "
                    f"Rename one side (the catalog's bronze_table_name often "
                    f"gives a more accurate id, as ap_aging→ap_aging_periods did)."
                )

    def test_every_extract_pvo_catalog_entry_is_registered_or_deferred(self) -> None:
        """Catalog↔registry invariant lint (Option C). Every PvoKind.EXTRACT_PVO
        catalog entry MUST be in BRONZE_EXTRACTS OR KNOWN_DEFERRED_DATASETS.
        """
        from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import (
            CATALOG, PvoKind,
        )
        extract_ids = {e.id for e in CATALOG.values() if e.kind == PvoKind.EXTRACT_PVO}
        accounted_for = set(registry.BRONZE_EXTRACTS) | set(registry.KNOWN_DEFERRED_DATASETS)
        leaked = extract_ids - accounted_for
        assert not leaked, (
            f"EXTRACT_PVO catalog entries with no registry slot: {leaked}. "
            f"Add to BRONZE_EXTRACTS (now-runnable) or KNOWN_DEFERRED_DATASETS (future)."
        )

    def test_bronze_extracts_resolve_via_catalog(self) -> None:
        """Every BronzeExtractSpec.pvo_id MUST resolve via fusion_catalog.get(...)."""
        from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import get
        for ds_id, spec in registry.BRONZE_EXTRACTS.items():
            entry = get(spec.pvo_id)  # raises KeyError if missing
            assert entry.id == spec.pvo_id

    def test_silver_dim_spec_includes_builder_from_dimensions_module(self) -> None:
        """P1.5ε-fix9 — locks that the metadata+builder join in
        ``orchestrator/registry.py`` wires each silver dim to the correct
        builder. A typo in the ``_SILVER_BUILDERS`` dict (e.g. mapping
        ``dim_supplier`` to ``dim_account.build``) silently produces a
        broken pipeline at runtime; this test catches the misroute at
        unit-test time.
        """
        from oracle_ai_data_platform_fusion_bundle.dimensions import (
            dim_account,
            dim_calendar,
            dim_supplier,
        )
        assert registry.SILVER_DIMS["dim_supplier"].builder is dim_supplier.build
        assert registry.SILVER_DIMS["dim_account"].builder is dim_account.build
        assert registry.SILVER_DIMS["dim_calendar"].builder is dim_calendar.build

    def test_gold_mart_spec_includes_builder_from_transforms_module(self) -> None:
        """P1.5ε-fix9 — same builder-wiring lock for gold marts."""
        from oracle_ai_data_platform_fusion_bundle.transforms.gold import (
            ap_aging,
            gl_balance,
            supplier_spend,
        )
        assert registry.GOLD_MARTS["supplier_spend"].builder is supplier_spend.build
        assert registry.GOLD_MARTS["gl_balance"].builder is gl_balance.build
        assert registry.GOLD_MARTS["ap_aging"].builder is ap_aging.build


# ---------------------------------------------------------------------------
# _resolve_password (§4.9 + B5 + R3)
# ---------------------------------------------------------------------------


class TestResolvePassword:
    def test_literal_succeeds_and_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            result = runtime._resolve_password("plain-literal")
        assert isinstance(result, SecretStr)
        assert result.get_secret_value() == "plain-literal"
        warns = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warns) == 1
        assert "literal" in warns[0].message.lower()

    def test_literal_warns_only_once_per_run(self, caplog: pytest.LogCaptureFixture) -> None:
        """R3 — _LITERAL_WARN_EMITTED gates the WARN so the double-call
        pattern (preflight + dispatch) emits exactly one log line."""
        with caplog.at_level(logging.WARNING):
            runtime._resolve_password("literal-1")
            runtime._resolve_password("literal-2")
            runtime._resolve_password("literal-3")
        warns = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warns) == 1, f"expected 1 WARN, got {len(warns)}"

    def test_env_var_resolves(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_FUSION_PWD_VAR", "from-env")
        result = runtime._resolve_password("${env:TEST_FUSION_PWD_VAR}")
        assert result.get_secret_value() == "from-env"

    def test_env_var_missing_raises_credential_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("NONEXISTENT_PWD_VAR_XYZ", raising=False)
        with pytest.raises(CredentialResolutionError) as ei:
            runtime._resolve_password("${env:NONEXISTENT_PWD_VAR_XYZ}")
        assert "NONEXISTENT_PWD_VAR_XYZ" in str(ei.value)
        assert isinstance(ei.value, OrchestratorConfigError)
        # Exception chain preserved
        assert isinstance(ei.value.__cause__, KeyError)

    def test_vault_inaccessible_raises_credential_error(self) -> None:
        """Mock aidputils to raise; assert CredentialResolutionError naming the OCID."""
        import sys
        fake_module = type(sys)("aidputils")
        fake_secrets = type(sys)("aidputils.secrets")

        def _fake_get(ocid: str) -> str:
            raise RuntimeError(f"403 forbidden on {ocid}")

        fake_secrets.get = _fake_get
        fake_module.secrets = fake_secrets

        with patch.dict(sys.modules, {"aidputils": fake_module, "aidputils.secrets": fake_secrets}):
            with pytest.raises(CredentialResolutionError) as ei:
                runtime._resolve_password("${vault:ocid1.bogus.oc1..abc}")
        assert "ocid1.bogus.oc1..abc" in str(ei.value)
        assert "SECRET_FAMILY_READ" in str(ei.value)


# ---------------------------------------------------------------------------
# _render_env_vars (§4.4a)
# ---------------------------------------------------------------------------


class TestRenderEnvVars:
    def test_expands_string_leaf(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_VAR", "expanded")
        result = runtime._render_env_vars({"key": "${TEST_VAR}"})
        assert result == {"key": "expanded"}

    def test_preserves_vault_sigil(self) -> None:
        # render_vars in schema.refs has a negative-lookahead for ${vault:...}
        original = "${vault:ocid1.example}"
        result = runtime._render_env_vars(original)
        assert result == original

    def test_recurses_dict_list(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("A", "1")
        monkeypatch.setenv("B", "2")
        result = runtime._render_env_vars({
            "x": [{"a": "${A}"}, {"b": "${B}"}],
        })
        assert result == {"x": [{"a": "1"}, {"b": "2"}]}

    def test_missing_var_raises_bundle_load_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("NOPE_NONEXISTENT_KEY_xyz", raising=False)
        with pytest.raises(BundleLoadError) as ei:
            runtime._render_env_vars("${NOPE_NONEXISTENT_KEY_xyz}")
        assert "NOPE_NONEXISTENT_KEY_xyz" in str(ei.value)

    def test_passes_through_non_strings(self) -> None:
        assert runtime._render_env_vars(42) == 42
        assert runtime._render_env_vars(True) is True
        assert runtime._render_env_vars(None) is None


# ---------------------------------------------------------------------------
# load_bundle — five failure modes + version routing
# ---------------------------------------------------------------------------


class TestLoadBundle:
    def test_happy_path(self, tmp_path: Path) -> None:
        fp = _write_yaml(tmp_path, _VALID_BUNDLE_YAML)
        bundle, paths = runtime.load_bundle(fp)
        assert bundle.project == "test-bundle"
        assert paths.catalog == "fusion_catalog"
        assert bundle.version == "0.2.0"

    def test_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(BundleLoadError, match="Bundle file not found"):
            runtime.load_bundle(tmp_path / "nope.yaml")

    def test_path_is_directory(self, tmp_path: Path) -> None:
        with pytest.raises(BundleLoadError, match="is a directory"):
            runtime.load_bundle(tmp_path)

    def test_malformed_yaml(self, tmp_path: Path) -> None:
        fp = _write_yaml(tmp_path, "key: : :\n  broken: : :")
        with pytest.raises(BundleLoadError, match="Malformed YAML"):
            runtime.load_bundle(fp)

    def test_empty_yaml(self, tmp_path: Path) -> None:
        fp = _write_yaml(tmp_path, "")
        with pytest.raises(BundleLoadError, match="must be a YAML mapping"):
            runtime.load_bundle(fp)

    def test_yaml_is_scalar(self, tmp_path: Path) -> None:
        fp = _write_yaml(tmp_path, "42")
        with pytest.raises(BundleLoadError, match="must be a YAML mapping"):
            runtime.load_bundle(fp)

    def test_missing_env_var(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("NOPE_BUNDLE_VAR_xyz", raising=False)
        bad = _VALID_BUNDLE_YAML.replace(
            "https://example.com", "${NOPE_BUNDLE_VAR_xyz}"
        )
        fp = _write_yaml(tmp_path, bad)
        with pytest.raises(BundleLoadError, match="NOPE_BUNDLE_VAR_xyz"):
            runtime.load_bundle(fp)

    def test_schema_violation_missing_project(self, tmp_path: Path) -> None:
        bad = _VALID_BUNDLE_YAML.replace("project: test-bundle", "")
        fp = _write_yaml(tmp_path, bad)
        with pytest.raises(BundleLoadError, match="failed schema validation"):
            runtime.load_bundle(fp)

    def test_old_version_raises_version_mismatch(self, tmp_path: Path) -> None:
        bad = _VALID_BUNDLE_YAML.replace(
            "project: test-bundle",
            "project: test-bundle\nversion: \"0.1.0\""
        )
        fp = _write_yaml(tmp_path, bad)
        with pytest.raises(BundleVersionMismatchError) as ei:
            runtime.load_bundle(fp)
        msg = str(ei.value)
        assert "0.1.0" in msg
        assert "0.2.0" in msg
        assert "migrate-bundle" in msg

    def test_future_version_raises_version_mismatch(self, tmp_path: Path) -> None:
        bad = _VALID_BUNDLE_YAML.replace(
            "project: test-bundle",
            "project: test-bundle\nversion: \"0.3.0\""
        )
        fp = _write_yaml(tmp_path, bad)
        with pytest.raises(BundleVersionMismatchError):
            runtime.load_bundle(fp)

    def test_bad_identifier_in_aidp_block(self, tmp_path: Path) -> None:
        bad = _VALID_BUNDLE_YAML + "\naidp:\n  catalog: 'my-lake'\n"
        fp = _write_yaml(tmp_path, bad)
        with pytest.raises(BundleLoadError, match="invalid aidp"):
            runtime.load_bundle(fp)


# ---------------------------------------------------------------------------
# Exception class hierarchy
# ---------------------------------------------------------------------------


class TestExceptionHierarchy:
    """All user-facing config errors inherit from OrchestratorConfigError so
    the CLI's exit-2 except clause catches them uniformly."""

    @pytest.mark.parametrize("cls", [
        BundleLoadError,
        BundleVersionMismatchError,
        MissingDependencyError,
        CredentialResolutionError,
        PrerequisiteError,
        UnsupportedModeError,
    ])
    def test_subclass_of_orchestrator_config_error(self, cls: type) -> None:
        assert issubclass(cls, OrchestratorConfigError)

    def test_version_mismatch_inherits_from_load_error(self) -> None:
        assert issubclass(BundleVersionMismatchError, BundleLoadError)

    def test_unsupported_mode_multi_inherits_value_error(self) -> None:
        # Legacy callers that `except ValueError:` still work.
        assert issubclass(UnsupportedModeError, ValueError)
        assert issubclass(UnsupportedModeError, OrchestratorConfigError)

    def test_every_public_error_class_inherits_marker(self) -> None:
        """Self-maintaining lint (P1.5α-fix6, broadened in P1.5β.1):
        every name exported from ``errors.__all__`` (other than the
        two marker base classes themselves) must inherit from EITHER
        ``OrchestratorConfigError`` (pre-dispatch / config errors —
        CLI exit-2 catch) OR ``OrchestratorRuntimeError`` (per-step
        dispatch errors — surface through the normal
        ``RunStep.failed`` cascade path).

        Guards against a future contributor adding
        ``class FooError(Exception)`` to errors.py without a marker
        base — that class would fall through both code paths and
        surface as a traceback to the operator OR mask a real
        cascade decision.

        Self-maintaining because it loops over ``errors.__all__``
        rather than enumerating class names.
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator import errors

        marker_bases = {"OrchestratorConfigError", "OrchestratorRuntimeError"}
        offenders: list[str] = []
        for name in errors.__all__:
            if name in marker_bases:
                continue
            cls = getattr(errors, name)
            if not issubclass(
                cls, (errors.OrchestratorConfigError, errors.OrchestratorRuntimeError)
            ):
                offenders.append(name)
        assert not offenders, (
            f"every exception class in errors.__all__ must inherit either "
            f"OrchestratorConfigError (CLI exit-2 marker) or "
            f"OrchestratorRuntimeError (per-step dispatch marker). "
            f"Offenders: {offenders}. Did you add a new error class without "
            f"a marker base?"
        )
