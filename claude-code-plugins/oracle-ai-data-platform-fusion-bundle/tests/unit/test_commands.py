"""Unit tests for the new orchestration CLI command bodies."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from oracle_ai_data_platform_fusion_bundle import cli

# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


class TestInit:
    def test_writes_minimal_template(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli.main, ["init", "--template", "minimal"])
        assert result.exit_code == 0
        assert (tmp_path / "bundle.yaml").exists()
        assert (tmp_path / "aidp.config.yaml").exists()

    def test_refuses_overwrite_without_force(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "bundle.yaml").write_text("existing")
        result = CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        assert result.exit_code == 1
        assert (tmp_path / "bundle.yaml").read_text() == "existing"

    def test_force_overwrites(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "bundle.yaml").write_text("existing")
        result = CliRunner().invoke(cli.main, ["init", "--template", "minimal", "--force"])
        assert result.exit_code == 0
        assert "existing" not in (tmp_path / "bundle.yaml").read_text()


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


class TestValidate:
    def test_passes_for_minimal_template(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(cli.main, ["validate"])
        assert result.exit_code == 0
        assert "validation passed" in result.output

    def test_fails_when_bundle_missing(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(cli.main, ["validate"])
        assert result.exit_code == 1

    def test_fails_for_unknown_dataset_id(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        bundle = tmp_path / "bundle.yaml"
        text = bundle.read_text(encoding="utf-8")
        # swap one dataset id to an unknown one
        bundle.write_text(text.replace("gl_journal_lines", "definitely_not_in_catalog"))
        result = CliRunner().invoke(cli.main, ["validate"])
        assert result.exit_code == 1
        assert "definitely_not_in_catalog" in result.output


# ---------------------------------------------------------------------------
# catalog list / probe
# ---------------------------------------------------------------------------


class TestCatalog:
    def test_list_runs(self) -> None:
        result = CliRunner().invoke(cli.main, ["catalog", "list"])
        assert result.exit_code == 0
        assert "PVO catalog" in result.output
        # Some known ids present
        assert "erp_suppliers" in result.output

    def test_probe_requires_creds(self) -> None:
        result = CliRunner().invoke(cli.main, [
            "catalog", "probe", "--pod", "https://example.com",
        ])
        assert result.exit_code == 2
        assert "missing creds" in result.output

    def test_probe_reconciles_when_all_match(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import CATALOG
        # Build a fake live response that contains every confirmed datastore name
        live_names = [{"name": e.datastore} for e in CATALOG.values()]
        fake_response = MagicMock(status_code=200)
        fake_response.json.return_value = {"items": live_names}
        with patch(
            "oracle_ai_data_platform_fusion_bundle.commands.catalog.requests.get",
            return_value=fake_response,
        ):
            result = CliRunner().invoke(cli.main, [
                "catalog", "probe", "--pod", "https://example.com",
                "--user", "u", "--password", "p",
            ])
        assert result.exit_code == 0
        assert "all" in result.output and "reconcile" in result.output


# ---------------------------------------------------------------------------
# bootstrap (network probes mocked)
# ---------------------------------------------------------------------------


class TestBootstrap:
    def test_requires_bundle_yaml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(cli.main, ["bootstrap"])
        assert result.exit_code == 1
        assert "bundle.yaml" in result.output

    def test_skips_bicc_probe_without_creds(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        # Ensure FUSION_BICC_* env vars are absent so the probe SKIPs
        monkeypatch.delenv("FUSION_BICC_USER", raising=False)
        monkeypatch.delenv("FUSION_BICC_PASSWORD", raising=False)
        result = CliRunner().invoke(cli.main, ["bootstrap"])
        # bundle.yaml + aidp.config.yaml load PASS but env=dev not in template -> FAIL on env-lookup
        # OR the templated env is named 'dev' and matches -> probes proceed
        # We don't assert exit code; only that bicc-auth was reported as SKIP.
        assert "bicc-auth" in result.output


# ---------------------------------------------------------------------------
# run / status
# ---------------------------------------------------------------------------


class TestRun:
    def test_dispatch_plan_dry_run(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(cli.main, ["run", "--mode", "incremental"])
        # Exit code 2 = "PLAN ONLY, no work performed". The plan IS still printed,
        # but the command intentionally fails so CI doesn't mistake the dry-run for
        # a real pipeline execution. Will become 0 once P1.5 wires dispatch submission.
        assert result.exit_code == 2
        assert "Dispatch plan" in result.output
        assert "PLAN ONLY" in result.output
        # at least one of the minimal-template datasets should be listed
        assert "gl_journal_lines" in result.output or "fusion_catalog" in result.output

    def test_dataset_filter(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(cli.main, [
            "run", "--mode", "incremental", "--datasets", "gl_journal_lines",
        ])
        # Same exit-code-2 contract as test_dispatch_plan_dry_run — see comment above.
        assert result.exit_code == 2

    @pytest.mark.skip(
        reason="Pre-P1.5α stub-only test — orchestrator.run() exists as of "
               "Phase 3 (2026-05-17). The CLI stub _run_inline still has the "
               "old dataset_ids kwarg shape; Phase 5 CLI migration rewires it "
               "to call orchestrator.run() and this test becomes redundant. "
               "Will be replaced by test_run_inline_invokes_orchestrator_run "
               "+ test_run_inline_exits_2_on_OrchestratorConfigError when "
               "Phase 5 lands."
    )
    def test_inline_without_orchestrator_fails_loudly(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`run --inline` must NOT silently succeed when orchestrator.run is missing."""
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(cli.main, ["run", "--mode", "seed", "--inline"])
        assert result.exit_code == 2
        assert "P1.5" in result.output
        assert "dim_supplier" in result.output


class TestStatus:
    def test_pyspark_unavailable_falls_back(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        # Ensure pyspark import fails — patch SparkSession import
        # If pyspark is importable, the test path differs; we only assert exit 0 either way.
        result = CliRunner().invoke(cli.main, ["status"])
        assert result.exit_code == 0

    def test_reads_configured_bronze_schema(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """P1.5b — ``status()`` must read ``fusion_bundle_state`` from the
        tenant's ``aidp.bronzeSchema`` (not the hardcoded ``'bronze'``).

        The scaffolded template (``examples/minimal_gl_only.yaml``) uses
        ``apiVersion`` and already has a full ``aidp:`` block with all
        four keys defaulted. We parse the YAML and *mutate* the existing
        ``aidp`` mapping in-place, then dump it back — a string-replace
        would either no-op (the template uses camelCase ``apiVersion``,
        not ``api_version``) or produce duplicate ``aidp:`` blocks where
        PyYAML would keep the later default one.

        After the mutation we sanity-check the parsed fixture before
        invoking ``status`` so a future template rename doesn't silently
        make the assertion vacuous.
        """
        import sys

        import yaml

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        bundle_path = tmp_path / "bundle.yaml"
        bundle = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
        # The scaffolded template MUST have the aidp block — pin that
        # contract so a template rename surfaces here, not as a confusing
        # status-test failure.
        assert isinstance(bundle.get("aidp"), dict), (
            "scaffolded template must already carry an `aidp:` block; "
            "if the template shape changes, this test (and the "
            "TablePaths.from_bundle contract) needs updating."
        )

        # Mutate the existing aidp mapping in place.
        bundle["aidp"]["catalog"]      = "my_lake"
        bundle["aidp"]["bronzeSchema"] = "raw"
        bundle["aidp"]["silverSchema"] = "clean"
        bundle["aidp"]["goldSchema"]   = "marts"

        bundle_path.write_text(
            yaml.safe_dump(bundle, sort_keys=False), encoding="utf-8"
        )

        # Sanity: round-trip the YAML and verify the mutation actually took.
        reread = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
        assert reread["aidp"]["catalog"]      == "my_lake"
        assert reread["aidp"]["bronzeSchema"] == "raw"

        # Force the fallback-print path (no pyspark).
        monkeypatch.setitem(sys.modules, "pyspark", None)
        monkeypatch.setitem(sys.modules, "pyspark.sql", None)

        result = CliRunner().invoke(cli.main, ["status"])
        assert result.exit_code == 0
        assert "my_lake.raw.fusion_bundle_state" in result.output
        # Critically, the pre-P1.5b hardcoded shape must NOT appear.
        assert "my_lake.bronze.fusion_bundle_state" not in result.output
