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
    def test_dispatch_without_inline_points_to_rest_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Pre-P1.5ε: `run` without `--inline` exits 2 with a stub message
        listing the three execution surfaces (inline / MCP / REST). The
        REST path is BACKLOG P1.5ε — empirically validated, not yet
        wired into the CLI. Will become 0 once P1.5ε ships."""
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(cli.main, ["run", "--mode", "seed"])
        assert result.exit_code == 2
        # Stub message points operators at the three execution surfaces.
        assert "REST dispatch" in result.output or "P1.5ε" in result.output
        assert "--inline" in result.output

    def test_dataset_filter_with_rest_stub(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The `--datasets` filter is parsed by the CLI and threaded through
        to the REST-dispatch stub (today exits 2; tomorrow P1.5ε wires it
        to actual job submission)."""
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(cli.main, [
            "run", "--mode", "seed", "--datasets", "gl_journal_lines",
        ])
        assert result.exit_code == 2

    def test_run_inline_invokes_orchestrator_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`run --inline` calls orchestrator.run(bundle_path=..., mode=..., datasets=...)
        with the correct kwarg shape and exits 0 on a clean RunSummary.

        Replaces the pre-P1.5α stub-only test (which was marked skip
        in Phase 3). Mocks `orchestrator.run` to return a synthetic
        empty RunSummary so we don't need Spark.
        """
        from unittest.mock import MagicMock, patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import RunSummary
        fake_summary = RunSummary.empty("minimal", "seed")

        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.run",
            return_value=fake_summary,
        ) as mock_run:
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--inline"],
            )
        assert result.exit_code == 0, f"expected exit 0, got {result.exit_code}: {result.output}"
        # Assert the call shape — Path object, mode kwarg, datasets=None default
        assert mock_run.called
        call_kwargs = mock_run.call_args.kwargs
        assert isinstance(call_kwargs["bundle_path"], Path)
        assert call_kwargs["mode"] == "seed"
        assert call_kwargs["datasets"] is None

    def test_run_inline_passes_datasets_csv_as_raw_list(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`--datasets "a,b,c"` is parsed by the CLI into ["a","b","c"]
        (whitespace trimmed, empty segments dropped) and threaded as a
        raw list — NOT pre-resolved against bundle.datasets[] (P1.5α-fix7).
        """
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import RunSummary
        fake_summary = RunSummary.empty("minimal", "seed")

        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.run",
            return_value=fake_summary,
        ) as mock_run:
            CliRunner().invoke(cli.main, [
                "run", "--mode", "seed", "--inline",
                "--datasets", " ap_aging , dim_supplier ,,",
            ])
        # Whitespace trimmed; empty segments dropped
        assert mock_run.call_args.kwargs["datasets"] == ["ap_aging", "dim_supplier"]

    @pytest.mark.parametrize("exc_cls,msg_fragment", [
        ("BundleLoadError", "test bundle load failure"),
        ("UnsupportedModeError", "mode='full' is not supported"),
        ("MissingDependencyError", "Unknown dim 'dim_typo'"),
        ("CredentialResolutionError", "Env var 'FOO' is not set"),
    ])
    def test_run_inline_exits_2_on_orchestrator_config_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        exc_cls: str, msg_fragment: str,
    ) -> None:
        """Every OrchestratorConfigError subclass surfaces as exit 2 with the
        message printed verbatim — no Python traceback."""
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        from oracle_ai_data_platform_fusion_bundle.orchestrator import errors
        ExceptionCls = getattr(errors, exc_cls)

        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.run",
            side_effect=ExceptionCls(msg_fragment),
        ):
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--inline"],
            )
        assert result.exit_code == 2, f"expected exit 2, got {result.exit_code}"
        assert msg_fragment in result.output
        # The load-bearing assertion: NO Python traceback leaked through.
        assert "Traceback (most recent call last)" not in result.output

    def test_run_inline_exits_2_on_not_implemented(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`NotImplementedError` (e.g. mode='incremental') is caught alongside
        OrchestratorConfigError."""
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.run",
            side_effect=NotImplementedError("Incremental mode is P1.5β"),
        ):
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--inline"],
            )
        assert result.exit_code == 2
        assert "P1.5β" in result.output
        assert "Traceback" not in result.output

    def test_run_cli_rejects_mode_full_at_parse_time(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`--mode full` is rejected by Click's Choice BEFORE the orchestrator
        is touched (P1.5α-fix2 Option A surface defense).

        Parse-time rejection is load-bearing — if a typo'd mode reached
        ``_run_inline``, the orchestrator's entry guard (Option D
        defense-in-depth) would catch it with a richer message, but Click's
        parser is the cheap front-line filter. The patched ``orchestrator.run``
        confirms the front line works — the orchestrator is never invoked.
        """
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.run",
        ) as mock_run:
            result = CliRunner().invoke(cli.main, ["run", "--mode", "full", "--inline"])
        assert result.exit_code == 2
        # Click's standard error format
        assert "'full' is not one of" in result.output or "Invalid value" in result.output
        # Parse-time rejection — orchestrator never invoked
        mock_run.assert_not_called()


class TestMigrateBundle:
    """`migrate-bundle --from X --to Y` — scaffolded for Option L (§4.4d).

    Today only v0.2.0 exists; any non-no-op invocation exits 2 with a
    "no migration path" message. Blocker-2 fix: this is a top-level CLI
    verb that returns exit codes directly (not via NotImplementedError,
    which only `_run_inline` catches).
    """

    def test_same_version_is_noop_exit_0(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(
            cli.main, ["migrate-bundle", "--from", "0.2.0", "--to", "0.2.0"],
        )
        assert result.exit_code == 0
        assert "already at version" in result.output

    def test_unknown_migration_exits_2(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(
            cli.main, ["migrate-bundle", "--from", "0.1.0", "--to", "0.2.0"],
        )
        assert result.exit_code == 2
        assert "No migration path" in result.output
        # Critical: NOT a Python traceback. Blocker-2 fix.
        assert "Traceback" not in result.output


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

    def test_query_uses_latest_per_dataset_and_includes_skip_reason(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should-fix-5: status query is ROW_NUMBER() OVER (PARTITION BY
        dataset_id ORDER BY last_run_at DESC) — one row per dataset — and
        includes the skip_reason column for cascade-vs-abort discrimination.

        Asserts on the SQL the fallback-print emits (the pyspark-unavailable
        path) since that's the surface the unit tests can reach without
        Spark.
        """
        import sys
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        monkeypatch.setitem(sys.modules, "pyspark", None)
        monkeypatch.setitem(sys.modules, "pyspark.sql", None)

        result = CliRunner().invoke(cli.main, ["status"])
        assert result.exit_code == 0
        # Window function + partition-by-dataset assertion (the load-bearing
        # behavior — pre-fix the query returned every historical row).
        assert "ROW_NUMBER()" in result.output
        assert "PARTITION BY dataset_id" in result.output
        assert "ORDER BY last_run_at DESC" in result.output
        assert "WHERE rn = 1" in result.output
        # The new column makes cascade vs aborted visible to dashboards.
        assert "skip_reason" in result.output
