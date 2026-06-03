"""P1.5ε §Step 6 — dispatch_via_rest entry-point integration tests.

These exercise the composition of all five primitives (preflight, wheel,
notebook, REST, marker parse) with mocked HTTP. The most important
invariants:

- AidpRestError NEVER escapes dispatch_via_rest — every call site wraps
  into the matching DispatchError subclass.
- Phase A failure short-circuits before any client construction.
- Dry-run path doesn't build a wheel, upload a notebook, or submit a job.
- SUCCESS-without-marker raises DispatchMarkerMissingError (evidence-
  capture failure).
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_bundle.dispatch import dispatch_via_rest
from oracle_ai_data_platform_fusion_bundle.dispatch.errors import (
    DispatchAuthError,
    DispatchFetchOutputError,
    DispatchJobSubmitError,
    DispatchMarkerMissingError,
    DispatchPreflightError,
    DispatchRunFailedError,
    DispatchUploadError,
)
from oracle_ai_data_platform_fusion_bundle.dispatch.rest_client import (
    AidpRestError,
    ClusterSummary,
    RunResult,
)
from oracle_ai_data_platform_fusion_bundle.schema.bundle import (
    AidpConfig,
    EnvSpec,
)
from oracle_ai_data_platform_fusion_bundle.schema.run_summary import (
    MARKER_SCHEMA_VERSION,
    RunStep,
    RunSummary,
)


_GOOD_BUNDLE = """\
apiVersion: aidp-fusion-bundle/v1
project: test-dispatch
fusion:
  serviceUrl: https://fusion.example.com
  username: user
  password: not-a-secret
  externalStorage: storage-1
datasets:
  - id: erp_suppliers
"""


@pytest.fixture
def bundle_path(tmp_path: Path) -> Path:
    p = tmp_path / "bundle.yaml"
    p.write_text(_GOOD_BUNDLE)
    return p


def _env() -> EnvSpec:
    return EnvSpec.model_validate(
        {
            "workspaceKey": "wk-123",
            "aiDataPlatformId": "ocid1.datalake.oc1.iad.test",
            "clusterKey": "cluster-uuid-1",
            "clusterName": "test-cluster",
            "ociProfile": "AIDP_SESSION",
        }
    )


def _config() -> AidpConfig:
    return AidpConfig.model_validate(
        {
            "apiVersion": "aidp-fusion-bundle/v1",
            "project": "test-dispatch",
            "environments": {"dev": _env().model_dump(by_alias=True)},
        }
    )


def _make_marker_payload(run_id: str = "test-run-1") -> dict:
    """Build a valid RunSummary marker payload."""
    return {
        "schema_version": MARKER_SCHEMA_VERSION,
        "run_id": run_id,
        "started_at": "2026-06-03T14:00:00Z",
        "finished_at": "2026-06-03T14:05:00Z",
        "bundle_project": "test-dispatch",
        "mode": "seed",
        "recommendations": [],
        "steps": [
            {
                "run_id": run_id,
                "dataset_id": "erp_suppliers",
                "layer": "bronze",
                "mode": "seed",
                "status": "success",
                "row_count": 42,
                "duration_seconds": 1.5,
                "error_message": None,
                "watermark_used": None,
                "last_watermark": None,
                "skip_reason": None,
                "plan_hash": None,
                "plan_snapshot": None,
            }
        ],
    }


def _executed_notebook_with_marker(payload: dict) -> str:
    """Build the JSON-string an AIDP fetchOutput would return."""
    marker_text = (
        f"AIDP_LIVE_TEST_RESULT_BEGIN {json.dumps(payload)} "
        "AIDP_LIVE_TEST_RESULT_END"
    )
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "outputs": [{"output_type": "stream", "name": "stdout", "text": marker_text}],
            }
        ]
    }
    return json.dumps(notebook)


@pytest.fixture(autouse=True)
def _stub_preflight_and_oci(monkeypatch: pytest.MonkeyPatch):
    """All-pass Phase A by default — individual tests can override."""

    def _ok_config(profile_name: str = "DEFAULT") -> dict:
        return {
            "tenancy": "t",
            "user": "u",
            "fingerprint": "f",
            "key_file": "/tmp/k",
        }

    monkeypatch.setattr(
        "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.oci.config.from_file",
        _ok_config,
    )
    monkeypatch.setattr(
        "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.subprocess.run",
        lambda *a, **kw: subprocess.CompletedProcess(args=[], returncode=0, stdout="ok", stderr=""),
    )
    # Stub the canonical client's signer construction too.
    monkeypatch.setattr(
        "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.config.from_file",
        _ok_config,
    )
    monkeypatch.setattr(
        "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client._build_signer",
        lambda cfg: MagicMock(name="signer"),
    )


def _stub_client(monkeypatch, **overrides):
    """Patch ``AidpRestClient`` to return a configured mock.

    Preserves the real ``parse_marker`` and ``resolve_task_run_key``
    @staticmethods on the substitute class so dispatch_via_rest's
    ``AidpRestClient.parse_marker(...)`` call goes through the actual
    notebook walker, not a MagicMock that returns a mock value.
    """
    from oracle_ai_data_platform_fusion_bundle.dispatch.rest_client import (
        AidpRestClient as RealAidpRestClient,
    )

    client_mock = MagicMock(name="AidpRestClient instance")
    client_mock.list_clusters.return_value = [
        ClusterSummary(key="cluster-uuid-1", display_name="dev", state="ACTIVE")
    ]
    client_mock.upload_notebook.return_value = "/Workspace/Shared/x/run.ipynb"
    client_mock.create_notebook_job.return_value = "job-key-1"
    client_mock.submit_run.return_value = "job-run-key-1"
    raw = {"taskToTaskRunMap": {"orchestrator_run": "task-run-key-1"}}
    client_mock.poll_run.return_value = RunResult(status="SUCCESS", raw=raw)
    client_mock.fetch_output.return_value = _executed_notebook_with_marker(
        _make_marker_payload()
    )
    for k, v in overrides.items():
        setattr(client_mock, k, v)

    factory = MagicMock(return_value=client_mock)
    factory.parse_marker = RealAidpRestClient.parse_marker
    factory.resolve_task_run_key = RealAidpRestClient.resolve_task_run_key
    monkeypatch.setattr(
        "oracle_ai_data_platform_fusion_bundle.dispatch.AidpRestClient",
        factory,
    )
    return client_mock


# ---------------------------------------------------------------------------
# Phase A boundary
# ---------------------------------------------------------------------------


class TestPhaseAGuards:
    def test_missing_dispatch_coords_fails_before_client_construction(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client_factory = MagicMock(side_effect=AssertionError("client must not be built"))
        monkeypatch.setattr(
            "oracle_ai_data_platform_fusion_bundle.dispatch.AidpRestClient",
            client_factory,
        )
        env = EnvSpec.model_validate(
            {"workspaceKey": "wk-123", "ociProfile": "AIDP_SESSION"}
        )
        with pytest.raises(DispatchPreflightError, match="aiDataPlatformId"):
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=env,
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )
        client_factory.assert_not_called()

    def test_bundle_load_failure_fails_before_client(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client_factory = MagicMock(side_effect=AssertionError("client must not be built"))
        monkeypatch.setattr(
            "oracle_ai_data_platform_fusion_bundle.dispatch.AidpRestClient",
            client_factory,
        )
        with pytest.raises(DispatchPreflightError):
            dispatch_via_rest(
                bundle_path=tmp_path / "no-such-bundle.yaml",
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )
        client_factory.assert_not_called()


# ---------------------------------------------------------------------------
# Dry-run short-circuit
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_dry_run_skips_wheel_and_upload(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _stub_client(monkeypatch)
        wheel_mock = MagicMock(
            side_effect=AssertionError("build_wheel must not be called in dry-run")
        )
        monkeypatch.setattr(
            "oracle_ai_data_platform_fusion_bundle.dispatch.build_wheel",
            wheel_mock,
        )
        summary = dispatch_via_rest(
            bundle_path=bundle_path,
            config=_config(),
            env=_env(),
            env_name="dev",
            mode="seed",
            datasets=None,
            layers=None,
            dry_run=True,
        )
        assert isinstance(summary, RunSummary)
        assert summary.steps == ()
        client.upload_notebook.assert_not_called()
        client.create_notebook_job.assert_not_called()
        client.submit_run.assert_not_called()


# ---------------------------------------------------------------------------
# Happy path — full round trip
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_full_round_trip_returns_run_summary(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _stub_client(monkeypatch)
        monkeypatch.setattr(
            "oracle_ai_data_platform_fusion_bundle.dispatch.build_wheel",
            lambda **_: Path("/tmp/fake.whl"),
        )
        # build_notebook reads wheel bytes — stub it too.
        monkeypatch.setattr(
            "oracle_ai_data_platform_fusion_bundle.dispatch.build_notebook",
            lambda **_: {"cells": [], "nbformat": 4, "nbformat_minor": 5},
        )

        summary = dispatch_via_rest(
            bundle_path=bundle_path,
            config=_config(),
            env=_env(),
            env_name="dev",
            mode="seed",
            datasets=None,
            layers=None,
        )
        assert summary.run_id == "test-run-1"
        assert summary.succeeded == 1
        assert summary.steps[0].dataset_id == "erp_suppliers"
        client.upload_notebook.assert_called_once()
        client.create_notebook_job.assert_called_once()
        client.submit_run.assert_called_once()
        client.poll_run.assert_called_once()
        client.fetch_output.assert_called_once_with("task-run-key-1")


# ---------------------------------------------------------------------------
# AidpRestError → DispatchError wrapping
# ---------------------------------------------------------------------------


class TestErrorWrapping:
    def _setup_happy_path_dispatch(self, monkeypatch):
        monkeypatch.setattr(
            "oracle_ai_data_platform_fusion_bundle.dispatch.build_wheel",
            lambda **_: Path("/tmp/fake.whl"),
        )
        monkeypatch.setattr(
            "oracle_ai_data_platform_fusion_bundle.dispatch.build_notebook",
            lambda **_: {"cells": [], "nbformat": 4, "nbformat_minor": 5},
        )

    def test_upload_failure_wrapped(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _stub_client(monkeypatch)
        client.upload_notebook.side_effect = AidpRestError(
            "PUT /notebook/api/contents/x: HTTP 401 body=bad token"
        )
        self._setup_happy_path_dispatch(monkeypatch)
        with pytest.raises(DispatchUploadError, match="HTTP 401"):
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )

    def test_job_submit_failure_wrapped(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _stub_client(monkeypatch)
        client.create_notebook_job.side_effect = AidpRestError(
            "POST /jobs: HTTP 500 body=CircuitBreaker"
        )
        self._setup_happy_path_dispatch(monkeypatch)
        with pytest.raises(DispatchJobSubmitError, match="CircuitBreaker"):
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )

    def test_poll_timeout_wrapped(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _stub_client(monkeypatch)
        client.poll_run.side_effect = AidpRestError(
            "poll_run(x): deadline exceeded after 1800s"
        )
        self._setup_happy_path_dispatch(monkeypatch)
        with pytest.raises(DispatchPollTimeoutError := __import__(
            "oracle_ai_data_platform_fusion_bundle.dispatch.errors",
            fromlist=["DispatchPollTimeoutError"],
        ).DispatchPollTimeoutError, match="deadline exceeded"):
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )

    def test_fetch_output_failure_wrapped(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _stub_client(monkeypatch)
        client.fetch_output.side_effect = AidpRestError(
            "fetch_output(x): HTTP 404"
        )
        self._setup_happy_path_dispatch(monkeypatch)
        with pytest.raises(DispatchFetchOutputError, match="HTTP 404"):
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )

    def test_terminal_failed_status_wrapped(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _stub_client(monkeypatch)
        raw = {"taskToTaskRunMap": {"orchestrator_run": "task-run-key-1"}}
        client.poll_run.return_value = RunResult(status="FAILED", raw=raw)
        self._setup_happy_path_dispatch(monkeypatch)
        with pytest.raises(DispatchRunFailedError, match="'FAILED'"):
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )

    def test_success_without_marker_raises_marker_missing(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _stub_client(monkeypatch)
        client.fetch_output.return_value = json.dumps(
            {"cells": [{"cell_type": "code", "outputs": [{"text": "hello"}]}]}
        )
        self._setup_happy_path_dispatch(monkeypatch)
        with pytest.raises(DispatchMarkerMissingError, match="no marker found"):
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )

    def test_truncated_marker_raises_marker_missing(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """BEGIN delimiter present but no matching END (truncated AIDP
        output) — parse_marker raises ValueError from `value.index(end)`.
        Must surface as DISPATCH_MARKER_MISSING with jobRunKey, NOT a raw
        ValueError traceback."""
        client = _stub_client(monkeypatch)
        # Stdout contains the BEGIN delimiter but the matching END was
        # cut off (cluster output truncated, or operator-killed mid-emit).
        truncated_text = (
            'AIDP_LIVE_TEST_RESULT_BEGIN {"schema_version": 1, "run_id": "x", '
            '"started_at": "2026-06-03T14:00:00Z", "finished_at"'
        )
        client.fetch_output.return_value = json.dumps(
            {"cells": [{"cell_type": "code", "outputs": [{"text": truncated_text}]}]}
        )
        self._setup_happy_path_dispatch(monkeypatch)
        with pytest.raises(
            DispatchMarkerMissingError, match="marker parse failed"
        ) as exc_info:
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )
        # Operator-actionable: jobRunKey is in the message so they can
        # correlate with the AIDP console.
        assert "job-run-key-1" in str(exc_info.value)
        # Underlying cause is preserved for --verbose / debug users.
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, ValueError)

    def test_malformed_executed_notebook_json_raises_marker_missing(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """fetchOutput returned a 200 with a body that's not valid JSON
        (truncated transport, server-side bug). The json.loads call must
        be wrapped — raw JSONDecodeError would escape the CLI taxonomy
        catch as an unhandled traceback."""
        client = _stub_client(monkeypatch)
        # Genuinely-malformed JSON — looks like the start of a notebook
        # response that got cut mid-stream.
        client.fetch_output.return_value = '{"cells": [{"cell_type": "code"'
        self._setup_happy_path_dispatch(monkeypatch)
        with pytest.raises(
            DispatchMarkerMissingError, match="JSON decode failed"
        ) as exc_info:
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )
        assert "job-run-key-1" in str(exc_info.value)
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, json.JSONDecodeError)

    def test_malformed_marker_payload_raises_marker_missing(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """BEGIN + END delimiters present but the JSON between them
        doesn't parse — parse_marker raises json.JSONDecodeError from
        the inner `json.loads(value[b:e])`. Same DISPATCH_MARKER_MISSING
        landing per the wrapped (ValueError, JSONDecodeError) clause."""
        client = _stub_client(monkeypatch)
        bad_marker_text = (
            "AIDP_LIVE_TEST_RESULT_BEGIN {not-valid-json-here} "
            "AIDP_LIVE_TEST_RESULT_END"
        )
        client.fetch_output.return_value = json.dumps(
            {"cells": [{"cell_type": "code", "outputs": [{"text": bad_marker_text}]}]}
        )
        self._setup_happy_path_dispatch(monkeypatch)
        with pytest.raises(
            DispatchMarkerMissingError, match="marker parse failed"
        ) as exc_info:
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )
        assert "job-run-key-1" in str(exc_info.value)

    def test_no_aidp_rest_error_escapes(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The invariant: every AidpRestError call site is wrapped. If a
        future call site forgets to wrap, this test would still pass
        (because we'd catch DispatchError) — but the more pointed
        per-site tests above pin each individual mapping."""
        client = _stub_client(monkeypatch)
        client.upload_notebook.side_effect = AidpRestError("synthetic")
        self._setup_happy_path_dispatch(monkeypatch)
        from oracle_ai_data_platform_fusion_bundle.dispatch.errors import DispatchError

        with pytest.raises(DispatchError):
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="seed",
                datasets=None,
                layers=None,
            )
