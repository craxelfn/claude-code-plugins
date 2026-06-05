"""Phase 3c — Schema-drift marker → SchemaDriftDetectedError translation.

Pins the contract: when the cluster-side run cell catches
``SchemaDriftDetectedError``, emits a discriminated marker
(``_kind == "schema_drift"``), and re-raises, ``dispatch_via_rest`` must:

1. Translate the marker into ``SchemaDriftDetectedError`` (NOT
   ``DispatchRunFailedError``) — even when ``result.status == "FAILED"``.
2. Write the embedded ``artifact_json`` to the laptop-side
   ``.aidp/diagnostics/<run_id>/AIDPF-2012.json`` so the operator can
   run ``bootstrap --refresh`` against it.
3. Surface ``run_id`` + ``summary`` + fingerprints on the raised
   exception so the CLI's exit-14 path can render to stderr.

Reuses the existing fixtures in ``test_dispatch_via_rest`` (``bundle_path``,
``_stub_client``, ``_env``, ``_config``, ``_stub_preflight_and_oci``) — this
file is co-located in ``tests/unit/dispatch/`` to inherit the autouse
preflight stub.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_bundle.dispatch import dispatch_via_rest
from oracle_ai_data_platform_fusion_bundle.dispatch.errors import (
    DispatchRunFailedError,
)
from oracle_ai_data_platform_fusion_bundle.dispatch.rest_client import (
    ClusterSummary,
    RunResult,
)
from oracle_ai_data_platform_fusion_bundle.schema.bundle import (
    AidpConfig,
    EnvSpec,
)
from oracle_ai_data_platform_fusion_bundle.schema.errors import (
    SchemaDriftDetectedError,
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
dimensions:
  build: []
gold:
  marts: []
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


@pytest.fixture(autouse=True)
def _stub_preflight_and_oci(monkeypatch: pytest.MonkeyPatch):
    """All-pass Phase A — mirrors the autouse fixture in
    ``test_dispatch_via_rest`` so this file is self-contained."""
    import subprocess

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
        lambda *a, **kw: subprocess.CompletedProcess(
            args=[], returncode=0, stdout="ok", stderr=""
        ),
    )
    monkeypatch.setattr(
        "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.config.from_file",
        _ok_config,
    )
    monkeypatch.setattr(
        "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client._build_signer",
        lambda cfg: MagicMock(name="signer"),
    )


def _stub_client(monkeypatch, **overrides):
    """Patch ``AidpRestClient`` with a configured mock, preserving real
    static methods for marker parsing."""
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
    client_mock.poll_run.return_value = RunResult(status="FAILED", raw=raw)

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


def _setup_build_stubs(monkeypatch):
    monkeypatch.setattr(
        "oracle_ai_data_platform_fusion_bundle.dispatch.build_wheel",
        lambda **_: Path("/tmp/fake.whl"),
    )
    monkeypatch.setattr(
        "oracle_ai_data_platform_fusion_bundle.dispatch.build_notebook",
        lambda **_: {"cells": [], "nbformat": 4, "nbformat_minor": 5},
    )


def _drift_artifact_json(run_id: str = "cp-drift-1") -> str:
    """Minimal AIDPF-2012 SchemaDriftDiagnosticV1 JSON, matching what the
    cluster-side ``write_schema_drift_diagnostic`` would write."""
    return json.dumps(
        {
            "schemaVersion": 1,
            "runId": run_id,
            "tenant": "finance-default",
            "errorCode": "AIDPF-2012",
            "errorMessage": "bronze schema fingerprint diverged from pinned profile",
            "generatedAt": "2026-06-06T12:00:00Z",
            "schemaDrift": {
                "priorFingerprint": "sha256:" + "a" * 64,
                "currentFingerprint": "sha256:" + "b" * 64,
                "pinnedAt": "2026-06-01T08:00:00Z",
                "affectedVariationPoints": [],
            },
        }
    )


def _executed_notebook_with_drift_marker(
    *,
    run_id: str = "cp-drift-1",
    artifact_json: str | None = None,
) -> str:
    """The fetchOutput JSON the cluster returns when the run cell caught
    SchemaDriftDetectedError, emitted the drift marker, then re-raised."""
    payload = {
        "_kind": "schema_drift",
        "run_id": run_id,
        "summary": (
            "Bronze schema drift detected — run "
            "`aidp-fusion-bundle bootstrap --refresh`."
        ),
        "prior_fingerprint": "sha256:" + "a" * 64,
        "current_fingerprint": "sha256:" + "b" * 64,
        "artifact_json": artifact_json or _drift_artifact_json(run_id),
    }
    marker_text = (
        f"AIDP_LIVE_TEST_RESULT_BEGIN {json.dumps(payload)} "
        "AIDP_LIVE_TEST_RESULT_END\n"
        "Traceback (most recent call last):\n"
        "SchemaDriftDetectedError: ..."
    )
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "outputs": [
                    {"output_type": "stream", "name": "stdout", "text": marker_text}
                ],
            }
        ]
    }
    return json.dumps(notebook)


class TestSchemaDriftMarkerTranslation:
    def test_drift_marker_raises_schema_drift_detected_error(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        client = _stub_client(monkeypatch)
        client.fetch_output.return_value = _executed_notebook_with_drift_marker()
        _setup_build_stubs(monkeypatch)

        with pytest.raises(SchemaDriftDetectedError) as exc_info:
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="incremental",
                datasets=None,
                layers=None,
            )
        exc = exc_info.value
        assert exc.run_id == "cp-drift-1"
        assert "bootstrap --refresh" in exc.summary
        assert exc.prior_fingerprint == "sha256:" + "a" * 64
        assert exc.current_fingerprint == "sha256:" + "b" * 64

    def test_drift_marker_precedes_dispatch_run_failed_error(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If marker-parse came AFTER the status check, FAILED + drift
        marker would surface as DispatchRunFailedError (exit 2) instead
        of SchemaDriftDetectedError (exit 14). Pin the ordering."""
        client = _stub_client(monkeypatch)
        # poll_run already returns FAILED via _stub_client default.
        client.fetch_output.return_value = _executed_notebook_with_drift_marker()
        _setup_build_stubs(monkeypatch)

        with pytest.raises(SchemaDriftDetectedError):
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="incremental",
                datasets=None,
                layers=None,
            )

    def test_drift_marker_writes_artifact_locally(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The artifact must land at
        ``<bundle_dir>/.aidp/diagnostics/<run_id>/AIDPF-2012.json`` so the
        operator's ``bootstrap --refresh`` (or ``/medallion-author``)
        finds it."""
        client = _stub_client(monkeypatch)
        client.fetch_output.return_value = _executed_notebook_with_drift_marker(
            run_id="cp-drift-write-1"
        )
        _setup_build_stubs(monkeypatch)

        with pytest.raises(SchemaDriftDetectedError) as exc_info:
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="incremental",
                datasets=None,
                layers=None,
            )
        expected_path = (
            bundle_path.resolve().parent
            / ".aidp"
            / "diagnostics"
            / "cp-drift-write-1"
            / "AIDPF-2012.json"
        )
        assert expected_path.exists()
        assert exc_info.value.diagnostic_path == expected_path
        body = json.loads(expected_path.read_text(encoding="utf-8"))
        assert body["errorCode"] == "AIDPF-2012"
        assert body["runId"] == "cp-drift-write-1"

    def test_failed_status_without_drift_marker_still_raises_run_failed(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression check: when there's NO drift marker (run failed for
        a non-drift reason), the status-check arm still fires and we
        surface ``DispatchRunFailedError`` (exit 2). The Phase 3c
        reordering must not swallow normal failures."""
        client = _stub_client(monkeypatch)
        # No marker at all — plain stderr trace.
        client.fetch_output.return_value = json.dumps(
            {
                "cells": [
                    {
                        "cell_type": "code",
                        "outputs": [{"text": "ZeroDivisionError: ..."}],
                    }
                ]
            }
        )
        _setup_build_stubs(monkeypatch)

        with pytest.raises(DispatchRunFailedError, match="'FAILED'"):
            dispatch_via_rest(
                bundle_path=bundle_path,
                config=_config(),
                env=_env(),
                env_name="dev",
                mode="incremental",
                datasets=None,
                layers=None,
            )
