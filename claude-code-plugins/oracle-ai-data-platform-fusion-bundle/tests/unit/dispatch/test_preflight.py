"""P1.5ε §Step 5 — dispatch/preflight.py tests.

Covers both phases:

- Phase A (local): bundle, dispatch coords, OCI profile + session-token
  validation. No HTTP, no AidpRestClient construction.
- Phase B (remote): control plane reachability, cluster state +
  auto-start. Mocked AidpRestClient.

Critical invariant — Phase A FAIL never lets Phase B run. Locked by the
two-phase function split: ``run_local_preflight`` returns SKIP entries for
subsequent checks once anything fails; ``run_remote_preflight`` is invoked
by the dispatch entry point only when local preflight is all-PASS.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import oci
import pytest

from oracle_ai_data_platform_fusion_bundle.dispatch.preflight import (
    PreflightResult,
    any_failed,
    render,
    run_local_preflight,
    run_remote_preflight,
)
from oracle_ai_data_platform_fusion_bundle.dispatch.rest_client import (
    AidpRestError,
    ClusterSummary,
)
from oracle_ai_data_platform_fusion_bundle.schema.bundle import (
    AidpConfig,
    AuthSpec,
    EnvSpec,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_GOOD_BUNDLE = """\
apiVersion: aidp-fusion-bundle/v1
project: test-preflight
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


def _env(**overrides) -> EnvSpec:
    base = dict(
        workspaceKey="wk-123",
        aiDataPlatformId="ocid1.datalake.oc1.iad.test",
        clusterKey="cluster-uuid-1",
        clusterName="test-cluster",
        ociProfile="AIDP_SESSION",
    )
    base.update(overrides)
    return EnvSpec.model_validate(base)


def _config() -> AidpConfig:
    return AidpConfig.model_validate(
        {
            "apiVersion": "aidp-fusion-bundle/v1",
            "project": "test",
            "environments": {"dev": _env().model_dump(by_alias=True)},
        }
    )


# ---------------------------------------------------------------------------
# Phase A — local preflight
# ---------------------------------------------------------------------------


class TestPhaseALocalPreflight:
    def test_all_pass_when_inputs_clean(
        self, bundle_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.oci.config.from_file",
            return_value={"tenancy": "t", "user": "u", "fingerprint": "f", "key_file": "/k"},
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        assert [r.status for r in results] == ["PASS", "PASS", "PASS"]
        assert not any_failed(results)

    def test_bundle_fail_skips_remaining(self, tmp_path: Path) -> None:
        bad_bundle = tmp_path / "missing.yaml"
        results = run_local_preflight(
            bundle_path=bad_bundle,
            config=_config(),
            env_name="dev",
            env=_env(),
        )
        assert results[0].status == "FAIL"
        assert results[1].status == "SKIP"
        assert results[2].status == "SKIP"
        assert any_failed(results)
        assert "validate" in (results[0].remediation or "")

    def test_missing_dispatch_coords_fails_with_field_names(
        self, bundle_path: Path
    ) -> None:
        env = _env(aiDataPlatformId=None, clusterKey=None)
        results = run_local_preflight(
            bundle_path=bundle_path,
            config=_config(),
            env_name="dev",
            env=env,
        )
        coords = results[1]
        assert coords.status == "FAIL"
        assert "aiDataPlatformId" in coords.detail
        assert "clusterKey" in coords.detail
        # OCI profile check must SKIP — we didn't get to construct a client.
        assert results[2].status == "SKIP"

    def test_vault_auth_rejected_with_fix6_hint(
        self, bundle_path: Path
    ) -> None:
        env = _env()
        env = env.model_copy(update={"auth": AuthSpec(mode="vault")})
        results = run_local_preflight(
            bundle_path=bundle_path,
            config=_config(),
            env_name="dev",
            env=env,
        )
        coords = results[1]
        assert coords.status == "FAIL"
        assert "vault" in coords.detail
        assert "fix6" in (coords.remediation or "").lower()

    def test_oci_profile_not_found_fails_cleanly(
        self, bundle_path: Path
    ) -> None:
        with patch(
            "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.oci.config.from_file",
            side_effect=oci.exceptions.ProfileNotFound("no such profile"),
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        oci_result = results[2]
        assert oci_result.status == "FAIL"
        assert "AIDP_SESSION" in (oci_result.remediation or "")

    def test_api_key_profile_skips_session_validation(
        self, bundle_path: Path
    ) -> None:
        # An API-key profile (no security_token_file) — no subprocess invocation.
        cfg = {"tenancy": "t", "user": "u", "fingerprint": "f", "key_file": "/k"}
        with (
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.oci.config.from_file",
                return_value=cfg,
            ),
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.subprocess.run",
                side_effect=AssertionError("subprocess.run must not be called for API-key profiles"),
            ),
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        assert results[2].status == "PASS"

    def test_session_token_valid_passes(self, bundle_path: Path) -> None:
        cfg = {
            "security_token_file": "/tmp/token",
            "key_file": "/tmp/key.pem",
        }
        with (
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.oci.config.from_file",
                return_value=cfg,
            ),
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="Session is valid", stderr=""
                ),
            ),
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        assert results[2].status == "PASS"

    def test_session_token_expired_fails_with_refresh_hint(
        self, bundle_path: Path
    ) -> None:
        cfg = {
            "security_token_file": "/tmp/token",
            "key_file": "/tmp/key.pem",
        }
        with (
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.oci.config.from_file",
                return_value=cfg,
            ),
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    args=[],
                    returncode=1,
                    stdout="",
                    stderr="session is not valid",
                ),
            ),
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        oci_result = results[2]
        assert oci_result.status == "FAIL"
        assert "oci session refresh" in (oci_result.remediation or "")
        assert "AIDP_SESSION" in (oci_result.remediation or "")

    def test_oci_cli_missing_session_profile_fails(
        self, bundle_path: Path
    ) -> None:
        cfg = {
            "security_token_file": "/tmp/token",
            "key_file": "/tmp/key.pem",
        }
        with (
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.oci.config.from_file",
                return_value=cfg,
            ),
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.preflight.subprocess.run",
                side_effect=FileNotFoundError("oci"),
            ),
        ):
            results = run_local_preflight(
                bundle_path=bundle_path,
                config=_config(),
                env_name="dev",
                env=_env(),
            )
        # Plan §Step 5 — session-token profile + missing oci CLI = FAIL,
        # NOT a soft SKIP. SKIP here would mask an expired session as a
        # Phase-B 401.
        assert results[2].status == "FAIL"
        assert "CLI" in results[2].detail or "CLI" in (results[2].remediation or "")


# ---------------------------------------------------------------------------
# Phase B — remote preflight
# ---------------------------------------------------------------------------


def _client_with(*, list_clusters=None, start_cluster=None, wait_active=None) -> MagicMock:
    client = MagicMock(spec=[
        "list_clusters", "start_cluster", "wait_cluster_active", "get_cluster"
    ])
    if list_clusters is not None:
        if isinstance(list_clusters, BaseException):
            client.list_clusters.side_effect = list_clusters
        else:
            client.list_clusters.return_value = list_clusters
    if start_cluster is not None:
        if isinstance(start_cluster, BaseException):
            client.start_cluster.side_effect = start_cluster
        else:
            client.start_cluster.return_value = start_cluster
    if wait_active is not None:
        if isinstance(wait_active, BaseException):
            client.wait_cluster_active.side_effect = wait_active
    return client


class TestPhaseBRemotePreflight:
    def test_control_plane_unreachable_fails_and_skips_cluster(self) -> None:
        client = _client_with(
            list_clusters=AidpRestError("HTTP 401 body=bad signature"),
        )
        results = run_remote_preflight(client=client, env=_env())
        assert results[0].status == "FAIL"
        assert results[1].status == "SKIP"
        assert "region" in (results[0].remediation or "")

    def test_cluster_active_passes(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="cluster-uuid-1", display_name="dev", state="ACTIVE")
            ],
        )
        results = run_remote_preflight(client=client, env=_env())
        assert [r.status for r in results] == ["PASS", "PASS"]
        client.start_cluster.assert_not_called()
        client.wait_cluster_active.assert_not_called()

    def test_cluster_not_found_fails(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="other-uuid", display_name="other", state="ACTIVE")
            ],
        )
        results = run_remote_preflight(client=client, env=_env())
        assert results[1].status == "FAIL"
        assert "cluster-uuid-1" in results[1].detail

    def test_stopped_auto_start_invokes_start_and_wait(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="cluster-uuid-1", display_name="dev", state="STOPPED")
            ],
        )
        client.start_cluster.return_value = {}
        results = run_remote_preflight(
            client=client, env=_env(), auto_start_cluster=True
        )
        client.start_cluster.assert_called_once_with("cluster-uuid-1")
        client.wait_cluster_active.assert_called_once()
        assert results[1].status == "PASS"

    def test_stopped_no_auto_start_fails(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="cluster-uuid-1", display_name="dev", state="STOPPED")
            ],
        )
        results = run_remote_preflight(
            client=client, env=_env(), auto_start_cluster=False
        )
        assert results[1].status == "FAIL"
        client.start_cluster.assert_not_called()

    def test_auto_start_failure_surfaces(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="cluster-uuid-1", display_name="dev", state="STOPPED")
            ],
        )
        client.start_cluster.return_value = {}
        client.wait_cluster_active.side_effect = AidpRestError(
            "cluster transitioned to FAILED while waiting"
        )
        results = run_remote_preflight(
            client=client, env=_env(), auto_start_cluster=True
        )
        assert results[1].status == "FAIL"
        assert "FAILED" in results[1].detail

    def test_cluster_failed_state_no_auto_recovery(self) -> None:
        client = _client_with(
            list_clusters=[
                ClusterSummary(key="cluster-uuid-1", display_name="dev", state="FAILED")
            ],
        )
        results = run_remote_preflight(
            client=client, env=_env(), auto_start_cluster=True
        )
        assert results[1].status == "FAIL"
        client.start_cluster.assert_not_called()


class TestRender:
    def test_renders_all_results(self) -> None:
        out = render(
            [
                PreflightResult(name="x", status="PASS", detail="ok"),
                PreflightResult(
                    name="y",
                    status="FAIL",
                    detail="bad",
                    remediation="fix it",
                ),
            ]
        )
        assert "PASS x: ok" in out
        assert "FAIL y: bad" in out
        assert "fix it" in out
