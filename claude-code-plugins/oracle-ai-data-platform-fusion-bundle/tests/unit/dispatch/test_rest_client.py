"""P1.5ε §Step 2 — dispatch.rest_client signer-factory tests.

The signer factory is the load-bearing addition over the old skill-folder
client: it picks ``SecurityTokenSigner`` for session-token profiles
(``oci session authenticate`` flow — the laptop-CLI default) and the
classic ``Signer`` for API-key profiles. Without this, an ``AIDP_SESSION``
profile passes ``oci session validate`` but every REST call returns 401
because the wrong signer was constructed.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import oci
import pytest

from oracle_ai_data_platform_fusion_bundle.dispatch.rest_client import (
    AidpRestClient,
    AidpRestError,
    _build_signer,
)


class TestBuildSignerApiKeyProfile:
    """Profiles without ``security_token_file`` use the API-key signer."""

    def test_returns_api_key_signer(self) -> None:
        cfg = {
            "tenancy": "ocid1.tenancy.oc1..xxx",
            "user": "ocid1.user.oc1..yyy",
            "fingerprint": "aa:bb:cc",
            "key_file": "/path/to/key.pem",
        }
        sentinel = MagicMock(name="api-key-signer")
        with patch(
            "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.signer.Signer",
            return_value=sentinel,
        ) as mock_signer:
            signer = _build_signer(cfg)
        assert signer is sentinel
        mock_signer.assert_called_once_with(
            tenancy="ocid1.tenancy.oc1..xxx",
            user="ocid1.user.oc1..yyy",
            fingerprint="aa:bb:cc",
            private_key_file_location="/path/to/key.pem",
        )

    def test_empty_string_token_file_treated_as_absent(self) -> None:
        # OCI config sometimes round-trips an absent value as "".
        cfg = {
            "security_token_file": "",
            "tenancy": "ocid1.tenancy.oc1..xxx",
            "user": "ocid1.user.oc1..yyy",
            "fingerprint": "aa:bb:cc",
            "key_file": "/path/to/key.pem",
        }
        with patch(
            "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.signer.Signer",
            return_value=MagicMock(),
        ) as mock_signer:
            _build_signer(cfg)
        mock_signer.assert_called_once()


class TestBuildSignerSessionTokenProfile:
    """Profiles with ``security_token_file`` use SecurityTokenSigner."""

    def test_returns_security_token_signer(self, tmp_path: Path) -> None:
        token_file = tmp_path / "token"
        token_file.write_text("eyJhbGciOiJSUzI1NiJ9.payload.sig\n")
        key_file = tmp_path / "key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\n")
        cfg = {
            "security_token_file": str(token_file),
            "key_file": str(key_file),
            # tenancy/user/fingerprint may be absent in session-token profiles
        }
        sentinel_key = MagicMock(name="parsed-key")
        sentinel_signer = MagicMock(name="security-token-signer")
        with (
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.signer.load_private_key_from_file",
                return_value=sentinel_key,
            ) as mock_load,
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.auth.signers.SecurityTokenSigner",
                return_value=sentinel_signer,
            ) as mock_signer,
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.signer.Signer",
                side_effect=AssertionError("API-key signer must not be called"),
            ),
        ):
            signer = _build_signer(cfg)
        assert signer is sentinel_signer
        mock_load.assert_called_once_with(str(key_file))
        # Token is read from the file and passed to the signer constructor.
        args, _ = mock_signer.call_args
        assert args[0] == "eyJhbGciOiJSUzI1NiJ9.payload.sig"
        assert args[1] is sentinel_key

    def test_missing_token_file_raises_aidp_rest_error(
        self, tmp_path: Path
    ) -> None:
        cfg = {
            "security_token_file": str(tmp_path / "no-such-file"),
            "key_file": str(tmp_path / "key.pem"),
        }
        with pytest.raises(AidpRestError, match="oci session refresh"):
            _build_signer(cfg)

    def test_empty_token_file_raises_aidp_rest_error(
        self, tmp_path: Path
    ) -> None:
        token_file = tmp_path / "token"
        token_file.write_text("")
        cfg = {
            "security_token_file": str(token_file),
            "key_file": str(tmp_path / "key.pem"),
        }
        with pytest.raises(AidpRestError, match="empty"):
            _build_signer(cfg)

    def test_tilde_expansion(self, tmp_path: Path, monkeypatch) -> None:
        # Session-token profiles often use ~/.oci/sessions/<name>/token paths.
        # Verify that ``~`` is expanded so the file actually opens.
        monkeypatch.setenv("HOME", str(tmp_path))
        sessions_dir = tmp_path / ".oci" / "sessions" / "AIDP_SESSION"
        sessions_dir.mkdir(parents=True)
        token_file = sessions_dir / "token"
        token_file.write_text("real-token")
        key_file = sessions_dir / "oci_api_key.pem"
        key_file.write_text("-----BEGIN PRIVATE KEY-----\n")
        cfg = {
            "security_token_file": "~/.oci/sessions/AIDP_SESSION/token",
            "key_file": str(key_file),
        }
        with (
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.signer.load_private_key_from_file",
                return_value=MagicMock(),
            ),
            patch(
                "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.auth.signers.SecurityTokenSigner",
                return_value=MagicMock(),
            ) as mock_signer,
        ):
            _build_signer(cfg)
        # Token actually got read despite the ~-relative config value.
        args, _ = mock_signer.call_args
        assert args[0] == "real-token"


# ---------------------------------------------------------------------------
# P1.5ε-fix8 — per-call timeout kwarg on get_run + fetch_output
# ---------------------------------------------------------------------------
#
# The underlying _request(..., timeout=...) already supports it; these tests
# lock that the public methods plumb the kwarg through cleanly so the
# diagnose-on-timeout enrichment in dispatch_via_rest can bound each
# diagnostic HTTP call. Without the kwarg, time.monotonic() budgets are
# meaningless against a blocking requests call.


def _make_client():
    """Build a client without touching ~/.oci/config."""
    from oracle_ai_data_platform_fusion_bundle.dispatch.rest_client import (
        AidpRestClient,
    )

    with (
        patch(
            "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.config.from_file",
            return_value={
                "tenancy": "t",
                "user": "u",
                "fingerprint": "f",
                "key_file": "/tmp/k",
            },
        ),
        patch(
            "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client.oci.signer.Signer",
            return_value=MagicMock(),
        ),
    ):
        return AidpRestClient(
            region="us-ashburn-1",
            aidp_id="ocid1.datalake.oc1.iad.test",
            workspace_key="00000000-0000-0000-0000-000000000000",
        )


class TestGetRunTimeout:
    def test_get_run_forwards_timeout_to_underlying_request(self) -> None:
        client = _make_client()
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"state": {"status": "RUNNING"}}
        with patch.object(client, "_request", return_value=mock_resp) as spy:
            client.get_run("run-key-1", timeout=5)
        _, kwargs = spy.call_args
        assert kwargs.get("timeout") == 5

    def test_get_run_default_timeout_is_none(self) -> None:
        """Locks the back-compat contract: no `timeout=` → `None` forwarded
        so `_request` falls back to ``self.request_timeout_s``. Existing
        callers (`poll_run`, skill consumers) keep today's behavior."""
        client = _make_client()
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"state": {"status": "RUNNING"}}
        with patch.object(client, "_request", return_value=mock_resp) as spy:
            client.get_run("run-key-1")
        _, kwargs = spy.call_args
        assert kwargs.get("timeout") is None


class TestFetchOutputTimeout:
    def test_fetch_output_forwards_timeout_to_underlying_request(self) -> None:
        client = _make_client()
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"data": [{"value": '{"cells":[]}'}]}
        with patch.object(client, "_request", return_value=mock_resp) as spy:
            client.fetch_output("task-run-key-1", timeout=5)
        _, kwargs = spy.call_args
        assert kwargs.get("timeout") == 5

    def test_fetch_output_default_timeout_is_none(self) -> None:
        client = _make_client()
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"data": [{"value": '{"cells":[]}'}]}
        with patch.object(client, "_request", return_value=mock_resp) as spy:
            client.fetch_output("task-run-key-1")
        _, kwargs = spy.call_args
        assert kwargs.get("timeout") is None


# ---------------------------------------------------------------------------
# P1.5ε-fix5 — parse_marker regex fallback for the TC27 trap
# ---------------------------------------------------------------------------
# Background (TC27 known fragility): the orchestrator emits its marker via
# print(json.dumps(...)) on the cluster. When the run includes a failed
# step, the payload contains error_message=repr(exc) (= 'RuntimeError("…")'
# with nested quotes). AIDP's notebook runtime captures stdout into
# display_data text/plain and strips JSON-escape backslashes from those
# nested quotes, producing invalid JSON. parse_marker's regex fallback
# recovers run_id so the operator can resume via --resume <id>.


_MARKER_BEGIN = "AIDP_LIVE_TEST_RESULT_BEGIN"
_MARKER_END = "AIDP_LIVE_TEST_RESULT_END"


def _make_executed_notebook(body: str) -> dict:
    """Wrap a marker body in a minimal executed-notebook dict shaped like
    AIDP's fetchOutput response (text channel — bypasses display_data so
    we control the body bytes exactly)."""
    return {
        "cells": [
            {
                "outputs": [
                    {
                        "text": f"{_MARKER_BEGIN} {body} {_MARKER_END}\n",
                    }
                ]
            }
        ]
    }


class TestParseMarkerRegexFallback:
    def test_parse_marker_clean_json_unaffected(self) -> None:
        """Regression lock — the fallback only activates after
        ``json.loads`` fails. Clean JSON returns the parsed dict
        directly (existing pre-fix5 behavior)."""
        body = '{"run_id":"clean-abc","steps":[]}'
        nb = _make_executed_notebook(body)
        marker = AidpRestClient.parse_marker(
            nb, begin=_MARKER_BEGIN, end=_MARKER_END,
        )
        assert marker == {"run_id": "clean-abc", "steps": []}
        assert marker is not None
        assert "_marker_parse_failed" not in marker

    def test_parse_marker_recovers_run_id_from_malformed_json(self) -> None:
        """TC27-shaped trap — AIDP stripped the JSON-escape backslashes
        from a failed step's error_message=repr(exc). The body is no
        longer parseable JSON but still contains "run_id": "<id>". The
        regex fallback recovers the run_id; the synthetic sentinel
        signals the dispatcher to raise DispatchMarkerDegradedError."""
        body = (
            '{"run_id":"recovered-xyz","steps":['
            '{"error_message":"RuntimeError("induced fail")"}'
            ']}'
        )
        nb = _make_executed_notebook(body)
        marker = AidpRestClient.parse_marker(
            nb, begin=_MARKER_BEGIN, end=_MARKER_END,
        )
        assert marker is not None
        assert marker["run_id"] == "recovered-xyz"
        assert marker["_marker_parse_failed"] is True
        assert "RuntimeError" in marker["_raw_marker"]

    def test_parse_marker_malformed_with_no_run_id_still_raises(self) -> None:
        """If the regex also can't find a run_id, the original
        ``json.JSONDecodeError`` propagates — the dispatcher's caller
        still converts to ``DispatchMarkerMissingError`` (existing
        behavior preserved for the unrecoverable case)."""
        import json as _json

        body = '{"steps":[{"error_message":"RuntimeError("oh no")"}]}'
        nb = _make_executed_notebook(body)
        with pytest.raises(_json.JSONDecodeError):
            AidpRestClient.parse_marker(
                nb, begin=_MARKER_BEGIN, end=_MARKER_END,
            )
