"""Regression tests for the aidp-rest skill's REST primitives (PR #4 review fix).

The skill sits outside the plugin's installed package (under
``.claude/skills/aidp-rest/``), so import via sys.path manipulation. Tests
here pin the public contract that downstream skills (fusion-tc26-run,
future fusion-medallion-doctor, etc.) depend on.

Reviewer catches captured here:
  - ``fetch_output`` must RAISE on non-200, not silently return ``""``.
    Otherwise an AIDP job that ran SUCCESS but whose output we can't fetch
    becomes a silent evidence-capture gap (dispatch.py exits 0 with empty
    marker). Tests pin both the 404 case and the legitimate empty-data case.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# The skill lives outside the standard package layout; import via sys.path.
_SKILL_DIR = (Path(__file__).resolve().parents[2]
              / ".claude" / "skills" / "aidp-rest")
sys.path.insert(0, str(_SKILL_DIR))
from client import AidpRestClient, AidpRestError  # noqa: E402


# Canonical patch target — the skill's ``client`` module is a re-export shim
# that imports from ``oracle_ai_data_platform_fusion_bundle.dispatch.rest_client``
# (P1.5ε §Step 2). The ``oci`` module is imported by the canonical module,
# not by the shim — patch it where it's looked up.
_REST_CLIENT_MOD = "oracle_ai_data_platform_fusion_bundle.dispatch.rest_client"


@pytest.fixture
def client():
    """Build a client without touching ~/.oci/config — patch the signer."""
    with patch(f"{_REST_CLIENT_MOD}.oci.config.from_file", return_value={
        "tenancy": "t", "user": "u", "fingerprint": "f", "key_file": "/tmp/k",
    }), patch(f"{_REST_CLIENT_MOD}.oci.signer.Signer", return_value=MagicMock()):
        return AidpRestClient(
            region="us-ashburn-1",
            aidp_id="ocid1.datalake.oc1.iad.aaaa",
            workspace_key="00000000-0000-0000-0000-000000000000",
            log=lambda s, **kw: None,
        )


# ---------------------------------------------------------------------------
# fetch_output — silent-failure regression (PR #4 review catch #2)
# ---------------------------------------------------------------------------


def test_fetch_output_raises_on_404(client):
    """A 404 from fetchOutput must raise AidpRestError, NOT silently return ""."""
    mock_resp = MagicMock(status_code=404, text="Unknown resource X")
    with patch.object(client, "_request", return_value=mock_resp):
        with pytest.raises(AidpRestError, match="HTTP 404"):
            client.fetch_output("task-run-key")


def test_fetch_output_raises_on_500(client):
    """A 500 from fetchOutput must raise too — masks evidence-capture gaps."""
    mock_resp = MagicMock(status_code=500, text="Internal error")
    with patch.object(client, "_request", return_value=mock_resp):
        with pytest.raises(AidpRestError, match="HTTP 500"):
            client.fetch_output("task-run-key")


def test_fetch_output_returns_empty_string_on_200_with_empty_data(client):
    """Empty ``data`` array at 200 is a legitimate "notebook printed nothing"
    case — return ``""`` so the caller can assert presence-of-marker downstream.
    This is the ONLY shape that may return ``""`` cleanly.
    """
    mock_resp = MagicMock(status_code=200)
    mock_resp.json.return_value = {"data": []}
    with patch.object(client, "_request", return_value=mock_resp):
        assert client.fetch_output("task-run-key") == ""


def test_fetch_output_returns_notebook_json_on_success(client):
    """Happy-path: 200 with data[0].value carries the executed notebook JSON."""
    mock_resp = MagicMock(status_code=200)
    mock_resp.json.return_value = {
        "data": [{"type": "NOTEBOOK", "value": '{"cells":[]}'}],
    }
    with patch.object(client, "_request", return_value=mock_resp):
        assert client.fetch_output("task-run-key") == '{"cells":[]}'


# ---------------------------------------------------------------------------
# parse_marker — sanity (used by dispatch.py to detect success-without-marker)
# ---------------------------------------------------------------------------


def test_parse_marker_returns_none_when_marker_absent():
    """The dispatch.py "success but no marker" exit-2 logic depends on this."""
    nb = {"cells": [{"outputs": [{"text": "no marker here"}]}]}
    assert AidpRestClient.parse_marker(nb, begin="X_BEGIN", end="X_END") is None


def test_parse_marker_extracts_json_payload():
    nb = {
        "cells": [{
            "outputs": [{
                "text": 'before X_BEGIN {"ok": true} X_END after',
            }],
        }],
    }
    assert AidpRestClient.parse_marker(nb, begin="X_BEGIN", end="X_END") == {"ok": True}
