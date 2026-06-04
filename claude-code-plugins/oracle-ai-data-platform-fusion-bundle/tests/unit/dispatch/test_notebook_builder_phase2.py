"""Phase 2 unit tests for ``dispatch/notebook_builder.py``.

Covers:
* build_notebook returns an nbformat-4 dict (NOT a string) — contract preserved.
* execution_backend="content-pack" emits the new bootstrap cell with
  base64-encoded primitives; raw values never appear in cell source.
* execution_backend="legacy-python" omits the bootstrap cell — cell list
  shape identical to Phase 1 baseline.
* Invariant: content-pack requires all three primitives non-None;
  legacy-python forbids them.
* Adversarial round-trip: profile YAML containing triple-quotes /
  backslashes / SQL-injection content survives base64 round-trip
  byte-for-byte and never appears as a raw substring of the notebook source.
"""

from __future__ import annotations

import base64
import json
import pathlib
import re

import pytest

from oracle_ai_data_platform_fusion_bundle.dispatch.notebook_builder import (
    _build_content_pack_bootstrap_cell,
    build_notebook,
)


@pytest.fixture
def tmp_wheel(tmp_path: pathlib.Path) -> pathlib.Path:
    """build_notebook reads the wheel file to base64-encode it. Provide a
    minimal placeholder so the install cell builds without I/O errors."""
    wheel = tmp_path / "fake.whl"
    wheel.write_bytes(b"PK\x03\x04\x00\x00\x00\x00")
    return wheel


def _minimal_args(wheel_path: pathlib.Path, **overrides) -> dict:
    base = dict(
        wheel_path=wheel_path,
        bundle_yaml="apiVersion: aidp-fusion-bundle/v1\nproject: x\n",
        mode="seed",
        datasets=None,
        layers=None,
    )
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Return type contract: nbformat-4 dict (NOT a string)
# ---------------------------------------------------------------------------


class TestReturnType:
    def test_returns_dict_legacy_backend(self, tmp_wheel) -> None:
        result = build_notebook(**_minimal_args(tmp_wheel))
        assert isinstance(result, dict)
        assert result["nbformat"] == 4
        assert "cells" in result

    def test_returns_dict_content_pack_backend(self, tmp_wheel) -> None:
        result = build_notebook(
            **_minimal_args(
                tmp_wheel,
                execution_backend="content-pack",
                profile_yaml="schemaVersion: 1\ntenant: x\n",
                pack_files={"__layer_0__/pack.yaml": "id: x\nversion: 1.0.0\n"},
                pack_manifest={"chain_layers": [], "entry_layer_index": 0},
            )
        )
        assert isinstance(result, dict)
        assert result["nbformat"] == 4


# ---------------------------------------------------------------------------
# Cell-list shape
# ---------------------------------------------------------------------------


class TestCellListShape:
    def test_legacy_backend_omits_bootstrap_cell(self, tmp_wheel) -> None:
        nb = build_notebook(**_minimal_args(tmp_wheel, execution_backend="legacy-python"))
        # markdown + install + creds + run + verify = 5 cells.
        assert len(nb["cells"]) == 5

    def test_content_pack_backend_inserts_bootstrap_cell(self, tmp_wheel) -> None:
        nb = build_notebook(
            **_minimal_args(
                tmp_wheel,
                execution_backend="content-pack",
                profile_yaml="schemaVersion: 1\ntenant: x\n",
                pack_files={"__layer_0__/pack.yaml": "id: x\nversion: 1.0.0\n"},
                pack_manifest={"chain_layers": [], "entry_layer_index": 0},
            )
        )
        # markdown + install + creds + bootstrap + run + verify = 6 cells.
        assert len(nb["cells"]) == 6


# ---------------------------------------------------------------------------
# Invariant: content-pack requires primitives; legacy-python forbids them
# ---------------------------------------------------------------------------


class TestInvariantChecks:
    def test_content_pack_missing_profile_yaml_raises(self, tmp_wheel) -> None:
        with pytest.raises(AssertionError):
            build_notebook(
                **_minimal_args(
                    tmp_wheel,
                    execution_backend="content-pack",
                    profile_yaml=None,
                    pack_files={"x": "y"},
                    pack_manifest={"a": 1},
                )
            )

    def test_content_pack_missing_pack_files_raises(self, tmp_wheel) -> None:
        with pytest.raises(AssertionError):
            build_notebook(
                **_minimal_args(
                    tmp_wheel,
                    execution_backend="content-pack",
                    profile_yaml="x",
                    pack_files=None,
                    pack_manifest={"a": 1},
                )
            )

    def test_legacy_python_with_pack_files_raises(self, tmp_wheel) -> None:
        with pytest.raises(AssertionError):
            build_notebook(
                **_minimal_args(
                    tmp_wheel,
                    execution_backend="legacy-python",
                    pack_files={"x": "y"},
                )
            )


# ---------------------------------------------------------------------------
# Visible literal: orchestrator.run kwarg
# ---------------------------------------------------------------------------


class TestRunCellLiteral:
    def test_run_cell_contains_execution_backend_literal(self, tmp_wheel) -> None:
        nb = build_notebook(
            **_minimal_args(
                tmp_wheel,
                execution_backend="content-pack",
                profile_yaml="schemaVersion: 1\ntenant: x\n",
                pack_files={"x": "y"},
                pack_manifest={"a": 1},
            )
        )
        all_sources = "".join(
            "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
            for c in nb["cells"] if c["cell_type"] == "code"
        )
        assert 'execution_backend="content-pack"' in all_sources

    def test_legacy_backend_run_cell_has_legacy_python_literal(self, tmp_wheel) -> None:
        nb = build_notebook(**_minimal_args(tmp_wheel, execution_backend="legacy-python"))
        all_sources = "".join(
            "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
            for c in nb["cells"] if c["cell_type"] == "code"
        )
        assert 'execution_backend="legacy-python"' in all_sources


# ---------------------------------------------------------------------------
# Adversarial round-trip — no raw payload leakage
# ---------------------------------------------------------------------------


class TestAdversarialRoundTrip:
    def test_profile_yaml_round_trips_via_base64(self) -> None:
        """Round-trip: take a profile YAML containing triple-quotes and
        backslashes and SQL-injection style values, encode it, embed in
        cell source, decode it back — equality holds byte-for-byte. The
        raw payload MUST NOT appear as a substring of the cell source.
        """
        adversarial = (
            'schemaVersion: 1\n'
            'tenant: adversarial\n'
            'pinnedAt: 2026-01-01T00:00:00+00:00\n'
            'bronzeSchemaFingerprint: "sha256:abc"\n'
            'malicious_value: "\'; DROP TABLE evil; --"\n'
            'backslash: "C:\\\\path"\n'
        )
        bootstrap = _build_content_pack_bootstrap_cell(
            profile_yaml=adversarial,
            pack_files={"__layer_0__/pack.yaml": "id: p\nversion: 1.0.0\n"},
            pack_manifest={"chain_layers": [], "entry_layer_index": 0},
        )

        # Extract the _PROFILE_YAML base64 token.
        m = re.search(r"_PROFILE_YAML = _b64\.b64decode\(['\"]([^'\"]+)['\"]\)", bootstrap)
        assert m is not None
        token = m.group(1)

        decoded = base64.b64decode(token).decode("utf-8")
        assert decoded == adversarial

        # No-raw-payload-leakage canaries.
        assert "DROP TABLE" not in bootstrap
        assert "malicious_value" not in bootstrap

    def test_pack_files_dict_round_trips_via_base64(self) -> None:
        pack_files = {
            "__layer_0__/pack.yaml": "id: malicious-pack\nversion: 1.0.0\n",
            "__layer_0__/silver/dim_x.sql": "MERGE INTO target USING src ON 1=1 WHEN MATCHED THEN DELETE",
        }
        bootstrap = _build_content_pack_bootstrap_cell(
            profile_yaml="x: 1\n",
            pack_files=pack_files,
            pack_manifest={"chain_layers": [], "entry_layer_index": 0},
        )

        m = re.search(
            r"_PACK_FILES = _json\.loads\(_b64\.b64decode\(['\"]([^'\"]+)['\"]\)",
            bootstrap,
        )
        assert m is not None
        token = m.group(1)
        decoded = json.loads(base64.b64decode(token).decode("utf-8"))
        assert decoded == pack_files

        # Raw SQL must NOT appear as substring.
        assert "MERGE INTO target" not in bootstrap
