"""CLI integration tests for `aidp-fusion-bundle content-pack {list,info,validate}`.

Tests invoke the CLI via subprocess so the end-to-end click → command →
loader/validator chain is exercised. Run with the editable-install Python
interpreter so the installed entry point is found.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parent.parent.parent


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    """Run `aidp-fusion-bundle <args>` via the current Python's module entry."""
    cmd = [
        sys.executable,
        "-m",
        "oracle_ai_data_platform_fusion_bundle.cli",
        *args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=PLUGIN_ROOT)


def test_cli_list_human_readable() -> None:
    result = _run_cli("content-pack", "list")
    assert result.returncode == 0, result.stderr
    # Starter pack should appear.
    assert "fusion-finance-starter" in result.stdout
    assert "0.1.0" in result.stdout


def test_cli_list_json() -> None:
    result = _run_cli("content-pack", "list", "--json")
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    packs = {p["name"]: p for p in data["packs"]}
    assert "fusion-finance-starter" in packs
    assert packs["fusion-finance-starter"]["version"] == "0.1.0"


def test_cli_info_human_readable() -> None:
    result = _run_cli("content-pack", "info", "fusion-finance-starter")
    assert result.returncode == 0, result.stderr
    assert "fusion-finance-starter" in result.stdout
    assert "3 silver, 3 gold" in result.stdout


def test_cli_info_json() -> None:
    result = _run_cli("content-pack", "info", "fusion-finance-starter", "--json")
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data["id"] == "fusion-finance-starter"
    assert data["version"] == "0.1.0"
    assert set(data["nodes"]["silver"]) == {"dim_supplier", "dim_account", "dim_calendar"}
    assert set(data["nodes"]["gold"]) == {"gl_balance", "supplier_spend", "ap_aging"}
    assert set(data["dashboards"]) == {"executive_cfo", "payables"}
    assert len(data["pack_hash"]) == 64  # sha256 hex


def test_cli_info_unknown_pack_exits_1() -> None:
    result = _run_cli("content-pack", "info", "nonexistent-pack")
    assert result.returncode == 1


def test_cli_validate_starter_pack_passes() -> None:
    result = _run_cli("content-pack", "validate", "fusion-finance-starter")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "validates clean" in result.stdout


def test_cli_validate_starter_pack_json() -> None:
    result = _run_cli("content-pack", "validate", "fusion-finance-starter", "--json")
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data["pack"] == "fusion-finance-starter"
    assert data["ok"] is True
    assert data["errors"] == []


def test_cli_validate_broken_overlay_exits_2(tmp_path: Path) -> None:
    """Validate against a hand-crafted broken overlay returns exit 2 + AIDPF code in JSON."""
    import yaml

    pack_root = tmp_path / "broken-pack"
    pack_root.mkdir()
    (pack_root / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "id": "broken-pack",
                "version": "0.1.0",
                "compatibility": {"pluginMinVersion": "0.3.0"},
                "extends": "fusion-finance-starter@0.1.0",
                "overrides": {
                    # Override a node that doesn't exist in base.
                    "silver/dim_nonexistent": {"profile": "finance-default"},
                },
            }
        )
    )

    # The validate verb only loads the named pack; orphan-override detection
    # currently lives in the merger. Here we exercise validation of a syntactically
    # broken pack via direct path argument.
    bad_path = tmp_path / "bad"
    bad_path.mkdir()
    (bad_path / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "id": "bad-pack",
                "version": "not-semver",  # AIDPF-2002
                "compatibility": {"pluginMinVersion": "0.3.0"},
            }
        )
    )
    result = _run_cli("content-pack", "validate", str(bad_path), "--json")
    assert result.returncode == 2, result.stdout + result.stderr
    data = json.loads(result.stdout)
    assert data["errors"], "expected at least one validation error"
    assert any("AIDPF-2002" in e.get("message", "") for e in data["errors"])
