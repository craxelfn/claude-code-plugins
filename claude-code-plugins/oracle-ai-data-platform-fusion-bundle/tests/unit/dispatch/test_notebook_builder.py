"""P1.5ε §Step 4 — dispatch/notebook_builder.py tests.

These lock the run-cell contract that the laptop-side dispatcher and the
schema-side ``RunSummary.from_marker_dict`` depend on. The most important
one — ``test_run_cell_calls_to_marker_dict_not_asdict`` — prevents a
regression where someone hand-rolls a subset dict and silently drops
``schema_version`` / ``watermark_used`` / etc.
"""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_bundle.dispatch.notebook_builder import (
    MARKER_BEGIN,
    MARKER_END,
    build_notebook,
)


@pytest.fixture
def wheel(tmp_path: Path) -> Path:
    p = tmp_path / "oracle_ai_data_platform_fusion_bundle-0.2.0-py3-none-any.whl"
    p.write_bytes(b"PK\x03\x04 fake wheel bytes")
    return p


def _all_source(nb: dict) -> str:
    """Concatenate every code-cell source into one big string for substring
    assertions. Tests should match by substring; the exact whitespace
    layout is intentionally not asserted (it's an implementation detail
    of the template strings)."""
    out: list[str] = []
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            out.extend(cell["source"])
    return "".join(out)


class TestNotebookStructure:
    def test_nbformat_metadata(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="apiVersion: aidp-fusion-bundle/v1\n",
            mode="seed",
            datasets=None,
            layers=None,
        )
        assert nb["nbformat"] == 4
        assert nb["nbformat_minor"] == 5
        # 1 markdown + 4 code cells.
        assert len(nb["cells"]) == 5
        cell_types = [c["cell_type"] for c in nb["cells"]]
        assert cell_types == ["markdown", "code", "code", "code", "code"]

    def test_title_in_markdown_cell(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
            title="TC29 narrow probe",
        )
        md_source = "".join(nb["cells"][0]["source"])
        assert "TC29 narrow probe" in md_source


class TestInstallCell:
    def test_install_cell_inlines_wheel_base64(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        install = "".join(nb["cells"][1]["source"])
        expected_b64 = base64.b64encode(wheel.read_bytes()).decode()
        assert expected_b64 in install

    def test_install_cell_uses_wheel_filename(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        install = "".join(nb["cells"][1]["source"])
        assert wheel.name in install


class TestCredsCell:
    def test_default_secret_name_and_key(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        creds = "".join(nb["cells"][2]["source"])
        assert "name='fusion_bicc_password'" in creds
        assert "key='password'" in creds

    def test_secret_override(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
            bicc_secret_name="custom_secret",
            bicc_secret_key="custom_key",
        )
        creds = "".join(nb["cells"][2]["source"])
        assert "name='custom_secret'" in creds
        assert "key='custom_key'" in creds

    def test_bundle_yaml_injected_via_repr(self, wheel: Path) -> None:
        # repr() preserves embedded newlines; the cluster-side write_text
        # gets the operator's bundle byte-for-byte.
        yaml_body = "apiVersion: aidp-fusion-bundle/v1\nproject: test\n"
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml=yaml_body,
            mode="seed",
            datasets=None,
            layers=None,
        )
        creds = "".join(nb["cells"][2]["source"])
        assert repr(yaml_body) in creds


class TestRunCell:
    def test_mode_injected(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="incremental",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "mode='incremental'" in run

    def test_datasets_filter_injected(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=["ap_invoices"],
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "datasets=['ap_invoices']" in run

    def test_layers_filter_injected(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=["gold"],
        )
        run = "".join(nb["cells"][3]["source"])
        assert "layers=['gold']" in run

    def test_none_filters_render_as_none_not_null(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "datasets=None" in run
        assert "layers=None" in run
        assert "datasets=null" not in run

    def test_resume_run_id_hardcoded_none(self, wheel: Path) -> None:
        # §3.1 — REST-dispatch resume is out of scope in P1.5ε.
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "resume_run_id=None" in run

    def test_marker_emit_present(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert MARKER_BEGIN in run
        assert MARKER_END in run

    def test_run_cell_calls_to_marker_dict_not_asdict(self, wheel: Path) -> None:
        # Locks the schema contract: the run-cell uses the canonical
        # serializer, NOT a hand-rolled dict literal or dataclasses.asdict
        # (which would drop schema_version + fail on datetime fields).
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "summary.to_marker_dict()" in run
        assert "dataclasses.asdict" not in run
        assert "asdict(summary)" not in run

    def test_no_resume_run_id_parameter_on_builder(self) -> None:
        # Locks the §3.1 scope boundary at the API level.
        import inspect

        sig = inspect.signature(build_notebook)
        assert "resume_run_id" not in sig.parameters


class TestVerifyCell:
    def test_imports_load_bundle_from_schema_not_runtime(
        self, wheel: Path
    ) -> None:
        # The verify cell should use the canonical schema-level location
        # (P1.5ε §Step 1d), not the back-compat re-export.
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        verify = "".join(nb["cells"][4]["source"])
        assert "schema.bundle import load_bundle" in verify
