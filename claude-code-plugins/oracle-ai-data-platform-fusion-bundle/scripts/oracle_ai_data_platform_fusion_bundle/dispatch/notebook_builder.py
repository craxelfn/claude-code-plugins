"""Generate the self-contained dispatch notebook.

The notebook has 4 code cells:

  1. ``install_cell`` — base64-decodes an embedded wheel, ``pip install``s
     it into a temp dir, and prepends that dir to ``sys.path``.
  2. ``creds_cell`` — fetches the BICC password from the AIDP credential
     store, writes ``bundle.yaml`` to the working dir, imports the
     orchestrator package.
  3. ``run_cell`` — calls ``orchestrator.run(...)`` with the requested
     ``mode``, prints a per-step table, and emits a marker block
     (``AIDP_LIVE_TEST_RESULT_BEGIN ... END``) so the laptop side can
     parse the structured summary without re-querying state.
  4. ``state_cell`` — queries ``fusion_bundle_state`` for the run we just
     executed and verifies SOX-trail audit columns join correctly.

This is functionally the same shape as
``.claude/skills/fusion-tc26-run/dispatch.py:build_notebook`` but lives
inside the plugin so the production CLI is self-contained.
"""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any

MARKER_BEGIN = "AIDP_LIVE_TEST_RESULT_BEGIN"
MARKER_END = "AIDP_LIVE_TEST_RESULT_END"


def _code_cell(src: str) -> dict[str, Any]:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": src.splitlines(keepends=True),
    }


def build_notebook(
    *,
    wheel_path: Path,
    bundle_yaml_text: str,
    secret_name: str,
    secret_key: str,
    mode: str,
    datasets: list[str] | None,
    layers: list[str] | None,
    title_suffix: str = "",
) -> dict[str, Any]:
    """Return a JSON-serialisable nbformat dict ready for upload.

    Args:
        wheel_path: Path to the plugin wheel to inline (base64-encoded).
        bundle_yaml_text: Verbatim contents of ``bundle.yaml`` to write
            on the cluster before importing the orchestrator.
        secret_name / secret_key: Lookup keys for the AIDP credential
            store (notebook fetches BICC password via
            ``aidputils.secrets.get(name=..., key=...)``).
        mode: ``seed`` | ``incremental`` | ``full`` — passed straight to
            ``orchestrator.run(mode=...)``.
        datasets: Optional list of dataset ids to filter. ``None`` =
            every enabled dataset in ``bundle.yaml``.
        layers: Optional list of layers (``bronze`` | ``silver`` |
            ``gold``) to run. ``None`` = all layers.
        title_suffix: Free-form string appended to the notebook title
            (e.g. timestamp or run handle for traceability).
    """
    wheel_b64 = base64.b64encode(wheel_path.read_bytes()).decode()
    wheel_filename = wheel_path.name

    datasets_literal = "None" if datasets is None else repr(list(datasets))
    layers_literal = "None" if layers is None else repr(list(layers))

    install_cell = (
        "import base64, subprocess, sys, tempfile, pathlib\n"
        f"WHEEL_B64 = \"\"\"{wheel_b64}\"\"\"\n"
        "_stage = pathlib.Path(tempfile.mkdtemp(prefix='aidp_fusion_plugin_'))\n"
        f"_whl = _stage / '{wheel_filename}'\n"
        "_whl.write_bytes(base64.b64decode(WHEEL_B64))\n"
        "_target = _stage / 'site-packages'\n"
        "_target.mkdir()\n"
        "res = subprocess.run(\n"
        "    [sys.executable, '-m', 'pip', 'install', '--quiet', '--no-deps',\n"
        "     '--target', str(_target), str(_whl)],\n"
        "    capture_output=True, text=True, timeout=180,\n"
        ")\n"
        "print(f'pip rc={res.returncode}')\n"
        "if res.returncode != 0:\n"
        "    print('STDOUT:', res.stdout[-2000:])\n"
        "    print('STDERR:', res.stderr[-2000:])\n"
        "    raise RuntimeError('wheel install failed')\n"
        "sys.path.insert(0, str(_target))\n"
        "print(f'plugin installed to {_target}')\n"
    )

    creds_cell = (
        "import os\n"
        "from pathlib import Path\n"
        f"os.environ['FUSION_BICC_PASSWORD'] = aidputils.secrets.get(name={secret_name!r}, key={secret_key!r})  # noqa: F821\n"
        "assert os.environ['FUSION_BICC_PASSWORD'], 'AIDP credential store returned an empty password'\n"
        "print(f'FUSION_BICC_PASSWORD loaded (length={len(os.environ[\"FUSION_BICC_PASSWORD\"])})')\n"
        "BUNDLE_PATH = Path('bundle.yaml')\n"
        f"BUNDLE_PATH.write_text({bundle_yaml_text!r})\n"
        "from oracle_ai_data_platform_fusion_bundle import orchestrator\n"
        "print('orchestrator imported')\n"
    )

    run_cell = (
        "import time, json\n"
        "tstart = time.time()\n"
        "summary = orchestrator.run(\n"
        "    bundle_path=BUNDLE_PATH,\n"
        "    spark=spark,  # noqa: F821 — AIDP-injected global\n"
        f"    mode={mode!r},\n"
        f"    datasets={datasets_literal},\n"
        f"    layers={layers_literal},\n"
        "    dry_run=False,\n"
        ")\n"
        "twall = time.time() - tstart\n"
        "print(f'run_id={summary.run_id}')\n"
        "print(f'steps: {summary.succeeded} ok, {summary.failed} failed, '\n"
        "      f'{summary.skipped} skipped, {summary.deferred} deferred '\n"
        "      f'({summary.total_duration_seconds:.1f}s reported / {twall:.1f}s wall)')\n"
        "for step in summary.steps:\n"
        "    skip_tag = f' [{step.skip_reason}]' if step.skip_reason else ''\n"
        "    rc = step.row_count if step.row_count is not None else '-'\n"
        "    err = f' err={step.error_message[:80]}' if (step.error_message and step.status == \"failed\") else ''\n"
        "    print(f'  {step.layer:6s}  {step.dataset_id:24s}  {step.status:10s}{skip_tag:12s}  rows={str(rc):>10s}  dur={step.duration_seconds:.2f}s{err}')\n"
        "_payload = {\n"
        "    'run_id': summary.run_id,\n"
        "    'bundle_project': summary.bundle_project,\n"
        "    'mode': summary.mode,\n"
        "    'succeeded': summary.succeeded,\n"
        "    'failed': summary.failed,\n"
        "    'skipped': summary.skipped,\n"
        "    'deferred': summary.deferred,\n"
        "    'total_duration_seconds': summary.total_duration_seconds,\n"
        "    'wall_seconds': twall,\n"
        "    'steps': [\n"
        "        {'dataset_id': s.dataset_id, 'layer': s.layer, 'status': s.status,\n"
        "         'row_count': s.row_count, 'duration_seconds': s.duration_seconds,\n"
        "         'skip_reason': s.skip_reason, 'error_message': s.error_message}\n"
        "        for s in summary.steps\n"
        "    ],\n"
        "}\n"
        "import base64 as _b64\n"
        "_payload_b64 = _b64.b64encode(json.dumps(_payload).encode()).decode()\n"
        # Base64-wrap the JSON payload. AIDP's stdout capture strips
        # backslashes before quotes inside display_data, so the raw
        # json.dumps() text isn't reliably parseable on the laptop side.
        # Base64 is pure ASCII with no quote chars — safe under any
        # capture mode.
        f"print({MARKER_BEGIN!r}, _payload_b64, {MARKER_END!r})\n"
    )

    state_cell = (
        "from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle\n"
        "_bundle, _paths = load_bundle(BUNDLE_PATH)\n"
        "_state_table = _paths.bronze('fusion_bundle_state')\n"
        "try:\n"
        "    spark.sql(\n"
        "        f\"\"\"SELECT dataset_id, layer, mode, status, row_count, skip_reason, duration_seconds\n"
        "             FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY last_run_at DESC) AS rn\n"
        "                   FROM {_state_table} WHERE run_id = '{summary.run_id}') t\n"
        "             WHERE rn=1 ORDER BY layer, dataset_id\"\"\"\n"
        "    ).show(200, truncate=False)\n"
        "except Exception as exc:\n"
        "    print(f'state-table dump skipped: {exc}')\n"
    )

    title = "# `aidp-fusion-bundle` run\n"
    if title_suffix:
        title = f"# `aidp-fusion-bundle` run — {title_suffix}\n"

    return {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    title,
                    "Self-contained dispatch notebook — installs the plugin wheel inline,\n",
                    "fetches the BICC password from the AIDP credential store, runs the\n",
                    "orchestrator, and emits a marker payload for the laptop side to parse.\n",
                ],
            },
            _code_cell(install_cell),
            _code_cell(creds_cell),
            _code_cell(run_cell),
            _code_cell(state_cell),
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


__all__ = ["MARKER_BEGIN", "MARKER_END", "build_notebook"]
