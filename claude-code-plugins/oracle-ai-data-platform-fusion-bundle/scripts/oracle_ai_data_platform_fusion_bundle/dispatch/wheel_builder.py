"""Build a wheel of the plugin package for inlining into the AIDP notebook.

Shells out to ``python -m build`` from the plugin checkout. Returns the
path to the freshest ``.whl`` in the output directory.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PLUGIN_PACKAGE_NAME = "oracle_ai_data_platform_fusion_bundle"


class WheelBuildError(RuntimeError):
    """``python -m build`` failed or produced no wheel."""


def build_wheel(plugin_checkout: Path, outdir: Path) -> Path:
    """Build a wheel of the plugin at ``plugin_checkout`` into ``outdir``.

    Returns the freshest produced wheel path. Raises :class:`WheelBuildError`
    on non-zero exit or empty output dir.
    """
    outdir.mkdir(parents=True, exist_ok=True)
    res = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(outdir)],
        cwd=plugin_checkout,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if res.returncode != 0:
        raise WheelBuildError(
            f"wheel build failed (rc={res.returncode}):\n{res.stdout}\n{res.stderr}"
        )
    wheels = sorted(
        outdir.glob(f"{PLUGIN_PACKAGE_NAME}-*.whl"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not wheels:
        raise WheelBuildError(f"no wheel produced in {outdir}")
    return wheels[0]


__all__ = ["PLUGIN_PACKAGE_NAME", "WheelBuildError", "build_wheel"]
