"""Laptop-side dispatcher for ``aidp-fusion-bundle run --mode <mode>``.

Owns the build-wheel → upload-notebook → create-job → submit-run → poll →
fetch-output flow that lets ``commands/run.py`` ship work to a live AIDP
Spark cluster without the customer having to open an AIDP notebook by hand.

The OCI-signed REST primitives live in ``rest_client.AidpRestClient`` —
copied from the in-repo ``.claude/skills/aidp-rest/client.py`` so the
production CLI doesn't depend on a Claude-Code-only skill path. Both
copies should stay byte-identical; the skill is the editorial source.
"""

from __future__ import annotations

from .rest_client import (
    AidpRestClient,
    AidpRestError,
    ClusterSummary,
    RunResult,
    WorkspaceSummary,
)
from .runner import DispatchError, dispatch_run, fetch_result

__all__ = [
    "AidpRestClient",
    "AidpRestError",
    "ClusterSummary",
    "DispatchError",
    "RunResult",
    "WorkspaceSummary",
    "dispatch_run",
    "fetch_result",
]
