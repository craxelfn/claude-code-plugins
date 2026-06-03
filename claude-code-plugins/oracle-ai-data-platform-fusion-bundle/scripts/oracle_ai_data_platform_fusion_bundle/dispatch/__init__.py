"""Dispatch package — laptop-CLI → AIDP REST round-trip (P1.5ε).

Promotes the empirically-validated dispatcher from
``.claude/skills/fusion-tc26-run/dispatch.py`` into the plugin source so
``aidp-fusion-bundle run --mode seed`` (no ``--inline``) actually works
from a bare laptop terminal instead of printing a stub.

The package is a strict client of the ``schema/`` layer
(:mod:`oracle_ai_data_platform_fusion_bundle.schema.bundle`,
:mod:`oracle_ai_data_platform_fusion_bundle.schema.errors`,
:mod:`oracle_ai_data_platform_fusion_bundle.schema.run_summary`). It MUST
NOT import from :mod:`oracle_ai_data_platform_fusion_bundle.orchestrator`
or any submodule under ``orchestrator/`` — that pulls extractors,
dimensions, transforms, and the registry into ``sys.modules`` and breaks
the §4.3 separation. The boundary is locked by
``tests/unit/dispatch/test_imports.py``.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Literal

import oci

from ..schema.bundle import AidpConfig, EnvSpec
from ..schema.run_summary import RunSummary
from .errors import (
    DispatchAuthError,
    DispatchError,
    DispatchFetchOutputError,
    DispatchJobSubmitError,
    DispatchMarkerMissingError,
    DispatchPollTimeoutError,
    DispatchPreflightError,
    DispatchRunFailedError,
    DispatchUploadError,
)
from .notebook_builder import MARKER_BEGIN, MARKER_END, build_notebook
from .preflight import (
    PreflightResult,
    any_failed,
    render as render_preflight,
    run_local_preflight,
    run_remote_preflight,
)
from .rest_client import AidpRestClient, AidpRestError
from .wheel_builder import DispatchWheelBuildError, build_wheel


def _format_preflight_failure(results: list[PreflightResult]) -> str:
    """One-line summary per failed check + remediation, suitable for the
    ``DispatchPreflightError`` message body."""
    lines: list[str] = []
    for r in results:
        if r.status == "FAIL":
            lines.append(f"{r.name}: {r.detail}")
            if r.remediation:
                lines.append(f"  → {r.remediation}")
    return "; ".join(lines) if not any(
        "\n" in line for line in lines
    ) else "\n".join(lines)


def dispatch_via_rest(
    *,
    bundle_path: Path,
    config: AidpConfig,
    env: EnvSpec,
    env_name: str,
    mode: Literal["seed", "incremental"],
    datasets: list[str] | None,
    layers: list[str] | None,
    dry_run: bool = False,
    plugin_checkout: Path | None = None,
    auto_start_cluster: bool = True,
    log: Callable[[str], None] = lambda msg: None,
) -> RunSummary:
    """Dispatch the orchestrator notebook to AIDP and return the parsed RunSummary.

    Composes the dispatch package's primitives:

    1. Phase A preflight (bundle.yaml, dispatch coords, OCI profile + session).
    2. Build the :class:`AidpRestClient` once Phase A is all-PASS.
    3. Phase B preflight (control plane reachable, cluster state + auto-start).
    4. **If** ``dry_run`` — return :meth:`RunSummary.empty` and stop.
    5. Build the wheel (content-hash cached).
    6. Generate the 4-cell notebook in-memory.
    7. Upload notebook → create job → submit run → poll → fetch output.
    8. Parse the ``AIDP_LIVE_TEST_RESULT_BEGIN/END`` marker into a RunSummary.

    Raises (all :class:`DispatchError` subclasses — :class:`AidpRestError`
    is wrapped at every call site so the CLI's ``except (DispatchError,
    OrchestratorConfigError)`` catch is exhaustive):
        :class:`DispatchPreflightError`: any local- or remote-phase check fails.
        :class:`DispatchAuthError`: OCI signer construction failed at client init.
        :class:`DispatchWheelBuildError`: ``python -m build`` failed.
        :class:`DispatchUploadError`: contents-API PUT non-2xx.
        :class:`DispatchJobSubmitError`: ``POST /jobs`` or ``POST /jobRuns`` non-2xx.
        :class:`DispatchPollTimeoutError`: ``poll_run`` deadline exceeded.
        :class:`DispatchRunFailedError`: terminal status FAILED/CANCELED/TIMED_OUT.
        :class:`DispatchFetchOutputError`: ``fetchOutput`` non-200.
        :class:`DispatchMarkerMissingError`: SUCCESS but no marker.

    No ``resume_run_id`` parameter — REST-dispatch resume is out of scope
    in this PR (see plan §3.1). Tracked as ``P1.5ε-fix5``.
    """
    # ---- Phase A — local preflight ---------------------------------------
    local_results = run_local_preflight(
        bundle_path=bundle_path,
        config=config,
        env_name=env_name,
        env=env,
    )
    log(render_preflight(local_results))
    if any_failed(local_results):
        raise DispatchPreflightError(_format_preflight_failure(local_results))

    # ---- Construct the REST client (cannot fail "out of band" of preflight
    # because Phase A validated the OCI profile already; defense in depth
    # catches malformed key files that slipped past from_file()).
    try:
        client = AidpRestClient(
            region=env.region or config.defaults.region,
            aidp_id=env.ai_data_platform_id or "",
            workspace_key=env.workspace_key,
            oci_profile=env.oci_profile or "DEFAULT",
            log=lambda stage, **kw: log(
                f"[rest] {stage} " + " ".join(f"{k}={v}" for k, v in kw.items())
            ),
        )
    except (
        oci.exceptions.ConfigFileNotFound,
        oci.exceptions.InvalidConfig,
        oci.exceptions.MissingPrivateKeyPassphrase,
    ) as exc:
        raise DispatchAuthError(f"OCI signer construction failed: {exc}") from exc
    except AidpRestError as exc:
        # _build_signer raises AidpRestError on missing/empty session-token
        # file — wrap into the AUTH code so the operator sees the correct
        # remediation hint.
        raise DispatchAuthError(str(exc)) from exc

    # ---- Phase B — remote preflight --------------------------------------
    remote_results = run_remote_preflight(
        client=client,
        env=env,
        auto_start_cluster=auto_start_cluster,
        log=log,
    )
    log(render_preflight(remote_results))
    if any_failed(remote_results):
        raise DispatchPreflightError(_format_preflight_failure(remote_results))

    # ---- Dry-run short-circuit -------------------------------------------
    if dry_run:
        log("dry-run requested — skipping wheel build + upload + dispatch")
        return RunSummary.empty(bundle_project=config.project, mode=mode)

    # ---- Build wheel -----------------------------------------------------
    checkout = plugin_checkout or _detect_plugin_checkout()
    wheel_path = build_wheel(plugin_checkout=checkout, log=log)

    # ---- Generate notebook + upload --------------------------------------
    bundle_yaml = bundle_path.read_text(encoding="utf-8")
    notebook = build_notebook(
        wheel_path=wheel_path,
        bundle_yaml=bundle_yaml,
        mode=mode,
        datasets=datasets,
        layers=layers,
        bicc_secret_name=env.bicc_secret_name,
        bicc_secret_key=env.bicc_secret_key,
    )

    workspace_root = config.defaults.workspace_root.strip("/")
    notebook_path = f"/Workspace/{workspace_root}/aidp-fusion-bundle-{config.project}/run.ipynb"
    try:
        client.upload_notebook(notebook_path, notebook)
    except AidpRestError as exc:
        raise DispatchUploadError(
            f"notebook upload failed: {str(exc).splitlines()[0][:200]}"
        ) from exc
    log(f"notebook uploaded to {notebook_path}")

    # ---- Create job + submit run ----------------------------------------
    # AIDP job-name rule (empirical): letters, underscores, slashes only.
    # No hyphens, no dots. Sanitize the project + env tokens; suffix with
    # epoch seconds so resubmits don't collide.
    _safe_proj = "".join(c if c.isalnum() or c == "_" else "_" for c in config.project)
    _safe_env = "".join(c if c.isalnum() or c == "_" else "_" for c in env_name)
    job_name = f"aidp_fusion_bundle_{_safe_proj}_{_safe_env}_{int(time.time())}"
    task_key = "orchestrator_run"
    try:
        job_key = client.create_notebook_job(
            name=job_name,
            description=f"aidp-fusion-bundle run (env={env_name}, mode={mode})",
            notebook_path=notebook_path,
            cluster_key=env.cluster_key or "",
            cluster_name=env.cluster_name or "",
            task_key=task_key,
        )
        log(f"jobKey={job_key}")
        job_run_key = client.submit_run(job_key)
        log(f"jobRunKey={job_run_key}")
    except AidpRestError as exc:
        raise DispatchJobSubmitError(
            f"job submission failed: {str(exc).splitlines()[0][:200]}"
        ) from exc

    # ---- Poll to terminal status -----------------------------------------
    try:
        result = client.poll_run(
            job_run_key,
            on_status_change=lambda status: log(f"status={status}"),
        )
    except AidpRestError as exc:
        msg = str(exc)
        if "deadline exceeded" in msg:
            raise DispatchPollTimeoutError(msg) from exc
        # Some other transport failure during polling — treat as fetch-level
        # since we can't tell whether the cluster work completed.
        raise DispatchFetchOutputError(
            f"poll_run transport failed: {str(exc).splitlines()[0][:200]}"
        ) from exc

    # ---- Fetch executed notebook + parse marker --------------------------
    try:
        task_run_key = AidpRestClient.resolve_task_run_key(result.raw, task_key)
        executed_notebook_json = client.fetch_output(task_run_key)
    except AidpRestError as exc:
        raise DispatchFetchOutputError(
            f"fetchOutput failed: {str(exc).splitlines()[0][:200]}"
        ) from exc

    # Decode + marker-parse defense (reviewer-driven): a truncated AIDP
    # output or a partial marker (BEGIN without END) would otherwise raise
    # raw json.JSONDecodeError / ValueError out of parse_marker — the CLI's
    # `except (DispatchError, OrchestratorConfigError)` clause wouldn't
    # catch those, so the operator would see a Python traceback instead of
    # exit 2 with DISPATCH_MARKER_MISSING. Wrap both the JSON decode AND
    # the marker walk so every malformed-output failure mode lands in the
    # typed taxonomy with jobRunKey context.
    try:
        executed_notebook = (
            json.loads(executed_notebook_json) if executed_notebook_json else {}
        )
    except json.JSONDecodeError as exc:
        raise DispatchMarkerMissingError(
            f"executed notebook JSON decode failed (jobRunKey={job_run_key}); "
            f"evidence-capture failure — underlying: "
            f"{type(exc).__name__}: {str(exc)[:200]}"
        ) from exc

    if result.status != "SUCCESS":
        raise DispatchRunFailedError(
            f"job_run_key={job_run_key} reached terminal status "
            f"{result.status!r}; see AIDP console / executed notebook for details"
        )

    try:
        marker = AidpRestClient.parse_marker(
            executed_notebook, begin=MARKER_BEGIN, end=MARKER_END
        )
    except (ValueError, json.JSONDecodeError) as exc:
        # ValueError covers `value.index(end, b)` failure (BEGIN found but
        # no END — truncated stdout); JSONDecodeError covers the inner
        # `json.loads(value[b:e])` blowing up on a malformed payload that
        # happens to sit between valid BEGIN/END delimiters.
        raise DispatchMarkerMissingError(
            f"marker parse failed (jobRunKey={job_run_key}); "
            f"evidence-capture failure — underlying: "
            f"{type(exc).__name__}: {str(exc)[:200]}"
        ) from exc

    if marker is None:
        raise DispatchMarkerMissingError(
            f"job reported SUCCESS but no marker found in executed notebook "
            f"(jobRunKey={job_run_key}); evidence-capture failure"
        )

    try:
        summary = RunSummary.from_marker_dict(marker)
    except ValueError as exc:
        raise DispatchMarkerMissingError(
            f"marker payload malformed (jobRunKey={job_run_key}): {exc}"
        ) from exc

    log(f"orchestrator run_id={summary.run_id}")
    return summary


def _detect_plugin_checkout() -> Path:
    """Walk up from this module's location to find the plugin checkout root
    (the directory containing ``pyproject.toml``).
    """
    current = Path(__file__).resolve().parent
    for _ in range(8):
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    raise DispatchError(
        "could not auto-detect plugin checkout root; pass plugin_checkout="
        " explicitly to dispatch_via_rest"
    )


__all__ = [
    "DispatchAuthError",
    "DispatchError",
    "DispatchFetchOutputError",
    "DispatchJobSubmitError",
    "DispatchMarkerMissingError",
    "DispatchPollTimeoutError",
    "DispatchPreflightError",
    "DispatchRunFailedError",
    "DispatchUploadError",
    "DispatchWheelBuildError",
    "dispatch_via_rest",
]
