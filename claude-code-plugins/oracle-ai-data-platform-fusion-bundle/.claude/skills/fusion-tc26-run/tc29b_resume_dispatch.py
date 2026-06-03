"""TC29b — Resume-via-REST live evidence dispatcher (P1.5ε-fix5).

Four-phase probe against a live AIDP cluster, exercising the
productized REST-dispatch resume path end-to-end:

  1. Clean baseline   — productized build_notebook(resume_run_id=None)
                        → R_clean, Δt_clean.
  2. Induced failure  — tc27-style monkeypatched dim_supplier silver
                        builder raises; produces a partial-failure run
                        with a failed-step error_message=repr(exc).
                        Tests the **marker-parse regex fallback** path
                        when AIDP's display_data text/plain capture
                        strips JSON-escape backslashes from nested
                        quotes (TC27 trap). Records whether the marker
                        was clean or degraded.
  3. Resume           — productized build_notebook(resume_run_id=R2).
                        Exercises the fix5 repr()-injection in the
                        run cell so orchestrator.run(resume_run_id=...)
                        runs cluster-side under the original run_id.
  4. Bad resume       — productized build_notebook(resume_run_id="not-a-real-id").
                        Cell 3 raises ResumeRunNotFoundError before
                        marker emit; exercises the **cell-error
                        enrichment** path in dispatch_via_rest.

Unlike TC26/TC27 dispatch scripts that hand-roll the notebook, Phases
1/3/4 use the *productized* ``build_notebook`` from
``dispatch/notebook_builder.py`` — the same code path
``dispatch_via_rest`` would invoke from the CLI. This validates the
fix5 plumbing end-to-end (resume_run_id repr()-injection, marker
shape, cell-error enrichment) rather than reproducing the dispatch
logic in the test harness.

Phase 2 must use a custom notebook to inject the silver-builder fault
(no operator-facing hook for this in the productized CLI), but the
marker emitted is identical in shape to the productized one so the
parser exercise is faithful.
"""
from __future__ import annotations

import argparse
import base64
import json
import sys
import time
from pathlib import Path

# Locate the plugin checkout and add its scripts dir to sys.path so we
# can import the productized dispatch + schema modules from this skill
# script directly.
_PLUGIN_CHECKOUT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_PLUGIN_CHECKOUT / "scripts"))

from oracle_ai_data_platform_fusion_bundle.dispatch.notebook_builder import (  # noqa: E402
    MARKER_BEGIN,
    MARKER_END,
    build_notebook,
)
from oracle_ai_data_platform_fusion_bundle.dispatch.rest_client import (  # noqa: E402
    AidpRestClient,
    AidpRestError,
)

# Sibling skill — reuse wheel-build helper.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import dispatch  # noqa: E402

PLUGIN_NAME = "oracle_ai_data_platform_fusion_bundle"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="TC29b — resume-via-REST live evidence dispatcher"
    )
    p.add_argument("--aidp-id", required=True)
    p.add_argument("--workspace-key", required=True)
    p.add_argument("--cluster-key", required=True)
    p.add_argument("--cluster-name", required=True)
    p.add_argument("--region", default="us-ashburn-1")
    p.add_argument("--secret-name", default="fusion_bicc_password")
    p.add_argument("--secret-key", default="password")
    p.add_argument("--fusion-service-url", required=True)
    p.add_argument("--fusion-user", required=True)
    p.add_argument("--external-storage", required=True)
    p.add_argument("--plugin-checkout",
                   default=str(_PLUGIN_CHECKOUT))
    p.add_argument("--poll-timeout", type=int, default=2700)
    p.add_argument("--poll-interval", type=int, default=20)
    p.add_argument("--workspace-dir",
                   default="/Workspace/Shared/fusion-bundle-tc29b")
    p.add_argument("--phases", default="1,2,3,4",
                   help="Comma-separated phase IDs (1=clean, 2=induced, "
                        "3=resume, 4=bad-resume)")
    p.add_argument("--resume-run-id",
                   help="Skip phases 1+2; resume the named run_id "
                        "directly (Phase 3 only)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Phase-2 custom notebook (tc27-style monkeypatch).
# Phases 1/3/4 use the productized build_notebook directly.
# ---------------------------------------------------------------------------


def _phase2_run_cell() -> str:
    """Mirror of TC27's induced-failure cell, but the marker payload
    is emitted via the *productized* RunSummary.to_marker_dict() so the
    parser exercise reflects the real shape (incl. error_message=repr(exc))."""
    return """\
# Phase 2: monkeypatch dim_supplier silver builder to raise mid-run.
import time, json
from oracle_ai_data_platform_fusion_bundle.orchestrator import registry as _reg
from oracle_ai_data_platform_fusion_bundle.dimensions import dim_supplier as _dim_supplier
_orig_build = _dim_supplier.build
def _patched_build(*args, **kw):
    raise RuntimeError(
        "TC29b induced failure: simulating mid-run failure on dim_supplier silver build"
    )
_dim_supplier.build = _patched_build
_reg.SILVER_DIMS["dim_supplier"] = _reg.SilverDimSpec(
    "dim_supplier",
    builder=_patched_build,
    depends_on_bronze=_reg.SILVER_DIMS["dim_supplier"].depends_on_bronze,
)

tstart = time.time()
summary = orchestrator.run(  # noqa: F821
    bundle_path=BUNDLE_PATH, spark=spark,  # noqa: F821
    mode="seed", datasets=None, layers=None, dry_run=False,
    resume_run_id=None,
)
twall = time.time() - tstart
print(f"PHASE_2_INDUCED_FAIL run_id={summary.run_id} wall={twall:.1f}s")
for step in summary.steps:
    skip_tag = f" [{step.skip_reason}]" if step.skip_reason else ""
    rc = step.row_count if step.row_count is not None else "-"
    err = f" err={step.error_message[:60]}" if step.error_message and step.status=="failed" else ""
    print(f"  {step.layer:6s}  {step.dataset_id:24s}  {step.status:16s}{skip_tag:14s}  rows={str(rc):>10s}  dur={step.duration_seconds:.2f}s{err}")

# Emit via the productized to_marker_dict() — same shape as build_notebook's
# run cell. This is what triggers the TC27 marker-parse trap when
# error_message=repr(RuntimeError("…")) hits AIDP's display_data capture.
_payload = summary.to_marker_dict()
import json as _json
print("AIDP_LIVE_TEST_RESULT_BEGIN", _json.dumps(_payload), "AIDP_LIVE_TEST_RESULT_END")

# Restore original builder so cell can be re-run cleanly.
_dim_supplier.build = _orig_build
_reg.SILVER_DIMS["dim_supplier"] = _reg.SilverDimSpec(
    "dim_supplier",
    builder=_orig_build,
    depends_on_bronze=_reg.SILVER_DIMS["dim_supplier"].depends_on_bronze,
)
"""


def _phase2_custom_notebook(wheel_path: Path, bundle_yaml: str,
                            secret_name: str, secret_key: str) -> dict:
    """Phase 2 custom notebook — install cell + creds cell are
    identical to the productized notebook (copied verbatim from the
    shape produced by build_notebook), only the run cell differs to
    inject the monkeypatch."""
    wheel_b64 = base64.b64encode(wheel_path.read_bytes()).decode()
    wheel_filename = wheel_path.name

    install_cell = (
        f'import base64, subprocess, sys, tempfile, pathlib\n'
        f'WHEEL_B64 = """{wheel_b64}"""\n'
        f'_stage = pathlib.Path(tempfile.mkdtemp(prefix="tc29b_plugin_"))\n'
        f'_whl = _stage / "{wheel_filename}"\n'
        f'_whl.write_bytes(base64.b64decode(WHEEL_B64))\n'
        f'_target = _stage / "site-packages"\n'
        f'_target.mkdir()\n'
        f'res = subprocess.run([sys.executable, "-m", "pip", "install", '
        f'"--quiet", "--no-deps", "--target", str(_target), str(_whl)], '
        f'capture_output=True, text=True, timeout=180)\n'
        f'print(f"pip rc={{res.returncode}}")\n'
        f'if res.returncode != 0:\n'
        f'    print("STDOUT:", res.stdout[-2000:]); print("STDERR:", res.stderr[-2000:])\n'
        f'    raise RuntimeError("wheel install failed")\n'
        f'sys.path.insert(0, str(_target))\n'
        f'print(f"plugin installed to {{_target}}")\n'
    )

    creds_cell = (
        f'import os\n'
        f'from pathlib import Path\n'
        f'os.environ["FUSION_BICC_PASSWORD"] = aidputils.secrets.get('
        f'name={secret_name!r}, key={secret_key!r})  # noqa: F821\n'
        f'assert os.environ["FUSION_BICC_PASSWORD"], "creds store returned empty"\n'
        f'_pw_len = len(os.environ["FUSION_BICC_PASSWORD"])\n'
        f'print(f"FUSION_BICC_PASSWORD loaded (length={{_pw_len}})")\n'
        f'BUNDLE_PATH = Path("bundle.yaml")\n'
        f'BUNDLE_PATH.write_text({bundle_yaml!r})\n'
        f'from oracle_ai_data_platform_fusion_bundle import orchestrator\n'
        f'print("orchestrator loaded")\n'
    )

    def code_cell(src: str) -> dict:
        return {"cell_type": "code", "execution_count": None, "metadata": {},
                "outputs": [], "source": src.splitlines(keepends=True)}

    return {
        "cells": [
            {"cell_type": "markdown", "metadata": {},
             "source": ["# TC29b — Phase 2 (induced mid-cascade failure)\n",
                        "Custom notebook (tc27-style monkeypatch) — productized build_notebook used for Phases 1/3/4."]},
            code_cell(install_cell),
            code_cell(creds_cell),
            code_cell(_phase2_run_cell()),
        ],
        "metadata": {"kernelspec": {"display_name": "Python 3",
                                    "language": "python", "name": "python3"},
                     "language_info": {"name": "python"}},
        "nbformat": 4, "nbformat_minor": 5,
    }


# ---------------------------------------------------------------------------
# Phase runner
# ---------------------------------------------------------------------------


def _log(stage: str, **kw) -> None:
    extras = " ".join(f"{k}={v}" for k, v in kw.items())
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [tc29b] {stage}: {extras}", flush=True)


def _dispatch_phase(
    client: AidpRestClient, args: argparse.Namespace, notebook: dict,
    phase_id: int, out_dir: Path,
) -> dict:
    """Upload + dispatch + poll + fetch for one phase. Returns dict with
    terminal_status, executed_notebook (parsed dict), parse_marker result
    (or None), cell_errors, run_id (best-effort)."""
    notebook_path = f"{args.workspace_dir}/phase{phase_id}.ipynb"
    _log("upload", phase=phase_id, path=notebook_path)
    client.upload_notebook(notebook_path, notebook)

    job_name = f"tc29b_phase{phase_id}_{int(time.time())}"
    _log("create_job", phase=phase_id, name=job_name)
    job_key = client.create_notebook_job(
        name=job_name, description=f"TC29b phase {phase_id}",
        notebook_path=notebook_path,
        cluster_key=args.cluster_key, cluster_name=args.cluster_name,
        task_key="tc29b_task",
    )
    run_key = client.submit_run(job_key)
    _log("poll_start", phase=phase_id, runKey=run_key)
    tstart = time.time()
    result = client.poll_run(
        run_key, timeout_s=args.poll_timeout, interval_s=args.poll_interval,
        on_status_change=lambda s: _log("status", phase=phase_id, status=s),
    )
    twall = time.time() - tstart
    _log("poll_done", phase=phase_id, terminal=result.status,
         wall_s=f"{twall:.1f}")

    task_run_key = client.resolve_task_run_key(result.raw, "tc29b_task")
    executed_nb_json = client.fetch_output(task_run_key)

    # Save executed notebook for debugging + the evidence file.
    out_path = out_dir / f"phase{phase_id}_executed.ipynb"
    out_path.write_text(executed_nb_json)
    _log("notebook_saved", phase=phase_id, path=str(out_path))

    # Parse the marker via the *productized* AidpRestClient — this is
    # what exercises the new regex-fallback path on degraded markers.
    executed = json.loads(executed_nb_json) if executed_nb_json else {}
    marker = None
    marker_parse_error = None
    try:
        marker = AidpRestClient.parse_marker(
            executed, begin=MARKER_BEGIN, end=MARKER_END,
        )
    except (ValueError, json.JSONDecodeError) as exc:
        marker_parse_error = f"{type(exc).__name__}: {str(exc)[:200]}"
        _log("marker_parse_failed", phase=phase_id, error=marker_parse_error)

    cell_errors = AidpRestClient.extract_cell_errors(executed)
    return {
        "phase": phase_id,
        "terminal_status": result.status,
        "wall_seconds": twall,
        "executed_notebook_path": str(out_path),
        "marker": marker,
        "marker_parse_error": marker_parse_error,
        "cell_errors": cell_errors,
        "task_run_key": task_run_key,
        "job_key": job_key,
        "run_key": run_key,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    args = parse_args()
    phases = sorted(set(int(p.strip()) for p in args.phases.split(",")))

    out_dir = Path(f"/tmp/tc29b-{int(time.time())}")
    out_dir.mkdir(parents=True, exist_ok=True)
    _log("start", out=str(out_dir), phases=phases)

    # Use the productized AidpRestClient via the canonical OCI profile path.
    client = AidpRestClient(
        region=args.region, aidp_id=args.aidp_id,
        workspace_key=args.workspace_key, log=_log,
    )

    # Confirm cluster ACTIVE.
    cluster = client.get_cluster(args.cluster_key)
    state = cluster.get("state")
    _log("cluster_state", state=state)
    if state != "ACTIVE":
        raise AidpRestError(
            f"cluster {args.cluster_key} state={state}; must be ACTIVE before dispatch"
        )

    # Build wheel once — reused across all 4 phases.
    _log("build_wheel")
    wheel_path = dispatch.build_wheel(Path(args.plugin_checkout), out_dir / "wheel")
    _log("wheel_ready", path=str(wheel_path), size=wheel_path.stat().st_size)

    # Render bundle (narrow scope — 2 bronze + 2 silver + 1 gold so Phase 3
    # has a meaningful resumed_skipped count).
    bundle_yaml = dispatch.NARROW_BUNDLE.format(
        service_url=args.fusion_service_url,
        username=args.fusion_user,
        external_storage=args.external_storage,
    )

    results: dict[int, dict] = {}
    resume_run_id = args.resume_run_id

    # -----------------------------------------------------------------------
    # Phase 1 — clean baseline (productized notebook, resume_run_id=None)
    # -----------------------------------------------------------------------
    if 1 in phases:
        _log("phase_1_start", desc="clean baseline")
        nb1 = build_notebook(
            wheel_path=wheel_path, bundle_yaml=bundle_yaml,
            mode="seed", datasets=None, layers=None,
            resume_run_id=None,
            bicc_secret_name=args.secret_name,
            bicc_secret_key=args.secret_key,
            title="TC29b — Phase 1 (clean baseline)",
        )
        results[1] = _dispatch_phase(client, args, nb1, 1, out_dir)
        marker = results[1]["marker"]
        run_id = marker.get("run_id") if marker else "<missing>"
        _log("phase_1_done", run_id=run_id, terminal=results[1]["terminal_status"])

    # -----------------------------------------------------------------------
    # Phase 2 — induced failure (custom monkeypatched notebook)
    # -----------------------------------------------------------------------
    if 2 in phases:
        _log("phase_2_start", desc="induced failure (custom notebook)")
        nb2 = _phase2_custom_notebook(
            wheel_path=wheel_path, bundle_yaml=bundle_yaml,
            secret_name=args.secret_name, secret_key=args.secret_key,
        )
        results[2] = _dispatch_phase(client, args, nb2, 2, out_dir)

        # Resolve phase-2 run_id from EITHER the clean marker OR the
        # regex-fallback synthetic shape. This is the live proof that
        # fix5's parse_marker hardening makes the Phase-2 → Phase-3
        # transition reachable without notebook archaeology.
        marker = results[2]["marker"]
        if marker is None:
            _log("phase_2_no_marker", error="parse_marker returned None")
        elif marker.get("_marker_parse_failed"):
            resume_run_id = marker["run_id"]
            _log("phase_2_marker_degraded",
                 recovered_run_id=resume_run_id,
                 raw_marker_preview=marker.get("_raw_marker", "")[:120])
            results[2]["marker_branch"] = "degraded (regex fallback)"
        else:
            resume_run_id = marker["run_id"]
            _log("phase_2_marker_clean", run_id=resume_run_id,
                 failed=marker.get("failed", 0),
                 succeeded=marker.get("succeeded", 0))
            results[2]["marker_branch"] = "clean (json parse)"

    # -----------------------------------------------------------------------
    # Phase 3 — resume (productized notebook with resume_run_id literal)
    # -----------------------------------------------------------------------
    if 3 in phases:
        if not resume_run_id:
            raise AidpRestError(
                "phase 3 requires --resume-run-id when phase 2 is skipped"
            )
        _log("phase_3_start", desc="resume", resume_run_id=resume_run_id)
        nb3 = build_notebook(
            wheel_path=wheel_path, bundle_yaml=bundle_yaml,
            mode="seed", datasets=None, layers=None,
            resume_run_id=resume_run_id,
            bicc_secret_name=args.secret_name,
            bicc_secret_key=args.secret_key,
            title=f"TC29b — Phase 3 (resume run_id={resume_run_id})",
        )
        results[3] = _dispatch_phase(client, args, nb3, 3, out_dir)
        marker = results[3]["marker"]
        if marker:
            _log("phase_3_done",
                 run_id=marker.get("run_id"),
                 succeeded=marker.get("succeeded"),
                 resumed_skipped=marker.get("resumed_skipped"),
                 failed=marker.get("failed"))
            # SOX-trail invariant: resumed run carries the ORIGINAL run_id.
            if marker.get("run_id") != resume_run_id:
                _log("phase_3_invariant_FAIL",
                     expected=resume_run_id, got=marker.get("run_id"))

    # -----------------------------------------------------------------------
    # Phase 4 — bad resume (cell-error enrichment path)
    # -----------------------------------------------------------------------
    if 4 in phases:
        _log("phase_4_start", desc="bad resume (cell-error enrichment)")
        nb4 = build_notebook(
            wheel_path=wheel_path, bundle_yaml=bundle_yaml,
            mode="seed", datasets=None, layers=None,
            resume_run_id="tc29b-not-a-real-id",
            bicc_secret_name=args.secret_name,
            bicc_secret_key=args.secret_key,
            title="TC29b — Phase 4 (bad resume id)",
        )
        results[4] = _dispatch_phase(client, args, nb4, 4, out_dir)
        # No marker expected — cell 3 should raise ResumeRunNotFoundError
        # before reaching the marker emit.
        cell_errors = results[4]["cell_errors"]
        run_cell_errors = [e for e in cell_errors if e.get("cell_index") == 3]
        if run_cell_errors:
            err = run_cell_errors[0]
            _log("phase_4_cell_error",
                 ename=err.get("ename"),
                 evalue=(err.get("evalue") or "")[:120])
        else:
            _log("phase_4_no_cell_error",
                 warning="expected ResumeRunNotFoundError on cell 3")

    # -----------------------------------------------------------------------
    # Aggregate report
    # -----------------------------------------------------------------------
    summary_path = out_dir / "tc29b_summary.json"
    # marker may contain non-serializable types — coerce defensively.
    summary_path.write_text(json.dumps(results, indent=2, default=str))
    _log("complete", summary=str(summary_path))

    print("\n" + "=" * 70)
    print("TC29b SUMMARY")
    print("=" * 70)
    for phase_id, r in results.items():
        marker = r.get("marker")
        run_id = marker.get("run_id") if marker else "<no marker>"
        branch = r.get("marker_branch", "")
        extra = f"  marker_branch={branch}" if branch else ""
        print(f"Phase {phase_id} ({r['terminal_status']:8s})  run_id={run_id}"
              f"  wall={r['wall_seconds']:.1f}s{extra}")
        if marker and not marker.get("_marker_parse_failed"):
            print(f"  succeeded={marker.get('succeeded')}  "
                  f"failed={marker.get('failed')}  "
                  f"skipped={marker.get('skipped')}  "
                  f"resumed_skipped={marker.get('resumed_skipped')}")
        cell_errors = r.get("cell_errors", [])
        for ce in cell_errors:
            print(f"  cell {ce['cell_index']}: {ce['ename']}: "
                  f"{(ce.get('evalue') or '')[:80]}")
    print(f"\nFull payloads + executed notebooks: {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
