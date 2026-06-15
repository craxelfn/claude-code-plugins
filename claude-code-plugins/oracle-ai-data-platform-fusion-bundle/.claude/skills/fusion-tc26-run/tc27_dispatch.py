"""TC27 — resume-from-checkpoint live evidence dispatcher.

Three-phase probe against a live AIDP cluster:

  1. **Clean baseline** — normal narrow-scope dispatch → R_clean, Δt_clean.
  2. **Induced failure** — narrow scope with monkeypatched ap_invoices
     extractor that raises mid-run → R_resume_initial (1 success + 1
     failed + 3 cascade-skipped).
  3. **Resume** — narrow scope with ``resume_run_id=R_resume_initial``;
     succeeded carry-forwards + re-attempt of the failed node → run
     completes under the original run_id, Δt_resume ≪ Δt_clean.

Reuses the wheel-build + notebook-template + dispatch machinery from
the sibling ``dispatch.py`` (TC26) — TC27 differs only in the
content of the run cells across phases.

Captures per-phase RunSummary payloads + the final latest-per-
(run_id, dataset_id) state-table projection for R_resume_initial.
"""
from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import time
from pathlib import Path

# aidp-rest now ships under the repo-root skills/ dir (skill reorg).
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "skills" / "aidp-rest"))
from client import AidpRestClient, AidpRestError  # noqa: E402

# Reuse wheel build + bundle template from dispatch.py.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import dispatch  # noqa: E402

PLUGIN_NAME = "oracle_ai_data_platform_fusion_bundle"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TC27 resume-from-checkpoint dispatcher")
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
                   default=str(Path(__file__).resolve().parents[3]))
    p.add_argument("--poll-timeout", type=int, default=2700)
    p.add_argument("--poll-interval", type=int, default=20)
    p.add_argument("--workspace-dir", default="/Workspace/Shared/fusion-bundle-tc27")
    p.add_argument("--phases", default="1,2,3",
                   help="Comma-separated phase IDs to run (1=baseline, 2=induced-fail, 3=resume).")
    p.add_argument("--resume-run-id",
                   help="Skip phases 1+2; resume the named run_id directly. "
                        "Use when re-running phase 3 after a transient failure "
                        "during the first attempt.")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Notebook templates — three flavors
# ---------------------------------------------------------------------------


def _install_cell(wheel_b64: str, wheel_filename: str) -> str:
    # Identical to TC26's install cell.
    return (
        f'import base64, subprocess, sys, tempfile, pathlib\n'
        f'WHEEL_B64 = """{wheel_b64}"""\n'
        f'_stage = pathlib.Path(tempfile.mkdtemp(prefix="tc27_plugin_"))\n'
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


def _creds_cell(bundle_yaml: str, secret_name: str, secret_key: str) -> str:
    return (
        f'import os\n'
        f'from pathlib import Path\n'
        f'os.environ["FUSION_BICC_PASSWORD"] = aidputils.secrets.get('
        f'name={secret_name!r}, key={secret_key!r})  # noqa: F821\n'
        f'assert os.environ["FUSION_BICC_PASSWORD"], "creds store returned empty"\n'
        f'pw_len = len(os.environ["FUSION_BICC_PASSWORD"])\n'
        f'print(f"FUSION_BICC_PASSWORD loaded (length={{pw_len}})")\n'
        f'BUNDLE_PATH = Path("bundle.yaml")\n'
        f'BUNDLE_PATH.write_text({bundle_yaml!r})\n'
        f'from oracle_ai_data_platform_fusion_bundle import orchestrator\n'
        f'print("orchestrator loaded")\n'
    )


_BASELINE_RUN_CELL = """\
import time, json
tstart = time.time()
summary = orchestrator.run(bundle_path=BUNDLE_PATH, spark=spark,
                           mode="seed", datasets=None, layers=None, dry_run=False)
twall = time.time() - tstart
print(f"PHASE_1_CLEAN run_id={summary.run_id} wall={twall:.1f}s")
for step in summary.steps:
    skip_tag = f" [{step.skip_reason}]" if step.skip_reason else ""
    rc = step.row_count if step.row_count is not None else "-"
    print(f"  {step.layer:6s}  {step.dataset_id:24s}  {step.status:16s}{skip_tag:14s}  rows={str(rc):>10s}  dur={step.duration_seconds:.2f}s")
_payload = {"tc":"TC27","phase":1,"run_id":summary.run_id,
            "bundle_project":summary.bundle_project,
            "succeeded":summary.succeeded,"failed":summary.failed,
            "skipped":summary.skipped,"deferred":summary.deferred,
            "resumed_skipped":summary.resumed_skipped,
            "total_duration_seconds":summary.total_duration_seconds,
            "wall_seconds":twall,
            "steps":[{"dataset_id":s.dataset_id,"layer":s.layer,"status":s.status,
                      "row_count":s.row_count,"duration_seconds":s.duration_seconds,
                      "skip_reason":s.skip_reason} for s in summary.steps]}
print("AIDP_LIVE_TEST_RESULT_BEGIN", json.dumps(_payload), "AIDP_LIVE_TEST_RESULT_END")
"""


# TC27 phase-2 induced-failure cell — NEEDS REWRITE on the content-pack
# dispatch path. The v1 mechanism (monkey-patching a Python builder for
# `dim_supplier`) is gone. A content-pack-equivalent induced failure
# would patch the strategy executor (``orchestrator/
# strategy_executors.py``) or stub ``sql_runner.execute_node`` for the
# `dim_supplier` node id. Tracked under a follow-up; until that lands
# TC27 phase 2 raises so the operator notices the gap rather than
# silently running phase 1 + phase 3.
_INDUCED_FAIL_RUN_CELL = """\
raise NotImplementedError(
    "TC27 phase-2 induced-failure cell is awaiting a content-pack rewrite. "
    "Phase 9 deleted the v1 dim_supplier monkey-patch target; the "
    "replacement should patch sql_runner.execute_node for the "
    "'dim_supplier' node id. See docs/features/"
    "v2-phase-9-followup-registry-deletion/idea.md."
)
"""


def _resume_run_cell(resume_run_id: str) -> str:
    return f"""\
# Phase 3: resume the failed run. NO monkeypatch — ap_invoices should
# now succeed; previously-succeeded erp_suppliers carry-forwards as
# resumed_skipped; downstream dim_supplier + supplier_spend re-dispatch
# under the original run_id.
import time, json
tstart = time.time()
summary = orchestrator.run(bundle_path=BUNDLE_PATH, spark=spark,
                           mode="seed", datasets=None, layers=None, dry_run=False,
                           resume_run_id={resume_run_id!r})
twall = time.time() - tstart
print(f"PHASE_3_RESUME run_id={{summary.run_id}} wall={{twall:.1f}}s")
for step in summary.steps:
    skip_tag = f" [{{step.skip_reason}}]" if step.skip_reason else ""
    rc = step.row_count if step.row_count is not None else "-"
    print(f"  {{step.layer:6s}}  {{step.dataset_id:24s}}  {{step.status:16s}}{{skip_tag:14s}}  rows={{str(rc):>10s}}  dur={{step.duration_seconds:.2f}}s")
_payload = {{"tc":"TC27","phase":3,"run_id":summary.run_id,
             "bundle_project":summary.bundle_project,
             "succeeded":summary.succeeded,"failed":summary.failed,
             "skipped":summary.skipped,"deferred":summary.deferred,
             "resumed_skipped":summary.resumed_skipped,
             "total_duration_seconds":summary.total_duration_seconds,
             "wall_seconds":twall,
             "steps":[{{"dataset_id":s.dataset_id,"layer":s.layer,"status":s.status,
                       "row_count":s.row_count,"duration_seconds":s.duration_seconds,
                       "skip_reason":s.skip_reason}} for s in summary.steps]}}
print("AIDP_LIVE_TEST_RESULT_BEGIN", json.dumps(_payload), "AIDP_LIVE_TEST_RESULT_END")

# Final latest-per-(run_id, dataset_id) projection — the audit story.
from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle
_bundle, _paths = load_bundle(BUNDLE_PATH)
_state_table = _paths.bronze("fusion_bundle_state")
print("\\nLatest-per-(run_id, dataset_id) for resumed run:")
spark.sql(f\"\"\"
  WITH ranked AS (
    SELECT dataset_id, layer, status, row_count, skip_reason,
           duration_seconds, last_run_at, plan_hash,
           ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY last_run_at DESC) AS rn
    FROM {{_state_table}}
    WHERE run_id = '{{summary.run_id}}'
  )
  SELECT dataset_id, layer, status, row_count, skip_reason,
         duration_seconds, SUBSTRING(plan_hash, 1, 12) AS plan_hash_short
  FROM ranked WHERE rn=1
  ORDER BY layer, dataset_id
\"\"\").show(50, truncate=False)

print("\\nCross-tab (run_id, dataset_id, status) — full append-only history:")
spark.sql(f\"\"\"
  SELECT dataset_id, status, COUNT(*) AS row_count
  FROM {{_state_table}}
  WHERE run_id = '{{summary.run_id}}'
  GROUP BY dataset_id, status
  ORDER BY dataset_id, status
\"\"\").show(50, truncate=False)
"""


def _state_cell_no_op() -> str:
    """Placeholder so phase 1+2 notebooks have the same 5-cell shape."""
    return 'print("state inspection skipped in this phase")\n'


def _build_notebook(wheel_path: Path, bundle_yaml: str, secret_name: str,
                    secret_key: str, run_cell_body: str, state_cell_body: str,
                    phase_id: int) -> dict:
    wheel_b64 = base64.b64encode(wheel_path.read_bytes()).decode()
    wheel_filename = wheel_path.name

    def code_cell(src: str) -> dict:
        return {"cell_type": "code", "execution_count": None, "metadata": {},
                "outputs": [], "source": src.splitlines(keepends=True)}

    return {
        "cells": [
            {"cell_type": "markdown", "metadata": {},
             "source": [f"# TC27 — Phase {phase_id} (resume from checkpoint)\n",
                        "Self-contained dispatch via fusion-tc26-run / tc27_dispatch.py."]},
            code_cell(_install_cell(wheel_b64, wheel_filename)),
            code_cell(_creds_cell(bundle_yaml, secret_name, secret_key)),
            code_cell(run_cell_body),
            code_cell(state_cell_body),
        ],
        "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                     "language_info": {"name": "python"}},
        "nbformat": 4, "nbformat_minor": 5,
    }


# ---------------------------------------------------------------------------
# Phase runner
# ---------------------------------------------------------------------------


def _log(stage: str, **kw):
    extras = " ".join(f"{k}={v}" for k, v in kw.items())
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [tc27] {stage}: {extras}", flush=True)


def _parse_marker(executed_nb_json: str) -> dict | None:
    """Walk cells[*].outputs[*] for AIDP_LIVE_TEST_RESULT_BEGIN ... END."""
    try:
        nb = json.loads(executed_nb_json)
    except json.JSONDecodeError:
        return None
    for cell in nb.get("cells", []):
        for out in cell.get("outputs", []):
            txt = ""
            if out.get("output_type") == "stream":
                src = out.get("text", "")
                txt = "".join(src) if isinstance(src, list) else src
            elif out.get("output_type") in ("execute_result", "display_data"):
                data = out.get("data", {}).get("text/plain", "")
                txt = "".join(data) if isinstance(data, list) else data
            if "AIDP_LIVE_TEST_RESULT_BEGIN" in txt:
                start = txt.index("AIDP_LIVE_TEST_RESULT_BEGIN") + len("AIDP_LIVE_TEST_RESULT_BEGIN")
                end = txt.index("AIDP_LIVE_TEST_RESULT_END")
                payload_str = txt[start:end].strip()
                try:
                    return json.loads(payload_str)
                except json.JSONDecodeError:
                    return None
    return None


def _dispatch_phase(
    client: AidpRestClient,
    args: argparse.Namespace,
    wheel_path: Path,
    bundle_yaml: str,
    run_cell: str,
    state_cell: str,
    phase_id: int,
    out_dir: Path,
) -> dict:
    """Upload + dispatch + poll + fetch + parse for one phase. Returns
    the parsed RunSummary payload + wall-clock metadata."""
    nb = _build_notebook(
        wheel_path=wheel_path, bundle_yaml=bundle_yaml,
        secret_name=args.secret_name, secret_key=args.secret_key,
        run_cell_body=run_cell, state_cell_body=state_cell, phase_id=phase_id,
    )
    notebook_path = f"{args.workspace_dir}/phase{phase_id}.ipynb"
    _log("upload", phase=phase_id, path=notebook_path)
    client.upload_notebook(notebook_path, nb)

    # AIDP rejects hyphens in job names — only underscore + slash allowed.
    job_name = f"tc27_phase{phase_id}_{int(time.time())}"
    _log("create_job", phase=phase_id, name=job_name)
    job_key = client.create_notebook_job(
        name=job_name, description=f"TC27 phase {phase_id}",
        notebook_path=notebook_path,
        cluster_key=args.cluster_key, cluster_name=args.cluster_name,
        task_key="tc27_task",
    )
    run_key = client.submit_run(job_key)
    _log("poll_start", phase=phase_id, runKey=run_key)
    result = client.poll_run(
        run_key, timeout_s=args.poll_timeout, interval_s=args.poll_interval,
        on_status_change=lambda s: _log("status", phase=phase_id, status=s),
    )
    _log("poll_done", phase=phase_id, terminal=result.status)
    task_run_key = client.resolve_task_run_key(result.raw, "tc27_task")
    executed_nb_json = client.fetch_output(task_run_key)

    # Save executed notebook for debugging.
    out_path = out_dir / f"phase{phase_id}_executed.ipynb"
    out_path.write_text(executed_nb_json)
    _log("notebook_saved", phase=phase_id, path=str(out_path))

    payload = _parse_marker(executed_nb_json)
    if payload is None:
        raise AidpRestError(
            f"phase {phase_id}: failed to parse AIDP_LIVE_TEST_RESULT marker "
            f"from executed notebook at {out_path}"
        )
    return {
        "phase": phase_id,
        "terminal_status": result.status,
        "payload": payload,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    args = parse_args()
    phases = sorted(set(int(p.strip()) for p in args.phases.split(",")))

    out_dir = Path(f"/tmp/tc27-{int(time.time())}")
    out_dir.mkdir(parents=True, exist_ok=True)
    _log("start", out=str(out_dir), phases=phases)

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

    # Build wheel once — reused across phases.
    _log("build_wheel")
    wheel_path = dispatch.build_wheel(Path(args.plugin_checkout), out_dir / "wheel")
    _log("wheel_ready", path=str(wheel_path), size=wheel_path.stat().st_size)

    # Render bundle (narrow scope — same for all phases).
    bundle_yaml = dispatch.NARROW_BUNDLE.format(
        service_url=args.fusion_service_url,
        username=args.fusion_user,
        external_storage=args.external_storage,
    )

    results: dict[int, dict] = {}

    if 1 in phases:
        _log("phase_1_start", desc="clean baseline")
        results[1] = _dispatch_phase(
            client, args, wheel_path, bundle_yaml,
            run_cell=_BASELINE_RUN_CELL, state_cell=_state_cell_no_op(),
            phase_id=1, out_dir=out_dir,
        )
        _log("phase_1_done", run_id=results[1]["payload"]["run_id"])

    resume_run_id = args.resume_run_id
    if 2 in phases:
        _log("phase_2_start", desc="induced failure")
        results[2] = _dispatch_phase(
            client, args, wheel_path, bundle_yaml,
            run_cell=_INDUCED_FAIL_RUN_CELL, state_cell=_state_cell_no_op(),
            phase_id=2, out_dir=out_dir,
        )
        resume_run_id = results[2]["payload"]["run_id"]
        _log("phase_2_done", run_id=resume_run_id,
             failed=results[2]["payload"]["failed"],
             skipped=results[2]["payload"]["skipped"])

    if 3 in phases:
        if not resume_run_id:
            raise AidpRestError(
                "phase 3 requires --resume-run-id when phase 2 is skipped"
            )
        _log("phase_3_start", desc="resume", resume_run_id=resume_run_id)
        results[3] = _dispatch_phase(
            client, args, wheel_path, bundle_yaml,
            run_cell=_resume_run_cell(resume_run_id),
            state_cell=_state_cell_no_op(),
            phase_id=3, out_dir=out_dir,
        )
        _log("phase_3_done", run_id=results[3]["payload"]["run_id"],
             succeeded=results[3]["payload"]["succeeded"],
             resumed_skipped=results[3]["payload"]["resumed_skipped"])

    # Aggregate report.
    summary_path = out_dir / "tc27_summary.json"
    summary_path.write_text(json.dumps(results, indent=2, default=str))
    _log("complete", summary=str(summary_path))

    print("\n" + "=" * 70)
    print("TC27 SUMMARY")
    print("=" * 70)
    for phase_id, r in results.items():
        p = r["payload"]
        print(f"Phase {phase_id} ({r['terminal_status']:8s})  run_id={p['run_id']}")
        print(f"  succeeded={p['succeeded']}  failed={p['failed']}  "
              f"skipped={p['skipped']}  resumed_skipped={p['resumed_skipped']}  "
              f"wall={p['wall_seconds']:.1f}s")
    if 1 in results and 3 in results:
        t_clean = results[1]["payload"]["wall_seconds"]
        t_resume = results[3]["payload"]["wall_seconds"]
        ratio = t_resume / t_clean if t_clean > 0 else float("inf")
        print(f"\nResume vs clean: {t_resume:.1f}s / {t_clean:.1f}s = {ratio:.2f}× "
              f"(target ≪ 1.0)")
    print(f"\nFull payloads + executed notebooks: {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
