"""TC26 orchestrator probe dispatcher (skill: fusion-tc26-run).

Uses the sibling ``aidp-rest`` skill for OCI signing + REST primitives, and
focuses on the TC26-specific concerns: wheel build, bundle templates, notebook
generation, and result presentation.

See SKILL.md for full workflow and prerequisites.
"""
from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import time
from pathlib import Path

# Import the sibling aidp-rest skill's client. Skills live as siblings under
# .claude/skills/, so we add that parent to sys.path before importing.
_SKILLS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_SKILLS_DIR / "aidp-rest"))
from client import AidpRestClient, AidpRestError  # type: ignore[import-not-found]  # noqa: E402

PLUGIN_NAME = "oracle_ai_data_platform_fusion_bundle"


# ---------------------------------------------------------------------------
# Bundle templates
# ---------------------------------------------------------------------------

NARROW_BUNDLE = """\
apiVersion: aidp-fusion-bundle/v1
version: "0.2.0"
project: tc26-narrow-probe
fusion:
  serviceUrl: {service_url}
  username: {username}
  password: ${{FUSION_BICC_PASSWORD}}
  externalStorage: {external_storage}
aidp:
  catalog: fusion_catalog
  bronzeSchema: bronze
  silverSchema: silver
  goldSchema: gold
  storageFormat: delta
datasets:
  - id: erp_suppliers
    mode: full
  - id: ap_invoices
    mode: full
dimensions:
  build:
    - dim_supplier
    - dim_calendar
gold:
  marts:
    - supplier_spend
"""

FULL_BUNDLE = """\
apiVersion: aidp-fusion-bundle/v1
version: "0.2.0"
project: tc26-full-happy-path
fusion:
  serviceUrl: {service_url}
  username: {username}
  password: ${{FUSION_BICC_PASSWORD}}
  externalStorage: {external_storage}
aidp:
  catalog: fusion_catalog
  bronzeSchema: bronze
  silverSchema: silver
  goldSchema: gold
  storageFormat: delta
datasets:
  - id: gl_journal_lines
    mode: full
  - id: gl_period_balances
    mode: full
  - id: gl_coa
    mode: full
  - id: erp_suppliers
    mode: full
  - id: ar_invoices
    mode: full
  - id: ar_receipts
    mode: full
  - id: ap_invoices
    mode: full
  - id: ap_payments
    mode: full
  - id: po_orders
    mode: full
  - id: po_receipts
    mode: full
  - id: scm_items
    mode: full
dimensions:
  build:
    - dim_account
    - dim_calendar
    - dim_org
    - dim_supplier
    - dim_item
gold:
  marts:
    - ar_aging
    - ap_aging
    - gl_balance
    - po_backlog
    - supplier_spend
"""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TC26 orchestrator probe dispatcher")
    p.add_argument("--scope", choices=["narrow", "full", "custom"], required=True)
    p.add_argument("--bundle-path", help="Required when --scope=custom; bundle.yaml to inline verbatim")
    p.add_argument("--aidp-id", required=True)
    p.add_argument("--workspace-key", required=True)
    p.add_argument("--cluster-key", required=True)
    p.add_argument("--cluster-name", required=True)
    p.add_argument("--region", default="us-ashburn-1")
    p.add_argument("--secret-name", default="fusion_bicc_password")
    p.add_argument("--secret-key", default="password")
    p.add_argument("--fusion-service-url",
                   help="Required for narrow/full; the Fusion pod base URL")
    p.add_argument("--fusion-user",
                   help="Required for narrow/full; BICC username")
    p.add_argument("--external-storage",
                   help="Required for narrow/full; BICC External Storage profile name")
    p.add_argument("--plugin-checkout", default=str(Path(__file__).resolve().parents[3]),
                   help="Path to the plugin checkout (where pyproject.toml lives)")
    p.add_argument("--poll-timeout", type=int, default=2700)
    p.add_argument("--poll-interval", type=int, default=20)
    p.add_argument("--workspace-dir", default=None,
                   help="Workspace path for the uploaded notebook")
    args = p.parse_args()

    if args.scope in ("narrow", "full"):
        missing = [n for n in ("fusion_service_url", "fusion_user", "external_storage")
                   if not getattr(args, n)]
        if missing:
            p.error(f"--scope={args.scope} requires: --"
                    + ", --".join(m.replace("_", "-") for m in missing))
    if args.scope == "custom" and not args.bundle_path:
        p.error("--scope=custom requires --bundle-path")

    if args.workspace_dir is None:
        args.workspace_dir = f"/Workspace/Shared/fusion-bundle-tc26-{args.scope}"
    return args


# ---------------------------------------------------------------------------
# Wheel + notebook generation
# ---------------------------------------------------------------------------

def build_wheel(plugin_checkout: Path, outdir: Path) -> Path:
    outdir.mkdir(parents=True, exist_ok=True)
    res = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(outdir)],
        cwd=plugin_checkout, capture_output=True, text=True, timeout=180,
    )
    if res.returncode != 0:
        raise RuntimeError(f"wheel build failed:\n{res.stdout}\n{res.stderr}")
    wheels = sorted(outdir.glob(f"{PLUGIN_NAME}-*.whl"),
                    key=lambda p: p.stat().st_mtime, reverse=True)
    if not wheels:
        raise RuntimeError(f"no wheel produced in {outdir}")
    return wheels[0]


def render_bundle_yaml(args: argparse.Namespace) -> str:
    if args.scope == "custom":
        return Path(args.bundle_path).read_text()
    template = NARROW_BUNDLE if args.scope == "narrow" else FULL_BUNDLE
    return template.format(
        service_url=args.fusion_service_url,
        username=args.fusion_user,
        external_storage=args.external_storage,
    )


def build_notebook(wheel_path: Path, bundle_yaml: str, secret_name: str,
                   secret_key: str, scope: str) -> dict:
    wheel_b64 = base64.b64encode(wheel_path.read_bytes()).decode()
    wheel_filename = wheel_path.name

    install_cell = (
        f'import base64, subprocess, sys, tempfile, pathlib\n'
        f'WHEEL_B64 = """{wheel_b64}"""\n'
        f'_stage = pathlib.Path(tempfile.mkdtemp(prefix="tc26_plugin_"))\n'
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
        f'pw_len = len(os.environ["FUSION_BICC_PASSWORD"])\n'
        f'print(f"FUSION_BICC_PASSWORD loaded (length={{pw_len}})")\n'
        f'BUNDLE_PATH = Path("bundle.yaml")\n'
        f'BUNDLE_PATH.write_text({bundle_yaml!r})\n'
        f'from oracle_ai_data_platform_fusion_bundle import orchestrator\n'
        f'print("orchestrator loaded")\n'
    )

    run_cell = (
        'import time, json\n'
        'tstart = time.time()\n'
        'summary = orchestrator.run(bundle_path=BUNDLE_PATH, spark=spark, '
        'mode="seed", datasets=None, layers=None, dry_run=False)  # noqa: F821\n'
        'twall = time.time() - tstart\n'
        'print(f"run_id={summary.run_id}")\n'
        'print(f"steps: {summary.succeeded} ok, {summary.failed} failed, "\n'
        '      f"{summary.skipped} skipped, {summary.deferred} deferred "\n'
        '      f"({summary.total_duration_seconds:.1f}s reported / {twall:.1f}s wall)")\n'
        'for step in summary.steps:\n'
        '    skip_tag = f" [{step.skip_reason}]" if step.skip_reason else ""\n'
        '    rc = step.row_count if step.row_count is not None else "-"\n'
        '    err = f" err={step.error_message[:80]}" if step.error_message and step.status=="failed" else ""\n'
        '    print(f"  {step.layer:6s}  {step.dataset_id:24s}  {step.status:10s}{skip_tag:12s}  rows={str(rc):>10s}  dur={step.duration_seconds:.2f}s{err}")\n'
        '_payload = {"tc":"TC26","run_id":summary.run_id,"bundle_project":summary.bundle_project,'
        '"mode":summary.mode,"succeeded":summary.succeeded,"failed":summary.failed,'
        '"skipped":summary.skipped,"deferred":summary.deferred,'
        '"total_duration_seconds":summary.total_duration_seconds,"wall_seconds":twall,'
        '"steps":[{"dataset_id":s.dataset_id,"layer":s.layer,"status":s.status,'
        '"row_count":s.row_count,"duration_seconds":s.duration_seconds,'
        '"skip_reason":s.skip_reason,"error_message":s.error_message} for s in summary.steps]}\n'
        'print("AIDP_LIVE_TEST_RESULT_BEGIN", json.dumps(_payload), '
        '"AIDP_LIVE_TEST_RESULT_END")\n'
    )

    state_cell = (
        'from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import load_bundle\n'
        '_bundle, _paths = load_bundle(BUNDLE_PATH)\n'
        '_state_table = _paths.bronze("fusion_bundle_state")\n'
        'spark.sql(f"""SELECT dataset_id, layer, mode, status, row_count, '
        'skip_reason, duration_seconds FROM (SELECT *, ROW_NUMBER() OVER '
        '(PARTITION BY dataset_id ORDER BY last_run_at DESC) AS rn FROM '
        '{_state_table} WHERE run_id = \'{summary.run_id}\') t WHERE rn=1 '
        'ORDER BY layer, dataset_id""").show(200, truncate=False)\n'
        'for layer in ("silver","gold"):\n'
        '    rc_col = f"{layer}_run_id"\n'
        '    candidate = next((s for s in summary.steps if s.layer==layer and s.status=="success"), None)\n'
        '    if candidate is None:\n'
        '        print(f"  (no successful {layer} rows)"); continue\n'
        '    table = _paths.silver(candidate.dataset_id) if layer=="silver" else _paths.gold(candidate.dataset_id)\n'
        '    n = spark.sql(f"SELECT COUNT(*) AS n FROM {table} WHERE {rc_col} = \'{summary.run_id}\'").collect()[0].n\n'
        '    total = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").collect()[0].n\n'
        '    print(f"SOX-trail {layer:6s} {candidate.dataset_id:20s}: {rc_col} matches on {n}/{total} rows")\n'
    )

    def code_cell(src: str) -> dict:
        return {"cell_type": "code", "execution_count": None, "metadata": {},
                "outputs": [], "source": src.splitlines(keepends=True)}

    return {
        "cells": [
            {"cell_type": "markdown", "metadata": {},
             "source": [f"# TC26 — {scope} (orchestrator end-to-end)\n",
                        "Self-contained dispatch via fusion-tc26-run skill."]},
            code_cell(install_cell),
            code_cell(creds_cell),
            code_cell(run_cell),
            code_cell(state_cell),
        ],
        "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                     "language_info": {"name": "python"}},
        "nbformat": 4, "nbformat_minor": 5,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _log(stage: str, **kw):
    extras = " ".join(f"{k}={v}" for k, v in kw.items())
    print(f"[{stage}] {extras}", flush=True)


def main() -> int:
    args = parse_args()

    client = AidpRestClient(
        region=args.region, aidp_id=args.aidp_id, workspace_key=args.workspace_key,
        log=_log,
    )

    stamp = int(time.time())
    workdir = Path(f"/tmp/tc26-{args.scope}-{stamp}")
    workdir.mkdir(parents=True, exist_ok=True)
    _log("workdir", path=str(workdir))

    try:
        client.verify_cluster_active(args.cluster_key)
    except AidpRestError as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 2

    _log("building_wheel")
    wheel = build_wheel(Path(args.plugin_checkout), workdir / "dist")
    _log("wheel_built", path=str(wheel), size=wheel.stat().st_size)

    bundle_yaml = render_bundle_yaml(args)
    ipynb = build_notebook(wheel, bundle_yaml, args.secret_name, args.secret_key, args.scope)
    nb_local = workdir / f"run_tc26_{args.scope}.ipynb"
    nb_local.write_text(json.dumps(ipynb, indent=1))
    _log("notebook_built", size=nb_local.stat().st_size)

    remote_nb = f"{args.workspace_dir}/run_tc26_{args.scope}.ipynb"
    client.upload_notebook(remote_nb, ipynb)

    task_key = f"tc26_{args.scope}_main"
    job_key = client.create_notebook_job(
        name=f"tc26_{args.scope}_{stamp}",
        description=f"TC26 {args.scope} probe",
        notebook_path=remote_nb,
        cluster_key=args.cluster_key, cluster_name=args.cluster_name,
        task_key=task_key,
    )
    run_key = client.submit_run(job_key)

    try:
        result = client.poll_run(run_key, timeout_s=args.poll_timeout,
                                 interval_s=args.poll_interval)
    except AidpRestError as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 2

    task_run_key = client.resolve_task_run_key(result.raw, task_key)
    _log("task_run_resolved", taskRunKey=task_run_key)

    if result.status != "SUCCESS":
        tr = (result.raw.get("taskRunSummaryMap") or {}).get(task_run_key, {})
        ts = tr.get("state", {})
        print(f"task status={ts.get('status')}  stateMessage={ts.get('stateMessage')}",
              file=sys.stderr)

    try:
        nb_str = client.fetch_output(task_run_key)
    except AidpRestError as e:
        # AIDP job ran (terminal status known) but evidence fetch failed. Don't
        # quietly exit 0 — this is an evidence-capture failure that CI/operator
        # must see. Reviewer catch: success-without-evidence is worse than a
        # clean failure because it masks the gap.
        print(f"FATAL: fetchOutput failed despite terminal status {result.status}: {e}",
              file=sys.stderr)
        return 2

    executed_path = workdir / f"run_tc26_{args.scope}.executed.ipynb"
    executed_path.write_text(nb_str)
    _log("executed_notebook_saved", path=str(executed_path))

    executed = json.loads(nb_str) if nb_str else {}
    marker = client.parse_marker(
        executed,
        begin="AIDP_LIVE_TEST_RESULT_BEGIN",
        end="AIDP_LIVE_TEST_RESULT_END",
    )

    print("\n=== RESULT ===")
    print(json.dumps({
        "terminal_status": result.status,
        "jobKey": job_key, "jobRunKey": run_key, "taskRunKey": task_run_key,
        "scope": args.scope, "executed_notebook": str(executed_path),
    }, indent=2))

    if marker:
        print("\n=== STEPS (from marker) ===")
        for s in marker.get("steps", []):
            rc = str(s.get("row_count")) if s.get("row_count") is not None else "-"
            skip = f" [{s.get('skip_reason')}]" if s.get("skip_reason") else ""
            err = (f" err={s.get('error_message','')[:60]}"
                   if s.get("error_message") and s.get("status") == "failed" else "")
            print(f"  {s['layer']:6s}  {s['dataset_id']:24s}  "
                  f"{s['status']:10s}{skip:12s}  rows={rc:>10s}  "
                  f"dur={s['duration_seconds']:.2f}s{err}")
        print(f"\nTotals: {marker['succeeded']} ok, {marker['failed']} failed, "
              f"{marker['skipped']} skipped, {marker['deferred']} deferred "
              f"({marker['total_duration_seconds']:.1f}s)")
    elif result.status != "SUCCESS":
        # No marker + non-success → surface cell errors so the operator can diagnose
        errors = client.extract_cell_errors(executed) if executed else []
        if errors:
            print("\n=== CELL ERRORS ===")
            for err in errors:
                print(f"cell {err['cell_index']}: {err['ename']}: {err['evalue']}")

    # Reviewer catch (success-without-evidence): if the AIDP job reported
    # SUCCESS but we couldn't parse the AIDP_LIVE_TEST_RESULT marker, treat
    # as a failure. "Job succeeded but no marker" is an evidence-capture gap
    # that CI must see — it usually means the notebook crashed AFTER the
    # orchestrator's marker emit but before AIDP wrote the run-output, OR
    # the notebook never reached the marker emit at all.
    if result.status == "SUCCESS" and marker is None:
        print("FATAL: AIDP job reported SUCCESS but no AIDP_LIVE_TEST_RESULT "
              "marker found in executed notebook — evidence capture failed. "
              "Inspect the saved notebook for the actual run state.",
              file=sys.stderr)
        return 2

    return 0 if result.status == "SUCCESS" else 2


if __name__ == "__main__":
    sys.exit(main())
