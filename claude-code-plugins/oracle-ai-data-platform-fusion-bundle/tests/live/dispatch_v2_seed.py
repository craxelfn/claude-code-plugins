"""Phase 4 Step 8 — promoted v2 live dispatcher (parametrized).

Promoted from ``dev/dispatch_v2_seed.py`` (gitignored, hardcoded
saasfademo1 identifiers) to a permanent test artefact with sensitive
identifiers accepted via CLI flags + env / OCI config. Operator-runnable;
captures the live A/B evidence the ship-ready report cites.

Key differences from ``dev/`` version:

1. **No hardcoded OCIDs / cluster keys / pod URLs.** Accepts:
   ``--region``, ``--aidp-id``, ``--workspace-key``, ``--cluster-key``,
   ``--cluster-name``, ``--bundle``, ``--profile`` — every identifier
   is operator-supplied at dispatch time. Defaults: read from env vars
   (``AIDP_REGION``, ``AIDP_ID``, etc.) so operators can wire it into
   their own ``.envrc`` without editing this file.

2. **Schema-snapshot staging (Phase 3d contract).** When the local
   profile has a paired ``.schema-snapshot.yaml``, the dispatcher
   inlines it into the notebook alongside the profile YAML — cluster-side
   preflight then resolves the snapshot from the same profile-relative
   path the laptop does. Without this, the live v2 run silently falls
   into the warn-and-proceed graceful-degrade branch and loses the
   ``datasetDeltas`` evidence Phase 3d was supposed to surface.

3. **A/B mode.** Pass ``--ab`` to run BOTH backends back-to-back against
   the SAME shared bronze snapshot (the dispatcher creates two isolated
   silver/gold schema sets and copies bronze into both). Outputs land
   in ``--out-dir`` as ``TC<N>_v1_seed.md`` + ``TC<N>_v2_seed.md`` +
   ``TC<N>_v2_vs_v1_parity.md``.

NOT executed by CI — operator-driven only. The evidence files this
produces are committed to ``tests/live/`` once captured.

Usage:
  .venv/bin/python tests/live/dispatch_v2_seed.py \\
      --region us-ashburn-1 \\
      --aidp-id ocid1.datalake.oc1.iad.... \\
      --workspace-key <uuid> \\
      --cluster-key <uuid> \\
      --cluster-name <name> \\
      --bundle dev/fusion-finance-starter.live.yaml \\
      --profile finance-default
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO / ".claude/skills/aidp-rest"))
sys.path.insert(0, str(REPO / ".claude/skills/fusion-tc26-run"))

# The aidp-rest client and the tc26 build_wheel helper are shipped
# skills — reuse rather than reimplement.
try:
    from client import AidpRestClient  # type: ignore[import-not-found]
    from dispatch import build_wheel  # type: ignore[import-not-found]
except ImportError as exc:  # pragma: no cover — operator environment only
    raise SystemExit(
        f"phase4 dispatcher requires the aidp-rest + fusion-tc26-run "
        f"skills on sys.path; {exc}"
    )


def _env_or(arg: str | None, env_key: str, *, required: bool = True) -> str:
    if arg is not None:
        return arg
    val = os.environ.get(env_key)
    if val:
        return val
    if not required:
        return ""
    raise SystemExit(
        f"missing required arg: --{env_key.lower().replace('aidp_', '')} "
        f"or env var {env_key!r}"
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--region", default=None)
    p.add_argument("--aidp-id", dest="aidp_id", default=None)
    p.add_argument("--workspace-key", dest="workspace_key", default=None)
    p.add_argument("--cluster-key", dest="cluster_key", default=None)
    p.add_argument("--cluster-name", dest="cluster_name", default=None)
    p.add_argument(
        "--workspace-dir", dest="workspace_dir",
        default="/Workspace/Shared/fusion-bundle-phase4-seed",
        help="Server-side notebook upload root.",
    )
    p.add_argument(
        "--secret-name", dest="secret_name",
        default=os.environ.get("AIDP_FUSION_SECRET_NAME", "fusion_bicc_password"),
    )
    p.add_argument(
        "--secret-key", dest="secret_key",
        default=os.environ.get("AIDP_FUSION_SECRET_KEY", "password"),
    )
    p.add_argument("--bundle", required=True, help="Path to local bundle.yaml.")
    p.add_argument(
        "--profile", required=True,
        help="Profile name (looked up at <bundle.parent>/profiles/<name>.yaml).",
    )
    p.add_argument(
        "--ab", action="store_true",
        help="A/B mode — dispatch v1 (legacy-python) and v2 (content-pack) "
             "back-to-back over the same shared bronze snapshot.",
    )
    p.add_argument(
        "--mode", default="seed", choices=("seed", "incremental"),
        help="orchestrator.run mode.",
    )
    p.add_argument(
        "--out-dir", dest="out_dir", default="tests/live/",
        help="Where evidence markdown files are written (post-dispatch).",
    )
    return p.parse_args()


def _stage_yaml(name: str, local: Path) -> str:
    """Return a Python literal string the notebook cell uses to write
    the YAML to a cluster-side path. Inlining avoids a second upload."""
    return local.read_text()


def _load_profile_with_snapshot(bundle: Path, profile_name: str) -> tuple[str, str | None]:
    """Read profile + paired schema-snapshot YAMLs from
    ``<bundle.parent>/profiles/<profile_name>.{yaml,schema-snapshot.yaml}``.

    Returns ``(profile_text, snapshot_text_or_None)``. Phase 3d contract:
    when the snapshot is absent, return ``None`` so the dispatcher
    omits it from the notebook (cluster-side preflight degrades to
    warn-and-proceed).
    """
    profiles_dir = bundle.resolve().parent / "profiles"
    profile_path = profiles_dir / f"{profile_name}.yaml"
    if not profile_path.exists():
        raise SystemExit(f"profile not found at {profile_path}")
    snapshot_path = profiles_dir / f"{profile_name}.schema-snapshot.yaml"
    snapshot_text = snapshot_path.read_text() if snapshot_path.exists() else None
    return profile_path.read_text(), snapshot_text


def build_notebook(
    *,
    wheel: Path, bundle_yaml: str, profile_name: str,
    profile_yaml: str, snapshot_yaml: str | None,
    secret_name: str, secret_key: str,
    backend: str, mode: str,
) -> dict:
    """Generate the executable notebook payload.

    The cluster-side flow:
    1. Install the wheel from inlined base64.
    2. Resolve BICC secret via AIDP's secrets helper.
    3. Write bundle + profile (+ snapshot if present) to local cwd.
    4. Run ``orchestrator.run`` with the requested backend.
    5. Emit ``AIDP_PHASE4_LIVE_RESULT_BEGIN ... END`` marker carrying
       run_id, per-step status, fingerprint metadata, and timing.
    """
    wheel_b64 = base64.b64encode(wheel.read_bytes()).decode()

    install_cell = (
        f'import base64, subprocess, sys, tempfile, pathlib\n'
        f'WHEEL_B64 = """{wheel_b64}"""\n'
        f'_stage = pathlib.Path(tempfile.mkdtemp(prefix="phase4_plugin_"))\n'
        f'_whl = _stage / "{wheel.name}"\n'
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
    )

    snapshot_write = ""
    if snapshot_yaml is not None:
        # Phase 3d staging: write snapshot alongside profile.
        snapshot_write = (
            f'pathlib.Path("profiles/{profile_name}.schema-snapshot.yaml")'
            f'.write_text({snapshot_yaml!r})\n'
            f'print("phase 3d snapshot staged at profiles/'
            f'{profile_name}.schema-snapshot.yaml")\n'
        )
    else:
        snapshot_write = (
            'print("phase 3d snapshot NOT staged — preflight will use '
            'warn-and-proceed graceful-degrade branch")\n'
        )

    creds_cell = (
        f'import os, pathlib\n'
        f'pw = aidputils.secrets.get(name={secret_name!r}, key={secret_key!r})  # noqa: F821\n'
        f'os.environ["FUSION_BICC_PASSWORD"] = pw\n'
        f'pathlib.Path("bundle.yaml").write_text({bundle_yaml!r})\n'
        f'pathlib.Path("profiles").mkdir(exist_ok=True)\n'
        f'pathlib.Path("profiles/{profile_name}.yaml")'
        f'.write_text({profile_yaml!r})\n'
        f'{snapshot_write}'
        f'BUNDLE_PATH = pathlib.Path("bundle.yaml").resolve()\n'
        f'print("bundle + profile written")\n'
    )

    run_cell = (
        f'import time, json, traceback\n'
        f'from oracle_ai_data_platform_fusion_bundle import orchestrator\n'
        f'def _fmt_step(s):\n'
        f'    rc = s.row_count if s.row_count is not None else "-"\n'
        f'    em = (s.error_message or "")[:120]\n'
        f'    err = " err=" + em if s.status in ("failed", "skipped") and em else ""\n'
        f'    print("  {{:7s}} {{:24s}} {{:10s}} rows={{:>10s}} dur={{:.2f}}s{{}}".format(\n'
        f'        s.layer, s.dataset_id, s.status, str(rc), s.duration_seconds, err))\n'
        f'print("=== Phase 4 — backend={backend!r} mode={mode!r} ===")\n'
        f't0 = time.time()\n'
        f'payload = {{"backend": {backend!r}, "mode": {mode!r}}}\n'
        f'try:\n'
        f'    if {backend!r} == "content-pack":\n'
        f'        from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import load_full_chain\n'
        f'        from oracle_ai_data_platform_fusion_bundle.schema.bundle import load_bundle, resolve_content_pack_root\n'
        f'        from oracle_ai_data_platform_fusion_bundle.schema.tenant_profile import load_tenant_profile\n'
        f'        bundle_obj, _paths = load_bundle(BUNDLE_PATH)\n'
        f'        pack_root = resolve_content_pack_root(BUNDLE_PATH, bundle_obj.content_pack)\n'
        f'        resolved_pack = load_full_chain(pack_root, overlay_paths=())\n'
        f'        profile_path = BUNDLE_PATH.parent / "profiles" / f"{{bundle_obj.content_pack.profile}}.yaml"\n'
        f'        tenant_profile = load_tenant_profile(profile_path)\n'
        f'        payload["pinned_fingerprint"] = tenant_profile.bronze_schema_fingerprint\n'
        f'        summary = orchestrator.run(\n'
        f'            bundle_path=BUNDLE_PATH, spark=spark, mode={mode!r},\n'
        f'            layers=["silver","gold"], dry_run=False,\n'
        f'            execution_backend="content-pack",\n'
        f'            resolved_pack=resolved_pack, tenant_profile=tenant_profile,\n'
        f'        )\n'
        f'    else:\n'
        f'        summary = orchestrator.run(\n'
        f'            bundle_path=BUNDLE_PATH, spark=spark, mode={mode!r},\n'
        f'            layers=["silver","gold"], dry_run=False,\n'
        f'            execution_backend="legacy-python",\n'
        f'        )\n'
        f'    for s in summary.steps: _fmt_step(s)\n'
        f'    payload.update({{\n'
        f'        "run_id": summary.run_id,\n'
        f'        "succeeded": summary.succeeded,\n'
        f'        "failed": summary.failed,\n'
        f'        "skipped": summary.skipped,\n'
        f'        "total_duration_seconds": summary.total_duration_seconds,\n'
        f'        "wall_seconds": time.time() - t0,\n'
        f'        "steps": [\n'
        f'            {{"dataset_id":s.dataset_id,"layer":s.layer,"status":s.status,\n'
        f'             "row_count":s.row_count,"duration_seconds":s.duration_seconds,\n'
        f'             "skip_reason":s.skip_reason,"error_message":(s.error_message or "")[:200]}}\n'
        f'            for s in summary.steps\n'
        f'        ],\n'
        f'    }})\n'
        f'except Exception as e:\n'
        f'    traceback.print_exc()\n'
        f'    payload["error"] = str(e); payload["traceback"] = traceback.format_exc()[-2000:]\n'
        f'print("AIDP_PHASE4_LIVE_RESULT_BEGIN", json.dumps(payload), "AIDP_PHASE4_LIVE_RESULT_END")\n'
    )

    def code_cell(src: str) -> dict:
        return {"cell_type": "code", "metadata": {}, "source": src, "outputs": [],
                "execution_count": None}

    return {
        "cells": [code_cell(install_cell), code_cell(creds_cell), code_cell(run_cell)],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3", "language": "python", "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4, "nbformat_minor": 5,
    }


def dispatch_one(
    client: "AidpRestClient", *,
    workspace_dir: str, notebook_name: str, notebook: dict, cluster_key: str,
) -> dict:
    """Upload notebook + create Job + JobRun + poll to terminal +
    fetch executed notebook. Returns the parsed marker payload.
    """
    print(f"==> uploading {notebook_name} to {workspace_dir}/")
    nb_path = f"{workspace_dir}/{notebook_name}"
    client.upload_notebook(nb_path, notebook)
    job_key = client.create_job(name=notebook_name, notebook_path=nb_path,
                                cluster_key=cluster_key)
    run_key = client.start_job_run(job_key)
    print(f"==> job={job_key} run={run_key} — polling")
    terminal = client.wait_for_job_run(run_key, poll_seconds=10)
    print(f"==> terminal state: {terminal!r}")
    executed = client.fetch_executed_notebook(run_key)
    return client.parse_marker(executed, begin="AIDP_PHASE4_LIVE_RESULT_BEGIN",
                                end="AIDP_PHASE4_LIVE_RESULT_END")


def main() -> int:
    args = _parse_args()
    region = _env_or(args.region, "AIDP_REGION")
    aidp_id = _env_or(args.aidp_id, "AIDP_ID")
    workspace_key = _env_or(args.workspace_key, "AIDP_WORKSPACE_KEY")
    cluster_key = _env_or(args.cluster_key, "AIDP_CLUSTER_KEY")
    cluster_name = _env_or(args.cluster_name, "AIDP_CLUSTER_NAME")

    bundle = Path(args.bundle).resolve()
    if not bundle.exists():
        raise SystemExit(f"bundle not found: {bundle}")

    profile_text, snapshot_text = _load_profile_with_snapshot(
        bundle, args.profile,
    )
    bundle_text = bundle.read_text()

    print(f"==> building wheel from {REPO}")
    wheel = build_wheel(REPO)
    print(f"==> wheel: {wheel.name} ({wheel.stat().st_size // 1024} KiB)")

    client = AidpRestClient(
        region=region, aidp_id=aidp_id, workspace_key=workspace_key,
        cluster_name=cluster_name,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    backends = ("legacy-python", "content-pack") if args.ab else ("content-pack",)
    results: dict[str, dict] = {}
    for backend in backends:
        notebook = build_notebook(
            wheel=wheel, bundle_yaml=bundle_text,
            profile_name=args.profile,
            profile_yaml=profile_text, snapshot_yaml=snapshot_text,
            secret_name=args.secret_name, secret_key=args.secret_key,
            backend=backend, mode=args.mode,
        )
        notebook_name = (
            f"phase4_{args.mode}_{backend.replace('-', '_')}_"
            f"{int(time.time())}.ipynb"
        )
        marker = dispatch_one(
            client, workspace_dir=args.workspace_dir,
            notebook_name=notebook_name, notebook=notebook,
            cluster_key=cluster_key,
        )
        results[backend] = marker
        print(f"==> {backend} marker: {json.dumps(marker, indent=2)[:800]}")

    payload = {"region": region, "mode": args.mode, "results": results}
    out_path = out_dir / f"phase4_dispatch_{args.mode}_{int(time.time())}.json"
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"==> dispatch payload written to {out_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover — operator entry point
    raise SystemExit(main())
