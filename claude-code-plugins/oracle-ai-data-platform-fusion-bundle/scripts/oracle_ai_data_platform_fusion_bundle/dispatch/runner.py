"""``dispatch_run`` — the laptop-side end-to-end orchestrator dispatcher.

This is what ``aidp-fusion-bundle run --mode <mode>`` calls today (via
``commands/run.py:_run_via_aidp_dispatch``). It owns the seven phases
that the operator used to do by hand:

  1. Resolve AIDP coordinates (workspace UUID, cluster UUID/state) from
     human-readable names in ``aidp.config.yaml``.
  2. Auto-start the cluster if STOPPED, wait until ACTIVE.
  3. Build a wheel of the plugin from the configured checkout.
  4. Generate the dispatch notebook (4 cells; see ``notebook_builder``).
  5. Upload to ``/Workspace/<workspaceRoot>/fusion-bundle/<stamp>/run.ipynb``.
  6. Create the NOTEBOOK_TASK job + submit a JobRun + poll to terminal.
  7. Fetch the executed notebook, parse the marker payload, render a
     per-step table.

Exit code:
  0 — every step succeeded.
  1 — at least one orchestrator step failed (marker present, ``failed > 0``).
  2 — dispatch never ran the orchestrator (cluster, upload, job, or
     marker parsing failed before a real per-step verdict was produced).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError
from rich.console import Console
from rich.table import Table

from ..schema.bundle import AidpConfig, Bundle, EnvSpec
from ..schema.refs import render_tree
from .notebook_builder import MARKER_BEGIN, MARKER_END, build_notebook
from .rest_client import (
    AidpRestClient,
    AidpRestError,
)
from .wheel_builder import WheelBuildError, build_wheel

DEFAULT_SECRET_NAME = "fusion_bicc_password"
DEFAULT_SECRET_KEY = "password"
DEFAULT_POLL_TIMEOUT_S = 7200  # 2h — first BICC extract on a big PVO can run 20-30 min, and seed-mode chains 11 of them
DEFAULT_POLL_INTERVAL_S = 20
DEFAULT_CLUSTER_WAIT_S = 600


class DispatchError(RuntimeError):
    """Anything fatal in the laptop-side dispatch flow before the
    orchestrator gets a chance to produce a verdict."""


@dataclass(frozen=True)
class _DispatchCoords:
    """Resolved AIDP coordinates for one dispatch."""

    aidp_id: str
    workspace_key: str
    workspace_display_name: str | None
    cluster_key: str
    cluster_display_name: str
    region: str
    oci_profile: str
    secret_name: str
    secret_key: str
    workspace_root: str


# ----------------------------------------------------------------------
# Public entry point
# ----------------------------------------------------------------------


def dispatch_run(
    *,
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    mode: str = "incremental",
    datasets: list[str] | None = None,
    layers: list[str] | None = None,
    plugin_checkout: Path | None = None,
    poll_timeout_s: int = DEFAULT_POLL_TIMEOUT_S,
    poll_interval_s: int = DEFAULT_POLL_INTERVAL_S,
    cluster_wait_s: int = DEFAULT_CLUSTER_WAIT_S,
    console: Console | None = None,
) -> int:
    """Dispatch ``orchestrator.run(mode=...)`` to a live AIDP cluster.

    See module docstring for the seven phases. Returns a process exit code.
    """
    console = console or Console()

    bundle, config, env = _load_inputs(bundle_path, config_path, env_name, console)
    if bundle is None or config is None or env is None:
        return 2

    coords = _resolve_dispatch_coords(env, config, console)
    if coords is None:
        return 2

    client = AidpRestClient(
        region=coords.region,
        aidp_id=coords.aidp_id,
        workspace_key=coords.workspace_key,
        oci_profile=coords.oci_profile,
    )

    # --- Phase 1+2 : cluster lifecycle ---------------------------------
    if not _ensure_cluster_active(client, coords, cluster_wait_s, console):
        return 2

    # --- Phase 3 : build wheel ----------------------------------------
    stamp = int(time.time())
    workdir = Path(f"/tmp/aidp-fusion-bundle-dispatch-{stamp}")
    workdir.mkdir(parents=True, exist_ok=True)

    checkout = plugin_checkout or _infer_plugin_checkout(bundle_path)
    console.print(f"[3/7] Building wheel from [cyan]{checkout}[/cyan] ...")
    try:
        wheel = build_wheel(checkout, workdir / "dist")
    except WheelBuildError as exc:
        console.print(f"[red]wheel build failed:[/red] {exc}")
        return 2
    console.print(f"      [green]wheel built[/green]: {wheel.name} ({wheel.stat().st_size // 1024} KB)")

    # --- Phase 4 : build notebook -------------------------------------
    console.print("[4/7] Building dispatch notebook ...")
    # Pre-render ${VAR} placeholders on the laptop side (the cluster has
    # no access to the operator's .env). ``render_tree`` leaves
    # ``${vault:OCID}`` untouched so cluster-side vault resolution still
    # works for vault-backed secrets.
    #
    # We DELIBERATELY hide ``FUSION_BICC_PASSWORD`` from the laptop-side
    # render: bundle.yaml's ``password: ${FUSION_BICC_PASSWORD}`` is
    # meant to be resolved INSIDE the cluster (where the credential-store
    # cell sets the env var). Pre-rendering it on the laptop would
    # silently bake the cleartext password into the uploaded yaml.
    import os as _os
    bundle_yaml_tree = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    _saved_pw = _os.environ.pop("FUSION_BICC_PASSWORD", None)
    try:
        bundle_yaml_rendered = render_tree(bundle_yaml_tree)
    finally:
        if _saved_pw is not None:
            _os.environ["FUSION_BICC_PASSWORD"] = _saved_pw
    bundle_yaml_text = yaml.safe_dump(bundle_yaml_rendered, default_flow_style=False, sort_keys=False)
    ipynb = build_notebook(
        wheel_path=wheel,
        bundle_yaml_text=bundle_yaml_text,
        secret_name=coords.secret_name,
        secret_key=coords.secret_key,
        mode=mode,
        datasets=datasets,
        layers=layers,
        title_suffix=f"{bundle.project} / {mode} / {stamp}",
    )
    local_nb = workdir / "run.ipynb"
    local_nb.write_text(json.dumps(ipynb, indent=1))

    # --- Phase 5 : upload ---------------------------------------------
    remote_dir = f"/Workspace/{coords.workspace_root}/fusion-bundle/{stamp}"
    remote_path = f"{remote_dir}/run.ipynb"
    console.print(f"[5/7] Uploading notebook to [cyan]{remote_path}[/cyan] ...")
    try:
        stored_path = client.upload_notebook(remote_path, ipynb)
    except AidpRestError as exc:
        console.print(f"[red]upload failed:[/red] {exc}")
        return 2
    console.print(f"      [green]uploaded[/green] ({local_nb.stat().st_size // 1024} KB)")

    # --- Phase 6 : create job + submit run + poll ---------------------
    console.print("[6/7] Creating job + submitting run ...")
    # AIDP /jobs rejects names with hyphens or any special char other than
    # underscore + slash + letters/digits. Sanitise the project slug.
    safe_project = "".join(c if c.isalnum() else "_" for c in bundle.project)
    job_name = f"fusion_bundle_{safe_project}_{mode}_{stamp}"
    try:
        job_key = client.create_notebook_job(
            name=job_name,
            description=f"aidp-fusion-bundle dispatch (mode={mode})",
            notebook_path=stored_path,
            cluster_key=coords.cluster_key,
            cluster_name=coords.cluster_display_name,
            task_key="run_orchestrator",
        )
        run_key = client.submit_run(job_key)
    except AidpRestError as exc:
        console.print(f"[red]job submission failed:[/red] {exc}")
        return 2
    console.print(f"      job_key=[cyan]{job_key}[/cyan]  run_key=[cyan]{run_key}[/cyan]")

    console.print("      polling ...", highlight=False)
    last_status: list[str] = []

    def _on_change(status: str) -> None:
        last_status.append(status)
        console.print(f"      [yellow]{status}[/yellow]", highlight=False)

    try:
        run_result = client.poll_run(
            run_key,
            timeout_s=poll_timeout_s,
            interval_s=poll_interval_s,
            on_status_change=_on_change,
        )
    except AidpRestError as exc:
        msg = str(exc).lower()
        # ``deadline exceeded`` is laptop-side patience; the cluster job
        # is still alive. Print a soft handoff so the operator can grab
        # the result later via ``fetch-result`` without re-running.
        if "deadline exceeded" in msg or "timeout" in msg:
            console.print(
                f"[yellow]Laptop poll gave up after {poll_timeout_s}s — the cluster job "
                f"is still alive. Resume with:[/yellow]\n"
                f"  [cyan]aidp-fusion-bundle fetch-result --run-key {run_key}[/cyan]"
            )
            return 0
        # ``HTTP 401`` mid-poll = OCI session expired. Same handoff.
        if "http 401" in msg or "notauthenticated" in msg:
            console.print(
                f"[yellow]OCI session expired mid-poll — refresh and resume with:[/yellow]\n"
                f"  [cyan]oci session refresh --profile {coords.oci_profile}[/cyan]\n"
                f"  [cyan]aidp-fusion-bundle fetch-result --run-key {run_key}[/cyan]"
            )
            return 0
        console.print(f"[red]poll failed:[/red] {exc}")
        return 2

    # --- Phase 7 : fetch + parse --------------------------------------
    return _fetch_and_render(
        client=client,
        run_result=run_result,
        task_key="run_orchestrator",
        workdir=workdir,
        console=console,
    )


def fetch_result(
    *,
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    run_key: str,
    task_key: str = "run_orchestrator",
    console: Console | None = None,
) -> int:
    """Fetch + render a previously-dispatched run's result.

    Used when ``dispatch_run`` returned early (poll timeout, 401), or
    when the operator manually wants to re-render an old run. Reads the
    same ``bundle.yaml`` / ``aidp.config.yaml`` to find the workspace
    coordinates, then walks Phase 7 only — no wheel build, no upload,
    no job creation.
    """
    console = console or Console()
    bundle, config, env = _load_inputs(bundle_path, config_path, env_name, console)
    if bundle is None or config is None or env is None:
        return 2

    coords = _resolve_dispatch_coords(env, config, console)
    if coords is None:
        return 2

    client = AidpRestClient(
        region=coords.region,
        aidp_id=coords.aidp_id,
        workspace_key=coords.workspace_key,
        oci_profile=coords.oci_profile,
    )
    # When the env block uses workspaceName not workspaceKey, the client
    # was built with an empty workspace_key; resolve it before any call
    # that touches /workspaces/<wk>/...
    if not coords.workspace_key and coords.workspace_display_name:
        try:
            ws = client.find_workspace_by_name(coords.workspace_display_name)
        except AidpRestError as exc:
            console.print(f"[red]workspace lookup failed:[/red] {exc}")
            return 2
        client.workspace_key = ws.key

    try:
        run_raw = client.get_run(run_key)
    except AidpRestError as exc:
        console.print(f"[red]get_run failed:[/red] {exc}")
        return 2
    status = run_raw.get("state", {}).get("status")
    console.print(f"Run [cyan]{run_key}[/cyan] state: [bold]{status}[/bold]")
    if status not in AidpRestClient.TERMINAL_STATUSES:
        console.print(f"[yellow]Run is still {status} — try again in a few minutes.[/yellow]")
        return 0

    workdir = Path(f"/tmp/aidp-fusion-bundle-fetch-{int(time.time())}")
    workdir.mkdir(parents=True, exist_ok=True)
    from .rest_client import RunResult

    return _fetch_and_render(
        client=client,
        run_result=RunResult(status=status, raw=run_raw),
        task_key=task_key,
        workdir=workdir,
        console=console,
    )


def _fetch_and_render(
    *,
    client: AidpRestClient,
    run_result,
    task_key: str,
    workdir: Path,
    console: Console,
) -> int:
    """Phase 7 implementation, shared by ``dispatch_run`` and ``fetch_result``.

    Resolves the taskRunKey, fetches the executed notebook, saves it
    locally, parses the orchestrator marker payload, renders the
    per-step table. Returns the same exit code semantics as
    :func:`dispatch_run`.
    """
    console.print(f"[7/7] Run terminal state: [bold]{run_result.status}[/bold]")
    task_run_key = AidpRestClient.resolve_task_run_key(run_result.raw, task_key)
    try:
        executed_nb_json = client.fetch_output(task_run_key, output_key="")
    except AidpRestError as exc:
        console.print(f"[red]fetch_output failed:[/red] {exc}")
        return 2

    executed_nb = json.loads(executed_nb_json) if executed_nb_json else {}
    local_executed = workdir / "run.executed.ipynb"
    local_executed.write_text(executed_nb_json or "{}")
    console.print(f"      executed notebook saved: [cyan]{local_executed}[/cyan]")

    # The notebook emits a base64-wrapped JSON payload — see
    # ``notebook_builder.py`` for why. ``parse_marker`` first tries to
    # JSON-decode whatever's between BEGIN/END markers; when that fails
    # (legacy raw-JSON markers, or unwrapping bugs), we fall back to
    # locating the base64 token and decoding it ourselves.
    marker = AidpRestClient.parse_marker(executed_nb, begin=MARKER_BEGIN, end=MARKER_END)
    if marker is None:
        import base64 as _b64

        for cell in executed_nb.get("cells", []):
            for output in cell.get("outputs", []):
                blob = output.get("text") or output.get("data", {}).get("text/plain", "")
                if isinstance(blob, list):
                    blob = "".join(blob)
                if MARKER_BEGIN in blob and MARKER_END in blob:
                    b = blob.index(MARKER_BEGIN) + len(MARKER_BEGIN)
                    e = blob.index(MARKER_END, b)
                    tok = blob[b:e].strip()
                    try:
                        marker = json.loads(_b64.b64decode(tok))
                        break
                    except Exception:
                        continue
            if marker is not None:
                break
    if marker is None:
        console.print("[red]marker not found in executed notebook[/red] — run likely crashed before orchestrator emitted summary")
        for err in AidpRestClient.extract_cell_errors(executed_nb):
            console.print(f"  cell {err['cell_index']}: {err.get('ename')}: {err.get('evalue')}")
        return 2

    _render_summary(marker, run_result.status, console)
    return 0 if marker.get("failed", 0) == 0 and run_result.status == "SUCCESS" else 1


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _load_inputs(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    console: Console,
) -> tuple[Bundle | None, AidpConfig | None, EnvSpec | None]:
    if not bundle_path.exists():
        console.print(f"[red]bundle not found:[/red] {bundle_path}")
        return None, None, None
    if not config_path.exists():
        console.print(f"[red]aidp.config not found:[/red] {config_path}")
        return None, None, None
    try:
        bundle_raw = render_tree(yaml.safe_load(bundle_path.read_text(encoding="utf-8")))
        bundle = Bundle.model_validate(bundle_raw)
    except (ValidationError, yaml.YAMLError) as exc:
        console.print(f"[red]bundle.yaml schema error:[/red] {str(exc).splitlines()[0]}")
        return None, None, None
    try:
        config_raw = render_tree(yaml.safe_load(config_path.read_text(encoding="utf-8")))
        config = AidpConfig.model_validate(config_raw)
    except (ValidationError, yaml.YAMLError) as exc:
        console.print(f"[red]aidp.config.yaml schema error:[/red] {str(exc).splitlines()[0]}")
        return None, None, None
    env = config.environments.get(env_name)
    if env is None:
        console.print(
            f"[red]env '{env_name}' not in aidp.config.yaml[/red] — "
            f"available: {sorted(config.environments.keys())}"
        )
        return None, None, None
    return bundle, config, env


def _resolve_dispatch_coords(
    env: EnvSpec,
    config: AidpConfig,
    console: Console,
) -> _DispatchCoords | None:
    """Validate the env block has everything the dispatcher needs and
    surface a precise remediation if not."""
    missing: list[str] = []
    if not env.aidp_id:
        missing.append("aidpId")
    if not env.workspace_key and not env.workspace_name:
        missing.append("workspaceKey or workspaceName")
    if not env.cluster_key and not env.cluster_name:
        missing.append("clusterKey or clusterName")
    if missing:
        console.print(
            f"[red]aidp.config.yaml missing required dispatch field(s):[/red] {', '.join(missing)}.\n"
            "Add them under [cyan]environments.<env>:[/cyan] — see "
            "[cyan]examples/aidp.config.example.yaml[/cyan]."
        )
        return None

    region = env.region or config.defaults.region
    return _DispatchCoords(
        aidp_id=env.aidp_id,  # type: ignore[arg-type]
        workspace_key=env.workspace_key or "",  # resolved below if name-only
        workspace_display_name=env.workspace_name,
        cluster_key=env.cluster_key or "",  # resolved below if name-only
        cluster_display_name=env.cluster_name or "",
        region=region,
        oci_profile=env.oci_profile or "DEFAULT",
        secret_name=(env.secret.name if env.secret else DEFAULT_SECRET_NAME),
        secret_key=(env.secret.key if env.secret else DEFAULT_SECRET_KEY),
        workspace_root=config.defaults.workspace_root,
    )


def _ensure_cluster_active(
    client: AidpRestClient,
    coords: _DispatchCoords,
    cluster_wait_s: int,
    console: Console,
) -> bool:
    """Phases 1+2: resolve workspace/cluster keys (when only names given),
    auto-start cluster if STOPPED, wait until ACTIVE."""
    console.print("[1/7] Resolving AIDP coordinates ...")

    # Workspace: if only name given, look up by name.
    workspace_key = coords.workspace_key
    if not workspace_key and coords.workspace_display_name:
        try:
            ws = client.find_workspace_by_name(coords.workspace_display_name)
        except AidpRestError as exc:
            console.print(f"[red]workspace lookup failed:[/red] {exc}")
            return False
        workspace_key = ws.key
        client.workspace_key = workspace_key  # rebind for subsequent calls
        console.print(f"      workspace [cyan]{coords.workspace_display_name}[/cyan] -> [cyan]{workspace_key}[/cyan]")

    # Cluster: by name if needed.
    cluster_key = coords.cluster_key
    cluster_name = coords.cluster_display_name
    if not cluster_key and cluster_name:
        try:
            cl = client.find_cluster_by_name(cluster_name)
        except AidpRestError as exc:
            console.print(f"[red]cluster lookup failed:[/red] {exc}")
            return False
        cluster_key = cl.key
        console.print(f"      cluster   [cyan]{cluster_name}[/cyan] -> [cyan]{cluster_key}[/cyan] (state={cl.state})")
    else:
        try:
            raw = client.get_cluster(cluster_key)
        except AidpRestError as exc:
            console.print(f"[red]get_cluster failed:[/red] {exc}")
            return False
        cluster_name = raw.get("displayName") or cluster_name or cluster_key
        console.print(f"      cluster   [cyan]{cluster_name}[/cyan] ({cluster_key}) state={raw.get('state')}")

    # Mutate the dataclass-equivalent state through the client.
    # We can't change the frozen _DispatchCoords; carry resolved keys back via attributes on client.
    coords_resolved = client.get_cluster(cluster_key)
    state = coords_resolved.get("state")

    if state == "ACTIVE":
        pass
    elif state == "STOPPED":
        console.print("[2/7] Cluster is STOPPED — starting it ...")
        try:
            client.start_cluster(cluster_key)
            client.wait_cluster_active(cluster_key, timeout_s=cluster_wait_s)
        except AidpRestError as exc:
            console.print(f"[red]cluster start failed:[/red] {exc}")
            return False
        console.print("      [green]cluster ACTIVE[/green]")
    else:
        console.print(f"[red]cluster in unrunnable state:[/red] {state}")
        return False

    # Stash the resolved keys on the client for the rest of the flow.
    client.workspace_key = workspace_key
    # Mutate coords in-place via object.__setattr__ on the frozen dataclass.
    object.__setattr__(coords, "workspace_key", workspace_key)
    object.__setattr__(coords, "cluster_key", cluster_key)
    object.__setattr__(coords, "cluster_display_name", cluster_name)
    return True


def _render_summary(marker: dict[str, Any], terminal_status: str, console: Console) -> None:
    table = Table(title=f"Orchestrator run summary  (terminal: {terminal_status})")
    table.add_column("layer")
    table.add_column("dataset", style="cyan")
    table.add_column("status")
    table.add_column("rows", justify="right")
    table.add_column("duration", justify="right")
    table.add_column("note", overflow="fold")
    for step in marker.get("steps", []):
        status = step.get("status", "?")
        style = {
            "success": "green",
            "failed": "red",
            "skipped": "yellow",
            "deferred": "yellow",
        }.get(status, "white")
        note = step.get("skip_reason") or (step.get("error_message") or "")[:80]
        row_count = step.get("row_count")
        table.add_row(
            step.get("layer", "?"),
            step.get("dataset_id", "?"),
            f"[{style}]{status}[/{style}]",
            "-" if row_count is None else str(row_count),
            f"{step.get('duration_seconds', 0):.1f}s",
            note,
        )
    console.print(table)
    succeeded = marker.get("succeeded", 0)
    failed = marker.get("failed", 0)
    skipped = marker.get("skipped", 0)
    deferred = marker.get("deferred", 0)
    total = marker.get("total_duration_seconds", 0)
    wall = marker.get("wall_seconds", 0)
    console.print(
        f"\nrun_id=[cyan]{marker.get('run_id', '?')}[/cyan]  "
        f"[green]{succeeded} ok[/green]  "
        f"[red]{failed} failed[/red]  "
        f"[yellow]{skipped} skipped, {deferred} deferred[/yellow]  "
        f"({total:.1f}s reported / {wall:.1f}s wall)"
    )


def _infer_plugin_checkout(bundle_path: Path) -> Path:
    """Find the plugin checkout — used to build the wheel. Walks up from
    where this module lives looking for the ``pyproject.toml`` that owns it.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise DispatchError(
        f"could not infer plugin checkout from {here}; pass plugin_checkout= explicitly"
    )


__all__ = ["DispatchError", "dispatch_run"]
