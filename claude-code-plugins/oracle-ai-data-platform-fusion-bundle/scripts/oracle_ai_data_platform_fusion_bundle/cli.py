"""CLI entry point for `aidp-fusion-bundle`.

Subcommand groups:
    init / validate / bootstrap / catalog / run / status         (orchestration)
    dashboard install / validate / uninstall / mcp-config        (OAC integration)

Each command body lives in its own module under this package — `cli.py`
only wires click together so `--help` is the single source of truth for
the user-facing surface.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click
from rich.console import Console

from . import __version__

console = Console()


# ---------------------------------------------------------------------------
# Top-level group
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(__version__, prog_name="aidp-fusion-bundle")
@click.option("--bundle", "bundle_path", type=click.Path(path_type=Path), default=Path("bundle.yaml"),
              help="Path to bundle.yaml (default: ./bundle.yaml).")
@click.option("--config", "config_path", type=click.Path(path_type=Path), default=Path("aidp.config.yaml"),
              help="Path to aidp.config.yaml (default: ./aidp.config.yaml).")
@click.option("--env", "env_name", default="dev", help="Environment name from aidp.config.yaml (default: dev).")
@click.pass_context
def main(ctx: click.Context, bundle_path: Path, config_path: Path, env_name: str) -> None:
    """Productized Fusion -> AIDP pipeline (BICC + Delta + OAC)."""
    ctx.ensure_object(dict)
    ctx.obj["bundle_path"] = bundle_path
    ctx.obj["config_path"] = config_path
    ctx.obj["env_name"] = env_name


# ---------------------------------------------------------------------------
# Orchestration commands
# ---------------------------------------------------------------------------


@main.command()
@click.option("--template", type=click.Choice(["minimal", "full-finance"]), default="minimal",
              help="Which example to scaffold (default: minimal).")
@click.option("--force", is_flag=True, help="Overwrite existing bundle.yaml / aidp.config.yaml.")
def init(template: str, force: bool) -> None:
    """Scaffold a bundle.yaml + aidp.config.yaml in the current directory."""
    from .commands.init import init as init_impl
    sys.exit(init_impl(template=template, force=force, console=console))


@main.command()
@click.pass_context
def validate(ctx: click.Context) -> None:
    """Validate bundle.yaml schema + ref-integrity (no network calls)."""
    from .commands.validate import validate as validate_impl
    sys.exit(validate_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        console=console,
    ))


@main.command()
@click.option("--check-iam", is_flag=True, help="Also probe OCI IAM policies (requires AIDP RP credentials).")
@click.option(
    "--refresh", is_flag=True,
    help="Re-walk every variation point against the live bronze; resolves drift per §9.5.5 Tier-1.",
)
@click.option(
    "--operator", "operator", type=str, default=None,
    help="Explicit operator identity for the SOX-floor audit trail (overrides $AIDP_OPERATOR / $USER).",
)
@click.option(
    "--non-interactive", is_flag=True,
    help="Sandbox/CI mode: multi-match auto-picks the first candidate; refuses --refresh changes to pinned values.",
)
@click.option(
    "--resolutions", "resolutions_path", type=click.Path(path_type=Path, exists=True),
    default=None,
    help="JSON file scripting multi-match resolutions (feature #3 / CI use).",
)
@click.option(
    "--skip-preonboarding-probes", is_flag=True,
    help=(
        "Skip phase-1 BICC / AIDP probes; useful for --refresh after initial "
        "onboarding succeeded. INCOMPATIBLE with --dispatch-mode=cluster "
        "(the aidp-rest probe is load-bearing in cluster mode — see "
        "Phase 4.1 / AIDPF-2047 reason=conflicting_flags)."
    ),
)
@click.option(
    "--dispatch-mode",
    "dispatch_mode",
    type=click.Choice(["cluster", "local"]),
    default="cluster",
    show_default=True,
    help=(
        "Where the variation-phase bronze probe runs. 'cluster' "
        "(default, Phase 4.1) dispatches a notebook to the AIDP cluster "
        "where 3-part-namespace DESCRIBE works natively. 'local' uses "
        "the laptop's in-process Spark session — backward-compat for "
        "unit tests and laptop-POC bundles."
    ),
)
@click.option(
    "--cluster-key", "cluster_key", type=str, default=None,
    help=(
        "Cluster UUID for cluster-mode dispatch; overrides "
        "EnvSpec.clusterKey. Env var: AIDP_FUSION_CLUSTER_KEY."
    ),
)
@click.option(
    "--cluster-name", "cluster_name", type=str, default=None,
    help=(
        "Cluster display name for cluster-mode dispatch; overrides "
        "EnvSpec.clusterName. Env var: AIDP_FUSION_CLUSTER_NAME."
    ),
)
@click.option(
    "--workspace-dir", "workspace_dir", type=str, default=None,
    help=(
        "Server-side notebook upload root for cluster-mode dispatch; "
        "overrides Defaults.workspaceDir. Env var: "
        "AIDP_FUSION_WORKSPACE_DIR. When unset (and not in EnvSpec / "
        "Defaults), derives '/Workspace/{workspace_root}/fusion-bundle-bootstrap'."
    ),
)
@click.pass_context
def bootstrap(
    ctx: click.Context,
    check_iam: bool,
    refresh: bool,
    operator: str | None,
    non_interactive: bool,
    resolutions_path: Path | None,
    skip_preonboarding_probes: bool,
    dispatch_mode: str,
    cluster_key: str | None,
    cluster_name: str | None,
    workspace_dir: str | None,
) -> None:
    """Probe all prerequisites + run the variation-resolution phase when content-pack-enabled."""
    from .commands.bootstrap import bootstrap as bootstrap_impl
    sys.exit(bootstrap_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        check_iam=check_iam,
        console=console,
        refresh=refresh,
        operator=operator,
        non_interactive=non_interactive,
        resolutions_path=resolutions_path,
        skip_preonboarding_probes=skip_preonboarding_probes,
        dispatch_mode=dispatch_mode,
        cluster_key_override=cluster_key,
        cluster_name_override=cluster_name,
        workspace_dir_override=workspace_dir,
    ))


@main.group()
def catalog() -> None:
    """Inspect and probe the curated PVO catalog."""


@catalog.command("list")
def catalog_list() -> None:
    """Show the bundle's curated PVO catalog."""
    from .commands.catalog import list_catalog
    sys.exit(list_catalog(console=console))


@catalog.command("probe")
@click.option("--pod", required=True, help="Fusion pod URL (e.g. https://<host>.fa.<region>.oraclecloud.com).")
@click.option("--user", "username", default=None, help="HTTP Basic username (else $FUSION_BICC_USER).")
@click.option("--password", default=None, help="HTTP Basic password (else $FUSION_BICC_PASSWORD).")
def catalog_probe(pod: str, username: str | None, password: str | None) -> None:
    """Probe the Fusion BICC console for live PVO names; reconcile against the bundle catalog."""
    from .commands.catalog import probe_catalog
    sys.exit(probe_catalog(pod=pod, username=username, password=password, console=console))


@main.command()
@click.option(
    "--mode", type=click.Choice(["seed", "incremental"]), default="seed",
    help="seed = rebuild from bronze every run; incremental = delta-merge "
         "(P1.5β, not implemented today). The retired alias 'full' is now 'seed'."
)
@click.option("--datasets", default=None, help="Comma-separated dataset/dim/mart names to filter (default: all in bundle.yaml).")
@click.option(
    "--layers", default=None,
    help="Comma-separated layer names to filter (bronze, silver, gold). "
         "Mutually compatible with --datasets — both apply. "
         "P1.5α-fix13: previously the orchestrator accepted layers= but the "
         "CLI didn't surface it, so --inline --layers gold errored at Click parse.",
)
@click.option("--inline", is_flag=True,
              help="Run the orchestrator in-process (architectural primary — needs Spark + checkpointer + vault from an AIDP notebook session).")
@click.option(
    "--resume", "resume_run_id", default=None,
    help="Resume an interrupted run by its run_id. Skips datasets whose latest "
         "terminal status under this run_id is 'success' or 'resumed_skipped'; "
         "re-attempts the rest under the ORIGINAL run_id (preserves the "
         "medallion _run_id audit invariant). Scope is reconstructed from the "
         "stored plan_snapshot when --datasets/--layers are omitted. Drift "
         "(plan shape, effective schemas, fusion pod/storage/user, AIDP target "
         "paths, plugin version) raises ResumeBundleMismatchError pre-dispatch. "
         "Requires --inline in P1.5ε; REST-dispatch resume is BACKLOG P1.5ε-fix5.",
)
@click.option(
    "--dry-run", "dry_run", is_flag=True, default=False,
    help="Resolve the plan + run preflight, then exit 0 without dispatching. "
         "On --inline: returns the orchestrator's dry-run summary. On REST "
         "dispatch: runs Phase A + B preflight (bundle, config, OCI, AIDP "
         "plane, cluster state) and returns an empty RunSummary. No wheel "
         "build, no notebook upload, no job submission.",
)
@click.option(
    "--poll-timeout", "poll_timeout_s",
    type=click.IntRange(60, 14400),
    default=3600,
    show_default=True,
    help="Seconds to wait for the dispatched cluster job to reach a terminal "
         "status before raising DISPATCH_TIMEOUT. Default 3600 (1h) covers "
         "cold-cache BICC extracts on slow tenants — the 1800s (30m) default "
         "from P1.5ε was insufficient per TC29 evidence on saasfademo1. Bump "
         "to 14400 (4h) for first-time seed runs against especially slow "
         "Fusion pods. Below 60s rejected at parse — anything that short is "
         "operator error. Only meaningful for REST dispatch (no --inline).",
)
@click.option(
    "--execution-backend", "execution_backend",
    type=click.Choice(["legacy-python", "content-pack"]),
    default="legacy-python",
    show_default=True,
    help="Execution backend (Phase 2): `legacy-python` runs the v1 "
         "hardcoded dim_*.py / gold_*.py modules (unchanged from v0.3); "
         "`content-pack` runs the content-pack SQL runner against the "
         "pack declared in bundle.yaml's `contentPack:` block. Phase 4's "
         "dual-runner parity gate decides when (or if) the default flips.",
)
@click.option(
    "--force-fingerprint-skip", "force_fingerprint_skip",
    is_flag=True,
    default=False,
    hidden=True,
    help="Phase 3c — dev/sandbox: bypass the bronze-schema fingerprint "
         "drift gate. Records an audit warn row in fusion_bundle_state "
         "with mode='fingerprint_skip'. Production runs MUST NOT use "
         "this; SOX-audit environments should policy-disable.",
)
@click.pass_context
def run(ctx: click.Context, mode: str, datasets: str | None, layers: str | None,
        inline: bool, resume_run_id: str | None, dry_run: bool,
        poll_timeout_s: int, execution_backend: str,
        force_fingerprint_skip: bool) -> None:
    """Invoke the orchestrator: extract -> bronze -> silver -> gold."""
    from .commands.run import run as run_impl
    sys.exit(run_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        mode=mode,
        datasets=datasets,
        layers=layers,
        inline=inline,
        resume_run_id=resume_run_id,
        dry_run=dry_run,
        poll_timeout_s=poll_timeout_s,
        execution_backend=execution_backend,
        force_fingerprint_skip=force_fingerprint_skip,
        console=console,
    ))


@main.command("migrate-bundle")
@click.option("--from", "from_version", required=True, help="Source schema version (e.g. 0.1.0).")
@click.option("--to", "to_version", required=True, help="Target schema version (e.g. 0.2.0).")
@click.pass_context
def migrate_bundle(ctx: click.Context, from_version: str, to_version: str) -> None:
    """Migrate bundle.yaml from one schema version to another (Option L, §4.4d).

    Scaffolded in P1.5α — today only v0.2.0 exists, so any non-no-op
    invocation exits 2 with a "no migration path" message. The verb is
    here so when v0.3 ships with a breaking schema change, callers
    don't have to update their scripts.
    """
    from .commands.migrate_bundle import migrate_bundle as migrate_impl
    sys.exit(migrate_impl(
        bundle_path=ctx.obj["bundle_path"],
        from_version=from_version,
        to_version=to_version,
        console=console,
    ))


@main.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show last-run summary per dataset (reads fusion_bundle_state Delta table)."""
    from .commands.run import status as status_impl
    sys.exit(status_impl(
        bundle_path=ctx.obj["bundle_path"],
        config_path=ctx.obj["config_path"],
        env_name=ctx.obj["env_name"],
        console=console,
    ))


# ---------------------------------------------------------------------------
# Content pack commands (v2 — schema validation + introspection)
# ---------------------------------------------------------------------------


@main.group("content-pack")
def content_pack() -> None:
    """Inspect and validate content packs (v2 schema layer)."""


@content_pack.command("list")
@click.option("--json", "json_output", is_flag=True, help="Emit JSON for tooling.")
def content_pack_list(json_output: bool) -> None:
    """List installed content packs."""
    from .commands.content_pack import list_packs

    sys.exit(list_packs(json_output=json_output, console=console))


@content_pack.command("info")
@click.argument("name")
@click.option("--json", "json_output", is_flag=True, help="Emit JSON for tooling.")
def content_pack_info(name: str, json_output: bool) -> None:
    """Show detailed info about an installed pack (or a pack by path)."""
    from .commands.content_pack import info_pack

    sys.exit(info_pack(name, json_output=json_output, console=console))


@content_pack.command("validate")
@click.argument("name")
@click.option("--json", "json_output", is_flag=True, help="Emit JSON for tooling.")
def content_pack_validate(name: str, json_output: bool) -> None:
    """Validate a content pack against the schema + content validators."""
    from .commands.content_pack import validate_pack_cli

    sys.exit(validate_pack_cli(name, json_output=json_output, console=console))


# ---------------------------------------------------------------------------
# Dashboard commands (OAC integration)
# ---------------------------------------------------------------------------


@main.group()
def dashboard() -> None:
    """OAC dashboard install/validate via OAC REST API. End-user chat uses OAC MCP."""


@dashboard.command("install")
@click.option("--target", type=click.Choice(["oac"]), default="oac",
              help="Dashboard target system (only OAC is wired today).")
@click.option("--oac-url", required=True, help="OAC instance URL (e.g. https://oac.example.com).")
@click.option("--connection-name", default="aidp_fusion_jdbc",
              help="Name of the OAC connection to create (default: aidp_fusion_jdbc).")
@click.option("--region", default="us-ashburn-1", help="OCI region key.")
@click.option("--user-ocid", required=True, help="OCID of the user that owns the registered API key.")
@click.option("--tenancy-ocid", required=True, help="OCID of the tenancy.")
@click.option("--fingerprint", required=True, help="Public-key fingerprint registered on the user.")
@click.option("--idl-ocid", required=True, help="AIDP DataLake OCID.")
@click.option("--cluster-key", required=True, help="AIDP cluster key (UUID-like).")
@click.option("--catalog", default="default", help="Default JDBC catalog (default: default).")
@click.option("--private-key-pem", required=True,
              type=click.Path(exists=True, dir_okay=False, path_type=Path),
              help="Path to the private API key PEM file.")
@click.option("--bar-bucket", default=None,
              help="OCI Object Storage bucket containing the bundle's .bar snapshot. "
                   "Customer-uploaded; the bundle ships only the connection + IAM glue. "
                   "Omit to skip workbook restore (connection-only install).")
@click.option("--bar-uri", default=None,
              help="Object name (relative path) of the .bar in --bar-bucket.")
@click.option("--bar-password", default=None,
              help="BAR password (if the .bar was created with password protection).")
@click.option("--snapshot-name", default="aidp-fusion-bundle",
              help="Display name for the registered snapshot (default: aidp-fusion-bundle).")
@click.option("--idcs-url", default=None,
              help="IDCS stripe URL (https://idcs-<stripe>.identity.oraclecloud.com). "
                   "Required unless --print-only.")
@click.option("--client-id", default=None,
              help="IDCS confidential-app client_id (must have authorization_code + "
                   "refresh_token grants enabled). Required unless --print-only.")
@click.option("--client-secret", default=None,
              help="IDCS confidential-app client_secret, or ${vault:OCID}. "
                   "Required unless --print-only.")
@click.option("--oauth-scope", default=None,
              help="Override the auto-derived scope (default: <audience>urn:opc:resource:consumer::all "
                   "offline_access). Only set if your IDCS admin published a custom scope.")
@click.option("--auth-flow", type=click.Choice(["auth_code", "device"]), default="auth_code",
              show_default=True,
              help="OAuth flow: auth_code opens browser (laptop), device for headless boxes.")
@click.option("--prompt-login", is_flag=True,
              help="Force IDCS to reprompt for credentials (don't reuse cached SSO session).")
@click.option("--print-only", is_flag=True,
              help="Skip OAC REST calls; write the connection JSON for manual UI upload.")
@click.option("--skip-workbooks", is_flag=True,
              help="Create the connection but don't restore the snapshot (workbooks).")
@click.option("--overwrite-connection", is_flag=True,
              help="Delete + recreate the connection if it already exists (default: skip).")
def dashboard_install(
    target: str,
    oac_url: str,
    connection_name: str,
    region: str,
    user_ocid: str,
    tenancy_ocid: str,
    fingerprint: str,
    idl_ocid: str,
    cluster_key: str,
    catalog: str,
    private_key_pem: Path,
    bar_bucket: str | None,
    bar_uri: str | None,
    bar_password: str | None,
    snapshot_name: str,
    idcs_url: str | None,
    client_id: str | None,
    client_secret: str | None,
    oauth_scope: str | None,
    auth_flow: str,
    prompt_login: bool,
    print_only: bool,
    skip_workbooks: bool,
    overwrite_connection: bool,
) -> None:
    """Register AIDP JDBC connection in OAC + restore the workbook snapshot via REST.

    Architecture (TC10h-2 refactor, 2026-05-01) — Oracle-documented endpoints only:
      1. POST /catalog/connections                       (creates AIDP connection)
      2. POST /snapshots                                 (registers customer-uploaded .bar)
      3. POST /system/actions/restoreSnapshot            (async restore)
      4. GET  /workRequests/{id}                         (polls until SUCCEEDED)

    Two modes:
      * Default: full REST install. First run opens browser for one-time SSO consent;
        refresh token persists for silent reuse. The signed-in user must hold the
        BI Service Administrator role on OAC.
      * --print-only: writes the 6-key JSON for manual UI upload (no IDCS app needed).
    """
    from .oac.install import InstallParams, install
    from .oac.rest import derive_oac_scope, discover_oac_audience
    from .utils import vault

    resolved_secret = vault.resolve(client_secret) if client_secret else None
    if oauth_scope:
        resolved_scope = oauth_scope
    else:
        try:
            audience = discover_oac_audience(oac_url)
            resolved_scope = derive_oac_scope(oac_url, audience=audience)
        except Exception as exc:
            console.print(f"[yellow]audience discovery failed ({exc}); falling back to oac_url[/yellow]")
            resolved_scope = derive_oac_scope(oac_url)

    params = InstallParams(
        oac_url=oac_url,
        connection_name=connection_name,
        region=region,
        user_ocid=user_ocid,
        tenancy_ocid=tenancy_ocid,
        fingerprint=fingerprint,
        idl_ocid=idl_ocid,
        cluster_key=cluster_key,
        catalog=catalog,
        idcs_url=idcs_url,
        client_id=client_id,
        client_secret=resolved_secret,
        oauth_scope=resolved_scope,
        auth_flow=auth_flow,
        prompt_login=prompt_login,
        private_key_pem_path=private_key_pem,
        bar_bucket=bar_bucket,
        bar_uri=bar_uri,
        bar_password=bar_password,
        snapshot_name=snapshot_name,
        print_only=print_only,
        skip_workbooks=skip_workbooks,
        overwrite_connection=overwrite_connection,
    )
    try:
        result = install(params, console=console)
    except Exception as exc:
        console.print(f"[red]install failed:[/red] {exc}")
        sys.exit(1)

    # Summary
    parts: list[str] = []
    if result.connection_id:
        parts.append(f"connection={connection_name} (id={result.connection_id})")
    if result.snapshot_id:
        parts.append(f"snapshot={result.snapshot_id} (status={result.work_request_status})")
    if result.json_template_path:
        parts.append(f"json={result.json_template_path}")
    if parts:
        console.print(f"\n[bold green]Done.[/bold green] " + " | ".join(parts))


@dashboard.command("validate")
@click.option("--target", type=click.Choice(["oac"]), default="oac")
@click.option("--oac-url", required=True)
@click.option("--connection-name", default="aidp_fusion_jdbc")
@click.option("--idcs-url", required=True)
@click.option("--client-id", required=True)
@click.option("--client-secret", required=True,
              help="IDCS confidential-app client_secret, or ${vault:OCID}.")
@click.option("--oauth-scope", default=None,
              help="Override auto-derived scope.")
@click.option("--snapshot-name", default=None,
              help="Snapshot display name to verify is registered (default: probe none).")
def dashboard_validate(
    target: str,
    oac_url: str,
    connection_name: str,
    idcs_url: str,
    client_id: str,
    client_secret: str,
    oauth_scope: str | None,
    snapshot_name: str | None,
) -> None:
    """Probe OAC: confirm connection (and optionally a snapshot) is present (read-only)."""
    from .oac.validate import ValidateParams, validate
    from .utils import vault

    params = ValidateParams(
        oac_url=oac_url,
        connection_name=connection_name,
        snapshot_name=snapshot_name,
        idcs_url=idcs_url,
        client_id=client_id,
        client_secret=vault.resolve(client_secret),
        oauth_scope=oauth_scope or "",
    )
    result = validate(params, console=console)
    sys.exit(0 if result.all_ok else 1)


@dashboard.command("uninstall")
@click.option("--target", type=click.Choice(["oac"]), default="oac")
@click.option("--oac-url", required=True)
@click.option("--connection-name", default="aidp_fusion_jdbc")
@click.option("--idcs-url", required=True)
@click.option("--client-id", required=True)
@click.option("--client-secret", required=True)
@click.option("--oauth-scope", default=None, help="Override auto-derived scope.")
@click.option("--snapshot-id", default=None,
              help="Snapshot ID to deregister (omit to skip).")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
def dashboard_uninstall(
    target: str,
    oac_url: str,
    connection_name: str,
    idcs_url: str,
    client_id: str,
    client_secret: str,
    oauth_scope: str | None,
    snapshot_id: str | None,
    yes: bool,
) -> None:
    """Remove the bundle's connection (and optionally deregister its snapshot).

    Note: workbook restored content cannot be selectively deleted via the public
    REST API — to fully roll back, restore an earlier snapshot of the OAC instance.
    Or use Console -> Catalog to delete the /shared/AIDP_Fusion_Bundle/ folder.
    """
    from .oac.uninstall import UninstallParams, uninstall
    from .utils import vault

    if not yes:
        click.confirm(
            f"Remove connection '{connection_name}'"
            + (f" + deregister snapshot {snapshot_id}" if snapshot_id else "")
            + f" from {oac_url}?",
            abort=True,
        )
    params = UninstallParams(
        oac_url=oac_url,
        connection_name=connection_name,
        snapshot_id=snapshot_id,
        idcs_url=idcs_url,
        client_id=client_id,
        client_secret=vault.resolve(client_secret),
        oauth_scope=oauth_scope or "",
    )
    result = uninstall(params, console=console)
    console.print(
        f"\n[bold]Removed:[/bold] "
        f"connection={result.connection_deleted}, "
        f"snapshot={result.snapshot_deleted}"
    )


@dashboard.command("mcp-config")
@click.option("--oac-url", required=True, help="OAC instance URL.")
@click.option("--oac-mcp-connect-js", required=True, type=click.Path(exists=True, path_type=Path),
              help="Local path to oac-mcp-connect.js (extract from oac-mcp-connect.zip — get from OAC Profile -> MCP Connect tab).")
def dashboard_mcp_config(oac_url: str, oac_mcp_connect_js: Path) -> None:
    """Print the JSON snippet to add to claude_desktop_config.json (or Claude Code / Cline / Copilot)."""
    import json
    snippet = {
        "mcpServers": {
            "oac-mcp-server": {
                "command": "node",
                "args": [str(oac_mcp_connect_js.resolve())],
                "env": {
                    "OAC_INSTANCE_URL": oac_url
                }
            }
        }
    }
    console.print("[bold]Paste into your AI client's MCP config:[/bold]\n")
    console.print(json.dumps(snippet, indent=2))
    console.print(
        "\n[dim]Note: this is a starter template; the canonical JSON is the one OAC's "
        "Profile -> MCP Connect tab generates. See:[/dim]\n"
        "  https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/add-oracle-analytics-cloud-mcp-server-your-ai-client-preview.html"
    )


if __name__ == "__main__":
    main()
