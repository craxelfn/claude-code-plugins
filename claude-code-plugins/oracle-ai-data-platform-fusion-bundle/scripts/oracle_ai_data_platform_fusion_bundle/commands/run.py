"""Implementation of ``aidp-fusion-bundle run`` and ``status``.

Post-P1.5α (Phase 5) wiring:

  * ``run --inline`` calls ``orchestrator.run(bundle_path, ...)`` directly
    (the architectural primary — Spark + checkpointer + vault + Delta
    catalog all live inside the AIDP notebook session). Catches every
    ``OrchestratorConfigError`` subclass + ``NotImplementedError`` and
    exits 2 with a single-line message (no traceback). Anything else
    propagates with full traceback — that's an orchestrator bug, not a
    user error.

  * ``run`` without ``--inline`` is the laptop-terminal REST dispatch
    path. Today it prints a "what would happen" message and exits 2;
    BACKLOG P1.5ε wires it to `dispatch/aidp_rest.py` (the empirical
    probe already validated the schema — see
    RESEARCH_aidp_rest_api_probe_results.md).

  * ``status`` reads ``fusion_bundle_state`` with one-row-per-dataset
    semantics (``ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY
    last_run_at DESC)``) and surfaces the ``skip_reason`` column
    distinctly (Should-fix-5 — was previously returning every historical
    row, ordered by time, which made the dashboard repeat each dataset N
    times for N runs).
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

logger = logging.getLogger(__name__)


def run(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    *,
    mode: str = "seed",
    datasets: str | None = None,
    inline: bool = False,
    console: Console | None = None,
) -> int:
    """Submit the bundle's pipeline to AIDP, or run inline if --inline.

    P1.5α-fix2: default mode is ``"seed"`` (was previously the retired
    ``"incremental"`` default — which would immediately exit 2 via the
    orchestrator's `NotImplementedError` guard, hostile UX).
    """
    console = console or Console()

    # One-time logging setup so mid-run WARNs from
    # `orchestrator._safe_write_state_row` (state-write soft-fails) and
    # `_resolve_password` (literal-credential WARN) surface on stderr with
    # Rich formatting alongside the run summary. The orchestrator emits via
    # stdlib `logging.getLogger(__name__).warning(...)` and takes no
    # `console` parameter (§4.7 + Option-2 design); the CLI wires the
    # RichHandler so the output is consistent.
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.WARNING,
            format="%(message)s",
            handlers=[
                RichHandler(console=console, show_time=False, show_path=False),
            ],
        )

    if not bundle_path.exists():
        console.print(f"[red]bundle not found:[/red] {bundle_path}")
        return 1

    # Parse CSV → list[str] or None. Do NOT pre-resolve against
    # bundle.datasets[] — that would limit the filter to bronze IDs
    # and silently skip silver/gold. The orchestrator's resolve_plan
    # classifies user-typed identifiers across all three registries
    # (BRONZE_EXTRACTS / SILVER_DIMS / GOLD_MARTS) and raises
    # MissingDependencyError (exit 2 via OrchestratorConfigError
    # marker) if a name doesn't exist. P1.5α-fix7.
    dataset_filter: list[str] | None = (
        [s.strip() for s in datasets.split(",") if s.strip()]
        if datasets else None
    )

    if inline:
        # Pass the PATH (not parsed dict): orchestrator.run re-reads
        # the file because `_render_env_vars` (§4.4a) must run BEFORE
        # Pydantic validation, and that step needs the raw YAML text.
        return _run_inline(bundle_path, mode, dataset_filter, console)
    return _run_via_aidp_dispatch(
        bundle_path, config_path, env_name, dataset_filter, mode, console,
    )


def _run_inline(
    bundle_path: Path,
    mode: str,
    datasets: list[str] | None,
    console: Console,
) -> int:
    """Run the orchestrator in-process.

    Catches `(OrchestratorConfigError, NotImplementedError)` and exits 2
    with a single-line message — no traceback. Any other exception
    propagates with full traceback (orchestrator bug, not user error).
    """
    from oracle_ai_data_platform_fusion_bundle import orchestrator
    from oracle_ai_data_platform_fusion_bundle.orchestrator.errors import (
        OrchestratorConfigError,
    )

    try:
        summary = orchestrator.run(
            bundle_path=bundle_path,
            mode=mode,
            datasets=datasets,
        )
    except (OrchestratorConfigError, NotImplementedError) as exc:
        # User-facing config / not-implemented errors. Exit 2 with a
        # single-line message and no traceback. The error class is
        # responsible for emitting a self-explanatory message; the
        # CLI prints `str(exc)` directly without extra framing.
        console.print(f"[red]{exc}[/red]")
        return 2
    _render_summary(console, summary)
    return 0 if summary.failed == 0 else 1


def _run_via_aidp_dispatch(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    datasets: list[str] | None,
    mode: str,
    console: Console,
) -> int:
    """Submit the bundle to AIDP via the REST job API.

    Today this is a stub — BACKLOG P1.5ε wires it to
    `dispatch/aidp_rest.py` (the empirical probe already validated
    every step: create_job + jobRuns + poll + fetchOutput; see
    RESEARCH_aidp_rest_api_probe_results.md). The exit-2 message
    points operators at the available execution surfaces.
    """
    console.print(
        f"[yellow]REST dispatch is not wired in P1.5α (tracked as BACKLOG P1.5ε).[/yellow]\n"
        f"\n"
        f"Three ways to run the orchestrator today:\n"
        f"  - In an AIDP notebook session:\n"
        f"      [cyan]aidp-fusion-bundle run --inline --mode {mode}[/cyan]\n"
        f"  - Via Claude Code MCP (BACKLOG P1.5δ — may be cancelled after P1.5ε):\n"
        f"      [cyan]/aidp-fusion-bundle run[/cyan]\n"
        f"  - From a laptop terminal via REST (BACKLOG P1.5ε — unblocked, empirically validated):\n"
        f"      OCI-signed POST to /jobs + /jobRuns; see\n"
        f"      [cyan]RESEARCH_aidp_rest_api_probe_results.md[/cyan]."
    )
    if datasets:
        console.print(f"\nWould have run: mode={mode}, datasets={datasets}")
    return 2


def _render_summary(console: Console, summary) -> None:
    """Render a RunSummary as a Rich table.

    Handles two shapes:
      - normal run: per-step table with success/failed/skipped/deferred counters.
      - empty-bundle / dry-run: shows the would-run plan + extra-plan prereqs.
    """
    # Empty-bundle / dry-run path — RunSummary.empty(...) shape.
    if not summary.steps:
        if summary.plan is None and summary.prereqs is None:
            console.print(
                f"[yellow]Empty plan for project [cyan]{summary.bundle_project}[/cyan]"
                f" (mode={summary.mode}) — nothing to do.[/yellow]"
            )
            return
        console.print(
            f"[bold]Dry-run plan[/bold] for project [cyan]{summary.bundle_project}[/cyan]"
            f" (mode={summary.mode}):"
        )
        if summary.plan:
            plan_table = Table(title="Would dispatch", show_lines=False)
            plan_table.add_column("dataset_id", style="cyan")
            plan_table.add_column("layer")
            for spec in summary.plan:
                layer = getattr(spec, "layer", None)
                if layer is None:
                    # BronzeExtractSpec / SilverDimSpec / GoldMartSpec — derive
                    # via the registry helper.
                    from oracle_ai_data_platform_fusion_bundle.orchestrator.registry import (
                        _layer_for_spec,
                    )
                    layer = _layer_for_spec(spec)
                plan_table.add_row(spec.dataset_id, layer)
            console.print(plan_table)
        if summary.prereqs:
            prereqs_table = Table(title="Extra-plan prerequisites (must exist on disk)")
            prereqs_table.add_column("dataset_id", style="cyan")
            prereqs_table.add_column("layer")
            prereqs_table.add_column("consumer")
            prereqs_table.add_column("table path", overflow="fold")
            for dep in summary.prereqs:
                prereqs_table.add_row(
                    dep.dataset_id, dep.layer, dep.consumer, dep.table_path,
                )
            console.print(prereqs_table)
        return

    # Normal run — per-step table.
    table = Table(
        title=f"Run summary — {summary.bundle_project} ({summary.mode})",
        show_lines=False,
    )
    for col in ("dataset_id", "layer", "status", "row_count", "duration_s"):
        table.add_column(col)
    for step in summary.steps:
        status_color = {
            "success": "green",
            "failed": "red",
            "skipped": "yellow",
            "deferred": "dim",
        }.get(step.status, "white")
        status_display = step.status.upper()
        if step.status == "skipped" and step.skip_reason:
            status_display = f"{status_display} ({step.skip_reason})"
        table.add_row(
            step.dataset_id,
            step.layer,
            f"[{status_color}]{status_display}[/{status_color}]",
            str(step.row_count) if step.row_count is not None else "-",
            f"{step.duration_seconds:.2f}",
        )
    console.print(table)

    # Summary counters
    console.print(
        f"\nrun_id=[dim]{summary.run_id}[/dim] · "
        f"[green]{summary.succeeded} success[/green] · "
        f"[red]{summary.failed} failed[/red] · "
        f"[yellow]{summary.skipped} skipped[/yellow] · "
        f"[dim]{summary.deferred} deferred[/dim] · "
        f"total {summary.total_duration_seconds:.2f}s"
    )


def status(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    *,
    console: Console | None = None,
) -> int:
    """Show last-run summary per dataset (reads ``fusion_bundle_state``).

    Should-fix-5 (2026-05-17): returns ONE row per dataset_id (the latest),
    not every historical row. Includes `skip_reason` so cascade-vs-abort
    is visible to the operator without grepping `error_message`.
    """
    console = console or Console()
    if not bundle_path.exists():
        console.print(f"[red]bundle not found:[/red] {bundle_path}")
        return 1
    bundle = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
    paths = TablePaths.from_bundle(bundle)
    state_table = paths.bronze("fusion_bundle_state")

    # Latest-per-dataset query via row_number window. Selects skip_reason
    # so the renderer can show cascade vs aborted on `status='skipped'` rows.
    latest_query = f"""
        WITH ranked AS (
          SELECT
            dataset_id, layer, mode, last_watermark, last_run_at, status,
            row_count, error_message, skip_reason, duration_seconds,
            ROW_NUMBER() OVER (
              PARTITION BY dataset_id
              ORDER BY last_run_at DESC
            ) AS rn
          FROM {state_table}
        )
        SELECT
          dataset_id, layer, mode, last_watermark, last_run_at, status,
          row_count, error_message, skip_reason, duration_seconds
        FROM ranked
        WHERE rn = 1
        ORDER BY layer, dataset_id
    """

    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    except ImportError:
        console.print(
            f"[yellow]pyspark not available locally; cannot read {state_table}[/yellow]"
        )
        console.print(
            "Run this query inside an AIDP notebook session:\n"
            f"  [cyan]{latest_query.strip()}[/cyan]"
        )
        return 0

    spark = SparkSession.builder.appName("aidp-fusion-bundle-status").getOrCreate()
    try:
        df = spark.sql(latest_query)
        rows = df.collect()
    except Exception as exc:
        console.print(f"[red]could not read {state_table}:[/red] {exc}")
        return 1

    if not rows:
        console.print(
            f"[yellow]{state_table} is empty — no runs recorded yet[/yellow]"
        )
        return 0

    table = Table(title=f"{state_table} (latest per dataset)")
    for col in (
        "dataset_id", "layer", "mode", "last_watermark", "last_run_at",
        "status", "skip_reason", "row_count",
    ):
        table.add_column(col)
    for r in rows:
        status_val = str(r["status"])
        if status_val == "skipped" and r["skip_reason"]:
            status_val = f"{status_val} ({r['skip_reason']})"
        table.add_row(
            str(r["dataset_id"]),
            str(r["layer"]),
            str(r["mode"]),
            str(r["last_watermark"]) if r["last_watermark"] else "-",
            str(r["last_run_at"]),
            status_val,
            str(r["skip_reason"]) if r["skip_reason"] else "-",
            str(r["row_count"]) if r["row_count"] is not None else "-",
        )
    console.print(table)
    return 0


__all__ = ["run", "status"]
