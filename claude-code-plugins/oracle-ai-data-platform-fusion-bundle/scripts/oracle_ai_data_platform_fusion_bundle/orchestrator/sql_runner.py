"""Content-pack execution backend — ``execute_node`` entry point.

This is the orchestrator's per-node runner. The content-pack runner
calls ``execute_node`` once per node;
``execute_node`` performs the full lifecycle (preflight → render →
plan-hash drift gate → strategy dispatch → quality tests → materialised
schema assertion → atomic state commit) and returns a typed result.

Critical ordering invariant (PLAN §11.9 / Step 11)
--------------------------------------------------

The plan-hash drift gate compares the *expected* content-pack plan-hash
(which includes ``rendered_sql_hash``) against the last successful
state row. The expected hash can only be computed AFTER the SQL has
been rendered with profile params. The flow is therefore:

1. Static schema validation (Phase 1; trusted from the loader).
2. Preflight (Step 7) — metadata + bronze DESCRIBE only, no render.
3. **Render SQL** (Step 3) — happens exactly once per execute_node call.
4. **Compute expected content-pack plan-hash** (Step 9) including the
   rendered_sql_hash.
5. **Plan-hash drift gate** (incremental only) — block resume on
   AIDPF-4040 BEFORE any Spark write.
6. Dispatch by strategy (Steps 5-6), reusing the same RenderedSql.
7. Quality tests (Step 8) — failures block cursor advance.
8. Materialised-schema assertion — Spark target schema must match
   ``node.outputSchema`` (AIDPF-4070).
9. Compute output_watermark.
10. Assemble the full state-row list (primary + every lookup) in memory.
11. ONE atomic batch state write via ``write_state_rows_hard`` (Step 10).
12. Return.

References:

* PLAN §11.9 (atomic cursor commit; plan-hash drift gate)
* PLAN §11.10 (multi-source primary/lookup)
* ADR-0017 (no LLM during seed/incremental — render is deterministic)
"""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from . import plan_hash as plan_hash_module
from . import state_phase2
from .node_preflight import preflight_node
from .quality_runner import run_quality_tests
from .sql_renderer import (
    RenderedSql,
    RunContext,
    SqlRendererError,
    compute_rendered_sql_hash,
    render_node_sql,
)
from .strategy_executors import StrategyExecutorError, execute_strategy

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ..config.paths import TablePaths
    from ..schema.medallion_pack import NodeYaml
    from ..schema.tenant_profile import TenantProfile
    from .content_pack import ResolvedPack


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_4040_PLAN_HASH_DRIFT = "AIDPF-4040"
"""Plan-hash drift on resume — rendered SQL, output schema, or profile hash
changed since the last successful run. AIDPF-4040 blocks resume."""

AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT = "AIDPF-4070"
"""Materialised target schema does not match node.outputSchema.

Detected post-execution via DESCRIBE TABLE; differs from AIDPF-4040
which catches YAML-author-induced drift pre-dispatch. Both gates fire
independently; if both conditions hold, the pre-dispatch gate fires first
and the SQL is never executed."""

AIDPF_5014_UNKNOWN_BUILTIN_DISPATCH = "AIDPF-5014"
"""Content-pack execute_node dispatched a ``type: builtin`` node whose
``implementation.callable`` is not in the builtin registry. Phase 3
contract is strict: the registry is the allowlist, missing entries fail
fast rather than auto-importing arbitrary callables."""

class ExecuteNodeError(Exception):
    """Base error class for execute_node failures."""


class UnknownBuiltinDispatchError(ExecuteNodeError):
    """Builtin node's callable id not in ``_BUILTIN_REGISTRY`` (AIDPF-5014)."""


class PlanHashDriftError(ExecuteNodeError):
    """Plan-hash drift detected (AIDPF-4040)."""


class MaterializedSchemaDriftError(ExecuteNodeError):
    """Materialised target schema mismatch (AIDPF-4070)."""


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NodeExecutionResult:
    """Result of one execute_node invocation.

    Attributes:
        status: ``'success'`` / ``'preflight_blocked'`` /
            ``'render_failed'`` / ``'resume_drift_blocked'`` /
            ``'quality_failed'`` / ``'output_schema_drift'`` /
            ``'state_commit_failed'``.
        row_count: rows scanned / written (0 for non-success paths).
        output_watermark: primary source's output watermark for this run,
            or None if the run did not advance the cursor.
        materialized_schema_hash: post-execution hash of the target's
            actual Spark schema (None on non-success paths).
        error_message: human-readable diagnostic for non-success paths.
        plan_hash: expected_plan_hash computed during the run.
    """

    status: str
    row_count: int = 0
    output_watermark: datetime | None = None
    materialized_schema_hash: str | None = None
    error_message: str = ""
    plan_hash: str = ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def execute_node(
    spark: "SparkSession",
    *,
    node: "NodeYaml",  # noqa: F821
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: RunContext,
    paths: "TablePaths",  # noqa: F821
    mode: Literal["seed", "incremental"],
    profile_hash: str,
    prior_plan_hash: str | None = None,
    target_override: str | None = None,
) -> NodeExecutionResult:
    """Execute one content-pack node end-to-end.

    Args:
        spark: live Spark session.
        node: validated NodeYaml whose SQL template is the unit of work.
        pack: assembled ResolvedPack carrying per-node provenance.
        profile: validated TenantProfile (variation-point picks + free-form values).
        ctx: render-time context (catalog/schemas/run_id/prior_watermark/mode/
            bronze_table_for_source).
        paths: TablePaths from the bundle (passed through to state-row
            assembly).
        mode: ``'seed'`` or ``'incremental'``.
        profile_hash: pre-computed profile hash (Step 2's
            ``compute_profile_hash``) — passed in to avoid recomputing
            inside the per-node loop.
        prior_plan_hash: the last successful state row's ``plan_hash``
            for this node, or ``None`` (seed mode / first run / no
            prior state). When non-None and incremental mode, drives
            the resume drift gate.
        target_override: fully-qualified target identifier override.
            When ``None``, the executor uses ``<catalog>.<silver|gold_schema>.<node.target>``
            from ``ctx``.

    Returns:
        :class:`NodeExecutionResult` describing the outcome. The caller
        (orchestrator.run) decides how to surface the result — for
        success/failure-with-state-row paths the function has already
        written the state rows itself; for hard programmer errors it
        re-raises.
    """
    # Phase 9 — dispatch on implementation type. The SQL path
    # (existing body) handles ``type: sql``; the builtin path handles
    # ``type: builtin`` (ADR-0011, e.g. dim_calendar); the bronze
    # path handles ``type: bronze_extract`` (Phase 9, ADR-0022).
    impl_type = node.implementation.type
    if impl_type == "builtin":
        return _execute_builtin_node(
            spark,
            node=node,
            pack=pack,
            profile=profile,
            ctx=ctx,
            paths=paths,
            mode=mode,
            profile_hash=profile_hash,
            prior_plan_hash=prior_plan_hash,
            target_override=target_override,
        )
    if impl_type == "bronze_extract":
        # Phase 9 — content-pack-driven bronze. Same lifecycle as the
        # builtin dispatch but the adapter returns
        # ``(target_df, bronze_output_watermark)`` because bronze cursor
        # semantics are extraction-time, not source-row-max.
        return _execute_bronze_extract_node(
            spark,
            node=node,
            pack=pack,
            profile=profile,
            ctx=ctx,
            paths=paths,
            mode=mode,
            profile_hash=profile_hash,
            prior_plan_hash=prior_plan_hash,
            target_override=target_override,
        )
    if impl_type != "sql":
        # Defensive — the loader's discriminated union already rejects
        # everything outside {sql, builtin, bronze_extract}. Reaching
        # here means a future implementation type slipped through the
        # loader gate without being wired in. Hard-raise so the bug is
        # visible.
        raise ValueError(
            f"execute_node: unsupported implementation.type={impl_type!r} "
            f"for node {node.id!r}. Expected 'sql', 'builtin', or "
            f"'bronze_extract'."
        )

    # ----- Step 1: static schema validation (Phase 1; loader did this).

    # ----- Step 2: preflight ------------------------------------------
    preflight = preflight_node(spark, node, pack, profile, ctx)
    if not preflight.ok:
        message = "; ".join(f"[{e.code}] {e.message}" for e in preflight.errors)
        _safe_write_preflight_blocked_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        )
        return NodeExecutionResult(
            status="preflight_blocked",
            error_message=message,
        )

    # ----- Step 3: render SQL (exactly once) --------------------------
    try:
        rendered = render_node_sql(node, pack, profile, ctx)
    except SqlRendererError as exc:
        message = f"render_failed: {exc}"
        _safe_write_render_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        )
        return NodeExecutionResult(status="render_failed", error_message=message)

    rendered_sql_hash = compute_rendered_sql_hash(rendered)
    output_schema_hash = plan_hash_module.compute_output_schema_hash(node)

    # ----- Step 4: compute expected content-pack plan-hash ------------
    expected_plan_hash = plan_hash_module.compute_content_pack_plan_hash(
        pack=pack,
        node=node,
        profile=profile,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
    )

    # ----- Step 5: plan-hash drift gate (incremental only) -----------
    if mode == "incremental" and prior_plan_hash and prior_plan_hash != expected_plan_hash:
        message = (
            f"{AIDPF_4040_PLAN_HASH_DRIFT}: plan-hash drift on resume — "
            f"expected={expected_plan_hash[:16]}... prior={prior_plan_hash[:16]}... "
            f"Re-run with --mode seed (or revert the YAML / SQL / profile change)."
        )
        _safe_write_resume_drift_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            expected_plan_hash=expected_plan_hash, prior_plan_hash=prior_plan_hash,
        )
        return NodeExecutionResult(
            status="resume_drift_blocked",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 6: dispatch by strategy, reusing RenderedSql ----------
    target = target_override or _build_target_identifier(node, ctx, paths)
    try:
        strategy_result = execute_strategy(
            spark, node=node, rendered=rendered, target=target, ctx=ctx, mode=mode,
        )
    except StrategyExecutorError as exc:
        message = f"strategy_failed: {exc}"
        _safe_write_strategy_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="strategy_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 7: quality tests --------------------------------------
    target_df = spark.table(target)
    quality_report = run_quality_tests(spark, node, target_df, ctx)
    if not quality_report.ok:
        message = "; ".join(f"[{f.test_type}] {f.message}" for f in quality_report.failures)
        _safe_write_quality_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="quality_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 8: materialised-schema assertion ----------------------
    try:
        materialized_schema_hash = _assert_materialized_matches_declared(
            spark, target, node
        )
    except MaterializedSchemaDriftError as exc:
        message = str(exc)
        _safe_write_schema_drift_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="output_schema_drift",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 9: compute output_watermark ---------------------------
    output_watermark = _compute_output_watermark(
        spark, node, ctx, paths, rendered, strategy_result,
    )

    # ----- Step 10: assemble state rows (primary + lookups) ----------
    state_rows = _assemble_success_state_rows(
        node=node,
        ctx=ctx,
        pack=pack,
        profile=profile,
        mode=mode,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
        plan_hash=expected_plan_hash,
        strategy_result=strategy_result,
        output_watermark=output_watermark,
    )

    # ----- Step 11: ONE atomic batch state write ----------------------
    try:
        state_phase2.write_state_rows_hard(spark, paths, state_rows)
    except state_phase2.StateCommitError as exc:
        message = f"state_commit_failed: {exc}"
        # Do NOT attempt a soft fallback — the contract is hard-commit
        # for cursor-advancing rows.
        return NodeExecutionResult(
            status="state_commit_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
            row_count=strategy_result.rows_scanned,
        )

    # ----- Step 12: return success result -----------------------------
    return NodeExecutionResult(
        status="success",
        row_count=strategy_result.rows_scanned,
        output_watermark=output_watermark,
        materialized_schema_hash=materialized_schema_hash,
        plan_hash=expected_plan_hash,
    )


# ---------------------------------------------------------------------------
# Materialised-schema assertion (Step 8 of the execute_node flow)
# ---------------------------------------------------------------------------


def _assert_materialized_matches_declared(
    spark: "SparkSession", target: str, node: "NodeYaml"  # noqa: F821
) -> str:
    """Validate the materialised target's Spark schema against node.outputSchema.

    Compares column name + type + nullable field-by-field. Mismatch
    raises :class:`MaterializedSchemaDriftError` with AIDPF-4070.
    Returns a sha256 of the canonicalised materialised schema on
    success — the caller threads it into the success state row for
    audit.
    """
    rows = spark.sql(f"DESCRIBE TABLE {target}").collect()
    materialized: list[tuple[str, str]] = []
    for r in rows:
        try:
            name = r["col_name"] if isinstance(r, dict) else r[0]
            dtype = r["data_type"] if isinstance(r, dict) else r[1]
        except (KeyError, IndexError, TypeError):
            continue
        if not name or name.startswith("#"):
            break
        materialized.append((name, str(dtype).lower()))

    declared = [
        (col.name, col.type.lower()) for col in node.output_schema.columns
    ]
    if len(materialized) != len(declared):
        raise MaterializedSchemaDriftError(
            f"{AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT}: target {target!r} has "
            f"{len(materialized)} column(s) but node declares {len(declared)}. "
            f"Declared: {declared!r}. Materialised: {materialized!r}."
        )
    for (m_name, m_type), (d_name, d_type) in zip(materialized, declared):
        if m_name != d_name:
            raise MaterializedSchemaDriftError(
                f"{AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT}: column name mismatch — "
                f"materialised={m_name!r} declared={d_name!r}."
            )
        if _normalise_spark_type(m_type) != _normalise_spark_type(d_type):
            raise MaterializedSchemaDriftError(
                f"{AIDPF_4070_MATERIALIZED_SCHEMA_DRIFT}: column {m_name!r} type "
                f"mismatch — materialised={m_type!r} declared={d_type!r}."
            )

    canonical = "\n".join(f"{n}|{t}" for n, t in materialized)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


_SPARK_TYPE_SYNONYMS = {
    "int": "integer",
    "long": "bigint",
    "double": "double",
    "string": "string",
    "boolean": "boolean",
    "timestamp": "timestamp",
    "date": "date",
}


def _normalise_spark_type(type_str: str) -> str:
    """Map common Spark type synonyms to a canonical form for comparison."""
    t = type_str.strip().lower()
    return _SPARK_TYPE_SYNONYMS.get(t, t)


# ---------------------------------------------------------------------------
# Helpers — target identifier + watermark + state-row assembly
# ---------------------------------------------------------------------------


def _build_target_identifier(
    node: "NodeYaml",  # noqa: F821
    ctx: RunContext,
    paths: "TablePaths",  # noqa: F821
) -> str:
    """Build the fully-qualified target identifier for a node.

    Phase 9 routes ALL three layers through ``TablePaths.bronze`` /
    ``.silver`` / ``.gold`` so identifier validation
    (``^[A-Za-z_][A-Za-z0-9_]*$``) fires centrally — malformed
    ``node.target`` raises ``ValueError`` BEFORE any executor logic
    runs (no BICC call, no Spark write, no state-row write attempt).

    Phase 9 follow-up: ``paths`` is now REQUIRED. The legacy ctx-only
    fallback (which composed ``f"{ctx.catalog}.{schema}.{node.target}"``
    without identifier validation) was deleted along with the v1
    dispatcher.
    """
    layer = node.layer
    if layer == "bronze":
        return paths.bronze(node.target)
    if layer == "silver":
        return paths.silver(node.target)
    if layer == "gold":
        return paths.gold(node.target)
    raise ValueError(
        f"_build_target_identifier: unsupported layer={layer!r} for "
        f"node {node.id!r}"
    )


def _compute_output_watermark(
    spark: "SparkSession",
    node: "NodeYaml",  # noqa: F821
    ctx: RunContext,
    paths: "TablePaths",  # noqa: F821
    rendered: RenderedSql,
    strategy_result,
) -> datetime | None:
    """Compute the primary source's max watermark over the rows scanned.

    For ``replace`` strategy: probe the target's max watermark column
    value. For ``merge`` with non-empty delta: probe the source SELECT.
    For ``merge`` with empty delta: preserve the prior watermark (None
    here; the caller threads the prior value when writing the row).

    Defensive against missing watermark column — returns None if the
    column doesn't exist (which means the node has no incremental
    semantics, in which case the watermark field is informational).
    """
    if strategy_result.merge_skipped_empty_delta:
        return ctx.prior_watermark.get(
            node.refresh.incremental.watermark.source
            if node.refresh.incremental and node.refresh.incremental.watermark
            else None,
        )

    inc = node.refresh.incremental
    if inc is None or inc.watermark is None:
        return None
    column = inc.watermark.column
    target = _build_target_identifier(node, ctx, paths)
    try:
        df = spark.sql(f"SELECT MAX({column}) AS wm FROM {target}")
        rows = df.collect()
        if not rows:
            return None
        wm = rows[0][0]
        if isinstance(wm, datetime):
            return wm
        if wm is None:
            return None
        # Spark may return strings for timestamp columns in some
        # configurations; defensive coercion.
        try:
            return datetime.fromisoformat(str(wm))
        except (TypeError, ValueError):
            return None
    except Exception:  # noqa: BLE001 — informational; missing column shouldn't fail the run
        return None


def _assemble_success_state_rows(
    *,
    node,
    ctx: RunContext,
    pack,
    profile,
    mode: str,
    rendered_sql_hash: str,
    output_schema_hash: str,
    profile_hash: str,
    plan_hash: str,
    strategy_result,
    output_watermark: datetime | None,
) -> list[dict[str, Any]]:
    """Assemble the full state-row list (primary + every lookup).

    The full list is built in memory BEFORE the atomic batch write —
    this is the Step 10/11 contract that makes Delta append atomicity
    a true all-or-nothing commit.

    Single-source nodes produce exactly one row (primary). Multi-source
    nodes (Phase 2 supports the §11.10 contract) produce N rows: one
    primary + one per lookup source.
    """
    now = datetime.now(timezone.utc)
    primary_source_id = _resolve_primary_source_id(node)

    common = {
        "run_id": ctx.run_id,
        "dataset_id": node.id,
        "layer": node.layer,
        "mode": mode,
        "last_run_at": now,
        "status": "success",
        "row_count": strategy_result.rows_scanned,
        "error_message": None,
        "skip_reason": None,
        "duration_seconds": None,
        "plan_hash": plan_hash,
        "plan_snapshot": None,
        # Phase 2 columns.
        "pack_id": pack.pack.id,
        "pack_version": pack.pack.version,
        "node_version": None,
        "node_implementation_type": node.implementation.type,
        "rendered_sql_hash": rendered_sql_hash,
        "output_schema_hash": output_schema_hash,
        "profile_hash": profile_hash,
        "tenant_fingerprint": profile.tenant,
        "fusion_version": None,
        "bronze_schema_fingerprint": profile.bronze_schema_fingerprint,
        "input_watermark_start": None,
        "input_watermark_end": None,
    }

    rows: list[dict[str, Any]] = []

    # Primary source row — advances last_watermark.
    primary_row = {
        **common,
        "source_id": primary_source_id,
        "source_role": "primary",
        "last_watermark": output_watermark,
        "output_watermark": output_watermark,
        "consumed_version": None,
        "delta_row_count": strategy_result.rows_scanned,
    }
    rows.append(primary_row)

    # Lookup source rows — one per declared dependency beyond the primary.
    lookup_sources = _resolve_lookup_source_ids(node, primary_source_id)
    for lookup_id in lookup_sources:
        rows.append({
            **common,
            "source_id": lookup_id,
            "source_role": "lookup",
            # Lookup rows do NOT drive watermark advancement.
            "last_watermark": None,
            "output_watermark": None,
            "consumed_version": now,
            "delta_row_count": None,
        })

    return rows


def _resolve_primary_source_id(node) -> str | None:
    """Identify the primary source for a node (PLAN §11.10)."""
    inc = node.refresh.incremental
    if inc is not None and inc.watermark is not None:
        return inc.watermark.source
    deps = getattr(node, "depends_on", None)
    if deps and deps.bronze:
        return deps.bronze[0].id
    return None


def _resolve_lookup_source_ids(node, primary_id: str | None) -> list[str]:
    """List source IDs that aren't the primary — every other declared dep."""
    deps = getattr(node, "depends_on", None)
    if not deps:
        return []
    ids: list[str] = []
    for src in list(deps.bronze) + list(deps.silver):
        if src.id != primary_id:
            ids.append(src.id)
    return ids


# ---------------------------------------------------------------------------
# Soft state-row writers for failure paths
# ---------------------------------------------------------------------------
#
# All four use a single helper. They MUST NOT raise on a write failure
# — diagnostic state rows are best-effort; a Spark write failure here
# only loses the audit trail, not the cursor-advancement semantics
# (which require the hard atomic batch in the success path).


def _safe_write_failure_row(
    spark, paths, *, node, ctx, status: str, message: str, profile, plan_hash: str = ""
) -> None:
    now = datetime.now(timezone.utc)
    row = {
        "run_id": ctx.run_id,
        "dataset_id": node.id,
        "layer": node.layer,
        "mode": ctx.mode,
        "last_watermark": ctx.prior_watermark.get(_resolve_primary_source_id(node)),
        "last_run_at": now,
        "status": status,
        "row_count": 0,
        "error_message": message,
        "skip_reason": None,
        "duration_seconds": None,
        "plan_hash": plan_hash,
        "plan_snapshot": None,
        "pack_id": None,
        "pack_version": None,
        "node_version": None,
        "node_implementation_type": node.implementation.type,
        "rendered_sql_hash": None,
        "output_schema_hash": None,
        "profile_hash": None,
        "tenant_fingerprint": profile.tenant if profile is not None else None,
        "fusion_version": None,
        "bronze_schema_fingerprint": profile.bronze_schema_fingerprint if profile is not None else None,
        "source_id": _resolve_primary_source_id(node),
        "source_role": "primary",
        "input_watermark_start": None,
        "input_watermark_end": None,
        "output_watermark": None,
        "consumed_version": None,
        "delta_row_count": None,
    }
    try:
        state_phase2.write_state_rows_hard(spark, paths, [row])
    except Exception:  # noqa: BLE001 — diagnostic write is best-effort
        return


def _safe_write_preflight_blocked_row(spark, paths, *, node, ctx, message, profile) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="preflight_blocked",
        message=message, profile=profile,
    )


def _safe_write_render_failed_row(spark, paths, *, node, ctx, message, profile) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="render_failed",
        message=message, profile=profile,
    )


def _safe_write_resume_drift_row(
    spark, paths, *, node, ctx, message, profile, expected_plan_hash, prior_plan_hash,
) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="resume_drift_blocked",
        message=message, profile=profile, plan_hash=expected_plan_hash,
    )


def _safe_write_strategy_failed_row(
    spark, paths, *, node, ctx, message, profile, plan_hash,
) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="strategy_failed",
        message=message, profile=profile, plan_hash=plan_hash,
    )


def _safe_write_quality_failed_row(
    spark, paths, *, node, ctx, message, profile, plan_hash,
) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="quality_failed",
        message=message, profile=profile, plan_hash=plan_hash,
    )


def _safe_write_schema_drift_row(
    spark, paths, *, node, ctx, message, profile, plan_hash,
) -> None:
    _safe_write_failure_row(
        spark, paths, node=node, ctx=ctx, status="output_schema_drift",
        message=message, profile=profile, plan_hash=plan_hash,
    )


# ---------------------------------------------------------------------------
# Phase 3 Step 3 — builtin dispatch
# ---------------------------------------------------------------------------


def _build_dim_calendar_adapter_entry():
    """Lazily import the dim_calendar adapter to avoid an orchestrator-load-time
    dependency cycle (``dim_calendar`` imports from ``config.paths`` which
    transitively imports parts of ``orchestrator``)."""
    from .builtins import dim_calendar_adapter
    return (dim_calendar_adapter.run, dim_calendar_adapter.VERSION)


_BUILTIN_REGISTRY: dict[str, "tuple[Any, str]"] = {
    # Keyed by NodeImpl.callable (the dotted ``<module>:<func>`` form authored
    # in node YAML). Value is (adapter_func, version_string). The adapter
    # function has uniform signature
    # ``(spark, *, node, pack, profile, ctx) -> DataFrame``; the version flows
    # into the content-pack plan-hash as the rendered_sql_hash substitute.
    #
    # Initial entry is dim_calendar per ADR-0011. New builtins MUST be
    # listed here — auto-importing arbitrary callables is the AIDPF-5014
    # surface this gate prevents.
}


def _ensure_registry_populated() -> None:
    """Populate :data:`_BUILTIN_REGISTRY` on first dispatch.

    Lazy population sidesteps an orchestrator-load-time circular import
    (sql_runner ← orchestrator ← dim_calendar) without forcing the
    adapter import at module top.
    """
    if not _BUILTIN_REGISTRY:
        _BUILTIN_REGISTRY[
            "oracle_ai_data_platform_fusion_bundle.dimensions.dim_calendar:build"
        ] = _build_dim_calendar_adapter_entry()


def _builtin_rendered_sql_hash_substitute(callable_id: str, version: str) -> str:
    """Compute the rendered_sql_hash substitute for a builtin dispatch.

    The content-pack plan-hash signature requires a ``rendered_sql_hash``
    string. For SQL nodes that's the canonical hash of the rendered
    template + bound params; for builtins it's a sha256 of
    ``<callable_id>:<version>``. Bumping the adapter's VERSION constant
    flips this hash, triggering the AIDPF-4040 drift gate just like a
    SQL-template edit does.

    Used for ``type: builtin`` and ``type: bronze_extract`` dispatch
    where the callable is plugin-internal (dim_calendar adapter,
    bronze_extract_adapter) — its source code moves with the plugin
    version, so the version constant is a sufficient drift signal.
    """
    return hashlib.sha256(f"{callable_id}:{version}".encode("utf-8")).hexdigest()


def _execute_builtin_node(
    spark: "SparkSession",
    *,
    node: "NodeYaml",  # noqa: F821
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: RunContext,
    paths: "TablePaths",  # noqa: F821
    mode: Literal["seed", "incremental"],
    profile_hash: str,
    prior_plan_hash: str | None,
    target_override: str | None,
) -> NodeExecutionResult:
    """Execute a ``type: builtin`` node via the registry.

    Lifecycle mirrors the SQL path
    (preflight → plan-hash → drift gate → execute → quality → schema
    assertion → state-row write) but skips ``render_node_sql`` (builtins
    have no SQL template) and substitutes the builtin's (callable,
    version) for the rendered_sql_hash so the §11.9 drift gate stays
    uniform.
    """
    _ensure_registry_populated()

    # ----- Step 1: static validation done by the loader. -------------

    # ----- Step 2: preflight (column probes + identity validation). --
    # For builtins with empty bronze deps (dim_calendar), preflight is a
    # no-op pass. Keeping the call uniform avoids special-casing.
    preflight = preflight_node(spark, node, pack, profile, ctx)
    if not preflight.ok:
        message = "; ".join(f"[{e.code}] {e.message}" for e in preflight.errors)
        _safe_write_preflight_blocked_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        )
        return NodeExecutionResult(status="preflight_blocked", error_message=message)

    # ----- Step 3: resolve the builtin adapter ----------------------
    callable_id = node.implementation.callable  # type: ignore[union-attr]
    entry = _BUILTIN_REGISTRY.get(callable_id)
    if entry is None:
        message = (
            f"{AIDPF_5014_UNKNOWN_BUILTIN_DISPATCH}: builtin callable "
            f"{callable_id!r} not in _BUILTIN_REGISTRY. Registered: "
            f"{sorted(_BUILTIN_REGISTRY.keys())!r}. Add an adapter under "
            f"orchestrator/builtins/ and register it before content-pack "
            f"dispatch can run."
        )
        _safe_write_render_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        )
        return NodeExecutionResult(status="render_failed", error_message=message)
    adapter_func, adapter_version = entry

    # ----- Step 4: compute plan-hash inputs --------------------------
    rendered_sql_hash = _builtin_rendered_sql_hash_substitute(callable_id, adapter_version)
    output_schema_hash = plan_hash_module.compute_output_schema_hash(node)
    expected_plan_hash = plan_hash_module.compute_content_pack_plan_hash(
        pack=pack,
        node=node,
        profile=profile,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
    )

    # ----- Step 5: plan-hash drift gate (incremental only) ----------
    if mode == "incremental" and prior_plan_hash and prior_plan_hash != expected_plan_hash:
        message = (
            f"{AIDPF_4040_PLAN_HASH_DRIFT}: plan-hash drift on resume — "
            f"expected={expected_plan_hash[:16]}... prior={prior_plan_hash[:16]}... "
            f"Re-run with --mode seed (or revert the YAML / adapter version change)."
        )
        _safe_write_resume_drift_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            expected_plan_hash=expected_plan_hash, prior_plan_hash=prior_plan_hash,
        )
        return NodeExecutionResult(
            status="resume_drift_blocked",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 6: invoke the adapter --------------------------------
    target = target_override or _build_target_identifier(node, ctx, paths)
    try:
        adapter_func(spark, node=node, pack=pack, profile=profile, ctx=ctx)
    except Exception as exc:  # noqa: BLE001 — surface any adapter failure uniformly
        message = f"builtin_failed: {exc}"
        _safe_write_strategy_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="strategy_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 7: quality tests -------------------------------------
    target_df = spark.table(target)
    quality_report = run_quality_tests(spark, node, target_df, ctx)
    if not quality_report.ok:
        message = "; ".join(f"[{f.test_type}] {f.message}" for f in quality_report.failures)
        _safe_write_quality_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="quality_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 8: materialised-schema assertion ---------------------
    try:
        materialized_schema_hash = _assert_materialized_matches_declared(
            spark, target, node
        )
    except MaterializedSchemaDriftError as exc:
        message = str(exc)
        _safe_write_schema_drift_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="output_schema_drift",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 9: assemble + write success state rows --------------
    # Builtins are parameter-driven (no bronze source → no watermark);
    # we synthesise a thin strategy_result-shaped object so the existing
    # _assemble_success_state_rows helper handles the row layout.
    class _BuiltinStrategyResult:
        rows_scanned = target_df.count() if hasattr(target_df, "count") else 0
        merge_skipped_empty_delta = False

    strategy_result = _BuiltinStrategyResult()
    state_rows = _assemble_success_state_rows(
        node=node,
        ctx=ctx,
        pack=pack,
        profile=profile,
        mode=mode,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
        plan_hash=expected_plan_hash,
        strategy_result=strategy_result,
        output_watermark=None,  # builtins are parameter-driven, no cursor.
    )

    try:
        state_phase2.write_state_rows_hard(spark, paths, state_rows)
    except state_phase2.StateCommitError as exc:
        message = f"state_commit_failed: {exc}"
        return NodeExecutionResult(
            status="state_commit_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
            row_count=strategy_result.rows_scanned,
        )

    return NodeExecutionResult(
        status="success",
        row_count=strategy_result.rows_scanned,
        output_watermark=None,
        materialized_schema_hash=materialized_schema_hash,
        plan_hash=expected_plan_hash,
    )


# ---------------------------------------------------------------------------
# Phase 9 Step 2 — bronze_extract dispatch
# ---------------------------------------------------------------------------


def _execute_bronze_extract_node(
    spark: "SparkSession",
    *,
    node: "NodeYaml",  # noqa: F821
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: RunContext,
    paths: "TablePaths",  # noqa: F821
    mode: Literal["seed", "incremental"],
    profile_hash: str,
    prior_plan_hash: str | None,
    target_override: str | None,
) -> NodeExecutionResult:
    """Execute a ``type: bronze_extract`` node via the bronze adapter.

    Lifecycle mirrors :func:`_execute_builtin_node` (preflight →
    plan-hash → drift gate → invoke → quality → schema assertion →
    state-row write); the adapter returns
    ``(target_df, bronze_output_watermark)`` instead of a bare
    DataFrame so the cursor (extraction-time, not source-row-max) can
    be passed straight to ``_assemble_success_state_rows``.
    """
    from .builtins import bronze_extract_adapter as _bronze_adapter

    # ----- Step 1: static validation done by the loader. -------------

    # ----- Step 2: preflight (identity + bundle-side validation). ----
    preflight = preflight_node(spark, node, pack, profile, ctx)
    if not preflight.ok:
        message = "; ".join(f"[{e.code}] {e.message}" for e in preflight.errors)
        _safe_write_preflight_blocked_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
        )
        return NodeExecutionResult(status="preflight_blocked", error_message=message)

    # ----- Step 4: compute plan-hash inputs --------------------------
    callable_id = f"bronze_extract:{node.implementation.datastore}"
    rendered_sql_hash = _builtin_rendered_sql_hash_substitute(
        callable_id, _bronze_adapter.VERSION
    )
    output_schema_hash = plan_hash_module.compute_output_schema_hash(node)
    expected_plan_hash = plan_hash_module.compute_content_pack_plan_hash(
        pack=pack,
        node=node,
        profile=profile,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
    )

    # ----- Step 5: plan-hash drift gate (incremental only) ----------
    if mode == "incremental" and prior_plan_hash and prior_plan_hash != expected_plan_hash:
        message = (
            f"{AIDPF_4040_PLAN_HASH_DRIFT}: plan-hash drift on resume — "
            f"expected={expected_plan_hash[:16]}... prior={prior_plan_hash[:16]}... "
            f"Re-run with --mode seed (or revert the YAML / adapter version change)."
        )
        _safe_write_resume_drift_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            expected_plan_hash=expected_plan_hash, prior_plan_hash=prior_plan_hash,
        )
        return NodeExecutionResult(
            status="resume_drift_blocked",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 6: invoke the bronze adapter --------------------------
    # Phase 9 review fix: pass `paths` so the bronze branch of
    # _build_target_identifier routes through TablePaths.bronze and the
    # post-write _assert_materialized_matches_declared describes the
    # actual bronze table (catalog.bronze.<target>), not the gold-schema
    # fallback the legacy f-string composed.
    target = target_override or _build_target_identifier(node, ctx, paths)
    try:
        target_df, bronze_output_watermark = _bronze_adapter.run(
            spark,
            node=node,
            pack=pack,
            profile=profile,
            ctx=ctx,
            paths=paths,
            mode=mode,
        )
    except Exception as exc:  # noqa: BLE001 — surface any adapter failure uniformly
        message = f"bronze_extract_failed: {exc}"
        _safe_write_strategy_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="strategy_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 7: quality tests -------------------------------------
    quality_report = run_quality_tests(spark, node, target_df, ctx)
    if not quality_report.ok:
        message = "; ".join(f"[{f.test_type}] {f.message}" for f in quality_report.failures)
        _safe_write_quality_failed_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="quality_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 8: materialised-schema assertion ---------------------
    try:
        materialized_schema_hash = _assert_materialized_matches_declared(
            spark, target, node
        )
    except MaterializedSchemaDriftError as exc:
        message = str(exc)
        _safe_write_schema_drift_row(
            spark, paths, node=node, ctx=ctx, message=message, profile=profile,
            plan_hash=expected_plan_hash,
        )
        return NodeExecutionResult(
            status="output_schema_drift",
            error_message=message,
            plan_hash=expected_plan_hash,
        )

    # ----- Step 9: state rows -----------------------------------------
    # Bronze cursor is extraction-time (not source-row-max), so we
    # use the adapter's returned value directly — NOT
    # _compute_output_watermark.
    rows_scanned = target_df.count() if hasattr(target_df, "count") else 0

    class _BronzeStrategyResult:
        merge_skipped_empty_delta = False

    _BronzeStrategyResult.rows_scanned = rows_scanned
    strategy_result = _BronzeStrategyResult()

    state_rows = _assemble_success_state_rows(
        node=node,
        ctx=ctx,
        pack=pack,
        profile=profile,
        mode=mode,
        rendered_sql_hash=rendered_sql_hash,
        output_schema_hash=output_schema_hash,
        profile_hash=profile_hash,
        plan_hash=expected_plan_hash,
        strategy_result=strategy_result,
        output_watermark=bronze_output_watermark,
    )

    try:
        state_phase2.write_state_rows_hard(spark, paths, state_rows)
    except state_phase2.StateCommitError as exc:
        message = f"state_commit_failed: {exc}"
        return NodeExecutionResult(
            status="state_commit_failed",
            error_message=message,
            plan_hash=expected_plan_hash,
            row_count=rows_scanned,
        )

    return NodeExecutionResult(
        status="success",
        row_count=rows_scanned,
        output_watermark=bronze_output_watermark,
        materialized_schema_hash=materialized_schema_hash,
        plan_hash=expected_plan_hash,
    )
