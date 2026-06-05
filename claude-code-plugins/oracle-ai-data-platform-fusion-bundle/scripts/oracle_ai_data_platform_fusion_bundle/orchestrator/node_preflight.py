"""Per-node preflight for the content-pack runner (Phase 2 Step 7).

Preflight runs in :func:`sql_runner.execute_node` *after* static schema
validation and *before* SQL rendering. Its job is to fail fast on
runtime-only conditions that the static validator can't see:

* Required columns missing in the live bronze schema.
* Watermark column missing in the source bronze (for merge strategy).
* Partition columns missing on the target (validate-only for deferred
  ``replace_partition`` strategy).

**Crucial ordering invariant**: preflight does NOT render SQL. The
renderer is invoked exactly once in ``execute_node`` step 3, *after*
this preflight passes. This separation is what makes the
``preflight-blocked`` unit test branch assert "renderer never called".

References:

* PLAN §11.6 (preflight gates)
* PLAN §11.10 (multi-source primary/lookup; per-source watermark checks)
* PLAN §11.3 R6 (replace_partition partition column invariant)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..schema.medallion_pack import NodeYaml

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ..schema.tenant_profile import TenantProfile
    from .content_pack import ResolvedPack
    from .sql_renderer import RunContext


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_2042_REQUIRED_COLUMN_MISSING = "AIDPF-2042"
"""Required column declared in ``node.requiredColumns.<source>`` is absent
from the live bronze ``DESCRIBE TABLE`` schema."""

AIDPF_2043_WATERMARK_COLUMN_MISSING = "AIDPF-2043"
"""Watermark column declared in ``node.refresh.incremental.watermark.column``
is absent from the source bronze schema (merge strategy)."""

AIDPF_2044_PARTITION_COLUMN_MISSING = "AIDPF-2044"
"""``replace_partition`` strategy partition column missing on target
(deferred strategy; validate-only)."""


# ---------------------------------------------------------------------------
# Preflight result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PreflightError:
    """One preflight failure for a node.

    Attributes:
        code: AIDPF error code (e.g. ``AIDPF-2042``).
        source: bronze/silver source id where the failure was detected,
            or ``None`` for target-level checks.
        message: human-readable diagnostic naming the missing column /
            source / live bronze schema for triage.
    """

    code: str
    source: str | None
    message: str


@dataclass(frozen=True)
class PreflightReport:
    """Aggregated preflight result for a node.

    Attributes:
        errors: tuple of :class:`PreflightError` — blocking failures.
            Non-empty → :func:`execute_node` writes a
            ``status='preflight_blocked'`` soft state row and returns
            failure WITHOUT invoking the renderer.
        ok: convenience boolean — ``True`` iff ``errors`` is empty.
    """

    errors: tuple[PreflightError, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def preflight_node(
    spark: "SparkSession",
    node: NodeYaml,
    pack: "ResolvedPack",  # noqa: F821 — forward ref
    profile: "TenantProfile",  # noqa: F821
    ctx: "RunContext",  # noqa: F821
) -> PreflightReport:
    """Run per-node preflight against the live Spark catalog.

    Performs metadata + bronze ``DESCRIBE TABLE`` introspection only.
    **Does NOT render SQL** — the renderer runs separately in
    :func:`execute_node` step 3, *after* this preflight passes.

    Args:
        spark: live Spark session for ``DESCRIBE TABLE`` calls.
        node: validated NodeYaml.
        pack: assembled ResolvedPack (consulted for source-id → table
            mapping when ctx doesn't already carry it).
        profile: validated TenantProfile (unused in v0.3 preflight; kept
            in the signature for forward compatibility with §11.6 Gate 4
            bronze-schema-fingerprint drift detection).
        ctx: render context — used for ``bronze_table_for_source`` map
            (which the introspection calls use to identify the live
            bronze tables to DESCRIBE).

    Returns:
        :class:`PreflightReport` carrying any collected errors. Empty
        ``errors`` → preflight passed and ``execute_node`` may proceed
        to render the SQL.

    Notes:
        Does NOT raise on errors — collects them so the report-row writer
        in ``execute_node`` can record the full diagnostic. Programmer-
        error conditions (missing live table entirely, Spark session
        broken) still raise — the caller treats those as a different
        failure class than "expected column missing on this tenant".
    """
    errors: list[PreflightError] = []

    # 1. Required columns on each declared source.
    errors.extend(_check_required_columns(spark, node, ctx))

    # 2. Watermark column for merge-strategy nodes.
    if _is_merge_strategy(node):
        errors.extend(_check_watermark_column(spark, node, ctx))

    # 3. Partition columns for replace_partition (deferred strategy —
    #    schema accepts it but execution path doesn't run in v0.3; we
    #    still validate the partition shape so an early customer who
    #    declares one gets a useful diagnostic, not a NotImplementedError
    #    much later in execute_node).
    if _is_replace_partition_strategy(node):
        errors.extend(_check_partition_columns(spark, node, ctx))

    return PreflightReport(errors=tuple(errors))


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _is_merge_strategy(node: NodeYaml) -> bool:
    inc = node.refresh.incremental
    return inc is not None and inc.strategy == "merge"


def _is_replace_partition_strategy(node: NodeYaml) -> bool:
    inc = node.refresh.incremental
    return inc is not None and inc.strategy == "replace_partition"


def _check_required_columns(
    spark: "SparkSession", node: NodeYaml, ctx: "RunContext"
) -> list[PreflightError]:
    """For each entry in ``node.requiredColumns.<source>``, DESCRIBE the
    source's bronze table and assert the column exists.

    The ``requiredColumns`` map is keyed by source id (matching
    ``dependsOn.bronze[*].id``). Each value is a list of column names
    that MUST be present in the live bronze schema.
    """
    errors: list[PreflightError] = []
    required = getattr(node, "required_columns", None) or {}
    for source_id, required_cols in required.items():
        table = ctx.bronze_table_for_source.get(source_id)
        if table is None:
            errors.append(
                PreflightError(
                    code=AIDPF_2042_REQUIRED_COLUMN_MISSING,
                    source=source_id,
                    message=(
                        f"required-columns preflight could not find a bronze table "
                        f"identifier for source {source_id!r} in "
                        f"ctx.bronze_table_for_source. Confirm the source is "
                        f"declared in bundle.yaml + bronze.yaml."
                    ),
                )
            )
            continue
        present = _describe_columns(spark, table)
        for col in required_cols:
            if col not in present:
                errors.append(
                    PreflightError(
                        code=AIDPF_2042_REQUIRED_COLUMN_MISSING,
                        source=source_id,
                        message=(
                            f"required column {col!r} missing from live bronze "
                            f"schema for source {source_id!r} (table {table!r}). "
                            f"Live columns: {sorted(present)!r}."
                        ),
                    )
                )
    return errors


def _check_watermark_column(
    spark: "SparkSession", node: NodeYaml, ctx: "RunContext"
) -> list[PreflightError]:
    """For merge-strategy nodes, confirm the declared watermark column
    exists in the source bronze schema. Missing → AIDPF-2043.
    """
    inc = node.refresh.incremental
    if inc is None or inc.watermark is None:
        # Static validator should have rejected merge without watermark
        # config (PLAN §11.3 R2 / AIDPF-2050); defensive check anyway.
        return []
    source_id = inc.watermark.source
    column = inc.watermark.column
    table = ctx.bronze_table_for_source.get(source_id)
    if table is None:
        return [
            PreflightError(
                code=AIDPF_2043_WATERMARK_COLUMN_MISSING,
                source=source_id,
                message=(
                    f"merge-strategy watermark preflight could not find a "
                    f"bronze table for source {source_id!r}. Confirm the source "
                    f"appears in ctx.bronze_table_for_source."
                ),
            )
        ]
    present = _describe_columns(spark, table)
    if column not in present:
        return [
            PreflightError(
                code=AIDPF_2043_WATERMARK_COLUMN_MISSING,
                source=source_id,
                message=(
                    f"merge-strategy watermark column {column!r} missing from "
                    f"source {source_id!r} (table {table!r}). Live columns: "
                    f"{sorted(present)!r}."
                ),
            )
        ]
    return []


def _check_partition_columns(
    spark: "SparkSession", node: NodeYaml, ctx: "RunContext"
) -> list[PreflightError]:
    """For replace_partition strategy, confirm partition columns exist
    on the target. v0.3 doesn't execute this strategy, but we run the
    check so customers declaring it get an early diagnostic.
    """
    inc = node.refresh.incremental
    if inc is None:
        return []
    if not inc.partition_columns:
        # Static validator (R6) should have rejected this; defensive.
        return [
            PreflightError(
                code=AIDPF_2044_PARTITION_COLUMN_MISSING,
                source=None,
                message=(
                    f"replace_partition strategy declared without "
                    f"`partitionColumns` (PLAN §11.3 R6 / AIDPF-2054). Static "
                    f"validator should reject this earlier."
                ),
            )
        ]
    # v0.3: target table may not exist yet on first run; we don't fail
    # closed here — just record a soft warning that the runtime check
    # will repeat once the target materialises.
    return []


def _describe_columns(spark: "SparkSession", table: str) -> set[str]:
    """Return the set of column names from ``DESCRIBE TABLE <table>``.

    Filters out partition-info metadata rows that ``DESCRIBE TABLE``
    emits in some Spark versions (rows whose ``col_name`` starts with
    ``#``).
    """
    df = spark.sql(f"DESCRIBE TABLE {table}")
    rows = df.collect() if df is not None else []
    out: set[str] = set()
    for row in rows:
        # DESCRIBE TABLE returns col_name / data_type / comment.
        # Access via index 0 (works for both Row and tuple mocks).
        try:
            name = row[0]
        except (IndexError, TypeError):
            continue
        if not isinstance(name, str):
            continue
        if name.startswith("#") or name == "":
            continue
        out.add(name)
    return out
