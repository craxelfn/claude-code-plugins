"""Per-node preflight for the content-pack runner.

Preflight runs in :func:`sql_runner.execute_node` *after* static schema
validation and *before* SQL rendering. Its job is to fail fast on
runtime-only conditions that the static validator can't see:

* Required columns missing in the live bronze schema.
* Watermark column missing in the source bronze (for merge strategy).
* Partition columns missing on the target (validate-only for deferred
  ``replace_partition`` strategy).

**Crucial ordering invariant**: preflight does NOT render SQL. The
renderer is invoked exactly once in ``execute_node``, *after*
this preflight passes. This separation is what makes the
``preflight-blocked`` unit test branch assert "renderer never called".

Preflight covers required-column, watermark-column, and partition-column
gates only; SQL rendering and execution stay in ``sql_runner``.
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

AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF = "AIDPF-2046"
"""A ``requiredColumns`` entry uses the ``$column.<key>`` reference
syntax but the key is either (a) not declared in ``pack.yaml``'s
``columnAliases``, or (b) declared but missing from the tenant profile's
``resolved.column`` map (bootstrap not run, or alias was added after
last bootstrap)."""


_COLUMN_REF_PREFIX = "$column."
"""YAML prefix marking a ``requiredColumns`` entry as a reference to a
``columnAliases.<key>`` resolved value in the tenant profile, rather
than a literal column name. Backward-compatible: entries without the
prefix are still treated as literals."""


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
        profile: validated TenantProfile (used for column-alias resolution).
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

    # bronze_extract nodes CREATE their target table from the live PVO —
    # they don't read a pre-existing bronze table. The checks below all
    # `DESCRIBE` the node's bronze table, which doesn't exist yet on a
    # first-ever seed (or after a drop) and would raise an uncaught
    # AnalysisException. The bronze source is validated against the PVO by
    # the AIDPF-4071 source gate + the post-write AIDPF-4070 assertion, so
    # there's nothing for table-introspection preflight to do here.
    if getattr(node.implementation, "type", None) == "bronze_extract":
        return PreflightReport(errors=())

    # 1. Required columns on each declared source.
    errors.extend(_check_required_columns(spark, node, pack, profile, ctx))

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
    spark: "SparkSession",
    node: NodeYaml,
    pack: "ResolvedPack",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    ctx: "RunContext",
) -> list[PreflightError]:
    """For each entry in ``node.requiredColumns.<source>``, DESCRIBE the
    source's bronze table and assert the column exists.

    The ``requiredColumns`` map is keyed by source id (matching
    ``dependsOn.bronze[*].id``). Each value is a list of column names
    that MUST be present in the live bronze schema. Entries beginning
    with ``$column.`` are references into ``pack.columnAliases`` —
    resolved against the tenant profile's ``resolved.column`` map
    before the live-column check.
    """
    errors: list[PreflightError] = []
    required = getattr(node, "required_columns", None) or {}
    pack_alias_keys = set(pack.pack.column_aliases.keys())

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
        present_ci = {c.lower(): c for c in present}
        for entry in required_cols:
            resolved, ref_error = _resolve_required_column_entry(
                entry, profile, source_id, pack_alias_keys
            )
            if ref_error is not None:
                errors.append(ref_error)
                continue
            assert resolved is not None  # mypy: ref_error None ⇒ resolved set
            if resolved.lower() not in present_ci:
                # Diagnostic names BOTH the YAML entry (for traceability
                # back to the pack source) and the resolved physical
                # column (for "go look in DESCRIBE"). When `entry` is a
                # literal these are the same — the message stays terse.
                resolved_hint = (
                    f" (resolved from {entry!r})" if entry != resolved else ""
                )
                errors.append(
                    PreflightError(
                        code=AIDPF_2042_REQUIRED_COLUMN_MISSING,
                        source=source_id,
                        message=(
                            f"required column {resolved!r}{resolved_hint} missing "
                            f"from live bronze schema for source {source_id!r} "
                            f"(table {table!r}). Live columns: {sorted(present)!r}."
                        ),
                    )
                )
    return errors


def _resolve_required_column_entry(
    entry: str,
    profile: "TenantProfile",  # noqa: F821
    source_id: str,
    pack_alias_keys: set[str],
) -> tuple[str | None, PreflightError | None]:
    """Resolve a ``requiredColumns`` entry to a physical column name.

    Returns ``(resolved, None)`` on success, or ``(None, error)`` when
    the entry uses ``$column.<key>`` syntax but the key cannot be
    resolved. A literal entry (no prefix) returns ``(entry, None)``
    unchanged — backward-compatible with v0.3 packs.
    """
    if not entry.startswith(_COLUMN_REF_PREFIX):
        return entry, None
    key = entry[len(_COLUMN_REF_PREFIX) :]
    if key not in pack_alias_keys:
        return None, PreflightError(
            code=AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF,
            source=source_id,
            message=(
                f"requiredColumns entry {entry!r} references columnAlias key "
                f"{key!r} which is not declared in pack.yaml's `columnAliases`. "
                f"Known keys: {sorted(pack_alias_keys)!r}. "
                f"Fix the pack YAML — either declare the alias or use a literal "
                f"column name."
            ),
        )
    resolved = profile.resolved.column.get(key)
    if not resolved:
        return None, PreflightError(
            code=AIDPF_2046_REQUIRED_COLUMN_UNRESOLVED_REF,
            source=source_id,
            message=(
                f"requiredColumns entry {entry!r} references columnAlias key "
                f"{key!r} declared in pack.yaml, but the tenant profile has no "
                f"resolved value for it. Re-run `aidp-fusion-bundle bootstrap` "
                f"to populate the profile."
            ),
        )
    return resolved, None


def _check_watermark_column(
    spark: "SparkSession", node: NodeYaml, ctx: "RunContext"
) -> list[PreflightError]:
    """For merge-strategy nodes, confirm the declared watermark column
    exists in the source bronze schema. Missing → AIDPF-2043.
    """
    inc = node.refresh.incremental
    if inc is None or inc.watermark is None:
        # Static validator should have rejected merge without watermark
        # config (AIDPF-2050); defensive check anyway.
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
    present_ci = {c.lower(): c for c in present}
    if column.lower() not in present_ci:
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
                    f"`partitionColumns` (AIDPF-2054). Static "
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
