"""Phase 2 state-layer additions — additive migration + atomic batch write.

This module sits ALONGSIDE the existing ``orchestrator/state.py`` (v1)
and adds:

* :data:`PHASE2_NEW_COLUMNS` — the tuple of new nullable columns added
  to ``fusion_bundle_state`` for content-pack runs.
* :func:`ensure_state_columns_v2` — additive migration using the same
  introspect-then-ADD COLUMNS pattern v1 uses (Spark rejects ``ADD
  COLUMN IF NOT EXISTS``; we DESCRIBE first, compute the missing set,
  emit a single ALTER).
* :func:`update_latest_view_for_phase2` — redeploys the ``fusion_bundle_state_latest``
  view with the widened partition ``(run_id, dataset_id, layer, source_id)``.
  v1 rows project identically in the common single-layer-dataset_id
  case; multi-layer dataset_id (rare; v1 collapsed them — that was a
  latent bug) now correctly returns one row per layer.
* :class:`StateCommitError` — raised on a hard-write failure.
* :func:`write_state_rows_hard` — **atomic batch** write. Builds one
  DataFrame from ``rows`` and appends it to the state table in a
  single Delta append. Delta's append atomicity guarantees all rows
  commit together or none do, so a multi-source success cannot
  partially commit (which would advance the primary's
  output_watermark without the lookup audit rows reaching the table).

The v1 ``ensure_state_table`` and ``write_state_row`` continue to work
unchanged — Phase 2 callers (Step 11 execute_node) explicitly invoke
the Phase 2 helpers when ``--execution-backend content-pack`` is in
effect.

References:

* PLAN §11.9 (state migration + hard cursor commit)
* PLAN §11.10 (multi-source primary/lookup; per-source rows)
* PLAN §10b (corrected latest-view grain)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Sequence

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ..config.paths import TablePaths


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_4060_STATE_COMMIT_FAILURE = "AIDPF-4060"
"""State-row hard commit failed — Delta append raised; no rows committed."""

AIDPF_4061_OUTPUT_WATERMARK_REGRESSED = "AIDPF-4061"
"""State row written with `output_watermark` lower than the prior successful row.
Defensive guard — shouldn't happen in practice."""


class StateCommitError(Exception):
    """Hard-commit failure (AIDPF-4060). Raised by :func:`write_state_rows_hard`.

    Carries the original Spark exception as ``__cause__`` so callers
    can surface the underlying diagnostic without losing the stack.
    """


# ---------------------------------------------------------------------------
# Phase 2 column set
# ---------------------------------------------------------------------------

PHASE2_NEW_COLUMNS: tuple[tuple[str, str], ...] = (
    # Pack / profile identity.
    ("pack_id", "STRING"),
    ("pack_version", "STRING"),
    ("node_version", "STRING"),
    ("node_implementation_type", "STRING"),
    ("rendered_sql_hash", "STRING"),
    ("output_schema_hash", "STRING"),
    ("profile_hash", "STRING"),
    # Identity fingerprints (consumed by Step 9 plan-hash + §11.6 Gate 4).
    ("tenant_fingerprint", "STRING"),
    ("fusion_version", "STRING"),
    ("bronze_schema_fingerprint", "STRING"),
    # Source-level cursors (multi-source primary/lookup per PLAN §11.10).
    ("source_id", "STRING"),
    ("source_role", "STRING"),  # 'primary' | 'lookup'
    ("input_watermark_start", "TIMESTAMP"),
    ("input_watermark_end", "TIMESTAMP"),
    ("output_watermark", "TIMESTAMP"),
    ("consumed_version", "TIMESTAMP"),
    ("delta_row_count", "LONG"),
)
"""New nullable columns added to ``fusion_bundle_state`` for content-pack
runs. v1 rows write NULL for these columns and continue to work; v2
readers read them when present."""


# ---------------------------------------------------------------------------
# Additive migration — introspect-then-ADD COLUMNS pattern
# ---------------------------------------------------------------------------


def ensure_state_columns_v2(spark: "SparkSession", paths: "TablePaths") -> None:
    """Apply the Phase 2 additive column migration to ``fusion_bundle_state``.

    Uses Spark's introspect-then-ADD COLUMNS pattern (matches the v1
    ``ensure_state_table`` migration logic). Spark's ``ALTER TABLE ...
    ADD COLUMNS`` parser rejects ``IF NOT EXISTS`` — we DESCRIBE the
    table, compute the missing column set, and emit a single ALTER
    with ONLY those columns. Empty-diff (every Phase 2 column already
    present) is a no-op.

    This function ALSO redeploys the ``fusion_bundle_state_latest``
    view with the Phase 2 grain (PARTITION BY widened to include
    ``layer`` + ``source_id``). The view's DDL is updated to project
    the new Phase 2 columns alongside the v1 columns.

    Idempotent. Safe to call on every content-pack run (re-running
    after the migration has applied is a no-op).

    Args:
        spark: live Spark session.
        paths: TablePaths from the loaded bundle.

    Raises:
        AnalysisException: if the catalog/schema is wrong (the
            existing :func:`state.ensure_state_table` should run
            before this and would have surfaced that case already).
    """
    from . import state as v1_state

    table_path = v1_state._state_table_path(paths)
    view_path = v1_state._state_latest_view_path(paths)

    existing = v1_state._existing_state_columns(spark, table_path)
    missing = [
        (name, dtype) for name, dtype in PHASE2_NEW_COLUMNS if name not in existing
    ]
    if missing:
        spark.sql(v1_state._build_add_columns_ddl(table_path, missing))

    # Redeploy the latest view with the Phase 2 grain. CREATE OR REPLACE
    # VIEW is idempotent and updates the projection in place.
    spark.sql(_phase2_latest_view_ddl(table_path, view_path))


def _phase2_latest_view_ddl(table_path: str, view_path: str) -> str:
    """The Phase 2 ``fusion_bundle_state_latest`` view DDL.

    Widens the PARTITION BY to ``(run_id, dataset_id, layer, source_id)``.

    Common case (single-layer dataset_id, no source-level rows): the
    cardinality and projection are identical to the v1 view — adding
    ``layer`` to the key doesn't split because all v1 rows for a given
    dataset_id share the same layer value; adding ``source_id`` doesn't
    split because v1 rows leave it NULL.

    Multi-source Phase 2 rows project as N rows per ``(run_id,
    dataset_id, layer)`` where N = number of sources, each
    distinguished by its ``source_id``. Multi-layer dataset_id (rare;
    v1 collapsed them silently — that was a latent bug) now correctly
    returns one row per layer.

    Projects v1 columns + the Phase 2 column set so v2 readers see
    them; v1 readers ignore the new columns. Both the v1 and v2 column
    sets MUST exist on the underlying table for this view to compile —
    callers MUST run :func:`ensure_state_columns_v2` first (which
    drops + redeploys this view as part of its idempotent flow).
    """
    return f"""
        CREATE OR REPLACE VIEW {view_path} AS
        WITH ranked AS (
          SELECT
            run_id, dataset_id, layer, mode, last_watermark, last_run_at,
            status, row_count, error_message, skip_reason, duration_seconds,
            plan_hash, plan_snapshot,
            pack_id, pack_version, node_version, node_implementation_type,
            rendered_sql_hash, output_schema_hash, profile_hash,
            tenant_fingerprint, fusion_version, bronze_schema_fingerprint,
            source_id, source_role, input_watermark_start, input_watermark_end,
            output_watermark, consumed_version, delta_row_count,
            ROW_NUMBER() OVER (
              PARTITION BY run_id, dataset_id, layer, source_id
              ORDER BY last_run_at DESC
            ) AS rn
          FROM {table_path}
        )
        SELECT
          run_id, dataset_id, layer, mode, last_watermark, last_run_at,
          status, row_count, error_message, skip_reason, duration_seconds,
          plan_hash, plan_snapshot,
          pack_id, pack_version, node_version, node_implementation_type,
          rendered_sql_hash, output_schema_hash, profile_hash,
          tenant_fingerprint, fusion_version, bronze_schema_fingerprint,
          source_id, source_role, input_watermark_start, input_watermark_end,
          output_watermark, consumed_version, delta_row_count
        FROM ranked
        WHERE rn = 1
    """


# ---------------------------------------------------------------------------
# Atomic batch hard write — multi-source rows commit together or not at all
# ---------------------------------------------------------------------------


def write_state_rows_hard(
    spark: "SparkSession",
    paths: "TablePaths",
    rows: Sequence[Mapping[str, Any]],
) -> None:
    """Append a batch of state rows to ``fusion_bundle_state`` atomically.

    Phase 2 (PLAN §11.9) writes one state row per source per node per
    run for a successful content-pack execution. Per-row writes would
    leave a window where the primary's ``output_watermark`` has
    committed but a lookup audit row hasn't — that would silently
    advance the cursor without the audit trail. This function does a
    **single Delta append** of the full row list so the entire batch
    commits or none of it does.

    The caller (Step 11 ``execute_node``) assembles every row (primary
    + every lookup) in memory FIRST, then calls this function exactly
    once. ``rows`` may be a single-element list for single-source
    nodes; the API shape is uniform.

    Args:
        spark: live Spark session.
        paths: TablePaths from the loaded bundle.
        rows: sequence of dict-shaped rows. Each dict must carry the
            columns of ``fusion_bundle_state`` (v1 + Phase 2). Missing
            columns are filled with NULL via the schema reconciliation
            Delta runs at append time. Empty sequence is a no-op.

    Raises:
        StateCommitError: AIDPF-4060 — the underlying Delta append
            raised. No rows from this batch are visible to subsequent
            reads. The previous run's primary row's ``last_watermark``
            remains the cursor on the next run.
    """
    if not rows:
        return

    from . import state as v1_state

    table_path = v1_state._state_table_path(paths)

    # Build the DataFrame from the rows. We let Spark infer the schema
    # from the row dicts; missing columns become NULL via the table's
    # Delta schema reconciliation at append time. Phase 2 state rows
    # always carry the same keys (the caller assembles them uniformly),
    # so the inferred schema is consistent.
    try:
        df = spark.createDataFrame(list(rows))
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table_path)
        )
    except Exception as exc:  # noqa: BLE001 — re-wrap any Spark failure
        raise StateCommitError(
            f"{AIDPF_4060_STATE_COMMIT_FAILURE}: failed to commit {len(rows)} "
            f"state row(s) to {table_path}. Delta append raised: {type(exc).__name__}: {exc}. "
            f"No rows from this batch are visible to subsequent reads; the "
            f"prior run's last_watermark remains the cursor."
        ) from exc
