"""``fusion_bundle_state`` Delta-table contract.

Schema and per-step write logic for the state table that records every
orchestrator step's outcome. Single source of truth for the table's DDL +
the canonical INSERT shape.

Two-layer failure semantics:
  - ``ensure_state_table`` is HARD — failure halts the run before any
    module dispatch (high-probability structural problems like catalog
    typo, missing schema, DDL grant misconfig).
  - ``write_state_row`` is wrapped by ``runtime._safe_write_state_row``,
    which logs WARN and continues on per-step failures (transient flakes
    shouldn't kill a long medallion run).

``read_last_watermark`` (P1.5β.1) returns the most-recent ``status='success'``
row's ``last_watermark`` for a given ``(dataset_id, layer)`` pair, ordered
by ``last_run_at DESC, last_watermark DESC NULLS LAST LIMIT 1``. Read is
SOFT: an underlying Spark/metastore exception returns ``None`` and emits
a structured WARN log carrying the marker ``watermark_read_soft_failed``
that operators can grep / alert on. Phase α stub semantics (always
``None``) are preserved for the no-prior-row + NULL-watermark cases.

Resume + multi-row semantics
============================

The table is **append-only**. A normal (non-resumed) run writes one row
per dataset_id. **A resumed run may write multiple rows per
(run_id, dataset_id)** — for example a `failed` row from the original
attempt + a `resumed_skipped` carry-forward + an eventual `success`
under the resume can all coexist under the same `run_id`. This is
intentional (preserves the CLAUDE.md medallion `_run_id` invariant —
gold/silver `<layer>_run_id` columns join 1:1 to a single logical
pipeline run, never split across resume attempts).

Consequences for consumers:
  * **Read from the ``fusion_bundle_state_latest`` Delta VIEW**
    (created by ``ensure_state_table``). It projects one row per
    ``(run_id, dataset_id)`` via
    ``ROW_NUMBER() OVER (PARTITION BY run_id, dataset_id ORDER BY
    last_run_at DESC)`` and is the safe default.
  * Naïve queries against the raw table
    (``WHERE status='failed'``, ``COUNT(*)``, ``SUM(row_count)``)
    over-count failures and miscount datasets on resumed runs.
  * The operator-facing global "latest snapshot across all runs"
    query in ``commands/run.py`` partitions by ``dataset_id`` alone
    (no ``run_id``) — different aggregation, kept inline.

The table also carries ``plan_hash`` and ``plan_snapshot`` columns —
the resume drift gate's metadata. Legacy rows written by earlier
plugin builds land NULL on both; ``read_resumable_state`` rejects
them as non-resumable.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal

from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

    from .runtime import RunStep

logger = logging.getLogger(__name__)

# Stable marker string embedded in the WARN log when
# ``read_last_watermark`` soft-fails (Spark/metastore exception swallowed
# → ``None`` return). Part of the public audit-signal contract — operator
# alerting / log shippers key off this exact string. Do NOT rename
# without coordinating with LIMITS.md F6 + D5c's regression test.
WATERMARK_READ_SOFT_FAILED_MARKER: Literal[
    "watermark_read_soft_failed"
] = "watermark_read_soft_failed"


def _normalize_to_utc(ts: "datetime | None") -> "datetime | None":
    """Coerce a Spark-returned ``datetime`` to aware UTC.

    Spark ``TIMESTAMP`` columns deserialize to Python ``datetime`` with
    session-dependent ``tzinfo`` — naive on some builds, aware-with-
    session-zone on others. The state-table write path always persists
    ``datetime.now(timezone.utc) - WATERMARK_SAFETY_WINDOW``, so any
    naive value coming back is a session-precision artifact of the same
    aware-UTC value that was written. Normalizing at the read boundary
    keeps every downstream comparison (monotonicity check, arithmetic)
    from raising ``TypeError: can't compare offset-naive and offset-
    aware datetimes`` — which would surface as a spurious step failure
    and cascade-skip rather than the intended
    ``WatermarkMonotonicityError``.
    """
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# DDL — fusion_bundle_state schema
# ---------------------------------------------------------------------------

_STATE_TABLE_NAME = "fusion_bundle_state"
_STATE_LATEST_VIEW_NAME = "fusion_bundle_state_latest"


def _state_table_path(paths: TablePaths) -> str:
    """3-part path: ``{catalog}.{bronze_schema}.fusion_bundle_state``."""
    return paths.bronze(_STATE_TABLE_NAME)


def _state_latest_view_path(paths: TablePaths) -> str:
    """3-part path for the latest-per-(run_id, dataset_id) VIEW."""
    return paths.bronze(_STATE_LATEST_VIEW_NAME)


def _ddl(table_path: str) -> str:
    """Append-only. Each orchestrator step writes exactly one row.
    ``skip_reason`` is the structured discriminator for cascade /
    aborted / resume-skip rows (nullable for non-skipped /
    non-resumed-skipped rows).

    ``plan_hash`` + ``plan_snapshot`` carry the resume drift gate's
    metadata. Both nullable so the table accepts rows written by
    earlier plugin builds during the migration window;
    ``read_resumable_state`` rejects a run whose rows have NULL on
    either column as non-resumable.
    """
    return f"""
        CREATE TABLE IF NOT EXISTS {table_path} (
          run_id           STRING       NOT NULL,
          dataset_id       STRING       NOT NULL,
          layer            STRING       NOT NULL,
          mode             STRING       NOT NULL,
          last_watermark   TIMESTAMP             ,
          last_run_at      TIMESTAMP    NOT NULL,
          status           STRING       NOT NULL,
          row_count        BIGINT                ,
          error_message    STRING                ,
          skip_reason      STRING                ,
          duration_seconds DOUBLE       NOT NULL,
          plan_hash        STRING                ,
          plan_snapshot    STRING
        )
        USING DELTA
        PARTITIONED BY (layer)
    """


_FIX21_NEW_COLUMNS: tuple[tuple[str, str], ...] = (
    ("plan_hash", "STRING"),
    ("plan_snapshot", "STRING"),
)


def _existing_state_columns(spark: "SparkSession", table_path: str) -> set[str]:
    """Return the set of column names currently on ``table_path``.

    Uses ``DESCRIBE TABLE`` because it's supported by both vanilla
    Spark and Databricks Delta — ``spark.catalog.listColumns`` exists
    too but goes through a different code path that the orchestrator
    doesn't otherwise exercise.

    DESCRIBE TABLE emits column rows followed by metadata rows for
    partitioning / detailed-info; the metadata block opens with a
    ``#``-prefixed marker row (``# Partitioning``, etc.), so we
    short-circuit at the first row whose ``col_name`` starts with
    ``#``. Defensive against the row class lacking ``col_name``
    (some Spark forks return the field as ``column``); falls back to
    the first column of the row tuple.
    """
    rows = spark.sql(f"DESCRIBE TABLE {table_path}").collect()
    columns: set[str] = set()
    for row in rows:
        try:
            name = row["col_name"]
        except (KeyError, TypeError, IndexError):
            try:
                name = row[0]
            except (KeyError, TypeError, IndexError):
                continue
        if not name or name.startswith("#"):
            # End of column block / metadata marker row.
            break
        columns.add(name)
    return columns


def _ensure_target_table_exists(
    spark: "SparkSession",
    target: str,
    schema: "object",
) -> None:
    """Idempotent ``CREATE TABLE IF NOT EXISTS`` for a Delta target whose
    schema is supplied by the source DataFrame.

    P1.17 B6c — used pre-MERGE for fresh-tenant bronze writes (the bronze
    MERGE against a non-existent target raises ``TABLE_OR_VIEW_NOT_FOUND``
    on the first incremental run for a tenant where seed never created
    that bronze table). On existing tables this is a no-op.

    ``schema`` is a Spark ``StructType`` from the source DataFrame; columns
    are emitted in the SAME order, each with the ``simpleString()`` form
    of its data type. NOTE: V1 ships ONLY the simple create path — the
    dropped-target silent-corruption guard (target missing AND prior
    cursor non-null) ships in P1.17c as ``IncrementalTargetMissingError``.
    """
    if spark.catalog.tableExists(target):
        return
    col_specs = ", ".join(
        f"{f.name} {f.dataType.simpleString()}" for f in schema.fields
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({col_specs}) USING DELTA")


def _build_add_columns_ddl(table_path: str, missing: list[tuple[str, str]]) -> str:
    """Schema-aware ``ALTER TABLE ... ADD COLUMNS (...)`` for the
    given ``(name, type)`` pairs.

    Spark SQL grammar does NOT accept ``IF NOT EXISTS`` inside the
    ``ADD COLUMNS`` clause — emitting that would fail the parser at
    every ``ensure_state_table`` call (i.e. every run + every
    resume). Caller (``ensure_state_table``) introspects the existing
    columns via :func:`_existing_state_columns` and invokes this
    helper only when at least one column is missing.
    """
    col_specs = ", ".join(f"{name} {dtype}" for name, dtype in missing)
    return f"ALTER TABLE {table_path} ADD COLUMNS ({col_specs})"


def _latest_view_ddl(table_path: str, view_path: str) -> str:
    """Delta VIEW projecting one row per ``(run_id, dataset_id)`` —
    the latest terminal state by ``last_run_at``.

    Resumed runs append multiple rows per ``(run_id, dataset_id)`` (a
    failed attempt + resumed-skipped carry-forward + eventual success
    may all coexist under the same ``run_id``). This VIEW collapses
    that to a single-row-per-pair projection so consumers don't have
    to remember the window pattern. Dashboard / alert / ad-hoc queries
    SHOULD ``SELECT FROM fusion_bundle_state_latest`` rather than the
    raw table.

    ``CREATE OR REPLACE VIEW`` is idempotent and updates the
    definition in place if the projected columns change in a future
    release.
    """
    return f"""
        CREATE OR REPLACE VIEW {view_path} AS
        WITH ranked AS (
          SELECT
            run_id, dataset_id, layer, mode, last_watermark,
            last_run_at, status, row_count, error_message,
            skip_reason, duration_seconds, plan_hash, plan_snapshot,
            ROW_NUMBER() OVER (
              PARTITION BY run_id, dataset_id
              ORDER BY last_run_at DESC
            ) AS rn
          FROM {table_path}
        )
        SELECT
          run_id, dataset_id, layer, mode, last_watermark, last_run_at,
          status, row_count, error_message, skip_reason,
          duration_seconds, plan_hash, plan_snapshot
        FROM ranked
        WHERE rn = 1
    """


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def ensure_state_table(spark: "SparkSession", paths: TablePaths) -> None:
    """HARD prerequisite — create the state table if missing AND probe
    writeability via INSERT/DELETE sentinel. Raises on any failure;
    the run loop's caller (``orchestrator.run``) lets this propagate
    uncaught so a structural problem halts BEFORE any module dispatch.

    Catches the high-probability failure modes:
      - wrong ``aidp.catalog`` (Spark AnalysisException at the DDL step)
      - missing ``aidp.bronzeSchema`` (same)
      - DDL/DML grant misconfig (PermissionError-shaped exception)
      - vault OCID unreachable for credential-bearing Delta paths

    After the CREATE, runs ``ALTER TABLE ADD COLUMNS IF NOT EXISTS``
    to ensure tables created by earlier plugin builds gain
    ``plan_hash`` + ``plan_snapshot``, then ``CREATE OR REPLACE VIEW
    fusion_bundle_state_latest`` so consumers have a one-row-per-
    ``(run_id, dataset_id)`` projection without remembering the window
    pattern. Both are idempotent.

    The probe writes a sentinel row with ``run_id='__ensure_probe__'``
    and ``status='probe'`` (NOT one of the four canonical values) so
    consumer queries that filter by canonical status never see it; the
    sentinel is deleted immediately after insertion.
    """
    table_path = _state_table_path(paths)
    view_path = _state_latest_view_path(paths)
    spark.sql(_ddl(table_path))
    # Schema-aware additive migration. `CREATE TABLE IF NOT EXISTS`
    # is a no-op when the table exists, so the new columns need an
    # `ALTER TABLE` follow-up to materialize on tables created by
    # earlier plugin builds. We can't write `ADD COLUMNS IF NOT
    # EXISTS (...)` — Spark SQL grammar rejects that — so introspect
    # the existing columns and ADD only the ones that are missing.
    # ALTER is skipped entirely when both are already present (the
    # common case for tables created at fix21+).
    existing_cols = _existing_state_columns(spark, table_path)
    missing = [
        (name, dtype) for name, dtype in _FIX21_NEW_COLUMNS
        if name not in existing_cols
    ]
    if missing:
        spark.sql(_build_add_columns_ddl(table_path, missing))
    # Idempotent view definition — CREATE OR REPLACE updates the
    # projection in place if the columns evolve.
    spark.sql(_latest_view_ddl(table_path, view_path))
    # Writeability probe — INSERT + DELETE sentinel.
    # Live-evidence fix (2026-05-17): every VALUES literal needs an explicit
    # CAST. Delta's strict type-merging refuses to coerce DECIMAL(2,1) → DOUBLE
    # on the `0.0` literal, and NULL needs a typed CAST for the nullable
    # columns. Unit tests with fake-Spark didn't catch this because they
    # accept any value; only the real Delta writer enforces the schema.
    # `plan_hash` + `plan_snapshot` are nullable in the schema, so the
    # probe writes NULL for both — keeps the sentinel row distinguishable
    # from real run rows (which carry non-NULL values when the
    # orchestrator stamps them).
    spark.sql(
        f"""
        INSERT INTO {table_path}
          (run_id, dataset_id, layer, mode, last_watermark, last_run_at,
           status, row_count, error_message, skip_reason, duration_seconds,
           plan_hash, plan_snapshot)
        VALUES
          ('__ensure_probe__', '__probe__', 'bronze', 'seed',
           CAST(NULL AS TIMESTAMP), current_timestamp(), 'probe',
           CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS STRING),
           CAST(0.0 AS DOUBLE),
           CAST(NULL AS STRING), CAST(NULL AS STRING))
        """
    )
    spark.sql(
        f"DELETE FROM {table_path} WHERE run_id = '__ensure_probe__'"
    )


def write_state_row(
    spark: "SparkSession", paths: TablePaths, step: "RunStep"
) -> None:
    """Insert one row into ``fusion_bundle_state``. Raw write — failures
    propagate. The orchestrator's ``_safe_write_state_row`` wrapper in
    ``runtime.py`` catches + logs the WARN per the soft-write contract.
    """
    table_path = _state_table_path(paths)
    # Build the INSERT via parameterized literals. Spark SQL doesn't
    # have native prepared statements for CREATE/INSERT, but quoting
    # via repr() + ``f""""`` is safe for the strict-SQL-identifier
    # values we accept (TablePaths._validate_identifier enforces).
    # The user-controlled values (error_message especially) need
    # escaping; we use a single-quote-doubled escape consistent with
    # Delta's SQL parser.

    # Live-evidence fix (2026-05-17): every NULL value needs a typed CAST
    # because Delta's schema-merge refuses bare NULL → BIGINT/STRING
    # promotion. Same fix as ensure_state_table's writeability probe.
    def _q(s: str | None) -> str:
        """Quote a string literal — None → typed CAST(NULL AS STRING)."""
        if s is None:
            return "CAST(NULL AS STRING)"
        escaped = s.replace("'", "''")
        return f"'{escaped}'"

    def _ts(t: "datetime | None") -> str:
        if t is None:
            return "CAST(NULL AS TIMESTAMP)"
        return f"TIMESTAMP '{t.isoformat(sep=' ')}'"

    def _bigint(n: int | None) -> str:
        if n is None:
            return "CAST(NULL AS BIGINT)"
        return f"CAST({n} AS BIGINT)"

    def _double(d: float) -> str:
        # Bare `0.0` is DECIMAL(2,1); needs explicit DOUBLE cast for Delta.
        return f"CAST({d} AS DOUBLE)"

    # P1.5β.1: persist ``step.last_watermark`` (the OUTPUT cursor —
    # captured pre-extract as ``extract_started_at -
    # WATERMARK_SAFETY_WINDOW`` for bronze, preserved on empty deltas,
    # ``None`` for silver/gold until P1.17). Phase α conflated
    # ``watermark_used`` (INPUT) with this column; β.1 separates the
    # two — ``watermark_used`` stays in-memory only on ``RunStep``
    # for debug/logs/__repr__, no state column carries it. See B0
    # of the P1.5β plan.
    spark.sql(
        f"""
        INSERT INTO {table_path}
          (run_id, dataset_id, layer, mode, last_watermark, last_run_at,
           status, row_count, error_message, skip_reason, duration_seconds,
           plan_hash, plan_snapshot)
        VALUES
          ({_q(step.run_id)},
           {_q(step.dataset_id)},
           {_q(step.layer)},
           {_q(step.mode)},
           {_ts(step.last_watermark)},
           current_timestamp(),
           {_q(step.status)},
           {_bigint(step.row_count)},
           {_q(step.error_message)},
           {_q(step.skip_reason)},
           {_double(step.duration_seconds)},
           {_q(step.plan_hash)},
           {_q(step.plan_snapshot)})
        """
    )


def read_last_watermark(
    spark: "SparkSession",
    paths: TablePaths,
    dataset_id: str,
    layer: Literal["bronze", "silver", "gold"] = "bronze",
) -> "datetime | None":
    """Return the most-recent ``status='success'`` row's
    ``last_watermark`` for ``(dataset_id, layer)``, as an aware UTC
    ``datetime``. Returns ``None`` when:

    - no ``status='success'`` row exists for the pair;
    - the most-recent success row has ``last_watermark IS NULL``
      (e.g. a true-first-empty bronze run, or any silver/gold row
      in β.1 — silver/gold ``last_watermark`` capture is deferred
      to P1.17);
    - the underlying Spark/metastore read raises (soft-fail: log
      WARN + return ``None``; the exception is swallowed).

    Ordering — ``last_run_at DESC, last_watermark DESC NULLS LAST
    LIMIT 1``. ``last_run_at`` is the primary key per Phase α's
    convention (see ``state.py:194`` and ``read_resumable_state``);
    the secondary key breaks ties deterministically by preferring the
    row that recorded more progress, aligning with the monotonicity
    invariant. There is no ``finished_at`` column on the schema —
    do not order by it.

    Read is issued via ``spark.sql(...)`` (matching Phase α's
    ``read_resumable_state`` convention so the same in-memory
    ``_FakeSpark`` test harness works); user-controlled identifiers
    (``dataset_id``, ``layer``) are escaped via :func:`_q` to defeat
    apostrophe-bearing strings without falling through to the
    DataFrame API (which would require pyspark at import time and
    break the unit-test environment).

    Failure semantics: a Spark/SQL exception logs a structured WARN
    carrying ``dataset_id``, ``layer``, ``repr(exc)`` and the stable
    marker ``"watermark_read_soft_failed"`` (see
    :data:`WATERMARK_READ_SOFT_FAILED_MARKER`), then returns ``None``.
    Operators monitor for the marker to detect the documented
    empty-delta + read-failure regression (LIMITS.md F6).
    """
    def _q(s: str | None) -> str:
        # Mirrors the helper defined locally inside ``write_state_row``
        # for the same escaping contract; defined here too so the read
        # path doesn't reach into the write helper's local scope.
        if s is None:
            return "CAST(NULL AS STRING)"
        escaped = s.replace("'", "''")
        return f"'{escaped}'"

    table_path = _state_table_path(paths)
    query = f"""
        SELECT last_watermark
        FROM {table_path}
        WHERE dataset_id = {_q(dataset_id)}
          AND layer = {_q(layer)}
          AND status = 'success'
        ORDER BY last_run_at DESC, last_watermark DESC NULLS LAST
        LIMIT 1
    """
    try:
        rows = spark.sql(query).collect()
    except Exception as exc:
        logger.warning(
            "%s dataset_id=%r layer=%r exc=%r",
            WATERMARK_READ_SOFT_FAILED_MARKER,
            dataset_id,
            layer,
            exc,
        )
        return None

    if not rows:
        return None
    return _normalize_to_utc(rows[0]["last_watermark"])


# ---------------------------------------------------------------------------
# Resume-time state read
# ---------------------------------------------------------------------------


from dataclasses import dataclass


@dataclass(frozen=True)
class ResumeContext:
    """Snapshot of ``fusion_bundle_state`` for a single ``run_id`` at
    resume-time. Returned by :func:`read_resumable_state` and consumed
    by the orchestrator's resume flow.

    ``succeeded``: set of ``dataset_id`` whose latest terminal status
    under this ``run_id`` is ``'success'`` or ``'resumed_skipped'``.
    Both count as "done, don't dispatch again" — the second case is a
    carry-forward from a prior resume of this same run_id, and a
    re-resume must treat it as already done (otherwise the contract
    breaks on a re-resume of an already-resumed run).

    ``plan_hash`` / ``plan_snapshot``: the single non-NULL values
    observed across the run's rows. ``read_resumable_state`` rejects
    runs whose rows are missing either, so consumers can assume both
    are populated.

    ``succeeded_schemas``: ``dataset_id`` → ``effective_schema`` for
    succeeded bronze nodes, parsed out of the snapshot. The resume
    flow uses this to compute the post-preflight plan hash without
    re-probing BICC for already-succeeded nodes.

    ``succeeded_row_counts``: ``(dataset_id, layer)`` → most-recent
    non-NULL ``row_count`` observed for that pair under this
    ``run_id``. Carry-forwarded into ``RunStep.resumed_skip`` so the
    latest-row projection (and the ``fusion_bundle_state_latest``
    VIEW) preserve the original logical row count instead of NULL.
    Walks back past any ``resumed_skipped`` rows (those have NULL
    row_count by definition — no work done) to the actual success
    row. **Tuple key** (P1.5β.1): matches the state table's
    primary-key grain; today no shipped registry entry reuses a
    ``dataset_id`` across layers, but a future addition that did
    would silently collide under the prior ``str``-only key.

    ``succeeded_last_watermarks`` (P1.5β.1): ``(dataset_id, layer)``
    → most-recent ``last_watermark`` observed for that pair under
    this ``run_id``. Carry-forwarded into ``RunStep.resumed_skip``
    so a resumed-skip row preserves the original bronze run's
    persisted cursor on the ``fusion_bundle_state_latest``
    projection rather than regressing it to NULL. Same tuple-key
    rationale as ``succeeded_row_counts``.

    ``original_started_at``: earliest ``last_run_at`` for this run_id.
    Surfaced in the resume-banner so the operator sees how old the
    checkpoint is.
    """

    run_id: str
    succeeded: frozenset[str]
    plan_hash: str
    plan_snapshot: str
    succeeded_schemas: "dict[str, str]"
    succeeded_row_counts: "dict[tuple[str, str], int]"
    succeeded_last_watermarks: "dict[tuple[str, str], datetime | None]"
    original_started_at: "datetime"


_RESUMABLE_TERMINAL_STATUSES = (
    "success", "failed", "skipped", "resumed_skipped", "deferred",
)


def read_resumable_state(
    spark: "SparkSession",
    paths: TablePaths,
    run_id: str,
) -> "ResumeContext":
    """Read ``fusion_bundle_state`` for ``run_id`` and return a
    ``ResumeContext`` summarizing what already succeeded + the stored
    plan-hash / plan-snapshot for drift comparison.

    SQL contract — the ``run_id`` filter MUST live inside the
    ranked CTE, before ``ROW_NUMBER()``. On a shared state table,
    a global window across multiple runs would pick the wrong row
    when two runs touched the same ``dataset_id``. The in-CTE filter
    constrains the window to this run_id alone:

        WITH ranked AS (
          SELECT ..., ROW_NUMBER() OVER (
            PARTITION BY dataset_id ORDER BY last_run_at DESC
          ) AS rn
          FROM <state_table>
          WHERE run_id = :resume_run_id
            AND status IN (<terminal>)
        )
        SELECT ... FROM ranked WHERE rn = 1

    Failure modes (all raise to the caller, which lets them propagate
    so the CLI exits 2 cleanly via OrchestratorConfigError):

      * Zero rows for ``run_id`` ⇒ ``ResumeRunNotFoundError``.
      * Any row has ``plan_hash IS NULL`` or
        ``plan_snapshot IS NULL`` ⇒
        ``ResumeRunNotResumableError`` (legacy row or partially-
        migrated write path; no degraded-metadata fallback).
      * Multiple distinct non-NULL ``plan_hash`` values across the
        result set ⇒ ``RuntimeError`` (state corruption — the
        orchestrator never writes more than one hash per run_id).
    """
    # Local imports to avoid circular dep with errors.py at module
    # load (state.py is imported very early in orchestrator init).
    from .errors import (
        ResumeRunNotFoundError,
        ResumeRunNotResumableError,
    )

    table_path = _state_table_path(paths)
    status_list = ", ".join(f"'{s}'" for s in _RESUMABLE_TERMINAL_STATUSES)
    # The `run_id` filter is parameterized via repr() to defeat
    # injection. TablePaths.__post_init__ already validates the
    # table_path components; the caller-supplied run_id is the only
    # value originating outside the trusted boundary.
    escaped_run_id = run_id.replace("'", "''")
    # P1.5β.1: partition by (dataset_id, layer) — the state-table
    # primary-key grain. Today no shipped registry entry reuses a
    # ``dataset_id`` across layers; the partition fix is paired with
    # the tuple-keyed ``succeeded_row_counts`` /
    # ``succeeded_last_watermarks`` dicts so a future registry
    # addition that collides ``dataset_id`` across layers doesn't
    # silently drop the upper-layer row from the window.
    query = f"""
        WITH ranked AS (
          SELECT
            dataset_id, status, plan_hash, plan_snapshot,
            last_run_at, layer, mode,
            ROW_NUMBER() OVER (
              PARTITION BY dataset_id, layer
              ORDER BY last_run_at DESC
            ) AS rn
          FROM {table_path}
          WHERE run_id = '{escaped_run_id}'
            AND status IN ({status_list})
        )
        SELECT dataset_id, status, plan_hash, plan_snapshot,
               last_run_at, layer
        FROM ranked
        WHERE rn = 1
    """
    rows = spark.sql(query).collect()

    if not rows:
        raise ResumeRunNotFoundError(
            f"--resume: no rows in fusion_bundle_state for run_id={run_id!r}. "
            f"Check the value (operator typo?) or use `aidp-fusion-bundle "
            f"status` to list recent run_ids."
        )

    # Validate that every row has both drift-gate metadata fields populated.
    null_hash_dsids = [r["dataset_id"] for r in rows if r["plan_hash"] is None]
    null_snapshot_dsids = [r["dataset_id"] for r in rows if r["plan_snapshot"] is None]
    if null_hash_dsids:
        raise ResumeRunNotResumableError(
            f"--resume: run_id={run_id!r} is not resumable — "
            f"{len(null_hash_dsids)} row(s) lack plan_hash. This run "
            f"was written by an earlier plugin build that didn't store "
            f"drift-gate metadata; re-run from scratch."
        )
    if null_snapshot_dsids:
        raise ResumeRunNotResumableError(
            f"--resume: run_id={run_id!r} is not resumable — "
            f"{len(null_snapshot_dsids)} row(s) have plan_hash set but "
            f"plan_snapshot is NULL (partially-migrated write path). "
            f"Re-run from scratch."
        )

    # Verify the plan_hash is consistent across all rows. A run never
    # writes more than one hash; multiple values means state corruption.
    distinct_hashes = {r["plan_hash"] for r in rows}
    if len(distinct_hashes) > 1:  # pragma: no cover — corruption guard
        raise RuntimeError(
            f"--resume: run_id={run_id!r} state corruption — multiple "
            f"distinct plan_hash values found: {sorted(distinct_hashes)}. "
            f"Each run_id writes exactly one hash."
        )
    plan_hash = next(iter(distinct_hashes))
    plan_snapshot = rows[0]["plan_snapshot"]

    # `succeeded` includes BOTH 'success' AND 'resumed_skipped' so a
    # re-resume of an already-resumed run treats carry-forwards as
    # already done. See ResumeContext docstring for rationale.
    succeeded: set[str] = {
        r["dataset_id"]
        for r in rows
        if r["status"] in ("success", "resumed_skipped")
    }

    # Parse `succeeded_schemas` out of the snapshot's `nodes` list —
    # bronze nodes only (silver/gold/deferred have effective_schema="").
    succeeded_schemas: dict[str, str] = {}
    import json as _json
    try:
        snapshot = _json.loads(plan_snapshot)
        for node in snapshot.get("nodes", []):
            ds_id = node.get("dataset_id")
            schema = node.get("effective_schema") or ""
            if ds_id in succeeded and schema:
                succeeded_schemas[ds_id] = schema
    except (ValueError, TypeError):  # pragma: no cover — corruption guard
        # If the snapshot is unparseable, treat the run as non-resumable.
        # The schema migration writes valid JSON so this only fires on
        # a hand-edited row.
        raise ResumeRunNotResumableError(
            f"--resume: run_id={run_id!r} plan_snapshot is not valid "
            f"JSON. State row was hand-edited or written by a broken "
            f"build; re-run from scratch."
        )

    original_started_at = min(r["last_run_at"] for r in rows)

    # Build succeeded_row_counts: for each succeeded (dataset_id, layer),
    # the most-recent non-NULL row_count under this run_id. A
    # `resumed_skipped` row has NULL row_count by definition (no work
    # done), so on a re-resume the latest terminal row may be NULL;
    # walk back to find the actual success row's count. Done as a
    # second small query so the existing latest-per-(dataset, layer)
    # window doesn't need to widen.
    #
    # P1.5β.1: partition + dict key are now (dataset_id, layer) tuples
    # matching the state-table primary-key grain. See ResumeContext
    # docstring for rationale.
    row_count_query = f"""
        WITH ranked AS (
          SELECT dataset_id, layer, row_count, last_run_at,
                 ROW_NUMBER() OVER (
                   PARTITION BY dataset_id, layer
                   ORDER BY last_run_at DESC
                 ) AS rn
          FROM {table_path}
          WHERE run_id = '{escaped_run_id}'
            AND status IN ({status_list})
            AND row_count IS NOT NULL
        )
        SELECT dataset_id, layer, row_count FROM ranked WHERE rn = 1
    """
    rc_rows = spark.sql(row_count_query).collect()
    succeeded_row_counts: dict[tuple[str, str], int] = {}
    for r in rc_rows:
        ds_id = r["dataset_id"]
        layer = r["layer"]
        if ds_id in succeeded:
            succeeded_row_counts[(ds_id, layer)] = int(r["row_count"])

    # Build succeeded_last_watermarks: for each succeeded
    # (dataset_id, layer), the most-recent ``last_watermark`` (which
    # may be NULL — e.g. silver/gold rows in β.1, or a true-first-
    # empty bronze run). Unlike succeeded_row_counts, we DO NOT
    # filter out NULL ``last_watermark`` rows in the WHERE clause —
    # a silver/gold success row with NULL watermark is the canonical
    # case in β.1, and carrying ``None`` forward is the correct
    # behavior. The latest terminal row per pair wins.
    last_watermark_query = f"""
        WITH ranked AS (
          SELECT dataset_id, layer, last_watermark, last_run_at,
                 ROW_NUMBER() OVER (
                   PARTITION BY dataset_id, layer
                   ORDER BY last_run_at DESC
                 ) AS rn
          FROM {table_path}
          WHERE run_id = '{escaped_run_id}'
            AND status IN ({status_list})
        )
        SELECT dataset_id, layer, last_watermark FROM ranked WHERE rn = 1
    """
    lw_rows = spark.sql(last_watermark_query).collect()
    succeeded_last_watermarks: dict[tuple[str, str], datetime | None] = {}
    for r in lw_rows:
        ds_id = r["dataset_id"]
        layer = r["layer"]
        if ds_id in succeeded:
            succeeded_last_watermarks[(ds_id, layer)] = _normalize_to_utc(
                r["last_watermark"]
            )

    return ResumeContext(
        run_id=run_id,
        succeeded=frozenset(succeeded),
        plan_hash=plan_hash,
        plan_snapshot=plan_snapshot,
        succeeded_schemas=succeeded_schemas,
        succeeded_row_counts=succeeded_row_counts,
        succeeded_last_watermarks=succeeded_last_watermarks,
        original_started_at=original_started_at,
    )


__all__ = [
    "ensure_state_table",
    "write_state_row",
    "read_last_watermark",
    "read_resumable_state",
    "ResumeContext",
    "WATERMARK_READ_SOFT_FAILED_MARKER",
    "_ensure_target_table_exists",
]
