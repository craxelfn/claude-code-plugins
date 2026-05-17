"""``fusion_bundle_state`` Delta-table contract.

Schema and per-step write logic for the state table that records every
orchestrator step's outcome. Single source of truth for the table's DDL +
the canonical INSERT shape.

Two-layer failure semantics (DECISION_state_table_failure_semantics.md):
  - ``ensure_state_table`` is HARD — failure halts the run before any
    module dispatch (high-probability structural problems like catalog
    typo, missing schema, DDL grant misconfig).
  - ``write_state_row`` is wrapped by ``runtime._safe_write_state_row``,
    which logs WARN and continues on per-step failures (transient flakes
    shouldn't kill a long medallion run).

``read_last_watermark`` is stubbed for α (returns None — incremental mode
is Phase β; seed mode never reads watermarks). Phase β implementation
will query the most-recent ``last_watermark`` for the given dataset_id.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths

if TYPE_CHECKING:  # pragma: no cover
    from datetime import datetime

    from pyspark.sql import SparkSession

    from .runtime import RunStep


# ---------------------------------------------------------------------------
# DDL — fusion_bundle_state schema
# ---------------------------------------------------------------------------

_STATE_TABLE_NAME = "fusion_bundle_state"


def _state_table_path(paths: TablePaths) -> str:
    """3-part path: ``{catalog}.{bronze_schema}.fusion_bundle_state``."""
    return paths.bronze(_STATE_TABLE_NAME)


def _ddl(table_path: str) -> str:
    """Mirrors §3.2 of PLAN_P1.5_orchestrator.md.

    Append-only. Each orchestrator step writes exactly one row.
    ``skip_reason`` is the structured B1.1 discriminator (nullable for
    non-skipped rows).
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
          duration_seconds DOUBLE       NOT NULL
        )
        USING DELTA
        PARTITIONED BY (layer)
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

    The probe writes a sentinel row with ``run_id='__ensure_probe__'``
    and ``status='probe'`` (NOT one of the four canonical values) so
    consumer queries that filter by canonical status never see it; the
    sentinel is deleted immediately after insertion.
    """
    table_path = _state_table_path(paths)
    spark.sql(_ddl(table_path))
    # Writeability probe — INSERT + DELETE sentinel.
    # Live-evidence fix (2026-05-17): every VALUES literal needs an explicit
    # CAST. Delta's strict type-merging refuses to coerce DECIMAL(2,1) → DOUBLE
    # on the `0.0` literal, and NULL needs a typed CAST for the nullable
    # columns. Unit tests with fake-Spark didn't catch this because they
    # accept any value; only the real Delta writer enforces the schema.
    spark.sql(
        f"""
        INSERT INTO {table_path}
          (run_id, dataset_id, layer, mode, last_watermark, last_run_at,
           status, row_count, error_message, skip_reason, duration_seconds)
        VALUES
          ('__ensure_probe__', '__probe__', 'bronze', 'seed',
           CAST(NULL AS TIMESTAMP), current_timestamp(), 'probe',
           CAST(NULL AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS STRING),
           CAST(0.0 AS DOUBLE))
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

    spark.sql(
        f"""
        INSERT INTO {table_path}
          (run_id, dataset_id, layer, mode, last_watermark, last_run_at,
           status, row_count, error_message, skip_reason, duration_seconds)
        VALUES
          ({_q(step.run_id)},
           {_q(step.dataset_id)},
           {_q(step.layer)},
           {_q(step.mode)},
           {_ts(step.watermark_used)},
           current_timestamp(),
           {_q(step.status)},
           {_bigint(step.row_count)},
           {_q(step.error_message)},
           {_q(step.skip_reason)},
           {_double(step.duration_seconds)})
        """
    )


def read_last_watermark(
    spark: "SparkSession", paths: TablePaths, dataset_id: str
) -> "datetime | None":
    """Phase β stub. Returns None in α (seed mode never reads watermarks).

    Phase β implementation:
        ``SELECT MAX(last_watermark) FROM fusion_bundle_state WHERE
        dataset_id = ? AND status = 'success'``
    """
    return None


__all__ = [
    "ensure_state_table",
    "write_state_row",
    "read_last_watermark",
]
