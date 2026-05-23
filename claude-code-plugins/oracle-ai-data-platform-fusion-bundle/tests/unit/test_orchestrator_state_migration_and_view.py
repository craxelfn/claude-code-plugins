"""Unit tests for the state-table migration + VIEW.

``ensure_state_table`` runs a schema-aware additive migration:
  1. ``CREATE TABLE IF NOT EXISTS`` with the full current schema.
  2. ``DESCRIBE TABLE`` to learn what columns are actually present
     (the table may have been created by an earlier plugin build).
  3. ``ALTER TABLE ... ADD COLUMNS (...)`` only for missing columns.
     Skipped entirely when every fix21 column already exists.
  4. ``CREATE OR REPLACE VIEW fusion_bundle_state_latest``
     — one-row-per-(run_id, dataset_id) projection. Idempotent.
  5. INSERT + DELETE sentinel probe — verifies writeability.

These tests inspect the SQL strings captured by a fake Spark to pin
the contract WITHOUT requiring a real Delta lake. Real-cluster
verification happens in TC26 / TC27 evidence.
"""

from __future__ import annotations

from typing import Iterable
from unittest.mock import MagicMock

from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
from oracle_ai_data_platform_fusion_bundle.orchestrator.state import (
    ensure_state_table,
)


# ---------------------------------------------------------------------------
# Fake Spark — captures sql_calls AND answers DESCRIBE TABLE
# ---------------------------------------------------------------------------


class _FakeRow:
    """pyspark.Row-shaped: supports both subscript and tuple access."""

    def __init__(self, col_name: str) -> None:
        self._data = {"col_name": col_name, "data_type": "string", "comment": None}

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self._data.values())[key]
        return self._data[key]


class _FakeDescribeFrame:
    def __init__(self, rows: Iterable[_FakeRow]) -> None:
        self._rows = list(rows)

    def collect(self) -> list[_FakeRow]:
        return self._rows


class _CapturingSpark:
    """Captures every spark.sql(...) call. DESCRIBE TABLE queries are
    routed to a canned column list so the migration can introspect
    the existing schema."""

    def __init__(self, existing_columns: list[str] | None = None) -> None:
        self.sql_calls: list[str] = []
        # Default = full fix21 schema present (no ALTER needed). Tests
        # for legacy-schema migration pass an older column list.
        if existing_columns is None:
            existing_columns = [
                "run_id", "dataset_id", "layer", "mode",
                "last_watermark", "last_run_at",
                "status", "row_count", "error_message", "skip_reason",
                "duration_seconds", "plan_hash", "plan_snapshot",
            ]
        self._existing_columns = list(existing_columns)

    def sql(self, query: str):
        self.sql_calls.append(query)
        if query.strip().startswith("DESCRIBE TABLE"):
            # DESCRIBE returns column rows + (sometimes) a `# Partitioning`
            # metadata row at the end. Mimic that shape so the parser
            # in _existing_state_columns gets exercised.
            rows = [_FakeRow(c) for c in self._existing_columns]
            rows.append(_FakeRow("# Partitioning"))
            rows.append(_FakeRow("layer"))  # partition column, ignored
            return _FakeDescribeFrame(rows)
        return MagicMock()


def _paths() -> TablePaths:
    return TablePaths(
        catalog="fusion_catalog",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
    )


_LEGACY_COLUMNS = [
    "run_id", "dataset_id", "layer", "mode",
    "last_watermark", "last_run_at",
    "status", "row_count", "error_message", "skip_reason",
    "duration_seconds",
    # No plan_hash, no plan_snapshot — table predates the migration.
]


# ---------------------------------------------------------------------------
# Schema migration — CREATE
# ---------------------------------------------------------------------------


def test_create_table_includes_plan_hash_and_plan_snapshot_columns() -> None:
    spark = _CapturingSpark()
    ensure_state_table(spark, _paths())
    create_sql = next(s for s in spark.sql_calls if "CREATE TABLE" in s)
    assert "plan_hash" in create_sql
    assert "plan_snapshot" in create_sql
    assert "STRING" in create_sql


# ---------------------------------------------------------------------------
# Schema migration — ALTER (the bug-fix surface)
# ---------------------------------------------------------------------------


def test_alter_adds_both_missing_columns_on_legacy_schema() -> None:
    """Legacy table (no plan_hash, no plan_snapshot) → ALTER must add
    BOTH columns with valid Spark SQL (no ``IF NOT EXISTS`` clause
    inside ADD COLUMNS, which the Spark parser would reject)."""
    spark = _CapturingSpark(existing_columns=_LEGACY_COLUMNS)
    ensure_state_table(spark, _paths())
    alter_calls = [s for s in spark.sql_calls if "ALTER TABLE" in s]
    assert len(alter_calls) == 1, (
        f"legacy schema must trigger exactly one ALTER; got {len(alter_calls)}"
    )
    alter_sql = alter_calls[0]
    assert "ADD COLUMNS" in alter_sql
    assert "plan_hash" in alter_sql
    assert "plan_snapshot" in alter_sql


def test_alter_does_not_use_if_not_exists_clause() -> None:
    """Regression for the parser bug: ``ALTER TABLE ... ADD COLUMNS
    IF NOT EXISTS (...)`` is invalid in both vanilla Spark SQL and
    Databricks Delta. Schema-aware migration must skip-or-include
    columns at the Python level, NOT delegate to an unsupported
    SQL modifier.
    """
    spark = _CapturingSpark(existing_columns=_LEGACY_COLUMNS)
    ensure_state_table(spark, _paths())
    alter_sql = next(s for s in spark.sql_calls if "ALTER TABLE" in s)
    assert "IF NOT EXISTS" not in alter_sql, (
        "ALTER TABLE ... ADD COLUMNS IF NOT EXISTS (...) is not valid "
        "Spark SQL. The migration must introspect existing columns "
        "and ADD only the missing ones."
    )


def test_alter_skipped_when_columns_already_exist() -> None:
    """A table created by fix21+ already has both columns. The
    migration must NOT emit an ALTER in that case — running ALTER
    against present columns would either fail (in older Spark) or
    waste a metadata write."""
    spark = _CapturingSpark()  # default fixture has both columns
    ensure_state_table(spark, _paths())
    alter_calls = [s for s in spark.sql_calls if "ALTER TABLE" in s]
    assert alter_calls == [], (
        f"ALTER must be skipped when fix21 columns already exist; "
        f"got {alter_calls}"
    )


def test_alter_adds_only_missing_column_when_partial_migration() -> None:
    """A partial migration (one column added by hand, the other
    missing) must result in an ALTER that names ONLY the missing
    column. Pins the schema-aware behavior."""
    partially_migrated = _LEGACY_COLUMNS + ["plan_hash"]  # snapshot still missing
    spark = _CapturingSpark(existing_columns=partially_migrated)
    ensure_state_table(spark, _paths())
    alter_sql = next(s for s in spark.sql_calls if "ALTER TABLE" in s)
    assert "plan_snapshot" in alter_sql
    assert "plan_hash" not in alter_sql, (
        "ALTER must not re-add plan_hash when it already exists"
    )


def test_alter_runs_after_describe_after_create() -> None:
    """Ordering invariant: CREATE → DESCRIBE → (optional) ALTER →
    VIEW. The DESCRIBE must follow CREATE (DESCRIBE on a missing
    table raises), and ALTER must follow DESCRIBE (we need the
    column list to decide what to add).
    """
    spark = _CapturingSpark(existing_columns=_LEGACY_COLUMNS)
    ensure_state_table(spark, _paths())
    create_idx = next(i for i, s in enumerate(spark.sql_calls) if "CREATE TABLE" in s)
    describe_idx = next(i for i, s in enumerate(spark.sql_calls) if s.strip().startswith("DESCRIBE TABLE"))
    alter_idx = next(i for i, s in enumerate(spark.sql_calls) if "ALTER TABLE" in s)
    assert create_idx < describe_idx < alter_idx


def test_alter_targets_three_part_table_path() -> None:
    spark = _CapturingSpark(existing_columns=_LEGACY_COLUMNS)
    paths = TablePaths(
        catalog="cust_cat", bronze_schema="cust_b",
        silver_schema="s", gold_schema="g",
    )
    ensure_state_table(spark, paths)
    alter_sql = next(s for s in spark.sql_calls if "ALTER TABLE" in s)
    # Three-part: catalog.bronze_schema.fusion_bundle_state
    assert "cust_cat.cust_b.fusion_bundle_state" in alter_sql


# ---------------------------------------------------------------------------
# Latest VIEW
# ---------------------------------------------------------------------------


def test_create_view_emits_after_create_table() -> None:
    spark = _CapturingSpark()
    ensure_state_table(spark, _paths())
    view_sql = next(
        s for s in spark.sql_calls
        if "CREATE OR REPLACE VIEW" in s and "fusion_bundle_state_latest" in s
    )
    assert "ROW_NUMBER() OVER" in view_sql
    assert "PARTITION BY run_id, dataset_id" in view_sql
    assert "ORDER BY last_run_at DESC" in view_sql
    assert "WHERE rn = 1" in view_sql


def test_view_projects_all_state_columns() -> None:
    """The view should project every column of the state table —
    including ``plan_hash`` and ``plan_snapshot`` so resume diagnostics
    can use the view as a drop-in replacement."""
    spark = _CapturingSpark()
    ensure_state_table(spark, _paths())
    view_sql = next(s for s in spark.sql_calls if "CREATE OR REPLACE VIEW" in s)
    for col in (
        "run_id", "dataset_id", "layer", "mode",
        "last_watermark", "last_run_at",
        "status", "row_count", "error_message", "skip_reason",
        "duration_seconds", "plan_hash", "plan_snapshot",
    ):
        assert col in view_sql, f"VIEW must project {col!r}"


def test_view_is_idempotent_create_or_replace() -> None:
    """CREATE OR REPLACE — re-running ensure_state_table updates the
    view definition in place rather than failing."""
    spark = _CapturingSpark()
    ensure_state_table(spark, _paths())
    view_sql = next(s for s in spark.sql_calls if "CREATE OR REPLACE VIEW" in s)
    assert "CREATE OR REPLACE VIEW" in view_sql


# ---------------------------------------------------------------------------
# Writeability sentinel
# ---------------------------------------------------------------------------


def test_sentinel_insert_includes_new_columns_as_null_cast() -> None:
    """The writeability sentinel must include plan_hash + plan_snapshot
    in the column list, with typed NULL casts (Delta refuses bare NULL
    on schema-strict writes)."""
    spark = _CapturingSpark()
    ensure_state_table(spark, _paths())
    insert_sql = next(s for s in spark.sql_calls if "INSERT INTO" in s and "__ensure_probe__" in s)
    assert "plan_hash" in insert_sql
    assert "plan_snapshot" in insert_sql
    # NULL cast pattern from state.py:_q.
    assert "CAST(NULL AS STRING)" in insert_sql


def test_sentinel_is_deleted_after_insert() -> None:
    spark = _CapturingSpark()
    ensure_state_table(spark, _paths())
    delete_sql = next(
        s for s in spark.sql_calls
        if "DELETE FROM" in s and "__ensure_probe__" in s
    )
    assert delete_sql is not None
