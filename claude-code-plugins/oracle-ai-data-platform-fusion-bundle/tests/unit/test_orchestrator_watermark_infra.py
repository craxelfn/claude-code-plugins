"""Unit tests for P1.5β.1 watermark infrastructure.

Covers Stage D test contract from
``docs/features/p1.5b-orchestrator-incremental/plan.md``:

- D1: ``read_last_watermark`` semantics (empty / missing / picker /
  failed-or-deferred interleaving / NULL / tie-break / tz normalization /
  apostrophe-bearing dataset_id / soft-fail with WARN marker).
- D6: ``_resolve_watermark_source`` for every shipped + synthetic spec.
- D7: ``orchestrator.run(..., mode="incremental")`` still raises
  ``NotImplementedError`` — the gate is preserved coupling test for
  P1.17.
- D8: name-collision regression (``resolved_password`` SecretStr vs
  ``prior_watermark`` datetime).
- D9: upper-bound + gap invariants on a captured bronze cursor.
- D10: ``_extract_ts`` is a deterministic literal (NOT
  ``F.current_timestamp()``).
- D-resume: tuple-keyed ``succeeded_row_counts`` /
  ``succeeded_last_watermarks`` carry-forward.

Integration paths exercise ``_execute_node`` directly while bypassing
the user-facing ``NotImplementedError`` gate (the gate stays in this PR;
the test scope is the underlying infrastructure).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from oracle_ai_data_platform_fusion_bundle import orchestrator
from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
from oracle_ai_data_platform_fusion_bundle.orchestrator import registry, state
from oracle_ai_data_platform_fusion_bundle.orchestrator.errors import (
    MultipleUpstreamWatermarkError,
    OrchestratorRuntimeError,
    WatermarkMonotonicityError,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.registry import (
    BRONZE_EXTRACTS,
    GOLD_MARTS,
    SILVER_DIMS,
    BronzeExtractSpec,
    DeferredSpec,
    GoldMartSpec,
    SilverDimSpec,
    _resolve_watermark_source,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import (
    WATERMARK_SAFETY_WINDOW,
    RunStep,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.state import (
    WATERMARK_READ_SOFT_FAILED_MARKER,
    ResumeContext,
    _normalize_to_utc,
    read_last_watermark,
)


# ---------------------------------------------------------------------------
# Fixtures — minimal in-memory state-row store + FakeSpark
# ---------------------------------------------------------------------------


_TEST_PATHS = TablePaths(
    catalog="fusion_catalog",
    bronze_schema="bronze",
    silver_schema="silver",
    gold_schema="gold",
)


class _FakeRow:
    """Row-shaped object — attribute + subscript access."""

    def __init__(self, **kwargs: Any) -> None:
        self._data = kwargs

    def __getitem__(self, key: str) -> Any:
        return self._data.get(key)

    def __getattr__(self, key: str) -> Any:
        if key in self._data:
            return self._data[key]
        raise AttributeError(key)


class _DfFromRows:
    """DataFrame-shaped object that ``.collect()``s a fixed row list."""

    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows

    def collect(self) -> list[_FakeRow]:
        return self._rows


class _StateOnlySpark:
    """Minimal Spark whose ``sql(...)`` returns canned state-table rows.

    Used for ``read_last_watermark`` tests. The matcher is intentionally
    permissive — any query that mentions ``last_watermark`` + the state
    table is routed to the canned rows; everything else returns empty.
    Supports per-call selectors so different queries (read vs resume)
    can return different row sets.
    """

    def __init__(
        self,
        rows: list[_FakeRow] | None = None,
        *,
        raise_on_match: Exception | None = None,
    ) -> None:
        self.rows = rows or []
        self.sql_calls: list[str] = []
        self._raise_on_match = raise_on_match

    def sql(self, query: str) -> _DfFromRows:
        self.sql_calls.append(query)
        if self._raise_on_match is not None and "last_watermark" in query:
            raise self._raise_on_match
        # Apply the WHERE filters in-memory so the picker / tie-break
        # tests can assert deterministic ordering against the same fake.
        if (
            "SELECT last_watermark" in query
            and "fusion_bundle_state" in query
        ):
            return _DfFromRows(self._apply_read_last_watermark_query(query))
        return _DfFromRows([])

    def _apply_read_last_watermark_query(self, query: str) -> list[_FakeRow]:
        # Parse out dataset_id / layer filter values from the query
        # string. The query format is fixed by ``read_last_watermark``
        # so the parsing is deterministic; brittle by design — if the
        # SQL shape changes the test surface is reviewed.
        import re

        m_ds = re.search(r"dataset_id\s*=\s*'((?:[^']|'')*)'", query)
        m_layer = re.search(r"layer\s*=\s*'((?:[^']|'')*)'", query)
        if not m_ds or not m_layer:
            return []
        ds = m_ds.group(1).replace("''", "'")
        layer = m_layer.group(1).replace("''", "'")
        matching = [
            r for r in self.rows
            if r["dataset_id"] == ds
            and r["layer"] == layer
            and r["status"] == "success"
        ]
        # Sort: last_run_at DESC, last_watermark DESC NULLS LAST
        def _key(r: _FakeRow) -> tuple:
            lwm = r["last_watermark"]
            # NULLS LAST → use a tuple where NULL sorts as the smallest
            return (r["last_run_at"], lwm is not None, lwm)

        matching.sort(key=_key, reverse=True)
        return matching[:1]


# ---------------------------------------------------------------------------
# D6: Resolver — pure-function over the spec
# ---------------------------------------------------------------------------


class TestResolveWatermarkSource:
    def test_bronze_reads_itself(self) -> None:
        spec = BronzeExtractSpec("ap_invoices", "ap_invoices")
        assert _resolve_watermark_source(spec) == ("ap_invoices", "bronze")

    def test_silver_dim_supplier_reads_erp_suppliers(self) -> None:
        assert _resolve_watermark_source(SILVER_DIMS["dim_supplier"]) == (
            "erp_suppliers",
            "bronze",
        )

    def test_silver_dim_account_reads_gl_coa(self) -> None:
        assert _resolve_watermark_source(SILVER_DIMS["dim_account"]) == (
            "gl_coa",
            "bronze",
        )

    def test_silver_dim_calendar_no_upstream(self) -> None:
        assert _resolve_watermark_source(SILVER_DIMS["dim_calendar"]) is None

    def test_gold_ap_aging_reads_ap_invoices(self) -> None:
        assert _resolve_watermark_source(GOLD_MARTS["ap_aging"]) == (
            "ap_invoices",
            "bronze",
        )

    def test_gold_gl_balance_reads_gl_period_balances(self) -> None:
        assert _resolve_watermark_source(GOLD_MARTS["gl_balance"]) == (
            "gl_period_balances",
            "bronze",
        )

    def test_gold_supplier_spend_reads_ap_invoices(self) -> None:
        assert _resolve_watermark_source(GOLD_MARTS["supplier_spend"]) == (
            "ap_invoices",
            "bronze",
        )

    def test_multi_upstream_gold_raises(self) -> None:
        spec = GoldMartSpec(
            "synthetic_multi",
            builder=lambda *a, **k: None,
            depends_on_bronze=("ap_invoices", "gl_coa"),
            depends_on_silver=(),
        )
        with pytest.raises(MultipleUpstreamWatermarkError, match="2 bronze upstreams"):
            _resolve_watermark_source(spec)

    def test_multi_upstream_silver_raises(self) -> None:
        spec = SilverDimSpec(
            "synthetic_dim",
            builder=lambda *a, **k: None,
            depends_on_bronze=("a", "b", "c"),
        )
        with pytest.raises(MultipleUpstreamWatermarkError, match="3 bronze upstreams"):
            _resolve_watermark_source(spec)

    def test_deferred_returns_none(self) -> None:
        spec = DeferredSpec("future_thing", layer="silver", reason="P1.42")
        assert _resolve_watermark_source(spec) is None

    def test_unknown_type_raises_typeerror(self) -> None:
        with pytest.raises(TypeError, match="unknown spec type"):
            _resolve_watermark_source(object())

    def test_multiple_upstream_error_is_runtime_error(self) -> None:
        assert issubclass(MultipleUpstreamWatermarkError, OrchestratorRuntimeError)


# ---------------------------------------------------------------------------
# D1: read_last_watermark — all read-path cases
# ---------------------------------------------------------------------------


class TestReadLastWatermark:
    def test_empty_table_returns_none(self) -> None:
        spark = _StateOnlySpark(rows=[])
        assert read_last_watermark(spark, _TEST_PATHS, "ap_invoices") is None

    def test_missing_pair_returns_none(self) -> None:
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id="erp_suppliers", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 21, 12, tzinfo=timezone.utc),
                last_run_at=datetime(2026, 5, 21, 12, tzinfo=timezone.utc),
            ),
        ])
        assert read_last_watermark(spark, _TEST_PATHS, "ap_invoices") is None

    def test_picks_most_recent_success(self) -> None:
        # Two successes for the same pair — most recent by last_run_at wins.
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 20, tzinfo=timezone.utc),
                last_run_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
            ),
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 22, tzinfo=timezone.utc),
                last_run_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
            ),
        ])
        assert read_last_watermark(spark, _TEST_PATHS, "ap_invoices") == datetime(
            2026, 5, 22, tzinfo=timezone.utc
        )

    def test_failed_row_between_successes_does_not_win(self) -> None:
        # Most recent attempt was a failure with NULL watermark; the
        # function must walk back to the prior success row.
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 20, tzinfo=timezone.utc),
                last_run_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
            ),
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="failed",
                last_watermark=None,
                last_run_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
            ),
        ])
        # The picker filters status='success', so the failed row is ignored.
        assert read_last_watermark(spark, _TEST_PATHS, "ap_invoices") == datetime(
            2026, 5, 20, tzinfo=timezone.utc
        )

    def test_deferred_row_between_successes_does_not_win(self) -> None:
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 20, tzinfo=timezone.utc),
                last_run_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
            ),
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="deferred",
                last_watermark=None,
                last_run_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
            ),
        ])
        assert read_last_watermark(spark, _TEST_PATHS, "ap_invoices") == datetime(
            2026, 5, 20, tzinfo=timezone.utc
        )

    def test_null_watermark_on_most_recent_success_returns_none(self) -> None:
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=None,
                last_run_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
            ),
        ])
        assert read_last_watermark(spark, _TEST_PATHS, "ap_invoices") is None

    def test_tie_break_higher_watermark_wins(self) -> None:
        # Two successes with IDENTICAL last_run_at; secondary key
        # `last_watermark DESC NULLS LAST` must pick the higher.
        same_time = datetime(2026, 5, 22, tzinfo=timezone.utc)
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 20, tzinfo=timezone.utc),
                last_run_at=same_time,
            ),
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 21, tzinfo=timezone.utc),
                last_run_at=same_time,
            ),
        ])
        assert read_last_watermark(spark, _TEST_PATHS, "ap_invoices") == datetime(
            2026, 5, 21, tzinfo=timezone.utc
        )

    def test_tie_break_null_loses_to_non_null(self) -> None:
        same_time = datetime(2026, 5, 22, tzinfo=timezone.utc)
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=None,
                last_run_at=same_time,
            ),
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 21, tzinfo=timezone.utc),
                last_run_at=same_time,
            ),
        ])
        assert read_last_watermark(spark, _TEST_PATHS, "ap_invoices") == datetime(
            2026, 5, 21, tzinfo=timezone.utc
        )

    def test_naive_datetime_normalized_to_utc(self) -> None:
        # Simulates the Spark session that returns naive datetimes.
        # The write path always persists UTC, so "naive → UTC" is correct.
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 22, 12, 30),  # naive
                last_run_at=datetime(2026, 5, 22, 12, 30),
            ),
        ])
        got = read_last_watermark(spark, _TEST_PATHS, "ap_invoices")
        assert got is not None
        assert got.tzinfo == timezone.utc
        assert got == datetime(2026, 5, 22, 12, 30, tzinfo=timezone.utc)

    def test_aware_non_utc_normalized_to_utc(self) -> None:
        # Simulates a session that stamps America/New_York. Same instant,
        # different wall clock; the read must hand back UTC-aware.
        ny = ZoneInfo("America/New_York")
        ny_ts = datetime(2026, 5, 22, 8, 30, tzinfo=ny)  # 12:30 UTC
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id="ap_invoices", layer="bronze", status="success",
                last_watermark=ny_ts,
                last_run_at=ny_ts,
            ),
        ])
        got = read_last_watermark(spark, _TEST_PATHS, "ap_invoices")
        assert got is not None
        assert got.tzinfo == timezone.utc
        assert got == ny_ts.astimezone(timezone.utc)

    def test_apostrophe_bearing_dataset_id(self) -> None:
        # SQL injection / escaping regression test — dataset_id can
        # contain apostrophes after registry widening. The escaping
        # helper must double them so the WHERE clause matches.
        evil = "my'evil'id"
        w1 = datetime(2026, 5, 22, tzinfo=timezone.utc)
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id=evil, layer="bronze", status="success",
                last_watermark=w1, last_run_at=w1,
            ),
            # A different apostrophe-bearing id MUST NOT match.
            _FakeRow(
                dataset_id="other'id", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 23, tzinfo=timezone.utc),
                last_run_at=datetime(2026, 5, 23, tzinfo=timezone.utc),
            ),
        ])
        got = read_last_watermark(spark, _TEST_PATHS, evil)
        assert got == w1
        # And it does NOT over-match the other apostrophe id.
        assert read_last_watermark(spark, _TEST_PATHS, "other'id") == datetime(
            2026, 5, 23, tzinfo=timezone.utc
        )

    def test_soft_fail_returns_none_with_warn_marker(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        spark = _StateOnlySpark(
            rows=[],
            raise_on_match=RuntimeError("simulated Spark/metastore failure"),
        )
        with caplog.at_level(logging.WARNING):
            got = read_last_watermark(spark, _TEST_PATHS, "ap_invoices")
        assert got is None
        # Exactly one structured WARN with the stable marker.
        marker_logs = [
            r for r in caplog.records
            if WATERMARK_READ_SOFT_FAILED_MARKER in r.getMessage()
        ]
        assert len(marker_logs) == 1, (
            f"expected exactly one WARN carrying "
            f"{WATERMARK_READ_SOFT_FAILED_MARKER!r}; got {len(marker_logs)}"
        )
        msg = marker_logs[0].getMessage()
        assert "'ap_invoices'" in msg
        assert "'bronze'" in msg
        assert "simulated Spark/metastore failure" in msg

    def test_layer_kwarg_selects_silver(self) -> None:
        spark = _StateOnlySpark(rows=[
            _FakeRow(
                dataset_id="dim_supplier", layer="bronze", status="success",
                last_watermark=datetime(2026, 5, 20, tzinfo=timezone.utc),
                last_run_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
            ),
            _FakeRow(
                dataset_id="dim_supplier", layer="silver", status="success",
                last_watermark=None,
                last_run_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
            ),
        ])
        # Bronze row has a watermark; silver row has NULL.
        assert read_last_watermark(spark, _TEST_PATHS, "dim_supplier", "bronze") == datetime(
            2026, 5, 20, tzinfo=timezone.utc
        )
        assert read_last_watermark(spark, _TEST_PATHS, "dim_supplier", "silver") is None


# ---------------------------------------------------------------------------
# _normalize_to_utc — utility coverage
# ---------------------------------------------------------------------------


class TestNormalizeToUtc:
    def test_none_passes_through(self) -> None:
        assert _normalize_to_utc(None) is None

    def test_naive_assumed_utc(self) -> None:
        got = _normalize_to_utc(datetime(2026, 1, 1, 12, 0))
        assert got == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    def test_aware_utc_unchanged(self) -> None:
        d = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        assert _normalize_to_utc(d) == d

    def test_aware_non_utc_reprojected(self) -> None:
        ny = ZoneInfo("America/New_York")
        d = datetime(2026, 1, 1, 8, 0, tzinfo=ny)  # 13:00 UTC (EST)
        got = _normalize_to_utc(d)
        assert got.tzinfo == timezone.utc
        assert got == d.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# D7: Gate-preserved test — surface MUST still raise NotImplementedError
# ---------------------------------------------------------------------------


_MIN_BUNDLE = """
apiVersion: aidp-fusion-bundle/v1
project: test-incremental-gate
fusion:
  serviceUrl: https://example.com
  username: alice@oracle
  password: literal-password
  externalStorage: oci://bucket@ns/path
datasets:
  - id: ap_invoices
    mode: full
dimensions:
  build: []
gold:
  marts: []
"""


# ---------------------------------------------------------------------------
# D-resume: tuple-keyed succeeded_row_counts + succeeded_last_watermarks
# ---------------------------------------------------------------------------


class _ResumeFakeSpark:
    """FakeSpark that routes the three known state-table queries by
    string-pattern matching. Returns DIFFERENT row subsets per query
    so the cross-layer test can seed independent values per pair.
    """

    def __init__(self, *, main_rows: list[_FakeRow], rc_rows: list[_FakeRow], lw_rows: list[_FakeRow]) -> None:
        self.main_rows = main_rows
        self.rc_rows = rc_rows
        self.lw_rows = lw_rows
        self.sql_calls: list[str] = []

    def sql(self, query: str) -> _DfFromRows:
        self.sql_calls.append(query)
        if "row_count IS NOT NULL" in query:
            return _DfFromRows(self.rc_rows)
        if "SELECT dataset_id, layer, last_watermark FROM ranked" in query:
            return _DfFromRows(self.lw_rows)
        if "fusion_bundle_state" in query and "ranked" in query:
            return _DfFromRows(self.main_rows)
        return _DfFromRows([])


def _wp_row(
    *,
    ds: str,
    layer: str,
    status: str = "success",
    last_watermark: datetime | None = None,
    row_count: int | None = None,
    plan_hash: str | None = "h",
    plan_snapshot: str | None = '{"nodes": []}',
    last_run_at: datetime | None = None,
) -> _FakeRow:
    return _FakeRow(
        dataset_id=ds,
        layer=layer,
        status=status,
        last_watermark=last_watermark,
        row_count=row_count,
        plan_hash=plan_hash,
        plan_snapshot=plan_snapshot,
        last_run_at=last_run_at or datetime(2026, 5, 22, 12, tzinfo=timezone.utc),
    )


class TestResumeContextTupleKeys:
    """D-resume — ResumeContext carries tuple-keyed
    ``succeeded_row_counts`` and the new ``succeeded_last_watermarks``;
    both map ``(dataset_id, layer)`` → carry-forward value.
    """

    def test_succeeded_watermarks_keyed_by_dataset_and_layer(self) -> None:
        W1 = datetime(2026, 5, 22, 11, tzinfo=timezone.utc)
        main = [
            _wp_row(ds="ap_invoices", layer="bronze", last_watermark=W1, row_count=42),
        ]
        rc = [
            _FakeRow(dataset_id="ap_invoices", layer="bronze", row_count=42),
        ]
        lw = [
            _FakeRow(dataset_id="ap_invoices", layer="bronze", last_watermark=W1),
        ]
        spark = _ResumeFakeSpark(main_rows=main, rc_rows=rc, lw_rows=lw)
        ctx = state.read_resumable_state(spark, _TEST_PATHS, "run-A")
        assert ctx.succeeded_last_watermarks[("ap_invoices", "bronze")] == W1
        assert ctx.succeeded_row_counts[("ap_invoices", "bronze")] == 42

    def test_cross_layer_key_collision_keeps_both(self) -> None:
        """C0d motivation: a synthetic registry that reuses
        ``dataset_id`` across layers must not collapse the two
        per-layer carry-forwards into one dict entry. Bronze
        ``last_watermark`` is preserved; silver's NULL is preserved
        too.
        """
        W_bronze = datetime(2026, 5, 22, 9, tzinfo=timezone.utc)
        main = [
            _wp_row(ds="foo", layer="bronze", last_watermark=W_bronze, row_count=11),
            _wp_row(ds="foo", layer="silver", last_watermark=None, row_count=99),
        ]
        rc = [
            _FakeRow(dataset_id="foo", layer="bronze", row_count=11),
            _FakeRow(dataset_id="foo", layer="silver", row_count=99),
        ]
        lw = [
            _FakeRow(dataset_id="foo", layer="bronze", last_watermark=W_bronze),
            _FakeRow(dataset_id="foo", layer="silver", last_watermark=None),
        ]
        spark = _ResumeFakeSpark(main_rows=main, rc_rows=rc, lw_rows=lw)
        ctx = state.read_resumable_state(spark, _TEST_PATHS, "run-B")
        assert ctx.succeeded_last_watermarks[("foo", "bronze")] == W_bronze
        assert ctx.succeeded_last_watermarks[("foo", "silver")] is None
        assert ctx.succeeded_row_counts[("foo", "bronze")] == 11
        assert ctx.succeeded_row_counts[("foo", "silver")] == 99

    def test_resumed_skip_factory_threads_last_watermark(self) -> None:
        W1 = datetime(2026, 5, 22, 12, tzinfo=timezone.utc)
        spec = BronzeExtractSpec("ap_invoices", "ap_invoices")
        step = RunStep.resumed_skip(
            spec, run_id="r-1", mode="seed",
            row_count=99, last_watermark=W1,
        )
        assert step.status == "resumed_skipped"
        assert step.row_count == 99
        assert step.last_watermark == W1
        assert step.watermark_used is None  # input audit unused on resume

    def test_resumed_skip_default_last_watermark_is_none(self) -> None:
        spec = BronzeExtractSpec("ap_invoices", "ap_invoices")
        step = RunStep.resumed_skip(spec, run_id="r-1", mode="seed")
        assert step.last_watermark is None


# ---------------------------------------------------------------------------
# D8: name-collision regression — resolved_password vs prior_watermark
# ---------------------------------------------------------------------------


class TestNameCollisionRegression:
    """If anyone reuses the ``resolved`` local name across the
    credential and watermark paths in ``_execute_node``, the step
    would either persist a SecretStr where a datetime should go, or
    leak the unwrapped credential into debug output. The bronze
    closure dispatch test below pins the contract.
    """

    def test_success_step_carries_datetime_in_last_watermark_not_secret(self) -> None:
        # Direct RunStep.success — verifies the field is typed by the
        # caller, not silently coerced.
        W1 = datetime(2026, 5, 22, 12, tzinfo=timezone.utc)
        spec = BronzeExtractSpec("ap_invoices", "ap_invoices")
        step = RunStep.success(
            spec, run_id="r-1", mode="seed",
            row_count=5,
            duration_seconds=0.1,
            watermark_used=None,
            last_watermark=W1,
        )
        assert step.last_watermark == W1
        # No SecretStr-y bytes in the repr.
        assert "SecretStr" not in repr(step)
        assert "sentinel-secret" not in repr(step)


# ---------------------------------------------------------------------------
# D9 / D10 / D2-D5 — bronze closure capture, monotonicity, empty-delta,
# deterministic _extract_ts. These exercise ``_execute_node`` directly
# (the user-facing gate at ``mode='incremental'`` stays in place; the
# tests pass ``mode='seed'`` because resolver + capture + check all run
# in seed mode too per the plan).
# ---------------------------------------------------------------------------


class _ExecuteNodeFakeSpark:
    """Drives a single ``_execute_node`` call. Returns canned rows
    for ``read_last_watermark``; ``table(target).count()`` returns
    a configurable row count to exercise the empty-delta + advancing
    paths.
    """

    def __init__(
        self,
        *,
        prior_rows: list[_FakeRow] | None = None,
        table_count: int = 5,
    ) -> None:
        self.prior_rows = prior_rows or []
        self.table_count = table_count
        self.sql_calls: list[str] = []

    def sql(self, query: str) -> _DfFromRows:
        self.sql_calls.append(query)
        if "SELECT last_watermark" in query and "fusion_bundle_state" in query:
            return _DfFromRows(self._filter_prior(query))
        return _DfFromRows([])

    def _filter_prior(self, query: str) -> list[_FakeRow]:
        import re
        m_ds = re.search(r"dataset_id\s*=\s*'((?:[^']|'')*)'", query)
        m_layer = re.search(r"layer\s*=\s*'((?:[^']|'')*)'", query)
        if not m_ds or not m_layer:
            return []
        ds = m_ds.group(1).replace("''", "'")
        layer = m_layer.group(1).replace("''", "'")
        matching = [
            r for r in self.prior_rows
            if r["dataset_id"] == ds
            and r["layer"] == layer
            and r["status"] == "success"
        ]
        def _key(r):
            lwm = r["last_watermark"]
            return (r["last_run_at"], lwm is not None, lwm)
        matching.sort(key=_key, reverse=True)
        return matching[:1]

    def table(self, name: str) -> Any:
        m = MagicMock()
        m.count.return_value = self.table_count
        return m


class _DfStub:
    """Lazy DataFrame stub for extract_pvo + enrich_bronze_audit_cols.
    Captures the kwargs passed to enrich + write.

    P1.17 (C4) added cache / unpersist / count / schema usage in the
    bronze closure; the stub honors those calls as no-ops so tests
    exercising the seed-mode path don't break on the cache lifecycle.
    """

    def __init__(self, count: int = 5) -> None:
        self.enrich_kwargs: dict[str, Any] | None = None
        self._count = count
        self.write = MagicMock()
        self.write.format.return_value = self.write
        self.write.mode.return_value = self.write
        self.write.option.return_value = self.write
        self.write.saveAsTable.return_value = None
        # Schema with at least one field so _ensure_target_table_exists
        # can render CREATE TABLE (incremental + fresh-tenant path).
        self.schema = MagicMock()
        _field = MagicMock()
        _field.name = "_extract_ts"
        _field.dataType.simpleString = lambda: "timestamp"
        self.schema.fields = [_field]

    def withColumn(self, *a, **kw):
        return self

    def cache(self):
        return self

    def unpersist(self) -> None:
        return None

    def count(self) -> int:
        return self._count

    def createOrReplaceTempView(self, _name: str) -> None:
        return None


def _build_bundle_for_dispatch(tmp_path: Path) -> Path:
    """Single-bronze bundle for ``_execute_node`` dispatch tests."""
    bundle_yaml = """
apiVersion: aidp-fusion-bundle/v1
project: test-watermark-capture
fusion:
  serviceUrl: https://example.com
  username: alice@oracle
  password: literal-password
  externalStorage: oci://bucket@ns/path
datasets:
  - id: ap_invoices
    mode: full
dimensions:
  build: []
gold:
  marts: []
"""
    p = tmp_path / "bundle.yaml"
    p.write_text(bundle_yaml, encoding="utf-8")
    return p


def _execute_bronze(
    *,
    spark: _ExecuteNodeFakeSpark,
    fake_now: datetime,
    extract_returns: _DfStub | None = None,
) -> RunStep:
    """Invoke ``_execute_node`` on the shipped ap_invoices bronze spec
    with the orchestrator wall clock patched to ``fake_now``.
    """
    from oracle_ai_data_platform_fusion_bundle.orchestrator import _execute_node
    from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle

    bundle = Bundle.model_validate({
        "apiVersion": "aidp-fusion-bundle/v1",
        "project": "test",
        "fusion": {
            "serviceUrl": "https://example.com",
            "username": "alice@oracle",
            "password": "literal-password",
            "externalStorage": "oci://bucket@ns/path",
        },
        "datasets": [{"id": "ap_invoices", "mode": "full"}],
    })
    df = extract_returns or _DfStub()

    captured: dict[str, Any] = {}

    def fake_enrich(df, **kwargs):
        captured["enrich_kwargs"] = kwargs
        return df

    with patch(
        "oracle_ai_data_platform_fusion_bundle.extractors.bicc.extract_pvo",
        return_value=df,
    ), patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.enrich_bronze_audit_cols",
        side_effect=fake_enrich,
    ), patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.datetime",
    ) as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        node = BRONZE_EXTRACTS["ap_invoices"]
        step = _execute_node(
            node,
            spark=spark,  # type: ignore[arg-type]
            paths=_TEST_PATHS,
            bundle=bundle,
            run_id="run-test",
            mode="seed",
            effective_schemas={"ap_invoices": "Financial"},
            plan_hash="h",
            plan_snapshot="{}",
        )
    return step, captured


class TestBronzeWatermarkCapture:
    def test_d9_first_run_persists_windowed_cursor_and_audit_literal(
        self, tmp_path: Path
    ) -> None:
        """D9 advancing-run invariants. No prior state row →
        ``last_watermark == fake_now - WATERMARK_SAFETY_WINDOW``.
        Audit-column literal == ``fake_now`` (un-windowed).
        Gap == WATERMARK_SAFETY_WINDOW exactly.
        """
        fake_now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        spark = _ExecuteNodeFakeSpark(prior_rows=[], table_count=5)
        step, captured = _execute_bronze(spark=spark, fake_now=fake_now)
        assert step.status == "success"
        assert step.last_watermark == fake_now - WATERMARK_SAFETY_WINDOW
        # D10: _extract_ts kwarg is the literal fake_now (NOT
        # current_timestamp()).
        assert captured["enrich_kwargs"]["extract_ts"] == fake_now
        # Gap invariant: extract_ts == last_watermark + WATERMARK_SAFETY_WINDOW.
        assert (
            captured["enrich_kwargs"]["extract_ts"] - step.last_watermark
            == WATERMARK_SAFETY_WINDOW
        )

    def test_d2_second_run_advances_watermark(self, tmp_path: Path) -> None:
        """First run leaves W1; second run captures W2 > W1."""
        fake_run2 = datetime(2026, 5, 22, 13, 0, 0, tzinfo=timezone.utc)
        W1 = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        spark = _ExecuteNodeFakeSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze", status="success",
                    last_watermark=W1, last_run_at=W1,
                ),
            ],
            table_count=5,
        )
        step, _ = _execute_bronze(spark=spark, fake_now=fake_run2)
        assert step.status == "success"
        assert step.last_watermark is not None
        assert step.last_watermark > W1
        assert step.last_watermark == fake_run2 - WATERMARK_SAFETY_WINDOW
        # The prior watermark lands on watermark_used (in-memory audit).
        assert step.watermark_used == W1

    def test_d5a_empty_delta_preserves_prior_watermark(self, tmp_path: Path) -> None:
        """Successful run with row_count=0 must NOT regress to NULL —
        the prior W1 is preserved. ``new_wm`` is a datetime, not a
        SecretStr (D8 collision-regression check).

        P1.17 (C4) — the empty-delta gate is now on **source** count
        (``df.count()``), not target count, so we inject a zero-count
        ``_DfStub`` rather than tweaking ``table_count``. Under MERGE
        semantics, ``spark.table(target).count()`` returns existing
        rows + applied delta (potentially non-zero on an empty delta
        against a non-empty target); the source-count gate is the only
        safe signal for "did anything arrive this cycle".
        """
        fake_now = datetime(2026, 5, 22, 13, 0, 0, tzinfo=timezone.utc)
        W1 = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        spark = _ExecuteNodeFakeSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze", status="success",
                    last_watermark=W1, last_run_at=W1,
                ),
            ],
            table_count=0,
        )
        step, _ = _execute_bronze(
            spark=spark, fake_now=fake_now, extract_returns=_DfStub(count=0),
        )
        assert step.status == "success"
        assert step.row_count == 0
        assert step.last_watermark == W1, "empty delta must preserve W1"
        # D8: type is datetime, not SecretStr.
        assert isinstance(step.last_watermark, datetime)

    def test_d5b_true_first_empty_persists_null(self, tmp_path: Path) -> None:
        """No prior row + zero rows → last_watermark == None.

        P1.17: empty-delta gate is on source DataFrame count; inject a
        zero-count stub.
        """
        fake_now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        spark = _ExecuteNodeFakeSpark(prior_rows=[], table_count=0)
        step, _ = _execute_bronze(
            spark=spark, fake_now=fake_now, extract_returns=_DfStub(count=0),
        )
        assert step.status == "success"
        assert step.row_count == 0
        assert step.last_watermark is None

    def test_d4_monotonicity_regression_fails_step(self, tmp_path: Path) -> None:
        """Synthetic prior with a future-dated W1. Captured
        new_wm (now - safety window) < W1 → WatermarkMonotonicityError
        → RunStep.failed with the exception repr in error_message.
        """
        fake_now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        # Prior watermark is far in the future relative to fake_now,
        # so the captured cursor (fake_now - 1h) is strictly less.
        W1 = datetime(2030, 1, 1, tzinfo=timezone.utc)
        spark = _ExecuteNodeFakeSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze", status="success",
                    last_watermark=W1, last_run_at=W1,
                ),
            ],
            table_count=5,
        )
        step, _ = _execute_bronze(spark=spark, fake_now=fake_now)
        assert step.status == "failed"
        assert "WatermarkMonotonicityError" in (step.error_message or "")
        # The dataset_id is carried through the exception message.
        assert "'ap_invoices'" in (step.error_message or "")

    def test_d4_naive_prior_does_not_typeerror(self, tmp_path: Path) -> None:
        """TypeError-prevention sub-case: a naive prior watermark
        must compare cleanly against the aware ``new_wm`` (no
        ``can't compare offset-naive and offset-aware`` regression).
        ``_normalize_to_utc`` runs on the read path before the
        monotonicity comparison reaches it.
        """
        fake_now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        # Far-future NAIVE prior — would TypeError without normalization.
        W1_naive = datetime(2030, 1, 1)  # no tzinfo
        spark = _ExecuteNodeFakeSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze", status="success",
                    last_watermark=W1_naive, last_run_at=W1_naive,
                ),
            ],
            table_count=5,
        )
        step, _ = _execute_bronze(spark=spark, fake_now=fake_now)
        # Expected = WatermarkMonotonicityError, NOT TypeError.
        assert step.status == "failed"
        assert "WatermarkMonotonicityError" in (step.error_message or "")
        assert "TypeError" not in (step.error_message or "")


# ---------------------------------------------------------------------------
# C5 — write_state_row persists step.last_watermark, NOT step.watermark_used
# ---------------------------------------------------------------------------
#
# The Phase α SQL conflated input and output: ``{_ts(step.watermark_used)}``
# went into the ``last_watermark`` column. β.1 swaps the source to
# ``{_ts(step.last_watermark)}`` so the persisted cursor is the captured
# output, not the consumed input. A regression in this SQL — accidentally
# reverting to ``watermark_used`` (visible to a future ``--amend``-style
# refactor), or breaking the timestamp rendering — would corrupt the
# fusion_bundle_state cursor without surfacing through any other test,
# because every other unit test asserts ``step.last_watermark`` BEFORE
# the persistence wrapper runs. Pin the SQL contract directly.


class _SqlCapturingSpark:
    """Captures every ``spark.sql(...)`` call as a string. Used by the
    write_state_row contract test to assert which watermark value lands
    in the SQL VALUES list.
    """

    def __init__(self) -> None:
        self.sql_calls: list[str] = []

    def sql(self, query: str) -> _DfFromRows:
        self.sql_calls.append(query)
        return _DfFromRows([])


class TestWriteStateRowPersistsLastWatermark:
    """C5 contract — ``write_state_row`` writes ``step.last_watermark``
    to the ``last_watermark`` column. The input audit ``watermark_used``
    is in-memory only on ``RunStep`` and MUST NOT appear in the SQL.
    """

    def _make_success_step(
        self, *, watermark_used: datetime | None, last_watermark: datetime | None,
    ) -> RunStep:
        spec = BronzeExtractSpec("ap_invoices", "ap_invoices")
        return RunStep.success(
            spec, run_id="r-test", mode="seed",
            row_count=42,
            duration_seconds=1.5,
            watermark_used=watermark_used,
            last_watermark=last_watermark,
        )

    def test_persists_last_watermark_not_watermark_used(self) -> None:
        from oracle_ai_data_platform_fusion_bundle.orchestrator.state import (
            write_state_row,
        )

        W_input = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        W_output = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        step = self._make_success_step(
            watermark_used=W_input, last_watermark=W_output,
        )
        spark = _SqlCapturingSpark()
        write_state_row(spark, _TEST_PATHS, step)

        assert len(spark.sql_calls) == 1, "write_state_row must issue exactly one INSERT"
        sql = spark.sql_calls[0]
        # The output W2 lands in the SQL VALUES list.
        assert W_output.isoformat(sep=" ") in sql, (
            f"expected last_watermark={W_output!r} rendered as TIMESTAMP "
            f"literal in the INSERT; sql={sql!r}"
        )
        # The input W1 does NOT — Phase α persisted it; β.1 must NOT.
        assert W_input.isoformat(sep=" ") not in sql, (
            f"watermark_used={W_input!r} must stay in-memory only; "
            f"finding it in the persisted SQL indicates a regression to "
            f"the Phase α conflation. sql={sql!r}"
        )

    def test_none_last_watermark_persists_as_typed_null(self) -> None:
        """Silver/gold rows in β.1 leave ``last_watermark=None``. The
        SQL must use ``CAST(NULL AS TIMESTAMP)`` so Delta's strict
        schema-merge accepts the row (matches the Phase α
        ``ensure_state_table`` writeability-probe fix).
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator.state import (
            write_state_row,
        )

        step = self._make_success_step(
            watermark_used=None, last_watermark=None,
        )
        spark = _SqlCapturingSpark()
        write_state_row(spark, _TEST_PATHS, step)

        sql = spark.sql_calls[0]
        # The last_watermark column position carries the typed-NULL cast.
        assert "CAST(NULL AS TIMESTAMP)" in sql

    def test_null_watermark_used_with_non_null_last_watermark_persists_output(
        self,
    ) -> None:
        """First-run case: no prior state row, so ``watermark_used=None``,
        but the bronze closure DID capture a new cursor. The output must
        still land in the SQL.
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator.state import (
            write_state_row,
        )

        W_output = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        step = self._make_success_step(
            watermark_used=None, last_watermark=W_output,
        )
        spark = _SqlCapturingSpark()
        write_state_row(spark, _TEST_PATHS, step)

        sql = spark.sql_calls[0]
        assert W_output.isoformat(sep=" ") in sql

    def test_timestamp_rendering_uses_iso_with_space_separator(self) -> None:
        """Pin the exact ``_ts`` rendering shape — Delta accepts
        ``TIMESTAMP 'YYYY-MM-DD HH:MM:SS[.ffffff][+HH:MM]'`` (space
        separator, NOT 'T'). A regression to ``isoformat()`` without
        ``sep=' '`` would land 'T' in the literal and Delta would
        reject the INSERT.
        """
        from oracle_ai_data_platform_fusion_bundle.orchestrator.state import (
            write_state_row,
        )

        W = datetime(2026, 5, 22, 12, 34, 56, 789012, tzinfo=timezone.utc)
        step = self._make_success_step(watermark_used=None, last_watermark=W)
        spark = _SqlCapturingSpark()
        write_state_row(spark, _TEST_PATHS, step)

        sql = spark.sql_calls[0]
        # Expected literal shape (space separator, no 'T').
        assert "TIMESTAMP '2026-05-22 12:34:56.789012+00:00'" in sql, (
            f"timestamp literal shape regression — Delta requires space "
            f"separator between date and time. sql={sql!r}"
        )
