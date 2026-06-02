"""P1.17 Stage D — orchestrator dispatch + preflight tests.

Covers (per docs/features/p1.17-incremental-merge/plan.md Stage D):
  - D3: bronze MERGE SQL contains MERGE INTO + correct natural-key join.
  - D4: extract_pvo receives the ISO-stringified prior watermark when
        mode="incremental" + prior_watermark is not None + PVO is
        incremental_capable=True.
  - D-non-incremental-pvo: PVOs flagged ``incremental_capable=False``
        get ``watermark=None`` in extract_pvo + still emit MERGE.
  - D5: silver/gold capture MAX(bronze_extract_ts) into
        RunStep.last_watermark in BOTH seed and incremental modes.
  - D6: dim_calendar dispatch does NOT pass refresh_mode/watermark
        kwargs (its build() signature doesn't accept them).
  - D10: orchestrator.run(mode="incremental") no longer raises
        NotImplementedError (the gate is gone).
  - D-fresh-tenant-bronze: bronze MERGE issues CREATE TABLE IF NOT
        EXISTS before MERGE INTO on first incremental for a fresh tenant.
  - D-empty-bronze-merge: source_delta_count == 0 short-circuits the
        MERGE and preserves the prior watermark (source-count gate, not
        target-count).
  - D-builder-kwargs: silver/gold builder receives the LAYER-LOCAL
        cursor (READ #2), not the upstream-bronze windowed cursor.
  - Preflight (B4b): fresh-tenant + soft-fail + consolidated +
        skip-incremental-exempt + bronze-tolerates-null-cursor.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
from oracle_ai_data_platform_fusion_bundle.orchestrator import (
    _execute_node,
    _natural_key_join_sql,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.errors import (
    IncrementalCursorMissingError,
    IncrementalTargetMissingError,
    StateReadFailedError,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.preflight import (
    _preflight_incremental_cursors,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.registry import (
    BRONZE_EXTRACTS,
    GOLD_MARTS,
    SILVER_DIMS,
    BronzeExtractSpec,
    DeferredSpec,
    GoldMartSpec,
    SilverDimSpec,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import (
    WATERMARK_SAFETY_WINDOW,
    RunStep,
)
from oracle_ai_data_platform_fusion_bundle.schema.bundle import (
    Bundle,
    DatasetSpec,
    FusionConn,
)


_TEST_PATHS = TablePaths(
    catalog="fusion_catalog",
    bronze_schema="bronze",
    silver_schema="silver",
    gold_schema="gold",
)


# ---------------------------------------------------------------------------
# Fixtures — fake Spark + bundle factories
# ---------------------------------------------------------------------------


class _FakeRow:
    def __init__(self, **kw: Any) -> None:
        self._d = kw

    def __getitem__(self, k: str) -> Any:
        return self._d.get(k)

    def __getattr__(self, k: str) -> Any:
        if k in self._d:
            return self._d[k]
        raise AttributeError(k)


class _DfFromRows:
    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows

    def collect(self) -> list[_FakeRow]:
        return self._rows

    def first(self) -> _FakeRow | None:
        return self._rows[0] if self._rows else None


class _DispatchSpark:
    """Compact fake Spark for orchestrator-dispatch tests.

    Routes by query substring:
      * read_last_watermark queries → seeded `prior_rows`
      * MAX(bronze_extract_ts) queries → seeded `silver_gold_wm`
      * everything else → empty result

    `table(...).count()` returns `table_count`. `catalog.tableExists(...)`
    returns `table_exists` (default True). `state_read_raises=True`
    (P1.17c) makes ``read_last_watermark``/``read_last_watermark_strict``
    queries fail with a synthetic RuntimeError so the strict-fail
    preflight gate can be exercised under unit-test conditions.
    """

    def __init__(
        self,
        *,
        prior_rows: list[_FakeRow] | None = None,
        silver_gold_wm: datetime | None = None,
        table_count: int = 0,
        table_exists: bool = True,
        state_read_raises: bool = False,
    ) -> None:
        self.prior_rows = prior_rows or []
        self.silver_gold_wm = silver_gold_wm
        self.table_count = table_count
        self.table_exists = table_exists
        self.state_read_raises = state_read_raises
        self.sql_calls: list[str] = []
        self.catalog = MagicMock()
        self.catalog.tableExists.side_effect = lambda *_a, **_kw: self.table_exists

    def sql(self, query: str) -> _DfFromRows:
        self.sql_calls.append(query)
        # read_last_watermark
        if "SELECT last_watermark" in query and "fusion_bundle_state" in query:
            if self.state_read_raises:
                # P1.17c — simulate a metastore failure during the
                # preflight state read so read_last_watermark_strict
                # surfaces StateReadFailedError. The soft variant
                # would swallow this and return None.
                raise RuntimeError("simulated metastore failure for test")
            return _DfFromRows(self._read_last_watermark(query))
        # MAX(bronze_extract_ts) capture (silver/gold post-build)
        if "MAX(bronze_extract_ts)" in query:
            return _DfFromRows(
                [_FakeRow(wm=self.silver_gold_wm)] if self.silver_gold_wm else [_FakeRow(wm=None)]
            )
        return _DfFromRows([])

    def _read_last_watermark(self, query: str) -> list[_FakeRow]:
        import re
        m_ds = re.search(r"dataset_id\s*=\s*'((?:[^']|'')*)'", query)
        m_layer = re.search(r"layer\s*=\s*'((?:[^']|'')*)'", query)
        if not m_ds or not m_layer:
            return []
        ds = m_ds.group(1).replace("''", "'")
        layer = m_layer.group(1).replace("''", "'")
        matching = [
            r for r in self.prior_rows
            if r["dataset_id"] == ds and r["layer"] == layer and r["status"] == "success"
        ]
        return matching[:1]

    def table(self, _name: str) -> Any:
        m = MagicMock()
        m.count.return_value = self.table_count
        return m


class _DfStub:
    """Cache-aware DataFrame stub used by bronze MERGE tests.

    Captures cache/unpersist + count + createOrReplaceTempView, exposes
    a schema with named fields, and lets the write chain no-op.
    """

    def __init__(self, *, schema_fields: list[tuple[str, str]] | None = None, count: int = 0) -> None:
        self._count = count
        self.cached = False
        self.unpersisted = False
        self.temp_view: str | None = None
        # Build a schema-like object with `.fields` iterable returning
        # `.name` + `.dataType.simpleString()`-bearing objects.
        fields = schema_fields or [("_extract_ts", "timestamp"), ("SEGMENT1", "string")]
        self.schema = MagicMock()
        self.schema.fields = [
            MagicMock(name=name, dataType=MagicMock(simpleString=lambda t=t: t)) | _name_mock(name)
            for name, t in fields
        ]
        # writer chain
        self.write = MagicMock()
        self.write.format.return_value = self.write
        self.write.mode.return_value = self.write
        self.write.option.return_value = self.write
        self.write.saveAsTable.return_value = None

    def cache(self) -> "_DfStub":
        self.cached = True
        return self

    def unpersist(self) -> None:
        self.unpersisted = True

    def count(self) -> int:
        return self._count

    def createOrReplaceTempView(self, name: str) -> None:
        self.temp_view = name


def _name_mock(name: str) -> MagicMock:
    """Helper — produce a MagicMock whose `.name` attribute returns the
    string `name`. Plain `MagicMock(name=...)` sets a *MagicMock* name
    (used in repr), not the `.name` attribute.
    """
    m = MagicMock()
    m.name = name
    return m


def _bundle(safety_window_seconds: int = 3600) -> Bundle:
    from oracle_ai_data_platform_fusion_bundle.schema.bundle import IncrementalConfig
    return Bundle(
        apiVersion="aidp-fusion-bundle/v1",
        project="p1.17-dispatch-test",
        fusion=FusionConn(
            serviceUrl="https://example.fa.oraclecloud.com",
            username="u",
            password="literal-password",
            externalStorage="s",
        ),
        datasets=[DatasetSpec(id="ap_invoices")],
        incremental=IncrementalConfig(
            watermark_safety_window_seconds=safety_window_seconds
        ),
    )


def _execute_bronze(
    *,
    spark: _DispatchSpark,
    fake_now: datetime,
    mode: str,
    pvo_id: str = "ap_invoices",
    df_stub: _DfStub | None = None,
) -> tuple[RunStep, dict[str, Any], _DfStub]:
    """Invoke `_execute_node` for a bronze spec. Returns (step, captured_kwargs, df_stub)."""
    df = df_stub if df_stub is not None else _DfStub(count=5)
    captured: dict[str, Any] = {}

    def fake_extract_pvo(_spark, _pvo, **kw):
        captured["extract_kwargs"] = kw
        return df

    def fake_enrich(df_arg, **kw):
        captured["enrich_kwargs"] = kw
        return df_arg

    with patch(
        "oracle_ai_data_platform_fusion_bundle.extractors.bicc.extract_pvo",
        side_effect=fake_extract_pvo,
    ), patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.enrich_bronze_audit_cols",
        side_effect=fake_enrich,
    ), patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.datetime",
    ) as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        spec = BRONZE_EXTRACTS[pvo_id]
        step = _execute_node(
            spec, spark=spark, paths=_TEST_PATHS, bundle=_bundle(),  # type: ignore[arg-type]
            run_id="run-test", mode=mode,
            effective_schemas={pvo_id: "Financial"},
            plan_hash="h", plan_snapshot="{}",
        )
    return step, captured, df


# ---------------------------------------------------------------------------
# D3 — bronze MERGE SQL shape (incremental mode + non-empty delta)
# ---------------------------------------------------------------------------


class TestBronzeMergeSql:
    def test_incremental_emits_merge_into_with_natural_key(self) -> None:
        # Prior watermark exists → incremental path with non-zero delta
        # should emit MERGE INTO using the catalog's natural key.
        W1 = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        fake_now = datetime(2026, 5, 22, 13, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze",
                    status="success", last_watermark=W1,
                    last_run_at=W1,
                ),
            ],
            table_count=10,
            table_exists=True,
        )
        step, _captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="incremental",
            df_stub=_DfStub(count=5),
        )
        assert step.status == "success"
        # Find the MERGE call in the SQL log.
        merge_calls = [q for q in spark.sql_calls if "MERGE INTO" in q]
        assert len(merge_calls) == 1, f"expected one MERGE, got {len(merge_calls)}"
        merge_sql = merge_calls[0]
        # ap_invoices.natural_key == "ApInvoicesInvoiceId" per A1 inventory.
        assert "ApInvoicesInvoiceId" in merge_sql
        # NULL-safe join operator.
        assert "<=>" in merge_sql
        assert "WHEN MATCHED THEN UPDATE SET *" in merge_sql
        assert "WHEN NOT MATCHED THEN INSERT *" in merge_sql

    def test_seed_does_not_emit_merge(self) -> None:
        # mode='seed' → mode("overwrite") write path, no MERGE INTO.
        fake_now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(prior_rows=[], table_count=5)
        step, _captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="seed", df_stub=_DfStub(count=5),
        )
        assert step.status == "success"
        merge_calls = [q for q in spark.sql_calls if "MERGE INTO" in q]
        assert merge_calls == []


# ---------------------------------------------------------------------------
# D4 + D-non-incremental-pvo — extract_pvo(watermark=) threading
# ---------------------------------------------------------------------------


class TestExtractPvoWatermarkThreading:
    def test_incremental_threads_iso_watermark(self) -> None:
        W1 = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        fake_now = datetime(2026, 5, 22, 13, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze",
                    status="success", last_watermark=W1, last_run_at=W1,
                ),
            ],
            table_count=5, table_exists=True,
        )
        step, captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="incremental",
            df_stub=_DfStub(count=5),
        )
        assert step.status == "success"
        # ISO-rendered, trailing Z, matches `_to_bicc_iso(W1)`.
        assert captured["extract_kwargs"]["watermark"] == "2026-05-22T10:00:00Z"

    def test_seed_threads_none(self) -> None:
        fake_now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(prior_rows=[], table_count=5)
        _step, captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="seed", df_stub=_DfStub(count=5),
        )
        assert captured["extract_kwargs"]["watermark"] is None

    def test_incremental_with_no_prior_threads_none(self) -> None:
        # Fresh tenant — no prior cursor → BICC gets watermark=None
        # (full extract; bronze degenerates cleanly).
        fake_now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(prior_rows=[], table_count=5, table_exists=False)
        _step, captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="incremental",
            df_stub=_DfStub(count=5),
        )
        assert captured["extract_kwargs"]["watermark"] is None

    def test_incremental_capable_false_pvo_threads_none(self) -> None:
        # gl_period_balances.incremental_capable == False — even with a
        # prior cursor + mode=incremental, BICC gets watermark=None
        # because the filter isn't respected for these PVOs.
        W1 = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        fake_now = datetime(2026, 5, 22, 13, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="gl_period_balances", layer="bronze",
                    status="success", last_watermark=W1, last_run_at=W1,
                ),
            ],
            table_count=5, table_exists=True,
        )
        _step, captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="incremental",
            pvo_id="gl_period_balances",
            df_stub=_DfStub(count=5),
        )
        assert captured["extract_kwargs"]["watermark"] is None

    # P1.17 reviewer catch — the `_watermark_used` bronze audit column
    # must reflect the SAME cursor BICC actually consumed. β.1 hardcoded
    # None because the NotImplementedError gate kept BICC from receiving
    # any cursor; P1.17 removes the gate and wires the audit column.
    # SOX traceability — every bronze row records which input window
    # produced it.

    def test_incremental_audit_column_records_prior_watermark_when_bicc_used_cursor(self) -> None:
        # The three-condition gate (prior_watermark non-None AND
        # incremental_capable AND mode==incremental) fires → BICC consumed
        # the ISO cursor → enrich_bronze_audit_cols must stamp the raw
        # datetime into `_watermark_used`. NOT the ISO string and NOT
        # `bicc_watermark` — runtime.enrich_bronze_audit_cols expects a
        # datetime that it casts to TIMESTAMP via F.lit(...).cast(...).
        W1 = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        fake_now = datetime(2026, 5, 22, 13, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze",
                    status="success", last_watermark=W1, last_run_at=W1,
                ),
            ],
            table_count=5, table_exists=True,
        )
        _step, captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="incremental",
            df_stub=_DfStub(count=5),
        )
        # BICC saw the ISO form …
        assert captured["extract_kwargs"]["watermark"] == "2026-05-22T10:00:00Z"
        # …and the bronze rows record the SAME instant as a datetime.
        assert captured["enrich_kwargs"]["watermark"] == W1

    def test_seed_mode_audit_column_is_none(self) -> None:
        # mode=seed → BICC threading gated off → no cursor consumed →
        # bronze rows record _watermark_used = None.
        fake_now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(prior_rows=[], table_count=5)
        _step, captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="seed",
            df_stub=_DfStub(count=5),
        )
        assert captured["extract_kwargs"]["watermark"] is None
        assert captured["enrich_kwargs"]["watermark"] is None

    def test_fresh_tenant_incremental_audit_column_is_none(self) -> None:
        # Fresh tenant → prior_watermark=None → BICC threading gated off
        # (first condition fails) → bronze rows record None. Without this
        # gate the audit column would lie about the input window.
        fake_now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(prior_rows=[], table_count=5, table_exists=False)
        _step, captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="incremental",
            df_stub=_DfStub(count=5),
        )
        assert captured["extract_kwargs"]["watermark"] is None
        assert captured["enrich_kwargs"]["watermark"] is None

    def test_incremental_capable_false_pvo_audit_column_is_none(self) -> None:
        # gl_period_balances.incremental_capable=False → BICC threading
        # gated off (second condition fails) → audit column NULL even
        # though a prior cursor exists. The bronze rows are NOT the
        # product of a windowed extract — BICC ignores the cursor for
        # this PVO — so the audit column correctly says "no window
        # consumed."
        W1 = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        fake_now = datetime(2026, 5, 22, 13, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="gl_period_balances", layer="bronze",
                    status="success", last_watermark=W1, last_run_at=W1,
                ),
            ],
            table_count=5, table_exists=True,
        )
        _step, captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="incremental",
            pvo_id="gl_period_balances",
            df_stub=_DfStub(count=5),
        )
        assert captured["extract_kwargs"]["watermark"] is None
        assert captured["enrich_kwargs"]["watermark"] is None


# ---------------------------------------------------------------------------
# D-fresh-tenant-bronze + D-empty-bronze-merge
# ---------------------------------------------------------------------------


class TestBronzeFreshTenantPaths:
    def test_fresh_tenant_issues_create_table_if_not_exists_before_merge(self) -> None:
        # tableExists → False → bronze closure must CREATE the target
        # before MERGE. The MERGE has nothing to UPDATE so all rows INSERT.
        fake_now = datetime(2026, 5, 22, 12, 0, 0, tzinfo=timezone.utc)
        W1 = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze",
                    status="success", last_watermark=W1, last_run_at=W1,
                ),
            ],
            table_count=3, table_exists=False,
        )
        step, _captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="incremental",
            df_stub=_DfStub(count=3),
        )
        assert step.status == "success"
        # The fake_now logic: tableExists=False → _ensure_target_table
        # calls `spark.sql("CREATE TABLE IF NOT EXISTS ...")` BEFORE MERGE.
        create_idx = next(
            (i for i, q in enumerate(spark.sql_calls) if "CREATE TABLE IF NOT EXISTS" in q),
            None,
        )
        merge_idx = next(
            (i for i, q in enumerate(spark.sql_calls) if "MERGE INTO" in q),
            None,
        )
        assert create_idx is not None, "CREATE TABLE IF NOT EXISTS missing on fresh tenant"
        assert merge_idx is not None
        assert create_idx < merge_idx, "CREATE must run BEFORE MERGE"

    def test_empty_source_short_circuits_merge_and_preserves_watermark(self) -> None:
        # source_delta_count == 0 → no MERGE; new_wm == prior_watermark
        # (preserved). This is the B6 source-count gate, not target-count.
        fake_now = datetime(2026, 5, 22, 13, 0, 0, tzinfo=timezone.utc)
        W1 = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze",
                    status="success", last_watermark=W1, last_run_at=W1,
                ),
            ],
            table_count=100,  # existing target rows
            table_exists=True,
        )
        step, _captured, _df = _execute_bronze(
            spark=spark, fake_now=fake_now, mode="incremental",
            df_stub=_DfStub(count=0),  # empty BICC delta
        )
        assert step.status == "success"
        # No MERGE was issued.
        assert not any("MERGE INTO" in q for q in spark.sql_calls)
        # Cursor preserved (not advanced to fake_now - safety_window).
        assert step.last_watermark == W1
        # row_count carries the materialized target count (100).
        assert step.row_count == 100


# ---------------------------------------------------------------------------
# D5 + D-builder-kwargs — silver/gold dispatch + capture
# ---------------------------------------------------------------------------


class TestSilverGoldDispatch:
    """The dispatch site at __init__.py:_execute_node for silver/gold
    must:
      1. Read upstream-bronze cursor → RunStep.watermark_used (in-memory).
      2. Read layer-local cursor → builder's `watermark` kwarg.
      3. Capture MAX(bronze_extract_ts) post-build → RunStep.last_watermark.
    """

    @staticmethod
    def _run_silver(spark: _DispatchSpark, mode: str, builder_capture: dict) -> RunStep:
        def fake_builder(_spark, **kw):
            builder_capture.update(kw)
            df = MagicMock()
            df.count.return_value = 7
            return df

        spec = SilverDimSpec(
            "dim_supplier",
            builder=fake_builder,
            depends_on_bronze=("erp_suppliers",),
            natural_key="supplier_number",
        )
        return _execute_node(
            spec, spark=spark, paths=_TEST_PATHS, bundle=_bundle(),  # type: ignore[arg-type]
            run_id="run-test", mode=mode,
            effective_schemas={}, plan_hash="h", plan_snapshot="{}",
        )

    def test_builder_receives_layer_local_watermark_not_upstream(self) -> None:
        # D-builder-kwargs — silver/gold builder gets READ #2
        # (layer-local), NOT READ #1 (upstream-bronze).
        upstream_wm = datetime(2026, 5, 22, 9, 0, 0, tzinfo=timezone.utc)
        layer_wm = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                # upstream-bronze cursor (erp_suppliers, bronze) — wm = 09:00
                _FakeRow(
                    dataset_id="erp_suppliers", layer="bronze",
                    status="success", last_watermark=upstream_wm,
                    last_run_at=upstream_wm,
                ),
                # layer-local cursor (dim_supplier, silver) — wm = 10:00
                _FakeRow(
                    dataset_id="dim_supplier", layer="silver",
                    status="success", last_watermark=layer_wm,
                    last_run_at=layer_wm,
                ),
            ],
            silver_gold_wm=datetime(2026, 5, 22, 11, 0, 0, tzinfo=timezone.utc),
        )
        captured: dict[str, Any] = {}
        step = self._run_silver(spark, "incremental", captured)
        assert step.status == "success"
        # Builder got the LAYER-LOCAL watermark (10:00), not upstream (09:00).
        assert captured["watermark"] == layer_wm
        # watermark_used carries the upstream cursor for in-memory audit.
        assert step.watermark_used == upstream_wm

    def test_builder_receives_refresh_mode_kwarg(self) -> None:
        upstream_wm = datetime(2026, 5, 22, 9, 0, 0, tzinfo=timezone.utc)
        layer_wm = datetime(2026, 5, 22, 10, 0, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="erp_suppliers", layer="bronze",
                    status="success", last_watermark=upstream_wm,
                    last_run_at=upstream_wm,
                ),
                _FakeRow(
                    dataset_id="dim_supplier", layer="silver",
                    status="success", last_watermark=layer_wm,
                    last_run_at=layer_wm,
                ),
            ],
        )
        captured: dict[str, Any] = {}
        self._run_silver(spark, "incremental", captured)
        assert captured["refresh_mode"] == "incremental"

    def test_seed_mode_passes_refresh_mode_seed(self) -> None:
        spark = _DispatchSpark(prior_rows=[])
        captured: dict[str, Any] = {}
        self._run_silver(spark, "seed", captured)
        assert captured["refresh_mode"] == "seed"

    def test_d5_captures_max_bronze_extract_ts_into_last_watermark(self) -> None:
        # Post-build MAX(bronze_extract_ts) lands on RunStep.last_watermark
        # in BOTH seed and incremental modes.
        captured_wm = datetime(2026, 5, 22, 11, 30, 0, tzinfo=timezone.utc)
        spark = _DispatchSpark(prior_rows=[], silver_gold_wm=captured_wm)
        captured: dict[str, Any] = {}
        step = self._run_silver(spark, "seed", captured)
        assert step.status == "success"
        assert step.last_watermark == captured_wm

    def test_d5_seed_mode_capture_populates_first_incremental_cursor(self) -> None:
        # Critical: even in SEED mode the silver capture must fire, so
        # the NEXT incremental run finds a non-null layer-local cursor.
        # Without this, every fresh-tenant pipeline would trip B4b's
        # preflight on its first incremental cycle.
        captured_wm = datetime(2026, 5, 22, 11, tzinfo=timezone.utc)
        spark = _DispatchSpark(prior_rows=[], silver_gold_wm=captured_wm)
        captured: dict[str, Any] = {}
        step = self._run_silver(spark, "seed", captured)
        assert step.last_watermark is not None
        assert step.last_watermark == captured_wm

    def test_dim_calendar_dispatch_omits_refresh_mode_and_watermark(self) -> None:
        # D6 — dim_calendar.build() doesn't accept refresh_mode /
        # watermark per Invariant 3; dispatch must omit them.
        spark = _DispatchSpark(prior_rows=[])

        def fake_calendar_builder(*args, **kw):
            # MUST NOT receive refresh_mode or watermark.
            assert "refresh_mode" not in kw, (
                "dim_calendar.build received refresh_mode — Invariant 3 broken"
            )
            assert "watermark" not in kw, (
                "dim_calendar.build received watermark — Invariant 3 broken"
            )
            df = MagicMock()
            df.count.return_value = 4018  # ~11 years of days
            return df

        # dim_calendar resolver returns None (no upstream bronze).
        spec = SilverDimSpec(
            "dim_calendar",
            builder=fake_calendar_builder,
            depends_on_bronze=(),  # no upstream → resolver returns None
            natural_key="",
        )
        step = _execute_node(
            spec, spark=spark, paths=_TEST_PATHS, bundle=_bundle(),  # type: ignore[arg-type]
            run_id="run-test", mode="incremental",
            effective_schemas={}, plan_hash="h", plan_snapshot="{}",
        )
        assert step.status == "success"


# ---------------------------------------------------------------------------
# Incremental-exempt gold mart dispatch — supplier_spend / ap_aging
# ---------------------------------------------------------------------------


class TestIncrementalExemptDispatch:
    """Gold marts flagged `incremental_capable=False` (supplier_spend +
    ap_aging) must receive `refresh_mode="seed"` from the orchestrator
    even when the run is in incremental mode. This is the dispatch-side
    half of the B3b/B2 exemption; the builder-side half ignores the
    kwarg either way.
    """

    @staticmethod
    def _run_gold(spark: _DispatchSpark, mart_id: str, mode: str, captured: dict) -> RunStep:
        def fake_builder(_spark, **kw):
            captured.update(kw)
            df = MagicMock()
            df.count.return_value = 309
            return df

        original = GOLD_MARTS[mart_id]
        spec = GoldMartSpec(
            mart_id,
            builder=fake_builder,
            depends_on_bronze=original.depends_on_bronze,
            depends_on_silver=original.depends_on_silver,
            natural_key=original.natural_key,
            incremental_capable=original.incremental_capable,
        )
        return _execute_node(
            spec, spark=spark, paths=_TEST_PATHS, bundle=_bundle(),  # type: ignore[arg-type]
            run_id="run-test", mode=mode,
            effective_schemas={}, plan_hash="h", plan_snapshot="{}",
        )

    def test_supplier_spend_gets_refresh_mode_seed_even_in_incremental(self) -> None:
        upstream_wm = datetime(2026, 5, 22, 9, tzinfo=timezone.utc)
        layer_wm = datetime(2026, 5, 22, 10, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze",
                    status="success", last_watermark=upstream_wm,
                    last_run_at=upstream_wm,
                ),
                _FakeRow(
                    dataset_id="supplier_spend", layer="gold",
                    status="success", last_watermark=layer_wm,
                    last_run_at=layer_wm,
                ),
            ],
        )
        captured: dict[str, Any] = {}
        self._run_gold(spark, "supplier_spend", "incremental", captured)
        # Even though orchestrator mode is "incremental", the exempt
        # mart sees "seed".
        assert captured["refresh_mode"] == "seed"

    def test_ap_aging_gets_refresh_mode_seed_even_in_incremental(self) -> None:
        upstream_wm = datetime(2026, 5, 22, 9, tzinfo=timezone.utc)
        layer_wm = datetime(2026, 5, 22, 10, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze",
                    status="success", last_watermark=upstream_wm,
                    last_run_at=upstream_wm,
                ),
                _FakeRow(
                    dataset_id="ap_aging", layer="gold",
                    status="success", last_watermark=layer_wm,
                    last_run_at=layer_wm,
                ),
            ],
        )
        captured: dict[str, Any] = {}
        self._run_gold(spark, "ap_aging", "incremental", captured)
        assert captured["refresh_mode"] == "seed"


# ---------------------------------------------------------------------------
# D10 — gate removal: incremental no longer raises NotImplementedError
# ---------------------------------------------------------------------------


class TestGateRemoval:
    """The β.1 NotImplementedError gate at __init__.py:641-645 is gone
    in P1.17 (C9). A request for incremental mode now reaches the
    preflight + dispatch path; an empty plan returns RunSummary.empty.
    """

    def test_incremental_mode_no_longer_raises_not_implemented(self, tmp_path) -> None:
        import sys
        from pathlib import Path
        from oracle_ai_data_platform_fusion_bundle import orchestrator
        # Minimal valid bundle with empty datasets/dimensions/gold so the
        # run short-circuits via RunSummary.empty before any Spark setup.
        bundle_yaml = """\
apiVersion: aidp-fusion-bundle/v1
project: p1.17-gate-removal-test
fusion:
  serviceUrl: https://example.fa.oraclecloud.com
  username: u
  password: literal-password
  externalStorage: s
datasets: []
dimensions:
  build: []
gold:
  marts: []
"""
        bundle_path = Path(tmp_path) / "bundle.yaml"
        bundle_path.write_text(bundle_yaml, encoding="utf-8")

        # Empty plan → run() returns RunSummary.empty BEFORE any
        # bootstrap_spark / preflight / dispatch.
        result = orchestrator.run(bundle_path, mode="incremental")
        assert result.mode == "incremental"
        assert result.bundle_project == "p1.17-gate-removal-test"
        # No NotImplementedError raised — this assertion is the gate-
        # removal contract. The β.1 D7 test pinned the opposite; that
        # test was deleted atomically in C9.


# ---------------------------------------------------------------------------
# B4b preflight tests
# ---------------------------------------------------------------------------


class TestPreflightIncrementalCursors:
    """B4b — run-level preflight that consolidates missing layer-local
    cursors into a single IncrementalCursorMissingError before any
    dispatch. Bronze + dim_calendar + incremental_capable=False marts
    are skipped.
    """

    def test_fresh_tenant_raises_with_all_missing_silver_gold(self) -> None:
        # Empty state → every silver/gold node missing a cursor.
        spark = _DispatchSpark(prior_rows=[])
        plan = [
            SILVER_DIMS["dim_supplier"],
            SILVER_DIMS["dim_account"],
            GOLD_MARTS["gl_balance"],
        ]
        with pytest.raises(IncrementalCursorMissingError) as exc_info:
            _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]
        missing = exc_info.value.missing
        # All three reported, with layer tags.
        assert ("dim_supplier", "silver") in missing
        assert ("dim_account", "silver") in missing
        assert ("gl_balance", "gold") in missing

    def test_bronze_node_not_checked(self) -> None:
        # Bronze tolerates null prior cursor (full-extract fallback);
        # preflight must not raise for a missing bronze cursor.
        spark = _DispatchSpark(prior_rows=[])
        plan = [BRONZE_EXTRACTS["ap_invoices"]]
        # No raise.
        _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]

    def test_dim_calendar_skipped(self) -> None:
        # dim_calendar is parameter-driven (no source watermark); the
        # preflight must not list it even when its state row is missing.
        spark = _DispatchSpark(prior_rows=[])
        plan = [SILVER_DIMS["dim_calendar"]]
        # No raise.
        _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]

    def test_incremental_capable_false_gold_skipped(self) -> None:
        # supplier_spend + ap_aging are incremental_capable=False — they
        # route through seed-shape regardless of mode. The preflight must
        # skip them even when their cursor is missing.
        spark = _DispatchSpark(prior_rows=[])
        plan = [GOLD_MARTS["supplier_spend"], GOLD_MARTS["ap_aging"]]
        # No raise.
        _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]

    def test_deferred_spec_skipped(self) -> None:
        # DeferredSpec never dispatches — preflight ignores it.
        spark = _DispatchSpark(prior_rows=[])
        plan = [DeferredSpec("dim_org", layer="silver", reason="P1.7")]
        _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]

    def test_consolidated_error_message_lists_all_missing(self) -> None:
        spark = _DispatchSpark(prior_rows=[])
        plan = [
            SILVER_DIMS["dim_supplier"],
            SILVER_DIMS["dim_account"],
            GOLD_MARTS["gl_balance"],
        ]
        with pytest.raises(IncrementalCursorMissingError) as exc_info:
            _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]
        message = str(exc_info.value)
        assert "dim_supplier" in message
        assert "dim_account" in message
        assert "gl_balance" in message
        # Remediation hint is present.
        assert "--mode seed" in message

    def test_partial_state_only_lists_missing(self) -> None:
        # dim_supplier has a cursor; dim_account does not → only
        # dim_account is reported.
        W = datetime(2026, 5, 22, 10, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="dim_supplier", layer="silver",
                    status="success", last_watermark=W, last_run_at=W,
                ),
            ],
        )
        plan = [SILVER_DIMS["dim_supplier"], SILVER_DIMS["dim_account"]]
        with pytest.raises(IncrementalCursorMissingError) as exc_info:
            _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]
        missing = exc_info.value.missing
        assert ("dim_account", "silver") in missing
        assert ("dim_supplier", "silver") not in missing

    # -----------------------------------------------------------------
    # P1.17c — target-existence preflight (the dropped-target guard)
    # -----------------------------------------------------------------

    def test_target_missing_with_non_null_cursor_raises(self) -> None:
        # D3 — single dropped silver dim. Prior cursor exists in state;
        # target Delta table doesn't exist on disk. Preflight raises
        # IncrementalTargetMissingError listing that one node.
        from oracle_ai_data_platform_fusion_bundle.orchestrator.registry import (
            _resolve_target_table,
        )
        W = datetime(2026, 5, 22, 10, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="dim_supplier", layer="silver",
                    status="success", last_watermark=W, last_run_at=W,
                ),
            ],
            table_exists=False,
        )
        plan = [SILVER_DIMS["dim_supplier"]]
        expected_target = _resolve_target_table(SILVER_DIMS["dim_supplier"], _TEST_PATHS)
        with pytest.raises(IncrementalTargetMissingError) as exc_info:
            _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]
        assert ("dim_supplier", "silver", expected_target) in exc_info.value.missing
        # Message names the dataset, layer, and target.
        message = str(exc_info.value)
        assert "dim_supplier" in message
        assert "silver" in message
        assert "silently lose history" in message
        assert "P1.17-L5" in message

    def test_consolidated_target_missing_lists_all_affected(self) -> None:
        # D4 — three dropped targets spanning all three layers. Each has
        # a non-NULL prior cursor in state; none of the targets exist on
        # disk. Preflight raises ONE IncrementalTargetMissingError
        # listing every affected (dataset_id, layer, target).
        from oracle_ai_data_platform_fusion_bundle.orchestrator.registry import (
            _resolve_target_table,
        )
        W = datetime(2026, 5, 22, 10, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="ap_invoices", layer="bronze",
                    status="success", last_watermark=W, last_run_at=W,
                ),
                _FakeRow(
                    dataset_id="dim_supplier", layer="silver",
                    status="success", last_watermark=W, last_run_at=W,
                ),
                _FakeRow(
                    dataset_id="gl_balance", layer="gold",
                    status="success", last_watermark=W, last_run_at=W,
                ),
            ],
            table_exists=False,
        )
        plan = [
            BRONZE_EXTRACTS["ap_invoices"],
            SILVER_DIMS["dim_supplier"],
            GOLD_MARTS["gl_balance"],
        ]
        with pytest.raises(IncrementalTargetMissingError) as exc_info:
            _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]
        missing = exc_info.value.missing
        bronze_target = _resolve_target_table(BRONZE_EXTRACTS["ap_invoices"], _TEST_PATHS)
        silver_target = _resolve_target_table(SILVER_DIMS["dim_supplier"], _TEST_PATHS)
        gold_target = _resolve_target_table(GOLD_MARTS["gl_balance"], _TEST_PATHS)
        assert ("ap_invoices", "bronze", bronze_target) in missing
        assert ("dim_supplier", "silver", silver_target) in missing
        assert ("gl_balance", "gold", gold_target) in missing
        # Message contains every dataset_id (operator can scan one
        # error to see the full remediation list).
        message = str(exc_info.value)
        assert "ap_invoices" in message
        assert "dim_supplier" in message
        assert "gl_balance" in message

    def test_target_exists_with_non_null_cursor_no_raise(self) -> None:
        # D5 — false-positive guard. Prior cursors exist for every
        # plan node; targets all exist on disk. No raise.
        W = datetime(2026, 5, 22, 10, tzinfo=timezone.utc)
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="dim_supplier", layer="silver",
                    status="success", last_watermark=W, last_run_at=W,
                ),
                _FakeRow(
                    dataset_id="dim_account", layer="silver",
                    status="success", last_watermark=W, last_run_at=W,
                ),
                _FakeRow(
                    dataset_id="gl_balance", layer="gold",
                    status="success", last_watermark=W, last_run_at=W,
                ),
            ],
            table_exists=True,  # explicit — default is True too
        )
        plan = [
            SILVER_DIMS["dim_supplier"],
            SILVER_DIMS["dim_account"],
            GOLD_MARTS["gl_balance"],
        ]
        # No raise.
        _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]

    def test_cursor_check_takes_precedence_over_target_check(self) -> None:
        # D6 — precedence guard. Cursor is NULL for the silver/gold
        # node AND target is missing. Preflight must raise
        # IncrementalCursorMissingError (NOT IncrementalTargetMissingError)
        # so the operator's remediation is "run seed", not
        # "clear state row + re-seed".
        spark = _DispatchSpark(
            prior_rows=[],  # no cursor → cursor check trips first
            table_exists=False,  # would trip target check if we got there
        )
        plan = [SILVER_DIMS["dim_supplier"]]
        with pytest.raises(IncrementalCursorMissingError):
            _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]

    def test_bronze_state_read_failure_with_table_missing_raises_strict(self) -> None:
        # D8 (v2 — blocking-issue coverage) — proves the strict-read
        # guard fails closed even when the metastore is flaky on a
        # bronze node. Soft-fail would have let this slip past.
        spark = _DispatchSpark(
            prior_rows=[],
            state_read_raises=True,  # synthetic metastore failure
            table_exists=False,      # ignored — we shouldn't reach here
        )
        plan = [BRONZE_EXTRACTS["ap_invoices"]]
        with pytest.raises(StateReadFailedError) as exc_info:
            _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]
        assert exc_info.value.dataset_id == "ap_invoices"
        assert exc_info.value.layer == "bronze"
        assert isinstance(exc_info.value.cause, RuntimeError)
        assert exc_info.value.__cause__ is exc_info.value.cause
        # Critical: the strict-read raises BEFORE the target check
        # loop runs, so spark.catalog.tableExists must NOT have been
        # called. This is the load-bearing assertion — it proves the
        # dropped-target check can't be bypassed by a metastore flake
        # masquerading as a missing cursor.
        spark.catalog.tableExists.assert_not_called()

    def test_skip_list_target_check_not_invoked_for_excluded_specs(self) -> None:
        # D9 (v2 — should-fix coverage #1) — pins the skip-list as a
        # contract. Inject a non-NULL cursor for every excluded node
        # AND set table_exists=False. None of the targets are checked
        # (DeferredSpec.target would raise; incremental_capable=False
        # and dim_calendar should silently skip). No raise expected.
        W = datetime(2026, 5, 22, 10, tzinfo=timezone.utc)
        deferred = DeferredSpec("dim_org", layer="silver", reason="P1.7")
        spark = _DispatchSpark(
            prior_rows=[
                _FakeRow(
                    dataset_id="dim_calendar", layer="silver",
                    status="success", last_watermark=W, last_run_at=W,
                ),
                _FakeRow(
                    dataset_id="supplier_spend", layer="gold",
                    status="success", last_watermark=W, last_run_at=W,
                ),
                _FakeRow(
                    dataset_id="ap_aging", layer="gold",
                    status="success", last_watermark=W, last_run_at=W,
                ),
                _FakeRow(
                    dataset_id="dim_org", layer="silver",
                    status="success", last_watermark=W, last_run_at=W,
                ),
            ],
            table_exists=False,  # would trigger raise if any of these reached the target check
        )
        plan = [
            SILVER_DIMS["dim_calendar"],
            GOLD_MARTS["supplier_spend"],
            GOLD_MARTS["ap_aging"],
            deferred,
        ]
        # No raise — every node is in the skip-list. An over-eager
        # refactor that called _resolve_target_table(DeferredSpec)
        # would raise RuntimeError (per registry.py); an
        # implementation that ignored the incremental_capable=False
        # / dim_calendar guards would raise IncrementalTargetMissingError.
        # Both bugs are caught by this test.
        _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]

    def test_bronze_target_missing_but_cursor_absent_no_raise(self) -> None:
        # D10 (v2 — should-fix coverage #2) — preserves the fresh-tenant
        # bronze fallback. Bronze with NULL cursor + missing target is
        # the normal first-seed state, NOT silent corruption. An
        # implementation that dropped the `cursor is None: continue`
        # guard in the target loop (e.g. mistakenly applied the
        # silver/gold "cursor required" rule to bronze) would raise
        # IncrementalTargetMissingError here.
        spark = _DispatchSpark(
            prior_rows=[],         # no prior state row anywhere
            table_exists=False,    # bronze target doesn't exist on disk
        )
        plan = [BRONZE_EXTRACTS["ap_invoices"]]
        # No raise.
        _preflight_incremental_cursors(spark, plan, _TEST_PATHS)  # type: ignore[arg-type]
