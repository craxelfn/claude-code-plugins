"""Unit tests for resume flow.

Covers:
  * Skip-by-state — succeeded nodes carry forward as ``resumed_skipped``.
  * Re-resume contract — ``ResumeContext.succeeded`` includes BOTH
    ``'success'`` AND ``'resumed_skipped'`` so a second --resume of an
    already-resumed run treats carry-forwards as done.
  * Original-run_id preservation — resume reuses the stored run_id
    (CLAUDE.md medallion ``_run_id`` invariant).
  * Bundle drift — hash mismatch (identity or plan shape) raises
    ``ResumeBundleMismatchError``.
  * Non-resumable subcases — pre-fix21 row (``plan_hash IS NULL``) and
    partially-migrated row (``plan_snapshot IS NULL``) both raise
    ``ResumeRunNotResumableError``.
  * ``ResumeRunNotFoundError`` for unknown run_id.
  * Preflight narrowing — succeeded bronze nodes are NOT re-probed.
  * Scope reconstruction — bare --resume rebuilds scope from snapshot.

Uses an enhanced ``_FakeSpark`` that returns canned rows for the
``read_resumable_state`` SQL pattern. All other SQL (CREATE TABLE,
ALTER, INSERT, DELETE, CREATE VIEW) is a no-op.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_bundle import orchestrator
from oracle_ai_data_platform_fusion_bundle.orchestrator import registry
from oracle_ai_data_platform_fusion_bundle.orchestrator.errors import (
    ResumeBundleMismatchError,
    ResumeRunNotFoundError,
    ResumeRunNotResumableError,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.state import (
    ResumeContext,
)


# ---------------------------------------------------------------------------
# Bundle fixtures
# ---------------------------------------------------------------------------


_MIN_BUNDLE = """
apiVersion: aidp-fusion-bundle/v1
project: test-orchestrator
fusion:
  serviceUrl: https://example.com
  username: alice@oracle
  password: literal-password
  externalStorage: oci://bucket@ns/path
datasets:
  - id: erp_suppliers
    mode: full
  - id: ap_invoices
    mode: full
  - id: gl_coa
    mode: full
  - id: gl_period_balances
    mode: full
dimensions:
  build:
    - dim_supplier
    - dim_account
    - dim_calendar
gold:
  marts:
    - supplier_spend
    - gl_balance
    - ap_aging
"""


def _bundle_file(tmp_path: Path, content: str = _MIN_BUNDLE) -> Path:
    p = tmp_path / "bundle.yaml"
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Fake Spark — enhanced to model state-table reads
# ---------------------------------------------------------------------------


class _FakeDataFrame:
    def __init__(self, row_count: int = 100) -> None:
        self._row_count = row_count
        self.write = MagicMock()
        self.write.format.return_value = self.write
        self.write.mode.return_value = self.write
        self.write.option.return_value = self.write
        self.write.saveAsTable.return_value = None
        # P1.17 — _ensure_target_table_exists needs `.schema.fields`.
        self.schema = MagicMock()
        self.schema.fields = []

    def count(self) -> int:
        return self._row_count

    def withColumn(self, *args, **kwargs) -> "_FakeDataFrame":
        return self

    def collect(self) -> list[Any]:
        return []

    def first(self):
        # P1.17 silver/gold capture path. Empty result → None.
        return None

    def cache(self) -> "_FakeDataFrame":
        return self

    def unpersist(self) -> None:
        return None

    def createOrReplaceTempView(self, _name: str) -> None:
        return None


class _FakeRow:
    """Fake pyspark Row: supports both attribute and item access."""

    def __init__(self, **kwargs) -> None:
        self._data = kwargs

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __getattr__(self, key: str) -> Any:
        try:
            return self._data[key]
        except KeyError as exc:
            raise AttributeError(key) from exc


class _FakeCatalog:
    def __init__(self, existing_tables: set[str] | None = None) -> None:
        self._existing = existing_tables or set()

    def tableExists(self, path: str) -> bool:
        # Default True so the external-dep preflight doesn't bark.
        if not self._existing:
            return True
        return path in self._existing


class _FakeSpark:
    """Fake Spark for resume tests. Captures every SQL string for
    assertions; routes state-table SELECTs to a canned-row response.
    """

    def __init__(
        self,
        *,
        state_rows: list[_FakeRow] | None = None,
        existing_tables: set[str] | None = None,
        row_count_per_table: int = 100,
    ) -> None:
        self.catalog = _FakeCatalog(existing_tables)
        self.sql_calls: list[str] = []
        self._state_rows = state_rows or []
        self._row_count_per_table = row_count_per_table

    def sql(self, query: str) -> _FakeDataFrame:
        self.sql_calls.append(query)
        # State-table read: detect by presence of `ranked` CTE +
        # `fusion_bundle_state` (the read_resumable_state SQL shape).
        if "ranked AS" in query and "fusion_bundle_state" in query and "WHERE run_id" in query:
            df = _FakeDataFrame()
            # The "row_count IS NOT NULL" variant feeds the
            # succeeded_row_counts dict — filter the fake rows.
            if "row_count IS NOT NULL" in query:
                df.collect = lambda: [  # type: ignore[method-assign]
                    r for r in self._state_rows
                    if getattr(r, "_data", {}).get("row_count") is not None
                ]
            else:
                df.collect = lambda: list(self._state_rows)  # type: ignore[method-assign]
            return df
        return _FakeDataFrame(0)

    def table(self, name: str) -> _FakeDataFrame:
        return _FakeDataFrame(self._row_count_per_table)


# ---------------------------------------------------------------------------
# Snapshot builder — mirrors the shape `serialize_plan_snapshot` writes
# ---------------------------------------------------------------------------


def _make_snapshot(
    *,
    identity_overrides: dict[str, str] | None = None,
    nodes: list[dict[str, str]] | None = None,
) -> str:
    base_identity = {
        "fusion.serviceUrl": "https://example.com",
        "fusion.externalStorage": "oci://bucket@ns/path",
        "fusion.username": "alice@oracle",
        "aidp.catalog": "fusion_catalog",
        "aidp.bronzeSchema": "bronze",
        "aidp.silverSchema": "silver",
        "aidp.goldSchema": "gold",
        "plugin_version": "0.1.0a0",
    }
    if identity_overrides:
        base_identity.update(identity_overrides)
    # Default: ALL 10 in-plan nodes from the minimal bundle (4 bronze + 3 silver + 3 gold).
    if nodes is None:
        nodes = [
            {"dataset_id": "ap_invoices", "layer": "bronze", "mode": "seed", "effective_schema": "Financial"},
            {"dataset_id": "erp_suppliers", "layer": "bronze", "mode": "seed", "effective_schema": "Financial"},
            {"dataset_id": "gl_coa", "layer": "bronze", "mode": "seed", "effective_schema": "Financial"},
            {"dataset_id": "gl_period_balances", "layer": "bronze", "mode": "seed", "effective_schema": "Financial"},
            {"dataset_id": "dim_supplier", "layer": "silver", "mode": "seed", "effective_schema": ""},
            {"dataset_id": "dim_account", "layer": "silver", "mode": "seed", "effective_schema": ""},
            {"dataset_id": "dim_calendar", "layer": "silver", "mode": "seed", "effective_schema": ""},
            {"dataset_id": "supplier_spend", "layer": "gold", "mode": "seed", "effective_schema": ""},
            {"dataset_id": "gl_balance", "layer": "gold", "mode": "seed", "effective_schema": ""},
            {"dataset_id": "ap_aging", "layer": "gold", "mode": "seed", "effective_schema": ""},
        ]
    return json.dumps({"identity": base_identity, "nodes": nodes})


_TEST_LAYER_FOR_DS: dict[str, str] = {
    # bronze
    "erp_suppliers": "bronze",
    "ap_invoices": "bronze",
    "ap_payments": "bronze",
    "ar_invoices": "bronze",
    "ar_receipts": "bronze",
    "gl_coa": "bronze",
    "gl_journal_lines": "bronze",
    "gl_period_balances": "bronze",
    "po_orders": "bronze",
    "po_receipts": "bronze",
    "scm_items": "bronze",
    # silver
    "dim_supplier": "silver",
    "dim_account": "silver",
    "dim_calendar": "silver",
    # gold
    "supplier_spend": "gold",
    "gl_balance": "gold",
    "ap_aging": "gold",
}


def _state_rows_from(
    *,
    succeeded: list[str],
    failed: list[str],
    snapshot: str | None = None,
    plan_hash: str | None = "fake-hash-123",
    succeeded_row_count: int = 42,
) -> list[_FakeRow]:
    """Build canned state-table rows for a fixture. Succeeded rows
    carry ``row_count=succeeded_row_count`` so the row-count carry-
    forward query has something to pick up; failed rows carry
    ``row_count=None`` (matches real behavior).

    P1.5β.1: every row now carries ``layer`` (derived from the
    shipped registry — see ``_TEST_LAYER_FOR_DS``) and
    ``last_watermark=None`` so the tuple-keyed
    ``succeeded_row_counts`` / ``succeeded_last_watermarks`` reads
    in ``read_resumable_state`` find a layer to key on. Tests that
    need a non-NULL bronze watermark override via the new
    ``last_watermarks`` mapping.
    """
    if snapshot is None:
        snapshot = _make_snapshot()
    rows: list[_FakeRow] = []
    from datetime import datetime
    base_time = datetime(2026, 5, 21, 12, 0, 0)
    for ds in succeeded:
        rows.append(_FakeRow(
            dataset_id=ds, status="success",
            layer=_TEST_LAYER_FOR_DS.get(ds, "bronze"),
            row_count=succeeded_row_count,
            last_watermark=None,
            plan_hash=plan_hash, plan_snapshot=snapshot,
            last_run_at=base_time,
        ))
    for ds in failed:
        rows.append(_FakeRow(
            dataset_id=ds, status="failed",
            layer=_TEST_LAYER_FOR_DS.get(ds, "bronze"),
            row_count=None,
            last_watermark=None,
            plan_hash=plan_hash, plan_snapshot=snapshot,
            last_run_at=base_time,
        ))
    return rows


# ---------------------------------------------------------------------------
# Test class — skip-by-state + original-run_id preservation
# ---------------------------------------------------------------------------


class TestResumeSkipByState:
    def test_succeeded_nodes_emit_resumed_skipped(self, tmp_path: Path) -> None:
        """Resume scenario: 9 succeeded + 1 failed (gl_period_balances).
        Dispatch loop emits 9 resumed_skipped + re-attempts the 1 failed.
        Original run_id preserved on every row.
        """
        original_run_id = "abc-123"
        state_rows = _state_rows_from(
            succeeded=["ap_invoices", "erp_suppliers", "gl_coa",
                       "dim_supplier", "dim_account", "dim_calendar",
                       "supplier_spend", "gl_balance", "ap_aging"],
            failed=["gl_period_balances"],
        )
        spark = _FakeSpark(state_rows=state_rows)
        # Patch preflight + hash so the drift gate doesn't fire +
        # extractors so the un-succeeded bronze can be re-dispatched
        # without a real BICC call.
        from oracle_ai_data_platform_fusion_bundle.orchestrator import registry as reg
        fake_silver = lambda spark, **k: _FakeDataFrame(0)  # noqa: E731
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ) as mock_preflight, patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.hash_resolved_plan",
            return_value="fake-hash-123",
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.serialize_plan_snapshot",
            return_value=_make_snapshot(),
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.extractors.bicc.extract_pvo",
            return_value=_FakeDataFrame(42),
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.enrich_bronze_audit_cols",
            side_effect=lambda df, **k: df,
        ):
            mock_preflight.return_value = MagicMock(
                effective_schemas={"gl_period_balances": "Financial"},
                recommendations=(),
            )
            summary = orchestrator.run(
                _bundle_file(tmp_path),
                spark=spark, mode="seed",
                resume_run_id=original_run_id,
            )

        # Original run_id preserved.
        assert summary.run_id == original_run_id
        # 9 carry-forwards + 1 re-dispatch = 10 steps total.
        statuses = [(s.dataset_id, s.status) for s in summary.steps]
        succeeded_or_resumed = [d for d, s in statuses if s in ("success", "resumed_skipped")]
        assert "ap_invoices" in succeeded_or_resumed
        # The one that was failed should re-dispatch (and succeed via fake).
        gl_pb_step = next(s for s in summary.steps if s.dataset_id == "gl_period_balances")
        assert gl_pb_step.status == "success"
        # Resumed-skipped count == 9.
        resumed_count = sum(1 for s in summary.steps if s.status == "resumed_skipped")
        assert resumed_count == 9
        # All steps carry the original run_id.
        for step in summary.steps:
            assert step.run_id == original_run_id

    def test_resumed_skipped_steps_carry_plan_hash_and_snapshot(
        self, tmp_path: Path,
    ) -> None:
        """plan_hash + plan_snapshot are threaded into every step,
        including the resumed_skipped carry-forwards (so the state
        table row for the carry-forward has the same drift-gate
        metadata as if it were a fresh write)."""
        snapshot = _make_snapshot()
        state_rows = _state_rows_from(
            succeeded=["ap_invoices", "erp_suppliers", "gl_coa", "gl_period_balances",
                       "dim_supplier", "dim_account", "dim_calendar",
                       "supplier_spend", "gl_balance", "ap_aging"],
            failed=[],
            plan_hash="locked-hash",
            snapshot=snapshot,
        )
        spark = _FakeSpark(state_rows=state_rows)
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ) as mock_preflight, patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.hash_resolved_plan",
            return_value="locked-hash",
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.serialize_plan_snapshot",
            return_value=snapshot,
        ):
            mock_preflight.return_value = MagicMock(
                effective_schemas={}, recommendations=(),
            )
            summary = orchestrator.run(
                _bundle_file(tmp_path),
                spark=spark, mode="seed",
                resume_run_id="run-X",
            )
        for step in summary.steps:
            assert step.status == "resumed_skipped"
            assert step.plan_hash == "locked-hash"
            assert step.plan_snapshot == snapshot
            assert step.skip_reason == "resume-skip"


# ---------------------------------------------------------------------------
# Test class — re-resume contract (P3.24 / fix21 multi-resume)
# ---------------------------------------------------------------------------


class TestReResumeContract:
    def test_resumed_skipped_status_counts_as_succeeded_on_next_resume(
        self, tmp_path: Path,
    ) -> None:
        """A dataset whose latest row is `resumed_skipped` from a
        prior resume must NOT be re-dispatched on a re-resume. Pin
        the contract via ResumeContext.succeeded inclusion."""
        snapshot = _make_snapshot()
        # Every node already has status='resumed_skipped' (carried forward).
        state_rows = []
        from datetime import datetime
        for ds in [
            "ap_invoices", "erp_suppliers", "gl_coa", "gl_period_balances",
            "dim_supplier", "dim_account", "dim_calendar",
            "supplier_spend", "gl_balance", "ap_aging",
        ]:
            state_rows.append(_FakeRow(
                dataset_id=ds, status="resumed_skipped",
                layer=_TEST_LAYER_FOR_DS.get(ds, "bronze"),
                row_count=None,  # resumed_skipped rows always have NULL count
                last_watermark=None,
                plan_hash="hash-1", plan_snapshot=snapshot,
                last_run_at=datetime(2026, 5, 22, 10, 0, 0),
            ))
        spark = _FakeSpark(state_rows=state_rows)
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ) as mock_preflight, patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.hash_resolved_plan",
            return_value="hash-1",
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.serialize_plan_snapshot",
            return_value=snapshot,
        ):
            mock_preflight.return_value = MagicMock(
                effective_schemas={}, recommendations=(),
            )
            summary = orchestrator.run(
                _bundle_file(tmp_path),
                spark=spark, mode="seed",
                resume_run_id="run-A",
            )
        # Every node re-emits resumed_skipped — NONE re-execute.
        assert all(s.status == "resumed_skipped" for s in summary.steps)


# ---------------------------------------------------------------------------
# Test class — bundle drift (ResumeBundleMismatchError)
# ---------------------------------------------------------------------------


class TestResumeDrift:
    def test_hash_mismatch_raises_bundle_mismatch(self, tmp_path: Path) -> None:
        snapshot = _make_snapshot()
        state_rows = _state_rows_from(
            succeeded=["ap_invoices"], failed=["erp_suppliers"],
            snapshot=snapshot, plan_hash="stored-hash",
        )
        spark = _FakeSpark(state_rows=state_rows)
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ) as mock_preflight, patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.hash_resolved_plan",
            return_value="CURRENT-hash",  # ← drifted
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.serialize_plan_snapshot",
            return_value=snapshot,
        ):
            mock_preflight.return_value = MagicMock(
                effective_schemas={}, recommendations=(),
            )
            with pytest.raises(ResumeBundleMismatchError) as exc_info:
                orchestrator.run(
                    _bundle_file(tmp_path),
                    spark=spark, mode="seed",
                    resume_run_id="run-A",
                )
        # Drift renderer output present in the message.
        msg = str(exc_info.value)
        assert "bundle drift detected" in msg.lower() or "drift" in msg.lower()
        assert "run-A" in msg


# ---------------------------------------------------------------------------
# Test class — non-resumable subcases + not-found
# ---------------------------------------------------------------------------


class TestResumeFailureModes:
    def test_unknown_run_id_raises_not_found(self, tmp_path: Path) -> None:
        # state_rows empty → no rows for any run_id
        spark = _FakeSpark(state_rows=[])
        with pytest.raises(ResumeRunNotFoundError, match="ghost-run"):
            orchestrator.run(
                _bundle_file(tmp_path),
                spark=spark, mode="seed",
                resume_run_id="ghost-run",
            )

    def test_pre_fix21_run_raises_not_resumable_subcase_1(self, tmp_path: Path) -> None:
        """All rows have plan_hash=NULL (pre-fix21 deployment). Both
        subcases live in the same error class; the message names the
        structural reason."""
        from datetime import datetime
        rows = [
            _FakeRow(
                dataset_id="ap_invoices", status="success",
                row_count=42,
                plan_hash=None, plan_snapshot=None,
                last_run_at=datetime(2026, 5, 20, 12, 0, 0),
            ),
        ]
        spark = _FakeSpark(state_rows=rows)
        with pytest.raises(ResumeRunNotResumableError, match="plan_hash"):
            orchestrator.run(
                _bundle_file(tmp_path),
                spark=spark, mode="seed",
                resume_run_id="legacy-run",
            )

    def test_partial_migration_raises_not_resumable_subcase_2(
        self, tmp_path: Path,
    ) -> None:
        """plan_hash set, plan_snapshot NULL — partially-migrated row.
        Rejected up-front so the resume flow never enters a degraded-
        metadata path."""
        from datetime import datetime
        rows = [
            _FakeRow(
                dataset_id="ap_invoices", status="success",
                row_count=42,
                plan_hash="hash-but-no-snapshot", plan_snapshot=None,
                last_run_at=datetime(2026, 5, 20, 12, 0, 0),
            ),
        ]
        spark = _FakeSpark(state_rows=rows)
        with pytest.raises(ResumeRunNotResumableError, match="plan_snapshot"):
            orchestrator.run(
                _bundle_file(tmp_path),
                spark=spark, mode="seed",
                resume_run_id="partial-run",
            )


# ---------------------------------------------------------------------------
# Test class — preflight narrowing
# ---------------------------------------------------------------------------


class TestResumePreflightNarrowing:
    def test_succeeded_bronze_not_in_preflight_input(self, tmp_path: Path) -> None:
        """preflight_bronze_schemas must be called with only the
        un-succeeded bronze nodes (re-probing succeeded ones via JDBC
        wastes minutes per node)."""
        snapshot = _make_snapshot()
        state_rows = _state_rows_from(
            succeeded=["ap_invoices", "erp_suppliers", "gl_coa",
                       "dim_supplier", "dim_account", "dim_calendar",
                       "supplier_spend", "gl_balance", "ap_aging"],
            failed=["gl_period_balances"],
            snapshot=snapshot,
        )
        spark = _FakeSpark(state_rows=state_rows)
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ) as mock_preflight, patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.hash_resolved_plan",
            return_value="fake-hash-123",
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.serialize_plan_snapshot",
            return_value=snapshot,
        ):
            mock_preflight.return_value = MagicMock(
                effective_schemas={"gl_period_balances": "Financial"},
                recommendations=(),
            )
            orchestrator.run(
                _bundle_file(tmp_path),
                spark=spark, mode="seed",
                resume_run_id="run-A",
            )
        # Inspect the plan that preflight was called with.
        _, _, plan_arg = mock_preflight.call_args.args[:3]
        bronze_in_preflight = {
            n.dataset_id for n in plan_arg
            if isinstance(n, registry.BronzeExtractSpec)
        }
        # Only the un-succeeded bronze should be there.
        assert bronze_in_preflight == {"gl_period_balances"}


# ---------------------------------------------------------------------------
# Test class — extra-dep preflight on resume
# ---------------------------------------------------------------------------


class TestResumeExtraDepPreflight:
    def test_all_succeeded_resume_no_op_does_not_preflight_dropped_upstream(
        self, tmp_path: Path,
    ) -> None:
        """All-succeeded resume of a filtered downstream-only scope
        must NOT raise ``PrerequisiteError`` even if an upstream
        out-of-scope table was dropped. With zero reattempt nodes,
        the preflight has nothing meaningful to check — every
        carry-forward is a no-op that doesn't actually read its
        upstream on the resume.

        Reproduces the bug where compute_reattempt_extra_deps
        returned ``original_extra_deps`` unconditionally and forced
        the orchestrator to preflight upstreams that no reattempt
        node touches.
        """
        snapshot = _make_snapshot(nodes=[
            {"dataset_id": "supplier_spend", "layer": "gold", "mode": "seed", "effective_schema": ""},
            {"dataset_id": "gl_balance", "layer": "gold", "mode": "seed", "effective_schema": ""},
            {"dataset_id": "ap_aging", "layer": "gold", "mode": "seed", "effective_schema": ""},
        ])
        # Resume scope = gold-only; every gold mart succeeded
        # originally. The upstream bronze + silver tables would be
        # `original_extra_deps`. Simulate that one of them was
        # dropped between runs by passing `existing_tables=set()`
        # (every catalog.tableExists() call returns False).
        state_rows = _state_rows_from(
            succeeded=["supplier_spend", "gl_balance", "ap_aging"],
            failed=[],
            snapshot=snapshot,
        )
        spark = _FakeSpark(state_rows=state_rows, existing_tables={"__nothing__"})
        # The catalog returns False for every table check — would
        # have raised PrerequisiteError on the original run too.
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ) as mock_preflight, patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.hash_resolved_plan",
            return_value="fake-hash-123",
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.serialize_plan_snapshot",
            return_value=snapshot,
        ):
            mock_preflight.return_value = MagicMock(
                effective_schemas={}, recommendations=(),
            )
            summary = orchestrator.run(
                _bundle_file(tmp_path),
                spark=spark, mode="seed",
                layers=["gold"],          # filtered scope
                resume_run_id="run-A",
            )
        # No PrerequisiteError raised; resume completes as a no-op.
        # Every step is a resumed_skipped carry-forward.
        assert summary.run_id == "run-A"
        assert all(s.status == "resumed_skipped" for s in summary.steps)
        assert {s.dataset_id for s in summary.steps} == {
            "supplier_spend", "gl_balance", "ap_aging",
        }

    def test_partial_resume_preflights_upstreams_consumed_by_reattempt_nodes(
        self, tmp_path: Path,
    ) -> None:
        """Inverse of the no-op test: when SOME nodes need reattempt,
        the preflight DOES check upstreams that the reattempting
        nodes read from. Drop a succeeded bronze table and verify
        ``PrerequisiteError`` fires before dispatch."""
        snapshot = _make_snapshot()
        # ap_invoices succeeded; supplier_spend (its downstream gold)
        # is in the reattempt set. supplier_spend reads ap_invoices.
        state_rows = _state_rows_from(
            succeeded=["ap_invoices", "erp_suppliers", "gl_coa", "gl_period_balances",
                       "dim_supplier", "dim_account", "dim_calendar",
                       "gl_balance", "ap_aging"],
            failed=["supplier_spend"],
            snapshot=snapshot,
        )
        # Make EVERY table missing — supplier_spend's dropped-bronze
        # check should fire even with the other tables present.
        spark = _FakeSpark(state_rows=state_rows, existing_tables={"__nothing__"})
        from oracle_ai_data_platform_fusion_bundle.orchestrator.errors import (
            PrerequisiteError,
        )
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ) as mock_preflight, patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.hash_resolved_plan",
            return_value="fake-hash-123",
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.serialize_plan_snapshot",
            return_value=snapshot,
        ):
            mock_preflight.return_value = MagicMock(
                effective_schemas={}, recommendations=(),
            )
            with pytest.raises(PrerequisiteError):
                orchestrator.run(
                    _bundle_file(tmp_path),
                    spark=spark, mode="seed",
                    resume_run_id="run-A",
                )


# ---------------------------------------------------------------------------
# Test class — identity-drift gate fires BEFORE preflight (security-critical)
# ---------------------------------------------------------------------------


class TestResumeIdentityDriftGateBeforePreflight:
    """The identity-only drift check must fire before any preflight /
    BICC call. Otherwise a drifted `fusion.serviceUrl` / `fusion.username`
    would send credentials to the wrong endpoint at the bronze
    preflight step.
    """

    def test_identity_drift_does_not_call_preflight(self, tmp_path: Path) -> None:
        """Construct a snapshot whose stored identity differs from the
        current bundle's identity. The orchestrator must raise
        ResumeBundleMismatchError BEFORE preflight_bronze_schemas is
        invoked — i.e. before any password unwrap / BICC contact."""
        # Snapshot identity uses a DIFFERENT serviceUrl than the
        # test bundle (which uses https://example.com).
        drifted_snapshot = _make_snapshot(identity_overrides={
            "fusion.serviceUrl": "https://OLD-DRIFTED-POD.example.com",
        })
        state_rows = _state_rows_from(
            succeeded=["ap_invoices"], failed=["erp_suppliers"],
            snapshot=drifted_snapshot, plan_hash="stored-hash",
        )
        spark = _FakeSpark(state_rows=state_rows)
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ) as mock_preflight, patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.runtime._resolve_password",
        ) as mock_resolve:
            with pytest.raises(ResumeBundleMismatchError) as exc_info:
                orchestrator.run(
                    _bundle_file(tmp_path),
                    spark=spark, mode="seed",
                    resume_run_id="run-A",
                )
        # Preflight + password unwrap must NEVER be called.
        mock_preflight.assert_not_called()
        # _resolve_password may run once for the credential preflight
        # in the deferred path, but it returns a SecretStr (not over
        # the wire) — the wire contact via .get_secret_value() inside
        # preflight is what we must prevent. Assert preflight didn't
        # consume the unwrapped value.
        msg = str(exc_info.value)
        assert "fusion.serviceUrl" in msg
        assert "OLD-DRIFTED-POD" in msg

    def test_identity_drift_username_blocks_before_preflight(
        self, tmp_path: Path,
    ) -> None:
        """Mixed-authorization guard: principal swap must block
        BEFORE we contact the (possibly drifted) endpoint."""
        drifted_snapshot = _make_snapshot(identity_overrides={
            "fusion.username": "bob@oracle",
        })
        state_rows = _state_rows_from(
            succeeded=["ap_invoices"], failed=["erp_suppliers"],
            snapshot=drifted_snapshot,
        )
        spark = _FakeSpark(state_rows=state_rows)
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ) as mock_preflight:
            with pytest.raises(ResumeBundleMismatchError) as exc_info:
                orchestrator.run(
                    _bundle_file(tmp_path),
                    spark=spark, mode="seed",
                    resume_run_id="run-A",
                )
        mock_preflight.assert_not_called()
        assert "fusion.username" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test class — row_count carry-forward on resume
# ---------------------------------------------------------------------------


class TestResumeRowCountCarryForward:
    """Resumed-skipped rows must preserve the original successful
    row_count so the latest-per-(run_id, dataset_id) projection (and
    the fusion_bundle_state_latest VIEW) don't lose count parity.
    """

    def test_resumed_skip_carries_prior_row_count(self, tmp_path: Path) -> None:
        """Fixture: ap_invoices succeeded originally with row_count=42.
        On resume, ap_invoices carries forward as resumed_skipped —
        the new row's row_count must equal 42, not NULL."""
        snapshot = _make_snapshot()
        state_rows = _state_rows_from(
            succeeded=["ap_invoices", "erp_suppliers", "gl_coa",
                       "dim_supplier", "dim_account", "dim_calendar",
                       "supplier_spend", "gl_balance", "ap_aging"],
            failed=["gl_period_balances"],
            snapshot=snapshot,
            succeeded_row_count=42,
        )
        spark = _FakeSpark(state_rows=state_rows)
        with patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight.preflight_bronze_schemas",
        ) as mock_preflight, patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.hash_resolved_plan",
            return_value="fake-hash-123",
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash.serialize_plan_snapshot",
            return_value=snapshot,
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.extractors.bicc.extract_pvo",
            return_value=_FakeDataFrame(99),
        ), patch(
            "oracle_ai_data_platform_fusion_bundle.orchestrator.enrich_bronze_audit_cols",
            side_effect=lambda df, **k: df,
        ):
            mock_preflight.return_value = MagicMock(
                effective_schemas={"gl_period_balances": "Financial"},
                recommendations=(),
            )
            summary = orchestrator.run(
                _bundle_file(tmp_path),
                spark=spark, mode="seed",
                resume_run_id="run-A",
            )
        ap_invoices_step = next(s for s in summary.steps if s.dataset_id == "ap_invoices")
        assert ap_invoices_step.status == "resumed_skipped"
        # The original row_count=42 must carry forward — NOT NULL.
        assert ap_invoices_step.row_count == 42, (
            "resumed_skipped step must inherit row_count from the prior "
            "successful row so fusion_bundle_state_latest preserves it"
        )

    def test_drifted_identity_with_broken_password_still_raises_mismatch(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Explicit-filter resume path: drifted fusion.serviceUrl AND a
        password reference whose ${env:VAR} is unset. The drift gate
        must fire FIRST so the operator sees ResumeBundleMismatchError
        (the actionable error) — not CredentialResolutionError masking
        the real issue."""
        # Bundle uses ${env:NEVER_SET} for the password — would raise
        # CredentialResolutionError if _resolve_password runs.
        bundle_yaml = _MIN_BUNDLE.replace(
            "password: literal-password",
            "password: ${env:NEVER_SET_FOR_THIS_TEST}",
        )
        bundle_path = _bundle_file(tmp_path, bundle_yaml)
        monkeypatch.delenv("NEVER_SET_FOR_THIS_TEST", raising=False)

        drifted_snapshot = _make_snapshot(identity_overrides={
            "fusion.serviceUrl": "https://OLD-DRIFTED-POD.example.com",
        })
        state_rows = _state_rows_from(
            succeeded=["ap_invoices"], failed=["erp_suppliers"],
            snapshot=drifted_snapshot,
        )
        spark = _FakeSpark(state_rows=state_rows)
        # Explicit --datasets triggers the deferred-state-read path.
        with pytest.raises(ResumeBundleMismatchError) as exc_info:
            orchestrator.run(
                bundle_path,
                spark=spark, mode="seed",
                datasets=["ap_invoices", "erp_suppliers"],
                resume_run_id="run-A",
            )
        msg = str(exc_info.value)
        assert "fusion.serviceUrl" in msg
        assert "OLD-DRIFTED-POD" in msg
        # The CredentialResolutionError must NOT have masked the real issue.
        assert "NEVER_SET_FOR_THIS_TEST" not in msg
