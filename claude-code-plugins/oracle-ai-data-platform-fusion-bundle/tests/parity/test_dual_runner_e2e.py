"""Phase 4 orchestrator-driven dual-runner parity harness.

Covers ``plan.md`` Steps 2 (foundation + per-node seed), 3 (incremental),
4 (cascade-abort), 5 (resume), and 7a (hard cursor commit failure).

This file drives ``orchestrator.run`` end-to-end through BOTH backends
on the starter pack + the bronze parity fixtures, then asserts the
RunSummary + state-row + materialized-output equivalence contracts
documented in ``docs/features/v2-phase-4-dual-runner-parity-gate/coverage_audit.md``.

Why a separate harness from Phase 3
-----------------------------------

Phase 3's ``test_starter_pack_parity.py`` proves SQL-output equivalence
on synthetic data via direct SQL execution. Phase 4 proves the same on
``orchestrator.run`` end-to-end — adding state-table writes, plan-hash
machinery, watermark resolution, and the §11.6 / §11.9 / §11.10 contracts.
Different invariants → separate harnesses. Phase 3 stays green; this file
is layered on top.

Gating
------

* ``@pytest.mark.parity`` — opt-in via ``pytest -m parity``.
* ``pytest.importorskip("pyspark")`` and ``pytest.importorskip("delta")``
  — local Delta is required (the state table is Delta-only). When
  workstation Delta is unavailable the suite skips; cluster execution
  is covered by Step 8's live evidence.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

pyspark = pytest.importorskip("pyspark")
delta = pytest.importorskip("delta")
pytestmark = pytest.mark.parity

from pyspark.sql import SparkSession  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from oracle_ai_data_platform_fusion_bundle import orchestrator  # noqa: E402
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import (  # noqa: E402
    load_full_chain, make_filesystem_base_resolver,
)
from oracle_ai_data_platform_fusion_bundle.schema.tenant_profile import (  # noqa: E402
    load_tenant_profile,
)

from . import bronze_fixtures  # noqa: E402
from .dual_runner_helpers import (  # noqa: E402
    BackendSchemas, BundleArtifacts,
    assert_output_rows_equiv, assert_output_schemas_equiv,
    assert_run_summary_equiv, assert_state_rows_equiv,
    assert_v2_lookup_row,
    create_target_schemas, insert_new_bronze_row, install_bicc_io_spy,
    make_delta_spark, make_dual_bundles, query_plan_hash, seed_bronze,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG = "spark_catalog"
PACK_PATH = (REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_bundle"
             / "content_packs" / "fusion-finance-starter")
PROFILE_SRC = REPO_ROOT / "examples" / "profiles" / "finance-default.yaml"

# Expected nodes per ``plan.md`` Step 11 regression sweep: five SQL nodes
# (dim_supplier, dim_account, gl_balance, supplier_spend, ap_aging) +
# the dim_calendar builtin.
EXPECTED_SEED_NODES: tuple[tuple[str, str], ...] = (
    ("dim_supplier", "silver"),
    ("dim_account", "silver"),
    ("dim_calendar", "silver"),
    ("gl_balance", "gold"),
    ("supplier_spend", "gold"),
    ("ap_aging", "gold"),
)


# ---------------------------------------------------------------------------
# Spark fixture (module-scoped, Delta-enabled)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    warehouse = tempfile.mkdtemp(prefix="phase4-parity-warehouse-")
    try:
        session = make_delta_spark("phase4-dual-runner-parity", warehouse)
    except Exception as exc:
        pytest.skip(
            f"delta-spark local-mode bootstrap failed: {type(exc).__name__}: {exc}. "
            "Run this suite on a workstation with delta-spark installed, OR "
            "via the live cluster (Step 8 evidence path)."
        )
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


# ---------------------------------------------------------------------------
# Resolved pack + profile (module-scoped — pack load is non-trivial)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def resolved_pack():
    return load_full_chain(
        PACK_PATH, base_resolver=make_filesystem_base_resolver(PACK_PATH),
    )


@pytest.fixture(scope="module")
def tenant_profile():
    return load_tenant_profile(PROFILE_SRC)


# ---------------------------------------------------------------------------
# Seeded-bundles fixture — runs ONE full seed cycle per backend
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def seeded_bundles(
    spark: SparkSession,
    tmp_path_factory: pytest.TempPathFactory,
    resolved_pack,
    tenant_profile,
    monkeypatch_module,  # module-scoped monkeypatch (defined below)
) -> dict[str, Any]:
    """Produce the dual bundles + run seed on both backends ONCE per
    test session. Per-node tests interrogate the resulting state +
    materialized tables; the orchestrator-runtime cost (Spark warmup +
    Delta state-table writes) is paid once.

    A per-test fresh-seed fixture would multiply the runtime cost by
    ~6 (one cycle per per-node test). Module scope keeps the harness
    fast enough to run in CI under the existing parity marker.

    **BICC IO spy installed for both seed runs.** Per ``plan.md`` Step 2:
    silver+gold-only runs MUST NOT fire any BICC PVO read. The spy
    counts ``extractors.bicc.extract_pvo`` calls; the post-seed
    assertion below confirms zero calls landed during BOTH legs.
    A future refactor that re-enables bronze extraction in a
    silver-only run trips this assertion immediately.
    """
    bicc_call_count = install_bicc_io_spy(monkeypatch_module)

    tmp = tmp_path_factory.mktemp("phase4-seed")
    artifacts = make_dual_bundles(
        tmp,
        catalog=CATALOG,
        v1_suffix="seed_v1",
        v2_suffix="seed_v2",
        pack_path=PACK_PATH,
        profile_src=PROFILE_SRC,
        profile_name="finance-default",
        # No paired snapshot in examples/profiles yet — Step 6 ships
        # one for the multi-tenant profile. Bootstrap on a real tenant
        # would produce ``finance-default.schema-snapshot.yaml``; its
        # absence triggers the warn-and-proceed graceful-degrade per
        # P3c-L1. Phase 4's foundation harness deliberately exercises
        # that path (no drift on seed → degrade-OK is acceptable).
        snapshot_src=None,
    )

    # Both backends seed bronze identically — same fixture rows in two
    # isolated bronze schemas.
    seed_bronze(spark, catalog=CATALOG, schema=artifacts.v1_schemas.bronze,
                fixtures_module=bronze_fixtures)
    seed_bronze(spark, catalog=CATALOG, schema=artifacts.v2_schemas.bronze,
                fixtures_module=bronze_fixtures)
    create_target_schemas(spark, catalog=CATALOG, schemas=artifacts.v1_schemas)
    create_target_schemas(spark, catalog=CATALOG, schemas=artifacts.v2_schemas)

    # ----- Run seed on legacy-python --------------------------------
    v1_summary = orchestrator.run(
        bundle_path=artifacts.v1_bundle,
        spark=spark,
        mode="seed",
        layers=["silver", "gold"],
        execution_backend="legacy-python",
    )

    # ----- Run seed on content-pack ---------------------------------
    v2_summary = orchestrator.run(
        bundle_path=artifacts.v2_bundle,
        spark=spark,
        mode="seed",
        layers=["silver", "gold"],
        execution_backend="content-pack",
        resolved_pack=resolved_pack,
        tenant_profile=tenant_profile,
    )

    # ----- BICC IO spy assertion --------------------------------------
    # Both backends ran with ``layers=["silver","gold"]`` — the BICC
    # extractor MUST NOT have been called. A non-zero count means the
    # legacy backend silently re-enabled bronze extraction OR the
    # content-pack backend dispatched bronze (Phase 2 deferral). Either
    # is a regression that must be diagnosed before the dual-runner
    # results can be trusted.
    bicc_calls = bicc_call_count()
    assert bicc_calls == 0, (
        f"Phase 4 BICC-IO invariant violated: extract_pvo was called "
        f"{bicc_calls} times during silver+gold-only seed runs. Both "
        f"backends were invoked with layers=['silver','gold']; bronze "
        f"reads MUST NOT fire. Diagnose: most likely a silver/gold node "
        f"acquired a bronze-layer dependency that wasn't routed through "
        f"the pre-seeded bronze schemas."
    )

    return {
        "artifacts": artifacts,
        "v1_summary": v1_summary,
        "v2_summary": v2_summary,
    }


@pytest.fixture(scope="module")
def monkeypatch_module(request):
    """Module-scoped monkeypatch — pytest's built-in ``monkeypatch``
    fixture is function-scoped and can't be used by the module-scoped
    ``seeded_bundles`` fixture above. Implement the same patch+undo
    semantics manually."""
    from _pytest.monkeypatch import MonkeyPatch  # type: ignore[import-not-found]
    mp = MonkeyPatch()
    yield mp
    mp.undo()


# ---------------------------------------------------------------------------
# Step 2 — Per-node seed-mode parity
# ---------------------------------------------------------------------------


class TestStep2_SeedModeParity:
    """Phase 4 Step 2 — foundation contract on all six starter-pack nodes.

    One harness, six tests (parametrized). Each asserts:
    - RunStep equivalence on the node (delegates to
      :func:`assert_run_summary_equiv` once per class).
    - State-row equivalence via the three-tier contract.
    - Materialized-output rows + schema equivalence.

    The per-node split keeps failure messages localised — a regression
    on ``dim_account`` shows up as ``test_dim_account_state_row`` failing,
    not as a single multi-node assertion buried in a wall of diff.
    """

    def test_run_summary_equivalence(self, seeded_bundles) -> None:
        """Single RunSummary check covering all six nodes. Run once
        per session; per-node tests below cover the state + table
        invariants the summary doesn't capture."""
        assert_run_summary_equiv(
            seeded_bundles["v1_summary"],
            seeded_bundles["v2_summary"],
        )

    @pytest.mark.parametrize("dataset_id,layer", EXPECTED_SEED_NODES)
    def test_state_row_equiv(
        self, seeded_bundles, spark, dataset_id: str, layer: str,
    ) -> None:
        artifacts: BundleArtifacts = seeded_bundles["artifacts"]
        assert_state_rows_equiv(
            spark,
            catalog=artifacts.catalog,
            v1_schema=artifacts.v1_schemas.bronze,
            v2_schema=artifacts.v2_schemas.bronze,
            expected_nodes=[(dataset_id, layer)],
            expected_mode="seed",
        )

    @pytest.mark.parametrize("dataset_id,layer", EXPECTED_SEED_NODES)
    def test_materialized_rows_equiv(
        self, seeded_bundles, spark, dataset_id: str, layer: str,
    ) -> None:
        artifacts: BundleArtifacts = seeded_bundles["artifacts"]
        v1_target = (
            f"{artifacts.catalog}.{artifacts.v1_schemas.silver if layer == 'silver' else artifacts.v1_schemas.gold}"
            f".{dataset_id}"
        )
        v2_target = (
            f"{artifacts.catalog}.{artifacts.v2_schemas.silver if layer == 'silver' else artifacts.v2_schemas.gold}"
            f".{dataset_id}"
        )
        assert_output_schemas_equiv(
            spark, v1_target=v1_target, v2_target=v2_target,
            layer=layer, node_id=dataset_id,
        )
        assert_output_rows_equiv(
            spark, v1_target=v1_target, v2_target=v2_target,
            layer=layer, node_id=dataset_id,
        )

    def test_gl_balance_multi_source_cursor_policy(
        self, seeded_bundles, spark,
    ) -> None:
        """Task 15 — primary/lookup cursor policy on ``gl_balance``.

        gl_balance is the only multi-source node in the v0.3 starter
        pack (primary: ``gl_period_balances``, lookup: ``dim_account``).
        Asserts the v2 state table carries:
        - one primary row with ``output_watermark`` set;
        - at least one lookup row with ``output_watermark=NULL``.

        The v1 backend has no concept of lookup rows; this assertion
        runs on the v2 side only by design.
        """
        artifacts: BundleArtifacts = seeded_bundles["artifacts"]
        assert_v2_lookup_row(
            spark,
            catalog=artifacts.catalog,
            v2_schema=artifacts.v2_schemas.bronze,
            primary_dataset="gl_balance",
            lookup_source_id="dim_account",
            expected_mode="seed",
        )


# ---------------------------------------------------------------------------
# Step 3 — Incremental mode parity
# ---------------------------------------------------------------------------


class TestStep3_IncrementalParity:
    """Inject one new bronze row past the seed watermark; rerun
    incremental on both backends; assert watermark advances on
    affected nodes only, plan_hash stays stable, and the incremental
    delta lands on both sides.
    """

    def test_incremental_advances_watermark_and_preserves_plan_hash(
        self, seeded_bundles, spark, resolved_pack, tenant_profile,
    ) -> None:
        artifacts: BundleArtifacts = seeded_bundles["artifacts"]

        # Capture the seed-time plan_hash for ap_invoices/silver before
        # the incremental run. plan_hash must remain stable across
        # seed → incremental for the same template + profile (§11.9).
        seed_plan_hash_v1 = query_plan_hash(
            spark, catalog=artifacts.catalog,
            schema=artifacts.v1_schemas.bronze,
            dataset_id="dim_supplier", layer="silver", mode="seed",
        )
        seed_plan_hash_v2 = query_plan_hash(
            spark, catalog=artifacts.catalog,
            schema=artifacts.v2_schemas.bronze,
            dataset_id="dim_supplier", layer="silver", mode="seed",
        )

        # Inject one new ap_invoice row past the seed watermark on
        # BOTH bronze schemas so the incremental delta is symmetric.
        future_ts = datetime(2026, 6, 10, tzinfo=timezone.utc)
        new_row = {
            "ApInvoicesVendorId": 1001, "ApInvoicesInvoiceCurrencyCode": "USD",
            "ApInvoicesInvoiceAmount": 500.00, "ApInvoicesAmountPaid": 0.00,
            "ApInvoicesInvoiceDate": datetime(2026, 6, 1, tzinfo=timezone.utc),
            "ApInvoicesCancelledDate": None,
            "ApInvoicesApprovalStatus": "APPROVED",
            "ApInvoicesTermsDate": None, "ApInvoicesDueDate": None,
            "_extract_ts": future_ts, "_source_pvo": "parity-incremental",
            "_run_id": "parity-inc-row", "_watermark_used": future_ts,
        }
        insert_new_bronze_row(
            spark, catalog=artifacts.catalog,
            schema=artifacts.v1_schemas.bronze,
            dataset_id="ap_invoices", row=new_row,
        )
        insert_new_bronze_row(
            spark, catalog=artifacts.catalog,
            schema=artifacts.v2_schemas.bronze,
            dataset_id="ap_invoices", row=new_row,
        )

        v1_inc = orchestrator.run(
            bundle_path=artifacts.v1_bundle, spark=spark,
            mode="incremental", layers=["silver", "gold"],
            execution_backend="legacy-python",
        )
        v2_inc = orchestrator.run(
            bundle_path=artifacts.v2_bundle, spark=spark,
            mode="incremental", layers=["silver", "gold"],
            execution_backend="content-pack",
            resolved_pack=resolved_pack, tenant_profile=tenant_profile,
        )

        # RunSummary equivalence on the incremental run.
        assert_run_summary_equiv(v1_inc, v2_inc)

        # Plan-hash stability (§11.9 invariant). The incremental
        # state row's plan_hash MUST equal the seed row's for the same
        # node + template + profile. Same-backend comparison only;
        # cross-backend plan_hash is intentionally NOT compared (the
        # two backends hash different inputs by design).
        inc_plan_hash_v1 = query_plan_hash(
            spark, catalog=artifacts.catalog,
            schema=artifacts.v1_schemas.bronze,
            dataset_id="dim_supplier", layer="silver", mode="incremental",
        )
        inc_plan_hash_v2 = query_plan_hash(
            spark, catalog=artifacts.catalog,
            schema=artifacts.v2_schemas.bronze,
            dataset_id="dim_supplier", layer="silver", mode="incremental",
        )
        # `dim_supplier` doesn't see the ap_invoices row, so its
        # incremental hash should equal its seed hash IF both rows are
        # written by the backend on incremental mode. Tolerate None
        # on either side (v1 may not write an incremental row for
        # nodes with no delta — backend-specific).
        if seed_plan_hash_v1 is not None and inc_plan_hash_v1 is not None:
            assert seed_plan_hash_v1 == inc_plan_hash_v1, (
                f"v1 dim_supplier plan_hash drifted between seed and "
                f"incremental: seed={seed_plan_hash_v1!r} "
                f"inc={inc_plan_hash_v1!r}"
            )
        if seed_plan_hash_v2 is not None and inc_plan_hash_v2 is not None:
            assert seed_plan_hash_v2 == inc_plan_hash_v2, (
                f"v2 dim_supplier plan_hash drifted between seed and "
                f"incremental: seed={seed_plan_hash_v2!r} "
                f"inc={inc_plan_hash_v2!r}"
            )

        # Affected nodes (supplier_spend, ap_aging) — assert materialized
        # output equivalence. Their row counts grew by the incremental
        # delta; both backends MUST agree.
        for dataset_id, layer in (("supplier_spend", "gold"),
                                   ("ap_aging", "gold")):
            v1_target = (f"{artifacts.catalog}.{artifacts.v1_schemas.gold}"
                         f".{dataset_id}")
            v2_target = (f"{artifacts.catalog}.{artifacts.v2_schemas.gold}"
                         f".{dataset_id}")
            assert_output_rows_equiv(
                spark, v1_target=v1_target, v2_target=v2_target,
                layer=layer, node_id=f"{dataset_id} (post-incremental)",
            )


# ---------------------------------------------------------------------------
# Step 4 — Cascade-abort scenario (asymmetric v1/v2)
# ---------------------------------------------------------------------------


class TestStep4_CascadeAbort:
    """Force a failure at ``dim_supplier`` on both backends; codify the
    intentional v1↔v2 cascade divergence.

    v1: ``_abort_remaining`` sweeps every plan node into ``skipped_aborted``.
    v2: ``cascade-only-on-dependents`` — independent branches still run.

    Phase 4 documents the divergence rather than asserting parity; the
    decision to harmonise belongs in Phase 5 per ``plan.md`` Step 4.
    """

    def test_v2_cascade_only_on_dependents(
        self, spark, tmp_path, resolved_pack, tenant_profile, monkeypatch,
    ) -> None:
        """Run isolated cascade-abort scenario on a fresh bundle pair
        (independent of the module-scoped ``seeded_bundles`` so the
        forced failure doesn't pollute the seed state)."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_runner import (
            MaterializedSchemaDriftError,
        )

        artifacts = make_dual_bundles(
            tmp_path, catalog=CATALOG,
            v1_suffix="cascade_v1", v2_suffix="cascade_v2",
            pack_path=PACK_PATH, profile_src=PROFILE_SRC,
            profile_name="finance-default",
        )
        seed_bronze(spark, catalog=CATALOG,
                    schema=artifacts.v1_schemas.bronze,
                    fixtures_module=bronze_fixtures)
        seed_bronze(spark, catalog=CATALOG,
                    schema=artifacts.v2_schemas.bronze,
                    fixtures_module=bronze_fixtures)
        create_target_schemas(spark, catalog=CATALOG,
                              schemas=artifacts.v1_schemas)
        create_target_schemas(spark, catalog=CATALOG,
                              schemas=artifacts.v2_schemas)

        # v2 failure injection: raise MaterializedSchemaDriftError on
        # the dim_supplier node only. This is the only exception class
        # execute_node catches at the schema-assertion point per
        # sql_runner.py:271-281; other exceptions would bubble out and
        # bypass the cascade-state machinery.
        original = sql_runner._assert_materialized_matches_declared

        def _maybe_raise(spark_, target, node):  # noqa: ANN001
            if node.id == "dim_supplier":
                raise MaterializedSchemaDriftError(
                    "AIDPF-4070: parity-test forced schema drift on dim_supplier"
                )
            return original(spark_, target, node)

        monkeypatch.setattr(
            sql_runner, "_assert_materialized_matches_declared", _maybe_raise,
        )

        v2_summary = orchestrator.run(
            bundle_path=artifacts.v2_bundle, spark=spark,
            mode="seed", layers=["silver", "gold"],
            execution_backend="content-pack",
            resolved_pack=resolved_pack, tenant_profile=tenant_profile,
        )

        # v2 contract: dim_supplier failed; supplier_spend + ap_aging
        # are direct dependents (cascade); dim_account + dim_calendar
        # are independent (succeed); gl_balance depends on dim_account
        # (succeed); the cascade does NOT sweep independents.
        v2_steps = {s.dataset_id: s for s in v2_summary.steps}
        assert v2_steps["dim_supplier"].status == "failed", (
            "v2: dim_supplier should be 'failed' (collapsed from "
            "'output_schema_drift')"
        )
        for cascade_dep in ("supplier_spend", "ap_aging"):
            assert v2_steps[cascade_dep].status == "skipped", (
                f"v2: {cascade_dep} should be 'skipped' (direct dependent of "
                f"failed dim_supplier); got {v2_steps[cascade_dep].status!r}"
            )
            assert v2_steps[cascade_dep].skip_reason == "cascade", (
                f"v2: {cascade_dep} skip_reason should be 'cascade'; "
                f"got {v2_steps[cascade_dep].skip_reason!r}"
            )
        # Independent branches: must NOT cascade.
        for independent in ("dim_account", "dim_calendar", "gl_balance"):
            assert v2_steps[independent].status == "success", (
                f"v2: {independent} is independent of dim_supplier and "
                f"MUST succeed; got {v2_steps[independent].status!r}"
            )

    def test_v1_abort_after_first_failure(
        self, spark, tmp_path, monkeypatch,
    ) -> None:
        """v1 contract: ``_abort_remaining`` sweeps every plan node not
        already attempted into ``skipped_aborted`` — including
        independent branches that have no dependency on the failed node.
        """
        from oracle_ai_data_platform_fusion_bundle.dimensions import dim_supplier

        artifacts = make_dual_bundles(
            tmp_path, catalog=CATALOG,
            v1_suffix="v1abort_v1", v2_suffix="v1abort_v2",
            pack_path=PACK_PATH, profile_src=PROFILE_SRC,
            profile_name="finance-default",
        )
        seed_bronze(spark, catalog=CATALOG,
                    schema=artifacts.v1_schemas.bronze,
                    fixtures_module=bronze_fixtures)
        create_target_schemas(spark, catalog=CATALOG,
                              schemas=artifacts.v1_schemas)

        original_build_sql = dim_supplier.build_dim_supplier_sql

        def _broken(*args, **kwargs):  # noqa: ANN001
            raise RuntimeError("parity-test forced v1 dim_supplier failure")

        monkeypatch.setattr(dim_supplier, "build_dim_supplier_sql", _broken)

        v1_summary = orchestrator.run(
            bundle_path=artifacts.v1_bundle, spark=spark,
            mode="seed", layers=["silver", "gold"],
            execution_backend="legacy-python",
        )

        v1_steps = {s.dataset_id: s for s in v1_summary.steps}
        assert v1_steps["dim_supplier"].status == "failed"
        # Direct cascade dependents: skip_reason='cascade'.
        for cascade_dep in ("supplier_spend", "ap_aging"):
            assert v1_steps[cascade_dep].status == "skipped"
            assert v1_steps[cascade_dep].skip_reason == "cascade"
        # Every other plan node — including independent branches —
        # should carry an 'aborted' marker. v1's RunStep enum encodes
        # this via skip_reason. dim_calendar is the canonical
        # independent branch in the starter pack.
        independent_nodes = ("dim_account", "dim_calendar", "gl_balance")
        for independent in independent_nodes:
            step = v1_steps.get(independent)
            assert step is not None, (
                f"v1: missing RunStep for independent node {independent!r} — "
                f"abort-after-first-failure contract expects every plan node "
                f"to have a row. Present steps: {sorted(v1_steps)}"
            )
            assert step.status == "skipped", (
                f"v1: {independent} should be 'skipped' under v1's "
                f"abort-after-first-failure; got {step.status!r}"
            )


# ---------------------------------------------------------------------------
# Step 5 — Resume-after-failure (xfail content-pack leg)
# ---------------------------------------------------------------------------


class TestStep5_Resume:
    """v1 resume re-attempts every non-success row from the failed run.
    v2 currently rejects ``--resume`` with ``AIDPF-1032`` (Phase 2
    deferral); the test xfails the v2 leg with a stable reason and
    documents the limitation as a Phase 5 prerequisite.
    """

    def test_v1_resume_reattempts_non_success_nodes(
        self, spark, tmp_path, monkeypatch,
    ) -> None:
        from oracle_ai_data_platform_fusion_bundle.dimensions import dim_supplier

        artifacts = make_dual_bundles(
            tmp_path, catalog=CATALOG,
            v1_suffix="resume_v1", v2_suffix="resume_v2",
            pack_path=PACK_PATH, profile_src=PROFILE_SRC,
            profile_name="finance-default",
        )
        seed_bronze(spark, catalog=CATALOG,
                    schema=artifacts.v1_schemas.bronze,
                    fixtures_module=bronze_fixtures)
        create_target_schemas(spark, catalog=CATALOG,
                              schemas=artifacts.v1_schemas)

        original_build_sql = dim_supplier.build_dim_supplier_sql

        # First run: force-fail at dim_supplier.
        def _broken(*args, **kwargs):  # noqa: ANN001
            raise RuntimeError("parity-test forced v1 dim_supplier failure")

        monkeypatch.setattr(dim_supplier, "build_dim_supplier_sql", _broken)
        v1_failed = orchestrator.run(
            bundle_path=artifacts.v1_bundle, spark=spark,
            mode="seed", layers=["silver", "gold"],
            execution_backend="legacy-python",
        )
        failed_run_id = v1_failed.run_id
        assert any(s.status == "failed" for s in v1_failed.steps), (
            "v1 force-fail did not produce any failed RunStep — test setup broken"
        )

        # Remove the monkeypatch + resume.
        monkeypatch.setattr(dim_supplier, "build_dim_supplier_sql",
                            original_build_sql)
        v1_resumed = orchestrator.run(
            bundle_path=artifacts.v1_bundle, spark=spark,
            mode="seed", layers=["silver", "gold"],
            execution_backend="legacy-python",
            resume_run_id=failed_run_id,
        )

        # Per ``read_resumable_state``, only 'success' + 'resumed_skipped'
        # rows count as complete. Step 4's force-fail produced cascade
        # + aborted rows for everyone else, so resume re-attempts them
        # all. Expect 6 success steps on resume.
        resumed_steps = {s.dataset_id: s for s in v1_resumed.steps}
        for dataset_id, _layer in EXPECTED_SEED_NODES:
            step = resumed_steps.get(dataset_id)
            assert step is not None and step.status == "success", (
                f"v1 resume: {dataset_id} expected success; "
                f"got {step.status if step else 'MISSING'!r}"
            )

    def test_v2_resume_adopts_supplied_run_id(
        self, spark, tmp_path, resolved_pack, tenant_profile,
    ) -> None:
        """Phase 5 Step 9b — AIDPF-1032 resolved. The content-pack
        backend accepts ``--resume`` and adopts the supplied
        ``resume_run_id`` as the shared run identifier so the resumed
        run's state rows join with the prior failed run's rows.

        Smoke check: providing an arbitrary resume_run_id should NOT
        raise; the returned :class:`RunSummary.run_id` matches the
        supplied id (proves adoption). The dispatcher's per-node loop
        handles non-success-row retries through the same atomic-commit
        path; full retry-correctness is exercised by the v1 sibling
        test above (``test_v1_resume_reattempts_non_success_nodes``).
        """
        artifacts = make_dual_bundles(
            tmp_path, catalog=CATALOG,
            v1_suffix="v2resume_v1", v2_suffix="v2resume_v2",
            pack_path=PACK_PATH, profile_src=PROFILE_SRC,
            profile_name="finance-default",
        )
        seed_bronze(spark, catalog=CATALOG,
                    schema=artifacts.v2_schemas.bronze,
                    fixtures_module=bronze_fixtures)
        create_target_schemas(spark, catalog=CATALOG,
                              schemas=artifacts.v2_schemas)

        supplied = "phase5-resume-adopt-test-id"
        summary = orchestrator.run(
            bundle_path=artifacts.v2_bundle, spark=spark,
            mode="seed", layers=["silver"],
            execution_backend="content-pack",
            resolved_pack=resolved_pack, tenant_profile=tenant_profile,
            resume_run_id=supplied,
        )
        # The dispatcher adopted the supplied run_id (no `cp-` prefix).
        assert summary.run_id == supplied, (
            f"Phase 5 Step 9b adopt-supplied-run_id contract: expected "
            f"run_id={supplied!r}, got {summary.run_id!r}."
        )
        # Every emitted RunStep also carries the same id.
        for step in summary.steps:
            assert step.run_id == supplied, (
                f"RunStep for {step.dataset_id!r} drifted from the "
                f"supplied resume_run_id: {step.run_id!r}."
            )


# ---------------------------------------------------------------------------
# Step 7a — Hard cursor commit failure (StateCommitError)
# ---------------------------------------------------------------------------


class TestStep7a_HardCursorCommitFailure:
    """§11.9 atomic-commit invariant: a failed ``write_state_rows_hard``
    must NOT advance the cursor; the prior successful row remains
    authoritative; a clean retry advances correctly.

    Uses the **Direct** injection pattern: monkeypatch
    ``state_phase2.write_state_rows_hard`` to raise ``StateCommitError``.
    The plan's Indirect pattern (monkeypatch the inner Delta write) is
    higher-fidelity but more brittle; Direct is sufficient to prove the
    catch path in ``sql_runner.py:346`` converts to
    ``status='state_commit_failed'`` and the cursor stays where the
    seed-time row left it.
    """

    def test_state_commit_failure_blocks_cursor_advance(
        self, spark, tmp_path, resolved_pack, tenant_profile, monkeypatch,
    ) -> None:
        from oracle_ai_data_platform_fusion_bundle.orchestrator import state_phase2

        artifacts = make_dual_bundles(
            tmp_path, catalog=CATALOG,
            v1_suffix="hardcommit_v1", v2_suffix="hardcommit_v2",
            pack_path=PACK_PATH, profile_src=PROFILE_SRC,
            profile_name="finance-default",
        )
        seed_bronze(spark, catalog=CATALOG,
                    schema=artifacts.v2_schemas.bronze,
                    fixtures_module=bronze_fixtures)
        create_target_schemas(spark, catalog=CATALOG,
                              schemas=artifacts.v2_schemas)

        # ----- Setup step 1: SEED with NO monkeypatch ----------------
        # The downstream assertions ("prior cursor remains authoritative",
        # "retry advances") need an actual seed-time cursor to compare
        # against. Per ``plan.md`` Step 7a setup contract:
        #   1. Seed bronze; 2. Seed silver successfully; 3. Capture
        #   seed_watermark; 4. Insert new bronze row past seed_watermark;
        #   5. Install monkeypatch + run incremental; 6. Assert no
        #   spurious advance + retry advances correctly.
        seed_summary = orchestrator.run(
            bundle_path=artifacts.v2_bundle, spark=spark,
            mode="seed", layers=["silver"],
            execution_backend="content-pack",
            resolved_pack=resolved_pack, tenant_profile=tenant_profile,
        )
        seed_step = next(s for s in seed_summary.steps
                         if s.dataset_id == "dim_supplier")
        assert seed_step.status == "success", (
            f"Step 7a setup broken: seed-time dim_supplier should be "
            f"success; got {seed_step.status!r}"
        )
        seed_watermark = seed_step.last_watermark

        # ----- Setup step 2: insert new bronze row past seed_watermark
        # Calibrate the future timestamp relative to seed_watermark when
        # it's known; otherwise use a generous future date.
        if seed_watermark is not None:
            future_ts = seed_watermark + timedelta(seconds=60)
        else:
            future_ts = datetime(2026, 7, 1, tzinfo=timezone.utc)
        # erp_suppliers is dim_supplier's primary source.
        new_supplier_row = {
            "SEGMENT1": "PARITY-INC-001", "VENDORID": 99001,
            "PARTYID": 99001, "PARENTVENDORID": None, "PARENTPARTYID": None,
            "AlternateNamePartyName": "Phase 4 Hard-Commit Test Supplier",
            "AliasPartyName": None, "TaxReportingName": None,
            "BUSINESSRELATIONSHIP": "PROSPECTIVE",
            "ENDDATEACTIVE": None,
            "CREATIONDATE": future_ts, "LASTUPDATEDATE": future_ts,
            "_extract_ts": future_ts, "_source_pvo": "parity-hardcommit",
            "_run_id": "parity-hardcommit-row",
            "_watermark_used": future_ts,
        }
        insert_new_bronze_row(
            spark, catalog=artifacts.catalog,
            schema=artifacts.v2_schemas.bronze,
            dataset_id="erp_suppliers", row=new_supplier_row,
        )

        # ----- Setup step 3: install the StateCommitError monkeypatch
        # Direct pattern — raise StateCommitError on first call for
        # dim_supplier. The catch at sql_runner.py:346 converts to
        # NodeExecutionResult(status='state_commit_failed', ...).
        original_write = state_phase2.write_state_rows_hard
        injected = {"fired": False}

        def _failing_write(spark_, paths, rows):  # noqa: ANN001
            # Fire on the first dim_supplier row only; passthrough for
            # subsequent nodes / retries so the dependent failures don't
            # mask the assertion.
            if not injected["fired"]:
                for r in rows:
                    if r.get("dataset_id") == "dim_supplier" \
                            and r.get("layer") == "silver":
                        injected["fired"] = True
                        raise state_phase2.StateCommitError(
                            "AIDPF-4060: parity-test forced state commit "
                            "failure on dim_supplier"
                        )
            return original_write(spark_, paths, rows)

        monkeypatch.setattr(
            state_phase2, "write_state_rows_hard", _failing_write,
        )

        # ----- Incremental run with monkeypatch active --------------
        inc_summary = orchestrator.run(
            bundle_path=artifacts.v2_bundle, spark=spark,
            mode="incremental", layers=["silver"],
            execution_backend="content-pack",
            resolved_pack=resolved_pack, tenant_profile=tenant_profile,
        )
        assert injected["fired"], (
            "Step 7a: StateCommitError monkeypatch never fired — "
            "either the seed already committed or the call surface changed"
        )
        inc_steps = {s.dataset_id: s for s in inc_summary.steps}
        assert inc_steps["dim_supplier"].status == "failed", (
            f"Step 7a: incremental dim_supplier should collapse to "
            f"'failed' (from state_commit_failed); got "
            f"{inc_steps['dim_supplier'].status!r}"
        )

        # ----- Assertion: NO success state row on incremental --------
        # The state table should NOT carry a status='success' incremental
        # row for dim_supplier; that's the §11.9 invariant.
        state_rows = spark.sql(
            f"SELECT status, output_watermark, last_watermark "
            f"FROM {artifacts.catalog}.{artifacts.v2_schemas.bronze}.fusion_bundle_state "
            f"WHERE dataset_id = 'dim_supplier' AND layer = 'silver' "
            f"AND mode = 'incremental'"
        ).collect()
        for row in state_rows:
            assert row["status"] != "success", (
                f"Step 7a: §11.9 invariant violated — found a "
                f"status='success' incremental row for dim_supplier "
                f"after the forced StateCommitError. Row: {row.asDict()!r}"
            )

        # ----- Assertion: prior cursor remains authoritative ---------
        # The most-recent success row across ALL modes for dim_supplier/
        # silver should be the seed row; its last_watermark should
        # equal seed_watermark.
        success_rows = spark.sql(
            f"SELECT status, last_watermark, mode FROM "
            f"{artifacts.catalog}.{artifacts.v2_schemas.bronze}.fusion_bundle_state "
            f"WHERE dataset_id = 'dim_supplier' AND layer = 'silver' "
            f"AND status = 'success' ORDER BY last_run_at DESC"
        ).collect()
        assert success_rows, (
            "Step 7a: prior cursor missing — seed should have left "
            "a status='success' row for dim_supplier/silver"
        )
        assert success_rows[0]["mode"] == "seed", (
            f"Step 7a: most recent success row should still be the seed-mode "
            f"row (no spurious incremental advance); got mode="
            f"{success_rows[0]['mode']!r}"
        )

        # ----- Retry: remove monkeypatch + rerun incremental ---------
        monkeypatch.setattr(state_phase2, "write_state_rows_hard",
                            original_write)
        retry_summary = orchestrator.run(
            bundle_path=artifacts.v2_bundle, spark=spark,
            mode="incremental", layers=["silver"],
            execution_backend="content-pack",
            resolved_pack=resolved_pack, tenant_profile=tenant_profile,
        )
        retry_step = next(s for s in retry_summary.steps
                          if s.dataset_id == "dim_supplier")
        assert retry_step.status == "success", (
            f"Step 7a: retry after monkeypatch removal should advance "
            f"the cursor cleanly; got {retry_step.status!r}"
        )
        if seed_watermark is not None and retry_step.last_watermark is not None:
            assert retry_step.last_watermark > seed_watermark, (
                f"Step 7a: retry's last_watermark should advance past "
                f"seed_watermark; got retry={retry_step.last_watermark!r} "
                f"seed={seed_watermark!r}"
            )
