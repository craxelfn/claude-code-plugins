"""Phase 4 Step 9 — concurrent-runs precheck.

LIMITS.md §L-Resume-Concurrency defers cross-run locking / leader
election to Phase γ. Phase 4 documents the CURRENT behaviour: what
happens when two ``orchestrator.run`` calls land against the same
target schema at the same time?

Likely outcomes (the test documents whichever holds, doesn't enforce):
- Both writes land in the same state table with interleaved rows.
- Per-(dataset_id, run_id) latest-per-key produces coherent terminal
  output, but the intermediate state-table view is inconsistent.
- One or both runs may fail on a Delta concurrent-write conflict —
  ``ConcurrentAppendException``-shaped.

The test result lands as a LIMITS.md ``P4-L<n>`` entry, NOT as a
behavioural pass-fail assertion on the runs themselves. Phase 5
reads the LIMITS row and decides whether to ship operator-discipline
guidance or wait for Phase γ's lock primitive.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

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
    create_target_schemas, make_delta_spark, make_dual_bundles, seed_bronze,
)


CATALOG = "spark_catalog"
PACK_PATH = (REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_bundle"
             / "content_packs" / "fusion-finance-starter")
PROFILE_SRC = REPO_ROOT / "examples" / "profiles" / "finance-default.yaml"


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    warehouse = tempfile.mkdtemp(prefix="phase4-concurrent-warehouse-")
    try:
        # Allow more parallel threads than the default harness — the
        # whole point of this suite is to exercise concurrent writes.
        session = make_delta_spark("phase4-concurrent-runs", warehouse)
    except Exception as exc:
        pytest.skip(
            f"delta-spark local-mode bootstrap failed: {type(exc).__name__}: {exc}"
        )
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(scope="module")
def resolved_pack():
    return load_full_chain(
        PACK_PATH, base_resolver=make_filesystem_base_resolver(PACK_PATH),
    )


@pytest.fixture(scope="module")
def tenant_profile():
    return load_tenant_profile(PROFILE_SRC)


class TestStep9_ConcurrentRunsBehaviour:
    """Spin two ``orchestrator.run`` calls into the SAME target schema
    from threads. Record observed terminal state — the LIMITS row in
    the ship-ready report cites what we saw.
    """

    def test_two_concurrent_seeds_observed_behaviour(
        self, spark, tmp_path, resolved_pack, tenant_profile,
    ) -> None:
        """Two threads, two calls to ``orchestrator.run`` against the
        SAME bundle path. Records both summaries' run_ids + step counts
        + which exceptions (if any) escaped. Asserts ONLY the harness
        survival contract: at least one call must complete (either
        success or controlled failure) — a deadlock / hang would be
        the regression the precheck guards against.
        """
        artifacts = make_dual_bundles(
            tmp_path, catalog=CATALOG,
            v1_suffix="concurrent_v1", v2_suffix="concurrent_v2",
            pack_path=PACK_PATH, profile_src=PROFILE_SRC,
            profile_name="finance-default",
        )
        # Use the v2 bundle for both threads (content-pack backend is
        # where the §11.9 hard-commit invariant lives — concurrency
        # against legacy-python has been deployed for years; the new
        # behaviour to characterize is the content-pack path).
        seed_bronze(spark, catalog=CATALOG,
                    schema=artifacts.v2_schemas.bronze,
                    fixtures_module=bronze_fixtures)
        create_target_schemas(spark, catalog=CATALOG,
                              schemas=artifacts.v2_schemas)

        results: dict[str, Any] = {"t1": None, "t2": None}
        errors: dict[str, BaseException | None] = {"t1": None, "t2": None}

        def _go(key: str) -> None:
            try:
                results[key] = orchestrator.run(
                    bundle_path=artifacts.v2_bundle, spark=spark,
                    mode="seed", layers=["silver"],
                    execution_backend="content-pack",
                    resolved_pack=resolved_pack, tenant_profile=tenant_profile,
                )
            except BaseException as exc:  # noqa: BLE001 — captureall
                errors[key] = exc

        t1 = threading.Thread(target=_go, args=("t1",), daemon=True)
        t2 = threading.Thread(target=_go, args=("t2",), daemon=True)
        t1.start()
        t2.start()
        # Generous timeout — synthetic fixture is small. The precheck
        # guards against deadlock, not slowness.
        t1.join(timeout=180)
        t2.join(timeout=180)

        assert not t1.is_alive() and not t2.is_alive(), (
            "Phase 4 concurrent-runs precheck — at least one thread "
            "deadlocked / hung past the 180s budget. This is a Phase 4 "
            "BLOCKER, not a LIMITS entry: a deadlock means a hung run "
            "can never be cleared without manual cluster intervention."
        )

        # Survival contract: at least one completed (success or
        # captured exception). Record the observed shape for the
        # LIMITS row.
        completed = sum(
            1 for k in ("t1", "t2")
            if results[k] is not None or errors[k] is not None
        )
        assert completed >= 1, (
            "Both threads vanished without producing a result or exception "
            "— harness setup broken"
        )

        # Diagnostic record — what each thread observed. The LIMITS
        # row in the ship-ready report cites this.
        for key in ("t1", "t2"):
            summary = results[key]
            err = errors[key]
            if summary is not None:
                print(f"{key}: completed with run_id={summary.run_id} "
                      f"steps={len(summary.steps)}")
            elif err is not None:
                print(f"{key}: raised {type(err).__name__}: {err}")
            else:
                print(f"{key}: <no result>")

        # NOTE: this test does NOT assert which-completed-which or any
        # state-table coherence claim. That's by design — the LIMITS
        # row documents the OBSERVED behaviour for Phase 5 / Phase γ
        # to act on; pinning a specific shape here would over-promise
        # an invariant that Phase γ's locking work is the one to lock
        # in.
