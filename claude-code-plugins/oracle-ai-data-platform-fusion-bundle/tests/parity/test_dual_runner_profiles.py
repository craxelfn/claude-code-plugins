"""Phase 4 Step 6 — multi-tenant profile coverage.

Drives the dual-runner harness across two profiles:

- ``finance-default.yaml`` — the canonical profile already exercised by
  ``test_dual_runner_e2e.py``. Uses ``cancelled_date`` semantic +
  ``snapshotDate='2026-06-05'``.
- ``finance-alt-cancelled-flag.yaml`` — alternate variation-point
  picks (``cancelled_flag`` semantic + ``snapshotDate='2025-12-31'`` +
  distinct ``bronzeSchemaFingerprint``). Exercises the variation-point
  machinery that ``finance-default`` does NOT.

Per ``plan.md`` Step 6:
- Asserts both profiles produce coherent output through both backends.
- Documents the cross-profile diff in
  ``docs/v2-phase-4-multi-tenant-coverage.md``.
- Non-conventional COA positioning is EXPLICITLY out of scope (Phase 5
  prerequisite).
"""

from __future__ import annotations

import shutil
import sys
import tempfile
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

from . import bronze_fixtures, bronze_fixtures_tenant_b  # noqa: E402
from .dual_runner_helpers import (  # noqa: E402
    assert_state_rows_equiv,
    create_target_schemas, make_delta_spark, make_dual_bundles, seed_bronze,
)


CATALOG = "spark_catalog"
PACK_PATH = (REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_bundle"
             / "content_packs" / "fusion-finance-starter")
PROFILE_DEFAULT = REPO_ROOT / "examples" / "profiles" / "finance-default.yaml"
PROFILE_TENANT_B = (Path(__file__).parent / "fixtures" / "profiles"
                    / "finance-alt-cancelled-flag.yaml")
SNAPSHOT_TENANT_B = (Path(__file__).parent / "fixtures" / "profiles"
                     / "finance-alt-cancelled-flag.schema-snapshot.yaml")


# Profile-to-fixture-module map. The conftest-style fixtures attribute
# lookup picks the right bronze module per profile.
_PROFILE_TABLE = {
    "finance-default": {
        "src": PROFILE_DEFAULT,
        "snapshot": None,
        "fixtures": bronze_fixtures,
        "profile_name": "finance-default",
    },
    "finance-alt-cancelled-flag": {
        "src": PROFILE_TENANT_B,
        "snapshot": SNAPSHOT_TENANT_B,
        "fixtures": bronze_fixtures_tenant_b,
        "profile_name": "finance-alt-cancelled-flag",
    },
}


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    warehouse = tempfile.mkdtemp(prefix="phase4-profiles-warehouse-")
    try:
        session = make_delta_spark("phase4-dual-runner-profiles", warehouse)
    except Exception as exc:
        pytest.skip(
            f"delta-spark local-mode bootstrap failed: {type(exc).__name__}: {exc}."
        )
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(scope="module")
def resolved_pack():
    return load_full_chain(
        PACK_PATH, base_resolver=make_filesystem_base_resolver(PACK_PATH),
    )


@pytest.mark.parametrize(
    "profile_key", ["finance-default", "finance-alt-cancelled-flag"],
)
class TestStep6_MultiTenantParity:
    """Per-profile parametrised parity check.

    Each parameter shape:
    - Bootstraps dual bundles with isolated schemas suffixed by the
      profile key.
    - Seeds bronze with the profile-matched fixture module.
    - Runs seed on both backends.
    - Asserts state-row parity via the three-tier contract.

    The cross-profile diff (which rows are filtered by ``cancelled_flag``
    vs ``cancelled_date``; how ``snapshot_date`` shifts ap_aging
    buckets) is documented in
    ``docs/v2-phase-4-multi-tenant-coverage.md`` rather than asserted
    in test code — the assertion contract is parity WITHIN a profile,
    not equivalence ACROSS profiles.
    """

    def test_profile_drives_coherent_dual_runner(
        self, profile_key: str, spark, resolved_pack,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> None:
        entry = _PROFILE_TABLE[profile_key]
        tmp = tmp_path_factory.mktemp(f"phase4-profile-{profile_key}")
        artifacts = make_dual_bundles(
            tmp, catalog=CATALOG,
            v1_suffix=f"{profile_key}_v1", v2_suffix=f"{profile_key}_v2",
            pack_path=PACK_PATH,
            profile_src=entry["src"],
            snapshot_src=entry["snapshot"],
            profile_name=entry["profile_name"],
        )

        seed_bronze(spark, catalog=CATALOG,
                    schema=artifacts.v1_schemas.bronze,
                    fixtures_module=entry["fixtures"])
        seed_bronze(spark, catalog=CATALOG,
                    schema=artifacts.v2_schemas.bronze,
                    fixtures_module=entry["fixtures"])
        create_target_schemas(spark, catalog=CATALOG,
                              schemas=artifacts.v1_schemas)
        create_target_schemas(spark, catalog=CATALOG,
                              schemas=artifacts.v2_schemas)

        tenant_profile = load_tenant_profile(entry["src"])

        v1_summary = orchestrator.run(
            bundle_path=artifacts.v1_bundle, spark=spark,
            mode="seed", layers=["silver", "gold"],
            execution_backend="legacy-python",
        )
        v2_summary = orchestrator.run(
            bundle_path=artifacts.v2_bundle, spark=spark,
            mode="seed", layers=["silver", "gold"],
            execution_backend="content-pack",
            resolved_pack=resolved_pack, tenant_profile=tenant_profile,
        )

        # Both backends MUST produce the same set of success steps
        # within a given profile. The fixture-specific filter math
        # (cancelled_flag vs cancelled_date) applies to BOTH backends
        # equally — v1's SQL and v2's SQL both read the same profile's
        # resolved.semantic.cancelled_status field via the renderer.
        v1_status = {s.dataset_id: s.status for s in v1_summary.steps}
        v2_status = {s.dataset_id: s.status for s in v2_summary.steps}
        for dataset_id in v1_status:
            assert v1_status[dataset_id] == v2_status.get(dataset_id), (
                f"profile={profile_key!r}: {dataset_id} status diverges — "
                f"v1={v1_status[dataset_id]!r} v2={v2_status.get(dataset_id)!r}"
            )

        # State-row equivalence via the three-tier contract. We use
        # the per-node node list from the seed expected set.
        from tests.parity.test_dual_runner_e2e import EXPECTED_SEED_NODES
        assert_state_rows_equiv(
            spark, catalog=artifacts.catalog,
            v1_schema=artifacts.v1_schemas.bronze,
            v2_schema=artifacts.v2_schemas.bronze,
            expected_nodes=list(EXPECTED_SEED_NODES),
            expected_mode="seed",
        )
