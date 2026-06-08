"""Phase 5 Step 2b — CLI scope-split inline-path coverage.

Verifies the CLI's inline (``--inline``) path handles bronze-only
fast-path, pack-aware classification, and fail-closed semantics
when content-pack is the default backend.

The cluster-side variant lands in ``test_cli_scope_split_cluster.py``
(deferred — requires REST mocking that's intertwined with the
dispatch package's preflight chain).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_bundle.orchestrator.scope import (
    AIDPF_1035_SCOPE_SPLIT_REJECTED,
    ScopeSplitError,
    split_run_scope_from_bundle,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import (
    load_full_chain,
    make_filesystem_base_resolver,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_BUNDLE = REPO_ROOT / "tests" / "fixtures" / "projects" / "phase2_project" / "bundle.yaml"
FIXTURE_PACK = REPO_ROOT / "tests" / "fixtures" / "content_packs" / "phase2_test_pack"


@pytest.fixture
def pack():
    return load_full_chain(
        FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK),
    )


@pytest.fixture
def bundle():
    from oracle_ai_data_platform_fusion_bundle.schema.bundle import load_bundle
    b, _ = load_bundle(FIXTURE_BUNDLE)
    return b


class TestCliInlineScopeSplit:
    def test_layers_silver_gold_only_routes_to_cp_filter(
        self, bundle, pack,
    ) -> None:
        scope = split_run_scope_from_bundle(
            bundle, pack, datasets=None, layers=["silver", "gold"],
        )
        assert scope.bronze_filter is None
        assert scope.cp_filter == (None, ["silver", "gold"])

    def test_layers_bronze_only_routes_to_bronze_filter(
        self, bundle, pack,
    ) -> None:
        scope = split_run_scope_from_bundle(
            bundle, pack, datasets=None, layers=["bronze"],
        )
        assert scope.bronze_filter == (None, ["bronze"])
        assert scope.cp_filter is None

    def test_no_filter_routes_both_branches(self, bundle, pack) -> None:
        scope = split_run_scope_from_bundle(
            bundle, pack, datasets=None, layers=None,
        )
        assert scope.bronze_filter is not None
        assert scope.cp_filter is not None

    def test_pack_less_bundle_emits_no_cp_filter(self, bundle) -> None:
        # Pack=None: silver_ids/gold_ids are empty, so cp_filter
        # branches only if --layers includes silver/gold.
        scope = split_run_scope_from_bundle(
            bundle, None, datasets=None, layers=["bronze"],
        )
        assert scope.bronze_filter == (None, ["bronze"])
        assert scope.cp_filter is None

    def test_silver_id_with_bronze_layer_raises_aidpf_1035(
        self, bundle, pack,
    ) -> None:
        # dim_thing is a silver id in the pack (verify by inspection).
        silver_ids = list(pack.silver.keys())
        assert silver_ids, "fixture pack must declare at least one silver node"
        a_silver = silver_ids[0]
        with pytest.raises(ScopeSplitError) as exc:
            split_run_scope_from_bundle(
                bundle, pack, datasets=[a_silver], layers=["bronze"],
            )
        assert AIDPF_1035_SCOPE_SPLIT_REJECTED in str(exc.value)
        assert "unsatisfiable" in str(exc.value)

    def test_unknown_dataset_raises_aidpf_1035(self, bundle, pack) -> None:
        with pytest.raises(ScopeSplitError) as exc:
            split_run_scope_from_bundle(
                bundle, pack, datasets=["totally_fake"], layers=None,
            )
        assert AIDPF_1035_SCOPE_SPLIT_REJECTED in str(exc.value)
        assert "totally_fake" in str(exc.value)
