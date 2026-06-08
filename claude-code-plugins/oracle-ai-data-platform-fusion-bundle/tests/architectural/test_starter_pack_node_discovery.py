"""Architectural test — every starter-pack node loads cleanly + has a
valid ``implementation.type`` (Phase 5 Step 7).

This is the discovery-side regression guard. The dispatch-side
regression guards live in the per-implementation-type tests
(``test_sql_runner``, ``test_sql_runner_builtin_dispatch``,
``test_python_legacy_adapter``).

If a future YAML edit drops a required field or introduces an unknown
``implementation.type``, this test catches it before the dispatcher
sees the malformed pack.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_bundle.orchestrator import (
    _resolve_node_from_pack,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import load_pack


STARTER_PACK_ROOT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "oracle_ai_data_platform_fusion_bundle"
    / "content_packs"
    / "fusion-finance-starter"
)


VALID_IMPL_TYPES = {"sql", "builtin", "python_legacy"}


@pytest.fixture(scope="module")
def starter_pack():
    """Load the shipped fusion-finance-starter pack once per module.

    Skips the entire module if the pack root is missing — protects
    against accidental layout shifts during local dev.
    """
    if not STARTER_PACK_ROOT.is_dir():
        pytest.skip(f"starter pack root missing: {STARTER_PACK_ROOT}")
    return load_pack(STARTER_PACK_ROOT)


class TestStarterPackLoads:
    def test_pack_loads_without_error(self, starter_pack) -> None:
        assert starter_pack is not None
        # Pack carries non-trivial silver + gold node sets.
        assert len(starter_pack.silver) > 0, "starter pack has no silver nodes"
        assert len(starter_pack.gold) > 0, "starter pack has no gold nodes"

    def test_every_silver_node_has_valid_impl_type(self, starter_pack) -> None:
        for node_id, node in starter_pack.silver.items():
            assert node.implementation.type in VALID_IMPL_TYPES, (
                f"silver/{node_id} has unknown implementation.type="
                f"{node.implementation.type!r}; expected one of "
                f"{VALID_IMPL_TYPES!r}."
            )

    def test_every_gold_node_has_valid_impl_type(self, starter_pack) -> None:
        for node_id, node in starter_pack.gold.items():
            assert node.implementation.type in VALID_IMPL_TYPES, (
                f"gold/{node_id} has unknown implementation.type="
                f"{node.implementation.type!r}; expected one of "
                f"{VALID_IMPL_TYPES!r}."
            )


class TestStarterPackDiscoveryViaHelper:
    def test_resolve_node_for_every_silver_id(self, starter_pack) -> None:
        for node_id in starter_pack.silver:
            node = _resolve_node_from_pack(starter_pack, "silver", node_id)
            assert node.id == node_id
            assert node.layer == "silver"

    def test_resolve_node_for_every_gold_id(self, starter_pack) -> None:
        for node_id in starter_pack.gold:
            node = _resolve_node_from_pack(starter_pack, "gold", node_id)
            assert node.id == node_id
            assert node.layer == "gold"

    def test_starter_pack_uses_sql_or_builtin_only(self, starter_pack) -> None:
        """Phase 5 deliverable — the SHIPPED starter pack uses sql / builtin
        (no python_legacy). python_legacy is reserved for customer-shipped
        migration overlays. If a future starter-pack edit introduces a
        python_legacy node, this test fails so the architectural decision
        gets explicit review.
        """
        for node_id, node in starter_pack.silver.items():
            assert node.implementation.type != "python_legacy", (
                f"silver/{node_id} declares python_legacy in the shipped "
                f"starter pack. python_legacy is for customer migration "
                f"overlays; SQL templates are the starter-pack contract."
            )
        for node_id, node in starter_pack.gold.items():
            assert node.implementation.type != "python_legacy", (
                f"gold/{node_id} declares python_legacy in the shipped "
                f"starter pack."
            )
