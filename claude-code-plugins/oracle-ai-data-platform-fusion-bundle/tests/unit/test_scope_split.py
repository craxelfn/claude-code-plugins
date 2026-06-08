"""Unit tests for the run-scope split helper (Phase 5 Step 2b — Option A).

Exhaustive decision-matrix coverage for
:func:`orchestrator.scope.split_run_scope`. Bundle-level wiring is
asserted via :func:`split_run_scope_from_bundle` in
``test_default_flip_bronze.py``; this file uses the lower-level
``split_run_scope`` to keep the test set tight.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_bundle.orchestrator.scope import (
    AIDPF_1035_SCOPE_SPLIT_REJECTED,
    RunScope,
    ScopeSplitError,
    split_run_scope,
)


# A canonical id set covering both bronze + pack silver/gold.
BRONZE_IDS = {"ap_invoices", "gl_period_balances", "erp_suppliers", "gl_accounts"}
SILVER_IDS = {"dim_supplier", "dim_account", "dim_calendar"}
GOLD_IDS = {"supplier_spend", "gl_balance", "ap_aging"}


def _split(datasets=None, layers=None):
    return split_run_scope(
        bronze_ids=BRONZE_IDS,
        silver_ids=SILVER_IDS,
        gold_ids=GOLD_IDS,
        datasets=datasets,
        layers=layers,
    )


# ---------------------------------------------------------------------------
# No filters — full medallion run
# ---------------------------------------------------------------------------


class TestNoFilters:
    def test_no_filters_produces_both_branches(self) -> None:
        scope = _split()
        assert scope.bronze_filter == (None, ["bronze"])
        assert scope.cp_filter == (None, ["silver", "gold"])  # medallion order
        assert not scope.is_empty


# ---------------------------------------------------------------------------
# --layers only
# ---------------------------------------------------------------------------


class TestLayersOnly:
    def test_layers_bronze_only(self) -> None:
        scope = _split(layers=["bronze"])
        assert scope.bronze_filter == (None, ["bronze"])
        assert scope.cp_filter is None

    def test_layers_silver_only(self) -> None:
        scope = _split(layers=["silver"])
        assert scope.bronze_filter is None
        assert scope.cp_filter == (None, ["silver"])

    def test_layers_gold_only(self) -> None:
        scope = _split(layers=["gold"])
        assert scope.bronze_filter is None
        assert scope.cp_filter == (None, ["gold"])

    def test_layers_silver_gold(self) -> None:
        scope = _split(layers=["silver", "gold"])
        assert scope.bronze_filter is None
        assert scope.cp_filter == (None, ["silver", "gold"])  # medallion order

    def test_layers_bronze_silver(self) -> None:
        scope = _split(layers=["bronze", "silver"])
        assert scope.bronze_filter == (None, ["bronze"])
        assert scope.cp_filter == (None, ["silver"])

    def test_layers_all_three(self) -> None:
        scope = _split(layers=["bronze", "silver", "gold"])
        assert scope.bronze_filter == (None, ["bronze"])
        assert scope.cp_filter == (None, ["silver", "gold"])  # medallion order

    def test_unknown_layer_raises(self) -> None:
        with pytest.raises(ScopeSplitError) as exc:
            _split(layers=["plutonium"])
        assert AIDPF_1035_SCOPE_SPLIT_REJECTED in str(exc.value)
        assert "plutonium" in str(exc.value)


# ---------------------------------------------------------------------------
# --datasets only
# ---------------------------------------------------------------------------


class TestDatasetsOnly:
    def test_single_bronze_id(self) -> None:
        scope = _split(datasets=["ap_invoices"])
        assert scope.bronze_filter == (["ap_invoices"], None)
        assert scope.cp_filter is None

    def test_single_silver_id(self) -> None:
        scope = _split(datasets=["dim_supplier"])
        assert scope.bronze_filter is None
        # cp_filter carries the layers actually present (silver here)
        # so the resolver enforces the layer contract.
        assert scope.cp_filter == (["dim_supplier"], ["silver"])

    def test_single_gold_id(self) -> None:
        scope = _split(datasets=["ap_aging"])
        assert scope.bronze_filter is None
        assert scope.cp_filter == (["ap_aging"], ["gold"])

    def test_mixed_silver_gold_emits_both_layers(self) -> None:
        scope = _split(datasets=["dim_supplier", "ap_aging"])
        assert scope.bronze_filter is None
        assert scope.cp_filter == (
            ["dim_supplier", "ap_aging"],
            ["silver", "gold"],
        )

    def test_mixed_bronze_silver_routes_both(self) -> None:
        scope = _split(datasets=["ap_invoices", "dim_supplier"])
        assert scope.bronze_filter == (["ap_invoices"], None)
        assert scope.cp_filter == (["dim_supplier"], ["silver"])

    def test_unknown_id_raises(self) -> None:
        with pytest.raises(ScopeSplitError) as exc:
            _split(datasets=["totally_unknown"])
        assert AIDPF_1035_SCOPE_SPLIT_REJECTED in str(exc.value)
        assert "totally_unknown" in str(exc.value)
        # Available ids surfaced so operators see the typo.
        assert "ap_invoices" in str(exc.value) or "dim_supplier" in str(exc.value)


# ---------------------------------------------------------------------------
# --datasets + --layers combinations
# ---------------------------------------------------------------------------


class TestDatasetsPlusLayers:
    def test_bronze_id_with_bronze_layer_ok(self) -> None:
        scope = _split(datasets=["ap_invoices"], layers=["bronze"])
        assert scope.bronze_filter == (["ap_invoices"], None)
        assert scope.cp_filter is None

    def test_silver_id_with_bronze_layer_unsatisfiable(self) -> None:
        # The classic failure mode the plan calls out — operator typed
        # `--datasets dim_supplier --layers bronze` (silver id with
        # bronze-only layer).
        with pytest.raises(ScopeSplitError) as exc:
            _split(datasets=["dim_supplier"], layers=["bronze"])
        assert AIDPF_1035_SCOPE_SPLIT_REJECTED in str(exc.value)
        assert "unsatisfiable" in str(exc.value)

    def test_bronze_id_with_silver_only_layer_unsatisfiable(self) -> None:
        with pytest.raises(ScopeSplitError):
            _split(datasets=["ap_invoices"], layers=["silver"])

    def test_silver_id_with_silver_layer_ok(self) -> None:
        scope = _split(datasets=["dim_supplier"], layers=["silver"])
        assert scope.bronze_filter is None
        assert scope.cp_filter == (["dim_supplier"], ["silver"])

    def test_silver_id_with_gold_only_layer_unsatisfiable(self) -> None:
        # Regression: previously the disjoint-vs-{silver,gold} check
        # passed for layers=["gold"] when only silver ids were named,
        # then cp_filter dropped the layer list and the resolver ran
        # the silver node anyway.
        with pytest.raises(ScopeSplitError) as exc:
            _split(datasets=["dim_supplier"], layers=["gold"])
        assert AIDPF_1035_SCOPE_SPLIT_REJECTED in str(exc.value)
        assert "excludes silver" in str(exc.value)
        assert "unsatisfiable" in str(exc.value)

    def test_gold_id_with_silver_only_layer_unsatisfiable(self) -> None:
        with pytest.raises(ScopeSplitError) as exc:
            _split(datasets=["ap_aging"], layers=["silver"])
        assert AIDPF_1035_SCOPE_SPLIT_REJECTED in str(exc.value)
        assert "excludes gold" in str(exc.value)

    def test_mixed_silver_gold_with_silver_only_layer_unsatisfiable(self) -> None:
        # When --datasets mixes silver + gold ids but --layers excludes
        # one of them, reject — do NOT silently narrow.
        with pytest.raises(ScopeSplitError) as exc:
            _split(datasets=["dim_supplier", "ap_aging"], layers=["silver"])
        assert AIDPF_1035_SCOPE_SPLIT_REJECTED in str(exc.value)
        assert "excludes gold" in str(exc.value)

    def test_silver_id_with_silver_gold_layer_emits_only_silver(self) -> None:
        # Operator named only silver ids but allowed both layers; the
        # emitted cp_filter narrows to silver (no point telling the
        # resolver gold is in-scope when no gold ids are requested).
        scope = _split(datasets=["dim_supplier"], layers=["silver", "gold"])
        assert scope.cp_filter == (["dim_supplier"], ["silver"])


# ---------------------------------------------------------------------------
# Empty effective scope
# ---------------------------------------------------------------------------


class TestEmptyEffectiveScope:
    def test_silver_only_pack_less_bundle_raises(self) -> None:
        # Pack-less bundle (silver+gold sets empty), --layers silver:
        # bronze_filter is None (layer excludes bronze), cp_filter
        # would be (None, ['silver']) — but the cp helper has no nodes.
        # The classifier returns the scope; the dispatcher's empty-cp
        # branch is responsible for surfacing the error. The classifier
        # only catches a TRULY empty effective scope (e.g.
        # --datasets <bronze_id> --layers silver,gold mis-routes to
        # empty filters).
        # For pack-less + silver layer, the classifier still produces
        # a non-empty cp_filter — it's the helper that raises later.
        scope = split_run_scope(
            bronze_ids={"ap_invoices"},
            silver_ids=set(),
            gold_ids=set(),
            datasets=None,
            layers=["silver"],
        )
        assert scope.cp_filter == (None, ["silver"])
        # bronze_filter None because layers excludes bronze.
        assert scope.bronze_filter is None
