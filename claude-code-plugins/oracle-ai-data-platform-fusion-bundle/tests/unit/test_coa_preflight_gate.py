"""Integration test for the COA preflight gate wiring (M2).

Drives `_check_coa_gate` through a fake Spark that answers DESCRIBE + the
multi-COA and Tier-B aggregate probes, against the shipped pack's dim_account
node. Proves: multi-COA fails closed without acceptance; bronze-column-name
contract (probes use CodeCombinationChartOfAccountsId, never the silver alias).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_bundle.orchestrator.coa_gate import (
    AIDPF_2018_MULTI_COA_UNCONFIGURED,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_bundle.orchestrator.node_preflight import (
    AIDPF_2013_STRUCTURAL_COA,
    _check_coa_gate,
    _coa_role_aliases,
    _evaluate_coa,
    _normalize_coa_structure,
)
from oracle_ai_data_platform_fusion_bundle.schema.medallion_pack import ColumnAlias

PACK_ROOT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "oracle_ai_data_platform_fusion_bundle"
    / "content_packs"
    / "fusion-finance-starter"
)

GL_COA_COLUMNS = [
    "CodeCombinationCodeCombinationId",
    "CodeCombinationChartOfAccountsId",
    "CodeCombinationSegment1",
    "CodeCombinationSegment2",
    "CodeCombinationSegment3",
    "CodeCombinationSegment4",
    "CodeCombinationSegment5",
    "CodeCombinationSegment6",
    "CodeCombinationAccountType",
    "CodeCombinationEnabledFlag",
]


def _fake_spark(chart_rows: dict[str, int], na_ambiguous: int = 0, na_total: int = 500):
    """Fake Spark: DESCRIBE returns gl_coa columns; the multi-COA GROUP BY
    returns chart_rows; the Tier-B aggregate returns (total, ambiguous)."""
    spark = MagicMock()

    def _sql(query: str):
        df = MagicMock()
        q = " ".join(query.split())
        if q.startswith("DESCRIBE TABLE"):
            df.collect.return_value = [(c, "string", None) for c in GL_COA_COLUMNS]
        elif "GROUP BY CAST(CodeCombinationChartOfAccountsId AS STRING)" in q:
            assert "chart_of_accounts_id" not in q.replace(
                "AS chart_id", ""
            ), "probe must use the bronze column, not the silver alias"
            df.collect.return_value = [(cid, n) for cid, n in chart_rows.items()]
        else:  # Tier-B natural-account aggregate
            df.collect.return_value = [(na_total, na_ambiguous)]
        return df

    spark.sql.side_effect = _sql
    return spark


def _ctx():
    ctx = MagicMock()
    ctx.bronze_table_for_source = {"gl_coa": "cat.bronze.gl_coa"}
    return ctx


def _profile(coa: dict):
    prof = MagicMock()
    prof.profile = {"chartOfAccounts": coa}
    return prof


SINGLETON_COA = {
    "default": {
        "balancingSegment": "CodeCombinationSegment1",
        "costCenterSegment": "CodeCombinationSegment2",
        "naturalAccountSegment": "CodeCombinationSegment3",
    }
}


def _dim_account_node(pack):
    return pack.silver["dim_account"]


def _pack_with_aliases(aliases: dict[str, ColumnAlias]):
    pack = MagicMock()
    pack.pack.column_aliases = aliases
    return pack


def test_coa_role_aliases_filters_to_coa_roles_only() -> None:
    """A non-COA `semanticRole` alias is NOT treated as a COA source, so no COA
    probe SQL is ever interpolated against a non-COA column."""
    aliases = {
        "coa_balancing_segment": ColumnAlias(
            appliesTo="bronze.gl_coa",
            candidates=["CodeCombinationSegment1"],
            resolution="semanticRole",
            role="coa.balancing",
        ),
        # A non-COA semanticRole alias (valid schema; free-form role string).
        "region_dimension": ColumnAlias(
            appliesTo="bronze.ap_invoices",
            candidates=["REGION_CODE"],
            resolution="semanticRole",
            role="geo.region",
        ),
        # A plain existence alias is never a COA role.
        "supplier_key": ColumnAlias(
            appliesTo="bronze.erp_suppliers", candidates=["SEGMENT1"]
        ),
    }
    out = _coa_role_aliases(_pack_with_aliases(aliases))
    assert set(out) == {"coa_balancing_segment"}
    assert out["coa_balancing_segment"] == ("coa.balancing", "gl_coa")
    assert "region_dimension" not in out


def test_single_coa_singleton_passes() -> None:
    pack = load_pack(PACK_ROOT)
    spark = _fake_spark({"101": 15000})
    errs = _check_coa_gate(
        spark, _dim_account_node(pack), pack, _profile(SINGLETON_COA), _ctx()
    )
    assert errs == [], [e.message for e in errs]


def test_multi_coa_singleton_fails_closed() -> None:
    pack = load_pack(PACK_ROOT)
    spark = _fake_spark({"101": 15000, "5023": 8000})
    errs = _check_coa_gate(
        spark, _dim_account_node(pack), pack, _profile(SINGLETON_COA), _ctx()
    )
    assert AIDPF_2018_MULTI_COA_UNCONFIGURED in {e.code for e in errs}


def test_multi_coa_with_singleton_accepted_passes() -> None:
    pack = load_pack(PACK_ROOT)
    coa = dict(SINGLETON_COA)
    coa["singletonAccepted"] = True
    spark = _fake_spark({"101": 15000, "5023": 8000})
    errs = _check_coa_gate(
        spark, _dim_account_node(pack), pack, _profile(coa), _ctx()
    )
    assert errs == []


# --- M3: $coa.* union existence + byChart completeness ----------------------

from oracle_ai_data_platform_fusion_bundle.orchestrator.node_preflight import (  # noqa: E402
    preflight_node,
)

BYCHART_COA = {
    "default": SINGLETON_COA["default"],
    "byChart": {
        "101": SINGLETON_COA["default"],
        "5023": {
            "balancingSegment": "CodeCombinationSegment4",
            "costCenterSegment": "CodeCombinationSegment2",
            "naturalAccountSegment": "CodeCombinationSegment5",
        },
    },
}


def test_required_columns_union_missing_arm_column_blocks_preflight() -> None:
    """A byChart arm referencing Segment4, absent from landed gl_coa, blocks
    preflight via the $coa.* union (M3 required addition #1)."""
    pack = load_pack(PACK_ROOT)
    # gl_coa fixture WITHOUT Segment4/5 (arm 5023's columns).
    cols = [c for c in GL_COA_COLUMNS if c not in ("CodeCombinationSegment4", "CodeCombinationSegment5")]
    spark = MagicMock()

    def _sql(query: str):
        df = MagicMock()
        q = " ".join(query.split())
        if q.startswith("DESCRIBE TABLE"):
            df.collect.return_value = [(c, "string", None) for c in cols]
        elif "GROUP BY CAST(CodeCombinationChartOfAccountsId AS STRING)" in q:
            df.collect.return_value = [("101", 15000), ("5023", 8000)]
        else:
            df.collect.return_value = [(500, 0)]
        return df

    spark.sql.side_effect = _sql
    report = preflight_node(
        spark, _dim_account_node(pack), pack, _profile(BYCHART_COA), _ctx()
    )
    assert not report.ok
    assert any(e.code == "AIDPF-2042" for e in report.errors)


def test_byChart_completeness_unmapped_chart_fails() -> None:
    """A present active chart with no byChart arm fails closed (L3.4)."""
    pack = load_pack(PACK_ROOT)
    # byChart maps 101 + 5023, but live gl_coa also has unmapped chart 999.
    spark = _fake_spark({"101": 15000, "5023": 8000, "999": 4000})
    errs = _check_coa_gate(
        spark, _dim_account_node(pack), pack, _profile(BYCHART_COA), _ctx()
    )
    assert any(e.code == "AIDPF-2018" for e in errs)
    assert any("999" in e.message for e in errs)


def test_malicious_coa_column_blocks_before_tier_b_probe() -> None:
    """A hand-edited tenant profile with an injection/invalid naturalAccountSegment
    must fail the identifier allowlist BEFORE any Tier B probe SQL is built."""
    pack = load_pack(PACK_ROOT)
    sql_calls: list[str] = []
    spark = MagicMock()

    def _sql(query: str):
        sql_calls.append(query)
        df = MagicMock()
        df.collect.return_value = [(c, "string", None) for c in GL_COA_COLUMNS]
        return df

    spark.sql.side_effect = _sql
    bad = {
        "default": {
            "balancingSegment": "CodeCombinationSegment1",
            "costCenterSegment": "CodeCombinationSegment2",
            "naturalAccountSegment": "CodeCombinationSegment3) FROM x; DROP TABLE y--",
        }
    }
    errs = _check_coa_gate(
        spark, _dim_account_node(pack), pack, _profile(bad), _ctx()
    )
    assert any(e.code == "AIDPF-5001" for e in errs), [e.code for e in errs]
    # No Tier B aggregate (the GROUP BY <na_col> probe) was ever constructed.
    assert not any("GROUP BY" in q and "DROP TABLE" in q for q in sql_calls)
    assert not any("DROP TABLE" in q for q in sql_calls)


def test_byChart_covering_all_active_charts_passes() -> None:
    """Remediation end state: byChart maps every active chart -> gate passes
    (the multi-COA block is resolved by authoring byChart, not --accept)."""
    pack = load_pack(PACK_ROOT)
    spark = _fake_spark({"101": 15000, "5023": 8000})
    errs = _check_coa_gate(
        spark, _dim_account_node(pack), pack, _profile(BYCHART_COA), _ctx()
    )
    assert errs == [], [e.message for e in errs]


# ---------------------------------------------------------------------------
# Structural COA gate (_normalize_coa_structure) — AIDPF-2013, pre-extraction
# ---------------------------------------------------------------------------

_ROLES = {"coa.balancing", "coa.cost_center", "coa.natural_account"}


def _is_2013(errs) -> bool:
    return any(e.code == AIDPF_2013_STRUCTURAL_COA for e in errs)


def test_structural_flat_legacy_shape_accepted() -> None:
    flat = {
        "balancingSegment": "CodeCombinationSegment1",
        "costCenterSegment": "CodeCombinationSegment2",
        "naturalAccountSegment": "CodeCombinationSegment3",
    }
    assert _normalize_coa_structure(flat, _ROLES, "gl_coa") == []


def test_structural_nested_default_shape_accepted() -> None:
    assert _normalize_coa_structure(SINGLETON_COA, _ROLES, "gl_coa") == []


def test_structural_missing_mapping_blocks_2013() -> None:
    assert _is_2013(_normalize_coa_structure(None, _ROLES, "gl_coa"))


def test_structural_empty_mapping_blocks_2013() -> None:
    assert _is_2013(_normalize_coa_structure({}, _ROLES, "gl_coa"))


def test_structural_bychart_only_no_default_blocks_2013() -> None:
    """byChart with no effective default cannot render fallback rows → 2013."""
    coa = {"byChart": {"101": SINGLETON_COA["default"]}}
    assert _is_2013(_normalize_coa_structure(coa, _ROLES, "gl_coa"))


def test_structural_mixed_flat_and_default_blocks_2013() -> None:
    coa = {**SINGLETON_COA, "balancingSegment": "CodeCombinationSegment1"}
    assert _is_2013(_normalize_coa_structure(coa, _ROLES, "gl_coa"))


def test_structural_incomplete_arm_blocks_2013() -> None:
    coa = {"default": {"balancingSegment": "CodeCombinationSegment1"}}
    assert _is_2013(_normalize_coa_structure(coa, _ROLES, "gl_coa"))


def test_structural_non_numeric_bychart_key_blocks_2013() -> None:
    coa = {
        **SINGLETON_COA,
        "byChart": {"NOT_NUMERIC": SINGLETON_COA["default"]},
    }
    assert _is_2013(_normalize_coa_structure(coa, _ROLES, "gl_coa"))


def test_structural_singleton_accepted_string_blocks_2013() -> None:
    """A hand-edited `"false"` string must not slip through — the model's
    StrictBool rejects it, surfaced as AIDPF-2013 pre-extraction."""
    coa = {**SINGLETON_COA, "singletonAccepted": "false"}
    assert _is_2013(_normalize_coa_structure(coa, _ROLES, "gl_coa"))


def test_backstop_hard_blocks_missing_coa_2013() -> None:
    """The per-node backstop no longer no-ops on a missing mapping — it
    hard-blocks AIDPF-2013 (closing the old fail-late hole)."""
    pack = load_pack(PACK_ROOT)
    spark = _fake_spark({"101": 15000})
    errs = _check_coa_gate(
        spark, _dim_account_node(pack), pack, _profile({}), _ctx()
    )
    assert _is_2013(errs)


# ---------------------------------------------------------------------------
# _evaluate_coa — structured result; a late Tier-B raise keeps 2018 violations
# ---------------------------------------------------------------------------


def _fake_spark_tierb_raises(chart_rows: dict[str, int]):
    """DESCRIBE + multi-COA GROUP BY succeed; the Tier-B aggregate raises."""
    spark = MagicMock()

    def _sql(query: str):
        df = MagicMock()
        q = " ".join(query.split())
        if q.startswith("DESCRIBE TABLE"):
            df.collect.return_value = [(c, "string", None) for c in GL_COA_COLUMNS]
            return df
        if "GROUP BY CAST(CodeCombinationChartOfAccountsId AS STRING)" in q:
            df.collect.return_value = [(cid, n) for cid, n in chart_rows.items()]
            return df
        raise RuntimeError("constrained session: Tier-B probe cannot execute")

    spark.sql.side_effect = _sql
    return spark


def test_evaluate_coa_retains_2018_when_tierb_probe_raises() -> None:
    """The multi-COA (2018) violation is retained in `violations` even though a
    later Tier-B query raises — the raise lands in `probe_failures`, it does NOT
    discard the earlier violation."""
    spark = _fake_spark_tierb_raises({"101": 15000, "5023": 8000})
    result = _evaluate_coa(
        spark, "cat.bronze.gl_coa", "gl_coa", SINGLETON_COA, _ROLES
    )
    assert AIDPF_2018_MULTI_COA_UNCONFIGURED in {v.code for v in result.violations}
    assert result.probe_failures  # the Tier-B raise was captured, not swallowed


def test_evaluate_coa_clean_singleton_ok() -> None:
    spark = _fake_spark({"101": 15000})
    result = _evaluate_coa(
        spark, "cat.bronze.gl_coa", "gl_coa", SINGLETON_COA, _ROLES
    )
    assert result.ok, ([v.message for v in result.violations], result.probe_failures)


def test_evaluate_coa_probe_failure_is_not_a_violation() -> None:
    """A DESCRIBE that raises records a probe_failure, not a violation — so the
    checkpoint (not this evaluator) decides block-vs-hatch."""
    spark = MagicMock()

    def _sql(query: str):
        raise RuntimeError("no session")

    spark.sql.side_effect = _sql
    result = _evaluate_coa(
        spark, "cat.bronze.gl_coa", "gl_coa", SINGLETON_COA, _ROLES
    )
    assert result.probe_failures
    assert not result.violations


# ---------------------------------------------------------------------------
# COA checkpoint helpers (ordering + applicability + disposition/hatch)
# ---------------------------------------------------------------------------

from oracle_ai_data_platform_fusion_bundle.orchestrator.node_preflight import (  # noqa: E402
    AIDPF_2074_COA_UNPROVABLE,
    coa_applicable_sources,
    evaluate_coa_checkpoint,
    order_coa_source_first,
)


def test_coa_applicable_sources_detects_dim_account() -> None:
    pack = load_pack(PACK_ROOT)
    srcs = coa_applicable_sources(pack, list(pack.silver.values()) + list(pack.gold.values()))
    assert "gl_coa" in srcs


def test_order_coa_source_first_hoists_gl_coa() -> None:
    pack = load_pack(PACK_ROOT)
    plan = list(pack.bronze.values()) + list(pack.silver.values())
    ordered = order_coa_source_first(plan, {"gl_coa"})
    assert ordered[0].id == "gl_coa"
    # non-COA nodes preserve their relative order
    assert [n.id for n in ordered if n.id != "gl_coa"] == [
        n.id for n in plan if n.id != "gl_coa"
    ]


def test_order_coa_source_first_noop_without_sources() -> None:
    pack = load_pack(PACK_ROOT)
    plan = list(pack.bronze.values())
    assert [n.id for n in order_coa_source_first(plan, set())] == [n.id for n in plan]


def _tables() -> dict[str, str]:
    return {"gl_coa": "cat.bronze.gl_coa"}


def test_checkpoint_structural_only_blocks_missing_mapping() -> None:
    """Pre-extraction structural gate: a missing mapping blocks (AIDPF-2013) with
    NO data probe (structural_only)."""
    pack = load_pack(PACK_ROOT)
    spark = MagicMock()  # must never be queried in structural_only mode
    res = evaluate_coa_checkpoint(
        spark, pack=pack, profile=_profile({}),
        bronze_table_for_source=_tables(), coa_sources={"gl_coa"},
        allow_unprovable=False, structural_only=True,
    )
    assert not res.ok
    assert _is_2013(res.blocking)
    spark.sql.assert_not_called()


def test_checkpoint_data_probe_clean_singleton_ok() -> None:
    pack = load_pack(PACK_ROOT)
    spark = _fake_spark({"101": 15000})
    res = evaluate_coa_checkpoint(
        spark, pack=pack, profile=_profile(SINGLETON_COA),
        bronze_table_for_source=_tables(), coa_sources={"gl_coa"},
        allow_unprovable=False, structural_only=False,
    )
    assert res.ok, [e.message for e in res.blocking]


def test_checkpoint_violation_blocks_even_with_hatch() -> None:
    """A real violation (multi-COA 2018) hard-blocks regardless of the hatch."""
    pack = load_pack(PACK_ROOT)
    spark = _fake_spark({"101": 15000, "5023": 8000})  # 2 charts, singleton
    res = evaluate_coa_checkpoint(
        spark, pack=pack, profile=_profile(SINGLETON_COA),
        bronze_table_for_source=_tables(), coa_sources={"gl_coa"},
        allow_unprovable=True, structural_only=False,
    )
    assert not res.ok
    assert AIDPF_2018_MULTI_COA_UNCONFIGURED in {e.code for e in res.blocking}


def test_checkpoint_probe_failure_blocks_2074_without_hatch() -> None:
    """A probe that cannot execute → AIDPF-2074 block when the hatch is off."""
    pack = load_pack(PACK_ROOT)
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("no session")
    res = evaluate_coa_checkpoint(
        spark, pack=pack, profile=_profile(SINGLETON_COA),
        bronze_table_for_source=_tables(), coa_sources={"gl_coa"},
        allow_unprovable=False, structural_only=False,
    )
    assert not res.ok
    assert AIDPF_2074_COA_UNPROVABLE in {e.code for e in res.blocking}


def test_checkpoint_probe_failure_downgraded_with_hatch() -> None:
    """With allowUnprovableCOA, a probe-execution failure downgrades to a WARN
    and the checkpoint passes (no violation retained)."""
    pack = load_pack(PACK_ROOT)
    spark = MagicMock()
    spark.sql.side_effect = RuntimeError("no session")
    res = evaluate_coa_checkpoint(
        spark, pack=pack, profile=_profile(SINGLETON_COA),
        bronze_table_for_source=_tables(), coa_sources={"gl_coa"},
        allow_unprovable=True, structural_only=False,
    )
    assert res.ok
    assert res.warnings
