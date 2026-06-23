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
    _check_coa_gate,
)

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
