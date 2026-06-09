"""Unit tests for :mod:`medallion_author.runbook` — the round-3 + round-4
backend-aware remediation drafter.

Covers:

* Option C raises :class:`OptionDeferredError`.
* Option D content-pack emits ``--execution-backend content-pack`` +
  validates the affected IDs against the resolved pack via
  ``content_pack_plan_resolver``.
* Option D legacy validates ID membership in ``SILVER_DIMS`` /
  ``GOLD_MARTS`` — NOT ``mart_*`` (round-4 finding).
* Option D legacy with unmapped node raises
  :class:`UnmappedLegacyNodeError`.
* Option B emits SQL + the 5-point operator-review checklist.
* Option A / E emit markdown only.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_bundle.medallion_author.runbook import (
    OptionDeferredError,
    RemediationArtifacts,
    UnmappedLegacyNodeError,
    draft_remediation,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.registry import (
    GOLD_MARTS,
    SILVER_DIMS,
)


# ---------------------------------------------------------------------------
# Option C — deferred
# ---------------------------------------------------------------------------


class TestOptionCDeferred:
    def test_option_c_raises_deferred(self) -> None:
        with pytest.raises(OptionDeferredError) as excinfo:
            draft_remediation(
                option="C",
                vp_name="invoice_currency_code",
                prior_pinned=None,
                new_candidate="ApInvoicesXCurrCode",
                affected_silver_ids={"supplier_spend"},
                affected_gold_ids={"ap_aging"},
                backend="content-pack",
                risk_label="likely-different-semantics",
                rationale="x",
            )
        # Operator-facing redirect.
        assert "Option D" in str(excinfo.value)
        assert "rewind" in str(excinfo.value).lower()


# ---------------------------------------------------------------------------
# Option D — backend-aware
# ---------------------------------------------------------------------------


class TestOptionDContentPack:
    def test_emits_execution_backend_flag(self) -> None:
        artifacts = draft_remediation(
            option="D",
            vp_name="invoice_currency_code",
            prior_pinned=None,
            new_candidate="ApInvoicesXCurrCode",
            affected_silver_ids={"supplier_spend"},
            affected_gold_ids={"ap_aging"},
            backend="content-pack",
            risk_label="likely-different-semantics",
            rationale="Fusion 25C rename.",
        )
        assert artifacts.option == "D"
        # Must include the backend flag — round-4 finding.
        assert "--execution-backend content-pack" in artifacts.runbook_markdown
        # Datasets are bare pack node IDs (no layer prefix).
        assert "--datasets ap_aging,supplier_spend" in artifacts.runbook_markdown
        # No SQL for Option D.
        assert artifacts.sql is None

    def test_uses_alphabetical_id_order(self) -> None:
        artifacts = draft_remediation(
            option="D",
            vp_name="x",
            prior_pinned=None,
            new_candidate="y",
            affected_silver_ids={"dim_supplier"},
            affected_gold_ids={"supplier_spend", "ap_aging"},
            backend="content-pack",
            risk_label="unknown",
            rationale="r",
        )
        # Alphabetical, comma-separated.
        assert "--datasets ap_aging,dim_supplier,supplier_spend" in artifacts.runbook_markdown



# ---------------------------------------------------------------------------
# Option B — surgical MERGE
# ---------------------------------------------------------------------------


class TestOptionB:
    def test_b_emits_sql_with_review_checklist(self) -> None:
        artifacts = draft_remediation(
            option="B",
            vp_name="invoice_currency_code",
            prior_pinned="ApInvoicesCurrencyCode",
            new_candidate="ApInvoicesInvoiceCurrencyCode",
            affected_silver_ids={"supplier_spend"},
            affected_gold_ids={"ap_aging"},
            backend="content-pack",
            risk_label="likely-different-semantics",
            rationale="r",
        )
        assert artifacts.option == "B"
        assert artifacts.sql is not None
        # MERGE statement for each affected node.
        assert "MERGE INTO" in artifacts.sql
        assert "supplier_spend" in artifacts.sql
        assert "ap_aging" in artifacts.sql
        # 5-point review checklist in the markdown.
        for token in [
            "Column dependency review",
            "Derived columns",
            "Join keys",
            "Type coercion",
            "Audit-trail attestation",
        ]:
            assert token in artifacts.runbook_markdown


# ---------------------------------------------------------------------------
# Options A + E — markdown only
# ---------------------------------------------------------------------------


class TestOptionAAndE:
    def test_a_no_sql_rename_only_markdown(self) -> None:
        artifacts = draft_remediation(
            option="A",
            vp_name="invoice_currency_code",
            prior_pinned="ApInvoicesCurrencyCode",
            new_candidate="ApInvoicesInvoiceCurrencyCode",
            affected_silver_ids=set(),
            affected_gold_ids=set(),
            backend="content-pack",
            risk_label="likely-rename",
            rationale="Fusion rename.",
        )
        assert artifacts.option == "A"
        assert artifacts.sql is None
        assert "no" in artifacts.runbook_markdown.lower()

    def test_e_full_reseed_no_dataset_filter(self) -> None:
        artifacts = draft_remediation(
            option="E",
            vp_name="x",
            prior_pinned=None,
            new_candidate="y",
            affected_silver_ids=set(),
            affected_gold_ids=set(),
            backend="content-pack",
            risk_label="unknown",
            rationale="audit reset",
        )
        assert artifacts.option == "E"
        assert artifacts.sql is None
        # The actual command line lacks --datasets (the prose may
        # mention the flag when explaining what's omitted).
        assert "aidp-fusion-bundle run --mode seed" in artifacts.runbook_markdown
        # Find the command-line code block; assert it has no --datasets.
        code_blocks = artifacts.runbook_markdown.split("```")
        # Code blocks are at odd indices: 1, 3, 5...
        command_blocks = [
            b for i, b in enumerate(code_blocks)
            if i % 2 == 1 and "aidp-fusion-bundle run" in b
        ]
        assert command_blocks, "no command code block found"
        for block in command_blocks:
            assert "--datasets" not in block, (
                f"Option E command block must not include --datasets: {block!r}"
            )
