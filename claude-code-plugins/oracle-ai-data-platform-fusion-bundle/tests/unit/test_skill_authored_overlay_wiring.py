"""Tests for the Phase 3b feature #2 schema extension + bootstrap wiring.

The skill (Phase 3b) drafts an overlay carrying
``provenance.skillId == "aidp-fusion-medallion-author"``. Bootstrap
(Phase 3a, extended here) detects that and:

* Stamps ``mechanism: skill_proposed`` on resolutions whose chosen
  candidate matches ``provenance.proposals[vp].candidateAdded`` —
  including AutoResolved outcomes with no ``--resolutions`` file
  (the initial-onboarding case the round-2 review explicitly
  required).
* Populates ``SnapshotProvenance.skill_version`` from the overlay.
* Mirrors ``provenance.incrementalImpact[vp]`` into the snapshot's
  per-resolution ``incremental_impact`` field.

Tests cover the round-3 PackProvenance camelCase-alias contract +
the round-2 initial-onboarding mechanism-stamping requirement.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from oracle_ai_data_platform_fusion_bundle.commands.variation_phase import (
    VariationPhaseOptions,
    run_variation_phase,
    _is_skill_authored_overlay,
    _load_entry_overlay_provenance,
)
from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle
from oracle_ai_data_platform_fusion_bundle.schema.incremental_impact import (
    IncrementalImpact,
    RemediationRecord,
)
from oracle_ai_data_platform_fusion_bundle.schema.medallion_pack import (
    PackProvenance,
    SkillProposalRecord,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
STARTER_PACK = (
    REPO_ROOT
    / "scripts"
    / "oracle_ai_data_platform_fusion_bundle"
    / "content_packs"
    / "fusion-finance-starter"
)


# ---------------------------------------------------------------------------
# Round-3 PackProvenance camelCase aliases
# ---------------------------------------------------------------------------


class TestPackProvenanceAliases:
    def test_skill_id_parses_from_camelcase(self) -> None:
        prov = PackProvenance.model_validate(
            {
                "skillId": "aidp-fusion-medallion-author",
                "skillVersion": "0.1.0",
                "modelId": "claude-opus-4-7",
                "generatedAt": "2026-06-06T12:00:00Z",
                "diagnosticRunId": "20260606T120000Z-abc12345",
            }
        )
        assert prov.skill_id == "aidp-fusion-medallion-author"
        assert prov.skill_version == "0.1.0"
        assert prov.model_id == "claude-opus-4-7"
        assert prov.diagnostic_run_id == "20260606T120000Z-abc12345"

    def test_incremental_impact_nested_aliases(self) -> None:
        prov = PackProvenance.model_validate(
            {
                "skillId": "aidp-fusion-medallion-author",
                "incrementalImpact": {
                    "invoice_currency_code": {
                        "changeKind": "promotion",
                        "priorPinned": "ApInvoicesCurrencyCode",
                        "newCandidate": "ApInvoicesInvoiceCurrencyCode",
                        "riskLabel": "likely-different-semantics",
                        "affectedNodes": ["silver.supplier_spend"],
                        "remediation": {
                            "recommended": "D",
                            "operatorChose": "D",
                            "rationale": "Targeted re-seed is the v0.3 default.",
                        },
                    }
                },
            }
        )
        impact = prov.incremental_impact["invoice_currency_code"]
        assert impact.change_kind == "promotion"
        assert impact.new_candidate == "ApInvoicesInvoiceCurrencyCode"
        assert impact.remediation.operator_chose == "D"

    def test_proposals_aliases(self) -> None:
        prov = PackProvenance.model_validate(
            {
                "skillId": "aidp-fusion-medallion-author",
                "proposals": {
                    "invoice_currency_code": {
                        "candidateAdded": "ApInvoicesXCurrCode",
                        "confidence": "high",
                        "reasoning": "Fusion 25C renamed CurrencyCode.",
                    }
                },
            }
        )
        proposal = prov.proposals["invoice_currency_code"]
        assert proposal.candidate_added == "ApInvoicesXCurrCode"
        assert proposal.confidence == "high"


# ---------------------------------------------------------------------------
# E2E — skill-authored overlay → bootstrap records skill_proposed
# ---------------------------------------------------------------------------


def _row(col_name: str, data_type: str = "string"):
    return {"col_name": col_name, "data_type": data_type, "comment": None}


def _mock_spark(per_table_columns: dict[str, list[str]]) -> MagicMock:
    spark = MagicMock(name="spark")

    def _sql(query: str):
        target = query.split()[-1]
        dataset = target.split(".")[-1]
        cols = per_table_columns.get(dataset, [])
        df = MagicMock(name=f"df_{dataset}")
        df.collect.return_value = [_row(c) for c in cols]
        return df

    spark.sql.side_effect = _sql
    return spark


@pytest.fixture
def bundle_dir_with_skill_overlay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """A bundle pointing at a skill-authored overlay that extends the
    starter pack's ``invoice_currency_code`` candidate list with a
    non-conventional column."""
    monkeypatch.setenv("USER", "alice@oracle.com")

    overlay_root = tmp_path / "overlays" / "test-currency-extension"
    overlay_root.mkdir(parents=True)
    (overlay_root / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "id": "test-currency-extension",
                "version": "0.1.0",
                "extends": "fusion-finance-starter@0.1.0",
                "compatibility": {
                    "pluginMinVersion": "0.3.0",
                    "fusionFamilies": ["ERP"],
                    "aidp": {"requiresDelta": True},
                },
                "columnAliases": {
                    "invoice_currency_code": {
                        "appliesTo": "bronze.ap_invoices",
                        "required": True,
                        # NB: in a real overlay you'd extend the base's
                        # candidates. For this test the starter pack
                        # already declares two candidates, so we just
                        # add the third. Bootstrap walks the merged list.
                        "candidates": [
                            "ApInvoicesInvoiceCurrencyCode",
                            "ApInvoicesCurrencyCode",
                            "ApInvoicesXCurrCode",
                        ],
                    },
                },
                "provenance": {
                    "skillId": "aidp-fusion-medallion-author",
                    "skillVersion": "0.1.0",
                    "modelId": "claude-opus-4-7",
                    "generatedAt": "2026-06-06T12:00:00Z",
                    "diagnosticRunId": "run-test-3b",
                    "proposals": {
                        "invoice_currency_code": {
                            "candidateAdded": "ApInvoicesXCurrCode",
                            "confidence": "high",
                            "reasoning": "Fusion 25C rename observed on tenant.",
                        },
                    },
                    "incrementalImpact": {
                        "invoice_currency_code": {
                            "changeKind": "initial",
                            "newCandidate": "ApInvoicesXCurrCode",
                            "riskLabel": "likely-different-semantics",
                            "affectedNodes": [
                                "silver.supplier_spend",
                                "silver.ap_aging",
                            ],
                            "remediation": {
                                "recommended": "D",
                                "operatorChose": "D",
                                "rationale": "v0.3 default targeted re-seed.",
                            },
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    # Symlink the starter pack as the base pack (sibling discovery).
    base_link = tmp_path / "overlays" / "fusion-finance-starter@0.1.0"
    base_link.symlink_to(STARTER_PACK)

    bundle_yaml = tmp_path / "bundle.yaml"
    bundle_yaml.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "aidp-fusion-bundle/v1",
                "version": "0.2.0",
                "project": "test",
                "fusion": {
                    "serviceUrl": "https://example.invalid",
                    "username": "stub",
                    "password": "stub",
                    "externalStorage": "stub",
                },
                "aidp": {
                    "catalog": "cat",
                    "bronzeSchema": "bronze",
                    "silverSchema": "silver",
                    "goldSchema": "gold",
                    "storageFormat": "delta",
                },
                "datasets": [
                    {"id": "erp_suppliers", "mode": "full"},
                    {"id": "ap_invoices", "mode": "incremental"},
                    {"id": "gl_coa", "mode": "full"},
                    {"id": "gl_period_balances", "mode": "full"},
                ],
                "contentPack": {
                    "name": "test-currency-extension",
                    "path": str(overlay_root),
                    "profile": "finance-default",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return tmp_path


def _load_bundle(p: Path) -> Bundle:
    return Bundle.model_validate(yaml.safe_load(p.read_text(encoding="utf-8")))


class TestSkillAuthoredDetection:
    def test_is_skill_authored_none_input(self) -> None:
        assert _is_skill_authored_overlay(None) is False

    def test_is_skill_authored_no_provenance(self) -> None:
        # Pack with no provenance → not skill-authored.
        from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import (
            load_pack,
        )
        pack = load_pack(STARTER_PACK)
        # The starter pack has no provenance block.
        # _is_skill_authored_overlay expects an entry overlay; passing a
        # base pack with no provenance is the equivalent of "no
        # skill-authored overlay in play".
        assert _is_skill_authored_overlay(pack) is False

    def test_is_skill_authored_true(
        self, bundle_dir_with_skill_overlay: Path
    ) -> None:
        overlay_root = bundle_dir_with_skill_overlay / "overlays" / "test-currency-extension"
        entry = _load_entry_overlay_provenance(overlay_root)
        assert entry is not None
        assert _is_skill_authored_overlay(entry) is True


class TestSkillProposedMechanism:
    """The round-2 finding: AutoResolved on a skill-added candidate must
    stamp ``mechanism: skill_proposed`` even without a --resolutions file."""

    def test_initial_onboarding_records_skill_proposed(
        self, bundle_dir_with_skill_overlay: Path
    ) -> None:
        # Bronze contains ONLY ApInvoicesXCurrCode (the skill-added
        # candidate). The walker AutoResolves on it.
        bronze = {
            "erp_suppliers": ["VENDORID", "SEGMENT1"],
            "ap_invoices": ["ApInvoicesXCurrCode", "ApInvoicesCancelledDate"],
            "gl_coa": [
                "CodeCombinationSegment1",
                "CodeCombinationSegment2",
                "CodeCombinationSegment3",
            ],
            "gl_period_balances": [],
        }
        bundle = _load_bundle(bundle_dir_with_skill_overlay / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir_with_skill_overlay / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(bronze),
                non_interactive=True,
            ),
        )
        assert outcome.exit_code == 0
        profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
        # Mechanism: skill_proposed (NOT auto_resolve) because the chosen
        # candidate matches the overlay's proposals entry.
        assert profile["provenance"]["approvedBy"]["mechanism"] == "skill_proposed"
        # Pinned the skill-added candidate.
        assert profile["resolved"]["column"]["invoice_currency_code"] == (
            "ApInvoicesXCurrCode"
        )

    def test_snapshot_carries_skill_version_and_impact(
        self, bundle_dir_with_skill_overlay: Path
    ) -> None:
        bronze = {
            "erp_suppliers": ["VENDORID", "SEGMENT1"],
            "ap_invoices": ["ApInvoicesXCurrCode", "ApInvoicesCancelledDate"],
            "gl_coa": [
                "CodeCombinationSegment1",
                "CodeCombinationSegment2",
                "CodeCombinationSegment3",
            ],
            "gl_period_balances": [],
        }
        bundle = _load_bundle(bundle_dir_with_skill_overlay / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir_with_skill_overlay / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(bronze),
                non_interactive=True,
            ),
        )
        assert outcome.exit_code == 0
        snapshot_payload = yaml.safe_load(
            outcome.evidence_path.read_text(encoding="utf-8")
        )
        # Top-level provenance.skillVersion populated from overlay.
        assert snapshot_payload["provenance"]["skillVersion"] == "0.1.0"
        # Per-resolution incremental_impact mirrors overlay's.
        snap_entry = snapshot_payload["provenance"]["evidence"]["snapshots"][0]
        for res in snap_entry["resolutions"]:
            if res["name"] == "invoice_currency_code":
                impact = res["incrementalImpact"]
                assert impact["newCandidate"] == "ApInvoicesXCurrCode"
                assert impact["remediation"]["operatorChose"] == "D"
                return
        pytest.fail("invoice_currency_code resolution not in snapshot")
