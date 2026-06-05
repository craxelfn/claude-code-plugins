"""Integration tests for the bootstrap variation phase
(:mod:`oracle_ai_data_platform_fusion_bundle.commands.variation_phase`).

Drives the full pipeline (probe → walk → write profile + evidence) with
an injected mock Spark and asserts:

* Happy path: profile + evidence files produced; resolutions match the
  starter-pack expectations.
* AIDPF-1020: missing operator identity → exit 1 + identity diagnostic
  written; no profile or evidence.
* AIDPF-2010 aggregation: two unresolved required columnAliases →
  two distinct diagnostic files; no profile or evidence.
* Multi-match with ``--non-interactive``: auto-picks first candidate
  deterministically.
* Multi-match with ``--resolutions`` JSON: scripted choice flows through.
* Workdir anchor: artifacts land relative to ``bundle.yaml`` parent,
  not ``cwd``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from oracle_ai_data_platform_fusion_bundle.commands.variation_phase import (
    VariationPhaseOptions,
    run_variation_phase,
)
from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle


REPO_ROOT = Path(__file__).resolve().parents[2]
PACK_ROOT = (
    REPO_ROOT
    / "scripts"
    / "oracle_ai_data_platform_fusion_bundle"
    / "content_packs"
    / "fusion-finance-starter"
)


SAASFADEMO_BRONZE: dict[str, list[str]] = {
    "erp_suppliers": ["VENDORID", "SEGMENT1"],
    "ap_invoices": [
        "ApInvoicesInvoiceCurrencyCode",
        "ApInvoicesCurrencyCode",  # → MultiMatch on invoice_currency_code
        "ApInvoicesCancelledDate",
    ],
    "gl_coa": [
        "CodeCombinationSegment1",
        "CodeCombinationSegment2",
        "CodeCombinationSegment3",
    ],
    "gl_period_balances": ["PeriodNetCredit"],
}


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
def bundle_dir(tmp_path: Path) -> Path:
    """Create a tmp bundle dir with bundle.yaml referencing the starter pack."""
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
                    "name": "fusion-finance-starter",
                    "path": str(PACK_ROOT),
                    "profile": "finance-default",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return tmp_path


def _load_bundle(bundle_path: Path) -> Bundle:
    raw = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    return Bundle.model_validate(raw)


# ---------------------------------------------------------------------------
# Happy path with ResolutionsInput
# ---------------------------------------------------------------------------


class TestHappyPathWithScriptedResolutions:
    def test_writes_profile_and_evidence_with_resolved_currency(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle_path = bundle_dir / "bundle.yaml"
        bundle = _load_bundle(bundle_path)

        resolutions_file = bundle_dir / "resolutions.json"
        resolutions_file.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "tenant": "finance-default",
                    "resolutions": [
                        {
                            "name": "invoice_currency_code",
                            "kind": "columnAliases",
                            "chosenCandidate": "ApInvoicesInvoiceCurrencyCode",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        outcome = run_variation_phase(
            bundle,
            bundle_path,
            options=VariationPhaseOptions(
                spark_session=_mock_spark(SAASFADEMO_BRONZE),
                resolutions_path=resolutions_file,
            ),
        )
        assert outcome.exit_code == 0
        assert outcome.profile_path == bundle_dir / "profiles" / "finance-default.yaml"
        assert outcome.profile_path.exists()
        assert outcome.evidence_path is not None
        assert outcome.evidence_path.parent == bundle_dir / "evidence" / "finance-default"

        profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
        # Every variation point resolved to the saasfademo1 conventional value.
        assert profile["resolved"]["column"] == {
            "supplier_natural_key": "SEGMENT1",
            "vendor_id": "VENDORID",
            "invoice_currency_code": "ApInvoicesInvoiceCurrencyCode",
            "coa_balancing_segment": "CodeCombinationSegment1",
            "coa_cost_center_segment": "CodeCombinationSegment2",
            "coa_natural_account_segment": "CodeCombinationSegment3",
        }
        assert profile["resolved"]["semantic"] == {"cancelled_status": "cancelled_date"}
        # Approval metadata recorded.
        approval = profile["provenance"]["approvedBy"]
        assert approval["operator"] == "alice@oracle.com"
        # Mechanism precedence: cli_flag (scripted) wins over auto_resolve.
        assert approval["mechanism"] == "cli_flag"


# ---------------------------------------------------------------------------
# AIDPF-1020 — missing operator
# ---------------------------------------------------------------------------


class TestAidpf1020IdentityGate:
    def test_missing_identity_writes_1020_artifact(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("AIDP_OPERATOR", raising=False)
        monkeypatch.delenv("USER", raising=False)
        bundle = _load_bundle(bundle_dir / "bundle.yaml")

        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(SAASFADEMO_BRONZE),
                non_interactive=True,
            ),
        )
        assert outcome.exit_code == 1
        assert len(outcome.diagnostic_paths) == 1
        assert outcome.diagnostic_paths[0].name == "AIDPF-1020.json"
        # No profile / evidence on identity-gate failure.
        assert outcome.profile_path is None
        assert outcome.evidence_path is None


# ---------------------------------------------------------------------------
# AIDPF-2010 aggregation — multiple unresolved required columnAliases
# ---------------------------------------------------------------------------


class TestAidpf2010Aggregation:
    def test_two_unresolved_columnaliases_write_two_artifacts(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")

        # Drop VENDORID + ApInvoicesInvoiceCurrencyCode → 2 NoMatch.
        drifted = {
            "erp_suppliers": ["SEGMENT1"],
            "ap_invoices": ["UnrelatedCurrencyCol", "ApInvoicesCancelledDate"],
            "gl_coa": [
                "CodeCombinationSegment1",
                "CodeCombinationSegment2",
                "CodeCombinationSegment3",
            ],
            "gl_period_balances": [],
        }
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(drifted),
            ),
        )
        assert outcome.exit_code == 1
        names = sorted(p.name for p in outcome.diagnostic_paths)
        assert names == [
            "AIDPF-2010__invoice_currency_code.json",
            "AIDPF-2010__vendor_id.json",
        ]
        # Profile + evidence MUST NOT be written when any required no-match fired.
        assert outcome.profile_path is None
        assert outcome.evidence_path is None


# ---------------------------------------------------------------------------
# --non-interactive multi-match auto-pick
# ---------------------------------------------------------------------------


class TestNonInteractiveMultiMatch:
    def test_auto_picks_first_candidate(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")

        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(SAASFADEMO_BRONZE),
                non_interactive=True,
            ),
        )
        assert outcome.exit_code == 0
        profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
        # Priority order says ApInvoicesInvoiceCurrencyCode comes first.
        assert profile["resolved"]["column"]["invoice_currency_code"] == (
            "ApInvoicesInvoiceCurrencyCode"
        )
        # Mechanism: non_interactive (multi-match auto-picked).
        assert profile["provenance"]["approvedBy"]["mechanism"] == "non_interactive"


# ---------------------------------------------------------------------------
# Workdir anchor — artifacts land beside bundle.yaml, not cwd
# ---------------------------------------------------------------------------


class TestWorkdirAnchor:
    def test_writes_under_bundle_parent_not_cwd(
        self,
        bundle_dir: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")

        other_cwd = tmp_path_factory.mktemp("elsewhere")
        monkeypatch.chdir(other_cwd)

        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(SAASFADEMO_BRONZE),
                non_interactive=True,
            ),
        )
        assert outcome.exit_code == 0
        # Artifacts under bundle_dir, NOT other_cwd.
        assert outcome.profile_path is not None
        assert outcome.profile_path.is_relative_to(bundle_dir)
        assert outcome.evidence_path.is_relative_to(bundle_dir)
        # And nothing accidentally written under cwd.
        assert not (other_cwd / "profiles").exists()
        assert not (other_cwd / "evidence").exists()
