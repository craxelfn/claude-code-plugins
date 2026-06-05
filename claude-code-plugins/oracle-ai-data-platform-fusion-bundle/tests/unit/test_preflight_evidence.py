"""Tests for :mod:`orchestrator.preflight_evidence` (Phase 3c).

Covers:

* Match → kind="match"; no artifact written.
* Mismatch → kind="drift"; artifact written; affectedVariationPoints
  computed correctly.
* mode="seed" → kind="skip_seed"; probe NOT called.
* force_skip → kind="skip_force_flag"; probe IS called (UNCONDITIONAL
  round-1 fix); both fingerprints populated.
* Legacy fingerprint (None / placeholder / malformed) →
  kind="skip_legacy_profile"; WARN log once.
* Round-trip with bootstrap: starter pack pinned via the same
  algorithm yields kind="match".
* dataset_ids source consistency: probe pulls from
  pack.bronze_yaml["datasets"] (NOT bundle.datasets).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_bundle.orchestrator.preflight_evidence import (
    PreflightOutcome,
    _bronze_dataset_ids,
    _is_legacy_fingerprint,
    _reset_legacy_warn,
    check_bronze_fingerprint_drift,
)
from oracle_ai_data_platform_fusion_bundle.schema.bronze_fingerprint import (
    compute_bronze_fingerprint,
    ColumnInfo,
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
# Test fixtures (mocked Spark + bundle/pack/profile)
# ---------------------------------------------------------------------------


def _row(name: str, type_: str = "string"):
    return {"col_name": name, "data_type": type_, "comment": None}


def _mock_spark(per_dataset_cols: dict[str, list[str]]) -> MagicMock:
    spark = MagicMock(name="spark")

    def _sql(query: str):
        target = query.split()[-1]
        dataset = target.split(".")[-1]
        cols = per_dataset_cols.get(dataset, [])
        df = MagicMock(name=f"df_{dataset}")
        df.collect.return_value = [_row(c) for c in cols]
        return df

    spark.sql.side_effect = _sql
    return spark


def _mock_bundle():
    bundle = MagicMock(name="bundle")
    bundle.aidp.catalog = "cat"
    bundle.aidp.bronze_schema = "bronze"
    return bundle


def _mock_pack(datasets: list[str]):
    pack = MagicMock(name="pack")
    pack.bronze_yaml = {"datasets": [{"id": d} for d in datasets]}
    return pack


def _mock_profile(
    *,
    pinned: str | None,
    resolved_column: dict[str, str] | None = None,
    resolved_semantic: dict[str, str] | None = None,
):
    profile = MagicMock(name="profile")
    profile.tenant = "finance-default"
    profile.pinned_at = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    profile.bronze_schema_fingerprint = pinned
    profile.resolved.column = resolved_column or {}
    profile.resolved.semantic = resolved_semantic or {}
    return profile


@pytest.fixture(autouse=True)
def _reset_warn():
    _reset_legacy_warn()
    yield
    _reset_legacy_warn()


# ---------------------------------------------------------------------------
# Match path
# ---------------------------------------------------------------------------


class TestMatch:
    def test_matching_fingerprint_returns_match(self, tmp_path: Path) -> None:
        # Compute the live fingerprint for the fixture bronze, then
        # use it as the "pinned" value → match.
        observed = {
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesInvoiceCurrencyCode", type="string"),
            ]
        }
        pinned = compute_bronze_fingerprint(observed=observed)

        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark({"ap_invoices": ["ApInvoicesInvoiceCurrencyCode"]}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(pinned=pinned),
            run_id="cp-test-1",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "match"
        assert outcome.diagnostic_path is None
        # No artifact written under workdir.
        assert not (tmp_path / ".aidp" / "diagnostics").exists()


# ---------------------------------------------------------------------------
# Drift path
# ---------------------------------------------------------------------------


class TestDrift:
    def test_mismatch_returns_drift_and_writes_artifact(
        self, tmp_path: Path
    ) -> None:
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark({"ap_invoices": ["ApInvoicesXCurrCode"]}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(
                pinned="sha256:" + "a" * 64,
                resolved_column={"invoice_currency_code": "ApInvoicesInvoiceCurrencyCode"},
            ),
            run_id="cp-drift-1",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        # Artifact written at the expected path.
        assert outcome.diagnostic_path == (
            tmp_path / ".aidp" / "diagnostics" / "cp-drift-1" / "AIDPF-2012.json"
        )
        assert outcome.diagnostic_path.exists()
        # Hand-off message references the failing VP.
        assert "AIDPF-2012" in outcome.summary
        assert "bootstrap --refresh" in outcome.summary

    def test_affected_variation_points_diff(self, tmp_path: Path) -> None:
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark(
                {"ap_invoices": ["ApInvoicesCurrencyCode"]}  # pinned column dropped
            ),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(
                pinned="sha256:" + "a" * 64,
                resolved_column={
                    "invoice_currency_code": "ApInvoicesInvoiceCurrencyCode",  # gone
                    "vendor_id": "VENDORID",  # absent too
                },
                resolved_semantic={"cancelled_status": "cancelled_date"},
            ),
            run_id="cp-drift-2",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "drift"
        import json
        payload = json.loads(outcome.diagnostic_path.read_text(encoding="utf-8"))
        affected = payload["schemaDrift"]["affectedVariationPoints"]
        by_name = {vp["name"]: vp for vp in affected}
        # Both columnAlias-pinned values are missing from bronze.
        assert by_name["invoice_currency_code"]["stillExistsOnBronze"] is False
        assert by_name["vendor_id"]["stillExistsOnBronze"] is False
        # SemanticVariant pin is reported "still exists" by convention
        # (the id isn't a bronze column).
        assert by_name["cancelled_status"]["stillExistsOnBronze"] is True


# ---------------------------------------------------------------------------
# Skip paths
# ---------------------------------------------------------------------------


class TestSkipPaths:
    def test_seed_mode_skips_without_probe(self, tmp_path: Path) -> None:
        spark = _mock_spark({})
        outcome = check_bronze_fingerprint_drift(
            spark=spark,
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(pinned="sha256:" + "a" * 64),
            run_id="cp-seed-1",
            mode="seed",
            workdir=tmp_path,
        )
        assert outcome.kind == "skip_seed"
        # Probe NOT called.
        spark.sql.assert_not_called()

    def test_force_skip_probes_and_records_both_fingerprints(
        self, tmp_path: Path
    ) -> None:
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark({"ap_invoices": ["ApInvoicesXCurrCode"]}),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(pinned="sha256:" + "a" * 64),
            run_id="cp-force-1",
            mode="incremental",
            workdir=tmp_path,
            force_skip=True,
        )
        assert outcome.kind == "skip_force_flag"
        # Both fingerprints populated — the audit row needs them.
        assert outcome.prior_fingerprint == "sha256:" + "a" * 64
        assert outcome.current_fingerprint is not None
        assert outcome.current_fingerprint.startswith("sha256:")
        # No artifact written under workdir.
        assert not (tmp_path / ".aidp" / "diagnostics").exists()

    @pytest.mark.parametrize(
        "pinned",
        [
            None,
            "sha256:placeholder-finance-default-2026-06-05",
            "not-a-fingerprint",
            "sha256:tooshort",
        ],
    )
    def test_legacy_fingerprint_variants_skip(
        self, tmp_path: Path, pinned: str | None
    ) -> None:
        spark = _mock_spark({})
        outcome = check_bronze_fingerprint_drift(
            spark=spark,
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["ap_invoices"]),
            profile=_mock_profile(pinned=pinned),
            run_id="cp-legacy-1",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "skip_legacy_profile"
        # Probe NOT attempted on legacy profiles.
        spark.sql.assert_not_called()


# ---------------------------------------------------------------------------
# Round-trip + helpers
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def test_bronze_dataset_ids_from_pack(self) -> None:
        """Round-1 finding: dataset_ids MUST come from
        pack.bronze_yaml["datasets"], NOT bundle.datasets."""
        pack = _mock_pack(["erp_suppliers", "ap_invoices", "gl_coa"])
        assert _bronze_dataset_ids(pack) == ["erp_suppliers", "ap_invoices", "gl_coa"]

    def test_starter_pack_round_trip(self, tmp_path: Path) -> None:
        """Compute fingerprint from a starter-pack-shaped observation,
        feed it back in as pinned → match. Pins the round-trip
        contract."""
        observed = {
            "erp_suppliers": [ColumnInfo(name="VENDORID", type="string")],
            "ap_invoices": [
                ColumnInfo(name="ApInvoicesInvoiceCurrencyCode", type="string"),
            ],
        }
        pinned = compute_bronze_fingerprint(observed=observed)
        outcome = check_bronze_fingerprint_drift(
            spark=_mock_spark(
                {
                    "erp_suppliers": ["VENDORID"],
                    "ap_invoices": ["ApInvoicesInvoiceCurrencyCode"],
                }
            ),
            bundle=_mock_bundle(),
            bundle_path=tmp_path / "bundle.yaml",
            pack=_mock_pack(["erp_suppliers", "ap_invoices"]),
            profile=_mock_profile(pinned=pinned),
            run_id="cp-roundtrip-1",
            mode="incremental",
            workdir=tmp_path,
        )
        assert outcome.kind == "match"


class TestLegacyDetection:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, True),
            ("", True),
            ("sha256:placeholder-x", True),
            ("not-a-fingerprint", True),
            ("sha256:abc", True),  # too short
            ("sha256:" + "a" * 64, False),  # valid
            ("sha256:" + "A" * 64, True),  # uppercase hex — strict regex
        ],
    )
    def test_legacy_fingerprint_classification(
        self, value: str | None, expected: bool
    ) -> None:
        assert _is_legacy_fingerprint(value) is expected
