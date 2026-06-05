"""Unit tests for :mod:`schema.bronze_schema_snapshot` (Phase 3d).

Pure-Python — no Spark touched. Exercises the snapshot model + writer
+ loader + cross-check helpers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_bundle.schema.bronze_fingerprint import (
    ColumnInfo,
    compute_bronze_fingerprint,
)
from oracle_ai_data_platform_fusion_bundle.schema.bronze_schema_snapshot import (
    BronzeSchemaSnapshotSchemaError,
    BronzeSchemaSnapshotV1,
    from_observed,
    load_bronze_schema_snapshot,
    resolve_snapshot_path,
    snapshot_to_observed,
    write_bronze_schema_snapshot,
)
from oracle_ai_data_platform_fusion_bundle.schema.path_segment import (
    UnsafePathSegmentError,
)


def _observed_two_datasets() -> dict[str, list[ColumnInfo]]:
    return {
        "erp_suppliers": [
            ColumnInfo(name="VENDORID", type="bigint"),
            ColumnInfo(name="SEGMENT1", type="string"),
        ],
        "ap_invoices": [
            ColumnInfo(name="ApInvoicesInvoiceCurrencyCode", type="string"),
            ColumnInfo(name="ApInvoicesCancelledDate", type="timestamp"),
        ],
    }


class TestRoundTrip:
    def test_build_write_read_identical(self, tmp_path: Path) -> None:
        observed = _observed_two_datasets()
        fingerprint = compute_bronze_fingerprint(observed=observed)
        snapshot = from_observed(
            tenant="acme",
            pinned_at=datetime(2026, 6, 6, 12, tzinfo=timezone.utc),
            fingerprint=fingerprint,
            observed=observed,
        )
        path = write_bronze_schema_snapshot(tmp_path, "acme", snapshot)
        loaded = load_bronze_schema_snapshot(path)
        assert loaded.model_dump(by_alias=True) == snapshot.model_dump(
            by_alias=True
        )

    def test_cross_check_fingerprint_matches(self) -> None:
        observed = _observed_two_datasets()
        fingerprint = compute_bronze_fingerprint(observed=observed)
        snapshot = from_observed(
            tenant="acme",
            pinned_at=datetime(2026, 6, 6, 12, tzinfo=timezone.utc),
            fingerprint=fingerprint,
            observed=observed,
        )
        recomputed = compute_bronze_fingerprint(
            observed=snapshot_to_observed(snapshot)
        )
        assert recomputed == snapshot.bronze_schema_fingerprint == fingerprint

    def test_column_order_preserved_across_write_read(
        self, tmp_path: Path
    ) -> None:
        observed = {
            "ds": [
                ColumnInfo(name="ZZZ", type="string"),
                ColumnInfo(name="AAA", type="bigint"),
                ColumnInfo(name="MMM", type="double"),
            ]
        }
        snapshot = from_observed(
            tenant="acme",
            pinned_at=datetime(2026, 6, 6, 12, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=observed,
        )
        path = write_bronze_schema_snapshot(tmp_path, "acme", snapshot)
        loaded = load_bronze_schema_snapshot(path)
        assert [c.name for c in loaded.datasets[0].columns] == [
            "ZZZ",
            "AAA",
            "MMM",
        ]


class TestValidation:
    def test_extra_top_level_key_rejected(self) -> None:
        with pytest.raises(Exception):  # Pydantic ValidationError
            BronzeSchemaSnapshotV1.model_validate(
                {
                    "schemaVersion": 1,
                    "tenant": "acme",
                    "pinnedAt": "2026-06-06T12:00:00+00:00",
                    "bronzeSchemaFingerprint": "sha256:" + "a" * 64,
                    "datasets": [],
                    "extraField": "nope",
                }
            )

    def test_unsupported_schema_version_rejected(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "snap.yaml"
        path.write_text(
            "schemaVersion: 2\n"
            "tenant: acme\n"
            "pinnedAt: '2026-06-06T12:00:00+00:00'\n"
            "bronzeSchemaFingerprint: sha256:" + "a" * 64 + "\n"
            "datasets: []\n",
            encoding="utf-8",
        )
        with pytest.raises(BronzeSchemaSnapshotSchemaError) as exc_info:
            load_bronze_schema_snapshot(path)
        assert "schemaVersion" in str(exc_info.value)

    def test_malformed_yaml_rejected(self, tmp_path: Path) -> None:
        path = tmp_path / "snap.yaml"
        path.write_text(":\n:not-yaml-at-all\n  - {", encoding="utf-8")
        with pytest.raises(BronzeSchemaSnapshotSchemaError):
            load_bronze_schema_snapshot(path)


class TestPathSafety:
    def test_path_traversal_in_profile_name_rejected(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(UnsafePathSegmentError):
            resolve_snapshot_path(tmp_path / "bundle.yaml", "../escape")
        with pytest.raises(UnsafePathSegmentError):
            snapshot = from_observed(
                tenant="acme",
                pinned_at=datetime(2026, 6, 6, 12, tzinfo=timezone.utc),
                fingerprint="sha256:" + "a" * 64,
                observed={},
            )
            write_bronze_schema_snapshot(tmp_path, "../escape", snapshot)


class TestAtomicWrite:
    def test_no_tmp_file_left_behind_on_success(
        self, tmp_path: Path
    ) -> None:
        snapshot = from_observed(
            tenant="acme",
            pinned_at=datetime(2026, 6, 6, 12, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=_observed_two_datasets(),
        )
        write_bronze_schema_snapshot(tmp_path, "acme", snapshot)
        profiles_dir = tmp_path / "profiles"
        leftovers = [
            p
            for p in profiles_dir.iterdir()
            if p.name.endswith(".tmp")
        ]
        assert leftovers == []

    def test_overwrites_on_success(self, tmp_path: Path) -> None:
        snap1 = from_observed(
            tenant="acme",
            pinned_at=datetime(2026, 6, 6, 12, tzinfo=timezone.utc),
            fingerprint="sha256:" + "a" * 64,
            observed=_observed_two_datasets(),
        )
        write_bronze_schema_snapshot(tmp_path, "acme", snap1)

        snap2 = from_observed(
            tenant="acme",
            pinned_at=datetime(2026, 6, 7, 12, tzinfo=timezone.utc),
            fingerprint="sha256:" + "b" * 64,
            observed=_observed_two_datasets(),
        )
        path = write_bronze_schema_snapshot(tmp_path, "acme", snap2)

        loaded = load_bronze_schema_snapshot(path)
        assert loaded.bronze_schema_fingerprint == "sha256:" + "b" * 64


class TestEmptyBronze:
    def test_zero_datasets_round_trips(self, tmp_path: Path) -> None:
        snapshot = from_observed(
            tenant="acme",
            pinned_at=datetime(2026, 6, 6, 12, tzinfo=timezone.utc),
            fingerprint=compute_bronze_fingerprint(observed={}),
            observed={},
        )
        path = write_bronze_schema_snapshot(tmp_path, "acme", snapshot)
        loaded = load_bronze_schema_snapshot(path)
        assert loaded.datasets == []
        assert loaded.bronze_schema_fingerprint == snapshot.bronze_schema_fingerprint
