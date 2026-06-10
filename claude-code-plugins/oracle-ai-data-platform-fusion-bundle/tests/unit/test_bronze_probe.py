"""Unit tests for :mod:`oracle_ai_data_platform_fusion_bundle.commands.bronze_probe`.

Tests use ``unittest.mock.MagicMock`` to simulate a Spark session whose
``sql("DESCRIBE TABLE ...")`` returns fixture rows. The integration
piece — walking every starter-pack variation point against the
saasfademo1 fixture — lives in ``test_variation_resolver_integration.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_bundle.commands.bronze_probe import (
    BronzeProbeFailure,
    describe_bronze,
)
from oracle_ai_data_platform_fusion_bundle.schema.bronze_fingerprint import (
    ColumnInfo,
)


def _row(col_name: str, data_type: str = "string"):
    """Build a tuple-shaped row that the probe's accessor can read."""
    return {"col_name": col_name, "data_type": data_type, "comment": None}


def _mock_spark(per_table_rows: dict[str, list[dict]]) -> MagicMock:
    """Build a MagicMock Spark whose `sql()` returns per-DESCRIBE rows."""
    spark = MagicMock(name="spark")

    def _sql(query: str):
        # Query is "DESCRIBE TABLE <catalog>.<schema>.<dataset>".
        target = query.split()[-1]  # "<catalog>.<schema>.<dataset>"
        dataset = target.split(".")[-1]
        rows = per_table_rows.get(dataset, [])
        df = MagicMock(name=f"df_{dataset}")
        df.collect.return_value = rows
        return df

    spark.sql.side_effect = _sql
    return spark


class TestProbe:
    def test_describes_each_dataset_once(self) -> None:
        spark = _mock_spark(
            {
                "erp_suppliers": [_row("VENDORID"), _row("SEGMENT1")],
                "ap_invoices": [_row("ApInvoicesInvoiceCurrencyCode")],
            }
        )
        observed = describe_bronze(
            spark,
            catalog="cat",
            bronze_schema="bronze",
            dataset_ids=["erp_suppliers", "ap_invoices"],
        )
        assert observed["erp_suppliers"] == [
            ColumnInfo(name="VENDORID", type="string"),
            ColumnInfo(name="SEGMENT1", type="string"),
        ]
        assert observed["ap_invoices"] == [
            ColumnInfo(name="ApInvoicesInvoiceCurrencyCode", type="string"),
        ]

    def test_skips_detailed_info_header(self) -> None:
        """Spark's extended DESCRIBE emits ``# col_name`` / ``# Detailed
        Table Information`` rows after the real columns; the probe
        truncates at the first ``#``-prefixed col_name."""
        spark = _mock_spark(
            {
                "erp_suppliers": [
                    _row("VENDORID"),
                    _row("SEGMENT1"),
                    {"col_name": "", "data_type": "", "comment": ""},  # blank separator
                    {"col_name": "# col_name", "data_type": "data_type", "comment": "comment"},
                    {
                        "col_name": "# Detailed Table Information",
                        "data_type": "",
                        "comment": "",
                    },
                    {"col_name": "Database", "data_type": "default", "comment": ""},
                ],
            }
        )
        observed = describe_bronze(
            spark,
            catalog="cat",
            bronze_schema="bronze",
            dataset_ids=["erp_suppliers"],
        )
        # `Database` row is metadata, not a column — must be excluded.
        assert observed["erp_suppliers"] == [
            ColumnInfo(name="VENDORID", type="string"),
            ColumnInfo(name="SEGMENT1", type="string"),
        ]

    def test_raises_bronze_probe_failure_on_spark_error(self) -> None:
        spark = MagicMock(name="spark")
        spark.sql.side_effect = RuntimeError("table not found")
        with pytest.raises(BronzeProbeFailure) as excinfo:
            describe_bronze(
                spark,
                catalog="cat",
                bronze_schema="bronze",
                dataset_ids=["erp_suppliers"],
            )
        assert excinfo.value.dataset_id == "erp_suppliers"
        assert "erp_suppliers" in str(excinfo.value)
        assert isinstance(excinfo.value.cause, RuntimeError)
