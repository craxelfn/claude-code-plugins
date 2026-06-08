"""Test fixture — a v1-shaped gold-mart ``build`` callable.

Mimics ``transforms/gold/supplier_spend.py::build``:
``(spark, *, paths, bronze_invoices, silver_dim, gold_table,
refresh_mode, watermark, run_id) -> DataFrame``. The fixture captures
kwargs so binding-layer tests can assert the v1-conventional names
land as expected.

Also exposes a narrower-signature variant (:func:`build_narrow`) that
accepts only a subset of the canonical binding-layer kwargs — used to
exercise the adapter's ``inspect.signature`` filtering.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

CAPTURED_KWARGS: dict[str, Any] = {}


def reset() -> None:
    CAPTURED_KWARGS.clear()


def build(
    spark,
    *,
    paths,
    bronze_invoices: str | None = None,
    silver_dim: str | None = None,
    gold_table: str | None = None,
    refresh_mode: Literal["seed", "incremental"] = "seed",
    watermark: datetime | None = None,
    run_id: str | None = None,
):
    """Fixture v1 gold-mart builder — captures kwargs, materialises target."""
    CAPTURED_KWARGS.update({
        "paths": paths,
        "bronze_invoices": bronze_invoices,
        "silver_dim": silver_dim,
        "gold_table": gold_table,
        "refresh_mode": refresh_mode,
        "watermark": watermark,
        "run_id": run_id,
    })

    spark.sql(
        f"CREATE OR REPLACE TABLE {gold_table} AS "
        f"SELECT 1 AS supplier_key, 100.0 AS total_amount"
    )
    return spark.table(gold_table)


def build_narrow(spark, *, paths, silver_table=None):
    """Narrow-signature fixture — accepts only ``(spark, paths, silver_table)``.

    Exercises the adapter's ``inspect.signature`` filtering: the
    binding layer constructs ``run_id``, ``refresh_mode``, ``watermark``,
    etc., but this callable only accepts three kwargs. Without
    filtering the adapter would raise ``TypeError``.
    """
    CAPTURED_KWARGS.update({
        "paths": paths,
        "silver_table": silver_table,
    })
    spark.sql(f"CREATE OR REPLACE TABLE {silver_table} AS SELECT 1 AS x")
    return spark.table(silver_table)
