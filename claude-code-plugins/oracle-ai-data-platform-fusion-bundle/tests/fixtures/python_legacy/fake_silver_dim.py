"""Test fixture — a v1-shaped silver-dim ``build`` callable.

Mimics the signature ``dimensions/dim_supplier.py::build`` ships:
``(spark, *, paths, bronze_table, silver_table, run_id, refresh_mode,
watermark) -> DataFrame``. The fixture also captures the kwargs it
was called with so tests can assert binding-layer correctness.

This module is reachable as the spec
``tests.fixtures.python_legacy.fake_silver_dim:build`` from the
python_legacy adapter under test.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

#: Last call's kwargs — set by ``build`` on every invocation so tests
#: can assert the binding layer constructed the expected dict.
CAPTURED_KWARGS: dict[str, Any] = {}

#: Toggle test fixtures into raise mode.
RAISE_ON_NEXT_CALL: bool = False


class FakeSilverDimDeliberateFailure(RuntimeError):
    """Raised by ``build`` when ``RAISE_ON_NEXT_CALL`` is True — exercises
    the ``strategy_failed`` path."""


def reset() -> None:
    """Reset module state between tests."""
    CAPTURED_KWARGS.clear()
    global RAISE_ON_NEXT_CALL
    RAISE_ON_NEXT_CALL = False


def build(
    spark,
    *,
    paths,
    bronze_table: str | None = None,
    silver_table: str | None = None,
    run_id: str | None = None,
    refresh_mode: Literal["seed", "incremental"] = "seed",
    watermark: datetime | None = None,
):
    """Fixture v1 silver-dim builder — captures kwargs, materialises the target.

    The materialisation is a no-op SQL statement against the mocked
    Spark — the real test pulls the SQL from a side_effect to verify
    the rendered identifier matches the bundle's configured paths
    (NOT ``DEFAULT_PATHS``).
    """
    CAPTURED_KWARGS.update({
        "paths": paths,
        "bronze_table": bronze_table,
        "silver_table": silver_table,
        "run_id": run_id,
        "refresh_mode": refresh_mode,
        "watermark": watermark,
    })

    if RAISE_ON_NEXT_CALL:
        raise FakeSilverDimDeliberateFailure(
            "fake_silver_dim.build: deliberate failure for adapter test"
        )

    # Materialise the target via a CREATE OR REPLACE TABLE statement.
    # The mocked Spark fixture asserts the SQL references silver_table
    # (which is paths.silver(node.target)).
    spark.sql(
        f"CREATE OR REPLACE TABLE {silver_table} AS "
        f"SELECT 1 AS supplier_key, 'ACME' AS supplier_name"
    )
    return spark.table(silver_table)
