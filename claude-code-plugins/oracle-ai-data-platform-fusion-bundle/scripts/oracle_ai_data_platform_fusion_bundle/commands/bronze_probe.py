"""Spark ``DESCRIBE TABLE`` wrapper for bootstrap's variation phase.

Probes each declared bronze dataset once, returning a
``{dataset_id: [ColumnInfo, ...]}`` mapping the walker
(:mod:`variation_resolver`) and the fingerprint helper
(:mod:`schema.bronze_fingerprint`) both consume.

The probe is the only Spark-touching seam in this feature — the walker
and the fingerprint algorithm are pure-Python. Tests inject a mock
Spark session whose ``sql("DESCRIBE TABLE ...")`` returns fixture rows.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ..schema.bronze_fingerprint import ColumnInfo

if TYPE_CHECKING:  # pragma: no cover — Spark import-guard
    from pyspark.sql import SparkSession


# SQL identifier allowlist. Standard unquoted Spark SQL identifier
# shape: alphanumeric / underscore start, alphanumeric / underscore
# body. No dots (each part of the three-part name is validated
# separately), no whitespace, no shell metacharacters, no quoting
# tokens. Rejects semicolons, backticks, single/double quotes,
# parentheses, dashes — anything that could alter the SQL text
# bootstrap issues. Matches the spirit of the renderer's identifier
# allowlist (``schema.identifier_validation``) — the bootstrap path
# enforces the same invariant before any Spark call.
_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class UnsafeIdentifierError(ValueError):
    """Raised when an SQL identifier (catalog / schema / dataset)
    fails the allowlist regex.

    Without this check the f-string interpolation in
    :func:`describe_bronze` would allow a malformed or malicious bundle
    to inject arbitrary SQL into the DESCRIBE TABLE statement
    (semicolons, backticks, whitespace, dotted IDs). Validation
    fails closed BEFORE any Spark call.
    """

    def __init__(self, *, value: str, field: str) -> None:
        self.value = value
        self.field = field
        super().__init__(
            f"unsafe SQL identifier for {field}={value!r}; must match "
            f"^[A-Za-z_][A-Za-z0-9_]*$. No semicolons, backticks, "
            f"whitespace, or dotted IDs."
        )


def _validate_identifier(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise UnsafeIdentifierError(value=str(value), field=field)
    if not _SQL_IDENT_RE.match(value):
        raise UnsafeIdentifierError(value=value, field=field)
    return value


def describe_bronze(
    spark: "SparkSession",
    *,
    catalog: str,
    bronze_schema: str,
    dataset_ids: list[str],
) -> dict[str, list[ColumnInfo]]:
    """Run ``DESCRIBE TABLE`` against each bronze dataset and return the
    parsed column metadata.

    Args:
        spark: an active Spark session.
        catalog: e.g. ``"fusion_catalog"``. Validated against the
            SQL-identifier allowlist; rejected with
            :class:`UnsafeIdentifierError` on failure.
        bronze_schema: e.g. ``"bronze"``. Same allowlist.
        dataset_ids: bronze dataset ids declared in the pack's
            ``bronze.yaml`` (e.g. ``["erp_suppliers", "ap_invoices"]``).
            Each id is validated.

    Returns:
        ``{dataset_id: [ColumnInfo, ...]}``. The walker takes a
        ``set[str]`` of column names per dataset; the fingerprint helper
        takes the full ColumnInfo list. Bootstrap (Step 8) feeds both
        from this single probe.

    Raises:
        UnsafeIdentifierError: any of ``catalog`` / ``bronze_schema`` /
            one of ``dataset_ids`` failed the SQL-identifier allowlist.
            Bootstrap fails closed BEFORE issuing the Spark query.
        BronzeProbeFailure: a ``DESCRIBE`` query failed for any dataset.
            Wraps the underlying Spark exception with the dataset id so
            the operator knows which bronze table was unreachable.
    """
    # Fail-closed identifier validation BEFORE any Spark call.
    _validate_identifier(catalog, field="aidp.catalog")
    _validate_identifier(bronze_schema, field="aidp.bronzeSchema")
    for dataset_id in dataset_ids:
        _validate_identifier(dataset_id, field="bronze.dataset.id")

    out: dict[str, list[ColumnInfo]] = {}
    for dataset_id in dataset_ids:
        fully_qualified = f"{catalog}.{bronze_schema}.{dataset_id}"
        try:
            rows = spark.sql(f"DESCRIBE TABLE {fully_qualified}").collect()
        except Exception as exc:  # noqa: BLE001 — Spark raises a variety of types
            raise BronzeProbeFailure(
                dataset_id=dataset_id,
                fully_qualified=fully_qualified,
                cause=exc,
            ) from exc
        out[dataset_id] = _parse_describe_rows(rows)
    return out


class BronzeProbeFailure(Exception):
    """Raised when ``DESCRIBE TABLE`` cannot reach a bronze dataset.

    Bootstrap maps this to a remediation message naming the offending
    dataset; the operator's typical cause is a missing bronze schema
    (the pre-onboarding probes should catch this, but the variation
    phase runs after them).
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        fully_qualified: str,
        cause: Exception,
    ) -> None:
        self.dataset_id = dataset_id
        self.fully_qualified = fully_qualified
        self.cause = cause
        super().__init__(
            f"DESCRIBE TABLE failed for {fully_qualified} ({dataset_id}): "
            f"{type(cause).__name__}: {cause}"
        )


def _parse_describe_rows(rows: list) -> list[ColumnInfo]:
    """Convert Spark DESCRIBE TABLE Row objects to ColumnInfo.

    Spark emits these output shapes for DESCRIBE:

    * Standard (Spark 3.x): ``col_name``, ``data_type``, ``comment``.
    * Extended: additional partition / detailed-info rows after a
      ``# col_name`` header. We stop reading at the first ``#``-prefixed
      ``col_name`` so partition / detailed-info rows don't pollute the
      column list.
    * Empty / null ``col_name`` rows separate sections; drop them.
    """
    columns: list[ColumnInfo] = []
    for row in rows:
        col_name = _row_field(row, "col_name", 0)
        data_type = _row_field(row, "data_type", 1)
        if col_name is None or not str(col_name).strip():
            continue
        name = str(col_name)
        if name.startswith("#"):
            # Detailed-info / partition header — everything after this
            # is metadata, not column rows.
            break
        if data_type is None:
            continue
        columns.append(ColumnInfo(name=name, type=str(data_type)))
    return columns


def _row_field(row, name: str, index: int):
    """Read a Row attribute by name (preferred) or positionally.

    Mocked rows in tests are often plain tuples; Spark's real Rows
    expose ``asDict()`` or attribute access. Try attribute first, then
    fall back to positional indexing.
    """
    # Attribute / dict access.
    try:
        return row[name]  # works for Spark Row + dict
    except (KeyError, TypeError, IndexError, AttributeError):
        pass
    try:
        return getattr(row, name)
    except AttributeError:
        pass
    # Positional fallback.
    try:
        return row[index]
    except (IndexError, TypeError, KeyError):
        return None


__all__ = ["BronzeProbeFailure", "UnsafeIdentifierError", "describe_bronze"]
