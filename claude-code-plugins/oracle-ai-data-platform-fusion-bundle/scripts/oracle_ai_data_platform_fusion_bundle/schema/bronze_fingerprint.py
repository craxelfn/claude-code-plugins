"""Shared bronze-schema fingerprint helper (PLAN §9.5.4 / §11.6).

Single source of truth for the algorithm bootstrap (this feature) uses to
PIN ``profile.bronzeSchemaFingerprint`` and feature #4
(``v2-phase-3c-runtime-preflight-evidence``) will use to COMPARE the live
bronze against the pinned value. If the two paths computed fingerprints
differently the drift gate would silently misfire — keep this as the
only producer of the value.

Algorithm:

1. For each dataset id in ``datasets``, the caller has either:

   * supplied a pre-probed ``observed`` mapping (dataset → list of
     :class:`ColumnInfo`), or
   * not supplied one, in which case we run ``DESCRIBE TABLE`` ourselves
     via :func:`compute_bronze_fingerprint`'s Spark wrapper.

2. Per dataset: drop metadata rows (the ``#`` partition header + comment
   rows Spark emits) and keep only ``(col_name, data_type, nullable)``
   triples for real columns.
3. Canonicalise each triple: lowercase ``col_name`` + ``data_type``,
   strip whitespace, drop nullable/comment metadata that Spark may
   reorder cosmetically. The Phase 2 contract is **type-shape stability**,
   not nullability — nullability is allowed to drift without changing
   the fingerprint (Hive/Spark surfaces it inconsistently across
   versions, and silver/gold logic re-asserts nullability anyway).
4. Sort the columns by ``col_name`` ascending. Stable sort guarantees
   that physical-column reordering on the Spark side (a cosmetic
   change) does not change the fingerprint.
5. Sort datasets by id (callers may pass the list in any order or
   include duplicates — the algorithm is idempotent).
6. Build a stable JSON serialisation, then SHA-256.

Returns: ``"sha256:<hex>"`` (the prefix matches the existing
``TenantProfile.bronzeSchemaFingerprint`` field's documented shape in
``examples/profiles/finance-default.yaml:30``).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:  # pragma: no cover — import guard for Spark
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class ColumnInfo:
    """One bronze column as observed via ``DESCRIBE TABLE``.

    The fields are exactly what bootstrap's walker (Step 6) needs to
    answer "does this column exist?" + what the fingerprint algorithm
    needs to detect type drift.
    """

    name: str
    type: str
    nullable: bool = True


def compute_bronze_fingerprint(
    *,
    observed: dict[str, list[ColumnInfo]],
) -> str:
    """Compute the canonical bronze-schema fingerprint from an
    already-probed observation.

    Pure function — does not call Spark. The variation-phase wiring
    (Step 8) probes via :mod:`bronze_probe`, then passes the
    ``observed`` dict here so the probe is run exactly once.

    Args:
        observed: ``{dataset_id: [ColumnInfo, ...]}`` mapping. Bootstrap
            populates this from the live ``DESCRIBE``; tests may pass a
            hand-built fixture.

    Returns:
        ``"sha256:<64-hex>"`` string ready to assign to
        :attr:`TenantProfile.bronze_schema_fingerprint`.
    """
    payload = [
        {
            "dataset": dataset_id,
            "columns": [
                {"name": col.name.strip().lower(), "type": col.type.strip().lower()}
                for col in sorted(
                    _dedupe_by_name(observed[dataset_id]),
                    key=lambda c: c.name.strip().lower(),
                )
            ],
        }
        for dataset_id in sorted(observed)
    ]
    serialised = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(serialised.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _dedupe_by_name(columns: Iterable[ColumnInfo]) -> list[ColumnInfo]:
    """Drop duplicate column names within a dataset.

    DESCRIBE rarely emits the same column twice, but Spark partition
    metadata can show columns once in the header + once in the partition
    footer; canonicalisation drops the duplicate. Keeps the first
    occurrence's type — type drift surfaces via the per-row sort below.
    """
    seen: set[str] = set()
    result: list[ColumnInfo] = []
    for col in columns:
        key = col.name.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(col)
    return result


__all__ = ["ColumnInfo", "compute_bronze_fingerprint"]
