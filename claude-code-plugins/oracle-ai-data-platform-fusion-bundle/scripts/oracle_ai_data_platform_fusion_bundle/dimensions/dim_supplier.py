"""silver.dim_supplier — conformed supplier dimension.

Reads ``bronze.erp_suppliers`` (BICC ``SupplierExtractPVO``), dedupes on the
natural key ``SEGMENT1`` (supplier_number), keeps the most recent extract per
supplier, projects 14 columns into ``silver.dim_supplier`` with bronze→silver
audit lineage.

Design notes
------------

* **Column case** — bronze columns from the BICC ``aidataplatform`` connector
  for ``SupplierExtractPVO`` are all-uppercase (``SEGMENT1``, ``VENDORID``,
  ``BUSINESSRELATIONSHIP``, …). Confirmed live on ``fusion_bundle_dev`` cluster
  (2026-05-07) and via direct CSV read of the 2026-04-30 etap-dev5 extract
  (sha256-12=c7b6c705c751). Other PVOs may differ — ``ap_invoices`` uses
  PascalCase with an ``ApInvoices`` prefix; that's P1.2's concern.

* **NULLIF on ID columns** — demo pods routinely return ``0`` for missing IDs.
  ``NULLIF(CAST(... AS BIGINT), 0)`` ensures ``NULL`` is the only "absent"
  signal, which makes :func:`id_populated_pct` honest.

* **Per-pod data shape varies** — etap-dev5 has ``VENDORID`` 100% populated;
  eseb-test has it 0%. The dim accepts both; the consumer (P1.2's
  ``gold.supplier_spend``) calls :func:`id_populated_pct` to decide between
  the canonical join form and a spend-only fallback.

* **Surrogate key strategy** — ``monotonically_increasing_id()`` for now.
  Downstream marts MUST join on ``supplier_number``, never on the surrogate.
  If a future mart needs stability across rebuilds, swap to
  ``xxhash64(supplier_number)`` non-breakingly.

* **Supplier-name column is sparse on demo pods** — eseb-test live probe shows
  no single name column is 100%-populated. We COALESCE through the most-likely
  party-name fields; production pods typically populate at least one cleanly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


SOURCE_BRONZE_TABLE: Final[str] = "fusion_catalog.bronze.erp_suppliers"
TARGET_SILVER_TABLE: Final[str] = "fusion_catalog.silver.dim_supplier"


def build_dim_supplier_sql(
    *,
    bronze_table: str = SOURCE_BRONZE_TABLE,
    silver_table: str = TARGET_SILVER_TABLE,
) -> str:
    """Return the CREATE-OR-REPLACE Delta SQL that produces ``silver.dim_supplier``.

    Pure string output — no Spark required. Used by unit tests to verify
    the projection shape; called by :func:`build` to materialize the table.

    The dedupe rule keeps the row with the most-recent ``_extract_ts`` per
    ``SEGMENT1``. NULL ``SEGMENT1`` rows are filtered (real data-quality issue
    if seen; bundle treats it as an error to surface, not silently quarantine).
    """
    return f"""\
CREATE OR REPLACE TABLE {silver_table}
USING DELTA
AS
SELECT
  monotonically_increasing_id()                                    AS supplier_key,
  SEGMENT1                                                         AS supplier_number,
  COALESCE(
    NULLIF(AlternateNamePartyName, ''),
    NULLIF(AliasPartyName,         ''),
    NULLIF(TaxReportingName,       ''),
    CAST(NULL AS STRING)
  )                                                                AS supplier_name,
  NULLIF(CAST(VENDORID         AS BIGINT), 0)                      AS vendor_id,
  NULLIF(CAST(PARTYID          AS BIGINT), 0)                      AS party_id,
  NULLIF(CAST(PARENTVENDORID   AS BIGINT), 0)                      AS parent_vendor_id,
  NULLIF(CAST(PARENTPARTYID    AS BIGINT), 0)                      AS parent_party_id,
  BUSINESSRELATIONSHIP                                             AS business_relationship,
  CAST(ENDDATEACTIVE     AS DATE)                                  AS inactive_date,
  CAST(CREATIONDATE      AS TIMESTAMP)                             AS creation_date,
  CAST(LASTUPDATEDATE    AS TIMESTAMP)                             AS last_update_date,
  _extract_ts                                                      AS bronze_extract_ts,
  _source_pvo                                                      AS bronze_source_pvo,
  current_timestamp()                                              AS silver_built_at
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY SEGMENT1 ORDER BY _extract_ts DESC) AS _rn
  FROM {bronze_table}
  WHERE SEGMENT1 IS NOT NULL
)
WHERE _rn = 1
"""


def build(
    spark: "SparkSession",
    *,
    bronze_table: str = SOURCE_BRONZE_TABLE,
    silver_table: str = TARGET_SILVER_TABLE,
) -> "DataFrame":
    """Materialize ``silver.dim_supplier`` from ``bronze.erp_suppliers``.

    Runs the SQL from :func:`build_dim_supplier_sql` against ``spark`` and
    returns a DataFrame backed by the freshly-written silver table.
    """
    spark.sql(build_dim_supplier_sql(bronze_table=bronze_table, silver_table=silver_table))
    return spark.table(silver_table)


def id_populated_pct(
    spark: "SparkSession",
    *,
    silver_table: str = TARGET_SILVER_TABLE,
    column: str = "vendor_id",
) -> float:
    """Return the fraction (0.0–1.0) of rows where ``column`` IS NOT NULL.

    Used by P1.2's ``gold.supplier_spend`` to decide between the canonical
    join-form (``vendor_id`` populated) and a spend-only fallback (``vendor_id``
    NULL on demo pods like eseb-test). Threshold convention: ``>= 0.5`` → join.
    """
    row = spark.sql(
        f"SELECT "
        f"  CAST(SUM(CASE WHEN {column} IS NOT NULL THEN 1 ELSE 0 END) AS DOUBLE) "
        f"/ NULLIF(COUNT(*), 0) AS pct "
        f"FROM {silver_table}"
    ).collect()
    if not row:
        return 0.0
    pct = row[0]["pct"]
    return float(pct) if pct is not None else 0.0


__all__ = [
    "SOURCE_BRONZE_TABLE",
    "TARGET_SILVER_TABLE",
    "build",
    "build_dim_supplier_sql",
    "id_populated_pct",
]
