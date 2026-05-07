"""gold.supplier_spend — supplier × approval-status spend mart.

Productizes TC8's prototype which proved the SQL on `saasfademo1` (etap-dev5)
demo pod: $3.2B aggregate / 236 records / top vendor `300000047507499` at
$892.7M.

Two forms — picked at runtime by :func:`build` per
:func:`...dimensions.dim_supplier.id_populated_pct`:

* **JOIN form** (canonical) — `silver.dim_supplier × bronze.ap_invoices` on
  `vendor_id = ApInvoicesVendorId`. Production pods land here. Carries
  supplier_name + business_relationship from the dim.

* **Spend-only fallback** — when supplier IDs aren't populated on the source
  pod (demo, e.g. eseb-test). Aggregates `bronze.ap_invoices` alone, keyed
  by `ApInvoicesVendorId`. supplier_name is NULL since no dim join is possible.

Both forms produce the same output schema. Downstream consumers (workbooks,
GenAI prompts, JDBC clients) don't need to know which form ran — only that
``gold.supplier_spend`` is queryable with a stable contract.

Bronze column convention
------------------------

`bronze.ap_invoices` from the BICC ``aidataplatform`` connector for
``InvoiceHeaderExtractPVO`` uses **PascalCase with `ApInvoices` prefix**
(``ApInvoicesVendorId``, ``ApInvoicesInvoiceAmount``, etc.). This is
different from `bronze.erp_suppliers` (which uses UPPERCASE without
prefix). Confirmed live on ``fusion_bundle_dev`` (2026-05-07).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


SOURCE_BRONZE_TABLE: Final[str] = "fusion_catalog.bronze.ap_invoices"
SOURCE_SILVER_DIM:   Final[str] = "fusion_catalog.silver.dim_supplier"
TARGET_GOLD_TABLE:   Final[str] = "fusion_catalog.gold.supplier_spend"

# When `id_populated_pct(vendor_id)` is at or above this threshold on
# `silver.dim_supplier`, prefer the JOIN form. Below it, fall back to the
# spend-only form. 0.5 is a generous default — any pod with even half-populated
# supplier IDs benefits from the dim attributes (supplier_name, business_relationship).
DEFAULT_JOIN_THRESHOLD: Final[float] = 0.5


def build_join_form_sql(
    *,
    bronze_invoices: str = SOURCE_BRONZE_TABLE,
    silver_dim:      str = SOURCE_SILVER_DIM,
    gold_table:      str = TARGET_GOLD_TABLE,
) -> str:
    """Canonical join-form SQL: silver.dim_supplier × bronze.ap_invoices."""
    return f"""\
CREATE OR REPLACE TABLE {gold_table}
USING DELTA
AS
SELECT
  ds.vendor_id                                                     AS vendor_id,
  ds.supplier_number                                               AS supplier_number,
  ds.supplier_name                                                 AS supplier_name,
  ds.business_relationship                                         AS business_relationship,
  inv.ApInvoicesApprovalStatus                                     AS approval_status,
  COUNT(*)                                                         AS invoice_count,
  ROUND(SUM(CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(20, 2))), 2)  AS total_invoice_amount,
  ROUND(SUM(CAST(inv.ApInvoicesAmountPaid    AS DECIMAL(20, 2))), 2)  AS total_paid,
  MAX(CAST(inv.ApInvoicesInvoiceDate AS DATE))                     AS last_invoice_date,
  current_timestamp()                                              AS gold_built_at
FROM {bronze_invoices} inv
JOIN {silver_dim} ds
  ON ds.vendor_id = CAST(inv.ApInvoicesVendorId AS BIGINT)
WHERE inv.ApInvoicesVendorId IS NOT NULL
GROUP BY
  ds.vendor_id,
  ds.supplier_number,
  ds.supplier_name,
  ds.business_relationship,
  inv.ApInvoicesApprovalStatus
"""


def build_spend_only_form_sql(
    *,
    bronze_invoices: str = SOURCE_BRONZE_TABLE,
    gold_table:      str = TARGET_GOLD_TABLE,
) -> str:
    """Fallback SQL: aggregate bronze.ap_invoices alone, no dim join.

    Used when the source pod's supplier extract returns NULL/0 ``vendor_id`` —
    common on demo pods. Output schema matches the join form, but
    ``supplier_number``, ``supplier_name``, and ``business_relationship`` are NULL.
    """
    return f"""\
CREATE OR REPLACE TABLE {gold_table}
USING DELTA
AS
SELECT
  CAST(inv.ApInvoicesVendorId AS BIGINT)                           AS vendor_id,
  CAST(NULL AS STRING)                                             AS supplier_number,
  CAST(NULL AS STRING)                                             AS supplier_name,
  CAST(NULL AS STRING)                                             AS business_relationship,
  inv.ApInvoicesApprovalStatus                                     AS approval_status,
  COUNT(*)                                                         AS invoice_count,
  ROUND(SUM(CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(20, 2))), 2)  AS total_invoice_amount,
  ROUND(SUM(CAST(inv.ApInvoicesAmountPaid    AS DECIMAL(20, 2))), 2)  AS total_paid,
  MAX(CAST(inv.ApInvoicesInvoiceDate AS DATE))                     AS last_invoice_date,
  current_timestamp()                                              AS gold_built_at
FROM {bronze_invoices} inv
WHERE inv.ApInvoicesVendorId IS NOT NULL
GROUP BY
  inv.ApInvoicesVendorId,
  inv.ApInvoicesApprovalStatus
"""


def build_supplier_spend_sql(
    *,
    use_join_form: bool,
    bronze_invoices: str = SOURCE_BRONZE_TABLE,
    silver_dim:      str = SOURCE_SILVER_DIM,
    gold_table:      str = TARGET_GOLD_TABLE,
) -> str:
    """Pick the right form's SQL based on ``use_join_form``."""
    if use_join_form:
        return build_join_form_sql(
            bronze_invoices=bronze_invoices,
            silver_dim=silver_dim,
            gold_table=gold_table,
        )
    return build_spend_only_form_sql(
        bronze_invoices=bronze_invoices,
        gold_table=gold_table,
    )


def build(
    spark: "SparkSession",
    *,
    bronze_invoices: str = SOURCE_BRONZE_TABLE,
    silver_dim:      str = SOURCE_SILVER_DIM,
    gold_table:      str = TARGET_GOLD_TABLE,
    id_populated_threshold: float = DEFAULT_JOIN_THRESHOLD,
) -> "DataFrame":
    """Materialize ``gold.supplier_spend`` using the right form per pod data shape.

    Calls :func:`...dimensions.dim_supplier.id_populated_pct` against
    ``silver_dim``; if it returns ``>= id_populated_threshold``, runs the
    JOIN form; else runs the spend-only fallback.

    Returns a DataFrame backed by the freshly-written gold table.
    """
    from ...dimensions.dim_supplier import id_populated_pct

    pct = id_populated_pct(spark, silver_table=silver_dim, column="vendor_id")
    use_join = pct >= id_populated_threshold
    sql = build_supplier_spend_sql(
        use_join_form=use_join,
        bronze_invoices=bronze_invoices,
        silver_dim=silver_dim,
        gold_table=gold_table,
    )
    spark.sql(sql)
    return spark.table(gold_table)


__all__ = [
    "SOURCE_BRONZE_TABLE",
    "SOURCE_SILVER_DIM",
    "TARGET_GOLD_TABLE",
    "DEFAULT_JOIN_THRESHOLD",
    "build",
    "build_join_form_sql",
    "build_spend_only_form_sql",
    "build_supplier_spend_sql",
]
