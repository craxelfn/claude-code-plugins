"""gold.supplier_spend — supplier x approval-status spend mart.

Productizes TC8's prototype which proved the SQL on `saasfademo1` (etap-dev5)
demo pod: $3.2B aggregate / 236 records / top vendor `300000047507499` at
$892.7M.

Design — single LEFT-JOIN form (financial-correctness invariant)
----------------------------------------------------------------

The grain is **(invoice's vendor_id, approval_status)**. We use a single
LEFT JOIN from `bronze.ap_invoices` → `silver.dim_supplier`:

* The invoice side is *always preserved* — every invoice dollar appears in
  exactly one output row, regardless of whether its vendor is in the dim.
  An INNER JOIN would silently drop invoices for vendors missing from the
  dim, which would understate financial spend. Not acceptable for a mart
  consumed by CFO dashboards.

* `vendor_id` in the output is `CAST(inv.ApInvoicesVendorId AS BIGINT)` —
  the invoice's claim of who got paid. `dim_supplier.vendor_id` is only
  used as the join condition; the dim's identity isn't authoritative for
  the gold mart's grain.

* `supplier_number`, `supplier_name`, `business_relationship` are pulled
  from the dim where the join matches; NULL otherwise. Demo pods like
  eseb-test (where every dim row has NULL `vendor_id`) naturally produce
  all-NULL dim attributes — same effective output as a "spend-only"
  fallback, but without a separate code path.

Earlier versions of this module had two SQL forms (INNER JOIN + spend-only)
picked by `id_populated_pct(vendor_id) >= 0.5`. That picker measured the
wrong thing — dim's internal completeness rather than invoice→dim join
coverage. The unified LEFT JOIN avoids the question entirely. The
`id_populated_pct` helper (still in `dim_supplier`) remains useful as a
diagnostic but is no longer load-bearing for path selection.

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


def build_supplier_spend_sql(
    *,
    bronze_invoices: str = SOURCE_BRONZE_TABLE,
    silver_dim:      str = SOURCE_SILVER_DIM,
    gold_table:      str = TARGET_GOLD_TABLE,
) -> str:
    """Return the CREATE-OR-REPLACE Delta SQL for ``gold.supplier_spend``.

    Single LEFT-JOIN form — see module docstring for the financial-correctness
    rationale. The invoice is always the preserved (left) side; every invoice
    dollar appears in the output regardless of whether its vendor matches the
    dim. Dim attributes (`supplier_number`, `supplier_name`,
    `business_relationship`) are NULL when no match exists.
    """
    return f"""\
CREATE OR REPLACE TABLE {gold_table}
USING DELTA
AS
SELECT
  CAST(inv.ApInvoicesVendorId AS BIGINT)                           AS vendor_id,
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
LEFT JOIN {silver_dim} ds
  ON ds.vendor_id = CAST(inv.ApInvoicesVendorId AS BIGINT)
WHERE inv.ApInvoicesVendorId IS NOT NULL
GROUP BY
  CAST(inv.ApInvoicesVendorId AS BIGINT),
  ds.supplier_number,
  ds.supplier_name,
  ds.business_relationship,
  inv.ApInvoicesApprovalStatus
"""


def build(
    spark: SparkSession,
    *,
    bronze_invoices: str = SOURCE_BRONZE_TABLE,
    silver_dim:      str = SOURCE_SILVER_DIM,
    gold_table:      str = TARGET_GOLD_TABLE,
) -> DataFrame:
    """Materialize ``gold.supplier_spend``; returns a DataFrame backed by it.

    The single LEFT-JOIN form preserves every invoice — no path-selection
    logic. The dim is consulted opportunistically for attributes; missing
    matches yield NULL dim columns rather than dropping invoices.
    """
    sql = build_supplier_spend_sql(
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
    "build",
    "build_supplier_spend_sql",
]
