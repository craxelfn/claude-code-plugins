WITH invoices AS (
  SELECT
    CAST(inv.ApInvoicesVendorId AS BIGINT)                          AS vendor_id,
    UPPER(CAST(inv.{{ column.invoice_currency_code }} AS STRING))   AS currency_code,
    inv.ApInvoicesApprovalStatus,
    inv.ApInvoicesInvoiceAmount,
    inv.ApInvoicesAmountPaid,
    CAST(inv.ApInvoicesInvoiceDate AS DATE)                         AS invoice_date,
    inv._extract_ts                                                 AS bronze_extract_ts
  FROM {{ catalog }}.{{ bronze_schema }}.ap_invoices inv
  WHERE inv.ApInvoicesVendorId IS NOT NULL
    AND {{ watermark_predicate }}
)
SELECT
  inv.vendor_id                                                     AS vendor_id,
  inv.currency_code                                                 AS currency_code,
  ds.supplier_number                                                AS supplier_number,
  ds.supplier_name                                                  AS supplier_name,
  ds.business_relationship                                          AS business_relationship,
  inv.ApInvoicesApprovalStatus                                      AS approval_status,
  COUNT(*)                                                          AS invoice_count,
  ROUND(SUM(COALESCE(CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 8)), 0)), 8)
                                                                    AS total_invoice_amount,
  ROUND(SUM(COALESCE(CAST(inv.ApInvoicesAmountPaid    AS DECIMAL(28, 8)), 0)), 8)
                                                                    AS total_paid,
  MAX(inv.invoice_date)                                             AS last_invoice_date,
  MAX(inv.bronze_extract_ts)                                        AS bronze_extract_ts,
  current_timestamp()                                               AS gold_built_at,
  {{ run_id_literal }}                                              AS gold_run_id
FROM invoices inv
LEFT JOIN {{ catalog }}.{{ silver_schema }}.dim_supplier ds
  ON ds.vendor_id = inv.vendor_id
GROUP BY
  inv.vendor_id,
  inv.currency_code,
  ds.supplier_number,
  ds.supplier_name,
  ds.business_relationship,
  inv.ApInvoicesApprovalStatus
