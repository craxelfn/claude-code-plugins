WITH invoices AS (
  SELECT
    CAST(inv.ApInvoicesVendorId AS BIGINT)                             AS vendor_id,
    UPPER(CAST(inv.{{ column.invoice_currency_code }} AS STRING))      AS currency_code,
    inv.ApInvoicesApprovalStatus,
    inv.ApInvoicesInvoiceAmount,
    inv._extract_ts                                                    AS bronze_extract_ts
  FROM {{ catalog }}.{{ bronze_schema }}.ap_invoices inv
  WHERE inv.ApInvoicesVendorId IS NOT NULL
    AND {{ watermark_predicate }}
)
SELECT
  ds.supplier_number                                                   AS supplier_number,
  ds.supplier_name                                                     AS supplier_name,
  inv.vendor_id                                                        AS vendor_id,
  ds.business_relationship                                             AS business_relationship,
  inv.currency_code                                                    AS currency_code,
  COALESCE(inv.ApInvoicesApprovalStatus, 'UNKNOWN')                    AS approval_status,
  ROUND(SUM(COALESCE(CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 8)), 0)), 8)
                                                                       AS total_invoice_amount,
  current_timestamp()                                                  AS gold_built_at,
  {{ run_id_literal }}                                                 AS gold_run_id
FROM invoices inv
LEFT JOIN {{ catalog }}.{{ silver_schema }}.dim_supplier ds
  ON ds.vendor_id = inv.vendor_id
GROUP BY
  ds.supplier_number,
  ds.supplier_name,
  inv.vendor_id,
  ds.business_relationship,
  inv.currency_code,
  COALESCE(inv.ApInvoicesApprovalStatus, 'UNKNOWN')
