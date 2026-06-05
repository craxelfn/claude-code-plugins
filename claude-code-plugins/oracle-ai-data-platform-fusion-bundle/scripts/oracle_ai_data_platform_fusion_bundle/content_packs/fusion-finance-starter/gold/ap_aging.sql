WITH open_invoices AS (
  SELECT
    CAST(inv.ApInvoicesVendorId AS BIGINT)                          AS vendor_id,
    UPPER(CAST(inv.{{ column.invoice_currency_code }} AS STRING))   AS currency_code,
    CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 8))             AS invoice_amount,
    CAST(COALESCE(inv.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 8))   AS amount_paid,
    CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 8))
      - CAST(COALESCE(inv.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 8)) AS open_amount,
    CAST(inv.ApInvoicesInvoiceDate AS DATE)                         AS invoice_date,
    inv._extract_ts                                                 AS bronze_extract_ts
  FROM {{ catalog }}.{{ bronze_schema }}.ap_invoices inv
  WHERE inv.ApInvoicesVendorId IS NOT NULL
    AND inv.ApInvoicesInvoiceDate IS NOT NULL
    AND {{ semantic.cancelled_status }}
    AND CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 2))
      - CAST(COALESCE(inv.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 2)) <> 0
)
SELECT
  o.vendor_id                                                       AS vendor_id,
  o.currency_code                                                   AS currency_code,
  ds.supplier_number                                                AS supplier_number,
  ds.supplier_name                                                  AS supplier_name,
  ds.business_relationship                                          AS business_relationship,
  CASE
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 0   THEN 'NOT_DUE'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 30  THEN '0-30'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 60  THEN '31-60'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 90  THEN '61-90'
    ELSE 'OVER_90'
  END                                                               AS age_bucket,
  'invoice_date'                                                    AS bucket_basis,
  COUNT(*)                                                          AS open_invoice_count,
  ROUND(SUM(o.open_amount), 8)                                      AS open_amount,
  ROUND(SUM(o.invoice_amount), 8)                                   AS invoice_amount_total,
  ROUND(SUM(o.amount_paid), 8)                                      AS amount_paid_total,
  ROUND(SUM(CASE WHEN o.open_amount < 0 THEN o.open_amount ELSE 0 END), 8)
                                                                    AS credit_open_amount,
  SUM(CASE WHEN o.open_amount < 0 THEN 1 ELSE 0 END)                AS credit_open_count,
  MIN(o.invoice_date)                                               AS oldest_invoice_date,
  MAX(DATEDIFF({{ snapshot_date }}, o.invoice_date))                AS max_days_outstanding,
  CAST({{ snapshot_date }} AS DATE)                                 AS as_of_date,
  MAX(o.bronze_extract_ts)                                          AS bronze_extract_ts,
  current_timestamp()                                               AS gold_built_at,
  {{ run_id_literal }}                                              AS gold_run_id
FROM open_invoices o
LEFT JOIN {{ catalog }}.{{ silver_schema }}.dim_supplier ds
  ON ds.vendor_id = o.vendor_id
GROUP BY
  o.vendor_id,
  o.currency_code,
  ds.supplier_number,
  ds.supplier_name,
  ds.business_relationship,
  CASE
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 0   THEN 'NOT_DUE'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 30  THEN '0-30'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 60  THEN '31-60'
    WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 90  THEN '61-90'
    ELSE 'OVER_90'
  END
