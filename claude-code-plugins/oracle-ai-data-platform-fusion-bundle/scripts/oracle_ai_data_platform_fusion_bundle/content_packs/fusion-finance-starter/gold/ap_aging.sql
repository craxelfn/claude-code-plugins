WITH open_invoices AS (
  SELECT
    CAST(inv.ApInvoicesVendorId AS BIGINT)                              AS vendor_id,
    UPPER(CAST(inv.{{ column.invoice_currency_code }} AS STRING))       AS currency_code,
    CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 8))                 AS invoice_amount,
    CAST(COALESCE(inv.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 8))       AS amount_paid,
    CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 8))
      - CAST(COALESCE(inv.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 8))   AS open_amount,
    CAST(inv.ApInvoicesInvoiceDate AS DATE)                             AS invoice_date
  FROM {{ catalog }}.{{ bronze_schema }}.ap_invoices inv
  WHERE inv.ApInvoicesVendorId IS NOT NULL
    AND inv.ApInvoicesInvoiceDate IS NOT NULL
    AND {{ semantic.cancelled_status }}
    AND CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 8))
      - CAST(COALESCE(inv.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 8)) <> 0
),
aged AS (
  SELECT
    o.vendor_id,
    o.currency_code,
    o.open_amount,
    CASE
      WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 0   THEN 'NOT_DUE'
      WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 30  THEN '0-30'
      WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 60  THEN '31-60'
      WHEN DATEDIFF({{ snapshot_date }}, o.invoice_date) <= 90  THEN '61-90'
      ELSE 'OVER_90'
    END                                                                 AS age_bucket
  FROM open_invoices o
)
SELECT
  ds.supplier_number                                                    AS supplier_number,
  ds.supplier_name                                                      AS supplier_name,
  a.vendor_id                                                           AS vendor_id,
  a.currency_code                                                       AS currency_code,
  a.age_bucket                                                          AS age_bucket,
  ROUND(SUM(COALESCE(a.open_amount, 0)), 8)                             AS open_amount,
  current_timestamp()                                                   AS gold_built_at,
  {{ run_id_literal }}                                                  AS gold_run_id
FROM aged a
LEFT JOIN {{ catalog }}.{{ silver_schema }}.dim_supplier ds
  ON ds.vendor_id = a.vendor_id
GROUP BY
  ds.supplier_number,
  ds.supplier_name,
  a.vendor_id,
  a.currency_code,
  a.age_bucket
