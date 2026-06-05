WITH balances AS (
  SELECT
    CAST(b.BalanceLedgerId            AS BIGINT)                       AS ledger_id,
    CAST(b.BalanceCodeCombinationId   AS BIGINT)                       AS account_id,
    CAST(b.BalancePeriodYear          AS INT)                          AS period_year,
    CAST(b.BalancePeriodNum           AS INT)                          AS period_num,
    UPPER(CAST(b.BalanceCurrencyCode  AS STRING))                      AS currency_code,
    b.BalanceActualFlag                                                AS actual_flag,
    COALESCE(b.BalanceTranslatedFlag, 'N')                             AS translated_flag,
    CAST(b.BalanceBeginBalanceDr AS DECIMAL(28, 8))                    AS begin_balance_dr,
    CAST(b.BalanceBeginBalanceCr AS DECIMAL(28, 8))                    AS begin_balance_cr,
    CAST(b.BalancePeriodNetDr    AS DECIMAL(28, 8))                    AS period_net_dr,
    CAST(b.BalancePeriodNetCr    AS DECIMAL(28, 8))                    AS period_net_cr
  FROM {{ catalog }}.{{ bronze_schema }}.gl_period_balances b
  WHERE b.BalanceCodeCombinationId IS NOT NULL
    AND {{ watermark_predicate }}
)
SELECT
  b.ledger_id                                                          AS ledger_id,
  b.account_id                                                         AS account_id,
  b.period_year                                                        AS period_year,
  b.period_num                                                         AS period_num,
  b.currency_code                                                      AS currency_code,
  b.actual_flag                                                        AS actual_flag,
  b.translated_flag                                                    AS translated_flag,
  ROUND(
      COALESCE(b.begin_balance_dr, 0)
    - COALESCE(b.begin_balance_cr, 0)
    + COALESCE(b.period_net_dr,    0)
    - COALESCE(b.period_net_cr,    0),
    8
  )                                                                    AS ending_balance,
  current_timestamp()                                                  AS gold_built_at,
  {{ run_id_literal }}                                                 AS gold_run_id
FROM balances b
