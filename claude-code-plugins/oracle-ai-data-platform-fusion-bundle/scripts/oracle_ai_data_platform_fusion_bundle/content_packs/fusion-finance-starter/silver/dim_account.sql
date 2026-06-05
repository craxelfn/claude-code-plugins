SELECT
  xxhash64(CAST(CodeCombinationCodeCombinationId AS STRING))       AS account_key,
  CAST(CodeCombinationCodeCombinationId AS BIGINT)                 AS account_id,
  CAST(CodeCombinationChartOfAccountsId AS BIGINT)                 AS chart_of_accounts_id,
  {{ column.coa_balancing_segment }}                               AS balancing_segment,
  {{ column.coa_cost_center_segment }}                             AS cost_center_segment,
  {{ column.coa_natural_account_segment }}                         AS natural_account_segment,
  CodeCombinationAccountType                                       AS account_type,
  CodeCombinationEnabledFlag                                       AS enabled_flag,
  CodeCombinationSummaryFlag                                       AS summary_flag,
  CodeCombinationDetailPostingAllowedFlag                          AS detail_posting_allowed_flag,
  CodeCombinationStartDateActive                                   AS start_date_active,
  CodeCombinationEndDateActive                                     AS end_date_active,
  _extract_ts                                                      AS bronze_extract_ts,
  _source_pvo                                                      AS bronze_source_pvo,
  current_timestamp()                                              AS silver_built_at,
  {{ run_id_literal }}                                             AS silver_run_id
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY CodeCombinationCodeCombinationId
      ORDER BY _extract_ts DESC
    ) AS _rn
  FROM {{ catalog }}.{{ bronze_schema }}.gl_coa
  WHERE CodeCombinationCodeCombinationId IS NOT NULL
    AND {{ watermark_predicate }}
)
WHERE _rn = 1
