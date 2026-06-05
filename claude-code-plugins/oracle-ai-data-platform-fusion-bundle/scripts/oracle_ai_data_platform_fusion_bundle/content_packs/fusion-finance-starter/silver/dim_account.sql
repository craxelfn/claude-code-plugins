SELECT
  xxhash64(CAST(CodeCombinationCodeCombinationId AS STRING))       AS account_key,
  CAST(CodeCombinationCodeCombinationId AS BIGINT)                 AS account_id,
  CAST(CodeCombinationChartOfAccountsId AS BIGINT)                 AS chart_of_accounts_id,
  CONCAT_WS('.',
    CodeCombinationSegment1, CodeCombinationSegment2,
    CodeCombinationSegment3, CodeCombinationSegment4,
    CodeCombinationSegment5, CodeCombinationSegment6
  )                                                                AS code_combination,
  CodeCombinationSegment1                                          AS segment_01,
  CodeCombinationSegment2                                          AS segment_02,
  CodeCombinationSegment3                                          AS segment_03,
  CodeCombinationSegment4                                          AS segment_04,
  CodeCombinationSegment5                                          AS segment_05,
  CodeCombinationSegment6                                          AS segment_06,
  CodeCombinationSegment1                                          AS company,
  CodeCombinationSegment2                                          AS cost_center,
  CodeCombinationSegment3                                          AS account,
  CodeCombinationSegment4                                          AS subaccount,
  CodeCombinationSegment5                                          AS product,
  CodeCombinationSegment6                                          AS intercompany,
  CodeCombinationAccountType                                       AS account_type,
  CodeCombinationEnabledFlag                                       AS enabled_flag,
  CodeCombinationSummaryFlag                                       AS summary_flag,
  CodeCombinationDetailPostingAllowedFlag                          AS detail_posting_allowed_flag,
  CodeCombinationFinancialCategory                                 AS financial_category,
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
