SELECT
  xxhash64(CAST(SEGMENT1 AS STRING))                               AS supplier_key,
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
  current_timestamp()                                              AS silver_built_at,
  {{ run_id_literal }}                                             AS silver_run_id
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY SEGMENT1 ORDER BY _extract_ts DESC) AS _rn
  FROM {{ catalog }}.{{ bronze_schema }}.erp_suppliers
  WHERE SEGMENT1 IS NOT NULL
    AND {{ watermark_predicate }}
)
WHERE _rn = 1
