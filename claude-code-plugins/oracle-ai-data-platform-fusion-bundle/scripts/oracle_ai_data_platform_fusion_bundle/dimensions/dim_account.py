"""silver.dim_account — Chart of Accounts conformed dimension.

Reads ``bronze.gl_coa`` (BICC ``CodeCombinationExtractPVO``), dedupes on the
natural key ``CodeCombinationCodeCombinationId`` (CCID), keeps the most
recent extract per CCID, projects 6 segments (the common case) plus key
flags / dates / classification into ``silver.dim_account`` with bronze→silver
audit lineage.

Design notes
------------

* **Column convention** — bronze columns from the ``aidataplatform`` connector
  for ``CodeCombinationExtractPVO`` use **PascalCase with `CodeCombination`
  prefix** (e.g. ``CodeCombinationCodeCombinationId``,
  ``CodeCombinationSegment1``, ``CodeCombinationEnabledFlag``). This matches
  ``ap_invoices`` (which uses ``ApInvoices`` prefix) and differs from
  ``erp_suppliers`` (which uses bare UPPERCASE). Confirmed live on
  ``fusion_bundle_dev`` (2026-05-07, 64 columns / 63,464 rows).

* **Segment projection** — Fusion's CoA has up to **30 segments**
  (``CodeCombinationSegment1`` … ``CodeCombinationSegment30``). Most tenants
  use ≤ 6; this module ships 6 named columns (``company``, ``cost_center``,
  ``account``, ``subaccount``, ``product``, ``intercompany``) by default.
  The "hook for custom COA segments" per BACKLOG P1.3 is the named-column
  projection — if a tenant needs segments 7-30, extend the SQL builder
  non-breakingly. For demo / most production pods, 6 is enough.

* **`account_id` is the natural BIGINT join key** for downstream gold marts
  (especially ``gold.gl_balance``, P1.8). Cast from ``decimal(18,0)`` source.

* **Surrogate `account_key`** — ``monotonically_increasing_id()``, non-stable
  across rebuilds. Downstream marts MUST join on ``account_id``, never on the
  surrogate. Same contract as ``dim_supplier``'s ``supplier_key``.

* **Empty-CoA edge case** — if ``bronze.gl_coa`` has 0 rows, the SQL still
  runs cleanly and produces ``silver.dim_account`` with 0 rows. Required by
  BACKLOG P1.3 acceptance.

* **Filter philosophy** — the dim does **not** filter to enabled / postable
  accounts; consumers (gold marts, GenAI prompts) decide what subset to
  query. The ``enabled_flag`` and ``summary_flag`` columns surface the data
  needed for those filters. Filtering in the dim would be a Fusion-side
  policy decision the bundle shouldn't make for customers.

* **No SCD Type-2** — fully rebuilt each load. Dim attributes get the latest
  snapshot. Future requirement to track history → ship a separate Type-2
  variant module non-breakingly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


SOURCE_BRONZE_TABLE: Final[str] = "fusion_catalog.bronze.gl_coa"
TARGET_SILVER_TABLE: Final[str] = "fusion_catalog.silver.dim_account"


def build_dim_account_sql(
    *,
    bronze_table: str = SOURCE_BRONZE_TABLE,
    silver_table: str = TARGET_SILVER_TABLE,
) -> str:
    """Return the CREATE-OR-REPLACE Delta SQL that produces ``silver.dim_account``.

    Pure string output — no Spark required. Used by unit tests to verify the
    projection shape; called by :func:`build` to materialize the table.

    The dedupe rule keeps the row with the most-recent ``_extract_ts`` per
    ``CodeCombinationCodeCombinationId``. Rows with NULL CCID are filtered
    (would never join anyway). 6 segments are projected as named columns;
    the underlying ``CodeCombinationSegment7`` … ``Segment30`` are still
    available in bronze for tenants that need them.
    """
    return f"""\
CREATE OR REPLACE TABLE {silver_table}
USING DELTA
AS
SELECT
  monotonically_increasing_id()                                    AS account_key,
  CAST(CodeCombinationCodeCombinationId AS BIGINT)                 AS account_id,
  CAST(CodeCombinationChartOfAccountsId AS BIGINT)                 AS chart_of_accounts_id,
  CONCAT_WS('.',
    CodeCombinationSegment1, CodeCombinationSegment2,
    CodeCombinationSegment3, CodeCombinationSegment4,
    CodeCombinationSegment5, CodeCombinationSegment6
  )                                                                AS code_combination,
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
  current_timestamp()                                              AS silver_built_at
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY CodeCombinationCodeCombinationId
      ORDER BY _extract_ts DESC
    ) AS _rn
  FROM {bronze_table}
  WHERE CodeCombinationCodeCombinationId IS NOT NULL
)
WHERE _rn = 1
"""


def build(
    spark: "SparkSession",
    *,
    bronze_table: str = SOURCE_BRONZE_TABLE,
    silver_table: str = TARGET_SILVER_TABLE,
) -> "DataFrame":
    """Materialize ``silver.dim_account`` from ``bronze.gl_coa``.

    Runs the SQL from :func:`build_dim_account_sql` against ``spark`` and
    returns a DataFrame backed by the freshly-written silver table.
    """
    spark.sql(build_dim_account_sql(bronze_table=bronze_table, silver_table=silver_table))
    return spark.table(silver_table)


__all__ = [
    "SOURCE_BRONZE_TABLE",
    "TARGET_SILVER_TABLE",
    "build",
    "build_dim_account_sql",
]
