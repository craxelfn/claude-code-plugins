"""gold.gl_balance — period balances by (account, period, ledger, currency).

Productizes the GL period-balance fact at grain
**(ledger_id, account_id, period_year, period_num, currency_code, actual_flag,
translated_flag)** for v0.2.0. Source is `bronze.gl_period_balances` from BICC
``BalanceExtractPVO``. Confirmed live on `fusion_bundle_dev` 2026-05-09 —
11,211,211 rows / 36 cols extracted clean (L2 BICC encoder bug did not fire on
this PVO; see ``LIMITS.md`` §L2).

Design — single LEFT-JOIN form (financial-correctness invariant)
----------------------------------------------------------------

The grain is sourced from the fact (one row per balance bucket). We LEFT JOIN
to ``silver.dim_account``:

* The fact side is *always preserved* — every balance row appears in exactly
  one output row regardless of whether its account is in the dim. INNER JOIN
  would silently drop balances for accounts missing from ``dim_account`` (e.g.
  recently-added segments, summary accounts, non-postable accounts that the
  dim chose to filter), which would understate or misreport the balance sheet.
  Same financial-correctness reasoning as ``supplier_spend``.

* ``account_id`` in the output is ``CAST(b.BalanceCodeCombinationId AS BIGINT)``
  — the fact's claim of which CCID this balance is for. ``dim_account``'s
  identity is not authoritative for the gold mart's grain; it provides
  attributes (account_type, segments, enabled flags) when matched, NULL when
  not.

No `dim_calendar` join — grain mismatch
---------------------------------------

`silver.dim_calendar` is **daily-grain** (one row per `calendar_date`). A
period-balance fact can't naturally join a daily-calendar dim without
introducing a derived "first-of-period" date and accepting period x day
blowup. Instead we surface ``BalancePeriodYear`` (BIGINT), ``BalancePeriodNum``
(BIGINT), and ``BalancePeriodName`` (string) directly from the fact. They
give callers the fiscal context (year/period number, label) without a join.
A period-grain calendar dim is out of scope for v0.2.0; revisit if a cross-
mart pattern needs one.

Note on ``BalancePeriodName``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The source column has a known data-quality issue — both `Sep-25` (mixed
case) and `SEP-26` (uppercase) variants appear, observed live on
`fusion_bundle_dev` 2026-05-09. We surface the raw value as `period_name`
without normalization (consumers can `UPPER()` if they need a clean filter).
The `(period_year, period_num)` pair is the unambiguous join key.

Closing balance formula
-----------------------

`closing_balance = COALESCE(begin_balance_dr, 0) - COALESCE(begin_balance_cr, 0)
+ COALESCE(period_net_dr, 0) - COALESCE(period_net_cr, 0)`

Fusion's GL stores debits and credits separately (`*Dr`/`*Cr`). The signed
closing is the standard accounting form. Account-type-dependent sign flipping
(asset/expense vs liability/equity/revenue) is *not* applied here — that's a
presentation concern best handled by the consumer (or by a future
`gl_balance_signed` mart) where account-type semantics are explicit.

The ``COALESCE(..., 0)`` wrappers inside the formula matter: Fusion legitimately
emits NULL for begin/period balance components on accounts that didn't exist in
a prior period, or for period halves with no posted activity. Without
``COALESCE``, a single NULL nullifies the entire ``closing_balance`` (NULL
propagation in arithmetic). Verified live on `fusion_bundle_dev` 2026-05-09 —
~20% of sample rows hit at least one NULL component. The individual
``begin_balance_dr`` / ``begin_balance_cr`` / ``period_net_dr`` /
``period_net_cr`` columns are surfaced **without** ``COALESCE`` so consumers
can still distinguish "no data" from "zero".

Bronze column convention
------------------------

`bronze.gl_period_balances` from the BICC ``aidataplatform`` connector for
``BalanceExtractPVO`` uses **PascalCase with `Balance` prefix**
(`BalanceCodeCombinationId`, `BalanceLedgerId`, `BalancePeriodNetDr`, etc.).
Confirmed live on `fusion_bundle_dev` 2026-05-09. Source amount columns are
`decimal(38,30)`; we cast to `DECIMAL(28,2)` for output (cents granularity is
the financial-reporting standard and saves storage; 30 fractional digits is
overkill for monetary data).

Filter philosophy
-----------------

* `BalanceActualFlag = 'A'` — actuals only for v0.2.0. Encumbrance (`'E'`) and
  budget (`'B'`) are deferred to v0.3 as additional `gl_*_balance` marts or as
  a non-breaking column expansion here. Confirmed live: 10.18M actuals + 1.03M
  encumbrances on this pod (~91 % retained).
* `BalanceCodeCombinationId IS NOT NULL` — null-CCID rows can't join the dim
  and aren't meaningful balance rows; mirrors ``supplier_spend``'s null-vendor
  filter.
* No `BalanceTranslatedFlag` filter — surfaced as a column so consumers can
  pick reporting vs entered currency. Most tenants run a single reporting
  currency and the flag is NULL or `'N'`; multi-currency aggregation rules
  belong to consumers, not the mart.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


SOURCE_BRONZE_TABLE: Final[str] = "fusion_catalog.bronze.gl_period_balances"
SOURCE_SILVER_DIM:   Final[str] = "fusion_catalog.silver.dim_account"
TARGET_GOLD_TABLE:   Final[str] = "fusion_catalog.gold.gl_balance"


def build_gl_balance_sql(
    *,
    bronze_balances: str = SOURCE_BRONZE_TABLE,
    silver_dim:      str = SOURCE_SILVER_DIM,
    gold_table:      str = TARGET_GOLD_TABLE,
) -> str:
    """Return the CREATE-OR-REPLACE Delta SQL for ``gold.gl_balance``.

    Pure string output — no Spark required. Used by unit tests to verify the
    projection shape; called by :func:`build` to materialize the table.

    Single LEFT JOIN to ``silver.dim_account`` (fact preserved); no
    ``dim_calendar`` join (grain mismatch — period vs day). Filters to
    ``BalanceActualFlag = 'A'`` and non-null CCID. Casts amount columns from
    source ``decimal(38,30)`` to ``DECIMAL(28,2)`` for output.
    """
    return f"""\
CREATE OR REPLACE TABLE {gold_table}
USING DELTA
AS
SELECT
  CAST(b.BalanceLedgerId            AS BIGINT)                     AS ledger_id,
  CAST(b.BalanceCodeCombinationId   AS BIGINT)                     AS account_id,
  da.code_combination                                              AS code_combination,
  da.account_type                                                  AS account_type,
  da.company                                                       AS company,
  da.cost_center                                                   AS cost_center,
  da.account                                                       AS natural_account,
  da.subaccount                                                    AS subaccount,
  da.product                                                       AS product,
  da.intercompany                                                  AS intercompany,
  CAST(b.BalancePeriodYear          AS BIGINT)                     AS period_year,
  CAST(b.BalancePeriodNum           AS BIGINT)                     AS period_num,
  b.BalancePeriodName                                              AS period_name,
  b.BalanceCurrencyCode                                            AS currency_code,
  b.BalanceActualFlag                                              AS actual_flag,
  b.BalanceTranslatedFlag                                          AS translated_flag,
  CAST(b.BalanceBeginBalanceDr      AS DECIMAL(28, 2))             AS begin_balance_dr,
  CAST(b.BalanceBeginBalanceCr      AS DECIMAL(28, 2))             AS begin_balance_cr,
  CAST(b.BalancePeriodNetDr         AS DECIMAL(28, 2))             AS period_net_dr,
  CAST(b.BalancePeriodNetCr         AS DECIMAL(28, 2))             AS period_net_cr,
  ROUND(
      COALESCE(CAST(b.BalanceBeginBalanceDr AS DECIMAL(28, 2)), 0)
    - COALESCE(CAST(b.BalanceBeginBalanceCr AS DECIMAL(28, 2)), 0)
    + COALESCE(CAST(b.BalancePeriodNetDr    AS DECIMAL(28, 2)), 0)
    - COALESCE(CAST(b.BalancePeriodNetCr    AS DECIMAL(28, 2)), 0),
    2
  )                                                                AS closing_balance,
  current_timestamp()                                              AS gold_built_at
FROM {bronze_balances} b
LEFT JOIN {silver_dim}  da
  ON da.account_id = CAST(b.BalanceCodeCombinationId AS BIGINT)
WHERE b.BalanceActualFlag = 'A'
  AND b.BalanceCodeCombinationId IS NOT NULL
"""


def build(
    spark: SparkSession,
    *,
    bronze_balances: str = SOURCE_BRONZE_TABLE,
    silver_dim:      str = SOURCE_SILVER_DIM,
    gold_table:      str = TARGET_GOLD_TABLE,
) -> DataFrame:
    """Materialize ``gold.gl_balance``; returns a DataFrame backed by it.

    Runs the SQL from :func:`build_gl_balance_sql` and returns the freshly-
    written gold table. Idempotent — uses ``CREATE OR REPLACE`` so reruns
    produce the same shape.
    """
    sql = build_gl_balance_sql(
        bronze_balances=bronze_balances,
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
    "build_gl_balance_sql",
]
