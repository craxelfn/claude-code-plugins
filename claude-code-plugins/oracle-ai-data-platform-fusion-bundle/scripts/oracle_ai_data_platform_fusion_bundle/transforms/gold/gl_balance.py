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

* `BalanceActualFlag` filter — defaults to ``'A'`` (actuals only), which is the
  canonical CFO-dashboard view. Configurable via ``actual_flag_filter`` so
  tenants whose dashboards need encumbrance (``'E'``) or budget (``'B'``)
  balances can override at build time, and ``None`` disables the filter
  entirely (surface every flag and let the consumer slice). Confirmed live
  on saasfademo1: 10.18M actuals + 1.03M encumbrances (~91 % retained at
  default). Without this knob the mart would silently drop encumbrance-heavy
  tenants' balances — a plugin-portability gap; per round-6 review.
* `BalanceCodeCombinationId IS NOT NULL` — null-CCID rows can't join the dim
  and aren't meaningful balance rows; mirrors ``supplier_spend``'s null-vendor
  filter.
* No `BalanceTranslatedFlag` filter — surfaced as a column so consumers can
  pick reporting vs entered currency. Most tenants run a single reporting
  currency and the flag is NULL or `'N'`; multi-currency aggregation rules
  belong to consumers, not the mart.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


SOURCE_BRONZE_TABLE: Final[str] = "fusion_catalog.bronze.gl_period_balances"
SOURCE_SILVER_DIM:   Final[str] = "fusion_catalog.silver.dim_account"
TARGET_GOLD_TABLE:   Final[str] = "fusion_catalog.gold.gl_balance"

DEFAULT_ACTUAL_FLAG_FILTER: Final[str] = "A"

#: Strict SQL-identifier pattern — same shape ``dim_account`` uses for its
#: own semantic-alias validation. Configured output column names must match
#: this so a misconfigured tenant config can't produce malformed SQL.
_SQL_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: Default mapping of COA segment position (1-based) → ``gl_balance`` output
#: column name. Matches the Fusion-conventional six-segment ordering so
#: tenants whose COA follows that layout (the saasfademo1 demo pod and the
#: majority of production tenants) reproduce the pre-refactor column shape
#: exactly. Tenants whose COA puts natural account at, say, segment 5 pass
#: a custom map (``{1: "company", 5: "natural_account", ...}``) so the
#: output column name matches its data. Pass ``{}`` to suppress segment
#: columns entirely (consumers read ``segment_NN`` from the dim directly).
DEFAULT_COA_SEGMENT_MAP: Final[Mapping[int, str]] = {
    1: "company",
    2: "cost_center",
    3: "natural_account",
    4: "subaccount",
    5: "product",
    6: "intercompany",
}

#: Maximum segment position ``dim_account`` can emit (Fusion's COA cap).
_MAX_COA_SEGMENT: Final[int] = 30


def _validate_coa_segment_map(coa_segment_map: Mapping[int, str]) -> None:
    for pos, alias in coa_segment_map.items():
        if not 1 <= pos <= _MAX_COA_SEGMENT:
            raise ValueError(
                f"coa_segment_map position {pos!r} (alias {alias!r}) is "
                f"out of range [1, {_MAX_COA_SEGMENT}]"
            )
        if not isinstance(alias, str) or not _SQL_IDENTIFIER_RE.match(alias):
            raise ValueError(
                f"coa_segment_map alias {alias!r} (position {pos}) is not "
                "a valid SQL identifier — must match ^[A-Za-z_][A-Za-z0-9_]*$"
            )
    seen: set[str] = set()
    for alias in coa_segment_map.values():
        if alias in seen:
            raise ValueError(
                f"coa_segment_map alias {alias!r} is duplicated — each "
                "alias must be unique"
            )
        seen.add(alias)


def _segment_select_lines(coa_segment_map: Mapping[int, str]) -> str:
    """Emit ``da.segment_NN AS <alias>`` lines for the configured map.

    Reading positional ``segment_NN`` columns (always emitted by
    ``dim_account``) decouples gl_balance from the dim's optional
    semantic aliases, which are tenant-configurable and may not exist on
    non-conventional COA designs. Returns ``""`` when the map is empty
    (suppress segment columns entirely — consumers read positional
    columns from the dim directly).
    """
    if not coa_segment_map:
        return ""
    return "\n".join(
        f"  da.segment_{pos:02d}{' ' * max(1, 64 - len(f'da.segment_{pos:02d}'))}AS {alias},"
        for pos, alias in sorted(coa_segment_map.items())
    )


def build_gl_balance_sql(
    *,
    bronze_balances: str = SOURCE_BRONZE_TABLE,
    silver_dim:      str = SOURCE_SILVER_DIM,
    gold_table:      str = TARGET_GOLD_TABLE,
    actual_flag_filter: str | None = DEFAULT_ACTUAL_FLAG_FILTER,
    coa_segment_map: Mapping[int, str] | None = None,
) -> str:
    """Return the CREATE-OR-REPLACE Delta SQL for ``gold.gl_balance``.

    Pure string output — no Spark required. Used by unit tests to verify the
    projection shape; called by :func:`build` to materialize the table.

    Single LEFT JOIN to ``silver.dim_account`` (fact preserved); no
    ``dim_calendar`` join (grain mismatch — period vs day). Casts amount
    columns from source ``decimal(38,30)`` to ``DECIMAL(28,2)`` for output.

    ``actual_flag_filter`` controls the WHERE-clause filter on
    ``BalanceActualFlag``:

    * ``"A"`` (default) — actuals only; the canonical CFO-dashboard view.
    * ``"E"`` / ``"B"`` — encumbrance / budget only for tenants whose
      dashboards need those balance types.
    * ``None`` — disable the filter; surface all flags. Consumers slice
      on ``actual_flag`` themselves.

    ``coa_segment_map`` controls how dim_account's positional
    ``segment_NN`` columns are surfaced. Defaults to
    :data:`DEFAULT_COA_SEGMENT_MAP` (the Fusion-conventional six). Tenants
    whose COA puts natural account at a different segment override the
    map (``{1: "company", 5: "natural_account", ...}``); the output column
    names follow the map values. Pass ``{}`` to omit segment columns
    entirely — consumers can read ``da.segment_NN`` from ``silver.dim_account``
    if they want a tenant-agnostic shape.

    Plugin-portability: gl_balance used to read ``da.company``,
    ``da.cost_center``, etc. directly. Those aliases are now optional in
    dim_account (the dim's ``semantic_segment_map`` is tenant-configurable),
    so a non-conventional COA tenant could find them missing. Reading
    positional ``segment_NN`` instead decouples gl_balance from the dim's
    alias contract — positional columns are always emitted.
    """
    if actual_flag_filter is None:
        where_clauses = "WHERE b.BalanceCodeCombinationId IS NOT NULL"
    else:
        if actual_flag_filter not in {"A", "E", "B"}:
            raise ValueError(
                "actual_flag_filter must be one of 'A' (actuals), 'E' "
                "(encumbrance), 'B' (budget), or None to disable; "
                f"got {actual_flag_filter!r}"
            )
        where_clauses = (
            f"WHERE b.BalanceActualFlag = '{actual_flag_filter}'\n"
            "  AND b.BalanceCodeCombinationId IS NOT NULL"
        )

    if coa_segment_map is None:
        coa_segment_map = DEFAULT_COA_SEGMENT_MAP
    _validate_coa_segment_map(coa_segment_map)
    segment_select_block = _segment_select_lines(coa_segment_map)
    segment_select_block = f"{segment_select_block}\n" if segment_select_block else ""

    return f"""\
CREATE OR REPLACE TABLE {gold_table}
USING DELTA
AS
SELECT
  CAST(b.BalanceLedgerId            AS BIGINT)                     AS ledger_id,
  CAST(b.BalanceCodeCombinationId   AS BIGINT)                     AS account_id,
  da.code_combination                                              AS code_combination,
  da.account_type                                                  AS account_type,
{segment_select_block}  CAST(b.BalancePeriodYear          AS BIGINT)                     AS period_year,
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
{where_clauses}
"""


def build(
    spark: SparkSession,
    *,
    bronze_balances: str = SOURCE_BRONZE_TABLE,
    silver_dim:      str = SOURCE_SILVER_DIM,
    gold_table:      str = TARGET_GOLD_TABLE,
    actual_flag_filter: str | None = DEFAULT_ACTUAL_FLAG_FILTER,
    coa_segment_map: Mapping[int, str] | None = None,
) -> DataFrame:
    """Materialize ``gold.gl_balance``; returns a DataFrame backed by it.

    Runs the SQL from :func:`build_gl_balance_sql` and returns the freshly-
    written gold table. Idempotent — uses ``CREATE OR REPLACE`` so reruns
    produce the same shape. ``actual_flag_filter`` is forwarded to the SQL
    builder (default ``'A'`` for the canonical actuals view; pass ``None``
    to surface all flags). ``coa_segment_map`` controls which positional
    ``segment_NN`` columns from ``silver.dim_account`` are surfaced and
    under what names (default: Fusion-conventional six-segment ordering).
    """
    sql = build_gl_balance_sql(
        bronze_balances=bronze_balances,
        silver_dim=silver_dim,
        gold_table=gold_table,
        actual_flag_filter=actual_flag_filter,
        coa_segment_map=coa_segment_map,
    )
    spark.sql(sql)
    return spark.table(gold_table)


__all__ = [
    "DEFAULT_ACTUAL_FLAG_FILTER",
    "DEFAULT_COA_SEGMENT_MAP",
    "SOURCE_BRONZE_TABLE",
    "SOURCE_SILVER_DIM",
    "TARGET_GOLD_TABLE",
    "build",
    "build_gl_balance_sql",
]
