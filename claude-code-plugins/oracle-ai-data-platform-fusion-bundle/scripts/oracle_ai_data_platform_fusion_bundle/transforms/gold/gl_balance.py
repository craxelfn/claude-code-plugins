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
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Final, Literal

from oracle_ai_data_platform_fusion_bundle.config.paths import DEFAULT_PATHS, TablePaths

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


SOURCE_BRONZE_TABLE: Final[str] = DEFAULT_PATHS.bronze("gl_period_balances")
SOURCE_SILVER_DIM:   Final[str] = DEFAULT_PATHS.silver("dim_account")
TARGET_GOLD_TABLE:   Final[str] = DEFAULT_PATHS.gold("gl_balance")

DEFAULT_ACTUAL_FLAG_FILTER: Final[str] = "A"

#: Strict SQL-identifier pattern — same shape ``dim_account`` uses for its
#: own semantic-alias validation. Configured output column names must match
#: this so a misconfigured tenant config can't produce malformed SQL.
_SQL_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: Composite gold-side natural key for the incremental MERGE ON predicate
#: (P1.17). Matches ``GoldMartSpec.natural_key`` in the registry. The
#: 7-column tuple follows the verified grain in the module docstring
#: (lines 4-5) — TC23 cross-confirmed.
NATURAL_KEY_COLUMNS: Final[tuple[str, ...]] = (
    "ledger_id",
    "account_id",
    "period_year",
    "period_num",
    "currency_code",
    "actual_flag",
    "translated_flag",
)

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


def _run_id_audit_sql(run_id: str | None) -> str:
    """SQL fragment for the gold_run_id audit column (§3.5a, B3)."""
    if run_id is None:
        return "NULL"
    escaped = run_id.replace("'", "''")
    return f"'{escaped}'"


def _balance_select_sql(
    bronze_balances: str,
    silver_dim: str,
    where_clauses: str,
    segment_select_block: str,
    run_id_sql: str,
) -> str:
    """The SELECT projection shared by seed + incremental SQL renderers.

    Adds the P1.17 ``bronze_extract_ts`` column (row-level passthrough
    from ``b._extract_ts`` — gl_balance is row-level, not aggregate, so
    no ``MAX()`` aggregation is needed). Used by the orchestrator's
    post-build ``MAX(bronze_extract_ts)`` capture (B8) to feed the next
    incremental run's layer-local cursor (B8a).
    """
    return f"""\
WITH balances AS (
  SELECT
    b.BalanceLedgerId,
    b.BalanceCodeCombinationId,
    b.BalancePeriodYear,
    b.BalancePeriodNum,
    b.BalancePeriodName,
    b.BalanceCurrencyCode,
    b.BalanceActualFlag,
    b.BalanceTranslatedFlag,
    b._extract_ts                                                  AS bronze_extract_ts,
    CAST(b.BalanceBeginBalanceDr AS DECIMAL(28, 2))                AS begin_balance_dr,
    CAST(b.BalanceBeginBalanceCr AS DECIMAL(28, 2))                AS begin_balance_cr,
    CAST(b.BalancePeriodNetDr    AS DECIMAL(28, 2))                AS period_net_dr,
    CAST(b.BalancePeriodNetCr    AS DECIMAL(28, 2))                AS period_net_cr
  FROM {bronze_balances} b
  {where_clauses}
)
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
  b.begin_balance_dr                                               AS begin_balance_dr,
  b.begin_balance_cr                                               AS begin_balance_cr,
  b.period_net_dr                                                  AS period_net_dr,
  b.period_net_cr                                                  AS period_net_cr,
  ROUND(
      COALESCE(b.begin_balance_dr, 0)
    - COALESCE(b.begin_balance_cr, 0)
    + COALESCE(b.period_net_dr,    0)
    - COALESCE(b.period_net_cr,    0),
    2
  )                                                                AS closing_balance,
  b.bronze_extract_ts                                              AS bronze_extract_ts,
  current_timestamp()                                              AS gold_built_at,
  {run_id_sql}                                                     AS gold_run_id
FROM balances b
LEFT JOIN {silver_dim}  da
  ON da.account_id = CAST(b.BalanceCodeCombinationId AS BIGINT)"""


def build_gl_balance_sql(
    *,
    paths:           TablePaths | None = None,
    bronze_balances: str | None = None,
    silver_dim:      str | None = None,
    gold_table:      str | None = None,
    actual_flag_filter: str | None = DEFAULT_ACTUAL_FLAG_FILTER,
    coa_segment_map: Mapping[int, str] | None = None,
    run_id:          str | None = None,
    refresh_mode: Literal["seed", "incremental"] = "seed",
    watermark:    datetime | None = None,
    target_only_columns: tuple[str, ...] = (),
    source_columns: tuple[str, ...] | None = None,
) -> str:
    """Return the SQL that produces ``gold.gl_balance`` in the requested mode.

    Pure string output — no Spark required. Used by unit tests to verify the
    projection shape; called by :func:`build` to materialize the table.

    ``refresh_mode`` (P1.17):
      * ``"seed"`` (default) — ``CREATE OR REPLACE TABLE`` Delta SQL,
        identical pre-P1.17 shape **except** for the new
        ``bronze_extract_ts`` column carried row-level from
        ``b._extract_ts`` (B3 lineage rollout).
      * ``"incremental"`` — ``MERGE INTO ... ON (7-column composite,
        NULL-safe) WHEN MATCHED UPDATE SET * WHEN NOT MATCHED INSERT *``.
        The source-side ``AND b._extract_ts > <watermark>`` predicate
        filters bronze rows the gold layer has already incorporated
        (layer-local cursor per B8a). NULL-safe ``<=>`` on the join
        predicate is required because ``BalanceTranslatedFlag`` is NULL
        on saasfademo1 (LIMITS.md P1.17-L8).

    ``watermark`` (P1.17 incremental only): UTC ``datetime`` of the
    layer-local prior cursor.

    Other knobs (``actual_flag_filter``, ``coa_segment_map``) unchanged
    from pre-P1.17.
    """
    if paths is None:
        paths = DEFAULT_PATHS
    if bronze_balances is None:
        bronze_balances = paths.bronze("gl_period_balances")
    if silver_dim is None:
        silver_dim = paths.silver("dim_account")
    if gold_table is None:
        gold_table = paths.gold("gl_balance")

    if actual_flag_filter is None:
        base_where = "WHERE b.BalanceCodeCombinationId IS NOT NULL"
    else:
        if actual_flag_filter not in {"A", "E", "B"}:
            raise ValueError(
                "actual_flag_filter must be one of 'A' (actuals), 'E' "
                "(encumbrance), 'B' (budget), or None to disable; "
                f"got {actual_flag_filter!r}"
            )
        base_where = (
            f"WHERE b.BalanceActualFlag = '{actual_flag_filter}'\n"
            "  AND b.BalanceCodeCombinationId IS NOT NULL"
        )

    if coa_segment_map is None:
        coa_segment_map = DEFAULT_COA_SEGMENT_MAP
    _validate_coa_segment_map(coa_segment_map)
    segment_select_block = _segment_select_lines(coa_segment_map)
    segment_select_block = f"{segment_select_block}\n" if segment_select_block else ""
    run_id_sql = _run_id_audit_sql(run_id)

    if refresh_mode == "seed":
        select_sql = _balance_select_sql(
            bronze_balances, silver_dim, base_where, segment_select_block, run_id_sql,
        )
        return f"CREATE OR REPLACE TABLE {gold_table}\nUSING DELTA\nAS\n{select_sql}\n"

    if refresh_mode == "incremental":
        if watermark is None:
            raise ValueError(
                "gl_balance.build_gl_balance_sql: refresh_mode='incremental' "
                "requires a non-None watermark (the layer-local prior cursor). "
                "The orchestrator's _preflight_incremental_cursors should have "
                "raised IncrementalCursorMissingError before reaching this path."
            )
        watermark_iso = watermark.astimezone(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
        # Append the layer-local watermark predicate to the existing WHERE clause.
        inc_where = f"{base_where}\n  AND b._extract_ts > '{watermark_iso}'"
        select_sql = _balance_select_sql(
            bronze_balances, silver_dim, inc_where, segment_select_block, run_id_sql,
        )
        on_predicate = " AND ".join(
            f"target.{c} <=> src.{c}" for c in NATURAL_KEY_COLUMNS
        )
        # P1.17d — render explicit-column-list MERGE when target has
        # columns the source lacks; preserves target-only columns by
        # omitting them from UPDATE/INSERT lists. Otherwise V1 shape.
        if target_only_columns:
            if source_columns is None:
                raise ValueError(
                    "build_gl_balance_sql: when target_only_columns is "
                    "non-empty (explicit-column-list MERGE shape), the "
                    "source_columns kwarg MUST be provided. The "
                    "orchestrator's build() supplies this from "
                    "spark.sql(<select>).schema."
                )
            from oracle_ai_data_platform_fusion_bundle.orchestrator.merge_sql import (
                build_explicit_when_matched_clause,
                build_explicit_when_not_matched_clause,
            )
            when_matched_clause = build_explicit_when_matched_clause(source_columns)
            when_not_matched_clause = build_explicit_when_not_matched_clause(source_columns)
        else:
            when_matched_clause = "WHEN MATCHED THEN UPDATE SET *"
            when_not_matched_clause = "WHEN NOT MATCHED THEN INSERT *"
        return (
            f"MERGE INTO {gold_table} AS target\n"
            f"USING (\n{select_sql}\n) AS src\n"
            f"ON {on_predicate}\n"
            f"{when_matched_clause}\n"
            f"{when_not_matched_clause}\n"
        )

    raise ValueError(
        f"gl_balance.build_gl_balance_sql: refresh_mode must be 'seed' or "
        f"'incremental'; got {refresh_mode!r}"
    )


def build(
    spark: SparkSession,
    *,
    paths:           TablePaths | None = None,
    bronze_balances: str | None = None,
    silver_dim:      str | None = None,
    gold_table:      str | None = None,
    actual_flag_filter: str | None = DEFAULT_ACTUAL_FLAG_FILTER,
    coa_segment_map: Mapping[int, str] | None = None,
    run_id:          str | None = None,
    refresh_mode: Literal["seed", "incremental"] = "seed",
    watermark:    datetime | None = None,
) -> DataFrame:
    """Materialize ``gold.gl_balance``; returns a DataFrame backed by it.

    Runs the SQL from :func:`build_gl_balance_sql` and returns the freshly-
    written gold table. ``actual_flag_filter`` (default ``'A'``) and
    ``coa_segment_map`` are forwarded to the SQL builder.

    ``refresh_mode`` (P1.17) selects ``"seed"`` (``CREATE OR REPLACE``) or
    ``"incremental"`` (``MERGE INTO`` row-level with 7-column composite
    key). Incremental requires ``watermark`` (the layer-local prior
    cursor).

    ``paths`` (defaults to ``DEFAULT_PATHS``) resolves the bronze/silver/gold
    table identifiers from the tenant's ``bundle.yaml.aidp.*`` config.
    """
    if paths is None:
        paths = DEFAULT_PATHS
    if bronze_balances is None:
        bronze_balances = paths.bronze("gl_period_balances")
    if silver_dim is None:
        silver_dim = paths.silver("dim_account")
    if gold_table is None:
        gold_table = paths.gold("gl_balance")

    # P1.17d — under incremental, reconcile target schema with source
    # projection BEFORE the MERGE. gl_balance's SELECT joins bronze ×
    # silver — the source's column list is the projection's union of
    # both layers' columns + the segment-select-block expansions.
    # spark.sql(<select>).schema resolves the full projection in one
    # planner call.
    target_only_columns: tuple[str, ...] = ()
    source_columns: tuple[str, ...] | None = None
    if refresh_mode == "incremental":
        from oracle_ai_data_platform_fusion_bundle.orchestrator import state as _state

        if actual_flag_filter is None:
            _base_where = "WHERE b.BalanceCodeCombinationId IS NOT NULL"
        else:
            _base_where = (
                f"WHERE b.BalanceActualFlag = '{actual_flag_filter}'\n"
                "  AND b.BalanceCodeCombinationId IS NOT NULL"
            )
        _coa = coa_segment_map if coa_segment_map is not None else DEFAULT_COA_SEGMENT_MAP
        _segment_select_block = _segment_select_lines(_coa)
        _segment_select_block = (
            f"{_segment_select_block}\n" if _segment_select_block else ""
        )
        _run_id_sql = _run_id_audit_sql(run_id)
        inner_select = _balance_select_sql(
            bronze_balances, silver_dim, _base_where,
            _segment_select_block, _run_id_sql,
        )
        source_schema = spark.sql(
            f"SELECT * FROM ({inner_select}) WHERE 1=0"
        ).schema
        reconcile = _state._ensure_target_schema_for_merge(
            spark, gold_table, source_schema.names, source_schema,
        )
        if reconcile.target_only_columns:
            target_only_columns = reconcile.target_only_columns
            source_columns = tuple(source_schema.names)

    sql = build_gl_balance_sql(
        bronze_balances=bronze_balances,
        silver_dim=silver_dim,
        gold_table=gold_table,
        actual_flag_filter=actual_flag_filter,
        coa_segment_map=coa_segment_map,
        run_id=run_id,
        refresh_mode=refresh_mode,
        watermark=watermark,
        target_only_columns=target_only_columns,
        source_columns=source_columns,
    )
    spark.sql(sql)
    return spark.table(gold_table)


__all__ = [
    "DEFAULT_ACTUAL_FLAG_FILTER",
    "DEFAULT_COA_SEGMENT_MAP",
    "NATURAL_KEY_COLUMNS",
    "SOURCE_BRONZE_TABLE",
    "SOURCE_SILVER_DIM",
    "TARGET_GOLD_TABLE",
    "build",
    "build_gl_balance_sql",
]
