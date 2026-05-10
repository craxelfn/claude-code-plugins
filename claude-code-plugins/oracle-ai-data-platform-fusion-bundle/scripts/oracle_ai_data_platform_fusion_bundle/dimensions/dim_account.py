"""silver.dim_account — Chart of Accounts conformed dimension.

Reads ``bronze.gl_coa`` (BICC ``CodeCombinationExtractPVO``), dedupes on the
natural key ``CodeCombinationCodeCombinationId`` (CCID), keeps the most
recent extract per CCID, projects **all 30 Fusion COA segments** as
positional ``segment_NN`` columns plus a tenant-configurable set of
**semantic alias columns** (default: the Fusion-conventional six —
``company``, ``cost_center``, ``account``, ``subaccount``, ``product``,
``intercompany``), plus key flags / dates / classification into
``silver.dim_account`` with bronze→silver audit lineage.

Plugin-portable COA shape
-------------------------

Fusion COA configuration is tenant-specific:

* **Segment count varies** (4-30). Tenants with >6 segments populated
  needed an extended projection; tenants with <6 simply see NULL for
  unused positions. Earlier versions of this dim hardcoded the first 6
  by their conventional semantic names, which truncated tenants with
  more segments populated and mislabeled tenants whose segment ordering
  differed.
* **Segment ordering varies**. The Fusion-conventional ordering
  (company / cost_center / account / subaccount / product /
  intercompany at positions 1..6) is common but not universal —
  customers configure their chart of accounts.
* **Segment meaning varies**. A customer's segment 2 might be
  "department", "region", or "project" rather than "cost_center".

The module's contract here:

* ``segment_01 … segment_NN`` (default ``n_segments=30``) — emitted
  **positionally**. These are universal across all Fusion tenants — they
  are the raw, unaliased values from ``CodeCombinationSegment1..30`` on
  bronze. Downstream consumers that need to be tenant-agnostic should
  read these.
* ``code_combination`` — ``CONCAT_WS('.', segment_01, …, segment_NN)``.
  ``CONCAT_WS`` skips NULL inputs, so a tenant with 4 active segments
  produces a 4-part dotted key without trailing dots.
* **Semantic aliases** — emitted per ``semantic_segment_map``, which
  defaults to the Fusion-conventional six but can be overridden per
  tenant. Aliases are still backed by ``CodeCombinationSegmentN``, so
  shipping a custom mapping doesn't require a re-extract — only the
  dim rebuild.

Other design notes
------------------

* **Column convention** — bronze columns from the ``aidataplatform``
  connector for ``CodeCombinationExtractPVO`` use **PascalCase with
  ``CodeCombination`` prefix** (e.g. ``CodeCombinationCodeCombinationId``,
  ``CodeCombinationSegment1``, ``CodeCombinationEnabledFlag``). Confirmed
  live on ``fusion_bundle_dev`` (2026-05-07, 64 columns / 63,464 rows).

* **``account_id`` is the natural BIGINT join key** for downstream gold
  marts (especially ``gold.gl_balance``, P1.8). Cast from
  ``decimal(18,0)`` source.

* **Surrogate ``account_key``** — ``monotonically_increasing_id()``,
  non-stable across rebuilds. Downstream marts MUST join on
  ``account_id``, never on the surrogate. Same contract as
  ``dim_supplier``'s ``supplier_key``.

* **Empty-CoA edge case** — if ``bronze.gl_coa`` has 0 rows, the SQL
  still runs cleanly and produces ``silver.dim_account`` with 0 rows.

* **Filter philosophy** — the dim does **not** filter to enabled /
  postable accounts; consumers (gold marts, GenAI prompts) decide what
  subset to query. The ``enabled_flag`` and ``summary_flag`` columns
  surface the data needed for those filters. Filtering in the dim would
  be a Fusion-side policy decision the bundle shouldn't make for
  customers.

* **No SCD Type-2** — fully rebuilt each load. Dim attributes get the
  latest snapshot. Future requirement to track history → ship a
  separate Type-2 variant module non-breakingly.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


SOURCE_BRONZE_TABLE: Final[str] = "fusion_catalog.bronze.gl_coa"
TARGET_SILVER_TABLE: Final[str] = "fusion_catalog.silver.dim_account"

#: Fusion's COA supports up to 30 segments. Most tenants populate ≤6.
MAX_FUSION_SEGMENTS: Final[int] = 30

#: Strict SQL-identifier pattern — must start with letter or underscore,
#: only ASCII letters / digits / underscores after. Matches what unquoted
#: identifiers can be in Spark SQL without backticks. We use this for
#: configured semantic alias names so a misconfigured tenant config can't
#: produce malformed SQL or accidentally allow injection.
_SQL_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: Conventional Fusion segment ordering at the canonical positions 1..6.
#: This is the **default** semantic mapping — tenants whose COA differs can
#: pass a custom ``semantic_segment_map`` to override or extend. Position
#: numbers (1-based) match ``CodeCombinationSegmentN`` source columns.
DEFAULT_SEMANTIC_SEGMENT_MAP: Final[Mapping[int, str]] = {
    1: "company",
    2: "cost_center",
    3: "account",
    4: "subaccount",
    5: "product",
    6: "intercompany",
}


def _segment_positional_lines(n_segments: int) -> str:
    """SELECT-clause snippet for positional ``segment_NN`` columns."""
    return ",\n".join(
        f"  CodeCombinationSegment{i}{' ' * max(1, 39 - len(str(i)))}"
        f"AS segment_{i:02d}"
        for i in range(1, n_segments + 1)
    )


def _code_combination_concat(n_segments: int) -> str:
    """``CONCAT_WS('.', ...)`` over all positional segment columns.

    ``CONCAT_WS`` skips NULL inputs, so tenants with sparse segment
    population produce a clean dotted key with no trailing dots.
    """
    parts = ", ".join(f"CodeCombinationSegment{i}" for i in range(1, n_segments + 1))
    return f"CONCAT_WS('.',\n    {parts}\n  )"


def _semantic_alias_lines(
    semantic_map: Mapping[int, str],
    n_segments: int,
) -> str:
    """SELECT-clause snippet for semantic alias columns.

    Each entry of ``semantic_map`` maps a segment position (1-based) to a
    column alias. Aliases are backed by ``CodeCombinationSegmentN``. Map
    keys outside ``1..n_segments`` are rejected by the caller — this
    helper assumes inputs already validated.
    """
    if not semantic_map:
        return ""
    return ",\n".join(
        f"  CodeCombinationSegment{pos}{' ' * max(1, 39 - len(str(pos)))}"
        f"AS {alias}"
        for pos, alias in sorted(semantic_map.items())
    )


def _validate_segment_map(
    semantic_map: Mapping[int, str],
    n_segments: int,
) -> None:
    if n_segments < 1 or n_segments > MAX_FUSION_SEGMENTS:
        raise ValueError(
            f"n_segments must be in [1, {MAX_FUSION_SEGMENTS}], got {n_segments!r}"
        )
    for pos, alias in semantic_map.items():
        if not 1 <= pos <= n_segments:
            raise ValueError(
                f"semantic_segment_map position {pos!r} (alias {alias!r}) is "
                f"out of range [1, {n_segments}]"
            )
        if not isinstance(alias, str) or not _SQL_IDENTIFIER_RE.match(alias):
            raise ValueError(
                f"semantic_segment_map alias {alias!r} (position {pos}) is not "
                "a valid SQL identifier — must match ^[A-Za-z_][A-Za-z0-9_]*$"
            )
    seen_aliases = set()
    for alias in semantic_map.values():
        if alias in seen_aliases:
            raise ValueError(
                f"semantic_segment_map alias {alias!r} is duplicated — each "
                "alias must be unique"
            )
        seen_aliases.add(alias)


def build_dim_account_sql(
    *,
    bronze_table: str = SOURCE_BRONZE_TABLE,
    silver_table: str = TARGET_SILVER_TABLE,
    n_segments: int = MAX_FUSION_SEGMENTS,
    semantic_segment_map: Mapping[int, str] | None = None,
) -> str:
    """Return the CREATE-OR-REPLACE Delta SQL that produces ``silver.dim_account``.

    Pure string output — no Spark required.

    Plugin-portable shape:

    * ``segment_01 … segment_<n_segments>`` are always emitted (default
      ``n_segments=30``, Fusion's maximum). Consumers that need to be
      tenant-agnostic should read these positional columns.
    * ``code_combination`` is ``CONCAT_WS('.', …)`` across all configured
      segments — ``CONCAT_WS`` naturally skips NULL inputs.
    * Semantic alias columns are emitted per ``semantic_segment_map``
      (default: the Fusion-conventional six — company / cost_center /
      account / subaccount / product / intercompany at positions 1..6).
      Pass an alternate dict to align with a customer's COA design;
      pass ``{}`` to suppress aliases entirely and read only positional
      columns.

    The dedupe rule keeps the row with the most-recent ``_extract_ts``
    per ``CodeCombinationCodeCombinationId``. Rows with NULL CCID are
    filtered (would never join anyway).
    """
    if semantic_segment_map is None:
        semantic_segment_map = DEFAULT_SEMANTIC_SEGMENT_MAP
    _validate_segment_map(semantic_segment_map, n_segments)

    positional_lines = _segment_positional_lines(n_segments)
    code_combination = _code_combination_concat(n_segments)
    semantic_lines = _semantic_alias_lines(semantic_segment_map, n_segments)
    semantic_block = f"{semantic_lines},\n" if semantic_lines else ""

    return f"""\
CREATE OR REPLACE TABLE {silver_table}
USING DELTA
AS
SELECT
  monotonically_increasing_id()                                    AS account_key,
  CAST(CodeCombinationCodeCombinationId AS BIGINT)                 AS account_id,
  CAST(CodeCombinationChartOfAccountsId AS BIGINT)                 AS chart_of_accounts_id,
  {code_combination}                                                AS code_combination,
{positional_lines},
{semantic_block}  CodeCombinationAccountType                                       AS account_type,
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


def detect_active_segments(
    spark: SparkSession,
    *,
    bronze_table: str = SOURCE_BRONZE_TABLE,
    min_populated_fraction: float = 0.01,
) -> list[int]:
    """Return the list of segment positions (1..30) that carry non-trivial data.

    Probes each ``CodeCombinationSegmentN`` for ``COUNT(*) - COUNT(NULL)
    / COUNT(*)`` and returns positions where the populated fraction
    exceeds ``min_populated_fraction`` (default 1%). Useful for the
    orchestrator to size ``n_segments`` in :func:`build` per tenant
    without emitting 30 columns of NULL on a 4-segment customer.

    Spark-side; not unit-tested directly. Exercised by live evidence.
    """
    if not 0.0 <= min_populated_fraction <= 1.0:
        raise ValueError(
            f"min_populated_fraction must be in [0.0, 1.0], got "
            f"{min_populated_fraction!r}"
        )
    select_exprs = ", ".join(
        f"SUM(CASE WHEN CodeCombinationSegment{i} IS NOT NULL THEN 1 ELSE 0 END) "
        f"* 1.0 / NULLIF(COUNT(*), 0) AS frac_{i:02d}"
        for i in range(1, MAX_FUSION_SEGMENTS + 1)
    )
    row = spark.sql(f"SELECT {select_exprs} FROM {bronze_table}").collect()[0]
    return [
        i for i in range(1, MAX_FUSION_SEGMENTS + 1)
        if (row[f"frac_{i:02d}"] or 0.0) >= min_populated_fraction
    ]


def build(
    spark: SparkSession,
    *,
    bronze_table: str = SOURCE_BRONZE_TABLE,
    silver_table: str = TARGET_SILVER_TABLE,
    n_segments: int = MAX_FUSION_SEGMENTS,
    semantic_segment_map: Mapping[int, str] | None = None,
) -> DataFrame:
    """Materialize ``silver.dim_account`` from ``bronze.gl_coa``.

    Runs the SQL from :func:`build_dim_account_sql` against ``spark`` and
    returns a DataFrame backed by the freshly-written silver table. All
    knobs (``n_segments``, ``semantic_segment_map``) are forwarded to the
    SQL builder unchanged. The semantic-alias defaults match the
    Fusion-conventional six-segment ordering so saasfademo1 (and other
    tenants on the conventional COA design) reproduce the pre-refactor
    column shape exactly.
    """
    spark.sql(
        build_dim_account_sql(
            bronze_table=bronze_table,
            silver_table=silver_table,
            n_segments=n_segments,
            semantic_segment_map=semantic_segment_map,
        )
    )
    return spark.table(silver_table)


__all__ = [
    "DEFAULT_SEMANTIC_SEGMENT_MAP",
    "MAX_FUSION_SEGMENTS",
    "SOURCE_BRONZE_TABLE",
    "TARGET_SILVER_TABLE",
    "build",
    "build_dim_account_sql",
    "detect_active_segments",
]
