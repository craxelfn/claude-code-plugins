"""gold.supplier_spend — supplier x currency x approval-status spend mart.

Productizes TC8's prototype which proved the SQL on `saasfademo1` (etap-dev5)
demo pod: $3.2B aggregate / 236 records / top vendor `300000047507499` at
$892.7M.

Currency in grain (mandatory)
-----------------------------

Round-6 review surfaced that an earlier version of this mart aggregated
amounts across all currencies — same bug class TC23 documented for
gl_balance and that reviewer Blocker #1 fixed for ap_aging. ``saasfademo1``
holds invoices in 12 different currencies (USD/GBP/EUR/CNY/JPY/AUD/INR/
CHF/AED/PLN/TRY/MXN as of 2026-05-10); summing them produces meaningless
totals. ``currency_code = UPPER(ApInvoicesInvoiceCurrencyCode)`` is now in
the grain. Consumers do their own FX rollups (or stay within a currency).

Design — single LEFT-JOIN form (financial-correctness invariant)
----------------------------------------------------------------

The grain is **(invoice's vendor_id, currency_code, approval_status)**.
We use a single LEFT JOIN from `bronze.ap_invoices` → `silver.dim_supplier`:

* The invoice side is *always preserved* — every invoice dollar appears in
  exactly one output row, regardless of whether its vendor is in the dim.
  An INNER JOIN would silently drop invoices for vendors missing from the
  dim, which would understate financial spend. Not acceptable for a mart
  consumed by CFO dashboards.

* `vendor_id` in the output is `CAST(inv.ApInvoicesVendorId AS BIGINT)` —
  the invoice's claim of who got paid. `dim_supplier.vendor_id` is only
  used as the join condition; the dim's identity isn't authoritative for
  the gold mart's grain.

* `supplier_number`, `supplier_name`, `business_relationship` are pulled
  from the dim where the join matches; NULL otherwise. Demo pods like
  eseb-test (where every dim row has NULL `vendor_id`) naturally produce
  all-NULL dim attributes — same effective output as a "spend-only"
  fallback, but without a separate code path.

Earlier versions of this module had two SQL forms (INNER JOIN + spend-only)
picked by `id_populated_pct(vendor_id) >= 0.5`. That picker measured the
wrong thing — dim's internal completeness rather than invoice→dim join
coverage. The unified LEFT JOIN avoids the question entirely. The
`id_populated_pct` helper (still in `dim_supplier`) remains useful as a
diagnostic but is no longer load-bearing for path selection.

Bronze column convention
------------------------

`bronze.ap_invoices` from the BICC ``aidataplatform`` connector for
``InvoiceHeaderExtractPVO`` uses **PascalCase with `ApInvoices` prefix**
(``ApInvoicesVendorId``, ``ApInvoicesInvoiceAmount``, etc.). This is
different from `bronze.erp_suppliers` (which uses UPPERCASE without
prefix). Confirmed live on ``fusion_bundle_dev`` (2026-05-07).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from oracle_ai_data_platform_fusion_bundle.config.paths import DEFAULT_PATHS, TablePaths

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


SOURCE_BRONZE_TABLE: Final[str] = DEFAULT_PATHS.bronze("ap_invoices")
SOURCE_SILVER_DIM:   Final[str] = DEFAULT_PATHS.silver("dim_supplier")
TARGET_GOLD_TABLE:   Final[str] = DEFAULT_PATHS.gold("supplier_spend")

DEFAULT_CURRENCY_COL: Final[str] = "ApInvoicesInvoiceCurrencyCode"

#: Aliases checked by :func:`detect_currency_col`. First-match wins, so
#: the canonical Fusion BICC name precedes the legacy alias.
KNOWN_CURRENCY_COL_ALIASES: Final[tuple[str, ...]] = (
    "ApInvoicesInvoiceCurrencyCode",
    "ApInvoicesCurrencyCode",
)


def _run_id_audit_sql(run_id: str | None) -> str:
    """SQL fragment for the gold_run_id audit column (§3.5a, B3)."""
    if run_id is None:
        return "NULL"
    escaped = run_id.replace("'", "''")
    return f"'{escaped}'"


def build_supplier_spend_sql(
    *,
    paths:           TablePaths | None = None,
    bronze_invoices: str | None = None,
    silver_dim:      str | None = None,
    gold_table:      str | None = None,
    currency_col:    str = DEFAULT_CURRENCY_COL,
    run_id:          str | None = None,
) -> str:
    """Return the CREATE-OR-REPLACE Delta SQL for ``gold.supplier_spend``.

    Single LEFT-JOIN form — see module docstring for the financial-correctness
    rationale. The invoice is always the preserved (left) side; every invoice
    dollar appears in the output regardless of whether its vendor matches the
    dim. Dim attributes (`supplier_number`, `supplier_name`,
    `business_relationship`) are NULL when no match exists.

    ``currency_col`` defaults to the canonical Fusion BICC column
    ``ApInvoicesInvoiceCurrencyCode``; pass ``ApInvoicesCurrencyCode`` (or
    any other alias) for tenants whose extract uses a different name.

    ``paths`` (defaults to ``DEFAULT_PATHS``) resolves the bronze/silver/gold
    table identifiers from the tenant's ``bundle.yaml.aidp.*`` config.
    Explicit per-table kwargs win over ``paths``.
    """
    if paths is None:
        paths = DEFAULT_PATHS
    if bronze_invoices is None:
        bronze_invoices = paths.bronze("ap_invoices")
    if silver_dim is None:
        silver_dim = paths.silver("dim_supplier")
    if gold_table is None:
        gold_table = paths.gold("supplier_spend")
    run_id_sql = _run_id_audit_sql(run_id)
    return f"""\
CREATE OR REPLACE TABLE {gold_table}
USING DELTA
AS
SELECT
  CAST(inv.ApInvoicesVendorId AS BIGINT)                           AS vendor_id,
  UPPER(CAST(inv.{currency_col} AS STRING))                        AS currency_code,
  ds.supplier_number                                               AS supplier_number,
  ds.supplier_name                                                 AS supplier_name,
  ds.business_relationship                                         AS business_relationship,
  inv.ApInvoicesApprovalStatus                                     AS approval_status,
  COUNT(*)                                                         AS invoice_count,
  ROUND(SUM(COALESCE(CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(20, 2)), 0)), 2)
                                                                       AS total_invoice_amount,
  ROUND(SUM(COALESCE(CAST(inv.ApInvoicesAmountPaid    AS DECIMAL(20, 2)), 0)), 2)
                                                                       AS total_paid,
  MAX(CAST(inv.ApInvoicesInvoiceDate AS DATE))                     AS last_invoice_date,
  current_timestamp()                                              AS gold_built_at,
  {run_id_sql}                                                     AS gold_run_id
FROM {bronze_invoices} inv
LEFT JOIN {silver_dim} ds
  ON ds.vendor_id = CAST(inv.ApInvoicesVendorId AS BIGINT)
WHERE inv.ApInvoicesVendorId IS NOT NULL
GROUP BY
  CAST(inv.ApInvoicesVendorId AS BIGINT),
  UPPER(CAST(inv.{currency_col} AS STRING)),
  ds.supplier_number,
  ds.supplier_name,
  ds.business_relationship,
  inv.ApInvoicesApprovalStatus
"""


def detect_currency_col(
    spark: SparkSession,
    *,
    bronze_invoices: str = SOURCE_BRONZE_TABLE,
) -> str | None:
    """Probe ``bronze.ap_invoices`` for a known currency column alias.

    Returns the matched column name (canonical first, alias second) or
    ``None`` if neither variant is present. ``None`` is a hard blocker —
    :func:`build` raises if so, because currency in grain is mandatory
    and shipping a single-currency-summed mart on a multi-currency
    tenant produces nonsense totals.

    Mirrors :func:`ap_aging.detect_ap_aging_params`'s currency-detect
    contract so the two AP-side marts agree on what counts as the
    "currency column".
    """
    schema_names = {f.name for f in spark.table(bronze_invoices).schema}
    return next((c for c in KNOWN_CURRENCY_COL_ALIASES if c in schema_names), None)


def build(
    spark: SparkSession,
    *,
    auto_detect: bool = True,
    paths:           TablePaths | None = None,
    bronze_invoices: str | None = None,
    silver_dim:      str | None = None,
    gold_table:      str | None = None,
    currency_col:    str = DEFAULT_CURRENCY_COL,
    run_id:          str | None = None,
) -> DataFrame:
    """Materialize ``gold.supplier_spend``; returns a DataFrame backed by it.

    The single LEFT-JOIN form preserves every invoice — no path-selection
    logic. The dim is consulted opportunistically for attributes; missing
    matches yield NULL dim columns rather than dropping invoices. Currency
    is in the grain — cross-currency aggregation is the consumer's
    responsibility.

    ``auto_detect=True`` (default) probes ``bronze.ap_invoices`` for the
    canonical ``ApInvoicesInvoiceCurrencyCode`` or its supported alias
    ``ApInvoicesCurrencyCode`` and uses whichever is present. An explicit
    ``currency_col`` kwarg different from the default wins (for tenants
    whose extract uses some other alias). If neither alias is found AND
    no explicit override is passed, ``build`` raises a clear ValueError
    — the mart cannot ship without currency in grain.

    ``paths`` (defaults to ``DEFAULT_PATHS``) resolves the bronze/silver/gold
    table identifiers from the tenant's ``bundle.yaml.aidp.*`` config.
    Explicit per-table kwargs win over ``paths``.
    """
    if paths is None:
        paths = DEFAULT_PATHS
    if bronze_invoices is None:
        bronze_invoices = paths.bronze("ap_invoices")
    if silver_dim is None:
        silver_dim = paths.silver("dim_supplier")
    if gold_table is None:
        gold_table = paths.gold("supplier_spend")

    if auto_detect and currency_col == DEFAULT_CURRENCY_COL:
        detected = detect_currency_col(spark, bronze_invoices=bronze_invoices)
        if detected is None:
            raise ValueError(
                "Currency column missing on bronze.ap_invoices — none of "
                f"{KNOWN_CURRENCY_COL_ALIASES!r} is present. Cannot ship a "
                "single-currency-summed supplier_spend mart (currency-in-grain "
                "rule). Re-extract bronze with currency, or pass an explicit "
                "currency_col= override if your tenant uses a different alias."
            )
        currency_col = detected

    sql = build_supplier_spend_sql(
        bronze_invoices=bronze_invoices,
        silver_dim=silver_dim,
        gold_table=gold_table,
        currency_col=currency_col,
        run_id=run_id,
    )
    spark.sql(sql)
    return spark.table(gold_table)


__all__ = [
    "DEFAULT_CURRENCY_COL",
    "KNOWN_CURRENCY_COL_ALIASES",
    "SOURCE_BRONZE_TABLE",
    "SOURCE_SILVER_DIM",
    "TARGET_GOLD_TABLE",
    "build",
    "build_supplier_spend_sql",
    "detect_currency_col",
]
