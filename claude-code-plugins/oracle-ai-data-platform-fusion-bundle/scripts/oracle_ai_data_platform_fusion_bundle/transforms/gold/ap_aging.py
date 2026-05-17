"""gold.ap_aging — AP payables aging by (vendor, currency, aging_bucket).

Productizes the canonical AP aging fact: per-vendor, per-currency open-invoice
amount split into ``current`` / ``1-30`` / ``31-60`` / ``61-90`` / ``91+``
days-past-due buckets. Source is ``bronze.ap_invoices`` from BICC
``InvoiceHeaderExtractPVO``. Single LEFT JOIN to ``silver.dim_supplier`` to
surface vendor attributes; vendor on the fact side is authoritative for the
grain.

Plugin-portable shape — schema variants
---------------------------------------

Fusion AP schemas vary slightly per tenant (column-name dialects driven by
extension-package activation and BICC export-config choices). The module
ships with sensible defaults that match the canonical BICC convention, plus
a small set of column-name knobs for the axes that are known to vary:

* ``due_date_col``  — ``ApInvoicesDueDate`` may be absent (pass ``None`` to
  disable). Some tenants populate ``ApInvoicesTermsDate`` exclusively.
* ``terms_date_col`` — symmetric inverse (pass ``None`` to disable).
* ``cancelled_col``  — variant is ``ApInvoicesCancelledDate`` (NULL means
  not cancelled) or ``ApInvoicesCancelledFlag`` (``Y`` means cancelled).
  Set ``cancelled_kind="date"`` or ``"flag"`` accordingly. Pass ``None``
  to skip the filter entirely.
* ``currency_col``  — universally ``ApInvoicesInvoiceCurrencyCode`` so far,
  but exposed as a knob in case an aliased extract surfaces.

Two modes, one parameter — ``due_date_mode``
--------------------------------------------

* ``"real"`` (default): aging = days past ``COALESCE(TermsDate, DueDate,
  invoice_date + 30 days)``. NET-30 is the standard Fusion AP terms default
  and only applies to *residual* NULLs after the configured due-date columns
  have been coalesced. Per-row provenance is emitted in ``due_date_source``
  (``'terms_date'`` | ``'due_date'`` | ``'net30_fallback'``). Live evidence
  should report ``net30_fallback_count`` so the consumer knows the share.

* ``"proxy"`` (fallback): aging = days since ``invoice_date``. Used when
  the tenant lacks real due-date columns (``terms_date_col`` and
  ``due_date_col`` both ``None``, or coalesced coverage below the 80 % gate
  the orchestrator/probe enforces). The mart is written to
  ``gold.ap_outstanding_by_invoice_age`` and an audit column
  ``bucket_basis = 'invoice_date'`` lets consumers tell the two shapes
  apart at-a-glance.

Both modes share most of the downstream column shape (vendor_id, currency_code,
supplier attributes from the dim, aging_bucket, open_amount aggregates,
credit aggregates, oldest_invoice_date, audit columns). Two shape differences
deliberately distinguish the modes at *schema* time so the column name
self-documents semantics:

* ``max_days_past_due`` (real mode) vs ``max_days_outstanding`` (proxy mode)
  — same MAX expression, but the label reflects what the days count is
  *measuring*. Calling invoice-age "days past due" would reintroduce the
  exact semantic confusion the mart-name gate (PLAN §3.2) exists to
  prevent.
* ``due_date_source`` / ``net30_fallback_count`` / ``terms_date_count`` /
  ``due_date_count`` — real-mode-only (proxy has no due-date concept).

Filter invariants
-----------------

* ``open_amount = invoice_amount - COALESCE(amount_paid, 0) <> 0`` —
  **invariant across tenants**. Preserves credit memos / overpayment
  offsets as negative open balances; absence of credits on one tenant is
  not justification to downgrade to ``> 0`` for the product. Per
  reviewer Blocker #1.
* ``invoice_date IS NOT NULL`` — prevents ``DATEDIFF(now, NULL) → NULL``
  from silently bucketing NULL-dated rows into ``'91+'``. Configurable
  via ``null_invoice_date_policy``: ``"drop"`` (default) filters them
  out; ``"unknown_bucket"`` keeps them and routes to a sixth bucket
  ``'unknown_date'``.
* Cancelled-invoice exclusion based on ``cancelled_col`` + ``cancelled_kind``.
* ``vendor_id IS NOT NULL`` — vendor-grain requires a vendor.

Currency in grain (mandatory)
-----------------------------

``currency_code = UPPER(ApInvoicesInvoiceCurrencyCode)`` is a key column,
not a slicer. Cross-currency totals are meaningless without FX conversion,
which is a consumer concern (same rule ``gl_balance`` applies). The mart
emits per-(vendor, currency, bucket) rows; dashboards aggregate within a
currency or apply consumer-side FX before rolling up.

Injectable as-of date
---------------------

``as_of_date_expr`` defaults to ``CURRENT_DATE()`` so a daily refresh
captures "today's" aging. Tests pin it to a literal (e.g.
``"DATE'2026-05-10'"``) for deterministic bucket assertions.

Decimal precision
-----------------

Amounts use ``DECIMAL(28, 2)`` (cents granularity) — the financial-reporting
standard, and matches what ``gl_balance`` / ``supplier_spend`` already
emit. Source ``ApInvoicesInvoiceAmount`` / ``ApInvoicesAmountPaid`` are
``decimal(38, 30)`` on this pod; downcast happens inside the CTE so the
GROUP BY and ROUND aggregate consistently.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from oracle_ai_data_platform_fusion_bundle.config.paths import DEFAULT_PATHS, TablePaths

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


SOURCE_BRONZE_TABLE:     Final[str] = DEFAULT_PATHS.bronze("ap_invoices")
SOURCE_SILVER_DIM:       Final[str] = DEFAULT_PATHS.silver("dim_supplier")
TARGET_GOLD_TABLE_REAL:  Final[str] = DEFAULT_PATHS.gold("ap_aging")
TARGET_GOLD_TABLE_PROXY: Final[str] = DEFAULT_PATHS.gold("ap_outstanding_by_invoice_age")

DUE_DATE_MODE_REAL:  Final[str] = "real"
DUE_DATE_MODE_PROXY: Final[str] = "proxy"
DUE_DATE_MODE_AUTO:  Final[str] = "auto"

DEFAULT_REAL_MODE_GATE_THRESHOLD: Final[float] = 0.80

NULL_INVOICE_DATE_POLICY_DROP:           Final[str] = "drop"
NULL_INVOICE_DATE_POLICY_UNKNOWN_BUCKET: Final[str] = "unknown_bucket"

CANCELLED_KIND_DATE: Final[str] = "date"
CANCELLED_KIND_FLAG: Final[str] = "flag"


def _default_target(due_date_mode: str, paths: TablePaths | None = None) -> str:
    """Resolve the default gold-table path for the given (concrete) due-date mode.

    Callers must have ALREADY resolved ``due_date_mode`` from
    :data:`DUE_DATE_MODE_AUTO` via :func:`decide_due_date_mode` before calling
    this helper — passing the still-unresolved ``"auto"`` sentinel raises.
    """
    if paths is None:
        paths = DEFAULT_PATHS
    if due_date_mode == DUE_DATE_MODE_REAL:
        return paths.gold("ap_aging")
    if due_date_mode == DUE_DATE_MODE_PROXY:
        return paths.gold("ap_outstanding_by_invoice_age")
    raise ValueError(
        f"due_date_mode must be {DUE_DATE_MODE_REAL!r} or {DUE_DATE_MODE_PROXY!r}, "
        f"got {due_date_mode!r}"
    )


def decide_due_date_mode(
    *,
    terms_date_col:  str | None,
    due_date_col:    str | None,
    coalesced_frac:  float | None,
    gate_threshold:  float = DEFAULT_REAL_MODE_GATE_THRESHOLD,
) -> str:
    """Pure decision function — returns ``"real"`` or ``"proxy"``.

    The rules (per PLAN §3.2 mart-name gate):

    * If neither ``terms_date_col`` nor ``due_date_col`` is set, there's no
      real due-date column to base aging on → ``"proxy"``.
    * If at least one real-date column exists, ``coalesced_frac`` (the
      non-NULL fraction of ``COALESCE(TermsDate, DueDate)`` over the
      open-invoice population) decides:
        * ``>= gate_threshold`` (default 0.80) → ``"real"``
        * ``<  gate_threshold``                → ``"proxy"``
    * If at least one real-date column exists but ``coalesced_frac`` is
      ``None`` — i.e. the measurement returned no data because the open
      population is empty — default to ``"real"``. Empty mart, canonical
      name; tomorrow's data fits the shape without re-pivot. Proxy mode
      is the answer for *low data quality*, not *no data*.

    Coverage below threshold means too many rows would land on NET-30
    silent fallback, which would publish fake due-date aging under the
    canonical ``gold.ap_aging`` name. Proxy mode (under a different table
    name) is the honest representation.

    This is a pure function so unit tests can exercise the gate logic
    without Spark; the build path computes ``coalesced_frac`` via
    :func:`_measure_due_date_coverage` and then calls this.
    """
    if not (terms_date_col or due_date_col):
        return DUE_DATE_MODE_PROXY
    if coalesced_frac is None:
        # Cols set, but no measurement (e.g. empty open-invoice population
        # — nothing to compute coverage over). Default to real mode so the
        # tenant ships an empty canonical ``gold.ap_aging``; tomorrow when
        # they have invoices the shape is already correct and consumers
        # don't need to re-pivot. Proxy mode is a fallback for *low data
        # quality* (sparse due-dates on present rows), not for *no data*.
        return DUE_DATE_MODE_REAL
    if not 0.0 <= coalesced_frac <= 1.0:
        raise ValueError(
            f"coalesced_frac must be in [0.0, 1.0], got {coalesced_frac!r}"
        )
    return DUE_DATE_MODE_REAL if coalesced_frac >= gate_threshold else DUE_DATE_MODE_PROXY


def _measure_due_date_coverage(
    spark: SparkSession,
    *,
    bronze_table:    str,
    terms_date_col:  str | None,
    due_date_col:    str | None,
    cancelled_col:   str | None,
    cancelled_kind:  str,
    null_invoice_date_policy: str,
) -> float | None:
    """Run the coalesced due-date coverage query over the open-invoice population.

    Uses the same WHERE clause the mart will use (vendor NOT NULL, optional
    invoice-date NOT NULL, cancelled exclusion, ``<> 0`` filter) so the gate
    measures coverage on exactly the rows the mart will aggregate. Spark-side;
    not unit-tested directly.

    Returns:

    * ``None`` if the open population is empty — there's no data to
      compute coverage against, so the caller (``decide_due_date_mode``)
      defaults to real mode rather than relabeling the empty mart.
    * ``None`` if neither real-date column is configured (caller will
      route directly to proxy mode regardless of fraction).
    * Otherwise the coalesced non-NULL fraction in ``[0.0, 1.0]``.

    The SQL uses ``NULLIF(COUNT(*), 0)`` for the divisor so the query
    doesn't fault under ANSI Spark when the WHERE clause yields zero
    rows. The ``open_n`` projection lets the caller distinguish "empty
    population" from "all rows missing real dates".
    """
    if not (terms_date_col or due_date_col):
        return None

    cancelled_clause   = _cancelled_filter(cancelled_col, cancelled_kind)
    invoice_date_clause = _invoice_date_filter(null_invoice_date_policy)

    parts: list[str] = []
    if terms_date_col:
        parts.append(f"inv.{terms_date_col} IS NOT NULL")
    if due_date_col:
        parts.append(f"inv.{due_date_col} IS NOT NULL")
    has_real_date_expr = " OR ".join(parts)

    sql = f"""\
SELECT
  COUNT(*)                                                                              AS open_n,
  SUM(CASE WHEN {has_real_date_expr} THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)     AS coalesced_frac
FROM {bronze_table} inv
WHERE inv.ApInvoicesVendorId IS NOT NULL
{invoice_date_clause}{cancelled_clause}    AND CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 2))
      - CAST(COALESCE(inv.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 2)) <> 0
"""
    row = spark.sql(sql).collect()[0]
    if row["open_n"] == 0 or row["coalesced_frac"] is None:
        return None
    return float(row["coalesced_frac"])


def _due_date_coalesce_expr(
    terms_date_col: str | None,
    due_date_col:   str | None,
) -> str:
    """The COALESCE expression for the effective due_date in real mode.

    Returns SQL that prefers ``terms_date_col`` over ``due_date_col`` and
    falls back to ``invoice_date + 30`` for residual NULLs. The orchestrator
    is responsible for routing to proxy mode when coalesced coverage of the
    real columns is below the 80% gate; this expression is only the
    *residual* NET-30 fallback, not a uniform replacement.
    """
    parts: list[str] = []
    if terms_date_col:
        parts.append(f"CAST(inv.{terms_date_col} AS DATE)")
    if due_date_col:
        parts.append(f"CAST(inv.{due_date_col}   AS DATE)")
    parts.append("DATE_ADD(CAST(inv.ApInvoicesInvoiceDate AS DATE), 30)")
    return "COALESCE(\n      " + ",\n      ".join(parts) + "\n    )"


def _due_date_source_expr(
    terms_date_col: str | None,
    due_date_col:   str | None,
) -> str:
    """Emits ``'terms_date'`` / ``'due_date'`` / ``'net30_fallback'`` per row."""
    branches: list[str] = []
    if terms_date_col:
        branches.append(
            f"WHEN inv.{terms_date_col} IS NOT NULL THEN 'terms_date'"
        )
    if due_date_col:
        branches.append(
            f"WHEN inv.{due_date_col}   IS NOT NULL THEN 'due_date'"
        )
    if not branches:
        return "'net30_fallback'"
    return "CASE\n      " + "\n      ".join(branches) + "\n      ELSE 'net30_fallback'\n    END"


def _cancelled_filter(cancelled_col: str | None, cancelled_kind: str) -> str:
    """WHERE-clause snippet excluding cancelled invoices; ``""`` if disabled."""
    if cancelled_col is None:
        return ""
    if cancelled_kind == CANCELLED_KIND_DATE:
        return f"    AND inv.{cancelled_col} IS NULL\n"
    if cancelled_kind == CANCELLED_KIND_FLAG:
        return f"    AND (inv.{cancelled_col} IS NULL OR inv.{cancelled_col} <> 'Y')\n"
    raise ValueError(
        f"cancelled_kind must be {CANCELLED_KIND_DATE!r} or {CANCELLED_KIND_FLAG!r}, "
        f"got {cancelled_kind!r}"
    )


def _invoice_date_filter(null_invoice_date_policy: str) -> str:
    if null_invoice_date_policy == NULL_INVOICE_DATE_POLICY_DROP:
        return "    AND inv.ApInvoicesInvoiceDate IS NOT NULL\n"
    if null_invoice_date_policy == NULL_INVOICE_DATE_POLICY_UNKNOWN_BUCKET:
        return ""
    raise ValueError(
        f"null_invoice_date_policy must be "
        f"{NULL_INVOICE_DATE_POLICY_DROP!r} or "
        f"{NULL_INVOICE_DATE_POLICY_UNKNOWN_BUCKET!r}, got {null_invoice_date_policy!r}"
    )


def _bucket_case(
    days_expr: str,
    null_invoice_date_policy: str,
) -> str:
    """The 5-or-6-branch CASE that assigns aging buckets.

    ``days_expr`` is the SQL expression that yields the days-past-due (real
    mode) or days-since-invoice (proxy mode). The CASE evaluates it once;
    Spark will common-subexpression-eliminate against the same expression
    used elsewhere in the projection.
    """
    unknown_branch = (
        "    WHEN o.invoice_date IS NULL THEN 'unknown_date'\n"
        if null_invoice_date_policy == NULL_INVOICE_DATE_POLICY_UNKNOWN_BUCKET
        else ""
    )
    return (
        "CASE\n"
        f"{unknown_branch}"
        f"    WHEN {days_expr} <=  0 THEN 'current'\n"
        f"    WHEN {days_expr} <= 30 THEN '1-30'\n"
        f"    WHEN {days_expr} <= 60 THEN '31-60'\n"
        f"    WHEN {days_expr} <= 90 THEN '61-90'\n"
        f"    ELSE                        '91+'\n"
        "  END"
    )


def _run_id_audit_sql(run_id: str | None) -> str:
    """SQL fragment for the gold_run_id audit column (§3.5a, B3)."""
    if run_id is None:
        return "NULL"
    escaped = run_id.replace("'", "''")
    return f"'{escaped}'"


def build_ap_aging_sql(
    *,
    paths:        TablePaths | None = None,
    bronze_table: str | None = None,
    silver_dim:   str | None = None,
    gold_table:   str | None = None,
    due_date_mode: str = DUE_DATE_MODE_REAL,
    as_of_date_expr: str = "CURRENT_DATE()",
    null_invoice_date_policy: str = NULL_INVOICE_DATE_POLICY_DROP,
    terms_date_col: str | None = "ApInvoicesTermsDate",
    due_date_col:   str | None = "ApInvoicesDueDate",
    cancelled_col:  str | None = "ApInvoicesCancelledDate",
    cancelled_kind: str        = CANCELLED_KIND_DATE,
    currency_col:   str        = "ApInvoicesInvoiceCurrencyCode",
    run_id:         str | None = None,
) -> str:
    """Return the CREATE-OR-REPLACE Delta SQL for the AP aging mart.

    Pure string output — no Spark required. Unit-tested for both
    ``due_date_mode`` branches and the full matrix of schema-variant knobs.

    Optimization choices:

    * **Single scan** of ``bronze.ap_invoices`` via the CTE.
    * **Open-amount is computed once** in the CTE and reused for the
      ``<> 0`` filter, the GROUP BY aggregates, and the credit aggregate.
    * **LEFT JOIN to a small dim** (``silver.dim_supplier`` is in the
      hundreds of rows on every Fusion tenant we've seen) is broadcast-
      friendly; Spark's AQE will choose broadcast at runtime without a
      hint.
    * **Type casts pushed into the CTE** so the outer SELECT operates on
      already-cast columns — avoids re-casting on every aggregate.
    * **``COALESCE`` around every amount arithmetic** prevents NULL
      propagation from a single ``ApInvoicesAmountPaid IS NULL`` row
      nullifying the whole aggregate.
    * **Dim-side columns referenced via the alias ``ds``** so the LEFT
      JOIN preserves invoice rows for unknown vendors (NULL supplier
      attributes), matching the financial-correctness invariant established
      by ``supplier_spend`` and ``gl_balance``.
    """
    if due_date_mode not in (DUE_DATE_MODE_REAL, DUE_DATE_MODE_PROXY):
        raise ValueError(
            f"due_date_mode must be {DUE_DATE_MODE_REAL!r} or {DUE_DATE_MODE_PROXY!r}, "
            f"got {due_date_mode!r}"
        )
    if due_date_mode == DUE_DATE_MODE_REAL and not (terms_date_col or due_date_col):
        raise ValueError(
            "real mode requires at least one of terms_date_col / due_date_col; "
            "pass due_date_mode='proxy' if neither column is available."
        )
    if paths is None:
        paths = DEFAULT_PATHS
    if bronze_table is None:
        bronze_table = paths.bronze("ap_invoices")
    if silver_dim is None:
        silver_dim = paths.silver("dim_supplier")
    if gold_table is None:
        gold_table = _default_target(due_date_mode, paths=paths)

    cancelled_clause   = _cancelled_filter(cancelled_col, cancelled_kind)
    invoice_date_clause = _invoice_date_filter(null_invoice_date_policy)

    if due_date_mode == DUE_DATE_MODE_REAL:
        due_date_expr      = _due_date_coalesce_expr(terms_date_col, due_date_col)
        due_date_src_expr  = _due_date_source_expr(terms_date_col, due_date_col)
        days_expr          = f"DATEDIFF({as_of_date_expr}, o.due_date)"
        bucket_basis_label = "'due_date'"
        max_days_alias     = "max_days_past_due"
        # real-mode-only projections — only aggregates (per-row due_date_source
        # lives in the CTE and is consumed by these aggregates; we don't surface
        # per-row provenance on the per-grain mart because there can be many
        # different sources collapsed into one (vendor, currency, bucket) row).
        due_date_source_proj      = ""
        net30_fallback_count_proj = (
            "SUM(CASE WHEN o.due_date_source = 'net30_fallback' THEN 1 ELSE 0 END) AS net30_fallback_count,\n  "
            "SUM(CASE WHEN o.due_date_source = 'terms_date'     THEN 1 ELSE 0 END) AS terms_date_count,\n  "
            "SUM(CASE WHEN o.due_date_source = 'due_date'       THEN 1 ELSE 0 END) AS due_date_count,\n  "
        )
        cte_due_date = f"""    {due_date_src_expr}                                                          AS due_date_source,
    {due_date_expr}                                                          AS due_date"""
    else:
        days_expr                 = f"DATEDIFF({as_of_date_expr}, o.invoice_date)"
        bucket_basis_label        = "'invoice_date'"
        max_days_alias            = "max_days_outstanding"
        due_date_source_proj      = ""
        net30_fallback_count_proj = ""
        cte_due_date = (
            "    CAST(NULL AS STRING)                                                       AS due_date_source,\n"
            "    CAST(NULL AS DATE)                                                         AS due_date"
        )

    bucket_case = _bucket_case(days_expr, null_invoice_date_policy)
    run_id_sql = _run_id_audit_sql(run_id)

    return f"""\
CREATE OR REPLACE TABLE {gold_table}
USING DELTA
AS
WITH open_invoices AS (
  SELECT
    CAST(inv.ApInvoicesVendorId AS BIGINT)                                   AS vendor_id,
    UPPER(CAST(inv.{currency_col} AS STRING))                                AS currency_code,
    CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 2))                      AS invoice_amount,
    CAST(COALESCE(inv.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 2))            AS amount_paid,
    CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 2))
      - CAST(COALESCE(inv.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 2))        AS open_amount,
    CAST(inv.ApInvoicesInvoiceDate AS DATE)                                  AS invoice_date,
{cte_due_date}
  FROM {bronze_table} inv
  WHERE inv.ApInvoicesVendorId IS NOT NULL
{invoice_date_clause}{cancelled_clause}    AND CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(28, 2))
      - CAST(COALESCE(inv.ApInvoicesAmountPaid, 0) AS DECIMAL(28, 2)) <> 0
)
SELECT
  o.vendor_id                                                              AS vendor_id,
  o.currency_code                                                          AS currency_code,
  ds.supplier_number                                                       AS supplier_number,
  ds.supplier_name                                                         AS supplier_name,
  ds.business_relationship                                                 AS business_relationship,
  {bucket_case}                                                            AS aging_bucket,
  {bucket_basis_label}                                                     AS bucket_basis,
  COUNT(*)                                                                 AS open_invoice_count,
  ROUND(SUM(o.open_amount), 2)                                             AS open_amount,
  ROUND(SUM(o.invoice_amount), 2)                                          AS invoice_amount_total,
  ROUND(SUM(o.amount_paid), 2)                                             AS amount_paid_total,
  ROUND(SUM(CASE WHEN o.open_amount < 0 THEN o.open_amount ELSE 0 END), 2) AS credit_open_amount,
  SUM(CASE WHEN o.open_amount < 0 THEN 1 ELSE 0 END)                       AS credit_open_count,
  {due_date_source_proj}{net30_fallback_count_proj}MIN(o.invoice_date)                                                      AS oldest_invoice_date,
  MAX({days_expr})                                                         AS {max_days_alias},
  CAST({as_of_date_expr} AS DATE)                                          AS as_of_date,
  current_timestamp()                                                      AS gold_built_at,
  {run_id_sql}                                                             AS gold_run_id
FROM open_invoices o
LEFT JOIN {silver_dim} ds
  ON ds.vendor_id = o.vendor_id
GROUP BY
  o.vendor_id,
  o.currency_code,
  ds.supplier_number,
  ds.supplier_name,
  ds.business_relationship,
  {bucket_case}
"""


def detect_ap_aging_params(
    spark: SparkSession,
    *,
    bronze_table: str = SOURCE_BRONZE_TABLE,
) -> dict[str, object]:
    """Schema-introspect ``bronze.ap_invoices`` and return a kwargs dict.

    The returned dict can be splatted into :func:`build` for tenants whose
    AP schema diverges from the defaults. Detected variants:

    * ``due_date_col``  — ``ApInvoicesDueDate`` if present, else ``None``
    * ``terms_date_col`` — ``ApInvoicesTermsDate`` if present, else ``None``
    * ``cancelled_col`` + ``cancelled_kind`` — ``ApInvoicesCancelledDate``
      (``"date"``) or ``ApInvoicesCancelledFlag`` (``"flag"``); ``None`` if
      neither is present.
    * ``currency_col`` — ``ApInvoicesInvoiceCurrencyCode`` (canonical) or
      ``ApInvoicesCurrencyCode`` (alias variant); ``None`` if neither is
      present. **Currency is mandatory** (currency-in-grain rule); the
      build path treats ``None`` as a hard gate and refuses to materialize
      the mart — see :func:`build`. Detection here just *measures*
      presence; enforcement happens at build time.

    Does **not** decide ``due_date_mode`` or ``null_invoice_date_policy`` —
    those depend on population statistics (coalesced non-NULL fraction over
    the open-invoice population, NULL invoice-date fraction) that the
    orchestrator/probe runner measures separately. The probe runner is the
    authority for those decisions; this helper only handles presence.
    """
    schema_names = {f.name for f in spark.table(bronze_table).schema}

    def has(col: str) -> bool:
        return col in schema_names

    terms_date_col = "ApInvoicesTermsDate" if has("ApInvoicesTermsDate") else None
    due_date_col   = "ApInvoicesDueDate"   if has("ApInvoicesDueDate")   else None

    if has("ApInvoicesCancelledDate"):
        cancelled_col, cancelled_kind = "ApInvoicesCancelledDate", CANCELLED_KIND_DATE
    elif has("ApInvoicesCancelDate"):
        # Alias variant (some Fusion extracts omit the "led" suffix).
        # Same semantics as ApInvoicesCancelledDate (non-NULL = cancelled).
        cancelled_col, cancelled_kind = "ApInvoicesCancelDate", CANCELLED_KIND_DATE
    elif has("ApInvoicesCancelledFlag"):
        cancelled_col, cancelled_kind = "ApInvoicesCancelledFlag", CANCELLED_KIND_FLAG
    else:
        cancelled_col, cancelled_kind = None, CANCELLED_KIND_DATE  # kind moot when col=None

    # Currency: first-match across the known Fusion BICC aliases. None means
    # neither alias exists — the build path hard-gates on this.
    currency_col = next(
        (c for c in ("ApInvoicesInvoiceCurrencyCode", "ApInvoicesCurrencyCode") if has(c)),
        None,
    )

    return {
        "terms_date_col": terms_date_col,
        "due_date_col":   due_date_col,
        "cancelled_col":  cancelled_col,
        "cancelled_kind": cancelled_kind,
        "currency_col":   currency_col,
    }


def build(
    spark: SparkSession,
    *,
    auto_detect: bool = True,
    paths:        TablePaths | None = None,
    bronze_table: str | None = None,
    silver_dim:   str | None = None,
    gold_table:   str | None = None,
    due_date_mode: str = DUE_DATE_MODE_AUTO,
    real_mode_gate_threshold: float = DEFAULT_REAL_MODE_GATE_THRESHOLD,
    as_of_date_expr: str = "CURRENT_DATE()",
    null_invoice_date_policy: str = NULL_INVOICE_DATE_POLICY_DROP,
    terms_date_col: str | None = "ApInvoicesTermsDate",
    due_date_col:   str | None = "ApInvoicesDueDate",
    cancelled_col:  str | None = "ApInvoicesCancelledDate",
    cancelled_kind: str        = CANCELLED_KIND_DATE,
    currency_col:   str        = "ApInvoicesInvoiceCurrencyCode",
    run_id:         str | None = None,
) -> DataFrame:
    """Materialize the AP aging mart; returns a DataFrame backed by the gold table.

    ``auto_detect=True`` (default) probes the bronze schema first to handle
    Fusion AP column-name variants (TermsDate vs DueDate presence, Cancelled-
    Date vs CancelledFlag, etc.); explicit kwarg overrides win over detected
    values. Set ``auto_detect=False`` to disable probing entirely.

    ``due_date_mode='auto'`` (default) is the **portable** path: after schema
    detection, the build *measures* coalesced ``(TermsDate OR DueDate)``
    non-NULL coverage over the open-invoice population on the actual tenant
    and applies the gate:

      * coverage ≥ ``real_mode_gate_threshold`` (default 0.80) → ``"real"`` →
        ``gold.ap_aging``
      * coverage <  ``real_mode_gate_threshold``               → ``"proxy"`` →
        ``gold.ap_outstanding_by_invoice_age``

    This prevents a tenant where ``TermsDate`` exists but is sparsely populated
    from silently shipping ``gold.ap_aging`` with majority NET-30 fallback —
    exactly what reviewer Blocker #3 forbids (fake due-date aging under the
    canonical name). For callers that already know which mode they want
    (e.g. an orchestrator that ran the probe separately), pass an explicit
    ``due_date_mode='real'`` or ``'proxy'`` to skip the coverage measurement.

    All other knobs are forwarded to :func:`build_ap_aging_sql` unchanged.

    Path resolution ordering (CRITICAL — see PLAN_P1.5b §4.2):

    1. ``paths`` / sentinel kwargs resolve ``bronze_table`` and ``silver_dim``
       (cheap; pure string assembly).
    2. Schema-variant detection runs against the resolved ``bronze_table``.
    3. ``due_date_mode='auto'`` auto-router measures coverage and resolves
       to ``'real'`` or ``'proxy'``.
    4. **Only now** can ``gold_table`` be resolved — its choice between the
       real and proxy table names depends on the resolved mode. Resolving
       earlier (under ``'auto'``) would catastrophically pick the proxy
       table for every above-threshold tenant.
    """
    if paths is None:
        paths = DEFAULT_PATHS
    if bronze_table is None:
        bronze_table = paths.bronze("ap_invoices")
    if silver_dim is None:
        silver_dim = paths.silver("dim_supplier")

    if auto_detect:
        detected = detect_ap_aging_params(spark, bronze_table=bronze_table)
        # Explicit overrides win — only fill in unspecified args
        if terms_date_col == "ApInvoicesTermsDate":
            terms_date_col = detected["terms_date_col"]  # type: ignore[assignment]
        if due_date_col   == "ApInvoicesDueDate":
            due_date_col   = detected["due_date_col"]    # type: ignore[assignment]
        if cancelled_col  == "ApInvoicesCancelledDate":
            cancelled_col  = detected["cancelled_col"]   # type: ignore[assignment]
            cancelled_kind = detected["cancelled_kind"]  # type: ignore[assignment]
        if currency_col   == "ApInvoicesInvoiceCurrencyCode":
            # Detected None means neither alias exists on the tenant; we
            # hard-gate below. A detected non-None alias (e.g. the
            # ``ApInvoicesCurrencyCode`` variant) overrides the default.
            if detected["currency_col"] is None:
                raise ValueError(
                    "Currency column missing on bronze.ap_invoices — neither "
                    "ApInvoicesInvoiceCurrencyCode nor ApInvoicesCurrencyCode "
                    "is present. Cannot ship a single-currency-summed AP "
                    "aging mart (currency-in-grain rule). Re-extract bronze "
                    "with currency, or pass an explicit currency_col= override "
                    "if your tenant uses a different alias."
                )
            currency_col = detected["currency_col"]      # type: ignore[assignment]

    if due_date_mode == DUE_DATE_MODE_AUTO:
        coalesced_frac: float | None
        if terms_date_col or due_date_col:
            coalesced_frac = _measure_due_date_coverage(
                spark,
                bronze_table=bronze_table,
                terms_date_col=terms_date_col,
                due_date_col=due_date_col,
                cancelled_col=cancelled_col,
                cancelled_kind=cancelled_kind,
                null_invoice_date_policy=null_invoice_date_policy,
            )
        else:
            coalesced_frac = None
        due_date_mode = decide_due_date_mode(
            terms_date_col=terms_date_col,
            due_date_col=due_date_col,
            coalesced_frac=coalesced_frac,
            gate_threshold=real_mode_gate_threshold,
        )

    # gold_table resolution MUST happen after due_date_mode is concrete —
    # see PLAN_P1.5b §4.2. Resolving earlier under the 'auto' sentinel
    # would silently pick the proxy table for high-coverage tenants.
    if gold_table is None:
        gold_table = _default_target(due_date_mode, paths=paths)

    sql = build_ap_aging_sql(
        bronze_table=bronze_table,
        silver_dim=silver_dim,
        gold_table=gold_table,
        due_date_mode=due_date_mode,
        as_of_date_expr=as_of_date_expr,
        null_invoice_date_policy=null_invoice_date_policy,
        terms_date_col=terms_date_col,
        due_date_col=due_date_col,
        cancelled_col=cancelled_col,
        cancelled_kind=cancelled_kind,
        currency_col=currency_col,
        run_id=run_id,
    )
    spark.sql(sql)
    return spark.table(gold_table)


__all__ = [
    "CANCELLED_KIND_DATE",
    "CANCELLED_KIND_FLAG",
    "DEFAULT_REAL_MODE_GATE_THRESHOLD",
    "DUE_DATE_MODE_AUTO",
    "DUE_DATE_MODE_PROXY",
    "DUE_DATE_MODE_REAL",
    "NULL_INVOICE_DATE_POLICY_DROP",
    "NULL_INVOICE_DATE_POLICY_UNKNOWN_BUCKET",
    "SOURCE_BRONZE_TABLE",
    "SOURCE_SILVER_DIM",
    "TARGET_GOLD_TABLE_PROXY",
    "TARGET_GOLD_TABLE_REAL",
    "build",
    "build_ap_aging_sql",
    "decide_due_date_mode",
    "detect_ap_aging_params",
]
