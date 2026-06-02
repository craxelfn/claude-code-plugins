"""silver.dim_supplier — conformed supplier dimension.

Reads ``bronze.erp_suppliers`` (BICC ``SupplierExtractPVO``), dedupes on the
natural key ``SEGMENT1`` (supplier_number), keeps the most recent extract per
supplier, projects 14 columns into ``silver.dim_supplier`` with bronze→silver
audit lineage.

Design notes
------------

* **Column case** — bronze columns from the BICC ``aidataplatform`` connector
  for ``SupplierExtractPVO`` are all-uppercase (``SEGMENT1``, ``VENDORID``,
  ``BUSINESSRELATIONSHIP``, …). Confirmed live on ``fusion_bundle_dev`` cluster
  (2026-05-07) and via direct CSV read of the 2026-04-30 etap-dev5 extract
  (sha256-12=c7b6c705c751). Other PVOs may differ — ``ap_invoices`` uses
  PascalCase with an ``ApInvoices`` prefix; that's P1.2's concern.

* **NULLIF on ID columns** — demo pods routinely return ``0`` for missing IDs.
  ``NULLIF(CAST(... AS BIGINT), 0)`` ensures ``NULL`` is the only "absent"
  signal, which makes :func:`id_populated_pct` honest.

* **Per-pod data shape varies** — etap-dev5 has ``VENDORID`` 100% populated;
  eseb-test has it 0%. The dim accepts both; the consumer (P1.2's
  ``gold.supplier_spend``) calls :func:`id_populated_pct` to decide between
  the canonical join form and a spend-only fallback.

* **Surrogate key strategy** — ``monotonically_increasing_id()`` for now.
  Downstream marts MUST join on ``supplier_number``, never on the surrogate.
  If a future mart needs stability across rebuilds, swap to
  ``xxhash64(supplier_number)`` non-breakingly.

* **Supplier-name column is sparse on demo pods** — eseb-test live probe shows
  no single name column is 100%-populated. We COALESCE through the most-likely
  party-name fields; production pods typically populate at least one cleanly.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Final, Literal

from oracle_ai_data_platform_fusion_bundle.config.paths import DEFAULT_PATHS, TablePaths

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


SOURCE_BRONZE_TABLE: Final[str] = DEFAULT_PATHS.bronze("erp_suppliers")
TARGET_SILVER_TABLE: Final[str] = DEFAULT_PATHS.silver("dim_supplier")


def _run_id_audit_sql(run_id: str | None) -> str:
    """SQL fragment for the layer-specific run_id audit column (§3.5a, B3).

    When the orchestrator threads its run_id, emit a single-quoted literal
    (UUID4 — safe per the catalog identifier regex). When None (standalone
    notebook / unit-test invocation), emit NULL. The column lets gold rows
    join back to ``fusion_bundle_state.run_id`` for SOX trail.
    """
    if run_id is None:
        return "NULL"
    # UUID4s are alphanumeric+hyphens only (safe for SQL literal interpolation).
    # Defensive single-quote-doubling in case a non-orchestrator caller passes
    # something unusual.
    escaped = run_id.replace("'", "''")
    return f"'{escaped}'"


#: Projected column name (silver-side) used as the MERGE ON predicate
#: under ``--mode incremental``. Matches ``SilverDimSpec.natural_key`` in
#: the orchestrator registry; pinned here so the module and the spec
#: cannot drift.
NATURAL_KEY_COLUMN: Final[str] = "supplier_number"


def _projection_select_sql(bronze_table: str, run_id: str | None) -> str:
    """The SELECT projection shared by seed + incremental SQL renderers.

    Emits the 14-column silver-side projection from the deduped bronze
    subquery. The dedupe rule (ROW_NUMBER over SEGMENT1) keeps the
    most-recent ``_extract_ts`` per supplier; NULL ``SEGMENT1`` rows
    are filtered (real DQ issue if seen).

    P1.19 — ``supplier_key`` uses ``xxhash64(SEGMENT1)`` for surrogate-key
    stability across MERGE refreshes; pre-P1.19 used
    ``monotonically_increasing_id()`` which was partition-local +
    non-deterministic and would silently invalidate any downstream
    cache keyed on it after the first incremental MERGE.
    """
    run_id_sql = _run_id_audit_sql(run_id)
    return f"""\
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
  {run_id_sql}                                                     AS silver_run_id
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY SEGMENT1 ORDER BY _extract_ts DESC) AS _rn
  FROM {bronze_table}
  WHERE SEGMENT1 IS NOT NULL
{{watermark_predicate}}
)
WHERE _rn = 1"""


def build_dim_supplier_sql(
    *,
    paths:        TablePaths | None = None,
    bronze_table: str | None = None,
    silver_table: str | None = None,
    run_id:       str | None = None,
    refresh_mode: Literal["seed", "incremental"] = "seed",
    watermark:    datetime | None = None,
    target_only_columns: tuple[str, ...] = (),
    source_columns: tuple[str, ...] | None = None,
) -> str:
    """Return the SQL that produces ``silver.dim_supplier`` in the requested mode.

    Pure string output — no Spark required. Used by unit tests to verify
    the projection shape; called by :func:`build` to materialize the table.

    ``refresh_mode`` (P1.17):
      * ``"seed"`` (default) — ``CREATE OR REPLACE TABLE`` Delta SQL,
        identical pre-P1.17 shape. The dedupe rule keeps the row with
        the most-recent ``_extract_ts`` per ``SEGMENT1``.
      * ``"incremental"`` — ``MERGE INTO ... ON supplier_number <=>
        src.supplier_number WHEN MATCHED UPDATE SET * WHEN NOT MATCHED
        INSERT *``. The source-side ``WHERE _extract_ts > <watermark>``
        predicate filters bronze rows the silver layer has already
        incorporated (the layer-local cursor — NOT the upstream bronze
        windowed cursor — per B8a).

    ``watermark`` (P1.17 incremental only): UTC ``datetime`` of the
    layer-local prior ``last_watermark`` read from
    ``fusion_bundle_state``. ``None`` is accepted only in ``"seed"``
    mode; passing ``None`` in incremental mode raises (the orchestrator's
    ``_preflight_incremental_cursors`` should have caught this).

    Resolution order for table paths: explicit ``bronze_table`` / ``silver_table``
    kwargs win, else ``paths.bronze("erp_suppliers")`` / ``paths.silver("dim_supplier")``
    when ``paths`` is set, else ``DEFAULT_PATHS``.

    ``run_id`` (§3.5a B3) — when set by the orchestrator, embedded as
    the ``silver_run_id`` audit column literal.
    """
    if paths is None:
        paths = DEFAULT_PATHS
    if bronze_table is None:
        bronze_table = paths.bronze("erp_suppliers")
    if silver_table is None:
        silver_table = paths.silver("dim_supplier")

    if refresh_mode == "seed":
        # Seed-mode predicate is empty — no watermark filter.
        select_sql = _projection_select_sql(bronze_table, run_id).format(
            watermark_predicate=""
        )
        return f"CREATE OR REPLACE TABLE {silver_table}\nUSING DELTA\nAS\n{select_sql}\n"

    if refresh_mode == "incremental":
        if watermark is None:
            raise ValueError(
                "dim_supplier.build_dim_supplier_sql: refresh_mode='incremental' "
                "requires a non-None watermark (the layer-local prior cursor). "
                "The orchestrator's _preflight_incremental_cursors should have "
                "raised IncrementalCursorMissingError before reaching this path."
            )
        watermark_iso = watermark.astimezone(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
        select_sql = _projection_select_sql(bronze_table, run_id).format(
            watermark_predicate=f"    AND _extract_ts > '{watermark_iso}'"
        )
        # P1.17d — render explicit-column-list MERGE when target has
        # columns the source lacks; preserves target-only columns by
        # omitting them from UPDATE/INSERT lists. Otherwise V1 shape.
        if target_only_columns:
            if source_columns is None:
                raise ValueError(
                    "build_dim_supplier_sql: when target_only_columns is "
                    "non-empty (explicit-column-list MERGE shape), the "
                    "source_columns kwarg MUST be provided (the list of "
                    "columns the source DataFrame emits). The orchestrator's "
                    "build() supplies this from spark.sql(<select>).schema."
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
            f"MERGE INTO {silver_table} AS target\n"
            f"USING (\n{select_sql}\n) AS src\n"
            f"ON target.{NATURAL_KEY_COLUMN} <=> src.{NATURAL_KEY_COLUMN}\n"
            f"{when_matched_clause}\n"
            f"{when_not_matched_clause}\n"
        )

    raise ValueError(
        f"dim_supplier.build_dim_supplier_sql: refresh_mode must be 'seed' or "
        f"'incremental'; got {refresh_mode!r}"
    )


def build(
    spark: SparkSession,
    *,
    paths:        TablePaths | None = None,
    bronze_table: str | None = None,
    silver_table: str | None = None,
    run_id:       str | None = None,
    refresh_mode: Literal["seed", "incremental"] = "seed",
    watermark:    datetime | None = None,
) -> DataFrame:
    """Materialize ``silver.dim_supplier`` from ``bronze.erp_suppliers``.

    Runs the SQL from :func:`build_dim_supplier_sql` against ``spark`` and
    returns a DataFrame backed by the freshly-written silver table.

    ``refresh_mode`` (P1.17) selects ``"seed"`` (``CREATE OR REPLACE``) or
    ``"incremental"`` (``MERGE INTO``). Incremental requires ``watermark``
    (the layer-local prior cursor — see :func:`build_dim_supplier_sql`).

    ``paths`` (defaults to ``DEFAULT_PATHS``) resolves the bronze/silver
    table identifiers from the tenant's ``bundle.yaml.aidp.*`` config.
    Explicit per-table kwargs win over ``paths``.
    """
    if paths is None:
        paths = DEFAULT_PATHS
    if bronze_table is None:
        bronze_table = paths.bronze("erp_suppliers")
    if silver_table is None:
        silver_table = paths.silver("dim_supplier")

    # P1.17d — under incremental, reconcile target schema with source
    # projection BEFORE the MERGE. Skips under seed (CREATE OR REPLACE
    # handles drift natively via overwriteSchema=true).
    target_only_columns: tuple[str, ...] = ()
    source_columns: tuple[str, ...] | None = None
    if refresh_mode == "incremental":
        # P1.17d v5 — VALIDATE INCREMENTAL PRECONDITIONS BEFORE running
        # _ensure_target_schema_for_merge (which can emit ALTER TABLE
        # ADD COLUMNS on the production target). Without this guard,
        # a debug call like `build(refresh_mode='incremental',
        # watermark=None)` would (1) introspect the source schema,
        # (2) ALTER the target if source-wider, (3) only THEN raise
        # the missing-watermark ValueError from build_dim_supplier_sql
        # — leaving the target half-mutated. The same check lives in
        # build_dim_supplier_sql for direct callers; duplicating it
        # here is the cheapest fail-fast guard at the dispatch boundary.
        if watermark is None:
            raise ValueError(
                "dim_supplier.build: refresh_mode='incremental' "
                "requires a non-None watermark (the layer-local prior "
                "cursor). The orchestrator's _preflight_incremental_"
                "cursors should have raised IncrementalCursorMissing"
                "Error before reaching this path."
            )
        # Lazy local imports — avoid circular at module-load time
        # (orchestrator.state is independent of the dimensions package,
        # but the orchestrator package itself imports registry which
        # imports this module; staying lazy is symmetric with the
        # SchemaEvolutionTypeConflictError lazy import in state.py).
        from oracle_ai_data_platform_fusion_bundle.orchestrator import state as _state

        # Don't apply the watermark filter here — schema is the same
        # with or without it; we just need the projected column list.
        inner_select = _projection_select_sql(bronze_table, run_id).format(
            watermark_predicate=""
        )
        source_schema = spark.sql(
            f"SELECT * FROM ({inner_select}) WHERE 1=0"
        ).schema
        reconcile = _state._ensure_target_schema_for_merge(
            spark, silver_table, source_schema.names, source_schema,
        )
        if reconcile.target_only_columns:
            target_only_columns = reconcile.target_only_columns
            # The explicit-list MERGE renders over the source projection
            # (the post-ALTER target carries all source cols + the
            # target-only cols; UPDATE/INSERT lists name only the source
            # cols, preserving target-only cols on UPDATE and leaving
            # them NULL/default on INSERT).
            source_columns = tuple(source_schema.names)

    spark.sql(build_dim_supplier_sql(
        bronze_table=bronze_table,
        silver_table=silver_table,
        run_id=run_id,
        refresh_mode=refresh_mode,
        watermark=watermark,
        target_only_columns=target_only_columns,
        source_columns=source_columns,
    ))
    return spark.table(silver_table)


def id_populated_pct(
    spark: SparkSession,
    *,
    silver_table: str = TARGET_SILVER_TABLE,
    column: str = "vendor_id",
) -> float:
    """Return the fraction (0.0-1.0) of rows where ``column`` IS NOT NULL.

    Used by P1.2's ``gold.supplier_spend`` to decide between the canonical
    join-form (``vendor_id`` populated) and a spend-only fallback (``vendor_id``
    NULL on demo pods like eseb-test). Threshold convention: ``>= 0.5`` → join.
    """
    row = spark.sql(
        f"SELECT "
        f"  CAST(SUM(CASE WHEN {column} IS NOT NULL THEN 1 ELSE 0 END) AS DOUBLE) "
        f"/ NULLIF(COUNT(*), 0) AS pct "
        f"FROM {silver_table}"
    ).collect()
    if not row:
        return 0.0
    pct = row[0]["pct"]
    return float(pct) if pct is not None else 0.0


__all__ = [
    "NATURAL_KEY_COLUMN",
    "SOURCE_BRONZE_TABLE",
    "TARGET_SILVER_TABLE",
    "build",
    "build_dim_supplier_sql",
    "id_populated_pct",
]
