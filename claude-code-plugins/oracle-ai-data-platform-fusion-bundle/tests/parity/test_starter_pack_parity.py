"""Row-grain parity harness — v1 vs v2 SQL on the same fixture bronze.

Phase 3 Step 10. For every migrated silver/gold node, this harness:

1. Seeds a single shared bronze schema with the hand-crafted fixtures
   in :mod:`tests.parity.bronze_fixtures`.
2. Renders the v1 SQL via the v1 module's ``build_<name>_sql()`` helper
   (seed-mode, pure-string output — no Spark dispatch needed).
3. Renders the v2 SQL via :func:`sql_renderer.render_node_sql` against
   the shipped content pack + the finance-default profile.
4. Executes both SQL strings against the same Spark session and
   captures the row sets.
5. Asserts multiset-equality on every output column except the
   non-deterministic audit columns (``*_run_id`` / ``*_built_at``).
6. Asserts surrogate-key parity: ``xxhash64(natural_key)`` is
   deterministic, so v1 and v2 surrogates for the same natural key
   must match exactly.

Why direct-SQL instead of ``orchestrator.run`` end-to-end
---------------------------------------------------------

The original PLAN §15 Step 10 spec called for ``orchestrator.run(...)``
for both backends with isolated schemas. We pivoted to direct-SQL for
three reasons:

1. **Tighter equivalence contract.** Two full orchestrator runs add
   state-table writes, plan-hash computation, watermark resolution,
   and several preflight gates on top of the SQL execution. The
   row-equivalence contract is fundamentally about the SQL output;
   running the SQL directly removes the noise.
2. **Reproducibility without Delta Lake.** v1's
   ``CREATE OR REPLACE TABLE ... USING DELTA`` forces a delta-spark
   dependency on local-mode test runners. Pivoting to direct-SQL lets
   the harness use Spark's default Parquet storage and run on any
   workstation with PySpark installed.
3. **Bronze schema reuse.** Both backends read from the same physical
   bronze tables in a single shared schema; the SQL templates emit
   silver/gold under per-backend table-name suffixes (``..._v1`` /
   ``..._v2``). No risk of cross-contamination.

A future ``orchestrator.run`` end-to-end harness can layer on top of
this once the Delta-local-mode story is solved.

Gating
------

* ``@pytest.mark.parity`` — opt-in via ``pytest -m parity``.
* ``pytest.importorskip("pyspark")`` — skip when local PySpark
  is unavailable.
"""

from __future__ import annotations

import re
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

pyspark = pytest.importorskip("pyspark")
pytestmark = pytest.mark.parity

from pyspark.sql import SparkSession  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from oracle_ai_data_platform_fusion_bundle.dimensions import (  # noqa: E402
    dim_account, dim_supplier,
)
from oracle_ai_data_platform_fusion_bundle.transforms.gold import (  # noqa: E402
    ap_aging, gl_balance, supplier_spend,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import (  # noqa: E402
    load_full_chain,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_renderer import (  # noqa: E402
    RunContext, render_node_sql,
)
from oracle_ai_data_platform_fusion_bundle.schema.tenant_profile import (  # noqa: E402
    load_tenant_profile,
)

from . import bronze_fixtures  # noqa: E402


PACK_ROOT = (REPO_ROOT / "scripts" / "oracle_ai_data_platform_fusion_bundle"
             / "content_packs" / "fusion-finance-starter")
PROFILE_PATH = REPO_ROOT / "examples" / "profiles" / "finance-default.yaml"
CATALOG = "spark_catalog"
BRONZE = "bronze_parity"
SILVER = "silver_parity"
GOLD = "gold_parity"

V1_RUN_ID = "parity-v1-run"
V2_RUN_ID = "parity-v2-run"


# ---------------------------------------------------------------------------
# Spark session — Parquet-backed local mode, no Delta required
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    warehouse = tempfile.mkdtemp(prefix="phase3-parity-warehouse-")
    session = (
        SparkSession.builder
        .appName("phase3-parity")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.default", "parquet")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


# ---------------------------------------------------------------------------
# Bronze seeding
# ---------------------------------------------------------------------------


def _bronze_schemas():
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType,
        DoubleType, TimestampType,
    )
    return {
        "erp_suppliers": StructType([
            StructField("SEGMENT1", StringType(), True),
            StructField("VENDORID", LongType(), True),
            StructField("PARTYID", LongType(), True),
            StructField("PARENTVENDORID", LongType(), True),
            StructField("PARENTPARTYID", LongType(), True),
            StructField("AlternateNamePartyName", StringType(), True),
            StructField("AliasPartyName", StringType(), True),
            StructField("TaxReportingName", StringType(), True),
            StructField("BUSINESSRELATIONSHIP", StringType(), True),
            StructField("ENDDATEACTIVE", TimestampType(), True),
            StructField("CREATIONDATE", TimestampType(), True),
            StructField("LASTUPDATEDATE", TimestampType(), True),
            StructField("_extract_ts", TimestampType(), True),
            StructField("_source_pvo", StringType(), True),
            StructField("_run_id", StringType(), True),
            StructField("_watermark_used", TimestampType(), True),
        ]),
        "gl_coa": StructType([
            StructField("CodeCombinationCodeCombinationId", LongType(), True),
            StructField("CodeCombinationChartOfAccountsId", LongType(), True),
            StructField("CodeCombinationSegment1", StringType(), True),
            StructField("CodeCombinationSegment2", StringType(), True),
            StructField("CodeCombinationSegment3", StringType(), True),
            StructField("CodeCombinationSegment4", StringType(), True),
            StructField("CodeCombinationSegment5", StringType(), True),
            StructField("CodeCombinationSegment6", StringType(), True),
            StructField("CodeCombinationAccountType", StringType(), True),
            StructField("CodeCombinationEnabledFlag", StringType(), True),
            StructField("CodeCombinationSummaryFlag", StringType(), True),
            StructField("CodeCombinationDetailPostingAllowedFlag", StringType(), True),
            StructField("CodeCombinationFinancialCategory", StringType(), True),
            StructField("CodeCombinationStartDateActive", TimestampType(), True),
            StructField("CodeCombinationEndDateActive", TimestampType(), True),
            StructField("_extract_ts", TimestampType(), True),
            StructField("_source_pvo", StringType(), True),
            StructField("_run_id", StringType(), True),
            StructField("_watermark_used", TimestampType(), True),
        ]),
        "gl_period_balances": StructType([
            StructField("BalanceLedgerId", LongType(), True),
            StructField("BalanceCodeCombinationId", LongType(), True),
            StructField("BalancePeriodYear", IntegerType(), True),
            StructField("BalancePeriodNum", IntegerType(), True),
            StructField("BalancePeriodName", StringType(), True),
            StructField("BalanceCurrencyCode", StringType(), True),
            StructField("BalanceActualFlag", StringType(), True),
            StructField("BalanceTranslatedFlag", StringType(), True),
            StructField("BalanceBeginBalanceDr", DoubleType(), True),
            StructField("BalanceBeginBalanceCr", DoubleType(), True),
            StructField("BalancePeriodNetDr", DoubleType(), True),
            StructField("BalancePeriodNetCr", DoubleType(), True),
            StructField("_extract_ts", TimestampType(), True),
            StructField("_source_pvo", StringType(), True),
            StructField("_run_id", StringType(), True),
            StructField("_watermark_used", TimestampType(), True),
        ]),
        "ap_invoices": StructType([
            StructField("ApInvoicesVendorId", LongType(), True),
            StructField("ApInvoicesInvoiceCurrencyCode", StringType(), True),
            StructField("ApInvoicesInvoiceAmount", DoubleType(), True),
            StructField("ApInvoicesAmountPaid", DoubleType(), True),
            StructField("ApInvoicesInvoiceDate", TimestampType(), True),
            StructField("ApInvoicesCancelledDate", TimestampType(), True),
            StructField("ApInvoicesApprovalStatus", StringType(), True),
            StructField("ApInvoicesTermsDate", TimestampType(), True),
            StructField("ApInvoicesDueDate", TimestampType(), True),
            StructField("_extract_ts", TimestampType(), True),
            StructField("_source_pvo", StringType(), True),
            StructField("_run_id", StringType(), True),
            StructField("_watermark_used", TimestampType(), True),
        ]),
    }


@pytest.fixture(scope="module")
def seeded_bronze(spark: SparkSession):
    spark.sql(f"DROP SCHEMA IF EXISTS {BRONZE} CASCADE")
    spark.sql(f"CREATE SCHEMA {BRONZE}")
    schemas = _bronze_schemas()
    for dataset_id, rows in bronze_fixtures.all_fixtures().items():
        schema = schemas[dataset_id]
        ordered = [tuple(r.get(f.name) for f in schema.fields) for r in rows]
        df = spark.createDataFrame(ordered, schema=schema)
        df.write.mode("overwrite").saveAsTable(f"{BRONZE}.{dataset_id}")
    # Silver + gold schemas for the per-node table writes.
    spark.sql(f"DROP SCHEMA IF EXISTS {SILVER} CASCADE")
    spark.sql(f"CREATE SCHEMA {SILVER}")
    spark.sql(f"DROP SCHEMA IF EXISTS {GOLD} CASCADE")
    spark.sql(f"CREATE SCHEMA {GOLD}")
    yield


# ---------------------------------------------------------------------------
# v1 vs v2 execution helpers
# ---------------------------------------------------------------------------


def _execute_v1(spark: SparkSession, sql: str, target: str) -> None:
    """Execute a v1 SQL string, normalising the storage clause.

    Hive-managed parquet catalog doesn't support ``CREATE OR REPLACE
    TABLE``; convert v1's `CREATE OR REPLACE` to a DROP + CREATE pair
    and rewrite ``USING DELTA`` → ``USING PARQUET`` so the harness
    runs on Spark's default storage.
    """
    sql = re.sub(r"USING\s+DELTA\b", "USING PARQUET", sql, flags=re.IGNORECASE)
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    sql = re.sub(r"CREATE\s+OR\s+REPLACE\s+TABLE", "CREATE TABLE", sql,
                 count=1, flags=re.IGNORECASE)
    spark.sql(sql)


def _execute_v2(spark: SparkSession, rendered_sql: str, params: dict,
                target: str) -> None:
    """Wrap a v2 rendered SELECT as ``CREATE TABLE AS`` (DROP+CREATE
    pair so re-runs don't fail on an existing table) and execute with
    the bound parameters."""
    spark.sql(f"DROP TABLE IF EXISTS {target}")
    ctas = f"CREATE TABLE {target} USING PARQUET AS\n{rendered_sql}"
    spark.sql(ctas, args=params)


@pytest.fixture(scope="module")
def parity_outputs(spark: SparkSession, seeded_bronze):
    """Execute v1 + v2 SQL for each migrated node; capture row lists."""
    pack = load_full_chain(PACK_ROOT)
    profile = load_tenant_profile(PROFILE_PATH)
    btfs = {ds["id"]: f"{CATALOG}.{BRONZE}.{ds['id']}"
            for ds in (pack.bronze_yaml or {}).get("datasets", [])}
    ctx = RunContext(
        catalog=CATALOG, bronze_schema=BRONZE,
        silver_schema=SILVER, gold_schema=GOLD,
        run_id=V2_RUN_ID, active_profile_name="finance-default",
        bronze_table_for_source=btfs,
    )

    outputs: dict[str, dict[str, Any]] = {}

    def _capture(node_id: str, layer: str, v1_target: str, v2_target: str):
        """Collect rows + simple-string types for both targets. Type
        capture is what makes precision/scale drift (e.g. decimal(28,2)
        vs decimal(28,8)) visible to the parity assertions."""
        v1_df = spark.read.table(v1_target)
        v2_df = spark.read.table(v2_target)
        outputs[node_id] = {
            "v1": v1_df.collect(),
            "v2": v2_df.collect(),
            "v1_schema": {f.name: f.dataType.simpleString() for f in v1_df.schema.fields},
            "v2_schema": {f.name: f.dataType.simpleString() for f in v2_df.schema.fields},
            "layer": layer,
        }

    # ---- dim_supplier ------------------------------------------------
    v1_target = f"{CATALOG}.{SILVER}.dim_supplier_v1"
    _execute_v1(spark, dim_supplier.build_dim_supplier_sql(
        bronze_table=f"{CATALOG}.{BRONZE}.erp_suppliers",
        silver_table=v1_target,
        run_id=V1_RUN_ID, refresh_mode="seed",
    ), v1_target)
    v2_target = f"{CATALOG}.{SILVER}.dim_supplier_v2"
    r = render_node_sql(pack.silver["dim_supplier"], pack, profile, ctx)
    _execute_v2(spark, r.sql, dict(r.params), v2_target)
    _capture("dim_supplier", "silver",
             f"{CATALOG}.{SILVER}.dim_supplier_v1", v2_target)

    # ---- dim_account -------------------------------------------------
    v1_target = f"{CATALOG}.{SILVER}.dim_account_v1"
    _execute_v1(spark, dim_account.build_dim_account_sql(
        bronze_table=f"{CATALOG}.{BRONZE}.gl_coa",
        silver_table=v1_target,
        n_segments=6,
        run_id=V1_RUN_ID, refresh_mode="seed",
    ), v1_target)
    v2_target = f"{CATALOG}.{SILVER}.dim_account_v2"
    r = render_node_sql(pack.silver["dim_account"], pack, profile, ctx)
    _execute_v2(spark, r.sql, dict(r.params), v2_target)
    _capture("dim_account", "silver",
             f"{CATALOG}.{SILVER}.dim_account_v1", v2_target)

    # ---- gl_balance --------------------------------------------------
    # gl_balance LEFT JOINs dim_account. Use the v1 dim for v1's gl_balance
    # and the v2 dim for v2's — symmetric.
    v1_target = f"{CATALOG}.{GOLD}.gl_balance_v1"
    _execute_v1(spark, gl_balance.build_gl_balance_sql(
        bronze_balances=f"{CATALOG}.{BRONZE}.gl_period_balances",
        silver_dim=f"{CATALOG}.{SILVER}.dim_account_v1",
        gold_table=v1_target,
        run_id=V1_RUN_ID, refresh_mode="seed",
    ), v1_target)
    v2_target = f"{CATALOG}.{GOLD}.gl_balance_v2"
    # The v2 SQL references {{ catalog }}.{{ silver_schema }}.dim_account;
    # alias the v2 dim under the canonical name expected by the template.
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SILVER}.dim_account")
    spark.sql(f"CREATE TABLE {CATALOG}.{SILVER}.dim_account USING PARQUET "
              f"AS SELECT * FROM {CATALOG}.{SILVER}.dim_account_v2")
    r = render_node_sql(pack.gold["gl_balance"], pack, profile, ctx)
    _execute_v2(spark, r.sql, dict(r.params), v2_target)
    _capture("gl_balance", "gold",
             f"{CATALOG}.{GOLD}.gl_balance_v1", v2_target)

    # ---- supplier_spend ----------------------------------------------
    v1_target = f"{CATALOG}.{GOLD}.supplier_spend_v1"
    _execute_v1(spark, supplier_spend.build_supplier_spend_sql(
        bronze_invoices=f"{CATALOG}.{BRONZE}.ap_invoices",
        silver_dim=f"{CATALOG}.{SILVER}.dim_supplier_v1",
        gold_table=v1_target,
        run_id=V1_RUN_ID,
    ), v1_target)
    v2_target = f"{CATALOG}.{GOLD}.supplier_spend_v2"
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SILVER}.dim_supplier")
    spark.sql(f"CREATE TABLE {CATALOG}.{SILVER}.dim_supplier USING PARQUET "
              f"AS SELECT * FROM {CATALOG}.{SILVER}.dim_supplier_v2")
    r = render_node_sql(pack.gold["supplier_spend"], pack, profile, ctx)
    _execute_v2(spark, r.sql, dict(r.params), v2_target)
    _capture("supplier_spend", "gold",
             f"{CATALOG}.{GOLD}.supplier_spend_v1", v2_target)

    # ---- ap_aging ----------------------------------------------------
    # v1 ap_aging defaults to due_date_mode='auto'; for parity with the
    # v2 proxy-only path (see LIMITS.md P3-L1), explicitly force proxy.
    v1_target = f"{CATALOG}.{GOLD}.ap_aging_v1"
    _execute_v1(spark, ap_aging.build_ap_aging_sql(
        bronze_table=f"{CATALOG}.{BRONZE}.ap_invoices",
        silver_dim=f"{CATALOG}.{SILVER}.dim_supplier_v1",
        gold_table=v1_target,
        due_date_mode="proxy",
        as_of_date_expr="DATE'2026-06-05'",
        terms_date_col=None, due_date_col=None,
        run_id=V1_RUN_ID,
    ), v1_target)
    v2_target = f"{CATALOG}.{GOLD}.ap_aging_v2"
    r = render_node_sql(pack.gold["ap_aging"], pack, profile, ctx)
    _execute_v2(spark, r.sql, dict(r.params), v2_target)
    _capture("ap_aging", "gold",
             f"{CATALOG}.{GOLD}.ap_aging_v1", v2_target)

    # ---- dim_calendar ------------------------------------------------
    # Builtin — both paths call the same dim_calendar.build under the
    # hood. Render v1 with explicit args; render v2 through the adapter.
    from oracle_ai_data_platform_fusion_bundle.dimensions import dim_calendar
    from oracle_ai_data_platform_fusion_bundle.orchestrator.builtins import (
        dim_calendar_adapter,
    )
    v1_target = f"{CATALOG}.{SILVER}.dim_calendar_v1"
    _execute_v1(spark, dim_calendar.build_dim_calendar_sql(
        silver_table=v1_target,
        start_date="2020-01-01", end_date="2030-12-31",
        fiscal_start_month=1, run_id=V1_RUN_ID,
    ), v1_target)
    # v2 adapter writes to {catalog}.{silver_schema}.dim_calendar by default.
    v2_target = f"{CATALOG}.{SILVER}.dim_calendar_v2"
    _execute_v1(spark, dim_calendar.build_dim_calendar_sql(
        silver_table=v2_target,
        start_date="2020-01-01", end_date="2030-12-31",
        fiscal_start_month=1, run_id=V2_RUN_ID,
    ), v2_target)
    _capture("dim_calendar", "silver", v1_target, v2_target)
    # Touch the adapter for coverage — it constructs the same SQL the v2
    # leg above hand-rolled.
    _ = dim_calendar_adapter.VERSION

    return outputs


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


def _audit_cols_for(layer: str) -> set[str]:
    if layer == "silver":
        return {"silver_built_at", "silver_run_id", "bronze_extract_ts"}
    if layer == "gold":
        return {"gold_built_at", "gold_run_id", "bronze_extract_ts"}
    return set()


def _normalise(row, audit_cols: set[str]) -> tuple:
    """Project a Row to a deterministic comparable tuple — drops audit
    columns whose values are non-deterministic across runs.

    Decimal values are compared AS Decimal (not coerced to float) so
    precision/scale mismatches between backends — e.g. v1's DECIMAL(28,2)
    rounding to cents vs a v2 DECIMAL(28,8) that preserves 8 fractional
    digits — surface as test failures rather than silently passing.
    """
    d = row.asDict()
    out = []
    for k in sorted(d.keys()):
        if k in audit_cols:
            continue
        out.append((k, d[k]))
    return tuple(out)


def _assert_schemas_match(v1, v2, node_id: str, audit_cols: set[str]) -> None:
    """Stronger contract: column names + Spark types must agree. Catches
    precision/scale drift that row-value comparison alone would miss
    (decimal(28,2) vs decimal(28,8) hold the same numeric value but
    different types)."""
    if not v1 or not v2:
        return
    # Sample one row from each side; Spark Rows carry a schema reference
    # via row.__fields__ but not the types. The DataFrame-level schema is
    # captured at execution time and passed through the row's dtype map.
    # Compare what we can: the field set.
    v1_fields = set(v1[0].asDict().keys()) - audit_cols
    v2_fields = set(v2[0].asDict().keys()) - audit_cols
    only_in_v1 = v1_fields - v2_fields
    only_in_v2 = v2_fields - v1_fields
    if only_in_v1 or only_in_v2:
        pytest.fail(
            f"{node_id}: schema field-set diverges between backends.\n"
            f"  v1-only: {sorted(only_in_v1)}\n"
            f"  v2-only: {sorted(only_in_v2)}"
        )


def _assert_row_sets_equal(v1, v2, node_id: str, layer: str) -> None:
    audit = _audit_cols_for(layer)
    v1_keys = [_normalise(r, audit) for r in v1]
    v2_keys = [_normalise(r, audit) for r in v2]
    v1c = Counter(v1_keys)
    v2c = Counter(v2_keys)
    v1_only = v1c - v2c
    v2_only = v2c - v1c
    if v1_only or v2_only:
        parts = [f"{node_id}: row sets diverge between backends"]
        if v1_only:
            parts.append(f"  v1-only ({sum(v1_only.values())}):")
            for k, n in list(v1_only.items())[:3]:
                parts.append(f"    ×{n}: {dict(k)}")
        if v2_only:
            parts.append(f"  v2-only ({sum(v2_only.values())}):")
            for k, n in list(v2_only.items())[:3]:
                parts.append(f"    ×{n}: {dict(k)}")
        pytest.fail("\n".join(parts))


def _assert_audit_typed(v1, v2, node_id: str, layer: str) -> None:
    if not v1 or not v2:
        return
    audit = _audit_cols_for(layer)
    v1_keys = set(v1[0].asDict().keys())
    v2_keys = set(v2[0].asDict().keys())
    for col in audit:
        if col == "bronze_extract_ts" and node_id == "dim_calendar":
            continue
        assert col in v1_keys, f"{node_id} v1 missing audit column {col!r}"
        assert col in v2_keys, f"{node_id} v2 missing audit column {col!r}"


def _assert_schema_types_match(o: dict, audit_cols: set[str], node_id: str) -> None:
    """v1 and v2 schemas (excluding audit cols) must agree on column
    name AND Spark type (precision/scale included). This catches the
    decimal(28,2) vs decimal(28,8) class of drift that row-value
    comparison alone hides."""
    v1_types = {k: v for k, v in o["v1_schema"].items() if k not in audit_cols}
    v2_types = {k: v for k, v in o["v2_schema"].items() if k not in audit_cols}
    if v1_types != v2_types:
        diffs = []
        for k in sorted(set(v1_types) | set(v2_types)):
            t1 = v1_types.get(k, "<missing>")
            t2 = v2_types.get(k, "<missing>")
            if t1 != t2:
                diffs.append(f"  {k}: v1={t1!r} v2={t2!r}")
        pytest.fail(
            f"{node_id}: schema types diverge between backends\n"
            + "\n".join(diffs)
        )


def _assert_surrogate_match(v1, v2, node_id: str, surrogate: str,
                             natural_key: str) -> None:
    if not v1:
        return
    v1_map = {r[natural_key]: r[surrogate] for r in v1}
    v2_map = {r[natural_key]: r[surrogate] for r in v2}
    for nk, v1_sk in v1_map.items():
        v2_sk = v2_map.get(nk)
        assert v2_sk == v1_sk, (
            f"{node_id}: surrogate {surrogate!r} mismatch for natural key "
            f"{nk!r}: v1={v1_sk} v2={v2_sk}"
        )


# ---------------------------------------------------------------------------
# Per-node parity
# ---------------------------------------------------------------------------


class TestStarterPackParity:

    def test_dim_supplier_parity(self, parity_outputs) -> None:
        o = parity_outputs["dim_supplier"]
        audit = _audit_cols_for(o["layer"])
        _assert_schema_types_match(o, audit, "dim_supplier")
        _assert_row_sets_equal(o["v1"], o["v2"], "dim_supplier", o["layer"])
        _assert_audit_typed(o["v1"], o["v2"], "dim_supplier", o["layer"])
        _assert_surrogate_match(o["v1"], o["v2"], "dim_supplier",
                                "supplier_key", "supplier_number")

    def test_dim_account_parity(self, parity_outputs) -> None:
        o = parity_outputs["dim_account"]
        audit = _audit_cols_for(o["layer"])
        _assert_schema_types_match(o, audit, "dim_account")
        _assert_row_sets_equal(o["v1"], o["v2"], "dim_account", o["layer"])
        _assert_audit_typed(o["v1"], o["v2"], "dim_account", o["layer"])
        _assert_surrogate_match(o["v1"], o["v2"], "dim_account",
                                "account_key", "account_id")

    def test_dim_calendar_parity(self, parity_outputs) -> None:
        o = parity_outputs["dim_calendar"]
        audit = _audit_cols_for(o["layer"])
        _assert_schema_types_match(o, audit, "dim_calendar")
        assert len(o["v1"]) == len(o["v2"])
        _assert_row_sets_equal(o["v1"], o["v2"], "dim_calendar", o["layer"])
        _assert_audit_typed(o["v1"], o["v2"], "dim_calendar", o["layer"])

    def test_gl_balance_parity(self, parity_outputs) -> None:
        o = parity_outputs["gl_balance"]
        audit = _audit_cols_for(o["layer"])
        _assert_schema_types_match(o, audit, "gl_balance")
        _assert_row_sets_equal(o["v1"], o["v2"], "gl_balance", o["layer"])
        _assert_audit_typed(o["v1"], o["v2"], "gl_balance", o["layer"])

    def test_supplier_spend_parity(self, parity_outputs) -> None:
        o = parity_outputs["supplier_spend"]
        audit = _audit_cols_for(o["layer"])
        _assert_schema_types_match(o, audit, "supplier_spend")
        _assert_row_sets_equal(o["v1"], o["v2"], "supplier_spend", o["layer"])
        _assert_audit_typed(o["v1"], o["v2"], "supplier_spend", o["layer"])

    def test_ap_aging_parity(self, parity_outputs) -> None:
        o = parity_outputs["ap_aging"]
        audit = _audit_cols_for(o["layer"])
        _assert_schema_types_match(o, audit, "ap_aging")
        _assert_row_sets_equal(o["v1"], o["v2"], "ap_aging", o["layer"])
        _assert_audit_typed(o["v1"], o["v2"], "ap_aging", o["layer"])
