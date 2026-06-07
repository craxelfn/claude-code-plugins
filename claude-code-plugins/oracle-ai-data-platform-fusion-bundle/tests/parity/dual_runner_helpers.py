"""Shared helpers for the Phase 4 dual-runner parity harness.

Used by:
- ``tests/parity/test_dual_runner_e2e.py`` (Steps 2, 3, 4, 5, 7a)
- ``tests/parity/test_dual_runner_profiles.py`` (Step 6 — multi-tenant)
- ``tests/parity/test_concurrent_runs.py`` (Step 9 — concurrency precheck)

The helpers stay separate from the test file so they can be unit-tested
in isolation and reused across the three Phase 4 test files without
circular-import gymnastics.

Three core contracts the helpers encode (per ``plan.md`` Step 2):

1. **Bundle isolation via bronze schemas.** State-table location follows
   ``paths.bronze("fusion_bundle_state")``; per-backend isolation =
   distinct ``aidp.bronzeSchema`` per bundle YAML.
2. **Tiered state-row contract.** Tier A common semantic fields with
   failure-path ``row_count`` normalization; Tier B watermark
   cross-shape; Tier C v2-only fields. Excluded:
   ``plan_hash`` / ``last_run_at`` / ``run_id`` / ``duration_seconds``.
3. **No BICC IO during silver+gold runs.** Spies the
   ``extractors.bicc.extract_pvo`` entrypoint and asserts call count is
   zero — catches a future refactor that re-enables bronze extraction
   in a silver-only run.
"""

from __future__ import annotations

import shutil
import sys
import textwrap
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

# Repo root resolution so test files can import without sys.path hacks
# of their own.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))


# ---------------------------------------------------------------------------
# Failure-class statuses (Tier A normalization)
# ---------------------------------------------------------------------------

# Used by ``_normalize_row_count``: any status in this set is failure /
# skip / drift; v1 carries ``row_count=None`` on those rows while v2
# carries ``row_count=0``. Normalize before equality.
FAILURE_CLASS_STATUSES = frozenset({
    "failed",
    "skipped",
    "skipped_aborted",
    "deferred",
    "resumed_skipped",
    "preflight_blocked",
    "render_failed",
    "resume_drift_blocked",
    "quality_failed",
    "output_schema_drift",
    "state_commit_failed",
    "strategy_failed",
})


# Status pairs the harness accepts as semantically equivalent even
# though the raw enum value differs. Used by ``_assert_state_rows_equiv``
# for the persisted ``status`` column. v1 collapses every failure into
# ``'failed'``; v2 keeps the specific persisted code.
PERSISTED_STATUS_EQUIVALENCE = {
    ("failed", "output_schema_drift"),
    ("failed", "resume_drift_blocked"),
    ("failed", "preflight_blocked"),
    ("failed", "render_failed"),
    ("failed", "quality_failed"),
    ("failed", "strategy_failed"),
    ("failed", "state_commit_failed"),
}


# Fields excluded from cross-backend state-row equality. Per-run identity
# or known-divergent-by-design.
EXCLUDED_STATE_FIELDS = frozenset({
    "plan_hash",
    "last_run_at",
    "run_id",
    "duration_seconds",
    "plan_snapshot",
})


# ---------------------------------------------------------------------------
# Bundle YAML construction
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BackendSchemas:
    """Per-backend schema-name triple, exposed for use in assertions."""

    bronze: str
    silver: str
    gold: str

    def state_table_path(self, catalog: str) -> str:
        """``fusion_bundle_state`` lives in the bronze schema per the v1
        ``paths.bronze("fusion_bundle_state")`` resolution."""
        return f"{catalog}.{self.bronze}.fusion_bundle_state"


@dataclass(frozen=True)
class BundleArtifacts:
    """Paths produced by :func:`make_dual_bundles`. Tests use the
    backend-specific bundle paths; helpers use the schema triples for
    state-table queries."""

    v1_bundle: Path
    v2_bundle: Path
    v1_schemas: BackendSchemas
    v2_schemas: BackendSchemas
    profile_path: Path
    snapshot_path: Path | None
    catalog: str


_V1_BUNDLE_TEMPLATE = textwrap.dedent("""\
    apiVersion: aidp-fusion-bundle/v1
    version: "0.2.0"
    project: phase4-parity-v1

    fusion:
      serviceUrl: https://parity.local/invalid
      username: parity
      password: parity
      externalStorage: parity-storage

    aidp:
      catalog: {catalog}
      bronzeSchema: {bronze}
      silverSchema: {silver}
      goldSchema: {gold}
      storageFormat: delta

    datasets:
      - id: erp_suppliers
        mode: full
      - id: gl_coa
        mode: full
      - id: gl_period_balances
        mode: incremental
      - id: ap_invoices
        mode: incremental

    dimensions:
      build:
        - dim_account
        - dim_calendar
        - dim_supplier

    gold:
      marts:
        - gl_balance
        - supplier_spend
        - ap_aging
""")


_V2_BUNDLE_TEMPLATE = textwrap.dedent("""\
    apiVersion: aidp-fusion-bundle/v1
    version: "0.2.0"
    project: phase4-parity-v2

    fusion:
      serviceUrl: https://parity.local/invalid
      username: parity
      password: parity
      externalStorage: parity-storage

    aidp:
      catalog: {catalog}
      bronzeSchema: {bronze}
      silverSchema: {silver}
      goldSchema: {gold}
      storageFormat: delta

    datasets:
      - id: erp_suppliers
        mode: full
      - id: gl_coa
        mode: full
      - id: gl_period_balances
        mode: incremental
      - id: ap_invoices
        mode: incremental

    dimensions:
      build:
        - dim_account
        - dim_calendar
        - dim_supplier

    gold:
      marts:
        - gl_balance
        - supplier_spend
        - ap_aging

    contentPack:
      name: fusion-finance-starter
      path: {pack_path}
      profile: {profile_name}
""")


def make_dual_bundles(
    tmp_path: Path,
    *,
    catalog: str,
    v1_suffix: str = "v1",
    v2_suffix: str = "v2",
    pack_path: Path,
    profile_src: Path,
    snapshot_src: Path | None = None,
    profile_name: str | None = None,
) -> BundleArtifacts:
    """Write v1 + v2 bundle YAMLs with isolated bronze/silver/gold schemas
    and copy the profile (+ paired schema-snapshot, when present) into a
    ``profiles/`` subdir under ``tmp_path`` so bundle-relative profile
    resolution works.

    Per ``plan.md`` Step 2, the v2 bundle's bronze schema is distinct
    from v1's; ``fusion_bundle_state`` then lands in two different
    physical tables (``<catalog>.<bronze_v1>.fusion_bundle_state`` vs
    ``<catalog>.<bronze_v2>.fusion_bundle_state``). Stays orthogonal to
    the absence of a ``stateSchema`` field on ``Bundle.aidp``.

    The snapshot file (``profiles/<tenant>.schema-snapshot.yaml``) is
    Phase 3d's pinned per-dataset bronze schema; runtime preflight reads
    it on drift. Copying it alongside the profile keeps the harness on
    the "fingerprint match" path during seed (Step 2) and lets Step 7's
    schema-drift variants tamper with it explicitly.
    """
    v1_schemas = BackendSchemas(
        bronze=f"bronze_{v1_suffix}",
        silver=f"silver_{v1_suffix}",
        gold=f"gold_{v1_suffix}",
    )
    v2_schemas = BackendSchemas(
        bronze=f"bronze_{v2_suffix}",
        silver=f"silver_{v2_suffix}",
        gold=f"gold_{v2_suffix}",
    )

    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    if profile_name is None:
        profile_name = profile_src.stem
    dst_profile = profiles_dir / f"{profile_name}.yaml"
    shutil.copy2(profile_src, dst_profile)

    dst_snapshot: Path | None = None
    if snapshot_src is not None and snapshot_src.exists():
        dst_snapshot = profiles_dir / f"{profile_name}.schema-snapshot.yaml"
        shutil.copy2(snapshot_src, dst_snapshot)

    v1_bundle = tmp_path / "v1_bundle.yaml"
    v1_bundle.write_text(_V1_BUNDLE_TEMPLATE.format(
        catalog=catalog,
        bronze=v1_schemas.bronze,
        silver=v1_schemas.silver,
        gold=v1_schemas.gold,
    ))

    v2_bundle = tmp_path / "v2_bundle.yaml"
    v2_bundle.write_text(_V2_BUNDLE_TEMPLATE.format(
        catalog=catalog,
        bronze=v2_schemas.bronze,
        silver=v2_schemas.silver,
        gold=v2_schemas.gold,
        pack_path=str(pack_path),
        profile_name=profile_name,
    ))

    return BundleArtifacts(
        v1_bundle=v1_bundle,
        v2_bundle=v2_bundle,
        v1_schemas=v1_schemas,
        v2_schemas=v2_schemas,
        profile_path=dst_profile,
        snapshot_path=dst_snapshot,
        catalog=catalog,
    )


# ---------------------------------------------------------------------------
# Spark + Delta bootstrap
# ---------------------------------------------------------------------------


def make_delta_spark(app_name: str, warehouse_dir: str):
    """Construct a local-mode SparkSession with Delta enabled.

    Phase 4's harness writes to ``fusion_bundle_state`` and uses Delta
    MERGE on the state table; Phase 3 dodged Delta by normalizing
    ``USING DELTA → USING PARQUET`` on the silver/gold targets, but
    the state-table machinery cannot be normalized away. So Phase 4
    requires real Delta.

    Honest knob: when ``delta.pip_utils`` import fails (workstation
    without delta-spark or without internet access on first Maven
    fetch), this raises ``ImportError`` and the calling test's
    ``pytest.importorskip("delta")`` gate skips. The plan's Risks
    table covers this: Step 2's first deliverable is to confirm Delta
    bootstrap works; fallback is ``@pytest.mark.parity_live`` for
    cluster execution.
    """
    from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    from delta.pip_utils import configure_spark_with_delta_pip  # type: ignore[import-not-found]

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        # Delta wiring per delta-spark docs.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


# ---------------------------------------------------------------------------
# Bronze seeding
# ---------------------------------------------------------------------------


def seed_bronze(spark, *, catalog: str, schema: str, fixtures_module) -> None:
    """Seed a backend-isolated bronze schema with the parity fixture rows.

    Uses Phase 3's ``bronze_fixtures.all_fixtures()`` map: dataset_id →
    list[dict]. The function expects ``fixtures_module`` to expose
    ``all_fixtures()`` and a ``bronze_pyspark_schemas()`` helper that
    returns the StructType per dataset.

    Writes each dataset as a Delta table; the legacy backend's silver/
    gold reads expect Delta on the source side of MERGE statements.
    """
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    schemas = fixtures_module.bronze_pyspark_schemas()
    for dataset_id, rows in fixtures_module.all_fixtures().items():
        schema_struct = schemas[dataset_id]
        ordered = [tuple(r.get(f.name) for f in schema_struct.fields) for r in rows]
        df = spark.createDataFrame(ordered, schema=schema_struct)
        (
            df.write.format("delta")
            .mode("overwrite")
            .saveAsTable(f"{catalog}.{schema}.{dataset_id}")
        )


def create_target_schemas(spark, *, catalog: str, schemas: BackendSchemas) -> None:
    """Create empty silver + gold schemas. Bronze is created+seeded by
    :func:`seed_bronze` so we don't drop it here."""
    for name in (schemas.silver, schemas.gold):
        spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")
        spark.sql(f"CREATE SCHEMA {name}")


# ---------------------------------------------------------------------------
# BICC IO spy
# ---------------------------------------------------------------------------


def install_bicc_io_spy(monkeypatch):
    """Monkeypatch ``extractors.bicc.extract_pvo`` to a counter that the
    test can interrogate. Returns a 0-arg callable ``call_count()``.

    Per ``plan.md`` Step 2: silver+gold runs MUST NOT fire any BICC
    read. ``extract_pvo`` is the canonical entrypoint; the higher-level
    ``preflight_bronze_schemas`` wrapper IS called by the legacy backend
    (it just enumerates target paths), but the actual PVO read sits
    behind ``extract_pvo``. Spying that boundary catches a refactor
    that silently re-enables bronze IO.
    """
    from oracle_ai_data_platform_fusion_bundle.extractors import bicc as bicc_mod  # type: ignore[import-not-found]

    counter = {"calls": 0}
    original = bicc_mod.extract_pvo

    def _spy(*args, **kwargs):  # noqa: ANN001 — passthrough signature
        counter["calls"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(bicc_mod, "extract_pvo", _spy)
    return lambda: counter["calls"]


# ---------------------------------------------------------------------------
# RunSummary equivalence (the orchestrator-shape check)
# ---------------------------------------------------------------------------


def assert_run_summary_equiv(v1_summary, v2_summary) -> None:
    """Cross-backend ``RunSummary`` equivalence per ``plan.md`` Step 2.

    Asserts:
    - Equal step counts.
    - Per-step ``status`` matches (collapsed to v1 enum — content-pack
      runner collapses every non-success to ``'failed'``).
    - Per-step ``row_count`` matches modulo failure-path normalization.
    - Per-step ``last_watermark`` matches modulo ``None`` handling.
    - ``RunSummary.run_id`` is DIFFERENT between backends (each backend
      assigns its own).
    - ``RunSummary.skip_reason`` matches where present (cascade-skipped
      nodes carry ``'cascade'`` on both backends).

    Does NOT compare ``plan_hash`` (different hash inputs by design),
    ``duration_seconds``, or timing fields.
    """
    assert v1_summary.run_id != v2_summary.run_id, (
        "RunSummary.run_id must be unique per backend; "
        f"got identical {v1_summary.run_id!r} on both legs"
    )

    v1_steps = {s.dataset_id: s for s in v1_summary.steps}
    v2_steps = {s.dataset_id: s for s in v2_summary.steps}
    only_v1 = set(v1_steps) - set(v2_steps)
    only_v2 = set(v2_steps) - set(v1_steps)
    assert not (only_v1 or only_v2), (
        f"RunSummary step sets diverge: v1-only={sorted(only_v1)} "
        f"v2-only={sorted(only_v2)}"
    )

    for dataset_id, v1_step in v1_steps.items():
        v2_step = v2_steps[dataset_id]
        assert v1_step.status == v2_step.status, (
            f"{dataset_id}: RunStep.status diverges — v1={v1_step.status!r} "
            f"v2={v2_step.status!r}"
        )
        # Watermark equivalence (None on either side ↔ None on other).
        assert v1_step.last_watermark == v2_step.last_watermark, (
            f"{dataset_id}: RunStep.last_watermark diverges — "
            f"v1={v1_step.last_watermark!r} v2={v2_step.last_watermark!r}"
        )
        # Row count — both successes compare exactly; failure-class is
        # normalized so None↔0 is acceptable.
        if v1_step.status in FAILURE_CLASS_STATUSES:
            v1_rc = 0 if v1_step.row_count is None else v1_step.row_count
            v2_rc = 0 if v2_step.row_count is None else v2_step.row_count
            assert v1_rc == v2_rc, (
                f"{dataset_id}: failure-class row_count diverges after "
                f"normalization — v1={v1_step.row_count!r}->{v1_rc} "
                f"v2={v2_step.row_count!r}->{v2_rc}"
            )
        else:
            assert v1_step.row_count == v2_step.row_count, (
                f"{dataset_id}: success row_count diverges — "
                f"v1={v1_step.row_count!r} v2={v2_step.row_count!r}"
            )


# ---------------------------------------------------------------------------
# State-row equivalence (tiered contract — Tier A / B / C)
# ---------------------------------------------------------------------------


def _query_state_rows(spark, catalog: str, schema: str) -> list[dict]:
    """Read every row from the backend's ``fusion_bundle_state`` table.
    Returns dicts so the equivalence helpers can use field-by-field
    access without leaking Spark Row types into assertions.
    """
    table = f"{catalog}.{schema}.fusion_bundle_state"
    rows = spark.sql(f"SELECT * FROM {table}").collect()
    return [r.asDict() for r in rows]


def _index_by_node_layer_mode(
    rows: Iterable[dict],
    *,
    source_role_filter: str | None = None,
) -> dict[tuple, dict]:
    """Index rows by ``(dataset_id, layer, mode)``. When
    ``source_role_filter`` is set (e.g. ``'primary'``), only rows
    matching that ``source_role`` are kept — used by Tier B to isolate
    the primary row in multi-source nodes (where the lookup row would
    confuse a naive key-equality check).

    Tolerates rows where ``source_role`` is absent (legacy shape) by
    treating ``None`` as primary; this matches the v1 backend which
    never sets ``source_role``.
    """
    out: dict[tuple, dict] = {}
    for r in rows:
        if source_role_filter is not None:
            role = r.get("source_role")
            if role is None and source_role_filter == "primary":
                pass  # legacy shape — treat as primary
            elif role != source_role_filter:
                continue
        key = (r.get("dataset_id"), r.get("layer"), r.get("mode"))
        out[key] = r
    return out


def assert_state_rows_equiv(
    spark,
    *,
    catalog: str,
    v1_schema: str,
    v2_schema: str,
    expected_nodes: Sequence[tuple[str, str]],
    expected_mode: str = "seed",
) -> None:
    """Three-tier state-row equivalence per ``plan.md`` Step 2.

    Args:
        spark: live Spark session.
        catalog: target catalog (same on both sides — only the schema
            differs).
        v1_schema: legacy backend's bronze schema name.
        v2_schema: content-pack backend's bronze schema name.
        expected_nodes: sequence of ``(dataset_id, layer)`` pairs every
            backend MUST have written.
        expected_mode: ``"seed"`` or ``"incremental"``.

    Tier A — common semantic fields with failure-path normalization
    (``dataset_id`` / ``layer`` / ``mode`` / ``row_count`` /
    ``status``).

    Tier B — watermark equivalence cross-shape: v1's ``last_watermark``
    maps to v2's primary row's ``last_watermark`` / ``output_watermark``.

    Tier C — v2-only fields are asserted present + correct on v2 (and
    absent on v1). Lookup rows are asserted on v2 only.
    """
    v1_rows = _query_state_rows(spark, catalog, v1_schema)
    v2_rows = _query_state_rows(spark, catalog, v2_schema)

    # Index. v1 is single-row-per-node so no role filter; v2 carries
    # lookup rows for multi-source nodes — Tier A/B compare PRIMARY rows
    # only (legacy shape OR source_role='primary').
    v1_idx = _index_by_node_layer_mode(v1_rows)
    v2_primary_idx = _index_by_node_layer_mode(
        v2_rows, source_role_filter="primary",
    )

    for dataset_id, layer in expected_nodes:
        key = (dataset_id, layer, expected_mode)
        v1_row = v1_idx.get(key)
        v2_row = v2_primary_idx.get(key)
        assert v1_row is not None, (
            f"Tier A: v1 missing state row for {key}; "
            f"present={sorted(v1_idx)}"
        )
        assert v2_row is not None, (
            f"Tier A: v2 missing primary state row for {key}; "
            f"present={sorted(v2_primary_idx)}"
        )

        # ----- Tier A: status equivalence ---------------------------
        v1_status = v1_row.get("status")
        v2_status = v2_row.get("status")
        if v1_status != v2_status:
            # Allow the documented equivalence class (v1 collapses
            # every v2-specific failure into 'failed').
            pair = (v1_status, v2_status)
            assert pair in PERSISTED_STATUS_EQUIVALENCE, (
                f"{key}: persisted status diverges outside the documented "
                f"equivalence class — v1={v1_status!r} v2={v2_status!r}"
            )

        # ----- Tier A: row_count with failure-path normalization ----
        v1_rc = v1_row.get("row_count")
        v2_rc = v2_row.get("row_count")
        if v1_status in FAILURE_CLASS_STATUSES or v2_status in FAILURE_CLASS_STATUSES:
            v1_rc_norm = 0 if v1_rc is None else v1_rc
            v2_rc_norm = 0 if v2_rc is None else v2_rc
            assert v1_rc_norm == v2_rc_norm, (
                f"{key}: failure-class row_count diverges after "
                f"normalization — v1={v1_rc!r}->{v1_rc_norm} "
                f"v2={v2_rc!r}->{v2_rc_norm}"
            )
        else:
            assert v1_rc == v2_rc, (
                f"{key}: success row_count diverges — "
                f"v1={v1_rc!r} v2={v2_rc!r}"
            )

        # ----- Tier B: watermark cross-shape -------------------------
        v1_wm = v1_row.get("last_watermark")
        v2_wm = v2_row.get("last_watermark")
        v2_owm = v2_row.get("output_watermark")
        # v2 primary row carries last_watermark AND output_watermark;
        # they must agree (sql_runner sets both on success). v1 has
        # only last_watermark. The cross-shape rule: v1.last_watermark
        # equals v2.last_watermark (which equals v2.output_watermark).
        if v2_owm is not None and v2_wm is not None:
            assert v2_owm == v2_wm, (
                f"{key}: v2 primary row has divergent last_watermark vs "
                f"output_watermark ({v2_wm!r} vs {v2_owm!r}) — runtime invariant violated"
            )
        assert v1_wm == v2_wm, (
            f"{key}: Tier B watermark cross-shape mismatch — "
            f"v1.last_watermark={v1_wm!r} v2.last_watermark={v2_wm!r}"
        )

        # ----- Tier C: v2-only fields ------------------------------
        # `pack_id`, `node_implementation_type`, `rendered_sql_hash`,
        # `profile_hash` MUST be present + populated on v2 success rows.
        if v2_status == "success":
            v2_pack_id = v2_row.get("pack_id")
            assert v2_pack_id, (
                f"{key}: Tier C — v2 success row missing pack_id"
            )
            impl_type = v2_row.get("node_implementation_type")
            assert impl_type in ("sql", "builtin"), (
                f"{key}: Tier C — v2 node_implementation_type={impl_type!r} "
                "not in ('sql', 'builtin')"
            )
            rendered_hash = v2_row.get("rendered_sql_hash")
            # builtin nodes legitimately carry no rendered_sql_hash;
            # only assert for sql nodes.
            if impl_type == "sql":
                assert rendered_hash, (
                    f"{key}: Tier C — v2 sql node missing rendered_sql_hash"
                )
            profile_hash = v2_row.get("profile_hash")
            assert profile_hash, (
                f"{key}: Tier C — v2 success row missing profile_hash"
            )

        # ----- Tier C: v2-only fields absent on v1 -------------------
        # v1's state-row schema doesn't carry these columns at all
        # (or carries them as None thanks to the Phase 2 additive
        # migration). Either way, asserting v1 doesn't populate them
        # would be too fragile (the additive migration WILL add them
        # to the v1-schema table once they're queried). Skipped.


def assert_v2_lookup_row(
    spark,
    *,
    catalog: str,
    v2_schema: str,
    primary_dataset: str,
    lookup_source_id: str,
    expected_mode: str = "seed",
) -> None:
    """Assert the v2 backend wrote a ``source_role='lookup'`` state row
    for the given multi-source node.

    Used by the Task 15 multi-source cursor-policy assertions on
    ``gl_balance`` (primary = ``gl_period_balances``, lookup =
    ``dim_account``).

    Per ``plan.md`` Step 10 / §11.10:
    - Lookup row carries ``source_role='lookup'``,
      ``output_watermark=NULL`` (lookups don't advance cursors),
      populated ``consumed_version`` (audit-only).
    """
    v2_rows = _query_state_rows(spark, catalog, v2_schema)
    matching = [
        r for r in v2_rows
        if r.get("dataset_id") == primary_dataset
        and r.get("source_role") == "lookup"
        and r.get("source_id") == lookup_source_id
        and r.get("mode") == expected_mode
    ]
    assert matching, (
        f"v2 state table missing lookup row for "
        f"({primary_dataset}, lookup, {lookup_source_id}, {expected_mode}); "
        f"have rows: "
        f"{[(r.get('dataset_id'), r.get('source_role'), r.get('source_id'), r.get('mode')) for r in v2_rows]}"
    )
    row = matching[0]
    # Lookup must not advance the node's cursor.
    assert row.get("output_watermark") is None, (
        f"{primary_dataset}/lookup/{lookup_source_id}: lookup row has "
        f"non-NULL output_watermark={row.get('output_watermark')!r} — "
        "violates the v0.3 primary/lookup contract (lookups don't advance cursors)"
    )


# ---------------------------------------------------------------------------
# Output row-set equivalence (silver/gold tables)
# ---------------------------------------------------------------------------


def _audit_cols_for_layer(layer: str) -> set[str]:
    if layer == "silver":
        return {"silver_built_at", "silver_run_id", "bronze_extract_ts"}
    if layer == "gold":
        return {"gold_built_at", "gold_run_id", "bronze_extract_ts"}
    return set()


def _normalize_row(row_dict: dict, audit_cols: set[str]) -> tuple:
    items = []
    for k in sorted(row_dict.keys()):
        if k in audit_cols:
            continue
        items.append((k, row_dict[k]))
    return tuple(items)


def assert_output_rows_equiv(
    spark,
    *,
    v1_target: str,
    v2_target: str,
    layer: str,
    node_id: str,
) -> None:
    """Read both backends' materialized tables and assert multiset
    equality on non-audit columns. Mirrors Phase 3's
    ``_assert_row_sets_equal`` but operates on full table identifiers
    rather than pre-collected row lists.
    """
    v1_rows = spark.read.table(v1_target).collect()
    v2_rows = spark.read.table(v2_target).collect()
    audit = _audit_cols_for_layer(layer)
    v1_keys = [_normalize_row(r.asDict(), audit) for r in v1_rows]
    v2_keys = [_normalize_row(r.asDict(), audit) for r in v2_rows]
    v1c = Counter(v1_keys)
    v2c = Counter(v2_keys)
    v1_only = v1c - v2c
    v2_only = v2c - v1c
    if v1_only or v2_only:
        parts = [f"{node_id}: materialized row sets diverge"]
        if v1_only:
            parts.append(f"  v1-only ({sum(v1_only.values())} rows):")
            for k, n in list(v1_only.items())[:3]:
                parts.append(f"    ×{n}: {dict(k)}")
        if v2_only:
            parts.append(f"  v2-only ({sum(v2_only.values())} rows):")
            for k, n in list(v2_only.items())[:3]:
                parts.append(f"    ×{n}: {dict(k)}")
        raise AssertionError("\n".join(parts))


def assert_output_schemas_equiv(
    spark,
    *,
    v1_target: str,
    v2_target: str,
    layer: str,
    node_id: str,
) -> None:
    """Cross-backend schema (column-name + Spark type) equivalence on
    materialized tables. Catches the decimal-precision class of drift
    that row-equality alone hides."""
    audit = _audit_cols_for_layer(layer)
    v1_df = spark.read.table(v1_target)
    v2_df = spark.read.table(v2_target)
    v1_types = {
        f.name: f.dataType.simpleString()
        for f in v1_df.schema.fields
        if f.name not in audit
    }
    v2_types = {
        f.name: f.dataType.simpleString()
        for f in v2_df.schema.fields
        if f.name not in audit
    }
    if v1_types != v2_types:
        diffs = []
        for k in sorted(set(v1_types) | set(v2_types)):
            t1 = v1_types.get(k, "<missing>")
            t2 = v2_types.get(k, "<missing>")
            if t1 != t2:
                diffs.append(f"  {k}: v1={t1!r} v2={t2!r}")
        raise AssertionError(
            f"{node_id}: materialized schema types diverge\n"
            + "\n".join(diffs)
        )


# ---------------------------------------------------------------------------
# Plan-hash stability (Step 3)
# ---------------------------------------------------------------------------


def query_plan_hash(
    spark,
    *,
    catalog: str,
    schema: str,
    dataset_id: str,
    layer: str,
    mode: str,
) -> str | None:
    """Look up ``plan_hash`` for the latest state row matching the
    given keys. Returns ``None`` when no row exists.
    """
    table = f"{catalog}.{schema}.fusion_bundle_state"
    rows = spark.sql(
        f"SELECT plan_hash FROM {table} "
        f"WHERE dataset_id = '{dataset_id}' AND layer = '{layer}' "
        f"AND mode = '{mode}' "
        f"ORDER BY last_run_at DESC LIMIT 1"
    ).collect()
    if not rows:
        return None
    return rows[0]["plan_hash"]


# ---------------------------------------------------------------------------
# Resume-context primitives (Step 5)
# ---------------------------------------------------------------------------


def insert_new_bronze_row(
    spark,
    *,
    catalog: str,
    schema: str,
    dataset_id: str,
    row: dict,
) -> None:
    """Append one row to a bronze Delta table. Used by Step 3 to
    introduce an incremental delta and by Step 7a to introduce the
    incremental row that the hard-cursor-commit failure test will
    fail to commit."""
    table = f"{catalog}.{schema}.{dataset_id}"
    # Read existing schema so the appended row matches column order
    # and types.
    existing = spark.read.table(table)
    field_order = [f.name for f in existing.schema.fields]
    schema_struct = existing.schema
    ordered = [tuple(row.get(name) for name in field_order)]
    new_df = spark.createDataFrame(ordered, schema=schema_struct)
    new_df.write.format("delta").mode("append").saveAsTable(table)


# ---------------------------------------------------------------------------
# Misc / introspection
# ---------------------------------------------------------------------------


def datetime_to_iso(value: datetime | None) -> str | None:
    """Helper for evidence files — ISO-format timestamps consistently
    so cross-tier comparisons read the same strings."""
    if value is None:
        return None
    return value.isoformat()
