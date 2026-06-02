# TC30c — P1.17d schema evolution under MERGE (live evidence)

**Test case ID**: TC30c
**Status**: 🟡 **PENDING DISPATCH 2026-06-02** on `fusion_bundle_dev` cluster / `playground` workspace via OCI-signed REST dispatch. Coordinates redacted per TC26 / TC30a / TC30b convention; full identifiers held by the dispatching operator. Three-phase probe on a 2-node DAG isolating the bronze + silver schema-evolution path.
**Tracks**: `BACKLOG.md` §P1.17d acceptance + `LIMITS.md` §P1.17-L6 resolution.

## What this verifies

P1.17d adds `_ensure_target_schema_for_merge` as a pre-MERGE step at all 4 integration sites (bronze + dim_supplier + dim_account + gl_balance). On real Delta tables, the helper:

- **Source-wider** — emits `ALTER TABLE <target> ADD COLUMNS (<new> <type>, ...)`; the subsequent MERGE proceeds with V1 `UPDATE SET *` / `INSERT *` shape (post-ALTER schemas match).
- **Target-wider** — returns `target_only_columns`; the renderer switches to explicit-column-list MERGE (`UPDATE SET t.c = s.c, ...; INSERT (c, ...) VALUES (s.c, ...)`) over `common + source_only`; target-only columns are preserved by exclusion from the UPDATE / INSERT lists.
- **Type-conflict** — raises `SchemaEvolutionTypeConflictError(OrchestratorConfigError)`; auto-flows to CLI exit-2.

TC30c proves all three behaviors at the engine level on a real Delta runtime. Contract has three layers, each with its own evidence:

1. **Helper-level unit tests** — `tests/unit/test_p117_orchestrator_dispatch.py::TestSchemaReconcileHelper` (7 tests).
2. **Dispatch-boundary tests** — `TestSchemaEvolution` (4 tests) + `tests/unit/test_p117_builder_merge_sql.py::TestExplicitColumnListMergeSyntax` (3 tests).
3. **End-to-end engine behavior on real Delta tables** — this document (TC30c).

All three layers required; none replaces the other.

## Scope (2 nodes)

| Layer | Node | Why included |
|---|---|---|
| bronze | `erp_suppliers` | Smallest BICC PVO (~209 rows on saasfademo1) — schema-evolution proof needs to fire ALTER + MERGE; the row count is irrelevant beyond "non-zero". |
| silver | `dim_supplier` | `depends_on_bronze=("erp_suppliers",)` per `registry.py` — exercises the silver-builder integration site (`_ensure_target_schema_for_merge` called from `build()`). |

Including only 2 nodes minimizes wall time (~3-5 min per phase). `dim_account` + `gl_balance` builders share the exact same integration shape as `dim_supplier`; one silver builder under live conditions is sufficient evidence that the shape works.

`bundle.tc30c.yaml` lives at `dev/bundle.tc30c.yaml` (gitignored). Generator: `dev/_make_tc30c_bundle.py`.

## Cross-references

- `BACKLOG.md` §P1.17d — backlog entry this feature implements.
- `LIMITS.md` §P1.17-L6 — limit resolved by this feature (moved to §Resolved 2026-06-02).
- `docs/features/p1.17d-schema-evolution-under-merge/idea.md` — problem statement + topology.
- `docs/features/p1.17d-schema-evolution-under-merge/plan.md` — implementation plan + per-phase assertions.
- `tests/live/TC30a_p117_incremental_merge_proof.md` — V1 baseline (no schema evolution).
- `tests/live/TC30b_p117e_payload_diff_results.md` — sibling P1.17e evidence; same 2-cycle / dispatcher pattern.
- `scripts/oracle_ai_data_platform_fusion_bundle/orchestrator/state.py` — `_ensure_target_schema_for_merge` helper + `SchemaReconcileResult` dataclass.
- `scripts/oracle_ai_data_platform_fusion_bundle/orchestrator/errors.py` — `SchemaEvolutionTypeConflictError`.
- `scripts/oracle_ai_data_platform_fusion_bundle/orchestrator/merge_sql.py` — explicit-column-list clause builders (NEW neutral module).
- `scripts/oracle_ai_data_platform_fusion_bundle/orchestrator/__init__.py` — bronze MERGE renderer integration.
- `scripts/oracle_ai_data_platform_fusion_bundle/dimensions/dim_supplier.py` — silver builder integration.
- `scripts/oracle_ai_data_platform_fusion_bundle/dimensions/dim_account.py` — silver builder integration.
- `scripts/oracle_ai_data_platform_fusion_bundle/transforms/gold/gl_balance.py` — gold builder integration.

---

## Phase A — Seed baseline

**Setup** (placeholders pending operator-side fill):

- `run_id`: `<RUN_A_ID>`
- Mode: `seed`
- Wall time: `<seconds>` reported / `<seconds>` wall

**Purpose**: establish bronze + silver cursors in `fusion_bundle_state`. Captures baseline column lists for `bronze.erp_suppliers` + `silver.dim_supplier`. No drift introduced.

### Per-step table (Phase A)

```
<paste from executed-notebook cell 3 stdout — AIDP_LIVE_TEST_RESULT marker payload>
```

Expected: 1 bronze success (`erp_suppliers` ~209 rows) + 1 silver success (`dim_supplier` ~209 rows). Zero deferred / failed / skipped.

### Baseline schemas (post-Phase-A)

```
-- DESCRIBE TABLE fusion_catalog.bronze.erp_suppliers
<paste column list>

-- DESCRIBE TABLE fusion_catalog.silver.dim_supplier
<paste column list>
```

---

## Phase B — Source-wider drift via monkey-patched extractor

**Setup**:

- `run_id`: `<RUN_B_ID>`
- Mode: `incremental`
- Wall time: `<seconds>`

**Purpose**: prove `_ensure_target_schema_for_merge` auto-ALTERs the bronze target when the source DataFrame carries a column the target lacks. The dispatcher's notebook monkey-patches `extractors.bicc.extract_pvo` to wrap the returned DataFrame and `.withColumn("_TC30C_TEST_DRIFT", F.lit("phase-B-sentinel"))` — pattern adapted from TC27's induced-failure monkey-patch.

### Per-step table (Phase B)

```
<paste from executed-notebook cell 3 stdout>
```

### Per-layer assertions

| Assertion | Expected | Observed |
|---|---|---|
| `bronze.erp_suppliers` post-Phase-B includes `_TC30C_TEST_DRIFT` | DESCRIBE TABLE shows the new column with `string` type | `<paste>` |
| Existing rows have `NULL` in the new column | `SELECT COUNT(*) FROM bronze.erp_suppliers WHERE _TC30C_TEST_DRIFT IS NULL` ≈ 209 | `<paste>` |
| Delta rows (the monkey-patched re-extract) carry the sentinel value | `SELECT COUNT(*) FROM bronze.erp_suppliers WHERE _TC30C_TEST_DRIFT = 'phase-B-sentinel'` ≥ 1 | `<paste>` |
| `DESCRIBE HISTORY bronze.erp_suppliers` post-Phase-B shows an ALTER TABLE commit | One Delta commit with `operation = 'ADD COLUMNS'` between Phase A and Phase B | `<paste>` |
| Silver `dim_supplier` MERGE succeeds (silver doesn't see the new bronze column unless its SELECT projects it — out of scope) | Step status `success` | `<paste>` |

**Source-wider proof.** Without P1.17d, this run would have failed with `AnalysisException` on the bronze MERGE; with P1.17d, the helper ALTERs the target first and the MERGE proceeds.

---

## Phase C — Target-wider drift via ALTER ADD COLUMNS + sentinel backfill

**Setup**:

- `run_id`: `<RUN_C_ID>`
- Mode: `incremental`
- Wall time: `<seconds>`

**Purpose**: prove the renderer switches to explicit-column-list MERGE when the target has a column the source lacks; prove that target-only columns are preserved (not nulled) on matched-row UPDATE.

**Pre-Phase-C setup** (in the dispatcher's notebook, before invoking `orchestrator.run`):

1. **Remove the Phase-B source monkey-patch** so the bronze extractor returns a DataFrame WITHOUT `_TC30C_TEST_DRIFT` — restores the original schema-narrower-than-target state.
2. **Inject a fresh target-only column**:
   ```sql
   ALTER TABLE fusion_catalog.bronze.erp_suppliers
     ADD COLUMNS (_TC30C_TARGET_ONLY STRING)
   ```
3. **Sentinel-backfill 3 existing rows**:
   ```sql
   UPDATE fusion_catalog.bronze.erp_suppliers
     SET _TC30C_TARGET_ONLY = 'pre-MERGE-sentinel'
     WHERE SEGMENT1 IN ('1', '2', '3')
   ```

**Why this Phase C shape** (per plan v3 reviewer feedback): dropping `_TC30C_TEST_DRIFT` from the target while the source still emits it would only re-create source-wider drift (the original v1 mistake). Adding a NEW target-only column with no source counterpart is the unambiguous target-wider scenario. Sentinel-backfill on a small subset proves the explicit-list MERGE truly preserves target-only data rather than letting `UPDATE SET *`-style behavior NULL them.

### Per-step table (Phase C)

```
<paste from executed-notebook cell 3 stdout>
```

### Per-layer assertions

| Assertion | Expected | Observed |
|---|---|---|
| Phase-C step status | `success` | `<paste>` |
| Captured MERGE SQL contains explicit `UPDATE SET target.<col> = src.<col>` clauses for EVERY common-or-source-only column | `UPDATE SET target.SEGMENT1 = src.SEGMENT1, ...` | `<paste>` |
| Captured MERGE SQL does NOT mention `_TC30C_TARGET_ONLY` in UPDATE or INSERT clauses | `_TC30C_TARGET_ONLY` absent from the WHEN MATCHED predicate AND from the INSERT column list | `<paste>` |
| `_TC30C_TARGET_ONLY` values for the 3 sentinel rows are PRESERVED post-MERGE | `SELECT _TC30C_TARGET_ONLY FROM bronze.erp_suppliers WHERE SEGMENT1 IN ('1','2','3')` returns 3 rows with value `'pre-MERGE-sentinel'` | `<paste>` |
| `_TC30C_TEST_DRIFT` from Phase B is still in the target (no DROP COLUMN happened) | DESCRIBE TABLE still shows `_TC30C_TEST_DRIFT` | `<paste>` |

**Target-wider proof — explicit-column-list MERGE exclusion + matched-row value preservation.**

---

## Behavioral contrast

**Pre-P1.17d** (current `main` before this feature shipped):
- Phase B would have raised `org.apache.spark.sql.AnalysisException: cannot resolve '_TC30C_TEST_DRIFT'`. Operator runs ALTER manually, retries the incremental cycle.
- Phase C (with `_TC30C_TARGET_ONLY` added by operator manually): MERGE `UPDATE SET *` either silently NULLs target-only columns on matched rows OR raises AnalysisException (Spark-version dependent).

**Post-P1.17d**:
- Phase B: `_ensure_target_schema_for_merge` auto-ALTERs the bronze target; MERGE proceeds without operator intervention.
- Phase C: renderer switches to explicit-column-list MERGE; target-only columns preserved.

## What this DOES NOT verify

- **Type-conflict path** — covered by `TestSchemaReconcileHelper::test_type_conflict_raises_before_any_alter` at unit level; the dispatcher would need to manually introduce a type mismatch (e.g., ALTER target's column type post-seed) to exercise it live. Out of scope for TC30c v1.
- **gl_balance gold builder** — same integration shape as `dim_supplier`; covered by unit tests + the import-graph smoke test. Live evidence on `gl_balance` could be added in a future TC30d if a real tenant exercises the path.
- **Non-`saasfademo1` tenant** — same blocker as P3.7 / P3.9 across all live evidence.

## Dispatcher metadata (redacted)

```
aidp-id        : <REDACTED — AIDP datalake OCID held by the operator>
workspace-key  : <REDACTED — workspace UUID>
cluster-key    : <REDACTED — cluster UUID>
fusion pod     : <REDACTED — Fusion demo pod base URL>
fusion user    : <REDACTED — BICC user>
storage profile: <REDACTED — BICC External Storage profile name>
secret entry   : <REDACTED — AIDP credential store entry name>
bundle         : tc30c-schema-evolution-proof (1 bronze + 1 silver)
```
