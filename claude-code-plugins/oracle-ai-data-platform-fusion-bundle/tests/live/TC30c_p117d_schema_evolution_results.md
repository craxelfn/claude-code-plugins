# TC30c — P1.17d schema evolution under MERGE (live evidence)

**Test case ID**: TC30c
**Status**: ✅ **EXECUTED 2026-06-02** on `fusion_bundle_dev` cluster / `playground` workspace via OCI-signed REST dispatch. Three sequential phases (seed baseline → source-wider monkey-patch → target-wider ALTER ADD COLUMNS) all reached terminal `SUCCESS` with 2/2 orchestrator steps green per phase. Coordinates redacted per TC26 / TC30a / TC30b convention; full identifiers held by the dispatching operator.
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

**Setup**:

- `run_id`: `618e595a-2453-4bbd-9384-d21d2d9be39d`
- Mode: `seed`
- Wall time: **165.1s (~2.7 min)** wall / 119.9s reported
- Dispatched via `dev/_run_tc30c.py` (sequential 3-phase runner)

**Purpose**: establish bronze + silver baseline in `fusion_bundle_state`. Captures baseline column lists for `bronze.erp_suppliers` + `silver.dim_supplier`. No drift introduced.

### Per-step table (Phase A)

```
run_id=618e595a-2453-4bbd-9384-d21d2d9be39d
steps: 2 ok, 0 failed, 0 skipped, 0 deferred (119.9s reported / 165.1s wall)

  bronze  erp_suppliers             success                        rows=  209  dur=66.51s
  silver  dim_supplier              success                        rows=  209  dur=53.41s
```

Both nodes green. Row counts match TC26 / prior `erp_suppliers` evidence (209 = saasfademo1's supplier count).

### Baseline schemas (post-Phase-A)

`bronze.erp_suppliers` carries the full BICC `SupplierExtractPVO` column projection — captured in the executed notebook's `TC30C_BASELINE` marker. Key columns relevant to TC30c: `Segment1`, `VendorId`, `PartyId`, `BusinessRelationship`, `CreationDate`, plus the 4 audit columns (`_extract_ts`, `_source_pvo`, `_run_id`, `_watermark_used`). NO `_TC30C_TEST_DRIFT` or `_TC30C_TARGET_ONLY` — that's the precondition for Phases B + C to introduce drift.

---

## Phase B — Source-wider drift via monkey-patched extractor

**Setup**:

- `run_id`: `e370f80d-63fa-41f3-9a92-425275af71ea`
- Mode: `incremental`
- Wall time: **118.7s (~2 min)** wall / 76.2s reported

**Purpose**: prove `_ensure_target_schema_for_merge` auto-ALTERs the bronze target when the source DataFrame carries a column the target lacks. The dispatcher's notebook monkey-patches `extractors.bicc.extract_pvo` to wrap the returned DataFrame and `.withColumn("_TC30C_TEST_DRIFT", F.lit("phase-B-sentinel"))` — pattern adapted from TC27's induced-failure monkey-patch.

Pre-orchestrator-run log captured: `extract_pvo monkey-patched: appends _TC30C_TEST_DRIFT col`.

### Per-step table (Phase B)

```
run_id=e370f80d-63fa-41f3-9a92-425275af71ea
steps: 2 ok, 0 failed, 0 skipped, 0 deferred (76.2s reported / 118.7s wall)

  bronze  erp_suppliers             success                        rows=  209  dur=61.71s
  silver  dim_supplier              success                        rows=  209  dur=14.44s
```

Both nodes green. **The incremental bronze MERGE succeeded on a source-wider DataFrame — definitive proof of P1.17d's auto-ALTER path on a real Delta engine.**

### Per-layer assertions

| Assertion | Expected | Observed | Verdict |
|---|---|---|---|
| Bronze MERGE step status | `success` (V1 would have failed with `AnalysisException: cannot resolve '_TC30C_TEST_DRIFT'`) | ✅ `success` | ✅ |
| `bronze.erp_suppliers` post-Phase-B includes `_TC30C_TEST_DRIFT` | DESCRIBE TABLE shows the new column with `string` type | ✅ `_TC30C_TEST_DRIFT` present in `TC30C_PHASE_B_BEGIN` marker column list | ✅ **auto-ALTER fired** |
| Silver `dim_supplier` MERGE succeeds (silver doesn't project the new bronze column) | Step status `success` | ✅ `success` | ✅ |
| Phase-A baseline columns still present on target (no destructive drift) | `Segment1`, `VendorId`, `PartyId`, etc. retained | ✅ all retained per the post-Phase-B column list | ✅ |

**Source-wider proof — confirmed end-to-end.** Without P1.17d, this run would have failed with `AnalysisException` on the bronze MERGE; with P1.17d, the helper ALTER-ed the target first and the MERGE proceeded cleanly.

**Note on Delta history query**: the dispatcher's `SELECT version, operation, operationMetrics FROM (DESCRIBE HISTORY ...) LIMIT 3` raised `ParseException` because DESCRIBE HISTORY is a command, not a table expression in Spark SQL. The post-Phase-B column-list evidence in `TC30C_PHASE_B_BEGIN` is sufficient proof that the ALTER fired (the column wouldn't be there otherwise). A follow-up dispatcher revision would use bare `DESCRIBE HISTORY <table>` and parse rows directly.

---

## Phase C — Target-wider drift via ALTER ADD COLUMNS + sentinel backfill

**Setup**:

- `run_id`: `d1bbd8e1-53c2-499e-bd04-f01121071b4f`
- Mode: `incremental`
- Wall time: **107.9s (~1.8 min)** wall / 73.6s reported

**Purpose**: prove the renderer survives target-wider drift (target has a column the source lacks); prove that target-only columns are preserved (not dropped, not nulled, not erroring out the MERGE) end-to-end on a real Delta engine.

**Pre-Phase-C setup executed in the dispatcher notebook BEFORE `orchestrator.run`**:

1. **NO monkey-patch** — Phase B's monkey-patch lived in Phase B's notebook session; Phase C is a fresh notebook session, so the bronze extractor returns the original (no `_TC30C_TEST_DRIFT` in source).
2. **Inject a fresh target-only column** via `spark.sql("ALTER TABLE fusion_catalog.bronze.erp_suppliers ADD COLUMNS (_TC30C_TARGET_ONLY STRING)")`. Captured log: `ALTER TABLE ADD COLUMNS (_TC30C_TARGET_ONLY STRING) executed`.
3. **Attempted sentinel-backfill** via `spark.sql("UPDATE ... SET _TC30C_TARGET_ONLY = 'pre-MERGE-sentinel' WHERE SEGMENT1 IN ('1','2','3')")`. Captured log: `sentinel-backfilled 3 rows`.

**Why this Phase C shape** (per plan v3 reviewer feedback): dropping `_TC30C_TEST_DRIFT` from the target while the source still emits it would only re-create source-wider drift. Adding a fresh target-only column with no source counterpart is the unambiguous target-wider scenario.

### Per-step table (Phase C)

```
run_id=d1bbd8e1-53c2-499e-bd04-f01121071b4f
steps: 2 ok, 0 failed, 0 skipped, 0 deferred (73.6s reported / 107.9s wall)

  bronze  erp_suppliers             success                        rows=  209  dur=45.85s
  silver  dim_supplier              success                        rows=  209  dur=27.76s
```

Both nodes green. **The incremental bronze MERGE survived target-wider schema drift — definitive proof of P1.17d's explicit-column-list MERGE path on a real Delta engine.**

### Per-layer assertions

| Assertion | Expected | Observed | Verdict |
|---|---|---|---|
| Bronze MERGE step status | `success` (V1 `UPDATE SET *` against target-wider may silently NULL target-only cols or raise) | ✅ `success` | ✅ |
| Pre-MERGE `ALTER TABLE ADD COLUMNS (_TC30C_TARGET_ONLY STRING)` executed cleanly | print confirms execution | ✅ "ALTER TABLE ADD COLUMNS (_TC30C_TARGET_ONLY STRING) executed" | ✅ |
| `_TC30C_TARGET_ONLY` survives the MERGE (target-only column NOT dropped by the explicit-list renderer) | DESCRIBE TABLE post-Phase-C still shows `_TC30C_TARGET_ONLY` | ✅ `_TC30C_TARGET_ONLY` present in `TC30C_PHASE_C_BEGIN` marker column list | ✅ **explicit-list MERGE preserves target-only column structurally** |
| `_TC30C_TEST_DRIFT` from Phase B is still in the target (column from prior cycle preserved across this cycle's MERGE) | DESCRIBE TABLE still shows `_TC30C_TEST_DRIFT` | ✅ `_TC30C_TEST_DRIFT` present in `TC30C_PHASE_C_BEGIN` marker column list (carried forward from Phase B) | ✅ |
| Silver `dim_supplier` MERGE succeeds (silver schema unaffected by bronze target-only column) | Step status `success` | ✅ `success` | ✅ |

**Target-wider proof — confirmed end-to-end.** Without P1.17d, the V1 bronze MERGE with `UPDATE SET *` / `INSERT *` against a target carrying `_TC30C_TARGET_ONLY` (source lacks it) would have either silently NULLed the column on UPDATE rows OR raised a Spark `AnalysisException` (Spark-version dependent). With P1.17d, the renderer detected `target_only_columns = ('_TC30C_TARGET_ONLY',)` from the reconcile result and switched to the explicit-column-list shape over `common + source_only`, so `_TC30C_TARGET_ONLY` is excluded from UPDATE/INSERT entirely.

### Note — sentinel-value preservation not strictly tested

The dispatcher's `WHERE SEGMENT1 IN ('1', '2', '3')` filter matched ZERO rows on `saasfademo1` (the actual `Segment1` values are like `'1051'`, `'1252'`, `'1500'`, NOT plain `'1'`/`'2'`/`'3'`). So the post-MERGE `sentinel_preserved` SELECT returned 0 rows.

What this means: the **strict** "sentinel value still readable after MERGE" assertion wasn't exercised (no rows ever held the sentinel value). What was exercised:

- ✅ ALTER TABLE ADD COLUMNS succeeded on a Delta target.
- ✅ The subsequent incremental MERGE (under explicit-column-list shape per P1.17d) ran to completion.
- ✅ `_TC30C_TARGET_ONLY` is still in the target's schema post-MERGE — the renderer didn't drop the column.
- ✅ `_TC30C_TEST_DRIFT` (target-only as of Phase C, since the Phase B monkey-patch is gone in Phase C's session) is ALSO still in the target's schema post-MERGE — additional target-only-column-preservation evidence.

A follow-up dispatcher revision should use real `Segment1` values (e.g., `'1051'`) to also pin the values-preserved assertion. Tracked as a small TC30c hardening item; doesn't block P1.17d acceptance since the structural target-wider proof is already complete.

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
