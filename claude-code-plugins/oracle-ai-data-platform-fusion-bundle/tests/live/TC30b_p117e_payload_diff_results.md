# TC30b — P1.17e bronze MERGE payload-diff predicate (live evidence)

**Test case ID**: TC30b
**Status**: 🟡 **IN FLIGHT 2026-06-02** on `fusion_bundle_dev` cluster / `playground` workspace via OCI-signed REST dispatch. Coordinates redacted per the TC26/TC30a convention; full identifiers held by the dispatching operator. Two-run probe (Run A seed → Run B incremental no-change) on a 5-node DAG isolating the `incremental_capable=False` propagation chain.
**Tracks**: `BACKLOG.md` §P1.17e acceptance + `LIMITS.md` §P1.17-L7 resolution.

## What this verifies

P1.17e replaces V1's unconditional bronze `WHEN MATCHED THEN UPDATE SET *` with a payload-diff-gated variant: `WHEN MATCHED AND (<OR-joined IS DISTINCT FROM clauses over non-audit data cols>) THEN UPDATE SET *`. On a no-change re-extract cycle of an `incremental_capable=False` PVO, the predicate evaluates `false` on every matched row, the bronze UPDATE is suppressed, `_extract_ts` is NOT rewritten, and the downstream silver/gold MERGE source predicate `WHERE bronze_extract_ts > <prior_silver_watermark>` matches zero rows — breaking the silver/gold-seed-mode-cost-on-every-cycle propagation chain documented in LIMITS §P1.17-L7.

TC30b proves the optimization fires end-to-end against a real Delta engine. The contract has three layers, each with their own evidence:

1. **Helper-level SQL shape** — `tests/unit/test_p117_builder_merge_sql.py::TestPayloadDiffPredicateHelper` + `::TestBronzeMergePayloadDiffSQLShape` (8 unit tests).
2. **Dispatch-boundary wiring** — `tests/unit/test_p117_orchestrator_dispatch.py::TestBronzeMergeSql::test_incremental_renders_payload_diff_predicate_excluding_audit_cols` (E4).
3. **End-to-end engine behavior on real Delta tables** — this document (TC30b).

All three layers are required; none replaces the other.

## Scope (5 nodes)

| Layer | Node | Why included |
|---|---|---|
| bronze | `gl_coa` | `incremental_capable=False`; feeds `silver.dim_account` per `registry.py:195` → proves the silver-propagation cutoff |
| bronze | `gl_period_balances` | `incremental_capable=False`; feeds `gold.gl_balance` directly per `registry.py:228` → proves the gold-propagation cutoff |
| silver | `dim_account` | `depends_on_bronze=("gl_coa",)`; downstream cutoff sentinel |
| silver | `dim_calendar` | parameter-driven (zero bronze deps); included only to mirror the orchestrator-shape default (~10s cost) |
| gold | `gl_balance` | `depends_on_bronze=("gl_period_balances",)` + `depends_on_silver=("dim_account",)` — exercises both propagation paths simultaneously |

`bundle.tc30b.yaml` lives at `dev/bundle.tc30b.yaml` (gitignored). Generator: `dev/_make_tc30b_bundle.py`.

## Cross-references

- `BACKLOG.md` §P1.17e — backlog entry this feature implements.
- `LIMITS.md` §P1.17-L7 — limit resolved by this feature (moved to §Resolved 2026-06-02).
- `docs/features/p1.17e-bronze-merge-payload-diff/idea.md` — problem statement + topology.
- `docs/features/p1.17e-bronze-merge-payload-diff/plan.md` — implementation plan + per-layer assertions.
- `tests/live/TC30a_p117_incremental_merge_proof.md` — sibling baseline (V1 MERGE behavior).
- `scripts/oracle_ai_data_platform_fusion_bundle/orchestrator/__init__.py` — `_payload_diff_predicate_sql` helper + bronze MERGE renderer.
- `scripts/oracle_ai_data_platform_fusion_bundle/orchestrator/runtime.py` — `BRONZE_AUDIT_COLUMNS` canonical set.

---

## Live evidence — Run A (seed mode)

**Setup** (placeholders pending operator-side fill):

- `run_id`: `<RUN_A_ID>`
- `bundle.project`: `tc30b-payload-diff-proof`
- `bundle.version`: `0.2.0`
- Mode: `seed`
- Wall time: `<seconds>` reported / `<seconds>` orchestrator-wall
- Dispatched via `.claude/skills/fusion-tc26-run/dispatch.py --scope custom --bundle-path dev/bundle.tc30b.yaml`

**Purpose**: establish initial bronze + silver + gold cursors in `fusion_bundle_state`. Captures baseline row counts for Run B's assertions. Must run before Run B because Run B is incremental and P1.17 + P1.17c preflight would otherwise raise `IncrementalCursorMissingError` / `IncrementalTargetMissingError` against a fresh state table.

### Per-step table (Run A)

```
<paste from executed-notebook cell 3 stdout — AIDP_LIVE_TEST_RESULT marker payload>
```

Expected node mix: 2 bronze success (`gl_coa`, `gl_period_balances`) + 2 silver success (`dim_account`, `dim_calendar`) + 1 gold success (`gl_balance`). Zero deferred / failed / skipped.

---

## Live evidence — Run B (incremental mode, no Fusion-side change)

**Setup** (placeholders pending operator-side fill):

- `run_id`: `<RUN_B_ID>`
- Mode: `incremental`
- Wall time: `<seconds>`

**Purpose**: prove the P1.17e payload-diff predicate fires end-to-end. With no Fusion-side data change between Run A and Run B, the bronze re-extract carries identical payloads on every row → bronze MERGE's `WHEN MATCHED AND (<payload-diff>)` predicate evaluates `false` on every match → `_extract_ts` is NOT rewritten → downstream silver/gold MERGE source filters match zero rows.

### Per-step table (Run B)

```
<paste from executed-notebook cell 3 stdout — AIDP_LIVE_TEST_RESULT marker payload>
```

### Per-layer Delta history assertions

Each layer is verified via `DESCRIBE HISTORY <table>` (cell 4 of the dispatcher notebook).

| Layer | Expected acceptance | Observed |
|---|---|---|
| `bronze.gl_coa` | post-Run-B MERGE commit with `operationMetrics.numTargetRowsUpdated = 0` | `<paste>` |
| `bronze.gl_period_balances` | post-Run-B MERGE commit with `operationMetrics.numTargetRowsUpdated = 0` | `<paste>` |
| `silver.dim_account` | either no new MERGE commit between Run A and Run B OR one with `numTargetRowsUpdated=0` AND `numTargetRowsInserted=0` | `<paste>` |
| `silver.dim_calendar` | exempt (parameter-driven; `CREATE OR REPLACE` every cycle by design) — no assertion | n/a |
| `gold.gl_balance` | either no new MERGE commit between Run A and Run B OR one with `numTargetRowsUpdated=0` AND `numTargetRowsInserted=0`. `transforms/gold/gl_balance.py:365` always emits a `MERGE INTO` statement under `refresh_mode='incremental'` regardless of source size, so Delta will record a zero-metrics commit even when the source predicate matches zero rows — the dual acceptance shape accommodates that. | `<paste>` |

### Behavioral contrast

**Pre-P1.17e baseline** (current main behavior before this feature shipped): bronze MERGE rewrites every matched row's `_extract_ts`. Downstream:

- `silver.dim_account` re-MERGEs all 63K coa rows on every incremental cycle.
- `gold.gl_balance` re-MERGEs all 10M+ rows on every incremental cycle.

**Post-P1.17e** (this feature): bronze MERGE evaluates payload-diff `false` on every row, doesn't rewrite `_extract_ts`, downstream silver/gold MERGE source predicate matches zero rows. Gold `gl_balance` still emits a MERGE statement per its builder shape, but Delta records it as a zero-metrics commit.

### Wall-time delta (optional)

| Cycle | Pre-P1.17e wall | Post-P1.17e wall | Saving |
|---|---|---|---|
| Run B (no-change incremental) | `<seconds>` | `<seconds>` | `<seconds / %>` |

Not a hard acceptance — useful for the PR description to quantify the cost-saving claim against real cluster numbers.

---

## What this DOES NOT verify

- **`incremental_capable=True` PVOs** — those PVOs (`ap_invoices`, `erp_suppliers`, etc.) use BICC's `IncrementalDateOnly` filter at extract time, so they never re-extract unchanged rows in the first place. The payload-diff predicate is harmless for them but doesn't fire (the source DataFrame already contains only actually-changed rows). Out of scope for TC30b.
- **`ap_aging_periods`** — the third `incremental_capable=False` PVO is currently a `DeferredSpec` (`KNOWN_DEFERRED_DATASETS["ap_aging_periods"]`) until P1.10b ships the SAAS_BATCH extractor. Add to TC30b's scope when that PVO becomes live-extractable.
- **Non-`saasfademo1` tenant** — same blocker as P3.7 / P3.9 across all live evidence.
- **`P1.17a + P1.17b` aggregate-MERGE pattern** — separate feature; would shift `supplier_spend.incremental_capable` from `False` to `True` and is its own M-effort follow-up.
- **Schema evolution under MERGE** — P1.17d (not yet shipped).

## Failure-mode probes (deferred)

These are useful future extensions of TC30b but are NOT part of the P1.17e acceptance:

1. **Single-column change probe** — Run A seed; manually `UPDATE bronze.gl_coa SET <one_data_col> = <new_value> WHERE ...` between A and B; assert Run B's bronze MERGE shows `numTargetRowsUpdated > 0` ONLY for the affected rows. Proves the predicate doesn't false-suppress real changes.
2. **All-audit-column-only schema probe** — synthesize a bronze schema with only audit columns; assert the renderer falls back to V1 unconditional `WHEN MATCHED THEN UPDATE SET *` shape via the `predicate is None` branch.

Both belong to a future hardening PR, not P1.17e's acceptance scope.

## Dispatcher metadata (redacted)

```
aidp-id        : <REDACTED — AIDP datalake OCID held by the operator>
workspace-key  : <REDACTED — workspace UUID>
cluster-key    : <REDACTED — cluster UUID>
fusion pod     : <REDACTED — Fusion demo pod base URL>
fusion user    : <REDACTED — BICC user>
storage profile: <REDACTED — BICC External Storage profile name>
secret entry   : <REDACTED — AIDP credential store entry name>
bundle         : tc30b-payload-diff-proof (2 bronze + 2 silver + 1 gold)
```
