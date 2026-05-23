# TC27 — Resume from checkpoint (P1.5α-fix21 acceptance evidence)

**Test case ID**: TC27
**Status**: ⏳ **TEMPLATE — dispatch pending** on `fusion_bundle_dev` cluster / `amitV2` AIDP workspace. Implementation shipped in PR <pr-number>; capture this file's "Recorded" sections after running the procedure below.
**Tracks**: `P1.5α-fix21` acceptance criterion in BACKLOG.md — "deliberate kill-mid-run + `--resume` produces a complete pipeline in (resume time) ≪ (clean run time)."

## What this verifies

The P1.5α-fix21 resume contract, end-to-end on a live tenant:

- **CLI surface**: `aidp-fusion-bundle run --inline --mode seed --resume <run_id>` is wired through to `commands/run.py` and the orchestrator's resume flow.
- **State read**: `read_resumable_state` reads `fusion_bundle_state` for the stored run_id, returns a `ResumeContext` carrying `succeeded` (= `'success'` ∪ `'resumed_skipped'`), `plan_hash`, `plan_snapshot`, `succeeded_schemas`.
- **Scope reconstruction**: bare `--resume` (no `--datasets` / `--layers`) rebuilds the original dispatch scope from `plan_snapshot.nodes`.
- **Preflight narrowing**: `preflight_bronze_schemas` is invoked only for un-succeeded bronze nodes. Already-succeeded bronze schemas are pulled from `plan_snapshot` (no re-probe via BICC).
- **Drift gate**: `hash_resolved_plan` blends preflight schemas with succeeded-snapshot schemas; mismatch against `resume_context.plan_hash` raises `ResumeBundleMismatchError` BEFORE any dispatch.
- **Extra-dep preflight**: `compute_reattempt_extra_deps` augments `_preflight_external_deps` so a manually-dropped upstream succeeded-bronze table fails fast as `PrerequisiteError` instead of a mid-flight crash.
- **Dispatch loop**: succeeded nodes emit `RunStep.resumed_skip(...)` with the ORIGINAL run_id; un-succeeded nodes re-dispatch under the original run_id (preserving the CLAUDE.md medallion `_run_id` invariant).
- **State-row continuity**: every row written by the resumed run carries the same `plan_hash` + `plan_snapshot` as the original, threaded through the factory kwargs.
- **`fusion_bundle_state_latest` VIEW**: created by `ensure_state_table`'s ALTER + CREATE OR REPLACE VIEW pass; projects one row per `(run_id, dataset_id)` so consumers don't see the append-only multi-row noise.

## Pre-flight checklist

```bash
# 1. Plugin checkout + tests green (including new fix21 modules)
cd <plugin-checkout>
.venv/bin/python -m pytest tests/unit tests/integration -q
# Expected: 624 passed (566 pre-fix21 + 18 plan_hash + 8 drift renderer
# + 8 resume + 8 state migration + 6 CLI resume + 6 runtime additions
# + 4 chaos integration)

# 2. AIDP workspace identifiers (supply via env vars or .aidp.env)
export AIDP_HOST="https://datalake.us-ashburn-1.oci.oraclecloud.com"
export AIDP_ID="<AIDP_ID>"
export AIDP_WORKSPACE_KEY="<WORKSPACE_KEY>"      # amitV2 / playground
export AIDP_CLUSTER_KEY="<CLUSTER_KEY>"          # fusion_bundle_dev

# 3. Confirm cluster ACTIVE (re-uses TC26 procedure).

# 4. Confirm BICC credentials current on saasfademo1 (Casey.Brown or natalie.salesrep).
```

## Procedure

### Phase 1 — Capture a clean baseline (run R_clean)

1. Dispatch the full happy-path via `/fusion-tc26-run` against `fusion_bundle_dev`.
2. Record the run_id (`R_clean`), wall time, and final `fusion_bundle_state` row count for the run_id.
3. **Latest-per-(run_id, dataset_id)** projection (NOT the global `commands/run.py:308` query — that's unsafe on a shared state table with concurrent runs):

```sql
WITH ranked AS (
  SELECT
    dataset_id, layer, mode, last_run_at, status, row_count,
    error_message, skip_reason, duration_seconds, plan_hash, plan_snapshot,
    ROW_NUMBER() OVER (
      PARTITION BY dataset_id
      ORDER BY last_run_at DESC
    ) AS rn
  FROM fusion_catalog.bronze.fusion_bundle_state
  WHERE run_id = '<R_clean>'                  -- ← REQUIRED in CTE, before window
)
SELECT * FROM ranked WHERE rn = 1 ORDER BY layer, dataset_id;
```

Persist the projection as `tc27_clean_baseline.csv`.

### Phase 2 — Kill mid-run + resume (run R_resume)

1. Re-dispatch via `/fusion-tc26-run`. Capture the new run_id (`R_resume_initial` — this is the run that will be killed and resumed).
2. Wait for ~5 bronze nodes to write `status='success'` (poll the JobRun until ~3 minutes in, then `Cancel` the AIDP JobRun).
3. Confirm `fusion_bundle_state` has a mix of `status` values under `R_resume_initial`:
   - ~5 `success` (the bronze nodes that completed)
   - ~5 `skipped` (cascade + abort fallout from the kill)
4. Dispatch the resume:
   ```bash
   aidp-fusion-bundle run --inline --mode seed --resume <R_resume_initial>
   ```
   (Or via `/fusion-tc26-run` once it accepts `--resume`.)
5. Record wall time (`Δt_resume`) and the `RunSummary` printout from the resumed run.

### Phase 3 — Verify

Compare the post-resume latest-per-(run_id, dataset_id) projection against the clean baseline:

```sql
WITH ranked AS (
  SELECT ..., ROW_NUMBER() OVER (
    PARTITION BY dataset_id ORDER BY last_run_at DESC
  ) AS rn
  FROM fusion_catalog.bronze.fusion_bundle_state
  WHERE run_id = '<R_resume_initial>'
)
SELECT * FROM ranked WHERE rn = 1 ORDER BY layer, dataset_id;
```

Assertions (record PASS/FAIL per row):

| # | Assertion | Recorded |
|---|---|---|
| 1 | Resume runtime ≪ clean runtime (resume skips succeeded nodes) | `Δt_resume = ?s` vs `Δt_clean = ?s` |
| 2 | Latest-per-dataset projection matches clean baseline for `(dataset_id, layer, mode, row_count)` | _ |
| 3 | Final silver/gold table row counts match TC26 baselines | _ |
| 4 | The `<R_resume_initial>` run has resumed_skipped rows for previously-succeeded nodes | _ |
| 5 | Every row carries the same `plan_hash` (single value across the run) | _ |
| 6 | Every silver/gold row in target tables has `<layer>_run_id = <R_resume_initial>` (NOT a new UUID) | _ |
| 7 | `fusion_bundle_state_latest` VIEW returns one row per dataset_id for `<R_resume_initial>` | _ |

### Phase 4 — Cross-tabulation (audit story)

```sql
SELECT dataset_id, status, COUNT(*) AS row_count
FROM fusion_catalog.bronze.fusion_bundle_state
WHERE run_id = '<R_resume_initial>'
GROUP BY dataset_id, status
ORDER BY dataset_id, status;
```

Document each `(dataset_id, status)` count — this is the explicit audit story for the resumed run. Expected shape:

| dataset_id | status | count | what this means |
|---|---|---|---|
| ap_invoices | success | 1 | original run wrote success at T0 |
| ap_invoices | resumed_skipped | 1 | resume carry-forward at T1 |
| gl_period_balances | skipped | 1 | original cascade-skip after the kill |
| gl_period_balances | success | 1 | resume re-dispatched + succeeded |
| … | … | … | … |

## Recorded results

### Run identifiers

- Clean baseline run_id: `<R_clean>` (captured _DATE_)
- Resumed run_id: `<R_resume_initial>` (captured _DATE_)

### Wall-time comparison

- `Δt_clean`: `<NN>s`
- `Δt_resume`: `<NN>s`
- Ratio: `Δt_resume / Δt_clean = <0.XX>` (target ≪ 1.0)

### Latest-per-dataset diff

_(Attach `tc27_resume_latest.csv` and a diff against `tc27_clean_baseline.csv` here. Should be a no-op diff — every dataset's terminal row matches the clean baseline.)_

### Cross-tabulation

_(Paste the Phase 4 query output here.)_

### Visual checks

_(Optional screenshots of the AIDP JobRun UI showing the kill + the resume, and the CLI's "Resuming run …" banner.)_

## Failure modes — what to record if any assertion fails

- Resume re-extracted a previously-succeeded bronze: state-row write contract broken. Capture the offending dataset_id + the two rows under the same run_id.
- Drift gate fired on the resume despite identical bundle: hash blend miscomputes succeeded-node schemas. Capture the `ResumeBundleMismatchError` message (it'll include both the identity diff and the stored vs current hashes).
- Resume produced a different `run_id` than the original: factory threading broken — the CLAUDE.md medallion invariant is violated. Investigate `orchestrator.__init__.run` immediately.

## Cross-references

- BACKLOG.md §P1.5α-fix21 (the canonical entry — strike-through with this evidence post-execution)
- TC26 evidence file — baseline for the clean-run comparison
- `docs/features/fix21-resume-from-checkpoint/plan.md` — the implementation plan this exercises
- LIMITS.md §L-Resume — known resume-specific caveats
