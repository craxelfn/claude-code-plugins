# TC27 — Resume from checkpoint (P1.5α-fix21 acceptance evidence)

**Test case ID**: TC27
**Status**: ✅ **EXECUTED 2026-05-23 21:30 UTC** against a live AIDP cluster on a Fusion demo tenant via OCI-signed REST dispatch. Coordinates redacted per the TC26 evidence convention; full identifiers held by the dispatching operator.
**Tracks**: `P1.5α-fix21` acceptance criterion in BACKLOG.md — "deliberate kill-mid-run + `--resume` produces a complete pipeline in (resume time) ≪ (clean run time)."

## What this verifies (all PASS)

| # | Assertion | Evidence |
|---|---|---|
| 1 | `aidp-fusion-bundle run --inline --resume <run_id>` end-to-end works on a live tenant | Phase 3 ran successfully, all 5 plan nodes terminal-success |
| 2 | **Original `run_id` preserved** — same UUID across original + resume; medallion `<layer>_run_id` invariant intact | Phase 2 and Phase 3 emit identical `run_id` in their RunSummary |
| 3 | Succeeded nodes carry forward as `resumed_skipped` with `skip_reason='resume-skip'`, `duration_seconds=0.0` | 3 nodes carried forward (erp_suppliers + ap_invoices + dim_calendar) |
| 4 | Failed + cascade-skipped nodes re-attempt + succeed on resume | dim_supplier (silver) + supplier_spend (gold) re-dispatched, both succeeded |
| 5 | Resume runtime ≪ clean runtime | **Phase 3 67.7s vs Phase 1 335.8s = 5× speedup** |
| 6 | Plan hash identical across all rows under the resumed run_id (drift gate didn't fire — same bundle) | All 10 rows share the same `plan_hash` |
| 7 | State table is append-only on resume — multi-row per `(run_id, dataset_id)` | Cross-tab below shows 2 rows per dataset under the same run_id |
| 8 | `fusion_bundle_state_latest` projection gives one row per dataset with terminal state | Window-projected table below shows 5 rows, terminal status per dataset |

## Coordinates (redacted)

```
aidp-id        : <REDACTED — AIDP datalake OCID held by the operator>
workspace-key  : <REDACTED — workspace UUID>
cluster-key    : <REDACTED — cluster UUID>
fusion pod     : <REDACTED — Fusion demo pod base URL>
fusion user    : <REDACTED — BICC user>
storage profile: <REDACTED — BICC External Storage profile name>
secret entry   : <REDACTED — AIDP credential store entry name>
bundle         : tc26-narrow-probe (2 bronze + 2 silver + 1 gold)
```

Bundle scope (narrow): `erp_suppliers`, `ap_invoices` (bronze) → `dim_supplier`, `dim_calendar` (silver) → `supplier_spend` (gold). Same shape as `dispatch.NARROW_BUNDLE` in `.claude/skills/fusion-tc26-run/dispatch.py`.

## Run identifiers

Phase | run_id | JobRun terminal | Wall time
---|---|---|---
1 — clean baseline   | `5c03905b-…` (Phase 1 orchestrator UUID — full UUID safe to share, internal correlation only) | SUCCESS | **335.8s**
2 — induced failure  | `6bebf134-…` (Phase 2 orchestrator UUID) | SUCCESS (cluster) / 1 failed step | 278.9s
3 — resume           | `6bebf134-…` (SAME as Phase 2 — preserved) | SUCCESS | **67.7s**

Full orchestrator UUIDs available in the operator's local executed-notebook captures. JobRun keys are redacted per TC26 convention (cluster-side identifiers).

**Δt_resume / Δt_clean = 67.7s / 335.8s ≈ 0.20** — resume is 5× faster than re-running from scratch on this narrow scope. On a full-finance bundle (~11 bronze with `gl_period_balances` ~10M rows + ~25min wall), the speedup amplifies further; the narrow probe demonstrates the contract.

## Per-phase RunSummary

### Phase 1 — clean baseline

```
PHASE_1_CLEAN wall=335.8s
  bronze  erp_suppliers             success                         rows=       209  dur=91.63s
  bronze  ap_invoices               success                         rows=     49552  dur=104.53s
  silver  dim_calendar              success                         rows=      4018  dur= 8.71s
  silver  dim_supplier              success                         rows=       209  dur=10.14s
  gold    supplier_spend            success                         rows=       309  dur=11.05s

5 success, 0 failed, 0 skipped, 0 resumed_skipped, 0 deferred
total_duration=226.06s  wall=335.79s
```

### Phase 2 — induced failure (monkeypatch dim_supplier silver builder → RuntimeError)

```
PHASE_2_INDUCED_FAIL wall=278.9s
  bronze  erp_suppliers             success                         rows=       209  dur=80.98s
  bronze  ap_invoices               success                         rows=     49552  dur=110.32s
  silver  dim_calendar              success                         rows=      4018  dur=15.18s
  silver  dim_supplier              failed                          rows=         -  dur= 0.00s  err=RuntimeError("TC27 induced failure: …")
  gold    supplier_spend            skipped          [cascade]      rows=         -  dur= 0.00s

3 success, 1 failed, 1 skipped (cascade), 0 resumed_skipped, 0 deferred
```

Three datasets land terminal-success (their tables are on disk), one fails (dim_supplier), one cascade-skips (supplier_spend depends on dim_supplier).

### Phase 3 — resume

```
PHASE_3_RESUME wall=67.7s
  bronze  erp_suppliers             resumed_skipped  [resume-skip]  rows=         -  dur=0.00s
  bronze  ap_invoices               resumed_skipped  [resume-skip]  rows=         -  dur=0.00s
  silver  dim_calendar              resumed_skipped  [resume-skip]  rows=         -  dur=0.00s
  silver  dim_supplier              success                         rows=       209  dur=9.01s
  gold    supplier_spend            success                         rows=       309  dur=21.44s

2 success, 0 failed, 0 skipped, 3 resumed_skipped, 0 deferred
total_duration=30.45s  wall=67.66s
```

- `run_id` identical to Phase 2 — preserved on resume per the medallion `<layer>_run_id` invariant.
- 3 carry-forwards: `erp_suppliers`, `ap_invoices`, `dim_calendar` (all `success` in Phase 2 → `resumed_skipped` here).
- 2 re-dispatches: `dim_supplier` (Phase 2 `failed` → now `success`, 209 rows match baseline), `supplier_spend` (Phase 2 `skipped` cascade → now `success`, 309 rows match baseline).

## State-table evidence (queried inside Phase 3 notebook)

### Latest-per-`(run_id, dataset_id)` projection

```
+--------------+------+---------------+---------+-----------+-----------------+
|dataset_id    |layer |status         |row_count|skip_reason|duration_seconds |
+--------------+------+---------------+---------+-----------+-----------------+
|ap_invoices   |bronze|resumed_skipped|NULL     |resume-skip|0.0              |
|erp_suppliers |bronze|resumed_skipped|NULL     |resume-skip|0.0              |
|supplier_spend|gold  |success        |309      |NULL       |21.4356974       |
|dim_calendar  |silver|resumed_skipped|NULL     |resume-skip|0.0              |
|dim_supplier  |silver|success        |209      |NULL       |9.0109928        |
+--------------+------+---------------+---------+-----------+-----------------+
```

- One row per dataset (the projection collapses the multi-row history).
- All 5 rows share the same `plan_hash` (truncated for display) — drift gate didn't fire (same bundle).
- Row counts match Phase 1 baseline (209 suppliers, 309 supplier-spend rows).

### Cross-tab — full append-only history under the resumed `run_id`

```
+--------------+---------------+---------+
|dataset_id    |status         |row_count|
+--------------+---------------+---------+
|ap_invoices   |success        |1        |    ← Phase 2: bronze succeeded
|ap_invoices   |resumed_skipped|1        |    ← Phase 3: carry-forward
|dim_calendar  |success        |1        |    ← Phase 2: silver succeeded
|dim_calendar  |resumed_skipped|1        |    ← Phase 3: carry-forward
|dim_supplier  |failed         |1        |    ← Phase 2: monkeypatch raised
|dim_supplier  |success        |1        |    ← Phase 3: re-dispatch succeeded
|erp_suppliers |success        |1        |    ← Phase 2: bronze succeeded
|erp_suppliers |resumed_skipped|1        |    ← Phase 3: carry-forward
|supplier_spend|skipped        |1        |    ← Phase 2: cascade-skipped
|supplier_spend|success        |1        |    ← Phase 3: re-dispatch succeeded
+--------------+---------------+---------+

10 rows total, 5 datasets × 2 attempts each.
```

This is exactly the append-only multi-row semantics LIMITS.md §L-Resume documents — consumers must read from `fusion_bundle_state_latest` or apply the latest-per-`(run_id, dataset_id)` window to get one row per dataset.

## Dispatcher

```bash
# All three phases in one shot (placeholders — operator fills in real values
# from their local `.aidp/aidp.config.yaml` + AIDP credential store):
.venv/bin/python .claude/skills/fusion-tc26-run/tc27_dispatch.py \
  --aidp-id        <AIDP-OCID> \
  --workspace-key  <WORKSPACE-UUID> \
  --cluster-key    <CLUSTER-UUID> \
  --cluster-name   <CLUSTER-DISPLAY-NAME> \
  --region         us-ashburn-1 \
  --fusion-service-url <FUSION-POD-URL> \
  --fusion-user        <BICC-USER> \
  --external-storage   <BICC-STORAGE-PROFILE> \
  --phases 1,2,3

# Or to retry just one phase, e.g. phase 3 against an existing failed run:
... --phases 3 --resume-run-id <R_resume_initial>
```

Executed notebooks + raw payloads are written to `/tmp/tc27-<timestamp>/` per dispatch — held locally on the operator's workstation.

## Known dispatcher notes

- **Marker-parse fragility on free-form error strings**: Phase 2's RunSummary includes an `error_message` field containing `repr(exc) = 'RuntimeError("…")'`. When this JSON marker is emitted via `print(json.dumps(...))` and captured into the notebook's `display_data text/plain`, the AIDP notebook runtime strips the JSON-escape backslashes from the nested quotes, producing invalid JSON for the dispatcher's marker parser. Fallback: extract `run_id` via a substring/regex pre-pass when the JSON load fails (Phase 2 evidence still recoverable from the notebook's pre-marker print). Tracked as a small dispatcher hardening item — non-blocking for TC27 acceptance.
- **First Phase 2 attempt aborted at preflight**: the original dispatcher monkeypatched `extractors.bicc.extract_pvo`, which also poisoned `preflight_bronze_schemas`'s schema-probe path and raised `BronzeSchemaProbeError` before any orchestrator work. Resolved by monkeypatching the `dim_supplier` silver builder instead (post-preflight boundary). Documented in `.claude/skills/fusion-tc26-run/tc27_dispatch.py:_INDUCED_FAIL_RUN_CELL`.

## Cross-references

- BACKLOG.md §P1.5α-fix21 — implementation tracking entry; this evidence closes the "live evidence (TC27 or extension to TC26 doc)" acceptance criterion.
- `docs/features/fix21-resume-from-checkpoint/plan.md` — local plan with full design decisions (untracked working notes).
- `LIMITS.md` §L-Resume — append-only multi-row state-table semantics (gitignored working notes).
- TC26 evidence file — baseline pacing reference for narrow-scope timings, redaction convention.
- PR #10 against `craxelfn/claude-code-plugins` — orchestrator + cli: P1.5α-fix21 — resume from checkpoint + chaos-test the retry classifier.
