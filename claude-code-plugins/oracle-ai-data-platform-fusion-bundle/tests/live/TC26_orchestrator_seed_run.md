# TC26 — Orchestrator seed run (Phase α end-to-end)

**Test case ID**: TC26
**Status**: 🟡 **PROCEDURE READY — pending live execution on `fusion_bundle_dev` / `amitV2` workspace**
**Tracks**: PLAN_P1.5_orchestrator.md §8 "Live evidence TC26" acceptance criterion + P1.5α-fix9 closing evidence

## What this verifies

The cumulative behavior of the P1.5α phases 1–5 work, in one live run:

- **§4.4 run loop** end-to-end: load_bundle → resolve_plan → credential preflight → Spark bootstrap → ensure_state_table → preflight extras → dispatch loop with two-phase cascade
- **§3.2 state-table contract**: every plan node lands exactly one row per run; `layer`, `status`, `skip_reason` columns populate per the contract
- **§3.5 bronze audit columns**: `_extract_ts`, `_source_pvo`, `_run_id`, `_watermark_used` enriched on every bronze row
- **§3.5a silver/gold audit columns (B3)**: `silver_run_id` / `gold_run_id` populated; joins back to `fusion_bundle_state.run_id` cleanly
- **§4.4c mode validation**: clean exit-2 on `--mode full`, zero Spark side effects
- **§4.4d Option L**: `bundle.version: "0.2.0"` accepted; missing/old version surfaces as `BundleVersionMismatchError` at exit 2
- **§4.9 + B5 credential preflight**: `${vault:OCID}` resolution; bad OCID exits 2 with zero Spark calls
- **B1.1 skip_reason discriminator**: cascade vs aborted distinguishable structurally (no substring parsing)
- **§4.8a Option A catalog migration** + **§4.3 KNOWN_DEFERRED_DATASETS**: deferred datasets (`hcm_worker_assignments`, `ap_aging_periods`) produce `status='deferred'` state rows rather than crashing
- **§8 invariant lints** pass at import (no catalog↔registry drift; no cross-registry name collisions)

## Pre-flight checklist

Run these BEFORE attempting the live execution.

```bash
# 1. Plugin checkout + tests green
cd <plugin-checkout>
.venv/bin/python -m pytest tests/unit -q
# Expected: 482 passed, 0 skipped

# 2. AIDP workspace identifiers (from RESEARCH_aidp_rest_api_probe_results.md §1)
export AIDP_HOST="https://datalake.us-ashburn-1.oci.oraclecloud.com"
export AIDP_ID="<AIDP_ID>"                     # ocid1.datalake.oc1.<region>.<tenancy-specific>
export AIDP_WORKSPACE_KEY="<WORKSPACE_KEY>"    # UUID, e.g. playground workspace
export AIDP_CLUSTER_KEY="<CLUSTER_KEY>"        # UUID, e.g. fusion_bundle_dev

# 3. Cluster state — must be ACTIVE
oci raw-request --target-uri \
  "${AIDP_HOST}/20260430/aiDataPlatforms/${AIDP_ID}/workspaces/${AIDP_WORKSPACE_KEY}/clusters/${AIDP_CLUSTER_KEY}" \
  --http-method GET | python3 -c "import json,sys; d=json.load(sys.stdin)['data']; print('state:', d['state'])"
# Expected: state: ACTIVE  (if STOPPED, POST .../actions/start with body {})

# 4. BICC credentials reachable (one of):
#    a) ${vault:OCID} in bundle.yaml + AIDP runtime identity has SECRET_FAMILY_READ
#    b) ${env:FUSION_BICC_PASSWORD} exported in the notebook session
```

## Execution procedure

### Path A — Inline from an AIDP notebook (architectural primary)

1. Upload `notebooks/run_orchestrator.ipynb` + your `bundle.yaml` to the AIDP workspace at `/Workspace/Shared/fusion-bundle/`.
2. Open `run_orchestrator.ipynb` in the AIDP workbench, attach `fusion_bundle_dev` as the cluster.
3. Run all cells. Cell 2 prints the per-step table; cell 3 inspects `fusion_bundle_state` + verifies SOX-trail audit columns on materialized silver/gold tables.
4. Cell 2 also emits the canonical `AIDP_LIVE_TEST_RESULT_BEGIN <json> AIDP_LIVE_TEST_RESULT_END` markers AND attempts `oidlUtils.notebook.exit(json)` (per the probe-results doc §10.6 — the marker pattern is the reliable channel; oidlUtils may not be available).

### Path B — Via REST dispatch from a laptop (BACKLOG P1.5ε; empirically validated, not wired in α)

`commands/run.py::_run_via_aidp_dispatch` is a stub today (Phase 5). The REST-dispatch primitives have been empirically validated end-to-end against the same workspace (`RESEARCH_aidp_rest_api_probe_results.md` §10) — `POST /jobs` → `POST /jobRuns` → poll → `fetchOutput` returned the test summary. When P1.5ε ships, this same notebook ships through that channel without modification.

## Expected outputs

### Per-step table (cell 2 stdout)

For `examples/full_finance.yaml` against `fusion_bundle_dev`:

```
run_id=<uuid4>
steps: 14 ok, 0 failed, 0 skipped, 4 deferred (≈90.0s total)
  bronze  erp_suppliers              success     rows=229
  bronze  ap_invoices                success     rows=49985
  bronze  ap_payments                success     rows=<N>
  bronze  ar_invoices                success     rows=<N>
  bronze  ar_receipts                success     rows=<N>
  bronze  gl_coa                     success     rows=<N>
  bronze  gl_journal_lines           success     rows=<N>
  bronze  gl_period_balances         success     rows=<N>
  bronze  po_orders                  success     rows=<N>
  bronze  po_receipts                success     rows=<N>
  bronze  scm_items                  success     rows=<N>
  silver  dim_supplier               success     rows=229
  silver  dim_account                success     rows=<N>
  silver  dim_calendar               success     rows=4018
  silver  dim_org                    deferred                rows=-     # P1.7 ref in error_message
  silver  dim_item                   deferred                rows=-     # P1.6 ref
  gold    ap_aging                   success     rows=132   # matches TC24
  gold    gl_balance                 success     rows=10180000  # matches TC23 (10.18M)
  gold    supplier_spend             success     rows=<N>
  gold    ar_aging                   deferred                rows=-     # P1.10 ref
  gold    po_backlog                 deferred                rows=-     # P1.11 ref
```

### State-table query (cell 3)

```sql
WITH ranked AS (
  SELECT dataset_id, layer, mode, status, row_count, skip_reason,
         duration_seconds, last_run_at,
         ROW_NUMBER() OVER (PARTITION BY dataset_id ORDER BY last_run_at DESC) AS rn
  FROM fusion_catalog.bronze.fusion_bundle_state
  WHERE run_id = '<our run_id>'
)
SELECT * FROM ranked WHERE rn = 1 ORDER BY layer, dataset_id
```

Expected: ~20 rows (one per plan node). `status='success'` for the 14 shipped builds; `status='deferred'` for the 4 deferred names; zero `failed` or `skipped` in a clean run.

### SOX-trail audit columns (cell 3 secondary verification)

```sql
SELECT silver_run_id FROM fusion_catalog.silver.dim_supplier LIMIT 3
-- Expected: every row's silver_run_id == <our run_id> (the literal embedded
-- by dim_supplier.build_dim_supplier_sql(run_id=...) at SQL-construction time)

SELECT gold_run_id FROM fusion_catalog.gold.ap_aging LIMIT 3
-- Same shape for gold marts.
```

## Failure-mode probes (run after the happy path)

These exercise the cascade + abort-remaining contracts. Each requires a deliberate failure injection (e.g. temporarily revoke BICC access to one PVO, or mock the extractor).

| Probe | Inject | Expected `RunSummary` |
|---|---|---|
| Cascade (linear) | `ap_invoices` extractor raises | `failed` row for ap_invoices; cascade-`skipped` rows for `supplier_spend`, `ap_aging` (downstream); abort-`skipped` rows for every other plan node not yet attempted. **Every plan node has exactly one row.** |
| Failing gold leaf | `gl_balance` builder raises | `failed` row for gl_balance; no downstreams (gold leaf); abort-`skipped` rows for any remaining gold marts. `_skip_dependents` is no-op-safe (zero downstreams). |
| Bad credential | Replace bundle.fusion.password with `${vault:ocid1.bogus}` | Run exits 2 via `CredentialResolutionError` at preflight; **zero Spark calls, zero state-table writes** (the load-bearing reorder check). No partial run rows. |
| `--mode full` | Pass `--mode full` to the CLI | Click rejects at parse time with `'full' is not one of ...`; exit 2. Zero Python execution downstream. |

## Captured evidence

When the live run completes:

1. Copy the per-step table from cell 2 stdout into the section below.
2. Copy the state-table query output from cell 3.
3. Confirm the SOX-trail audit columns on one silver + one gold table.
4. Note any deviations from expected — every deviation is either (a) a real bug needing follow-up or (b) a missing assumption in this procedure that needs documenting.

### Live evidence (to be filled in)

```
run_id: <fill in>
captured_at: <fill in ISO timestamp>
cluster: fusion_bundle_dev (<CLUSTER_KEY>)
workspace: playground / amitV2
bundle: <which bundle.yaml + version>

--- per-step table ---
<paste cell 2 output here>

--- state-table query result ---
<paste cell 3 output here>

--- SOX-trail audit column samples ---
silver.dim_supplier.silver_run_id:
<paste 3 rows>

gold.ap_aging.gold_run_id:
<paste 3 rows>
```

## Cross-references

- `PLAN_P1.5_orchestrator.md` §5 step 9 (Live run + TC26)
- `PLAN_P1.5_orchestrator.md` §8 acceptance criteria
- `RESEARCH_aidp_rest_api_probe_results.md` — workspace/cluster identifiers + REST dispatch evidence
- Commits: `9e15d79` (P0) → `c6f4ace` (Phase 2) → `f113fb2` (Phase 3) → `2df8cc3` (Phase 4) → `7f57d38` (Phase 5)
- Prior live TCs: TC23 (gl_balance 10.18M rows), TC24 (ap_aging 132 rows), TC8 (supplier_spend) — TC26 reproduces these numbers through the orchestrator instead of by-hand `build()` calls.
