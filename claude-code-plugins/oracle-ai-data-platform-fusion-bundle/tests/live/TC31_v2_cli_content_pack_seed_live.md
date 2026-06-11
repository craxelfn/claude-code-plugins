# TC31_v2_cli_content_pack_seed_live — Live evidence trail

**Status:** PASS — captured 2026-06-10/11 on `saasfademo1` via a dedicated
dev cluster. Operator-driven REST dispatch through the production CLI
(`aidp-fusion-bundle run`, content-pack backend — the only backend
post-Phase-9).

Infra identifiers (datalake OCID, workspace/cluster keys, pod URL, BICC
user, storage profile, password) are intentionally omitted per the
repo redaction rule; they live in the gitignored `dev/` config. `run_id`s
are shown as 8-char prefixes.

## What this validates

The v2 content-pack medallion seed runs end-to-end through the real CLI
on a live Fusion tenant, plus the six fixes/features on branch
`fix/v2-seed-bronze-gates-marker-4071`.

## A — full medallion seed (all 10 nodes)

`aidp-fusion-bundle run --mode seed` (bundle declares the finance gold
marts; D-1 transitive include pulls silver + bronze). `run_id 6694262a…`,
**10 success / 0 failed / 0 skipped**, ~28 min wall.

| dataset_id | layer | status | row_count | dur (s) |
|---|---|---|---|---|
| ap_invoices | bronze | SUCCESS | 49,552 | 88.5 |
| erp_suppliers | bronze | SUCCESS | 209 | 85.2 |
| gl_coa | bronze | SUCCESS | 63,464 | 87.4 |
| gl_period_balances | bronze | SUCCESS | 11,211,211 | 1057.3 |
| dim_account | silver | SUCCESS | 63,464 | 19.7 |
| dim_calendar | silver | SUCCESS | 4,018 | 51.1 |
| dim_supplier | silver | SUCCESS | 209 | 43.1 |
| ap_aging | gold | SUCCESS | 131 | 45.1 |
| gl_balance | gold | SUCCESS | 10,184,102 | 74.7 |
| supplier_spend | gold | SUCCESS | 309 | 40.0 |

Row counts match the Phase-4 baseline expectations recorded in
`TC_phase5_v2_default_seed_live.md` exactly. SOX audit trail verified:
`dim_account` 63,464/63,464 and `ap_aging` 131/131 rows carry the run's
`silver_run_id` / `gold_run_id`.

## B — the six fixes proven

1. **`content_pack_staging` bronze staging (`AIDPF-1045`)** — before the
   fix, every content-pack CLI dispatch failed `AIDPF-1045
   LayerFilterEmptiedPlanError` because `bronze/*.yaml` nodes were never
   staged to the cluster (empty `pack.bronze`). After: bronze nodes
   resolve and execute (table A).
2. **`AIDPF-4070` bronze subset + case-insensitive** — bronze writes the
   full raw PVO (143–232 cols) vs the curated `outputSchema`; Step-8 now
   asserts declared ⊆ materialised, case-insensitive. Live bronze passed.
3. **Marker base64 fix** — before, a run with any failed step degraded to
   `DISPATCH_MARKER_DEGRADED` (regex-recovered run_id, no table). After,
   the CLI renders the step table on success **and** failure (see C/D).
4. **`dispatch_v2_seed` stray-comma** — generated notebook compiles.
5. **Bronze YAML type alignment** — 19 columns aligned to live BICC
   (`long`→`decimal(18,0)/(38,30)`, `timestamp`→`date`); A passed Step-8.
6. **`AIDPF-4071` pre-ingest gate** — see C.

## C — `AIDPF-4071` pre-ingest gate (fail-fast)

Injected a column absent from the PVO (`DRIFT_TEST_NONEXISTENT_COL`) into
`erp_suppliers.outputSchema` and ran `--datasets erp_suppliers`:

- `run_id 37ed8760…` — `erp_suppliers` failed `source_schema_missing`
  (`AIDPF-4071`) in **12.45 s** (metadata probe only — vs ~85 s for a
  real extract). No row pull. Fail-fast confirmed.
- `run_id ae93a1d5…` — same, plus the laptop wrote
  `.aidp/diagnostics/<run_id>/AIDPF-4071__erp_suppliers.json` carrying the
  missing column + the full live PVO schema (143 cols, name+type). This is
  the `medallion-author` input.

## D — marker base64 before/after (same drift case)

- **Before:** `[DISPATCH_MARKER_DEGRADED] … summary marker is unparseable.
  Recovered run_id=… from regex fallback`. No table.
- **After (`run_id 64c05f05…`):** the CLI renders the Run summary table
  with `erp_suppliers │ bronze │ FAILED`, `0 success · 1 failed`, total
  32.6 s. `grep DISPATCH_MARKER_DEGRADED` → 0.

## Notes

- The 7 starter-pack bronze nodes with no downstream silver/gold
  (`ap_payments`, `ar_invoices`, `ar_receipts`, `gl_journal_lines`,
  `po_orders`, `po_receipts`, `scm_items`) ship with never-live-validated
  column **names** that don't match the live PVO. Tracked separately (see
  `LIMITS.md`); the `AIDPF-4071` gate now diagnoses them automatically.
- Non-`saasfademo1` tenant evidence (P3.7 / P3.9) is still outstanding for
  any "plugin-portable" claim — this run proves the demo pod only.
