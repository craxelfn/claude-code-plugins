# oracle-ai-data-platform-fusion-bundle

> **Productized Fusion → Oracle AI Data Platform pipeline.** Curated BICC extracts for Fusion ERP/HCM/SCM, bronze/silver/gold medallion in Delta, conformed COA/calendar/org/supplier/item dimensions, ready-made AR-aging / AP-aging / GL-balance / PO-backlog / Supplier-spend gold marts, and **Oracle Analytics Cloud (OAC) workbooks installable via OAC REST API**. End-user consumption via [OAC MCP (Preview)](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html) chat in Claude / Cline / Copilot.
>
> Same pattern shown in the official Oracle blog [Bring Fusion Data into AIDP Workbench Using BICC](https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc), productized.

**Status**: alpha (`0.1.0a0`) — Tier-1 features complete and live-validated end-to-end against the saasfademo1 Fusion demo pod + multiple OAC instances (`oacai.cealinfra.com` for TC10/b/c/d/h/h-2; disposable OAC1 for TC10h-3/h-4 — see [tests/live/](tests/live/) for full evidence trail). **207 unit tests passing.** **`dashboard install` validated end-to-end on OAC1 (TC10h-4, 2026-05-03)**: precheck → snapshot REGISTER → RESTORE → workRequest poll, all four documented OAC REST calls green in a single command. OAC integration uses **only Oracle-documented public REST endpoints** (snapshot-based workbook delivery; the audit rejected per-workbook `.dva` imports as UI-only). CLI commands wired: `init`, `validate`, `bootstrap`, `catalog list/probe`, `run`, `status`, `dashboard install/validate/uninstall`, `dashboard mcp-config`.

**Positioning**: This bundle is **additive to and complementary with** Oracle's managed Fusion data offerings. It productizes Option 1 of the BICC blog's three-option architecture (BICC into AIDP for "Custom AI and ML, raw data access, data engineering"). Never positioned as a replacement for FDI, OAC, OTBI, BIP, or Data Transforms — different jobs, same Oracle ecosystem.

---

## What you get (per pdf1 §"What Can You Do Once the Data is in Oracle AI Data Platform")

1. **Custom ML/AI training** on operational ERP/HCM/SCM data (PySpark + Python in AIDP notebooks)
2. **Cross-source enrichment** — join Fusion data with non-Fusion sources via the AIDP `aidataplatform` connector family
3. **Medallion architecture** — bronze (raw audit) → silver (typed + dim-joined) → gold (business marts) in Delta
4. **GenAI agent grounding** — `ai_generate("which suppliers had >$1M Q1 spend?")` against gold marts via OCI Generative AI
5. **BI & reporting via JDBC** — OAC, Tableau, Power BI consume the gold layer
6. **Delta Sharing** (v3 roadmap) — share curated datasets with other teams or external partners

> **Phase 1 vs Phase 2**:
> - **Wired in 0.1.0-alpha** (✅): BICC → bronze landing, OAC connection install (REST), `dashboard install` / `validate` / `uninstall`, MCP config emission, 207 unit tests, end-to-end live validation on disposable OAC1.
> - **Stubbed (Phase 2 / 0.2.0)** (🚧): silver/gold transforms, conformed dimensions (`dim_supplier`, `dim_account`, `dim_calendar`, `dim_item`, `dim_org`), 5 gold marts (`supplier_spend`, `gl_balance`, `ap_aging`, `ar_aging`, `po_backlog`), `.bar` release artifact.

---

## Quickstart

```bash
# 1. Install the CLI on your laptop (development install from local source)
pip install -e .

# 1a. (optional, for contributors) install test deps + run the unit suite
pip install -e '.[test]'
make test

# 2. Scaffold a bundle in your repo
mkdir my-fusion-lake && cd my-fusion-lake
aidp-fusion-bundle init

# 3. Probe prerequisites against your Fusion pod + AIDP workspace
aidp-fusion-bundle bootstrap --check-iam

# 4. Run the orchestrator (first time = full extract; subsequent = incremental)
aidp-fusion-bundle run --mode seed

# 5. Upload the bundle's snapshot .bar to your OCI Object Storage bucket.
#    Snapshots use a folder-prefixed object name; `--bar-uri` later passes the
#    Oracle-documented `file:///<folder>/<name>.bar` shape.
oci os object put --bucket-name aidp-fusion-bundle-bar \
                  --file ./bundle-v0.1.0a0.bar \
                  --name aidp-fusion-bundle/bundle-v0.1.0a0.bar

# 6. Create the AIDP connection in OAC once via the UI (one-time per OAC).
#    OAC's REST validator does not yet bless AIDP's `idljdbc` connectionType,
#    so the connection is created via the UI on first install (see step 6a).
#    Subsequent runs of `dashboard install` re-use the existing connection
#    automatically (precheck via `find_connection`).
#
# 6a. (One-time, in OAC UI) Data → Connections → Create → "Oracle AI Data
#     Platform" → upload the 6-key JSON written by `--print-only`:
aidp-fusion-bundle dashboard install --target oac --oac-url ... --print-only
# Then upload `oac/data_source/aidp_fusion_jdbc.json` + your private key PEM.

# 6b. Run the REST install (snapshot register + restore; reuses the
#     UI-created connection):
aidp-fusion-bundle dashboard install --target oac \
  --oac-url https://your-oac.example.com \
  --bar-bucket aidp-fusion-bundle-bar \
  --bar-uri 'file:///aidp-fusion-bundle/bundle-v0.1.0a0.bar'
# (See docs/oac_rest_api_setup.md for the full args + IAM/Resource Principal setup)

# 7. Print MCP config snippet for end users (paste into claude_desktop_config.json)
aidp-fusion-bundle dashboard mcp-config --oac-url https://your-oac.example.com \
  --oac-mcp-connect-js /path/to/oac-mcp-connect.js
```

After step 7, restart your AI client and ask "what's our AR aging?" — OAC MCP will route through `discoverData` → `describeData` → `executeLogicalSQL` against `fusion_catalog.gold.ar_aging`.

---

## Resuming an interrupted run (`--resume`)

A 25-minute pipeline can hit a transient BICC outage, a cluster auto-termination, or an operator Ctrl-C halfway through. Re-running from scratch eats ~14M row-writes and ~25 minutes of cluster time. `--resume` solves this — re-attempting only the failed/skipped steps under the original `run_id`.

```bash
# After an interrupted run, find the run_id you want to resume:
aidp-fusion-bundle status      # surfaces the latest fusion_bundle_state per dataset_id

# Resume by run_id (must run with --inline; REST dispatch wiring is P1.5ε scope):
aidp-fusion-bundle run --inline --mode seed --resume <run_id>
```

What happens on resume:

- The orchestrator reads `fusion_bundle_state` for `<run_id>`. Datasets whose latest terminal status is `success` or `resumed_skipped` carry forward without re-dispatch.
- All other datasets re-attempt under the **original `run_id`**, preserving the medallion `<layer>_run_id` audit invariant (one logical pipeline = one `run_id` across the resumed history).
- `preflight_bronze_schemas` only probes un-succeeded bronze nodes — already-succeeded schemas are pulled from the stored `plan_snapshot`.
- A drift gate compares the current plan + execution identity (Fusion pod URL, BICC storage, Fusion username, AIDP target paths, plugin version) against the stored hash. Any change raises `ResumeBundleMismatchError` pre-dispatch with the diff rendered: identity changes first, dataset changes second, hash echo last.

The state table becomes append-only on resumed runs — multiple rows per `(run_id, dataset_id)` are expected (failed attempt + carry-forward + eventual success). **Always read from the `fusion_bundle_state_latest` Delta VIEW** (created automatically by `ensure_state_table`), which projects one row per `(run_id, dataset_id)` via `ROW_NUMBER() OVER (PARTITION BY run_id, dataset_id ORDER BY last_run_at DESC)`. See `LIMITS.md` §L-Resume for the full consumer-side contract.

If a `--resume` raises one of:

- `ResumeRunNotFoundError` — typo in run_id, or the state table was truncated.
- `ResumeRunNotResumableError` — the run predates fix21 (`plan_hash IS NULL`) or was written by a partially-migrated build (`plan_snapshot IS NULL`). The remediation is to re-run from scratch.
- `ResumeBundleMismatchError` — bundle drift. The error message names which identity field or dataset diverged.

…the CLI exits with code 2 and no traceback (all three classes subclass `OrchestratorConfigError`).

---

## Incremental refresh (`--mode incremental`)

`--mode seed` rebuilds bronze + silver + gold from scratch on every cycle — full BICC extract, `CREATE OR REPLACE TABLE` everywhere. Fine for a fresh-tenant first run; wasteful for a daily refresh that touches the same 50M-row GL fact only a few thousand rows at a time. P1.17 ships `--mode incremental`:

- **Bronze** — BICC's `fusion.initial.extract-date` filter receives the prior run's safety-windowed watermark; the orchestrator `MERGE INTO bronze_target ... ON target.<natural_key> = src.<natural_key>` instead of `mode("overwrite")`. The overlap re-extracted by the safety window dedupes by natural key.
- **Silver `dim_supplier`, `dim_account` + Gold `gl_balance` (row-level)** — `MERGE INTO target USING (... WHERE bronze_extract_ts > <layer-local watermark>) ON target.<natural_key> = src.<natural_key>`. One bronze row changed → one silver/gold row updated.
- **Exempt marts (`supplier_spend`, `ap_aging`, `dim_calendar`)** — always run `CREATE OR REPLACE TABLE` regardless of mode. `supplier_spend`'s GROUP BY mixes a mutable fact attribute (`approval_status`) so partial-MERGE would leave stale aggregate rows on status flips (correct incremental ships in P1.17b). `ap_aging` buckets are `CURRENT_DATE()`-anchored — incremental MERGE would freeze the bucket assignment a row had on the last run, going stale by one day daily. `dim_calendar` is parameter-driven, no source watermark.

```bash
# First incremental run requires a prior --mode seed run to have populated each
# layer's last_watermark in fusion_bundle_state. The orchestrator raises
# IncrementalCursorMissingError listing every silver/gold dataset that lacks one.
aidp-fusion-bundle run --inline --mode seed              # day 1
aidp-fusion-bundle run --inline --mode incremental       # day 2+
```

### Tuning the safety window — `bundle.incremental.watermark_safety_window_seconds`

The bronze cursor is stored as `extract_started_at − safety_window` (not `extract_started_at` directly) to absorb AIDP-vs-Fusion clock skew. Default is 3600s (one hour) — wider than typical NTP-synced drift between OCI-hosted AIDP and Fusion Cloud.

```yaml
# bundle.yaml — opt in only when needed
incremental:
  watermarkSafetyWindowSeconds: 7200   # widen to 2h if observed skew exceeds 1h
```

Validated `gt=0`. Setting `0` or a negative value is rejected at bundle load — those would erase the buffer or send a future-dated cursor to BICC.

### Clock-skew probe (per-tenant onboarding step)

Before flipping a new tenant to `--mode incremental`, run the TC28b clock-skew probe to confirm the safety window absorbs the observed skew comfortably. The probe is a single round-trip via `extract_pvo`:

```python
from datetime import datetime, timezone
from oracle_ai_data_platform_fusion_bundle.extractors import bicc as bicc_mod
from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog

pvo = fusion_catalog.get("erp_suppliers")
t0 = datetime.now(timezone.utc)
df = bicc_mod.extract_pvo(spark, pvo, fusion_service_url=..., username=..., password=..., fusion_external_storage=..., watermark=None)
_ = df.limit(1).count()
t1 = datetime.now(timezone.utc)
skew_seconds = (t1 - t0).total_seconds()
print(f"AIDP→BICC round-trip: {skew_seconds:.1f}s")
print(f"bundle.incremental.watermark_safety_window_seconds: {bundle.incremental.watermark_safety_window_seconds}")
assert skew_seconds < bundle.incremental.watermark_safety_window_seconds
```

If the assertion fails, widen `watermarkSafetyWindowSeconds` to comfortably exceed the observed skew before enabling incremental mode.

### Empty-delta + soft-fail operator playbook

Two cases land at the same place (preserved bronze cursor + a WARN-log marker):

- **Empty delta** — BICC's `fusion.initial.extract-date` filter returned zero rows. Expected and harmless on a no-op cycle (Fusion didn't change between runs). The bronze cursor is preserved (NOT advanced) so the next run picks up the same time window. State-table row is written with `status='success'` and the prior `last_watermark` value.
- **`watermark_read_soft_failed` WARN** — a transient metastore failure prevented reading the prior `fusion_bundle_state` cursor. The orchestrator logs a structured WARN with the `watermark_read_soft_failed` marker key (set up alerts on this string) and proceeds with `prior_watermark=None`, falling back to a full extract for that node. Re-running the same `--mode incremental` command after the metastore recovers usually clears it. If the WARN persists across multiple runs, see `LIMITS.md §L6`.

Both signals show up in the orchestrator stdout under the same `[step]` line for the affected dataset — no separate audit table needed.

---

## Architecture

```
                                                  ┌──────────────────────────────┐
                                                  │   AIDP cluster (`tpcds`)     │
                                                  │   Spark Thrift JDBC endpoint │
                                                  │   schema=fusion_catalog.gold │
                                                  └──────────────┬───────────────┘
                                                                 │ JDBC
                                                                 ▼
┌─────────────────────────────────┐    REST API    ┌──────────────────────────────┐
│ aidp-fusion-bundle dashboard    │───────────────▶│       Oracle Analytics       │
│  install --target oac           │  (1) GET       │       Cloud (OAC)            │
│                                 │      /catalog  │                              │
│  - GET  /catalog?type=conns     │      ?search=* │  - data source: aidp_fusion  │
│         &search=<name> (precheck│  (2) POST      │    (created via UI once;     │
│         — skip POST if exists)  │      /snapshot │    bundle reuses on re-run)  │
│  - POST /snapshots (.bar)       │      register  │  - workbooks: cfo_dashboard, │
│  - POST /system/.../restore     │  (3) POST      │    ar_aging, ap_aging, ...   │
│  - GET  /workRequests/{id}      │      /restore  │                              │
└─────────────────────────────────┘                └──────────────┬───────────────┘
                                                                 │ Logical SQL
                                                                 ▼
                                                  ┌──────────────────────────────┐
                                                  │   OAC MCP Server (Preview)   │
                                                  │   - discoverData             │
                                                  │   - describeData             │
                                                  │   - executeLogicalSQL        │
                                                  └──────────────┬───────────────┘
                                                                 │ MCP (stdio)
                                                                 ▼
                                                  ┌──────────────────────────────┐
                                                  │  End-user AI client          │
                                                  │  (Claude / Cline / Copilot)  │
                                                  │  "what's our AR aging?"      │
                                                  └──────────────────────────────┘
```

The bundle authors content in OAC (workbooks), captures it as a Custom snapshot (`.bar`) excluding per-customer secrets, ships the `.bar` as a release artifact, and installs via four documented public REST calls. End users consume via OAC MCP. AIDP serves the data via JDBC throughout.

---

## Curated PVO catalog (v1, ERP-Finance)

| Bundle id | Datastore | Source | Confirmed? |
|---|---|---|---|
| `erp_suppliers` | `FscmTopModelAM.SupplierExtractPVO` | pdf1 Step 3 | ✅ |
| `po_orders` | `FscmTopModelAM.PrcExtractPO` | pdf2 p2 default | ✅ |
| `scm_items` | `ItemExtractPVO` | pdf2 p2 default | ✅ |
| `hcm_worker_assignments` | `workerAssignmentExtracts` (saas-batch) | pdf2 p4 | ✅ (v2) |
| `gl_journal_lines` | `JournalLinesPVO` | placeholder | 🟡 verify-live |
| `gl_period_balances` | `GLBalancePVO` | placeholder | 🟡 verify-live |
| `gl_coa` | `ChartOfAccountsPVO` | placeholder | 🟡 verify-live |
| `ar_invoices` / `ar_receipts` / `ar_aging` | `AR*PVO` | placeholders | 🟡 verify-live |
| `ap_invoices` / `ap_payments` / `ap_aging` | `AP*PVO` | placeholders | 🟡 verify-live |
| `po_receipts` | `RcvShipmentLinePVO` | placeholder | 🟡 verify-live |

Run `aidp-fusion-bundle catalog probe --pod <url>` to reconcile placeholders against your live BICC console.

---

## Use cases

1. **New AIDP customer onboarding** *(Phase 2 🚧)* — `bundle.yaml` with `examples/full_finance.yaml`, run orchestrator, walk away, return to a populated bronze + silver + gold + OAC workbooks.
2. **CFO demo in 30 minutes** *(0.1.0a ⚠ partial — gold marts stubbed)* — clone repo → `bootstrap` → `run --mode seed` → `dashboard install --target oac` → open OAC workbook → optionally chat via OAC MCP.
3. **Custom GenAI agents grounded on Fusion data** *(0.1.0a ✅)* — `ai_generate("which suppliers had >$1M Q1 spend?")` against the bundle's curated gold marts via OCI Generative AI.
4. **Fusion-side of the SAP-modernization pattern** *(Phase 2 🚧)* — Fusion data lands here; SAP data via parallel pipeline; both unified in AIDP gold layer.
5. **Build cross-source data products** *(Phase 2 🚧)* — combine Fusion + Salesforce/Workday/S3/Postgres via the same `aidataplatform` connector family.
6. **Cross-module analytics** *(Phase 2 🚧)* — order-to-cash health (AR × PO), commitments-vs-actuals (PO × GL), with conformed dimensions.
7. **Conformed dim reuse** *(Phase 2 🚧)* — your existing AIDP notebooks join to `fusion_silver.dim_account` instead of re-deriving.
8. **Daily incremental refresh** *(Phase 2 🚧)* — schedule the orchestrator as an AIDP job; bundle handles watermarks + Fusion's first-then-incremental BICC behavior.
9. **Fusion quarterly-update resilience** *(Phase 2 🚧)* — schema-drift detection auto-evolves on adds, quarantines on remove/change.
10. **SOX-ready audit trail** *(0.1.0a ✅)* — every load writes `_extract_ts`, `_source_pvo`, `_run_id`, `_watermark_used`; Iceberg/Delta time-travel + audit columns satisfy auditors.
11. **Customer customizations** *(Phase 2 🚧)* — extend `dim_account` for additional COA segments per `docs/customizing.md`; no fork needed.
12. **Pod migration** *(Phase 2 🚧)* — change `fusion.serviceUrl` in `bundle.yaml`, re-run `seed`, bundle reloads everything against new pod.

---

## References

- **Plan**: [`oracle-ai-data-platform-fusion-bundle.md`](../../../../../.claude/plans/oracle-ai-data-platform-fusion-bundle.md)
- **Sibling plugin** (single-PVO connector skill): [`oracle-ai-data-platform-workbench-spark-connectors`](../oracle-ai-data-platform-workbench-spark-connectors/)
- **Official Oracle BICC blog**: https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc
- **Ateam saas-batch blog**: https://www.ateam-oracle.com/how-to-extract-fusion-data-using-oracle-ai-data-platform
- **Official sample notebook**: [`Read_Only_Ingestion_Connectors.ipynb`](../../../data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb)
- **OAC MCP Preview**: https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html
- **OAC MCP Server announcement**: https://blogs.oracle.com/analytics/oracle-analytics-cloud-mcp-server-bridging-enterprise-analytics-and-ai
- **Modernize SAP with AIDP + Fusion**: https://docs.oracle.com/en/solutions/modernize-sap-aidp-fusion/

---

## License

[MIT](LICENSE) © 2026 Ahmed Awan
