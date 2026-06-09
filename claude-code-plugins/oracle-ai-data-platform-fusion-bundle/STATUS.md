# Plugin Status — `oracle-ai-data-platform-fusion-bundle`

> **Phase 9 status (2026-06-09)**: v1 silver/gold modules + the
> python_legacy adapter + the parity harness + the
> `--execution-backend` CLI flag + the v1 main-loop body have all been
> deleted. Bronze migrated to content-pack
> (`implementation.type: bronze_extract`) per-file YAMLs. Single
> execution path through `sql_runner.execute_node`. D-1 implicit
> transitive include + `--strict-scope` opt-out. ~10,000 LOC net
> deletion across 7 commits. **1360 unit + 12 architectural + 5
> integration tests pass.** The snapshot below is from 2026-05-10 and
> documents the pre-Phase-9 state — sections referencing v1 modules
> (`dimensions/dim_supplier.py`, `transforms/gold/*.py`,
> `extractors/bicc.py`) describe code that no longer exists. See
> ADR-0021 (pack-as-registry) and ADR-0022 (full v1 deletion + bronze
> as content-pack) for the current architectural authority.

> **Snapshot date**: 2026-05-10
> **Version**: 0.1.0-alpha (Phase 2 in progress toward 0.2.0)
> **Verdict**: **Phase 2 mid-flight.** Dim layer 3/4 shipped + 1 deferred (P1.6 blocked by L2). Gold layer 3/5 shipped (`supplier_spend`, `gl_balance`, `ap_aging`). Tests 268 → **306** all pass (38 P1.5b plumbing tests). Limit registry at [`LIMITS.md`](LIMITS.md).
>
> **Recent (2026-05-11)** — P1.5b catalog/schema plumbing shipped. New `config/paths.py` (`TablePaths` frozen dataclass + `DEFAULT_PATHS` singleton + `from_bundle()` classmethod with strict SQL-identifier validation). Every shipped mart/dim accepts `paths: TablePaths | None` on `build()`; `commands/run.py status()` now reads `aidp.bronzeSchema` instead of hardcoding `'bronze'`. `ap_aging.build()` resolves `gold_table` AFTER the auto-router resolves `due_date_mode` (critical ordering invariant locked by F + G build()-level fake-Spark tests). Backwards-compat byte-perfect — module-level `Final[str]` constants derive from `DEFAULT_PATHS` so their string values are unchanged.
> **Recent (2026-05-10)** — P1.9 `gold.ap_aging` shipped + live (TC24, 132 rows × 12 currencies, $-126K credits preserved across 5 currencies). Plugin-portable design: `due_date_mode='auto'` with 80% coverage gate, mode-aware `max_days_*` column name, schema-variant knobs for the Fusion AP column dialects observed across tenants.
> **Recent (2026-05-09)** — P1.8 `gold.gl_balance` shipped + live (TC23, 10.18M rows). BOOTSTRAP extended with Step 7 + step-shape probe; NULL-propagation regression caught + fixed in same session via COALESCE.

This document is a current-state audit of the second plugin in this repo: what's already implemented and live-validated, how the pieces fit together, what's still on the roadmap, and what's blocking each remaining item.

---

## 1. TL;DR (one paragraph)

The fusion-bundle plugin is a CLI-driven productized pipeline for **Fusion ERP/HCM/SCM → Oracle AI Data Platform → Oracle Analytics Cloud**. Phase 1 (the alpha) ships a fully-wired CLI (11 commands), curated PVO catalog (14 entries, all live-confirmed), BICC extract → bronze write (live-validated), an end-to-end OAC integration that uses **only Oracle-documented public REST endpoints** (snapshot register + restore + workRequest poll + connection precheck), and an MCP-config emitter so end-users chat with the data via Claude Desktop / Cline / Copilot. **139 unit tests pass; 8 live test cases pass** against the `saasfademo1` Fusion demo pod and multiple OAC instances (TC10h-4 succeeded end-to-end on a disposable OAC1, 2026-05-03). The bronze→silver→gold transforms, conformed dimensions, and remaining gold marts are stubbed-but-blueprinted in Python; **Phase 2 is the implementation of those transforms** plus packaging the workbook `.bar` and validating saas-batch against a real Fusion HCM pod. No critical blockers — every known issue has a documented workaround or is purely environmental (demo-pod feature gating).

---

## 2. What it already handles (DONE)

### 2.1 Plugin packaging & distribution
- ✅ `.claude-plugin/plugin.json` v0.1.0-alpha
- ✅ `pyproject.toml` (Python 3.10+, six core deps: click, pydantic, pyyaml, requests, oci, rich)
- ✅ Installable via `pip install -e .` (dev) or marketplace (`/plugin install oracle-ai-data-platform-fusion-bundle@aidp-connectors`)
- ✅ Single Claude Code skill at `skills/aidp-fusion-bundle/SKILL.md` with full triggers, when-to-use, and positioning

### 2.2 CLI surface (11 commands, all wired)

```
aidp-fusion-bundle init        --template {minimal, full-finance} [--force]
aidp-fusion-bundle validate
aidp-fusion-bundle bootstrap   [--check-iam]
aidp-fusion-bundle catalog list
aidp-fusion-bundle catalog probe --pod <url> [--user] [--password]
aidp-fusion-bundle run         --mode {full, incremental, seed} [--datasets CSV] [--inline]
aidp-fusion-bundle status
aidp-fusion-bundle dashboard install   --target oac [...20 options]
aidp-fusion-bundle dashboard validate  --target oac [...]
aidp-fusion-bundle dashboard uninstall --target oac [...]
aidp-fusion-bundle dashboard mcp-config --oac-url --oac-mcp-connect-js
```

No stubs. All 11 functional and unit-tested. Live-validated paths covered via TC1..TC10h-7.

### 2.3 PVO catalog — 14 entries, all confirmed ✅

Source: `scripts/oracle_ai_data_platform_fusion_bundle/schema/fusion_catalog.py` (256 LOC).

| Bundle ID | Datastore (full AM-hierarchy) | Source proof |
|---|---|---|
| `erp_suppliers` | `FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO` | TC1 (229 rows) |
| `po_orders` | `FscmTopModelAM.PrcExtractAM.PoBiccExtractAM.PurchasingDocumentHeaderExtractPVO` | pdf2 + verified |
| `scm_items` | `FscmTopModelAM.ScmExtractAM.EgpBiccExtractAM.ItemExtractPVO` | pdf2 + verified |
| `gl_journal_lines` | `FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalHeaderExtractPVO` | verified 2026-04-30 |
| `gl_period_balances` | `FscmTopModelAM.FinExtractAM.GlBiccExtractAM.BalanceExtractPVO` | verified 2026-04-30 |
| `gl_coa` | `FscmTopModelAM.FinExtractAM.GlBiccExtractAM.CodeCombinationExtractPVO` | verified 2026-04-30 |
| `ar_invoices` | `FscmTopModelAM.FinExtractAM.ArBiccExtractAM.TransactionHeaderExtractPVO` | verified |
| `ar_receipts` | `FscmTopModelAM.FinExtractAM.ArBiccExtractAM.ReceiptHeaderExtractPVO` | verified |
| `ar_aging` | computed gold mart (no direct PVO) | derived from `ar_invoices` + `ar_receipts` |
| `ap_invoices` | `FscmTopModelAM.FinExtractAM.ApBiccExtractAM.InvoiceHeaderExtractPVO` | TC8 (49,985 rows) |
| `ap_payments` | `FscmTopModelAM.FinExtractAM.ApBiccExtractAM.PaymentHistoryDistributionExtractPVO` | verified |
| `ap_aging` | computed gold mart (no direct PVO) | derived from `ap_invoices` + `ap_payments` |
| `po_receipts` | `FscmTopModelAM.ScmExtractAM.RcvBiccExtractAM.ReceivingReceiptTransactionExtractPVO` | verified |
| `hcm_worker_assignments` | `workerAssignmentExtracts` (saas-batch path) | pdf2 p4 |

Key finding (TC1, 2026-04-30): **pdf1's PVO names are abbreviated** (e.g. wrote `FscmTopModelAM.SupplierExtractPVO`; live BICC requires the full AM-hierarchy). Bundle catalog now uses full paths. The `catalog probe` command reconciles against any customer pod via `GET /biacm/api/v1/metadata/datastores`.

### 2.4 BICC extract → bronze (live-validated path)

Module: `extractors/bicc.py` (142 LOC). Uses AIDP's built-in `spark.read.format("aidataplatform").option("type","FUSION_BICC")` — mirrors the official Oracle AIDP sample. Adds audit columns (`_extract_ts`, `_source_pvo`, `_run_id`, `_watermark_used`) and writes Delta to `fusion_catalog.bronze.<table>`.

### 2.5 Fusion REST + saas-batch fallbacks
- `extractors/rest.py` (75 LOC) — Fusion REST `/analytics/api` paged fetch for <5k rows, respects 499-row hard cap (MOS Doc ID 2429019.1)
- `extractors/saas_batch_rest.py` (339 LOC) — Full HCM saas-batch path: token-relay → submit `AsyncDataExtraction` job → poll → fetch output files. **14 unit tests PASS; live blocked on demo pod (env-only, see §5)**

### 2.6 OAC integration (the centerpiece)

Module: `oac/` (752 LOC across `install.py`, `validate.py`, `uninstall.py`, `rest/{client,oauth,connection}.py`).

End-to-end flow uses **only 4 Oracle-documented public REST calls** (audit done 2026-05-01 against [openapi.json](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/openapi.json), TC10h-2):

```
1. GET  /api/20210901/catalog?type=connections&search=<name>     ← precheck (defaults search="*")
2. POST /api/20210901/catalog/connections                        ← create AIDP conn (skipped if precheck hits)
3. POST /api/20210901/snapshots                                  ← register .bar from OCI Object Storage
4. POST /api/20210901/system/actions/restoreSnapshot             ← async restore
5. GET  /api/20210901/workRequests/{id}                          ← poll until SUCCEEDED
```

OAuth: **Authorization Code + PKCE + Refresh Token** flow with audience auto-discovery via `<oac-url>/ui/` redirect probe (fallback to OCI CLI `identity-domains apps list ... --query 'data.resources[0].audience'`). Device-code flow available for headless via `--auth-flow device`. **Client Credentials grant explicitly NOT supported** by OAC (per Oracle's Authenticate doc).

Live-validated TC10h-4 (2026-05-03): full `dashboard install` succeeded end-to-end on disposable OAC1 — precheck, snapshot REGISTER (async 202), RESTORE (async), polling all green. Snapshot `bd820501-9a3f-426e-8354-2d8c279b35b2` restored; workRequest `lfc-cc:13347-c9:3962654` SUCCEEDED.

### 2.7 OAC MCP config emission

`dashboard mcp-config` prints a ready-to-paste JSON snippet for `claude_desktop_config.json` (or Claude Code / Cline / Copilot equivalent). Uses the OAC-issued `oac-mcp-connect.js` (Node.js 18+) to bridge stdio MCP traffic; auth piggybacks on the user's OAC web session — no separate IDCS app needed for MCP.

### 2.8 Test coverage

**Unit: 139 passing** (1,851 LOC across 12 test files):

| File | Tests | Focus |
|---|---:|---|
| `test_oac_rest_client.py` | 33 | Full OAC REST surface (snapshot lifecycle, connection CRUD, work requests) |
| `test_commands.py` | 14 | CLI dispatch + error handling |
| `test_saas_batch_rest.py` | 14 | Token relay + job polling |
| `test_oac_oauth.py` | 13 | PKCE flow + refresh token + device flow + audience discovery |
| `test_fusion_catalog.py` | 12 | PVO catalog + `list_by_id` + confirmed/verify-live split |
| `test_refs.py` | 12 | OCID/ARN/URI validation |
| `test_oac_install.py` | 9 | Connection JSON gen + snapshot register precheck |
| `test_vault.py` | 7 | OCI Vault `${vault:OCID}` resolution |
| `test_oac_connection.py` | 7 | Connection object model |
| `test_rest_extractor.py` | 7 | Fusion REST fallback options |
| `test_bundle_schema.py` | 6 | bundle.yaml + aidp.config.yaml parsing |
| `test_extractor_bicc.py` | 5 | BICC `spark.read` options |
| **Total** | **139** | All mock external services |

**Live: 8 PASS / 1 BLOCKED-ENV** (`tests/live/`):

| TC | Status | Date | Outcome |
|---|---|---|---|
| TC1 + TC7 | ✅ PASS | 2026-04-30 | BICC bulk extract → `bronze.erp_suppliers` (229 rows); audit columns written |
| TC8 | ✅ PASS | 2026-04-30 | Bronze → silver → `gold.supplier_spend` ($3.2B from 49,985 AP invoices); 236 vendor×status combos |
| TC9 | ✅ PASS | 2026-04-30 | GenAI grounding via `ai_generate('openai.gpt-5.4', ...)` against `gold.supplier_spend`; agent cited top vendor `300000047507499` at $892.7M correctly with 26.18% concentration math + anomaly detection (negative `vendor_id=-10016`, stale invoice `2018-12-21`) |
| TC10 / 10b / 10c / 10d / 10e | ✅ PASS | 2026-04-29 → 2026-05-03 | OAC integration: native AIDP connection; live tile rendered $3.2B; 6-viz exec dashboard; `--print-only` byte-for-byte match; vendor-id Treat-As Attribute; top-N vendors bar |
| TC10h / 10h-2 / 10h-3 / 10h-4 / 10h-5 / 10h-6 / 10h-7 | ✅ PASS | 2026-04-30 → 2026-05-03 | OAuth refactor (PKCE), REST-only refactor, snapshot round-trip, full install end-to-end on disposable OAC1, multi-workbook restore on natalie pod |
| TC11–TC17 (saas-batch) | ⚠ BLOCKED-ENV | 2026-04-30 | Endpoint not enabled on `saasfademo1` (HCM-tier feature). 14 unit tests PASS. |

Evidence files: 5 markdown results files + 16 screenshots + 5 reference SQL queries in `tests/live/`.

---

## 3. How it does it (architecture)

```
                                                  ┌──────────────────────────────┐
                                                  │   AIDP cluster (`tpcds`)     │
                                                  │   Spark Thrift JDBC endpoint │
                                                  │   schema=fusion_catalog.gold │
                                                  └──────────────┬───────────────┘
                                                                 │ JDBC
                                                                 ▼
┌─────────────────────────────────┐    REST API    ┌──────────────────────────────┐
│ aidp-fusion-bundle CLI          │───────────────▶│       Oracle Analytics       │
│   dashboard install --target oac│  (1) GET       │       Cloud (OAC)            │
│                                 │      /catalog  │                              │
│  - GET  /catalog?search=<name>  │      ?type=    │  - data source: aidp_fusion  │
│         (precheck — skip POST   │      connecs   │    (created via UI once;     │
│          if exists)             │  (2) POST      │    bundle reuses on re-run)  │
│  - POST /catalog/connections    │      /snapshot │  - workbooks: cfo_dashboard, │
│  - POST /snapshots (.bar)       │      register  │    ar_aging, ap_aging, ...   │
│  - POST /system/.../restore     │  (3) POST      │                              │
│  - GET  /workRequests/{id}      │      /restore  │                              │
└─────────────────────────────────┘                └──────────────┬───────────────┘
         │                                                        │ Logical SQL
         │                                                        ▼
         │                                         ┌──────────────────────────────┐
         │                                         │   OAC MCP Server (Preview)   │
         │                                         │   - discoverData             │
         │                                         │   - describeData             │
         │                                         │   - executeLogicalSQL        │
         │                                         └──────────────┬───────────────┘
         │                                                        │ MCP (stdio)
         │                                                        ▼
         │                                         ┌──────────────────────────────┐
         │                                         │  Claude / Cline / Copilot    │
         │                                         │  "what's our supplier spend?"│
         │                                         └──────────────────────────────┘
         │
         └────────────────────────────────────────────────┐ CLI dispatch (REST)
                                                          ▼
                                    ┌──────────────────────────────────┐
                                    │ AIDP Notebook (Orchestrator)     │
                                    │                                  │
                                    │ 1. Extract PVO via BICC          │
                                    │    (extractors.bicc)             │
                                    │    → bronze layer (Delta)  ✅    │
                                    │                                  │
                                    │ 2. Build dimensions              │
                                    │    (dimensions.*)                │
                                    │    → silver layer (Delta)  🚧    │
                                    │                                  │
                                    │ 3. Gold business marts           │
                                    │    (transforms.*)                │
                                    │    → gold layer (Delta)    🚧    │
                                    │                                  │
                                    │ 4. Persist state to              │
                                    │    fusion_bundle_state table     │
                                    └──────────────────────────────────┘
```

### 3.1 Realistic deployment flow (TC10h-4 validated, 2026-05-03)

OAC's REST validator does not bless the AIDP `idljdbc` connectionType (falls through to generic Oracle DB schemas requiring `serviceName`/`password`/`connectionString`). So the real-world flow is:

**First install** (one-time per OAC instance):
1. `aidp-fusion-bundle dashboard install --target oac --print-only` → writes `oac/data_source/aidp_fusion_jdbc.json` (6-key shape)
2. Customer creates the connection via OAC UI: Data → Connections → Create → "Oracle AI Data Platform" → upload JSON + API-key PEM. Documented in [Oracle's AIDP Quick Start blog](https://blogs.oracle.com/ai-data-platform/continuing-your-oracle-ai-data-platform-journey-quick-start-guide).

**All subsequent runs** are pure REST:
1. Precheck `GET /catalog?type=connections&search=<name>` finds existing connection → skips POST
2. `POST /snapshots` registers customer-uploaded `.bar`
3. `POST /system/actions/restoreSnapshot` triggers async restore
4. Poll `GET /workRequests/{id}` until SUCCEEDED

### 3.2 Module map (Python package)

```
scripts/oracle_ai_data_platform_fusion_bundle/      4,261 LOC total
├── __init__.py              19 LOC   __version__ + __all__
├── cli.py                  424 LOC   Click groups + 11 commands [DONE]
├── schema/
│   ├── bundle.py           222 LOC   Pydantic models for bundle.yaml [DONE]
│   ├── fusion_catalog.py   256 LOC   14 PvoEntry, all confirmed ✅ [DONE]
│   └── refs.py              84 LOC   OCI OCID/ARN/URI type safety [DONE]
├── commands/
│   ├── init.py              75 LOC   Scaffolds bundle.yaml + aidp.config.yaml [DONE]
│   ├── validate.py         132 LOC   Schema + ref-integrity check (no network) [DONE]
│   ├── bootstrap.py        262 LOC   Probes BICC role + Ext Storage + AIDP + IAM [DONE]
│   ├── catalog.py          146 LOC   list + probe (live BICC reconciliation) [DONE]
│   └── run.py              200 LOC   Dispatches AIDP job or runs inline [DONE]
├── extractors/
│   ├── bicc.py             142 LOC   spark.read.format("aidataplatform") [DONE]
│   ├── rest.py              75 LOC   Fusion REST <5k row fallback [DONE]
│   └── saas_batch_rest.py  339 LOC   HCM saas-batch token-relay [DONE in code, BLOCKED on live]
├── orchestrator/             1 LOC   STUB — Phase 2 deliverable 🚧
├── transforms/               1 LOC   STUB — Phase 2 deliverable 🚧
├── dimensions/               4 LOC   STUB — Phase 2 deliverable 🚧
├── utils/
│   ├── params.py            56 LOC   InstallParams dataclass [DONE]
│   └── vault.py             76 LOC   ${vault:OCID} resolver [DONE]
└── oac/
    ├── install.py          291 LOC   Orchestrates 4 REST calls [DONE]
    ├── validate.py          89 LOC   Read-only probe [DONE]
    ├── uninstall.py         93 LOC   Connection + snapshot cleanup [DONE]
    └── rest/
        ├── oauth.py        458 LOC   PKCE Authorization Code + refresh + device [DONE]
        ├── connection.py   126 LOC   AIDP connection CRUD [DONE]
        └── client.py       616 LOC   OacRestClient — full surface [DONE]
```

---

## 4. What remains (Phase 2 roadmap)

Sourced from CHANGELOG.md "Planned" + README "What you get" + the three stub modules (`orchestrator/`, `transforms/`, `dimensions/`).

### 4.1 Orchestrator notebook — `orchestrator/__init__.py`
**Status**: stub (1 LOC).
**Scope**: Implements the bronze→silver→gold medallion orchestration entry point for AIDP notebooks. Integrates extractors (BICC, REST, saas-batch) with transforms. Handles watermarking for incremental runs. Expected shape: `orchestrator.run(bundle_path, mode, datasets) -> RunSummary`.
**Effort**: ~400-600 LOC + 1 notebook (`notebooks/run_orchestrator.ipynb`).

### 4.2 Transforms layer — `transforms/__init__.py`
**Status**: stub.
**Scope**:
- Bronze typing + standardization (cast types, audit columns, dedup)
- Silver dimension joins (dim_supplier, dim_account, dim_calendar, dim_org, dim_item)
- Gold mart builders (5 marts; see §4.4)
**Effort**: ~600-1,000 LOC + tests.

### 4.3 Conformed dimensions — `dimensions/__init__.py`
**Status**: stub (4 LOC).
**Scope**: Five dim builders, each reading from one (or more) bronze tables, applying business rules (dedup, type cast, null handling), writing to `silver.dim_*`:
| Dimension | Source | Notes |
|---|---|---|
| `dim_account` | `gl_coa` (CodeCombinationExtractPVO) | Supports custom COA segment extensions per `docs/customizing.md` (TBD) |
| `dim_calendar` | Generated (system) | Gregorian + Fiscal calendars 2020–2030 |
| `dim_org` | Fusion HR Operating Unit (PVO TBD) | Pending PVO confirmation |
| `dim_supplier` | `erp_suppliers` (SupplierExtractPVO) | Already proven via TC8; deduped on supplier_number; handles null IDs (demo pod) |
| `dim_item` | `scm_items` (ItemExtractPVO) | Pending implementation |
**Effort**: ~300-500 LOC + tests.

### 4.4 Gold marts (5 verified blueprints, 1 live-tested)
**Status**: blueprint only; supplier_spend prototype validated TC8 ($3.2B grand total).

| Mart ID | Target table | Sources | Live result |
|---|---|---|---|
| `ar_aging` | `gold.ar_aging` | `ar_invoices` + `ar_receipts` + AR aging period config | Pending |
| `ap_aging` | `gold.ap_aging` | `ap_invoices` + `ap_payments` + `AgingPeriodHeaderExtractPVO` | Pending |
| `gl_balance` | `gold.gl_balance` | `gl_period_balances` + `dim_account` + `dim_calendar` | Pending |
| `po_backlog` | `gold.po_backlog` | `po_orders` + `po_receipts` + `dim_supplier` + `dim_calendar` | Pending |
| `supplier_spend` | `gold.supplier_spend` | `silver.fact_ap_invoice` + `dim_supplier` | ✅ TC8 (236 records, $3.2B) |
**Effort**: ~200 LOC per mart × 5 = ~1,000 LOC + integration tests.

### 4.5 saas-batch live validation
**Status**: code complete, 14/14 unit tests PASS; live blocked by demo-pod env (see §5).
**Need**: a customer Fusion HCM pod where `/saas-batch/security/tokenrelay` is enabled. Flag for v0.2.0 beta.

### 4.6 OAC workbook .bar (release artifact)
**Status**: 5 workbooks live-authored on `oacai.cealinfra.com` (TC10b–TC10e); GL_Balance_Workbook polished with Treemap (recent commit `8b36d33`).
**Need**:
1. Build the 5+ workbooks under `/shared/AIDP_Fusion_Bundle/` against an `aidp_fusion_jdbc` connection on a clean dev OAC
2. OAC Console → Snapshots → Take Snapshot → Custom (Include: Catalog Content + Shared Folders + Application Roles; Exclude: Credentials, Connections, User Folders, File-based Data, Day by Day, Jobs, Plug-ins, Configuration)
3. Strong password (committed in release notes)
4. Export to OCI Object Storage in bundle-author tenancy
5. Attach `.bar` as GitHub release artifact

Envisioned workbooks for v0.2.0 release `.bar`:
- **CFO dashboard** (AR-aging + AP-aging + GL-balance overview)
- **Supplier spend** (already authored; needs to be packaged)
- **PO backlog** (spend by order status)
- **GL balance trend** (monthly actuals + variance)
- **AR aging drill-down** (customer × amount × days-past-due)
- **AP aging drill-down** (vendor × amount × days-overdue)

### 4.7 Documentation gaps
- `docs/customizing.md` (TBD) — customer COA segment extensions for `dim_account`; per-customer org dimension flavors
- Cross-source enrichment patterns (Fusion ×Salesforce → opportunity-to-cash; Fusion ×S3 (customer data) → segment analytics; Fusion ×Workday → talent cost modeling) — likely as `docs/cross-source-recipes.md`

### 4.8 v3 roadmap (long-term)
- **Delta Sharing** — share curated gold-layer datasets with external partners (provider setup on AIDP + partner consumer setup)
- **Agent CLI helper**: `aidp-fusion-bundle agent ask "question"` — wraps `ai_generate(...)` + grounding for ad-hoc agent queries (TC9 proved the pattern)

---

## 5. Main blockers (with severity + workarounds)

| # | Blocker | Severity | Status | Workaround |
|---|---|---|---|---|
| 1 | **`POST /catalog/connections` REST validator does not bless AIDP `idljdbc` connectionType** | P2 | Documented + workaround shipped | OAC validator falls through to generic Oracle DB schemas requiring `serviceName`/`password`/`connectionString`. The realistic flow: customer creates connection via OAC UI **once** using the 6-key JSON from `dashboard install --print-only`; subsequent runs reuse it via the precheck. Live-validated TC10h-4. **Future improvement**: lobby Oracle to add AIDP `idljdbc` to `openapi.json`. |
| 2 | **saas-batch live test blocked on demo pod 404** | P3 | Env-only; code complete | `/saas-batch/security/tokenrelay` not enabled on `saasfademo1` (HCM-tier feature, paying customers only). Unit tests (14/14) verify the code path. **Remediation**: link to customer's HCM pod during v0.2.0 beta. |
| 3 | **Snapshot BAR URI shape is `file:///<folder>/<name>.bar` only** | P2 | Resolved | 7 variants tried before the right one was found. NOT `oci://...`, NOT bare object name, NOT OCI Object Storage HTTPS URL, NOT pre-authenticated request URL. CLI now validates this shape; documented in `docs/oac_rest_api_setup.md`. |
| 4 | **`GET /catalog?type=connections` returns TypeInfo header (not item list) when `search` is omitted** | P2 | Resolved (commit `52ce2c7`) | Without `search=*`, endpoint returns `[{"type":"connections"}]` (header only). Bundle's `list_connections` now defaults `search="*"` and filters out TypeInfo rows. Found + fixed during TC10h-3 live validation. |
| 5 | **Fusion BICC API-key propagation takes 60–90 seconds** | P3 | Documented | At 30s, Test Connection returns `BIACM0145 Invalid connection`; retry at 90s succeeds. Bootstrap waits 90s + retry; documented in skill gotchas. |
| 6 | **Demo pod has masked supplier identifiers** | P3 | Workaround shipped | `SupplierExtractPVO` returns NULL/0 for `VendorId`, `PartyId`; only `Segment1` (supplier_number) + names populated. Bundle's `gold.supplier_spend` uses spend-only aggregation that works on demo pods. **Future enhancement**: detect populated supplier IDs and switch to supplier×spend join automatically. |
| 7 | **PDF1's PVO names are abbreviated (don't work live)** | P2 | Resolved | pdf1 wrote `FscmTopModelAM.SupplierExtractPVO`; live BICC requires `FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO`. Bundle catalog corrected post-TC1; `catalog probe` reconciles against any customer pod. |
| 8 | **AIDP Instance Principal + Resource Principal blocked at platform level** | P2 (platform-wide) | Documented; not bundle-specific | IMDS unreachable from notebook pods; RP env vars not provided. Must use API Key + inline PEM (`from_inline_pem(...)`). Affects every plugin in the marketplace. AIDP team aware; pending Oracle fix. |
| 9 | **OAC does not support `client_credentials` grant** | P2 | Documented; using Auth Code + PKCE instead | Per [Authenticate](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/authenticate.html). Bundle uses Authorization Code + PKCE + Refresh Token; device-code fallback for headless. |

**No P0 blockers. No P1 blockers. All P2 issues have shipped workarounds and are well-documented. Phase 2 work has no known blockers — only effort.**

### Lone TODO in production code
- `commands/run.py` (line ~1, docstring): "The bundle ships `notebooks/run_orchestrator.ipynb` (TODO)" — Phase 2 deliverable, blocked on §4.1.

Codebase is otherwise clean: no `FIXME`, no `XXX`, no other unresolved TODOs.

---

## 6. Recent activity (last 12 commits, plugin path only)

```
8b36d33  fusion-bundle: GL_Balance_Workbook polish (Treemap by company) + rc5 verify
d0a208b  fusion-bundle: TC10h-7 — full 6-workbook scope with AR/GL on natalie pod
f5521e8  fusion-bundle: TC10h-6 — 4 workbooks visible end-to-end after install
20dfb92  fusion-bundle: TC10h-5 — bundle plumbing carries real cargo end-to-end
64cdf5c  fusion-bundle: docs polish-pass to match TC10h-3/h-4 reality
c7d0437  fusion-bundle: TC10h-4 — dashboard install end-to-end SUCCESS on OAC1
52ce2c7  fusion-bundle: list_connections needs search=* (OAC TypeInfo-header gotcha)
f622316  fusion-bundle: TC10h-3 live snapshot register+restore round-trip on OAC1
b55c076  fusion-bundle: TC10h-2 live re-validation on disposable OAC1 (2026-05-03)
30b2897  fusion-bundle: clean up stale .dva/workbooks references after TC10h-2 refactor
c8ee914  fusion-bundle: refactor OAC integration to Oracle-documented endpoints (TC10h-2)
ae756d8  fusion-bundle: add OacRestClient.export_workbook() + dashboard export CLI
```

**Trend**: April–May 2026 was heavy OAC integration + TC10h series multi-instance live validation. Workbook authoring (TC10b–TC10e) and `.bar` snapshot install (TC10h-3 / h-4) are the most recent technical wins. No work-in-progress branches in flight.

---

## 7. Concrete metrics

| Metric | Value |
|---|---:|
| Plugin version | `0.1.0-alpha` |
| Total Python LOC (`scripts/`) | 4,261 |
| Unit test LOC (`tests/unit/`) | 1,851 |
| Unit test count (12 files) | **139 PASS** |
| Live test count | **8 PASS / 1 BLOCKED-ENV** |
| CLI commands wired | 11 (no stubs) |
| PVO catalog entries | 14 (all confirmed ✅) |
| OAC REST endpoints used | 5 (all Oracle-documented public, no UI-only) |
| Conformed dimensions designed | 5 (all stub) |
| Gold marts designed | 5 (1 prototype validated TC8) |
| OAC workbooks live-authored | 5 (CFO dashboard not yet packaged in `.bar`) |
| Stub modules (Phase 2) | 3 (`orchestrator/`, `transforms/`, `dimensions/`) |
| Production TODOs | 1 (notebook entry point) |
| Recent commits (plugin path) | 12 |

---

## 8. Where to start (suggested next sessions)

Roughly in priority order:

1. **Implement `dimensions/dim_supplier.py`** (smallest, already prototyped in TC8). Drives `gold.supplier_spend`. ~150 LOC + tests.
2. **Implement `dimensions/dim_account.py` + `dim_calendar.py`** — needed before `gold.gl_balance` can be built. ~200-300 LOC + tests.
3. **Implement `transforms/gold/supplier_spend.py`** as the first gold-mart transform — already validated as a one-shot SQL in TC8; productize it. ~200 LOC + tests.
4. **Implement `orchestrator/__init__.py`** — wire extract → bronze → silver → gold sequence. Defines the public `orchestrator.run(bundle_path, mode, datasets)` API. ~400 LOC + 1 notebook + tests.
5. **Implement remaining gold marts** (`ar_aging`, `ap_aging`, `gl_balance`, `po_backlog`) — each ~200 LOC. Order by business value: probably `gl_balance` → `ap_aging` → `ar_aging` → `po_backlog`.
6. **Build the v0.2.0 `.bar` release artifact** — package the 5+ workbooks on a clean dev OAC, snapshot Custom-mode, attach to GitHub release.
7. **saas-batch live test** when a customer HCM pod is available.

Each item above corresponds to a single feature branch + PR; nothing is sequenced more than 2 deep.

---

## 9. References (project-internal)

- Cross-cutting reference set: `/Users/oussamalakrafi/Workspace/Claude-Context/claude-code-plugins-ahmed/` (8 files; covers Claude Code plugin system, AIDP platform, BICC, OAC REST, OAC MCP, both plugins). Auto-loaded for any work in this repo.
- Skill: `skills/aidp-fusion-bundle/SKILL.md`
- Setup docs: `docs/oac_rest_api_setup.md`, `docs/oac_mcp_setup.md`
- Examples: `examples/minimal_gl_only.yaml`, `examples/full_finance.yaml`, `examples/aidp.config.example.yaml`
- Live evidence: `tests/live/TC1_TC7_results.md`, `TC8_supplier_spend_results.md`, `TC9_genai_results.md`, `TC10_oac_integration_results.md`, `TC11_TC17_saas_batch_results.md`
- Connection JSON template: `oac/data_source/aidp_fusion_jdbc.json`

## 10. References (Oracle official)

- AIDP blog (the canonical pattern this plugin productizes): <https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc>
- A-Team saas-batch blog: <https://www.ateam-oracle.com/how-to-extract-fusion-data-using-oracle-ai-data-platform>
- AIDP-from-OAC Quick Start (UI flow for AIDP connection): <https://blogs.oracle.com/ai-data-platform/continuing-your-oracle-ai-data-platform-journey-quick-start-guide>
- OAC REST API authenticate: <https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/authenticate.html>
- OAC OpenAPI spec: <https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/openapi.json>
- OAC Snapshot REST prerequisites: <https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/prerequisites.html>
- Take Snapshots and Restore: <https://docs.oracle.com/en/cloud/paas/analytics-cloud/acabi/take-snapshots-and-restore-information.html>
- OAC MCP Server (Preview): <https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html>
