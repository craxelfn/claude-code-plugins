---
description: Productized Fusion → Oracle AI Data Platform pipeline with curated BICC extracts (GL/AR/AP/PO/Suppliers/Items), bronze/silver/gold medallion in Delta, conformed dimensions (account/calendar/org/supplier/item), gold marts (AR-Aging/AP-Aging/GL-Balance/PO-Backlog/Supplier-Spend), and OAC workbooks installable via OAC REST API. Use when the user wants to load Fusion ERP/HCM/SCM data into AIDP, build a CFO dashboard from Fusion, set up a Fusion-backed lakehouse, install OAC dashboards for Fusion data, set up OAC MCP for natural-language Fusion analytics in Claude/Cline/Copilot, run BICC extracts incrementally, productize the Oracle blog "Bring Fusion Data into AIDP Workbench Using BICC", or extract Fusion via the saas-batch REST API. Triggers — "load Fusion into AIDP", "set up Fusion bronze layer", "build CFO dashboard from Fusion", "install OAC workbooks for Fusion", "run BICC extract", "Fusion AIDP medallion", "saas-batch Fusion extract".
allowed-tools: Read, Write, Edit, Bash, Glob, Grep
---

# `aidp-fusion-bundle` — Fusion ERP/HCM/SCM → AIDP, batteries included

Productizes the official Oracle blog [Bring Fusion Data into Oracle AI Data Platform Workbench Using BICC](https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc) plus the ateam companion [How to Extract Fusion Data using Oracle AI Data Platform](https://www.ateam-oracle.com/how-to-extract-fusion-data-using-oracle-ai-data-platform). One install, three commands, populated lakehouse + installed OAC dashboards.

## When to use

- User wants Fusion data in AIDP and asks "where do I start"
- User has BICC privileges and wants curated bronze/silver/gold layers without writing the pipeline
- User is preparing a CFO/analytics demo and needs OAC dashboards on Fusion data
- User wants to use [OAC MCP (Preview)](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html) to chat with Fusion data via Claude/Cline/Copilot

## When NOT to use

- For a single one-off PVO read → use [`aidp-fusion-bicc`](../../../oracle-ai-data-platform-workbench-spark-connectors/skills/aidp-fusion-bicc/SKILL.md) (sibling plugin, smaller scope).
- For Fusion REST queries with <50k rows → [`aidp-fusion-rest`](../../../oracle-ai-data-platform-workbench-spark-connectors/skills/aidp-fusion-rest/SKILL.md).
- For EPM Cloud Planning data slices → [`aidp-epm-cloud`](../../../oracle-ai-data-platform-workbench-spark-connectors/skills/aidp-epm-cloud/SKILL.md).
- For Essbase MDX → [`aidp-essbase`](../../../oracle-ai-data-platform-workbench-spark-connectors/skills/aidp-essbase/SKILL.md).

## Positioning

This bundle is **additive to and complementary with** Oracle's managed Fusion data offerings. It productizes Option 1 of pdf1's three-option architecture (BICC into AIDP for "Custom AI and ML, raw data access, data engineering"). Never positioned as a replacement for FDI, OAC, OTBI, BIP, or Data Transforms.

## What you get

Mirrors pdf1 §"What Can You Do Once the Data is in Oracle AI Data Platform":

1. **Custom ML/AI training** on operational ERP/HCM/SCM data (PySpark + Python in AIDP notebooks)
2. **Cross-source enrichment** — join Fusion data with non-Fusion sources via the `aidataplatform` connector family
3. **Medallion architecture** — bronze (raw audit) → silver (typed + dim-joined) → gold (business marts) in Delta
4. **GenAI agent grounding** — `ai_generate("which suppliers had >$1M Q1 spend?")` against gold marts via OCI Generative AI
5. **BI & reporting via JDBC** — OAC, Tableau, Power BI consume the gold layer
6. **Delta Sharing** (v3 roadmap) — share curated datasets with other teams or external partners

## Quickstart

1. **Install the CLI** on your laptop:
   ```bash
   pip install -e /path/to/oracle-ai-data-platform-fusion-bundle
   ```

2. **Scaffold a bundle in your repo**:
   ```bash
   aidp-fusion-bundle init
   ```
   Edits `bundle.yaml` and `aidp.config.yaml` to match your environment (Fusion pod URL, AIDP workspace, OAC URL, OCI Vault refs for credentials).

3. **Probe prerequisites against your pod**:
   ```bash
   aidp-fusion-bundle bootstrap --check-iam
   ```
   Confirms BICC role, BICC External Storage profile (set in BICC console), AIDP catalog, IAM policies, Vault access.

4. **Run the orchestrator**:
   ```bash
   aidp-fusion-bundle run --mode seed     # first-time full extract
   aidp-fusion-bundle run --mode incremental  # daily delta
   ```

5. **Install OAC dashboards** (one-time per OAC instance):
   ```bash
   # 5a. Upload bundle-vN.bar to your OCI Object Storage bucket. Use a folder
   #     prefix in the object name; --bar-uri later passes the Oracle-documented
   #     `file:///<folder>/<name>.bar` shape.
   oci os object put --bucket-name aidp-fusion-bundle-bar \
                     --file ./bundle-v0.1.0a0.bar \
                     --name aidp-fusion-bundle/bundle-v0.1.0a0.bar

   # 5b. (One-time, in OAC UI) Create the AIDP connection. Run the bundle
   #     in --print-only mode to write the 6-key JSON, then upload it via
   #     OAC UI: Data → Connections → Create → "Oracle AI Data Platform".
   aidp-fusion-bundle dashboard install --target oac --oac-url ... --print-only

   # 5c. Run the REST install (snapshot register + restore + poll). The bundle
   #     uses GET /catalog?type=connections&search=<name> to find the existing
   #     connection and skip the POST. Subsequent installs re-use it.
   aidp-fusion-bundle dashboard install --target oac \
     --oac-url https://oac.example.com \
     --bar-bucket aidp-fusion-bundle-bar \
     --bar-uri 'file:///aidp-fusion-bundle/bundle-v0.1.0a0.bar'
   ```
   Uses ONLY Oracle-documented public REST endpoints: `GET /catalog?type=connections&search=<name>` (precheck), `POST /catalog/connections` (skipped when precheck finds the existing connection — the realistic flow), `POST /snapshots`, `POST /system/actions/restoreSnapshot`, `GET /workRequests/{id}`. See `docs/oac_rest_api_setup.md` for the one-time IDCS confidential-app + Object Storage Resource Principal setup.

6. **End users chat with the data** via OAC MCP. Print the MCP config snippet:
   ```bash
   aidp-fusion-bundle dashboard mcp-config
   ```
   Paste into `claude_desktop_config.json` (or Claude Code / Cline / Copilot equivalent), restart the AI client. Then ask "what's our AR aging?" and watch MCP call `discoverData` → `describeData` → `executeLogicalSQL` against `fusion_catalog.gold.ar_aging`.

## Key gotchas (live-validated where ✅)

- **BICC role required** — Fusion user must hold `BIA_ADMINISTRATOR_DUTY` *or* `ORA_ASM_APPLICATION_IMPLEMENTATION_ADMIN_ABSTRACT`. Without it, `/biacm/api/v[12]/*` endpoints 302-redirect to IDCS. Bootstrap probes for this. (✅ Casey.Brown demo pod: BIAdmin granted; works.)
- **BICC External Storage profile** — must be configured **once in the BICC console** (admin task: BICC Console → Configure External Storage → OCI Object Storage Connection tab → bucket name + namespace + region + OCI username + auth token → Test Connection → Save). The `fusion.external.storage` Spark option references this BICC profile name. **There is no parallel AIDP-side registration.** Bundle does not provision the BICC profile; bootstrap verifies it exists.
- **First extract is slow** — BICC builds a full snapshot on first call; subsequent runs use `fusion.initial.extract-date` for incremental.
- **499 row/page hard cap on Fusion REST** (per MOS Doc ID 2429019.1) — bundle's REST fallback enforces this; anything >5k rows must use BICC.
- **OAC MCP is read-only** — it cannot create workbooks or register data sources. Bundle uses **OAC REST API** for write operations; MCP is for end-user chat consumption only.
- **`POST /catalog/connections` REST validator does not bless AIDP `idljdbc`** — Oracle's validator falls through to generic Oracle DB schemas requiring `serviceName`/`password`/`connectionString`. The realistic flow is therefore: customer creates the connection via OAC UI once (using the 6-key JSON written by `--print-only`), then `dashboard install` re-uses it via the precheck on subsequent runs. (✅ Live-validated TC10h-4, 2026-05-03 against disposable OAC1.)
- **Snapshot BAR URI shape is `file:///<folder>/<name>.bar`** — NOT `oci://...`, NOT bare object name, NOT the OCI Object Storage HTTPS URL. None of the seven URI variants tried during TC10h were correct. Verified live TC10h-3.
- **OAC catalog browse needs `search=*`** — `GET /catalog?type=connections` (no search) returns a single-element TypeInfo header (`[{"type":"connections"}]`), NOT the actual list. Bundle's `list_connections` defaults `search="*"` so the precheck works. (Caught + fixed during TC10h-3 live validation.)
- **Use ExtractPVOs for bulk, NOT OTBI reporting PVOs** — pdf1 Pro Tip; bundle's catalog refuses OTBI PVOs with a clear warning.

## References

- Plan: `C:\Users\anuma\.claude\plans\oracle-ai-data-platform-fusion-bundle.md`
- Sibling plugin (single-PVO connector): [`oracle-ai-data-platform-workbench-spark-connectors`](../../../oracle-ai-data-platform-workbench-spark-connectors/)
- Official Oracle BICC blog: https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc
- Ateam blog (saas-batch path): https://www.ateam-oracle.com/how-to-extract-fusion-data-using-oracle-ai-data-platform
- Official sample notebook: [`oracle-aidp-samples/data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb`](../../../../data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb)
- OAC MCP Preview docs: https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html
