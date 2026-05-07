# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Added — Phase 2 dataflow (towards v0.2.0)
- **2026-05-07 (P1.1) — `dimensions/dim_supplier.py`** — first conformed dimension shipped as a Python module. Reads `bronze.erp_suppliers`, dedupes on `SEGMENT1` (supplier_number), handles NULL/0 ID columns with `NULLIF(CAST(... AS BIGINT), 0)`, and writes `silver.dim_supplier` with bronze→silver audit lineage. Ships an `id_populated_pct(spark, column="vendor_id")` helper that downstream marts consult to pick between canonical join and spend-only fallback paths. 14 unit tests pass. **Live-validated** end-to-end on `fusion_bundle_dev` AIDP cluster — see [`tests/live/TC8b_dim_supplier_module_results.md`](tests/live/TC8b_dim_supplier_module_results.md). Productizes (and corrects) TC8's prototype: TC8 misdiagnosed column case (`Segment1` → actually `SEGMENT1`) and overgeneralized "demo pod has masked IDs" — eseb-test does, etap-dev5 doesn't, and the new helper handles both.
- **2026-05-07 (P1.2) — `transforms/gold/supplier_spend.py`** — first gold mart shipped as a Python module. Two SQL forms picked at runtime by `id_populated_pct() >= 0.5`: (a) canonical JOIN form (`silver.dim_supplier × bronze.ap_invoices`), or (b) spend-only fallback (aggregate `bronze.ap_invoices` alone, fill dim attributes with NULL). Both produce identical 10-column output schema so downstream consumers don't care which form ran. 20 unit tests pass (including a schema-parity invariant). **Live-validated**: TC8c reproduces TC8's $3.21B aggregate within 2% on eseb-test (different demo pod, different supplier counts) — see [`tests/live/TC8c_supplier_spend_module_results.md`](tests/live/TC8c_supplier_spend_module_results.md). Top-5 vendor IDs match TC8 exactly. JOIN-form path is theoretically supported and unit-tested but not yet live-verified (blocked on a pod with populated VENDORID).
- **2026-05-07 (P1.4) — `dimensions/dim_calendar.py`** — system-generated calendar dimension; no bronze source. Generates Gregorian + Fiscal calendars for a configurable date range (default 2020-2030, 4,018 days) via `sequence(DATE, DATE, INTERVAL 1 DAY) + EXPLODE`. Surrogate `calendar_key = YYYYMMDD as BIGINT` is deterministic from the date (stable across rebuilds). Configurable `fiscal_start_month` parameter handles calendar-year (default), Jul-Jun, Oct-Sep, etc. 16 unit tests pass. **Live-validated** at 100% — see [`tests/live/TC21_dim_calendar_results.md`](tests/live/TC21_dim_calendar_results.md): 4018 rows, 0 gaps, 0 surrogate-key mismatches, leap day 2024-02-29 present, Saturday/Sunday correctly flagged. Required by `gold.gl_balance` (P1.8) and `gold.po_backlog` (P1.11), both of which are now unblocked dim-side.
- **AIDP Credential Store integration discovered** — `aidputils.secrets.get(name=..., key=...)` is the documented Oracle pattern for resolving secrets from AIDP notebooks (Resource Principals are not exposed to the notebook kernel context, so the bundle's standard OCI-Vault flow can't run there directly; the AIDP credential store wraps Vault references and brokers access via `aidputils`). Used by the `fusion_bundle_dev` cluster bootstrap path. Documented in [`tests/live/TC8b_dim_supplier_module_results.md`](tests/live/TC8b_dim_supplier_module_results.md) §"Live bootstrap on dedicated cluster".
- **Test count**: 139 → **190** (51 new tests, zero regressions).

### Changed (Phase 2 in progress)
- **2026-05-07 — `run` command now exits non-zero when no real work is performed.** Both `--inline` (when `orchestrator.run` is missing) and the default dispatch path (which currently only prints a plan) now return exit code `2` instead of `0`. The plan / status messages still print, but a CI script doing `aidp-fusion-bundle run && next-step` will no longer mistake a no-op for a successful pipeline execution. Exit code returns to `0` once **P1.5** wires the orchestrator entry point + dispatch submission.

### Known limitations (Phase 2 in progress)
- **`run` CLI is a stub until P1.5 lands.** The Phase 2 silver/gold modules shipped above (`dim_supplier`, `dim_calendar`, `supplier_spend`) are importable as a Python package — a customer can call `dim_supplier.build(spark)` etc. directly from inside an AIDP notebook session — but the `aidp-fusion-bundle run` CLI command does **not** yet invoke them. The CLI surfaces this with a clear error (exit 2) and points at the importable module names. Full CLI wiring (orchestrator + notebook entry point + state-table watermarking) lands in P1.5.

### Changed
- **2026-05-03 (TC10h-4 — `dashboard install` end-to-end SUCCESS on disposable OAC1)** — first clean end-to-end run of the install command with all four documented OAC REST calls green. The `find_connection` precheck (already in install.py since TC10h-2) lit up after the `search=*` fix below, allowing the realistic deployment flow to run cleanly: customer creates the AIDP connection via OAC UI once, then re-runs `dashboard install` REST-only thereafter. Evidence: snapshot `bd820501-9a3f-426e-8354-2d8c279b35b2` REGISTERed; workRequest `lfc-cc:13347-c9:3962654` RESTORE_SNAPSHOT SUCCEEDED.
- **2026-05-03 (TC10h-3 — snapshot register/restore round-trip live-validated + 2 helper bug fixes)**:
  - **BAR URI shape**: live-validated correct shape is `file:///<folder>/<name>.bar` (NOT `oci://...`, NOT bare object name, NOT the OCI Object Storage HTTPS URL). Documented in `tests/live/TC10_oac_integration_results.md` § TC10h-3 and `docs/oac_rest_api_setup.md`.
  - **Fixed**: `register_snapshot` now handles the async response shape (`202 + {"workRequestId": "..."}`). Previously assumed synchronous `{"id": "..."}`. Helper polls the work request to terminal status, then resolves the snapshot record by name (or via `workRequest.resources[].identifier`). Added `wait=False` opt-out for callers that want raw async.
  - **Fixed**: `list_connections` now defaults `search="*"`. Without it, OAC's `/catalog?type=connections` returns a single-element TypeInfo header (`[{"type":"connections"}]`) instead of items, causing `find_connection` to always return `None`. TypeInfo header rows are filtered out of the result.
  - **Fixed**: `find_connection` now passes `search=<name>` server-side for narrowing and enforces exact-match client-side (OAC search is substring; `aidp_fusion_jdbc` would otherwise false-match `aidp_fusion_jdbc_v2`).
  - Tests: 132 → 139 passing (+3 async REGISTER paths, +4 list/find connection paths).
- **2026-05-01 (TC10h-2 refactor)** — OAC integration refactored to use only Oracle-documented public REST endpoints, after a full audit of the OAC docs portal + canonical openapi.json:
  - **Removed** (endpoints not in openapi.json): `OacRestClient.import_workbook` (POST /catalog/workbooks/imports is UI-only), `OacRestClient.export_workbook` (would have exported PDF/PNG, not .dva), `OacRestClient.delete_workbook` (no public DELETE), the `dashboard export` CLI subcommand.
  - **Fixed**: `list_connections` now uses the documented `/catalog?type=connections&search=` shape. `delete_connection` Base64URL-encodes the `'<owner>'.'<name>'` object ID per Oracle's contract.
  - **Added**: snapshot lifecycle helpers (`register_snapshot`, `restore_snapshot`, `poll_work_request`, `delete_snapshot`, `get_snapshot`, `list_snapshots`). New `WorkRequestStatus` enum + `encode_catalog_id` helper. Snapshot register reads from OCI Object Storage with Resource Principal auth.
  - **Reworked**: `dashboard install` now performs four documented REST calls (POST /catalog/connections, POST /snapshots, POST /system/actions/restoreSnapshot, GET /workRequests/{id} polling). Workbook content delivery is via a single `bundle-vN.bar` snapshot the customer uploads to their own OCI Object Storage bucket (instead of per-workbook .dva files in the repo).
  - **Added** CLI flags: `--bar-bucket`, `--bar-uri`, `--bar-password`, `--snapshot-name`, `--overwrite-connection`, `--prompt-login`. **Removed**: `--workbooks-dir`.
  - **Bundle.yaml schema**: replaced `oac.workbooks: [...]` with `oac.snapshot: {bucket, uri, password, snapshotName}`.
- **2026-04-30 (TC10h)** — OAC auth model corrected to Authorization Code + PKCE + Refresh Token (after Oracle's docs explicitly forbade client_credentials grant). Audience now auto-discovered via OAC `/ui/` redirect probe. AIDP `idljdbc` connectionType captured from UI network traffic.

### Added
- Initial repo skeleton (2026-04-30): plugin metadata, skill, Python package skeleton, schema models, BICC + REST extractors mirroring the official Oracle AIDP sample, examples, unit tests.

### Tests
- 139/139 passing (was 132 after TC10h-2; net +7 for TC10h-3 fixes — 3 async REGISTER paths, 4 list/find connection paths). End-to-end `dashboard install` validated live on disposable OAC1.

## [0.1.0-alpha] — 2026-05-05

Phase 1 deliverable per [PLAN](../../../.claude/plans/oracle-ai-data-platform-fusion-bundle.md): core BICC path + Supplier Extract.

### Achieved
- BICC extractor for `FscmTopModelAM.SupplierExtractPVO` mirroring [`oracle-aidp-samples/data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb`](../../../data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb)
- GL trio (Journal Lines, Period Balances, Chart of Accounts)
- `dim_account` + `dim_calendar` + `dim_supplier`
- Bootstrap probe (BICC role, External Storage profile, IAM policy)
- Live-test TC1-TC8 against demo Fusion pod (`saasfademo1`)

## [0.1.0] — TBD (after live tests pass)

End-of-Phase-3 release. Tier-1 gate per the plan.
