# Changelog

All notable changes to this project are documented here.

## [Unreleased]

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
