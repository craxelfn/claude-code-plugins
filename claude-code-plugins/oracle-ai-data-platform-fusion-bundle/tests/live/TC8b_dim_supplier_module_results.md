# TC8b — `silver.dim_supplier` module re-validation (2026-05-07)

> **Status**: ✅ **PASS (live bootstrap)** — bronze tables landed on the dedicated `fusion_bundle_dev` cluster (workspace `f95a83f8-9bd1-4259-a45f-ea1c3a5a7516`); column shape probed via live Spark. **Silver `dim_supplier.build()` end-to-end run still pending** — happens once the dim module is wired into the orchestrator (P1.5).
>
> **Why this exists**: P1.1 (`dimensions/dim_supplier.py`) productizes TC8's inline silver step. While preparing the implementation plan, we discovered two errors in TC8's findings that materially affect the SQL builder. This document captures the re-validation that surfaced them.

## Method

The TC8 supplier-extract `csv.gz` files are still present in the original OCI Object Storage bucket from the 2026-04-30 run. Rather than re-running the full BICC → Spark pipeline (requires AIDP cluster session, see §"What's still pending"), we:

1. Listed the bucket contents via `oci os object list`
2. Downloaded the file referenced by TC1 (`...supplierextractpvo-9577814-20260430_152423.csv.gz`)
3. Decompressed and parsed the CSV directly with stdlib `gzip` + `csv`
4. Verified row/column counts, sampled values, and computed populated-percentage per ID column

Bucket: `oci://fusion-bicc-saasfademo1@idseylbmv0mm/`

## Confirmations of TC1 / TC8 (still true)

| Claim | Verified |
|---|---|
| 229 rows in supplier extract | ✅ exact match |
| 143 columns | ✅ exact match |
| Segment1 values `1252, 1254, 1256, 1265, 1266` present | ✅ all 5 found |
| Segment1 (supplier_number) is 100% populated | ✅ confirmed |
| BICC pipeline wrote to `oci://fusion-bicc-saasfademo1` correctly | ✅ artifacts intact since 2026-04-30 |

## 🚨 Corrections to TC8 — column case + populated-ID claims

### Correction 1 — Column names are UPPERCASE, not mixed-case

TC8's prose referenced columns as `Segment1`, `VendorId`, `PartyId`, `Vendor`, `LastUpdateDate`. The actual BICC CSV header uses **all-uppercase, no spaces**:

| TC8 wrote | Reality in extract | Header position |
|---|---|---|
| `Segment1` | `SEGMENT1` | #30 |
| `VendorId` | `VENDORID` | #39 |
| `VendorId1` | `VENDORID1` | #40 |
| `PartyId` | `PARTYID` | #29 |
| `ParentVendorId` | `PARENTVENDORID` | #28 |
| `ParentPartyId` | `PARENTPARTYID` | #27 |
| `LastUpdateDate` | `LASTUPDATEDATE` | #18 |
| `CreationDate` | `CREATIONDATE` | #11 |
| `Vendor` (claimed name col) | does not exist; see Correction 3 | — |

**Probable cause**: TC8 used pdf1's field-name documentation (PVO Java-style attribute names) verbatim, but BICC exports normalize to DB column names (Oracle DBA UPPERCASE convention).

### Correction 2 — `VENDORID` and `PARTYID` are 100% populated on demo pod

TC8 stated:

> *"the demo pod's SupplierExtractPVO returns VendorId, VendorId1=0, PartyId, ParentVendorId, ParentPartyId all NULL or 0."*

Bytes from the same extract file say otherwise:

| Column | Pct populated | TC8 claimed |
|---|---:|---:|
| `SEGMENT1` | 100.0% | 100% ✅ |
| **`VENDORID`** | **100.0%** | 0% ❌ |
| `VENDORID1` | 0.0% | 0% ✅ |
| **`PARTYID`** | **100.0%** | 0% ❌ |
| `PARENTVENDORID` | 0.4% | 0% ✅ basically |
| `PARENTPARTYID` | 0.4% | 0% ✅ basically |

Sample real values:

```
SEGMENT1   VENDORID             PARTYID              CREATEDBY
1252       300000047414503      300000047414501      CALVIN.ROTH
1254       300000047414635      300000047414633      CALVIN.ROTH
1256       300000047507113      300000047507111      CALVIN.ROTH
1272       300000047837244      300000047837242      LIZ.MORGAN
1274       300000049521222      300000049521220      LIZ.MORGAN
```

**Smoking gun**: all 5 of TC8's top-spending vendors from `gold.supplier_spend` (`300000047507499`, `300000075895541`, `300000047414571`, `300000047414635`, `300000047414679`) are present verbatim in the supplier extract's `VENDORID` column.

**Inference**: TC8's "zero matches when joining on vendor_id" was almost certainly a column-case bug in the prototype query (`VendorId` vs `VENDORID`). Spark/Delta with `spark.sql.caseSensitive=true` (or schema mismatch) would silently return NULL for the wrong-case column → 0% join hit rate → developer concludes "data is missing" → falls back to spend-only path.

### Correction 3 — `Vendor` (supplier name) column doesn't exist

TC8 referenced "`Vendor` (human name)" — `CALVIN.ROTH`, `anu.rathi`, etc. The actual extract has **no `VENDOR` or `VENDORNAME` column**. The `CALVIN.ROTH` value lives in `CREATEDBY` (header position #10), which is the **Fusion user who created the supplier record**, not the supplier's name.

The 143-column extract has multiple name-shaped fields (`ALIASPARTYNAME`, `ALTERNATENAMEPARTYNAME`, etc.), but the primary supplier DBA name needs identification via `DESCRIBE`-and-sample on the live AIDP catalog.

## Impact on P1.1 plan

The PLAN_P1.1_dim_supplier.md was updated 2026-05-07 to reflect:
- **§3.3 / §3.4** — all SQL uses UPPERCASE column refs
- **§3.3** — `supplier_name` becomes `supplier_name_proxy` until the right column is identified
- **§4.2** — `id_populated_pct(vendor_id)` expectation flipped from `0.0` to `1.0`; new assertion: 5/5 top-spend vendors must appear in `silver.dim_supplier.vendor_id`
- **§6 / §7** — column-name risk closed; supplier-name column risk added (smaller scope)

## Side effect on backlog

**P3.7 ("customer pod with populated supplier IDs is needed to validate the join-form `gold.supplier_spend`")** is **no longer a blocker.** The demo pod has populated `VENDORID` and `PARTYID`. The canonical join can be validated on demo pod once we run the real Spark pipeline (P1.2 implementation — see §"What's still pending").

The BACKLOG entry for P3.7 should be marked accordingly when next touched.

## Live bootstrap on dedicated cluster (2026-05-07 evening)

A dedicated cluster `fusion_bundle_dev` (id `838d3aff-1f5a-4aec-a020-57fd9ad27b58`) was provisioned in workspace `f95a83f8-9bd1-4259-a45f-ea1c3a5a7516`. The tpcds workspace from TC1 is gone. Bootstrap notebook:
1. Created `fusion_catalog` (INTERNAL) + `bronze`/`silver`/`gold` schemas
2. Resolved BICC password via `aidputils.secrets.get(name="fusion_bicc_password", key="password")` — AIDP's documented Credential Store API ([Oracle AIDP Workbench docs — Credential Store](https://docs.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/ai-data-platform/aidwn&id=AIDUG-GUID-2EB8F6D9-702E-4427-96B7-288DC4C19C3C)).
3. Pivoted from etap-dev5 → eseb-test pod after Casey.Brown creds rotated; used `natalie.salesrep` instead. Required a different External Storage profile name (`fusion_bicc_external_storage_natalie`).

**Bootstrap results**:

| Table | Rows | TC1/TC8 expected (etap-dev5) | Δ |
|---|---|---|---|
| `bronze.erp_suppliers` | **209** | 229 | -9% (different demo pod) |
| `bronze.ap_invoices` | **49,552** | 49,985 | -1% |

## Live findings — supplier-name column + per-pod data shape

### Per-pod data shape varies (important — bundle must handle BOTH)

| Aspect | etap-dev5 (TC1, via CSV read) | eseb-test (today, via live Spark) |
|---|---|---|
| Supplier rows | 229 | 209 |
| `VENDORID` populated | 100% (sample: `300000047414503`) | **0% — all NULL** |
| `PARTYID` populated | 100% | **0% — all NULL** |
| `SEGMENT1` populated | 100% | 100% |
| `BUSINESSRELATIONSHIP` | 100% (`SPEND_AUTHORIZED`) | 100% |

**Implication**: TC8's "VendorId is NULL on demo pod" claim was wrong for etap-dev5 but right for eseb-test. **Both shapes are real.** The bundle's `id_populated_pct(silver_table, column="vendor_id")` helper is the correct pattern — it returns `0.0` on eseb-test (P1.2 chooses spend-only fallback) and `1.0` on etap-dev5 (P1.2 chooses canonical join). Defensive design wins.

### Supplier-name column on eseb-test (147 cols, 209 rows)

No single column is 100%-populated. Coalesce chain (in order):

| Source column | Pop% | Sample values |
|---|---|---|
| `AlternateNamePartyName` | **7.2%** | `Dell Inc`, `Cardinal Health, Inc`, `St. Jude Medical S.C.` |
| `AliasPartyName` | 0.5% | `Becton Dickinson` |
| `TaxReportingName` | 1.4% | `David Draper`, `ABC Consulting` |

`dim_supplier.py` ships with `COALESCE(AlternateNamePartyName, AliasPartyName, TaxReportingName, NULL)` for `supplier_name`. Production pods are expected to populate at least one of these cleanly. On demo, ~92% of rows will have NULL `supplier_name` — that's accurate; the bundle does not invent data.

### Column naming differs by PVO

A surprise — `bronze.erp_suppliers` and `bronze.ap_invoices` use **different column-naming conventions** on this cluster:

| Bronze table | Convention | Examples |
|---|---|---|
| `erp_suppliers` (`SupplierExtractPVO`) | UPPERCASE, no prefix | `SEGMENT1`, `VENDORID`, `BUSINESSRELATIONSHIP` |
| `ap_invoices` (`InvoiceHeaderExtractPVO`) | PascalCase + `ApInvoices` prefix | `ApInvoicesVendorId`, `ApInvoicesInvoiceAmount`, `ApInvoicesApprovalStatus` |

`dim_supplier.py` uses UPPERCASE refs (correct). P1.2's `gold.supplier_spend` will need PascalCase `ApInvoices*` refs.

## Status

| Aspect | Status |
|---|---|
| Bucket reachability + creds | ✅ PASS |
| Row/column count match TC1 | ✅ PASS (229 / 143) |
| Sample-value spot check (Segment1, top vendors) | ✅ PASS (5/5) |
| Column-name correction documented | ✅ PASS |
| Populated-ID correction documented | ✅ PASS |
| Live Spark `dim_supplier.build()` | ⏸ pending AIDP session |
| Live join-form `gold.supplier_spend` | ⏸ pending P1.2 implementation |

**Net**: P1.1 implementation can proceed with corrected column names. Live Spark verification is a follow-up that does not block tomorrow's PR.

## References
- Original TC8 results (the document being corrected): [`TC8_supplier_spend_results.md`](TC8_supplier_spend_results.md)
- TC1 / TC7 BICC bronze evidence: [`TC1_TC7_results.md`](TC1_TC7_results.md)
- Catalog entry under correction: [`schema/fusion_catalog.py:67-78`](../../scripts/oracle_ai_data_platform_fusion_bundle/schema/fusion_catalog.py#L67) (no change required — bronze table name is unaffected)
- P1.1 plan: [`PLAN_P1.1_dim_supplier.md`](../../PLAN_P1.1_dim_supplier.md) (untracked working note)
