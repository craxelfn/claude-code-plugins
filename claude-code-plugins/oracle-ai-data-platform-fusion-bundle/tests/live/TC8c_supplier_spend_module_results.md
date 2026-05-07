# TC8c — `gold.supplier_spend` mart live verification (2026-05-07)

> **Status**: ✅ **PASS (live, spend-only fallback path)** — `gold.supplier_spend` materialized end-to-end on `fusion_bundle_dev` cluster against `bronze.ap_invoices` (49,552 rows) and `silver.dim_supplier` (209 rows). Reproduces TC8's $3.2B aggregate within 2% on the eseb-test pod. Picker correctly chose the spend-only fallback path (eseb-test has all-NULL `vendor_id`).

## Test lineage

* **TC8** (2026-04-30, etap-dev5) — original prototype: hand-written SQL in a notebook produced gold.supplier_spend with $3.21B / 236 records / 116 vendors. Used spend-only path because TC8 misdiagnosed the demo pod's `VendorId` column (column-case bug; verified via direct CSV read in TC8b).
* **TC8b** (2026-05-07, eseb-test) — productized `silver.dim_supplier` as `dimensions/dim_supplier.py`. 209 rows, dedupe + NULLIF + COALESCE name chain all live-validated.
* **TC8c** (2026-05-07, eseb-test) — productized `gold.supplier_spend` as `transforms/gold/supplier_spend.py`. **This file.**

## Picker logic — verified live

`build()` calls `id_populated_pct(spark, silver_table=..., column="vendor_id")` and compares against `DEFAULT_JOIN_THRESHOLD = 0.5`. On eseb-test:

```
[picker] id_populated_pct(vendor_id) = 0.000, threshold = 0.5
[picker] use_join_form = False  →  spend-only fallback chosen
```

This is the canonical demo-pod path. Production pods (where `vendor_id` is populated) would land on the JOIN form — same module, different runtime decision, identical output schema.

## Counts vs TC8 reference

| Metric | TC8 (etap-dev5) | TC8c (eseb-test) | Δ |
|---|---:|---:|---:|
| Spend records | 236 | **230** | -2.5% |
| Distinct vendors | 116 | **113** | -2.6% |
| Approved records | 109 | **108** | -0.9% |
| Grand total | $3,208,423,850.91 | **$3,145,528,157.43** | -2.0% |

Differences explained: eseb-test is a different demo pod with slightly different data (209 vs 229 suppliers, 49,552 vs 49,985 invoices). The relative shape — same top-5 vendor IDs, same approval-status split, same aggregation grain — is preserved.

## Top-5 vendors by total invoice amount

The same five vendor IDs that TC8 surfaced as top spenders show up here in identical order:

| vendor_id | approval_status | invoice_count | total_invoice_amount | total_paid | last_invoice_date |
|---|---|---:|---:|---:|---|
| 300000047507499 | APPROVED | 2,944 | $876,649,485.57 | $861,814,067.51 | 2025-07-16 |
| 300000075895541 | APPROVED | 461 | $447,250,758.55 | $442,114,342.01 | 2025-07-10 |
| 300000047414571 | APPROVED | 2,269 | $392,346,309.29 | $384,139,988.69 | 2025-07-10 |
| 300000047414635 | APPROVED | 1,999 | $293,786,226.47 | $254,566,710.74 | 2025-07-10 |
| 300000047414679 | APPROVED | 1,293 | $162,637,727.49 | $161,171,206.05 | 2025-07-10 |

(TC8 first three: $892.7M / $453.1M / $399.3M — same vendors, ~2% lower numbers here, consistent with the overall aggregate delta.)

## Fallback null-fill check (schema parity)

Both forms must produce the same column set so downstream consumers (workbooks, GenAI prompts, JDBC clients) don't need to know which form ran. The unit test asserts this; live evidence:

| Column | NULL count / total | Result |
|---|---|---|
| `supplier_number` | 230/230 (100%) | ✅ all NULL — fallback semantics |
| `supplier_name` | 230/230 | ✅ |
| `business_relationship` | 230/230 | ✅ |

In contrast, the join-form populates these from `silver.dim_supplier`. The schema-parity invariant is preserved across forms.

## Final schema (10 columns — matches plan)

```
vendor_id              bigint
supplier_number        string         # NULL on fallback; populated on join
supplier_name          string         # NULL on fallback; populated on join
business_relationship  string         # NULL on fallback; populated on join
approval_status        string
invoice_count          bigint
total_invoice_amount   decimal(31,2)
total_paid             decimal(31,2)
last_invoice_date      date
gold_built_at          timestamp
```

## Verdict

**TC8c: ✅ PASS.** P1.2 acceptance criteria fully satisfied:
- ✅ Module reads `bronze.ap_invoices`, optionally joins `silver.dim_supplier`, writes `gold.supplier_spend`
- ✅ Demo-pod / production switch works at runtime via `id_populated_pct() >= 0.5`
- ✅ Unit tests cover both forms + picker + schema parity (20 cases / 23 sub-assertions, all pass; 173/173 full suite pass with zero regressions)
- ✅ Live row added — this section, with TC8c runner output evidence

`gold.supplier_spend` is now ready for downstream consumption (OAC workbooks, GenAI grounding, JDBC clients). The pattern is set for the remaining 4 gold marts: `gl_balance` (P1.8), `ap_aging` (P1.9), `ar_aging` (P1.10), `po_backlog` (P1.11).

## What's still pending

* **JOIN form live verification** — needs a pod with populated `VENDORID`/`PARTYID` (etap-dev5 or a customer pod). Currently blocked by Casey.Brown credential rotation (P3.7 in BACKLOG). The branch is theoretically supported (same module, same SQL, just `pct >= threshold` returns true instead of false) and the helper math has been live-verified to return 0.0 — it would simply return 1.0 on the production-shape side.

## References

* P1.2 acceptance criteria: [`BACKLOG.md`](../../BACKLOG.md) §P1.2 (untracked working note)
* TC8 original prototype evidence: [`TC8_supplier_spend_results.md`](TC8_supplier_spend_results.md)
* TC8b dim_supplier module: [`TC8b_dim_supplier_module_results.md`](TC8b_dim_supplier_module_results.md)
* Module: [`scripts/.../transforms/gold/supplier_spend.py`](../../scripts/oracle_ai_data_platform_fusion_bundle/transforms/gold/supplier_spend.py)
* Unit tests: [`tests/unit/test_supplier_spend.py`](../unit/test_supplier_spend.py)
