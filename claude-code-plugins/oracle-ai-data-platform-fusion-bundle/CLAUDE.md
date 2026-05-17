# `oracle-ai-data-platform-fusion-bundle` — working principles

> These are the load-bearing principles for working on this plugin. They sit on top of (not instead of) the workspace-level `/Users/oussamalakrafi/Workspace/CLAUDE.md` AIDP rules. The mechanical PR checklist lives in [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Mission

The fusion-bundle plugin must run on **any Fusion ERP/HCM/SCM tenant**, not just the `saasfademo1` demo pod. Hardcoded tenant-specific assumptions are bugs. The plugin's value proposition is that a customer can clone the repo, point it at *their* Fusion + AIDP + OAC, and have bronze + silver + gold materialize without editing Python.

## What varies per tenant — and where it comes from

There are two distinct categories of variability. Treat them differently.

### Tenant-declared *policy* → `bundle.yaml`

Things the customer knows upfront and declares. The orchestrator reads these and threads them into every `build()` call. **Never probe them.**

- 3-part table names — `aidp.catalog`, `aidp.bronzeSchema`, `aidp.silverSchema`, `aidp.goldSchema`
- Aging bucket boundaries, NET-N residual fallback, cancelled-flag truthy value
- Fiscal start month, FY-naming convention, calendar date range
- COA semantic-segment map (when the customer's COA differs from Fusion-conventional six)

### Data-shape *discovery* → runtime probe

Things the customer often doesn't know without running `DESCRIBE` themselves — they depend on what BICC actually emitted, which depends on extension packages, export config, and data quality. **Probe these.**

- Column-name dialects (`ApInvoicesInvoiceCurrencyCode` vs `ApInvoicesCurrencyCode`)
- Variant-with-semantics (cancelled = `CancelledDate` null-means-not vs `CancelledFlag = 'Y'`)
- Coverage measurements that gate downstream behavior (real-vs-proxy AP aging at 80% threshold)
- Populated-segment detection for COA sizing

**Detection contract**:
1. Required columns (no meaningful fallback — currency, vendor_id, invoice_amount) → priority list (`KNOWN_*_ALIASES`) → hard-fail with `ValueError` naming the aliases tried.
2. Optional attributes (dim columns like `supplier_name`, `business_relationship`) → `COALESCE` through known alternates → emit NULL when absent. Don't gate.
3. **Explicit `kwarg=` always wins over detection.** Customers who know their tenant short-circuit the probe.

**Add knobs evidence-driven, not preemptively.** A knob ships when a real tenant has hit the variant, or a published Oracle source documents it. Don't speculate. See [`BACKLOG.md`](BACKLOG.md) P2.22 for the deferred-knob backlog.

## Architecture: the CLI is the contract

`aidp-fusion-bundle run --mode seed` must materialize bronze + silver + gold end-to-end. If a customer has to open a notebook and call `build()` by hand, the architecture has drifted from the README.

- **Every new dim/mart is wired into the orchestrator DAG in the same PR.** No leaf modules without a caller. (This is the failure mode of `oussama-dev` through 2026-05-11 — 6 working `build()` functions, zero callers.)
- **The orchestrator owns state.** Watermarks, `fusion_bundle_state`, run-IDs are the orchestrator's responsibility. Modules read Spark in, write Spark out, accept paths and kwargs. A module that touches `fusion_bundle_state` directly is a layering violation.
- **Modules are stateless library functions.** Each exposes `build_<mart>_sql(...) → str` for unit testing without Spark, plus `build(spark, ...) → DataFrame` that executes it. No exceptions to this pattern.

## Medallion correctness invariants

- **`CREATE OR REPLACE TABLE` is for seed mode only.** Incremental mode uses `MERGE INTO target USING <filtered-by-watermark> ON target.<natural_key> = src.<natural_key>`. Full rewrite on every refresh defeats the medallion concept. Exception: `dim_calendar` is deterministic + tiny, stays on `CREATE OR REPLACE`.
- **Surrogate keys are deterministic — `xxhash64(natural_key)`, never `monotonically_increasing_id()`.** Non-deterministic surrogates break incrementality and Type-2 SCD.
- **`COALESCE(amount, 0)` around every arithmetic operation on Fusion amount columns.** Fusion legitimately emits NULL for absent period components, opening balances on new accounts, sparse dim attributes. Without `COALESCE`, a single NULL nullifies the entire expression (NULL propagation). Live-validated: ~20% of `gl_period_balances` sample rows hit at least one NULL component (2026-05-09).
- **Currency-in-grain is mandatory for any amount aggregate.** Cross-currency rollup is the consumer's responsibility, never the mart's. Hard-fail the build if currency col is missing — no single-currency-summed marts on a multi-currency tenant.
- **Prefer one financially-correct SQL shape (single LEFT JOIN, fact preserved) over runtime path-selection.** Runtime decisions belong in data-quality gates (real-vs-proxy), not join topology. The round-6 audit killed the `id_populated_pct >= 0.5` picker for exactly this reason.
- **Audit columns are non-negotiable.** Bronze: `_extract_ts`, `_source_pvo`, `_run_id`, `_watermark_used`. Silver: `bronze_extract_ts`, `bronze_source_pvo`, `silver_built_at`, `silver_run_id`. Gold: `gold_built_at`, `gold_run_id`. SOX trail — the `<layer>_run_id` columns join silver/gold rows to `fusion_bundle_state.run_id`, so a row in `gold.ap_aging` can be traced to the exact orchestrator run that produced it. Modules accept `run_id: str | None = None` as a keyword-only kwarg on `build(...)`; when None (standalone notebook / unit-test use), the audit column emits NULL; when threaded by the orchestrator, the literal `run_id` is embedded in the SQL. See PLAN_P1.5_orchestrator.md §3.5a.

## Testing discipline

- **Live evidence is required for any plugin-portability claim.** Unit tests verify SQL shape; only a live run against a real tenant's BICC extract verifies the SQL actually works. A new mart isn't done when `pytest` passes — it's done when there's a `tests/live/TC<N>_<mart>_results.md` showing real numbers from a real pod.
- **Live evidence on at least one non-`saasfademo1` tenant before any "plugin-portable" claim ships.** Verifying on the demo pod proves it works on the demo pod, not that it's portable. Tracked as P3.7 / P3.9.
- **The empty-source case is part of the contract.** Every dim and mart produces a sensible empty result (zero rows, correct schema, audit columns populated) when bronze is empty. Don't crash, don't silently relabel — `ap_aging`'s "empty population → real mode" decision is the template.

## Fusion specifics — non-obvious

- **PVO names use the full AM-hierarchy from live BICC.** `FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO`, not pdf1's abbreviated `FscmTopModelAM.SupplierExtractPVO`. `catalog probe` is the source of truth; the curated catalog in [`schema/fusion_catalog.py`](scripts/oracle_ai_data_platform_fusion_bundle/schema/fusion_catalog.py) reflects live confirmation.
- **Bronze column conventions are inconsistent across PVOs.** `SupplierExtractPVO` uses UPPERCASE no prefix (`SEGMENT1`, `VENDORID`). `InvoiceHeaderExtractPVO` uses PascalCase with `ApInvoices` prefix (`ApInvoicesVendorId`). `BalanceExtractPVO` uses PascalCase with `Balance` prefix. Document the convention in each module's docstring; don't assume uniformity.
- **OAC's REST validator does not bless AIDP's `idljdbc` connectionType.** Customer creates the OAC connection via the UI once (using the 6-key JSON from `--print-only`); subsequent `dashboard install` runs reuse via precheck. Documented in [`docs/oac_rest_api_setup.md`](docs/oac_rest_api_setup.md). Don't try to bypass with REST.

## Cross-references

- Backlog (untracked working notes): [`BACKLOG.md`](BACKLOG.md)
- Status snapshot (untracked working notes): [`STATUS.md`](STATUS.md)
- Live evidence trail: [`tests/live/`](tests/live/)
- Limit registry (known L1/L2 caveats): [`LIMITS.md`](LIMITS.md)
- Plugin reference set: `/Users/oussamalakrafi/Workspace/Claude-Context/claude-code-plugins-ahmed/07-fusion-bundle-plugin.md`
- Workspace-level AIDP rules: `/Users/oussamalakrafi/Workspace/CLAUDE.md`
