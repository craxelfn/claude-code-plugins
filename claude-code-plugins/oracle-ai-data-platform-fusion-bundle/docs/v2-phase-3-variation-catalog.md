---
title: Phase 3 variation-point audit — v1 modules → v2 pack vocabulary
generated_for: v2-phase-3-export-modules-to-sql
date: 2026-06-05
---

# Variation-point catalog

This document audits every tenant-variation knob the five v1 silver/gold
modules currently probe at runtime and maps each one to the v2 content-pack
vocabulary (`columnAliases.<name>` / `semanticVariants.<name>` /
`profile.<key>` / `column.<name>` / `semantic.<name>` renderer tokens).

**Evidence discipline (PLAN §13.3.2)**: Phase 3 catalogs only what v1 modules
already probe against real tenants OR what published Oracle source documents
shows. No speculative additions.

## Existing pack.yaml declarations (no change in Phase 3)

| v1 probe | pack.yaml binding | Renderer token | Source module |
|---|---|---|---|
| `SEGMENT1` (supplier natural key) | `columnAliases.supplier_natural_key` (candidates: `[SEGMENT1]`) | `{{ column.supplier_natural_key }}` | `dim_supplier.py` (line 93) |
| `VENDORID` | `columnAliases.vendor_id` (candidates: `[VENDORID]`) | `{{ column.vendor_id }}` | `dim_supplier.py` (line 101) |
| `KNOWN_CURRENCY_COL_ALIASES` | `columnAliases.invoice_currency_code` (candidates: `[ApInvoicesInvoiceCurrencyCode, ApInvoicesCurrencyCode]`) | `{{ column.invoice_currency_code }}` | `supplier_spend.py` (lines 77–80), `ap_aging.py` (line 557) |
| `cancelled_*` probe (date / flag) | `semanticVariants.cancelled_status` (candidates: `cancelled_date`, `cancelled_flag`) | `{{ semantic.cancelled_status }}` | `ap_aging.py` (lines 544–553) |

## NEW pack.yaml declarations (added in Phase 3)

### COA segment identifiers — Step 1's primary ask

`dim_account.py:122` reads `CodeCombinationSegment1..CodeCombinationSegment30`
as source columns; `DEFAULT_SEMANTIC_SEGMENT_MAP` (lines 109–116) maps
positions 1–6 to semantic roles (`company`, `cost_center`, `account`,
`subaccount`, `product`, `intercompany`). The starter pack's
`dim_account.sql` (Step 6) needs to substitute three SQL identifiers —
balancing / cost-center / natural-account — at render time. These cannot
be `profile.*` lookups because the renderer binds those as parameters,
not identifiers (`sql_renderer.py:23` / `:422`).

Three NEW `columnAliases`, all `appliesTo: bronze.gl_coa`, candidates
enumerate the conventional six positions (tenants whose COA places
balancing/cost-center/natural-account at higher positions extend the
candidate list in a pack overlay):

| columnAlias | candidates | finance-default `resolved.column.*` |
|---|---|---|
| `coa_balancing_segment` | `[CodeCombinationSegment1, …Segment2, …Segment3, …Segment4, …Segment5, …Segment6]` | `CodeCombinationSegment1` |
| `coa_cost_center_segment` | (same list) | `CodeCombinationSegment2` |
| `coa_natural_account_segment` | (same list) | `CodeCombinationSegment3` |

**Bootstrap nuance**: all six `CodeCombinationSegmentN` columns exist on
every Fusion `gl_coa` extract, so bootstrap's "first column that exists"
heuristic cannot pick the role. COA-role resolution is a tenant
configuration call, not a probe — the finance-default profile pre-authors
the resolution; tenants override via `profiles/<tenant>.yaml`. A future
bootstrap-skill task must not auto-resolve COA aliases blindly.

The pre-existing `profiles.finance-default.chartOfAccounts.{balancingSegment,
costCenterSegment, naturalAccountSegment}` block in pack.yaml (values
`segment1`/`segment2`/`segment3` — *positional output aliases*, not source
column names) is migration-period redundant: Phase 3 SQL templates consume
`resolved.column.coa_*`, not `profile.chartOfAccounts.*`. The block stays
in pack.yaml so Phase 1 / Phase 2 validation surface is unchanged; a
comment marks it superseded by the new columnAliases.

## Audit discoveries — DEFERRED (NOT added in Phase 3)

### `ApInvoicesCancelDate` alias variant

`ap_aging.py:546-549` probes a third cancelled-status variant
(`ApInvoicesCancelDate` — the alias variant some Fusion extracts use,
omitting the "led" suffix). The v1 comment marks it as an alias of
`ApInvoicesCancelledDate` with identical semantics. This IS real-tenant
evidence (probed in live code) and would add cleanly as a third
candidate to the existing `semanticVariants.cancelled_status`:

```yaml
- id: cancelled_date_alias
  detect:
    columnExists: ApInvoicesCancelDate
  fragment: "{table}.ApInvoicesCancelDate IS NULL"
```

**Deferred to a follow-up Phase 3 fix-commit** rather than landing in the
initial migration: the existing pack.yaml shape has only two candidates
and adding a third without a corresponding live-evidence test artefact
risks under-validated variation. The v1 module continues to handle this
variant in legacy-python mode while Phase 3 ships; once a live tenant
hits the variant under content-pack execution, a fix-commit adds the
candidate.

### Optional `terms_date_col` / `due_date_col`

`ap_aging.py:541-542` probes for `ApInvoicesTermsDate` / `ApInvoicesDueDate`
and emits `None` when absent. v1 then SQL-templates the column ref or
substitutes a NULL literal based on presence. This is an *optional
identifier substitution* — the renderer's current `{{ column.<name> }}`
vocabulary REQUIRES a resolved identifier (raises AIDPF-5003 on missing).

**Deferred — requires renderer support beyond Phase 3 scope**: an
"optional column" token semantic (or a templating shape like
`{{ column.<name> | nullable }}` that emits `NULL AS <alias>` when
unresolved) is a new renderer feature, not a v1 ↔ v2 1:1 export. Phase 3
makes a pragmatic choice for the `ap_aging.sql` template: assume both
columns are present (Fusion-conventional; saasfademo1 confirmed) and
document the limitation in `LIMITS.md` for follow-up.

## `dim_calendar` (builtin) — no SQL template

ADR-0011: `dim_calendar` stays `implementation.type: builtin`. Phase 3's
contribution is the content-pack builtin-dispatch path (Step 3) and the
widened `outputSchema` (16 columns matching the actual builtin emit at
`dim_calendar.py:94-138`). No variation points — the calendar is
parameter-driven, not bronze-driven.

Calendar parameters (consumed by the new `dim_calendar_adapter` per Step 3):

| Parameter | Source (precedence: tenant profile → pack default → builtin default) |
|---|---|
| `start_date` | `profile.profile.calendar.startDate` → `pack.pack.profiles[<active>].calendar.startDate` (`'2020-01-01'`) → builtin default (`'2020-01-01'`) |
| `end_date` | `profile.profile.calendar.endDate` → pack default (`'2030-12-31'`) → builtin (`'2030-12-31'`) |
| `fiscal_start_month` | `profile.profile.calendar.fiscalStartMonth` → pack default (`1`) → builtin (`1`) |
| `silver_table` | `f"{ctx.catalog}.{ctx.silver_schema}.{node.target}"` |
| `run_id` | `ctx.run_id` |

## Snapshot-date handling — Step 2 dedicated token

`ap_aging.py` uses `CURRENT_DATE()` inline for bucket anchoring. v2 SQL
template uses the new dedicated `{{ snapshot_date }}` renderer token
(Step 2), NOT `{{ profile.snapshotDate }}` (the generic profile resolver
binds values as parameters, not as DATE expressions). Semantics:

- `profile.profile.snapshotDate` absent / empty → emit literal
  `CURRENT_DATE()` (production default).
- Present + valid ISO date (`^\d{4}-\d{2}-\d{2}$`) → bind as
  `:snapshot_date` parameter (test determinism).
- Anything else → reject with `AIDPF-5013` (`InvalidSnapshotDateError`).

## Summary

| v1 probe | Disposition in Phase 3 |
|---|---|
| `SEGMENT1` / `VENDORID` | Already in pack.yaml (`columnAliases.supplier_natural_key` / `vendor_id`) |
| `KNOWN_CURRENCY_COL_ALIASES` | Already in pack.yaml (`columnAliases.invoice_currency_code`) |
| `cancelled_*` (date / flag) | Already in pack.yaml (`semanticVariants.cancelled_status`, two candidates) |
| `semantic_segment_map` (3 roles) | **NEW** in this PR — `columnAliases.coa_{balancing,cost_center,natural_account}_segment` |
| `CURRENT_DATE()` anchor | **NEW** in this PR — `{{ snapshot_date }}` renderer token (Step 2) |
| `dim_calendar` parameters | **NEW** in this PR — builtin adapter (Step 3) |
| `ApInvoicesCancelDate` alias variant | **DEFERRED** — fix-commit when live tenant hits it |
| `terms_date_col` / `due_date_col` optionality | **DEFERRED** — requires renderer "optional column" feature |
