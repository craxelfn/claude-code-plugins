---
title: Phase 3 variation-point audit — v1 modules → v2 pack vocabulary
generated_for: v2-phase-3-export-modules-to-sql
date: 2026-06-05
updated: 2026-06-06 (round-1 rework — widen gold projections to v1 parity)
---

# Variation-point catalog

This document audits every tenant-variation knob the five v1 silver/gold
modules currently probe at runtime and maps each one to the v2 content-pack
vocabulary (`columnAliases.<name>` / `semanticVariants.<name>` /
`profile.<key>` / `column.<name>` / `semantic.<name>` renderer tokens).

**Evidence discipline (PLAN §13.3.2)**: Phase 3 catalogs only what v1 modules
already probe against real tenants OR what published Oracle source
documents. No speculative additions.

## pack.yaml declarations as shipped

| v1 probe | pack.yaml binding | Renderer token | Source module |
|---|---|---|---|
| `SEGMENT1` (supplier natural key) | `columnAliases.supplier_natural_key` (candidates: `[SEGMENT1]`) | `{{ column.supplier_natural_key }}` | `dim_supplier.py` (line 93) |
| `VENDORID` | `columnAliases.vendor_id` (candidates: `[VENDORID]`) | `{{ column.vendor_id }}` | `dim_supplier.py` (line 101) |
| `KNOWN_CURRENCY_COL_ALIASES` | `columnAliases.invoice_currency_code` (candidates: `[ApInvoicesInvoiceCurrencyCode, ApInvoicesCurrencyCode]`) | `{{ column.invoice_currency_code }}` | `supplier_spend.py` (lines 77–80), `ap_aging.py` (line 557) |
| `cancelled_*` probe (date / flag) | `semanticVariants.cancelled_status` (candidates: `cancelled_date`, `cancelled_flag`) | `{{ semantic.cancelled_status }}` | `ap_aging.py` (lines 544–553) |

That is the complete set of declared variation points as of Phase 3
round-1. **No `coa_*_segment` columnAliases exist** — see "Round-1 rework
notes" below for the rationale.

## Round-1 rework notes — what changed since the initial PR push

### COA segment handling

The initial Phase 3 commit declared three new `columnAliases`
(`coa_balancing_segment` / `coa_cost_center_segment` /
`coa_natural_account_segment`) so a slimmed-down `dim_account.sql` could
read three role-aliased COA columns through `{{ column.coa_*_segment }}`.

Reviewer feedback (round 1) flagged that the resulting `dim_account`
output schema diverged from v1's default emit (positional `segment_01..30`
plus the `DEFAULT_SEMANTIC_SEGMENT_MAP` six semantic aliases), breaking
the row-equivalence Phase 3's exit criteria require.

Round-1 rework restored v1 parity: `dim_account.sql` now hardcodes the
six-position positional + semantic emit (`segment_01..06` plus
`company` / `cost_center` / `natural_account` / `subaccount` / `product` /
`intercompany`) reading `CodeCombinationSegment1..6` directly from
`bronze.gl_coa`. The three `coa_*_segment` columnAliases were dropped
because `dim_account.sql` no longer references them.

**Future work** (not Phase 3): tenants whose COA puts roles at
non-conventional positions are served today by a pack overlay that ships
a different `dim_account.sql`. A future declarative role-to-position
override mechanism (likely a new renderer token shape like
`{{ coa.<role> }}` plus a structured `chartOfAccounts:` block in the
profile) is the natural successor — but it requires both new renderer
vocabulary and live-tenant evidence of role-positioning variation, neither
of which is in Phase 3 scope.

### AP aging — proxy mode only (intentional v2 narrow)

v1 `transforms/gold/ap_aging.py` defaults `due_date_mode='auto'` and
probes `ApInvoicesTermsDate` / `ApInvoicesDueDate` coverage at runtime
(`detect_ap_aging_params` at lines 504–568). If coverage exceeds
`DEFAULT_REAL_MODE_GATE_THRESHOLD` (10% — line 663), v1 switches to
**real mode** which buckets on `DATEDIFF(<snapshot>, due_date)`, emits
`bucket_basis = 'due_date'`, renames the max column to
`max_days_past_due`, and adds three provenance counts
(`due_date_count`, `terms_date_count`, `net30_fallback_count`). Otherwise
v1 falls back to **proxy mode** (line 487+) using
`DATEDIFF(<snapshot>, invoice_date)`, `bucket_basis = 'invoice_date'`,
`max_days_outstanding`.

The v2 content-pack `ap_aging.sql` ships **proxy mode only** as an
intentional scope decision. Three reasons:

1. **Runtime coverage probing is exactly what ADR-0014 removes.** v2
   replaces "detect column at runtime + branch" with declarative
   variation-point resolution at bootstrap. The auto-detection logic
   in v1 (`detect_ap_aging_params` line 504+) is the canonical example
   of v1 behaviour we're refactoring out, not preserving.
2. **The two modes have different output schemas.** Real mode emits
   `max_days_past_due` + provenance counts; proxy mode emits
   `max_days_outstanding`. The renderer's static-token vocabulary
   cannot conditionally select between two schema shapes from one
   template — that's a different renderer feature (`outputSchema`
   per-mode variants) which is not v0.3.
3. **The on-pack `dashboards/payables.yaml` and `executive_cfo.yaml`
   bind the proxy-mode column shape** (`age_bucket`, `open_amount`).
   Shipping real-mode would force a dashboard rewrite that's
   independent of the migration's stated value.

**Out-of-scope for Phase 3 — tracked for follow-up**: auto/real-mode
AP aging is a future feature that needs (a) a renderer extension for
optional column projection / two-schema variants, (b) declarative
tenant-side coverage threshold configuration, and (c) live evidence
that any saasfademo1-or-comparable tenant has Terms/Due-date coverage
above the threshold (the v1 default 10%).

**Acceptance impact**: the parity harness ships proxy-mode-only fixture
rows so v1 and v2 land in proxy mode under the same conditions. Tenants
whose live AP data would auto-route to real mode under v1 will see
different `ap_aging` output than v2 — that's a documented divergence,
not a bug. See `LIMITS.md` for the resulting Phase 3 limitation entry.

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

## Audit discoveries — DEFERRED (NOT added in Phase 3)

### `ApInvoicesCancelDate` alias variant

`ap_aging.py:546-549` probes a third cancelled-status variant
(`ApInvoicesCancelDate` — the alias variant some Fusion extracts use,
omitting the "led" suffix). The v1 comment marks it as an alias of
`ApInvoicesCancelledDate` with identical semantics. **Deferred** to a
follow-up fix-commit when a live tenant hits the variant under
content-pack execution.

### Optional `terms_date_col` / `due_date_col`

Covered above under "AP aging — proxy mode only". Deferred for the
same reasons as the auto/real-mode split.

## Summary

| v1 probe | Disposition in Phase 3 |
|---|---|
| `SEGMENT1` / `VENDORID` | Declared as `columnAliases.supplier_natural_key` / `vendor_id` |
| `KNOWN_CURRENCY_COL_ALIASES` | Declared as `columnAliases.invoice_currency_code` |
| `cancelled_*` (date / flag) | Declared as `semanticVariants.cancelled_status` (two candidates) |
| `semantic_segment_map` (six COA roles) | Hardcoded in `dim_account.sql` to the v1 `DEFAULT_SEMANTIC_SEGMENT_MAP` — round-1 rework dropped the parameterised approach for v1 parity |
| `CURRENT_DATE()` anchor | **NEW** renderer token `{{ snapshot_date }}` (Step 2) |
| `dim_calendar` parameters | **NEW** builtin adapter (Step 3) |
| `due_date_mode='auto'` runtime probe | **DEFERRED** — v2 ships proxy-mode-only; auto/real awaits a renderer feature + live-tenant evidence |
| `ApInvoicesCancelDate` alias variant | **DEFERRED** — fix-commit when live tenant hits it |
