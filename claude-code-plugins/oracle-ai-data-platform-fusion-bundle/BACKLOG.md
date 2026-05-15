# Backlog — `oracle-ai-data-platform-fusion-bundle`

> **Scope**: every actionable item identified in the 2026-05-05 status audit (see [`STATUS.md`](STATUS.md)). Classified by priority class **(P0 → P3)** and grouped by theme. Pick from the top.
>
> **How to use**: each item is self-contained — title, why, size, dependencies, acceptance criteria. When you start one, mark `[ ]` → `[~]`; when done, `[~]` → `[x]` and add the commit SHA.

## Priority legend

| Class | Meaning | Total |
|---|---|---:|
| **P0** | Pre-flight hygiene — fix things that make the alpha misleading or shipping-blocked | 6 |
| **P1** | Phase 2 dataflow — implement the actual product (transforms / dimensions / gold marts / release) | 20 |
| **P2** | Quality, coverage, polish — testing, bug fixes, docs, versioning | 22 |
| **P3** | Roadmap, upstream advocacy, tracked blockers | 9 |
| **Total** | | **57** |

## Effort legend

| Size | Range | Examples |
|---|---|---|
| **XS** | <1h | one-liner, doc tweak, CHANGELOG date stamp |
| **S** | 1–4h | small feature in single file, focused test |
| **M** | 4–16h | single subsystem, ~1 week-day |
| **L** | 16–40h | multi-file feature, ~1 week |
| **XL** | 40h+ | multi-week, depends on multiple others |

---

# P0 — Pre-flight hygiene (do these first; small, high-leverage)

> Goal: stop the alpha from being misleading. None of these add features; they tell the truth about state.

## Theme: Misleading state

### `[x]` P0.1 — Stamp date on `CHANGELOG.md [0.1.0-alpha]` section
**Why**: Section currently reads `## [0.1.0-alpha] — TBD (Phase 1 gate, week 1)` even though TC1..TC10h-7 are all green. Reads as "incomplete" to a reader who doesn't know the project history.
**Size**: XS
**Depends on**: nothing
**Accept**: header shows actual date (e.g. `## [0.1.0-alpha] — 2026-05-05`); the "Planned" subsection is moved to a `### Achieved` since all bullets there were live-tested.

### `[x]` P0.2 — Remove or fulfill the dangling TODO in `commands/run.py:175`
**Why**: Docstring at `scripts/oracle_ai_data_platform_fusion_bundle/commands/run.py:175` says *"The bundle ships ``notebooks/run_orchestrator.ipynb`` (TODO)"*. That notebook does not exist. New users will look for it.
**Size**: XS (doc fix) or M (ship the notebook — see P1.5)
**Depends on**: nothing for the doc fix; on P1.5 for the real notebook
**Accept**: either (a) docstring rephrased to "Phase 2 will ship a notebook entry point", or (b) `notebooks/run_orchestrator.ipynb` exists and the TODO is removed.

### `[~]` ~~P0.3 — Decide `STATUS.md` + `BACKLOG.md` git fate~~ — **CANCELLED**
**Decision (2026-05-06)**: skipped permanently. STATUS.md / BACKLOG.md / PLAN_*.md stay untracked as ephemeral working notes; do not commit, do not `.gitignore`. Applies for the rest of this project.

## Theme: README / surface accuracy

### `[x]` P0.4 — Add "What's NOT in 0.1.0-alpha" callout to README
**Why**: README's "What you get" section lists 6 capabilities (medallion, GenAI grounding, BI via JDBC, Delta Sharing, etc.) without flagging which are blueprint-only. New users may assume gold marts ship working.
**Size**: XS
**Depends on**: nothing
**Accept**: README has a `> **Phase 1 vs Phase 2**: ...` callout listing what is wired (BICC→bronze, OAC install, MCP config) vs stubbed (silver/gold transforms, conformed dimensions, gold marts).

### `[x]` P0.5 — Annotate "Use cases" in README with phase tags
**Why**: README lists 12 use cases. Only ~3 are actually achievable in 0.1.0-alpha (BICC bronze landing, OAC connection install, GenAI grounding on demo gold). The rest depend on Phase 2.
**Size**: XS
**Depends on**: P0.4 (use the same Phase 1 vs Phase 2 framing)
**Accept**: each use case in §"Use cases" tagged `(0.1.0a ✅)` or `(Phase 2 🚧)`.

### `[~]` ~~P0.6 — Mention `STATUS.md` + `BACKLOG.md` in README~~ — **CANCELLED**
**Decision (2026-05-06)**: skipped — depends on P0.3, which was cancelled. Those files stay untracked working notes, so the README intentionally does not reference them.

---

# P1 — Phase 2 dataflow (the actual v0.2.0 product)

> Goal: turn `0.1.0-alpha` into `0.2.0`. The three stub modules (`orchestrator/`, `transforms/`, `dimensions/`) become real, the 5 gold marts become wired, and a `.bar` ships as a release artifact. Suggested execution order is reflected in the IDs below; respect dependencies.

## Theme: Foundation (one-shot wiring; everything else depends on this pattern)

### `[x]` P1.1 — Implement `dimensions/dim_supplier.py` (commit `2d44b1d`, live `91ddcbc`+`bee18aa`)
**Why**: Smallest dimension; already prototyped in TC8 (live-validated $3.2B aggregate). Establishes the pattern for the other 4 dims.
**Size**: S
**Depends on**: nothing
**Accept**:
- `dimensions/dim_supplier.py` reads `bronze.erp_suppliers`, dedupes on `supplier_number`, handles null IDs (demo pod), writes `silver.dim_supplier`.
- Unit test in `tests/unit/test_dim_supplier.py` covers dedup, null-handling, schema.
- One live test row added to TC8 results (or new TC8b file) verifying production-shape vs demo-shape.

### `[x]` P1.2 — Productize `transforms/gold/supplier_spend.py` (commit `61d1348`, live `618c0c2`)
**Why**: TC8 already proved the SQL on demo pod ($3.2B / 236 records / top vendor `300000047507499` at $892.7M). Wrap it as a transform module — model for the next 4 marts.
**Size**: S
**Depends on**: P1.1
**Accept**:
- `transforms/gold/supplier_spend.py` exposes `build(spark, fusion_catalog) → DataFrame`, writes `gold.supplier_spend`.
- Demo-pod / production switch: if `dim_supplier` has populated IDs, join form; else spend-only fallback (resolves bug A4 from STATUS.md §5).
- Unit test on synthetic data.
- Live test re-runs TC8 against `silver.dim_supplier` instead of inline aggregation.

### `[x]` P1.3 — Implement `dimensions/dim_account.py` (commit `d743979`, live `7d765f4`)
**Why**: Required by `gl_balance` mart (P1.7). Read from `bronze.gl_coa` (`CodeCombinationExtractPVO`).
**Size**: S
**Depends on**: nothing
**Accept**:
- Reads `bronze.gl_coa`, surrogate `account_id`, natural `code_combination`, hierarchy attributes.
- Unit test covers empty-coa edge case, parent-child segment handling.
- Hook for custom COA segments (deferred to P2.E1's `docs/customizing.md`).

### `[x]` P1.4 — Implement `dimensions/dim_calendar.py` (commit `9003e00`, live `022245c`)
**Why**: Required by `gl_balance` and `po_backlog`. System-generated (no source PVO).
**Size**: S
**Depends on**: nothing
**Accept**:
- Generates Gregorian + Fiscal calendars for 2020–2030 (configurable range).
- Surrogate `calendar_key`, `fiscal_year`, `fiscal_period`, `calendar_date`.
- Unit test verifies coverage + no gaps.

### `[ ]` P1.5 — Implement `orchestrator/__init__.py` + `notebooks/run_orchestrator.ipynb`
**Why**: Public entry point that wires extract → bronze → silver → gold sequence and persists state to `fusion_bundle_state` Delta table. Resolves P0.2 fully.
**Size**: M
**Depends on**: P1.1, P1.2, P1.3, P1.4 (need at least one full extract → silver → gold path working first to validate the orchestrator API shape)
**Accept**:
- `orchestrator.run(bundle_path: str, mode: Literal["full","incremental","seed"], datasets: list[str] | None) → RunSummary`.
- Handles incremental watermarking (read prior `_watermark_used` from `fusion_bundle_state`).
- Notebook at `notebooks/run_orchestrator.ipynb` demonstrating inline use.
- `cli.py` `run` command can dispatch via REST OR via inline notebook invocation when `--inline`.
- Removes the TODO from `commands/run.py:175` (closes P0.2).
- Unit tests for state machine + watermark logic.

## Theme: Remaining dimensions

### `[ ]` P1.6 — Implement `dimensions/dim_item.py`
**Why**: Required by future cross-module marts (PO × Items). Source: `bronze.scm_items` (`ItemExtractPVO`).
**Size**: S
**Depends on**: nothing
**Accept**: writes `silver.dim_item`; unit-tested.
**Zero-diff landing contract** (post-P1.5α): `dim_item` is registered in `orchestrator/registry.py` `KNOWN_DEFERRED_DIMS` with this ticket ID. When the module ships, the **only** orchestrator-side edit is moving `"dim_item"` from `KNOWN_DEFERRED_DIMS` into `SILVER_DIMS` with its builder + `depends_on_bronze`. No `schema/bundle.py` default edit, no `examples/*.yaml` edit, no customer-YAML migration. Any deviation from this is a P1.5α regression and blocks merge. The acceptance criterion above must include: "P1.5α deferred test for `dim_item` flips from `deferred` to `success` with no other diff" (one-line test update only).

### `[ ]` P1.7 — Implement `dimensions/dim_org.py` (pending PVO)
**Why**: Cross-module dim; needed for HCM × Finance joins.
**Size**: S (after PVO confirmed); blocked indefinitely without
**Depends on**: customer pod access OR confirmed PVO name from BICC catalog (`catalog probe`); P3.8 unblocks.
**Accept**: PVO name added to `schema/fusion_catalog.py` with ✅; `dim_org.py` writes `silver.dim_org`; unit-tested.
**⚠ Blocker**: PVO name not yet identified. Treat as deferred until P3.8 (customer HCM pod) becomes available.
**Zero-diff landing contract** (post-P1.5α): `dim_org` is **in** the `DimensionsSpec.build` default (`schema/bundle.py:110`), **in** `examples/full_finance.yaml`, and is registered in `orchestrator/registry.py` `KNOWN_DEFERRED_DIMS` with this ticket ID. Today every seed run emits `RunStep(name="dim_org", status="deferred", error_message="P1.7 — …")`. When P1.7 ships, the **only** orchestrator-side edit is moving `"dim_org"` from `KNOWN_DEFERRED_DIMS` into `SILVER_DIMS` with its builder + `depends_on_bronze=("erp_org_hierarchy",)` (or whichever bronze the confirmed PVO lands as). No `schema/bundle.py` default edit, no `examples/full_finance.yaml` edit, no customer-YAML migration — customer bundles that already list `dim_org` just start producing rows. Any deviation from this is a P1.5α regression and blocks merge. The acceptance criterion above must include: "P1.5α deferred test for `dim_org` flips from `deferred` to `success` with no other diff" (one-line test update only).

## Theme: Remaining gold marts (each ~200 LOC; replicate P1.2 pattern)

### `[~]` P1.8 — `transforms/gold/gl_balance.py` (commit pending; live `TC23_gl_balance_results.md`)
**Why**: Period balances by account × period — core CFO dashboard mart.
**Size**: S → **delivered S+** (added BOOTSTRAP Step 7 + COALESCE fix from live finding)
**Depends on**: P1.3 (`dim_account`) ✅; P1.4 (`dim_calendar`) ✅ — but **dim_calendar dep was nominal**, not used in the SQL (grain mismatch: daily dim vs period fact; period context comes from fact's `period_year`/`period_num` directly). See [`PLAN_P1.8_gl_balance.md`](PLAN_P1.8_gl_balance.md) §2.5 for the deviation rationale.
**Accept**:
- ✅ `transforms/gold/gl_balance.py` follows `supplier_spend.py` pattern (constants → SQL builder → Spark wrapper)
- ✅ Writes `fusion_catalog.gold.gl_balance` Delta — 10,184,102 rows / 22 cols landed live (`actual_flag='A'` only; encumbrance + budget deferred to v0.3)
- ✅ Single LEFT JOIN to `silver.dim_account`; **no `dim_calendar` join** (grain mismatch)
- ✅ NULL-propagation regression caught + fixed: `closing_balance` formula uses `COALESCE(..., 0)` per cast (live `null_closing_balance` = 0)
- ✅ 21 new unit tests; suite 207 → **228** all pass; ruff clean
- ✅ Live evidence: [`tests/live/TC23_gl_balance_results.md`](tests/live/TC23_gl_balance_results.md)
- ✅ BOOTSTRAP extended with **Step 7** (`BalanceExtractPVO` → `bronze.gl_period_balances`) + Step 8 column-shape probe

### `[~]` P1.9 — `transforms/gold/ap_aging.py` (shipped 2026-05-10, TC24 live)
**Why**: Payable age bands (current / 1–30 / 31–60 / 61–90 / 91+). Drives AP aging dashboard.
**Size**: M (plugin-portable schema variants + due-date-mode gate + currency-in-grain)
**Depends on**: bronze.ap_invoices ✅, silver.dim_supplier ✅ (lean path; no ap_payments / ap_aging_periods needed)
**Accept**: ✅ writes `gold.ap_aging` (real mode) or `gold.ap_outstanding_by_invoice_age` (proxy mode) on `fusion_bundle_dev`; 40 unit tests covering both modes + schema variants + decision gate; TC24 live evidence shows per-currency reconciliation `delta = 0.00` across 12 currencies, 100% terms_date provenance, $-126K credits preserved across 5 currencies.
**Shipped**: `transforms/gold/ap_aging.py` (plugin-portable; `due_date_mode='auto'` default + 80% coverage gate; `<> 0` filter invariant; mode-aware `max_days_*` column name). Live evidence: `tests/live/TC24_ap_aging_results.md`.

### `[ ]` P1.10 — `transforms/gold/ar_aging.py`
**Why**: Customer aging — collections KPI.
**Size**: M
**Depends on**: bronze.ar_invoices ✅, bronze.ar_receipts ✅
**Accept**: writes `gold.ar_aging`; unit-tested; sample SQL committed.
**Zero-diff landing contract** (post-P1.5α): `ar_aging` is **in** the `GoldSpec.marts` default (`schema/bundle.py:116`), **in** `examples/full_finance.yaml`, and is registered in `orchestrator/registry.py` `KNOWN_DEFERRED_MARTS` with this ticket ID. Today every seed run emits `RunStep(name="ar_aging", status="deferred", error_message="P1.10 — …")`. When P1.10 ships, the **only** orchestrator-side edit is moving `"ar_aging"` from `KNOWN_DEFERRED_MARTS` into `GOLD_MARTS` with its builder + `depends_on_bronze=("ar_invoices", "ar_receipts")` + `depends_on_silver=("dim_supplier", "dim_calendar")` (mirror the `ap_aging` registry entry as the template). No `schema/bundle.py` default edit, no `examples/full_finance.yaml` edit, no customer-YAML migration. Any deviation from this is a P1.5α regression and blocks merge. The acceptance criterion above must include: "P1.5α deferred test for `ar_aging` flips from `deferred` to `success` with no other diff" (one-line test update only). The schema-default ↔ registry invariant lint (`tests/unit/test_registry_default_coverage.py`, shipped in P1.5α) enforces this contract automatically — moving the key from `KNOWN_DEFERRED_MARTS` to `GOLD_MARTS` keeps the lint green.

### `[ ]` P1.11 — `transforms/gold/po_backlog.py`
**Why**: Open POs by supplier × due date — procurement KPI.
**Size**: M
**Depends on**: P1.1 (`dim_supplier`), P1.4 (`dim_calendar`); bronze.po_orders ✅, bronze.po_receipts ✅
**Accept**: writes `gold.po_backlog`; unit-tested; sample SQL committed.
**Zero-diff landing contract** (post-P1.5α): `po_backlog` is **in** the `GoldSpec.marts` default (`schema/bundle.py:116`), **in** `examples/full_finance.yaml`, and is registered in `orchestrator/registry.py` `KNOWN_DEFERRED_MARTS` with this ticket ID. Today every seed run emits `RunStep(name="po_backlog", status="deferred", error_message="P1.11 — …")`. When P1.11 ships, the **only** orchestrator-side edit is moving `"po_backlog"` from `KNOWN_DEFERRED_MARTS` into `GOLD_MARTS` with its builder + `depends_on_bronze=("po_orders", "po_receipts")` + `depends_on_silver=("dim_supplier", "dim_calendar")`. No `schema/bundle.py` default edit, no `examples/full_finance.yaml` edit, no customer-YAML migration. Any deviation from this is a P1.5α regression and blocks merge. The acceptance criterion above must include: "P1.5α deferred test for `po_backlog` flips from `deferred` to `success` with no other diff" (one-line test update only). The schema-default ↔ registry invariant lint (`tests/unit/test_registry_default_coverage.py`, shipped in P1.5α) enforces this contract automatically.

## Theme: Plugin-portability follow-ups (round-6 audit)

### `[~]` P1.11a — `dim_account` segment portability (shipped 2026-05-11)
**Why**: `dim_account` hardcoded **six** COA segments with semantic names; tenants with >6 populated segments lost data, tenants with different segment ordering got wrong labels.
**Done**: `dim_account` now emits all 30 positional `segment_01..segment_30` columns by default (configurable via `n_segments`), `code_combination` is built via `CONCAT_WS` over all configured segments (`CONCAT_WS` skips NULLs so sparse tenants produce clean keys), and semantic aliases are tenant-configurable via `semantic_segment_map: Mapping[int, str]` with the Fusion-conventional six as the default (preserves `gl_balance`'s consumer interface — `company`, `cost_center`, etc. all still emitted on the demo pod). Adds `detect_active_segments(spark)` probe helper for orchestrators that want to size `n_segments` per tenant. Validation rejects out-of-range positions, invalid SQL identifiers, and duplicate aliases. 12 new unit tests (test_dim_account 20 → 32).
**Note**: `gl_balance` was subsequently updated (commit `50d450a`) to read positional `da.segment_NN` columns through its own `coa_segment_map` knob, so tenants with non-conventional COA designs work end-to-end without needing to author a mart variant. Old consumer-facing column names (`company`, `cost_center`, `natural_account`, etc.) are preserved by the default map.

### `[ ]` P1.5a — Orchestrator portability surface (per-tenant config plumbing)
**Why**: The mart modules now expose plenty of portability knobs (`dim_account.n_segments` / `semantic_segment_map`, `gl_balance.coa_segment_map` / `actual_flag_filter`, `ap_aging.due_date_mode` / `real_mode_gate_threshold` / `null_invoice_date_policy` / `semantic-cancelled-variant`, `supplier_spend.currency_col`). Each currently has a sensible default, but a multi-tenant production deployment needs the orchestrator (P1.5) to wire these through a per-tenant config (YAML / Vault / Terraform-controlled) so customers don't have to edit Python to onboard their pod.
**Size**: M — depends on P1.5 landing first. Add a tenant-config schema (Pydantic), a config loader, and pass-through wiring through the orchestrator's mart-build entry points.
**Performance hints (orchestrator-side, captured for the design)**:
* `ap_aging.build()` runs a coverage probe before each build (one extra filtered scan of `bronze.ap_invoices`). Correct for portability, but the orchestrator should **cache probe results per run** so multiple marts on the same bronze don't repeat schema/coverage scans.
* `dim_account` defaults to 30 segments — portable but wider than most tenants need. The orchestrator can call `detect_active_segments(spark)` once per refresh and pass `n_segments` to size the dim per-tenant.
* `gl_balance` does a large fact `LEFT JOIN` to a small dim — broadcast-friendly. Spark AQE handles this automatically; **do not add a broadcast hint blindly**. Only add hints after live measurement on a tenant whose shuffle cost is documented.
**Accept**: per-tenant config flows from a single YAML to all four mart modules; orchestrator caches probe results within a refresh; coverage in live evidence on at least one non-saasfademo1 tenant (or a synthesized schema-variant test pod).

### `[~]` P1.5b — Catalog/schema name plumbing (shipped 2026-05-11)
**Why**: `bundle.yaml` declared `aidp.{catalog,bronzeSchema,silverSchema,goldSchema}` and the Pydantic schema accepted them — but no module read them at build time. Every dim/gold module hardcoded `fusion_catalog.X.Y` as `Final[str]` defaults. `commands/run.py:78-79` had the same bug in `status()` (hardcoded `'bronze'` schema for `fusion_bundle_state`).
**Done**: New `scripts/.../config/paths.py` with the `TablePaths` frozen dataclass + `DEFAULT_PATHS` singleton + `from_bundle()` classmethod. Strict SQL-identifier validation (`^[A-Za-z_][A-Za-z0-9_]*$`) at construction — rejects injection, non-strings, leading-digit identifiers, hyphens, dots. Every shipped module (`dim_supplier`, `dim_account`, `dim_calendar`, `supplier_spend`, `gl_balance`, `ap_aging`) accepts `paths: TablePaths | None` on its `build()`; module-level constants derive from `DEFAULT_PATHS` so value strings stay byte-identical (every existing test passes unchanged). Explicit per-table kwargs still win over `paths`. `commands/run.py status()` now uses `TablePaths.from_bundle(bundle).bronze("fusion_bundle_state")`. `ap_aging.build()` resolves `gold_table` AFTER the auto-router resolves `due_date_mode` (critical ordering — F + G build()-level fake-Spark tests lock this invariant). 38 new tests (23 in `test_paths.py` + 14 mart/dim threading tests + 1 status test).
**Source rules**: CLAUDE.md §"What varies per tenant: Tenant-declared policy → bundle.yaml". CONTRIBUTING.md §"Module checklist" + §"Wiring".

### `[~]` P1.5α-fix1 — PLAN §4.4 review corrections (blocker on α implementation)
**Why**: Read-through of `PLAN_P1.5_orchestrator.md` §4.4 (the `_execute_node` + run-loop pseudocode) surfaced two correctness bugs in the as-drafted code. Both must be reflected in the plan BEFORE α implementation starts — they're not "fix in α" issues, they're "α as drafted is wrong" issues. Filing as a single trackable item so the corrections don't get lost between drafting and committing.

**Bug 1 — BICC double-pull on bronze count** (PLAN line 525). `[FIXED in plan 2026-05-15]`
- **Problem**: bronze branch did `df.write...saveAsTable(target); return RunStep.success(..., row_count=df.count())`. `df` is the lazy `extract_pvo()` (`reader.load()`) wrapped with audit columns — calling `.count()` after the write actions the plan a SECOND time, triggering a second BICC HTTP fetch against Fusion. BICC extracts are not idempotent (each call opens a new `_extract_ts` window), so the count could differ from what was just written, and every bronze extract doubles Fusion load on the customer's tenant.
- **Fix**: count from the materialized Delta target — `row_count=spark.table(target).count()`. Applied to PLAN §4.4 lines 525-537. Acceptance-criteria checklist updated with the unit-test contract: fake-Spark stub records every method call on the `extract_pvo` return; assert exactly one action terminator (`saveAsTable`) and zero `.count()` / `.collect()` / `.show()` calls. Silver/gold branches exempt: module contract is that `build()` writes the target inside the call and returns `spark.table(<resolved>)`, so `.count()` is a cheap Delta read.

**Bug 2 — Failure cascade never runs** (PLAN line 477). `[ANALYZED, decision pending]`
- **Problem**: The success-path branch checks `if step.status == "failed" and node.is_required_upstream(): _skip_dependents(...)`, but `_execute_node` only ever returns `RunStep.success(...)` or **raises** — there's no return path that produces `status="failed"`. So that branch is dead code. The exception-path branch catches, writes a failed step, then `break`s — **without calling `_skip_dependents`**. Net: failed upstreams produce 1 `failed` row + 0 `skipped` rows, contradicting §4.7 and the acceptance criterion that mandates downstream `status="skipped"` cascade rows.
- **Decision pending**: three options on the table.
  - **Option A** — `_execute_node` catches everything, returns `RunStep.failed(...)` on any exception. Single loop branch; risk of over-catching (state-write/cascade-helper bugs masked as "module failures").
  - **Option B** — Keep `_execute_node` raising; add `_skip_dependents(...)` call inside the orchestrator's `except` block. Smallest change; requires editing §4.7 prose ("`_execute_node` caught" → "orchestrator caught"); two-path structure persists.
  - **Option C** — Hybrid: `_execute_node` wraps **only** the module dispatch in try/except (returns `RunStep.failed` on module errors), state-write / cascade-helper exceptions propagate as orchestrator bugs. Single loop branch + preserves "module failure vs orchestrator bug" distinction. Recommended.
  - Tradeoffs and reasoning in conversation log; not duplicating here to avoid drift.
- **Fix path**: pick option (default **C**), patch PLAN §4.4 pseudocode, add the cascade unit test to acceptance criteria:
  > Cascade test: stub `dim_supplier.build` to raise; submit a plan with `supplier_spend` depending on `dim_supplier`. Assert RunSummary contains 1 `failed` step (with traceback in `error_message`) + 1 `skipped` step for `supplier_spend` (with `error_message` referencing `dim_supplier`); both rows written to state-table (two `write_state_row` calls); loop terminates after cascade — no later nodes attempted.

**Size**: S — plan edits only, no code. ~30 min for Option C application + checklist update.
**Depends on**: nothing. Must land before any α implementation commit.
**Accept**:
- Bug 1: PLAN §4.4 + acceptance criteria reflect target-table counting. **(Done 2026-05-15.)**
- Bug 2: PLAN §4.4 pseudocode rewritten per chosen option; §4.7 prose aligned; acceptance criteria gains the cascade test.
- Both bugs traceable from PLAN_P1.5 back to this BACKLOG entry for audit.

### `[ ]` P1.5δ — Claude-Code-driven MCP dispatch slash command — **reassess after P1.5ε**
**Status note (2026-05-15)**: Original justification was that surface #3 (laptop terminal → REST) was blocked upstream, leaving MCP as the only way for Claude Code users to dispatch. That premise broke when the `aiwap` REST API shipped 2026-04-30 (see P1.5ε). Once P1.5ε lands and TC28 confirms OCI signing works, Claude Code users can just shell out to `aidp-fusion-bundle run --mode seed` — no slash command, no MCP, no second dispatch path to maintain. **Decision deferred**: keep this entry alive but do not start work. After P1.5ε ships, choose one of: (a) **cancel** P1.5δ if REST works cleanly for Claude Code users with `~/.oci/config` set up; (b) **keep** P1.5δ if REST's auth-setup friction or batch-only semantics (no live kernel for interactive bundle debugging) make it the wrong fit for Claude-Code-driven exploration. Default expectation today: lean toward cancellation — REST is the cleaner primitive and one dispatch path beats two.

**Why (original)**: P1.5α ships `--inline` as the architectural primary — works from inside an AIDP notebook session. But the CLAUDE.md "CLI is the contract" goal includes a second customer journey: **customer with Claude Code installed on their laptop** wants to type `/aidp-fusion-bundle run --mode seed` and have the bundle materialize without opening a browser or AIDP notebook by hand. The MCP-based dispatch primitive exists today — `oracle-ai-data-platform-workbench-spark-connectors/tools/live_test_driver.py` documents the canonical flow: `mcp__aidp__nb_save_file` → `mcp__aidp__nb_create_session` → `mcp__aidp__nb_execute_code` against a chosen cluster, with stdout captured between `AIDP_LIVE_TEST_RESULT_BEGIN/END` markers. This is **us-implementable** (no upstream gap); we just need to wrap the pattern as a slash command + companion skill on the fusion-bundle's existing Claude Code plugin surface (`.claude-plugin/plugin.json` already exists; `skills/aidp-fusion-bundle/` is the namespace).

Intentionally separated from P1.5α: TC27 (live MCP-dispatch evidence) needs a working Claude Code MCP session against `fusion_bundle_dev`; if that integration surfaces issues, P1.5α's `--inline` correctness (TC26) shouldn't get held hostage. Ship the foundation, then build the convenience layer on top.

**Size**: M — slash command file (`.claude-plugin/commands/run.md`) + companion skill (`skills/aidp-fusion-bundle/SKILL.md` extended with the dispatch flow) + a small `AIDP_LIVE_TEST_RESULT_BEGIN/END` marker emitter added to `_render_summary` so the captured stdout has parseable RunSummary JSON. ~3-4h plus live verification.
**Depends on**: P1.5α shipped (slash command uploads `notebooks/run_orchestrator.ipynb`, which P1.5α produces). Modeled directly on `oracle-ai-data-platform-workbench-spark-connectors/tools/live_test_driver.py` — same pattern, production use instead of test-harness use.
**Accept**:
- `.claude-plugin/commands/run.md` slash command: takes `--mode`, `--datasets`, `--cluster` (default `fusion_bundle_dev`); orchestrates the MCP flow.
- Companion skill: documents the per-step MCP calls so the skill is runnable end-to-end as a Claude Code agent flow (upload `notebooks/run_orchestrator.ipynb` + `bundle.yaml` → create session → execute cells → parse markers → render the RunSummary inline).
- `_render_summary` emits the parseable JSON envelope between `AIDP_LIVE_TEST_RESULT_BEGIN` / `_END` markers (one extra `console.print(...)` in P1.5δ scope, ~10 LOC).
- Live evidence: **TC27** captures one full dispatch on `fusion_bundle_dev` — slash command runs, MCP tools dispatch to AIDP, RunSummary JSON parsed, all 11 bronze + 3+2 silver + 3+2 gold rows verified in `fusion_bundle_state` post-run.
- Failure-mode tests: MCP session unavailable → clear error; cluster name invalid → clear error; notebook execution timeout → clear error with timeout configuration hint.

### `[ ]` P1.5ε — Laptop-terminal REST dispatch (formerly P3.13 advocacy; REST API shipped 2026-04-30)
**Why**: Surface 3 of the three execution surfaces for `aidp-fusion-bundle run` — a bare laptop terminal, no Claude Code, no notebook session (CI / cron / scripts) — was thought to be blocked upstream. As of the 2026-04-30 `aiwap` REST release (https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/rest-endpoints.html, OpenAPI at `aiwap/swagger.json`), it's implementable. Public model is the **Workflow `jobs`/`jobRuns` job-submission pattern**, not a kernel-execute channel (the `sessions` endpoints carry metadata only — no public `/execute`). The three customer journeys for `aidp-fusion-bundle run` become:
1. ✅ From inside an AIDP notebook session: `--inline` works (P1.5α).
2. ✅ From Claude Code on a laptop: MCP-based dispatch (P1.5δ).
3. 🟡 From a bare laptop terminal: REST dispatch (this item).

**Why P1.5ε, not P1.5α**: P1.5α (`--inline`) is the architectural primary because the orchestrator needs Spark + checkpointer + `aidputils.secrets` + Delta catalog — all notebook-runtime objects. REST dispatch is a wrapper that uploads `notebooks/run_orchestrator.ipynb` to AIDP and submits it as a job; it depends on the notebook existing and being final, which is a P1.5α deliverable. Ship α first, ε after.

**Schema facts** (captured from `aiwap/swagger.json` so the implementer doesn't re-derive):
- **Path prefix**: `/20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces/{workspaceKey}/...`
- **Flow**: `POST .../notebook/api/contents/{path}` (upload `.ipynb`) → `POST .../jobs` (create job; one `tasks[]` entry of `type: NOTEBOOK_TASK`) → `POST .../jobRuns` (submit; `{jobKey, parameters[], queue}`) → poll `GET .../jobRuns/{key}` for `state.status` ∈ `{PENDING, QUEUED, RUNNING, SUCCESS, FAILED, CANCELED, TIMED_OUT}` → `POST .../taskRuns/{taskRunKey}/actions/fetchOutput {outputKey}` for the RunSummary.
- **`NotebookTask`**: `notebookPath: string` (required), `cluster: JobCluster` (required), `source: WORKSPACE | GIT_PROVIDER` (default `WORKSPACE`), `parameters: array<{name, value}>` (**not a map** — both fields string-typed), `timeoutSeconds`, `isStreaming`. **`SPARK_SUBMIT_TASK` is in the `Task.type` enum but has no schema definition — treat as reserved.**
- **`JobCluster`**: `clusterKey` (task-local nickname, **not a global cluster OCID**) + `newCluster: NewClusterConfiguration`. Existing-cluster reuse happens at the **job** level via `jobClusters[]` (referenced by `clusterKey`); there is no `existingClusterId` field on the task.
- **Output**: `fetchOutput` returns `data[]` typed `NOTEBOOK | TEXT_PLAIN | APPLICATION_JSON | NOTEBOOK_PATH | FILE_PATH | …`, plus `errorTrace`, `isTruncated`, `outputParameters[]`. `oidlUtils.notebook.exit(json.dumps(summary.to_dict()))` at the end of `run_orchestrator.ipynb` is the correct surfacing primitive — NOT the `AIDP_LIVE_TEST_RESULT_BEGIN/END` stdout markers P1.5δ uses (those are an MCP-channel artifact). Plan a small notebook tweak (one cell, ~3 LOC) to call `notebook.exit(...)` so the RunSummary comes back as a typed `APPLICATION_JSON` output rather than scraped from stdout.
- **Auth**: *inferred OCI request signing, not literally confirmed in the `aiwap` doc tree.* `swagger.json` has `securityDefinitions: {}` and no `aiwap` page mentions auth at all. Strong indirect signals — `oci.ai_data_platform.AiDataPlatformClient` exists in the OCI Python SDK (control-plane only — data-plane endpoints under `/workspaces/{wk}/...` are **not yet wrapped**), the OCID-keyed path shape, and the absence of any other auth scheme in the spec — all point to OCI request signing (RSA-SHA256 over canonical header set). **Empirical signed-curl probe against a real tenant is the load-bearing prerequisite for this item** — do it before anything else.
- **`datalake-tenant-id` header**: required only on `/notebook/api/sessions` and `/notebook/api/contents/{contentPath}` (the Jupyter passthrough); **not on `/jobs`, `/jobRuns`, or `fetchOutput`**. Origin of the value is undocumented; if the upload step needs it, probe.

**Implementation sketch**:
- Build an `aidp_rest` client module: `requests` with `auth=oci.signer.Signer(...)`, or alternatively shell out to `oci raw-request` (CLI does the signing). Resource-principal / instance-principal signers when running in-cloud.
- New file: `scripts/.../dispatch/aidp_rest.py` — `upload_notebook(path) → workspace_path`, `create_job(notebook_path, cluster_ref) → job_key`, `submit_run(job_key, parameters) → run_key`, `poll_run(run_key) → terminal_status`, `fetch_output(task_run_key) → RunSummary`.
- `commands/run.py:_run_via_aidp_dispatch()` becomes a real implementation that threads `bundle_path` + cluster reference from `aidp-deploy.config.json`.
- Add `notebook.exit(json.dumps(summary.to_dict()))` cell to `notebooks/run_orchestrator.ipynb` (1 LOC + 1 import).

**Size**: M (~1-2 days). Lion's share is the auth empirical work + the `aidp_rest` client wrapper; the orchestrator is unaffected.
**Depends on**: P1.5α shipped (notebook + orchestrator exist); empirical confirmation of OCI signing against a real tenant (one signed `curl` against `GET /workspaces/{wk}` or similar low-stakes endpoint).
**Accept**:
- Empirical evidence file `tests/live/TC28_rest_auth_probe.md` showing a signed request to AIDP returning 200 (not 401/403).
- `aidp-fusion-bundle run --mode seed` (no `--inline`) against `fusion_bundle_dev` from a laptop terminal returns exit code 0 and prints the RunSummary. Live evidence at `tests/live/TC29_rest_dispatch.md`.
- Unit tests cover the four `aidp_rest` primitives with `responses`-mocked HTTP.
- `_run_via_aidp_dispatch()` error message removed (function does real work now).

**File upstream issue if blocked**: if OCI signing turns out NOT to be the right scheme, OR if `datalake-tenant-id` is required on `/notebook/api/contents` and the origin is non-discoverable, file an issue with the AIDP team to get the auth-and-headers spec published in the `aiwap` doc tree (current gap: `swagger.json` has empty `securityDefinitions`).

### `[ ]` P1.Xb — Schema preflight before `CREATE OR REPLACE TABLE`
**Why**: Today each mart module validates its own kwargs and (in ap_aging's case) hard-gates on the currency column. But required bronze / silver column existence isn't checked uniformly — a missing column failures inside Spark with a cryptic `UNRESOLVED_COLUMN` analysis error. A unified preflight that runs before `spark.sql(CREATE OR REPLACE)` gives customers a clear, actionable error.
**Size**: S — one helper (`preflight_required_columns(spark, table, required_cols) → None | raise`), invoked from each mart's `build()` after kwarg validation and before SQL execution. Per-mart required-column lists tied to the post-detect kwargs (e.g. `ap_aging` requires `ApInvoicesVendorId`, `ApInvoicesInvoiceDate`, `ApInvoicesInvoiceAmount`, `ApInvoicesAmountPaid`, the detected currency col, and the detected/configured cancelled + terms-date cols).
**Accept**: every shipped mart's `build()` raises a `MartPreflightError` (or similar) listing the missing column(s) by name when bronze/silver schema doesn't match expectations; unit-tested via the same fake-Spark stub pattern used for `detect_*_params` tests; ap_aging's existing currency-presence hard-gate is folded into this preflight so the contract is uniform.

## Theme: Medallion performance & incrementality (round-6 perf audit, 2026-05-11)

### `[ ]` P1.17 — Switch dims + gold marts from `CREATE OR REPLACE` to `MERGE INTO` with watermark gate
**Why**: Every silver/gold module emits `CREATE OR REPLACE TABLE … USING DELTA AS SELECT …` (`dim_account.py:223`, `dim_supplier.py:64`, `transforms/gold/supplier_spend.py:100`, `transforms/gold/gl_balance.py:248`, `transforms/gold/ap_aging.py:428`). That's a full table rewrite every refresh — the **medallion-architecture concept break**: bronze is supposed to grow incrementally, silver/gold MERGE on changed slices, but today a daily refresh of `gold.gl_balance` rewrites all 11M rows. On a tenant with 5 years of GL history (~50M rows projected), daily incremental refresh costs the same as the seed load. Same problem applies to `supplier_spend` and `ap_aging`. Cascades into three already-noted side-effects: `monotonically_increasing_id()` surrogate keys are unstable (P1.19); window-function dedupe sorts the full bronze every rebuild (`dim_account.py:243-252`, `dim_supplier.py:87-94`); `ap_aging` double-scans `bronze.ap_invoices` (P2.20). Fix the root, the rest fall out.
**Size**: L — six modules + watermark-write contract + live re-verification of TC22 / TC23 / TC24 incremental shape.
**Depends on**: P1.5 (orchestrator) — MERGE needs the orchestrator to advance the watermark in `fusion_bundle_state` after each successful build. Building MERGE logic on top of a not-yet-wired dispatch path is wasted work.
**Accept**:
- Each `build()` accepts `refresh_mode: Literal["seed", "incremental"]`. `"seed"` keeps the existing `CREATE OR REPLACE` shape (first run, full backfill). `"incremental"` emits `MERGE INTO target USING (… filtered by _extract_ts > last_watermark …) ON target.<natural_key> = src.<natural_key> WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`.
- Watermark is read from + written to `fusion_bundle_state` by the orchestrator only — mart modules stay stateless.
- `dim_calendar` is exempt — fully deterministic, no source watermark; stays on `CREATE OR REPLACE`.
- Live evidence: TC22b / TC23b / TC24b — same tenant, two consecutive runs with synthetic mid-extract delta; assert second run touches only delta rows (Delta-table version diff or `OPTIMIZE`-side stats).

### `[ ]` P1.18 — Partition + Z-ORDER bronze + silver + gold tables
**Why**: None of the `CREATE OR REPLACE TABLE … USING DELTA` statements declare `PARTITIONED BY` or run `OPTIMIZE … ZORDER BY`. OAC dashboards filtering `gold.gl_balance` by `period_year` or `currency_code` do full-table scans every query — on 11M rows + future history that's a 1s tile vs a 30s tile. Bronze `gl_period_balances` (11M rows today on `fusion_bundle_dev`) isn't partitioned either, so even gold-side `WHERE BalanceActualFlag = 'A'` filters scan every file. Delta data-skipping helps but only on the first ~32 columns; explicit Z-ORDER on dashboard-filter columns is order-of-magnitude better.
**Size**: M — pure DDL changes to the `CREATE TABLE` SQL each module emits + optional post-MERGE `OPTIMIZE ZORDER BY` runs. No logic changes.
**Depends on**: nothing — independent of P1.17 (partitioning works under both `CREATE OR REPLACE` and `MERGE`). Ships now as a quick win.
**Accept**:
- `bronze.gl_period_balances`: `PARTITIONED BY (BalancePeriodYear)`.
- `bronze.ap_invoices`: `PARTITIONED BY (_extract_date)` (computed audit column; supports incremental MERGE in P1.17).
- `gold.gl_balance`: `PARTITIONED BY (period_year)` + `OPTIMIZE … ZORDER BY (currency_code, ledger_id, account_id)`.
- `gold.ap_aging` / `gold.ap_outstanding_by_invoice_age` / `gold.supplier_spend`: no partition (small relative to balance fact) but `OPTIMIZE … ZORDER BY (currency_code, vendor_id)`.
- `dim_account`, `dim_supplier`, `dim_calendar`: no partitioning (tiny; broadcast-joinable as-is).
- Live evidence: re-run TC23 (gl_balance) and TC24 (ap_aging) with `EXPLAIN FORMATTED` captured pre + post, showing partition-pruning + data-skipping firing for a `WHERE period_year = 2025 AND currency_code = 'USD'`-style filter.

### `[ ]` P1.19 — Replace `monotonically_increasing_id()` with `xxhash64(natural_key)` for surrogate keys
**Why**: `dim_account.account_key` (`dim_account.py:227`) and `dim_supplier.supplier_key` (`dim_supplier.py:68`) both use `monotonically_increasing_id()`. Partition-local, non-deterministic across rebuilds — documented in the module docstrings as "downstream marts MUST join on the natural key, never on the surrogate". Fine under today's full-rebuild pattern, but breaks under P1.17's incremental MERGE (a row's surrogate would change every refresh, invalidating any downstream cache keyed on it). Same blocker for any future Type-2 SCD variant. `dim_supplier`'s docstring already names the upgrade: `xxhash64(natural_key)`. Apply to `dim_account` (`xxhash64(CAST(CodeCombinationCodeCombinationId AS STRING))`) too.
**Size**: S — one SQL expression per dim + a unit test asserting stability across two builds of the same bronze snapshot.
**Depends on**: nothing for the change itself; logically pairs with P1.17 — ship together so MERGE's correctness story includes stable surrogates.
**Accept**:
- `dim_account.account_key = xxhash64(CAST(CodeCombinationCodeCombinationId AS STRING))`.
- `dim_supplier.supplier_key = xxhash64(SEGMENT1)`.
- Unit test: build the same dim twice from a fixed bronze snapshot; assert every surrogate value matches.
- Docstring updated in both modules to drop the "non-stable across rebuilds" caveat.

### `[ ]` P1.20 — Implement Type-2 SCD on dim tables (`dim_supplier`, `dim_account`)
**Why**: Today's dims overwrite on every rebuild — no history. A supplier's name change, a payment-terms revision, a COA account re-mapping, all silently mutate dim rows in place. Downstream marts joining on the natural key see "as-of-now" only; historical fact rows lose their original dim context (the GL balance from FY23 joins to the *current* account hierarchy, not the FY23 one). SOX trail and any "what did this look like at period close" question are unanswerable. Named as a future blocker in P1.17 and P1.19 but never tracked as its own deliverable. Reference shape exists at `oracle-aidp-samples/data-engineering/transformation/scd/slowly_changing_dimension_template.ipynb` — Jinja2-templated two-step MERGE+INSERT (expire matched-but-differing current row, then insert new version). Needs adaptation: replace `current_date()` with the orchestrator's run timestamp, add `xxhash64(natural_key || effective_start_date)` surrogate for the *version key* (separate from the natural-key surrogate from P1.19), wire `_extract_ts` / `_run_id` audit columns, templatize the PK (the sample hardcodes `customer_id`).
**Size**: M — two dims × (DDL with `effective_start_date`, `effective_end_date`, `is_current` + two-step MERGE+INSERT + tracked-cols list + SQL-shape unit test + live evidence under TC25 / TC26 showing a tracked-col change produces two rows for the same natural key).
**Depends on**: P1.17 (incremental MERGE foundation) and P1.19 (deterministic surrogates) — ship after both so the Type-2 version key is `xxhash64(natural_key || effective_start_date)` and the MERGE machinery already exists.
**Accept**:
- `dim_supplier` and `dim_account` carry `effective_start_date TIMESTAMP`, `effective_end_date TIMESTAMP` (NULL for current), `is_current BOOLEAN`, `version_key BIGINT` (= `xxhash64(natural_key || CAST(effective_start_date AS STRING))`).
- Tracked-columns list per dim is explicit at the top of the module (e.g. `dim_supplier`: `supplier_name`, `business_relationship`, `pay_group`; `dim_account`: segment value descriptions).
- Two-step pattern: (a) `MERGE INTO dim USING src ON dim.natural_key = src.natural_key AND dim.is_current = true WHEN MATCHED AND (any-tracked-col differs) THEN UPDATE SET is_current = false, effective_end_date = :run_ts`; (b) `INSERT` new versions where natural key is new OR any tracked col differs from current.
- Downstream marts unchanged — they continue to join on the natural-key surrogate from P1.19, which is stable across versions. Point-in-time joins (fact's `_extract_ts` BETWEEN dim's `effective_start_date` AND `COALESCE(effective_end_date, '9999-12-31')`) are a follow-up, not part of this item.
- Live evidence: TC25 (dim_supplier) and TC26 (dim_account) showing (1) initial seed produces N current rows, (2) re-run with a tracked-col mutation produces N+1 rows with the mutated supplier/account having one `is_current=false` row and one `is_current=true` row, (3) re-run with no changes is a no-op (no spurious new versions).
- Empty-source case: zero rows, schema intact, no crash.

## Theme: Transforms framework (extract reusable pieces)

### `[ ]` P1.12 — Refactor `transforms/__init__.py` into a real framework
**Why**: After P1.2 + P1.8–P1.11, common patterns will emerge (audit columns, write modes, schema validation). Pull them out so future marts are ~50 LOC each not ~200.
**Size**: M
**Depends on**: at least 3 of P1.2 / P1.8 / P1.9 / P1.10 / P1.11 implemented (extract once you see the duplication)
**Accept**: each gold mart's main module is ≤80 LOC; common helpers live in `transforms/` (e.g. `audit_cols()`, `with_dim_join()`, `write_gold_table()`).

## Theme: Release packaging (cuts v0.2.0)

### `[ ]` P1.13 — Build the v0.2.0 `.bar` with 5+ workbooks
**Why**: The CLI does `dashboard install --bar-uri ...`, but no `.bar` ships today. Customers can't run the OAC install end-to-end without authoring workbooks themselves. TC10b–TC10e + TC10h-7 already proved the workbooks render; just need to package them.
**Size**: M (build on dev OAC; export Custom snapshot; smoke-test on second OAC)
**Depends on**: P1.2 (supplier_spend) and P1.8–P1.11 wired so dashboards have real data
**Accept**:
- Custom snapshot (Include: Catalog Content + Shared Folders + Application Roles; Exclude: Credentials, Connections, User Folders, File-based Data, Day by Day, Jobs, Plug-ins, Configuration).
- 5 workbooks under `/shared/AIDP_Fusion_Bundle/`: CFO dashboard, supplier_spend, PO backlog, GL balance trend, AR aging drill-down. Optional 6th: AP aging.
- Strong password (committed in release notes).
- Smoke-tested by running `dashboard install --target oac --bar-uri 'file:///aidp-fusion-bundle/bundle-v0.2.0.bar'` on a clean OAC and getting all 5 workbooks visible.

### `[ ]` P1.14 — Attach `.bar` as GitHub release artifact + bump versions
**Why**: Customers download the `.bar` from the release page, upload to their bucket. Current release page is empty.
**Size**: S
**Depends on**: P1.13
**Accept**:
- GitHub release `v0.2.0` with `.bar` attached + release notes (`.bar` password disclosed there).
- `plugin.json` version → `0.2.0`.
- `pyproject.toml` version → `0.2.0`.
- `__init__.py` `__version__` → `0.2.0`.
- CHANGELOG.md cuts `[0.2.0]` section dated.

### `[ ]` P1.15 — Submit PR to `oracle-samples/oracle-aidp-samples`
**Why**: This personal mirror's whole purpose is staging. Canonical home is the oracle-samples repo. Without the PR, end users can't `/plugin install` from Anthropic's curated marketplace.
**Size**: M (depends on review cycles)
**Depends on**: P1.14
**Accept**: PR open at `oracle-samples/oracle-aidp-samples/ai/claude-code-plugins/oracle-ai-data-platform-fusion-bundle/`; merged or in review.

### `[ ]` P1.16 — Bump marketplace metadata version
**Why**: `marketplace.json` is at `0.5.0` (marketplace-level); when bundle hits 0.2.0, marketplace bumps to track. Decide: every plugin release? Only major plugin changes? Document the policy.
**Size**: XS
**Depends on**: P1.14
**Accept**: `marketplace.json.metadata.version` bumped (recommend `0.6.0` to mark "fusion-bundle leaves alpha"); README notes the versioning policy.

---

# P2 — Quality, coverage, polish (do interleaved with P1; not blocking)

## Theme: Bug fixes (real defects, not gaps)

### `[ ]` P2.1 — Replace hardcoded 90s BICC API-key wait with exp backoff
**Why**: `commands/bootstrap.py` waits a fixed 90s for IDCS federation propagation. Fast pods waste 60s; slow pods (>120s) silently fail.
**Size**: S
**Depends on**: nothing
**Accept**: bootstrap polls `Test Connection` every 15s up to 180s with exp backoff (15, 30, 45, 60, 60); succeeds early when pod is fast; surfaces clear error after 180s.

### `[ ]` P2.2 — Auto-detect populated supplier IDs in `gold.supplier_spend`
**Why**: STATUS.md §5 issue #6: demo pod returns NULL/0 for `VendorId`/`PartyId`; bundle uses spend-only fallback. Production pods should switch to dim_supplier-joined form automatically.
**Size**: S
**Depends on**: P1.1 + P1.2 (folds into P1.2's accept criteria — track here for visibility)
**Accept**: `transforms/gold/supplier_spend.py` checks `dim_supplier.id_populated_pct() > 0.5` to pick join vs fallback. Both paths unit-tested.

### `[ ]` P2.3 — Verify `find_connection` substring-vs-exact filter
**Why**: TC10h-3 fix added exact-match filter (`aidp_fusion_jdbc` shouldn't match `aidp_fusion_jdbc_v2`). Need a regression test or it will silently regress.
**Size**: S
**Depends on**: nothing
**Accept**: `tests/unit/test_oac_rest_client.py` adds parametrized test covering `aidp_fusion_jdbc` vs `aidp_fusion_jdbc_v2` vs `aidp_fusion_jdbc_dev` with mocked OAC response; only exact `aidp_fusion_jdbc` matches.

## Theme: Test coverage

### `[ ]` P2.4 — Add `make test` target so pytest works regardless of shell PATH
**Why**: This recon session: `pytest` not on PATH → confusing failure. `python -m pytest` works regardless of activation state.
**Size**: XS
**Depends on**: nothing
**Accept**: `Makefile` (or `tasks.py`) has `test` target running `python -m pytest tests/unit -q`. README's quick-start mentions `make test`.

### `[ ]` P2.5 — Live test for `dashboard validate`
**Why**: Read-only probe, easy to test, currently no live coverage.
**Size**: S
**Depends on**: existing OAC instance with installed connection (TC10h-4 covered install; reuse)
**Accept**: `tests/live/TC18_dashboard_validate_results.md` with green run.

### `[ ]` P2.6 — Live test for `dashboard uninstall`
**Why**: Closes the install/uninstall round-trip; today only install is live-tested.
**Size**: S
**Depends on**: P2.5 (uninstall after validate)
**Accept**: `tests/live/TC19_dashboard_uninstall_results.md` showing connection deleted + snapshot deregistered.

### `[ ]` P2.7 — Smoke test for `dashboard mcp-config`
**Why**: Just prints JSON, but verifying the JSON is valid + paths-substituted-correctly catches future regressions cheaply.
**Size**: XS
**Depends on**: nothing
**Accept**: unit test in `tests/unit/test_commands.py` parses the printed JSON, asserts `mcpServers.oac-mcp-server.{command,args[0],args[1]}`.

### `[ ]` P2.8 — Live test for `--auth-flow device` headless OAuth
**Why**: Device-code path is implemented in `oac/rest/oauth.py` but only mock-tested.
**Size**: S
**Depends on**: nothing
**Accept**: `tests/live/TC20_device_code_oauth_results.md` showing fresh device-code flow getting an access token.

### `[ ]` P2.9 — Regression test for "PVO names abbreviated" finding
**Why**: TC1 found pdf1's abbreviated names don't work live; bundle catalog now uses full AM-hierarchies. Lock that in so a future "simplification" PR doesn't regress.
**Size**: XS
**Depends on**: nothing
**Accept**: `tests/unit/test_fusion_catalog.py` parametrized test asserting every confirmed PVO has at least 4 dot-separated AM segments (e.g. `FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO`).

### `[ ]` P2.10 — Schema migration test for `oac.workbooks` → `oac.snapshot`
**Why**: TC10h-2 changed bundle.yaml schema. Pre-TC10h-2 bundle.yaml files silently break. Validate clearly.
**Size**: S
**Depends on**: nothing
**Accept**: `validate` command emits a clear error when it sees the legacy `oac.workbooks: [...]` shape, points user at the migration note in CHANGELOG.

### `[ ]` P2.11 — saas-batch live test (placeholder)
**Why**: When a customer HCM pod becomes available (P3.C2), we need a TC ready to drop in.
**Size**: XS (skeleton); S (when run live)
**Depends on**: P3.C2 customer access
**Accept**: `tests/live/TC11_TC17_saas_batch_results.md` already documents the path; add a `### Pending live` section so contributors know what to fill in when the pod arrives.

## Theme: Documentation

### `[ ]` P2.12 — Write `docs/customizing.md` (custom COA segments + per-customer org dim flavors)
**Why**: STATUS.md §4.7 references this; doesn't exist.
**Size**: M
**Depends on**: P1.3 (`dim_account`) — content needs the actual extension points
**Accept**: doc covers (a) adding custom COA segment columns, (b) regional org-hierarchy variants, (c) test patterns for customizations.

### `[ ]` P2.13 — Write `docs/cross-source-recipes.md` (Fusion ×Salesforce / ×S3 / ×Workday)
**Why**: README hints at this use case; no concrete pattern documented.
**Size**: M
**Depends on**: at least one gold mart implemented (P1.2 minimum)
**Accept**: 3 worked examples joining Fusion gold marts to non-Fusion sources via the connectors plugin.

### `[ ]` P2.14 — Add `PRIVACY.md` matching the connectors plugin
**Why**: Sibling plugin (`oracle-ai-data-platform-workbench-spark-connectors`) ships a `PRIVACY.md`. Fusion-bundle should match for consistency + customer trust (data-handling statement).
**Size**: S
**Depends on**: nothing
**Accept**: `PRIVACY.md` exists with at minimum: data-flow diagram, what credentials touch what files, retention policy.

### `[x]` P2.15 — Add `CONTRIBUTING.md` (shipped 2026-05-11)
**Why**: Once the oracle-samples PR merges (P1.15), external contributors will arrive. Set the bar.
**Done**: `CONTRIBUTING.md` ships covering (a) `make test` + `ruff` pre-commit, (b) test running (unit + live-gated under `AIDP_FUSION_BUNDLE_INTEGRATION=1`), (c) live-test conventions (TC numbering, evidence-file shape, tenant identification, anomaly handling, re-verification-after-refactor rule), (d) PR template with plugin-portability checklist, (e) module checklist for new dim/mart spanning code shape, plugin-portability, medallion correctness, performance, SQL correctness, and CLI wiring. Cross-refs `CLAUDE.md` for the working principles split.

## Theme: Plugin durability across Fusion releases

### `[ ]` P2.16 — Schema-drift fingerprint + `catalog drift` command
**Why**: Every gold mart and silver dim hardcodes column names that came from a one-time live probe of the source PVO (e.g. `CodeCombinationCodeCombinationId`, `ApInvoicesVendorId`). Oracle revs PVOs across Fusion releases — column renames are uncommon but documented (the abbreviated-vs-full-AM-hierarchy thing in pdf1 was exactly this drift class). Today nothing detects this; first symptom on a customer's upgraded pod is `silver` build failing with "column not found" — loud, but no mitigation path.
**Size**: M
**Depends on**: P1.1 / P1.3 / P1.4 bronze tables existing on a live pod (✅ all done)
**Accept**:
- New `tests/live/schemas/<pvo_id>.json` snapshot per confirmed PVO, capturing `[(col_name, dtype)]` plus the date + Fusion release the snapshot was taken on.
- New `aidp-fusion-bundle catalog drift` CLI command that re-extracts each PVO, computes a fresh fingerprint, diffs vs stored, exits non-zero with a clear summary of added/removed/renamed/retyped columns.
- Snapshots committed for the existing PVOs (`erp_suppliers`, `ap_invoices`, `gl_coa`, `ar_invoices`, `ar_receipts`, `po_orders`, `po_receipts`).
- Unit test on the diff function with synthetic before/after schemas.
- README "operations" section documents the command and recommends running it after Fusion-release upgrades.

### `[ ]` P2.17 — Fusion release-version detection + support-matrix warning
**Why**: Even before any drift fires, customers should know whether their Fusion release is one we've actually verified. Today the bundle is silent; if a customer is on an unverified release, they discover the gap only when something breaks.
**Size**: S
**Depends on**: nothing
**Accept**:
- `SUPPORTED_FUSION_RELEASES: set[str]` constant in `schema/fusion_catalog.py` (or new `schema/support_matrix.py`); seeded with the releases we've live-verified against (e.g. `{"25C", "26A"}`).
- New helper that reads the customer's Fusion release at runtime (Fusion exposes its release version via a REST `about`-style endpoint — confirm exact path during implementation; pdf1 / aidp-fusion-bicc skill likely have a hint).
- `aidp-fusion-bundle install` and `aidp-fusion-bundle run` print a clear warning (not a hard failure) when the detected release is not in `SUPPORTED_FUSION_RELEASES`. Exit code 0 — informational.
- README "compatibility" section lists the supported releases and the policy ("verified releases get version-pinned bundle releases; later releases require running `catalog drift` first").
- Unit test mocks the about-endpoint response and verifies the warning fires for an unknown release and stays silent for a known one.

## Theme: Medallion performance — quick wins (round-6 perf audit, 2026-05-11)

### `[ ]` P2.18 — Hoist decimal casts in `gl_balance` into a CTE
**Why**: `transforms/gold/gl_balance.py:262-272` casts the same four `decimal(38,30)` amount columns to `DECIMAL(28, 2)` twice each — once in the surfaced projection (`begin_balance_dr`, `begin_balance_cr`, `period_net_dr`, `period_net_cr`) and again inside the `closing_balance` formula's `COALESCE(CAST(...))` wrappers. Catalyst doesn't reliably CSE across `CAST` boundaries on high-precision decimals; at 11M rows this is measurable CPU. `ap_aging` already gets this right via the `open_invoices` CTE (`ap_aging.py:431-445`) — cast once, outer SELECT operates on cast values.
**Size**: XS — one CTE refactor + existing unit tests should pass unmodified (output column shape is the contract).
**Depends on**: nothing.
**Accept**: `build_gl_balance_sql` emits a `WITH balances AS (SELECT cast-once)` CTE; outer SELECT references `b.begin_balance_dr` etc. instead of `CAST(b.BalanceBeginBalanceDr AS DECIMAL(28,2))`; existing `test_gl_balance.py` 24+ tests pass without changes.

### `[ ]` P2.19 — Project `currency_code` once in `supplier_spend` CTE
**Why**: `transforms/gold/supplier_spend.py:105, 122-123` emits `UPPER(CAST(inv.{currency_col} AS STRING))` in both the SELECT projection and the GROUP BY — same expression twice. Spark usually CSEs this but with `UPPER(CAST(...))` chains it sometimes doesn't, and it prevents the shuffle from using a precomputed partition column. `ap_aging` already projects `currency_code` once in its `open_invoices` CTE; mirror the pattern.
**Size**: XS — one CTE refactor.
**Depends on**: nothing.
**Accept**: `build_supplier_spend_sql` emits a CTE that projects `UPPER(CAST(inv.{currency_col} AS STRING)) AS currency_code` once; outer SELECT and GROUP BY reference `inv.currency_code` (or alias); existing `test_supplier_spend.py` tests pass with no output-shape change.

### `[ ]` P2.20 — Single-pass `ap_aging` build (cache filtered bronze)
**Why**: `ap_aging.build()` with `due_date_mode='auto'` runs `_measure_due_date_coverage()` (`transforms/gold/ap_aging.py:608-619`) — one full scan of `bronze.ap_invoices` with the open-invoice WHERE clause — then `build_ap_aging_sql()` re-scans the same filtered bronze for materialization. 50k rows on demo is nothing; on a tenant with 10M+ open invoices that's 2× the IO with identical filter predicates. Two viable fixes: (1) cache the filtered DataFrame between the two queries; (2) compute coverage as a windowed column inside the materialization, abort/rerun as proxy if below threshold (single scan, but couples concerns). Recommend (1) unless live evidence shows the cache size is prohibitive.
**Size**: S — small refactor + live re-verification of TC24 to confirm timing improvement; ensure cache is released after the build.
**Depends on**: nothing.
**Accept**: one filtered-bronze scan per build in `due_date_mode='auto'`; live evidence (TC24c) shows ~halved IO vs TC24 baseline on the same tenant; existing 30+ `test_ap_aging.py` tests pass (cache is Spark-side, doesn't change the asserted SQL shape).

### `[ ]` P2.21 — Add Delta auto-optimize table properties to bronze + silver + gold
**Why**: None of the `CREATE OR REPLACE TABLE … USING DELTA` statements set `TBLPROPERTIES`. Daily incremental refresh on AIDP's Spark cluster will produce thousands of small files within a few months → manifest read time dominates per-query latency. Standard Delta-Lake fix is `delta.autoOptimize.optimizeWrite=true` + `delta.autoOptimize.autoCompact=true` on tables that get frequent writes (bronze + silver primarily; gold benefits less because gold is read-target, not write-hot-path).
**Size**: S — DDL-only addition to each `CREATE TABLE` template + a periodic `OPTIMIZE` call in the orchestrator.
**Depends on**: nothing.
**Accept**:
- Every bronze + silver `CREATE OR REPLACE TABLE` includes `TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')`.
- Gold tables get `TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')` (autoCompact less relevant for write-once-per-refresh gold).
- Orchestrator (P1.5) runs `OPTIMIZE <gold_table>` weekly (or after seed load).
- Unit test asserts emitted SQL contains the expected `TBLPROPERTIES` clauses.

## Theme: Plugin-portability — evidence-driven knobs (deferred)

### `[ ]` P2.22 — Evidence-driven knob backlog (defer until a customer hits each)
**Why**: Round-6 plugin-portability audit (2026-05-11) surfaced more hardcoded values in the new dim/gold modules. The principle established with P1.5a / P1.11a is: knobs ship when a real tenant surfaces the variant, not preemptively. Capture the list so future-us doesn't re-derive it. None of these block any current customer.
**Specific candidates** (location → trigger condition → knob shape when promoted):
- **Aging bucket boundaries `0/30/60/90`** (`transforms/gold/ap_aging.py:314-339`, `_bucket_case`) — promote when a customer needs `0/15/30/45/60` or `0/30/60/90/120/150`. Shape: `aging_buckets: Sequence[tuple[int, str]]`.
- **NET-30 residual fallback** (`transforms/gold/ap_aging.py:258-266`, `_due_date_coalesce_expr`) — promote when a customer's standard terms are NET-45 or NET-60. Shape: `net_days_fallback: int = 30`.
- **Cancelled-flag truthy value `'Y'`** (`transforms/gold/ap_aging.py:295`, `_cancelled_filter`) — promote when a tenant's extract emits `'Cancelled'` / `'1'` / `'TRUE'`. Shape: `cancelled_flag_truthy: str = 'Y'`.
- **`dim_supplier` hardcoded column names** (`dimensions/dim_supplier.py:63-95`) — no schema-variant knobs or `detect_*_params()` probe (regression from the `ap_aging` standard). Promote when a tenant's `SupplierExtractPVO` is missing `AlternateNamePartyName` / `BUSINESSRELATIONSHIP` / similar and crashes with `UNRESOLVED_COLUMN`. Fix shape: apply the same detect+kwargs pattern `ap_aging` uses.
- **Fiscal-year naming convention** (`dimensions/dim_calendar.py:97-103`) — assumes "FY = calendar year FY ends in". Promote when an EU tenant uses "FY = calendar year FY begins in". Shape: `fy_naming: Literal["ends_in", "begins_in"] = "ends_in"`.

**Out of scope (intentionally skipped)**:
- COA segment default map (`dimensions/dim_account.py:106-113`, `transforms/gold/gl_balance.py:132-139`) — already overridable via `semantic_segment_map` / `coa_segment_map`; default matches majority Fusion convention; no action needed.
- Calendar date range default `2020 → 2030` (`dimensions/dim_calendar.py:41-42`) — `start_date` / `end_date` kwargs already exist; only gap is surfacing them in `bundle.yaml` schema, which falls under P1.5b's plumbing scope.

**Size**: 0 today (capture only); each promoted item is XS-S when triggered.
**Depends on**: customer-driven evidence.
**Accept**: this entry stays open until either (a) every sub-item has a fielded report + promoted backlog entry, or (b) v1.0 ships with confidence the list is non-load-bearing.

## Theme: Security hardening

### `[ ]` P2.23 — Secret-handling hardening before first non-`saasfademo1` customer
**Why**: P1.5α ships `SecretStr` wrapping (`_resolve_password()` in `orchestrator/runtime.py` — see `PLAN_P1.5_orchestrator.md` §4.9) so resolved credentials don't leak through `repr`/`str`/`debug` accidents. But the schema-level footgun is still open: `schema/bundle.py:73` declares `password: str` and accepts a literal value equally with `${vault:OCID}` / `${env:VAR}` — Pydantic does not reject `password: hunter2`. In dev phase this is acceptable (1 user, both example bundles use the sigil, demo-pod creds, `_resolve_password()` logs a WARN on literals). At first non-`saasfademo1` customer onboarding, this becomes a real "creds in git history" risk and must be closed before the customer's `bundle.yaml` lands in a repo. Four hardening items, each cheap individually, sized together because they share the secret-resolution code path.
**Size**: M — schema-validator + preflight + env-var gating + lint, ~3-4h plus tests.
**Depends on**: P1.5α shipped (this builds on `_resolve_password()` + `SecretStr` plumbing). Triggered by P3.7 (first non-`saasfademo1` customer) — must land **before** that customer's bundle is committed anywhere.
**Items**:
1. **Reject literal passwords at config-load**: Pydantic `field_validator` on `FusionConn.password` enforces the sigil grammar (`^\$\{vault:OCID\}$` or `^\$\{env:VAR\}$`). Literal values raise `ValueError` at `bundle.yaml` load — fails fast, before any orchestrator code runs, before Spark touches anything. Removes the dev-phase WARN from `_resolve_password()` since the validator catches it first.
2. **Vault-OCID accessibility preflight**: `orchestrator.run()` setup calls `aidputils.secrets.get(ocid)` once before any DAG dispatch. Fails fast with a clear "vault OCID not accessible — check IAM policy" before the first bronze extract. Same shape for `${env:VAR}` — assert env-var is set at startup, not at first BICC call.
3. **Env-var gating in `commands/catalog.py:76`**: today's `pwd = password or os.environ.get("FUSION_BICC_PASSWORD")` is a perfectly valid dev convenience for the ad-hoc CLI flow, but bundle-driven `run` should agree with the bundle path on policy. Add `--allow-env-creds` flag (or `AIDP_ENV=dev` gate) so prod CLI runs reject env-var-derived creds unless the bundle explicitly opts in via `password: ${env:VAR}`.
4. **Debug-log masking lint**: grep rule (CI step) preventing `debug(...password...)` / `debug(...bundle.fusion...)` patterns. Catches the future "added a debug call and accidentally logged the password" defect at PR time, not production time. Complements `SecretStr`'s `repr` masking — the regex catches the case where someone calls `secret.get_secret_value()` and logs the result.

**Accept**:
- `bundle.yaml` with `password: hunter2` is rejected by Pydantic with a clear error message naming both sigil forms.
- `orchestrator.run()` exits 2 with "vault OCID `ocid1.vaultsecret.…` not accessible — check IAM" when the OCID is bad, before any Spark work.
- `aidp-fusion-bundle catalog probe --pod X` (no `--password`, no `--allow-env-creds`) errors with "set --password or pass --allow-env-creds for dev use" instead of silently picking up `FUSION_BICC_PASSWORD` from env.
- CI greps the repo for `debug(.*password|debug(.*\.fusion\.` and fails the build on a match.
- Unit tests cover all four items; live evidence on `saasfademo1` shows the validator + preflight running cleanly with the existing example bundles.

---

# P3 — Roadmap, upstream, tracked blockers (don't act now; track)

## Theme: v3+ roadmap

### `[ ]` P3.1 — `agent ask "..."` CLI helper
**Why**: TC9 proved `ai_generate('openai.gpt-5.4', ...)` against `gold.supplier_spend`. Wrap as a CLI sugar for ad-hoc agent queries.
**Size**: M
**Depends on**: P1.2+ gold marts available
**Accept**: `aidp-fusion-bundle agent ask "which suppliers had >$1M Q1 spend?"` returns grounded answer with citations.

### `[ ]` P3.2 — Delta Sharing provider config
**Why**: README mentions it as v3 roadmap. Share curated gold-layer datasets with external partners without copies.
**Size**: L
**Depends on**: P1.13 (need the marts to share); AIDP-side Delta Sharing provisioning
**Accept**: bundle.yaml `delta_sharing: { enabled: true, recipients: [...] }` block; CLI emits share-recipient config.

### `[ ]` P3.10 — Orchestrator parallel execution
**Why**: P1.5α explicitly chose sequential execution (`PLAN_P1.5_orchestrator.md` §7). Rationale at the time: saasfademo1 seed run finishes in <2 min and parallelism complicates failure-mode semantics. Trigger to revisit: any tenant where the seed run exceeds ~5 min wall-clock, OR where multiple bronze extracts could run concurrently against independent PVOs. The orchestrator's DAG already encodes dependencies (`depends_on_bronze`, `depends_on_silver`) — parallelism is a scheduler swap, not a re-architecture (e.g. `concurrent.futures` thread pool driving `graphlib.TopologicalSorter`'s ready-set).
**Size**: M — swap the topo executor for a ready-set scheduler; preserve fail-fast semantics; bounded worker count (config knob, default 4).
**Depends on**: P1.5α shipped; live evidence on at least one tenant where sequential runtime is the bottleneck.
**Accept**:
- `orchestrator.run()` gains `max_workers: int = 1` kwarg (default keeps today's sequential behavior).
- Independent bronze extracts (no shared PVO) and independent dim builds run concurrently up to `max_workers`.
- Fail-fast preserved: a failed step still skips dependents and halts new dispatches.
- Live evidence: TC<N> showing wall-clock reduction on a tenant with ≥4 enabled datasets.

### `[ ]` P3.11 — Orchestrator step-level retries
**Why**: P1.5α explicitly chose fail-fast (`PLAN_P1.5_orchestrator.md` §7) — re-run the CLI if a step fails. Trigger to revisit: transient BICC failures (rate-limit 429s, network blips, OAC connection timeouts) observed in real customer runs. Distinct from P2.1 (BICC API-key bootstrap exp backoff, one-shot at install time) — this is per-step retry at run time. Should be scoped to *transient* errors only (network, rate-limit), not data-correctness errors (schema mismatch, NULL currency hard-gate); the orchestrator must classify before retrying or it will mask real bugs.
**Size**: M — retry policy (max attempts, backoff curve), error classification (`RetryableError` vs `FatalError`), `fusion_bundle_state` schema extension (attempt count per step).
**Depends on**: P1.5α shipped; a documented transient-failure incident from a real run.
**Accept**:
- `orchestrator.run()` gains `retry_policy: RetryPolicy | None = None` kwarg (default: no retries — preserves today's fail-fast).
- Module-raised exceptions classified into retryable (network, rate-limit) vs fatal (schema, data); only retryable trigger retry.
- `fusion_bundle_state` rows record `attempt: int` so post-hoc analysis sees retry behavior.
- Unit-tested with a fake extractor that raises retryable then succeeds.

### `[ ]` P3.12 — Orchestrator failure alerting / notifications
**Why**: `NotificationsSpec` already exists in `schema/bundle.py` but no consumer. P1.5α §7 acknowledges this and defers. Trigger to revisit: first customer asking for "tell me when the daily seed run fails" — likely after the bundle is in scheduled production use (post-v0.2.0). Channels customers will want: email (SMTP), Slack webhook, OCI Notifications service. Keep the alerter pluggable so a customer with a custom incident-management tool can wire their own.
**Size**: M — define `Alerter` protocol; ship two concrete implementations (Slack webhook + OCI Notifications); orchestrator invokes on `RunSummary.failed > 0` after the run completes.
**Depends on**: P1.5α shipped; at least one customer asking for it (don't speculate on payload shape).
**Accept**:
- `bundle.yaml` `notifications: { on_failure: [...] }` block consumed by the orchestrator after the run.
- Slack webhook + OCI Notifications implementations included; both unit-tested with a fake HTTP layer.
- Failure alert payload includes: bundle project, run_id, failed step name + error message, link to `fusion_bundle_state` query for full detail.
- Alerter invocation never blocks or fails the run itself (log + swallow on alerter exception).

## Theme: Upstream advocacy (not bundle-fixable)

### `[ ]` P3.3 — File issue with Oracle AIDP team re: Resource Principal env vars
**Why**: AIDP sets `AIDP_AUTH=resource_principal` but doesn't provide `OCI_RESOURCE_PRINCIPAL_RPST` / `OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM` → RP fails. Affects every plugin; bundle works around with API Key + inline PEM.
**Size**: XS (file issue); blocking until resolved
**Depends on**: nothing on our side
**Accept**: issue filed, link captured in this backlog. When Oracle ships the fix, simplify auth helpers (delete inline-PEM code path).

### `[ ]` P3.4 — File issue with Oracle OAC team re: `idljdbc` connectionType
**Why**: OAC's REST validator doesn't bless AIDP's `idljdbc` → `POST /catalog/connections` 400s on first install. Customer must use OAC UI workaround.
**Size**: XS (file issue); blocking until resolved
**Depends on**: nothing on our side
**Accept**: issue filed referencing TC10h-4 evidence. When OAC ships AIDP connection-type validation, we can remove the `--print-only` UI-upload step.

### `[ ]` P3.5 — File issue with Oracle Fusion team re: PVO name documentation
**Why**: pdf1's abbreviated PVO names don't work live (TC1). Doc should match the live BICC catalog format.
**Size**: XS
**Depends on**: nothing on our side
**Accept**: issue filed; if accepted, this backlog item references the doc fix.

### `[~]` ~~P3.13 — File issue with Oracle AIDP team re: notebook-job submission REST API~~ — **PROMOTED to P1.5ε**
**Why cancelled**: Oracle published the `aiwap` REST API on 2026-04-30, including the `POST /jobs` + `POST /jobRuns` + `fetchOutput` flow this item asked for. No longer an advocacy item — implementable work, now tracked as **P1.5ε** under "Plugin-portability follow-ups." See that entry for schema facts and acceptance criteria.

## Theme: Tracked blockers (waiting for environments)

### `[ ]` P3.6 — Customer Fusion HCM pod for saas-batch live test
**Why**: Demo pod (`saasfademo1`) returns 404 on `/saas-batch/security/tokenrelay` — HCM-tier feature, paying customers only. 14 unit tests cover the path.
**Size**: 0 (blocker only)
**Depends on**: customer engagement
**Accept**: when a customer pod arrives, run TC11–TC17 (P2.11) and update results.

### `[ ]` P3.7 — Customer pod with populated supplier IDs
**Why**: Demo pod's `SupplierExtractPVO` returns NULL/0 for `VendorId`/`PartyId`. Production pods needed to validate the join-form `gold.supplier_spend` (P2.2).
**Size**: 0 (blocker only)
**Depends on**: customer engagement
**Accept**: TC8 re-run on production-shape data; gold mart auto-detection (P2.2) verified.

### `[ ]` P3.8 — Customer pod for `dim_org` PVO confirmation
**Why**: P1.7 (`dim_org`) blocked on identifying the right HCM/HR PVO via live `catalog probe`.
**Size**: 0 (blocker only)
**Depends on**: customer engagement
**Accept**: PVO name added to `schema/fusion_catalog.py`; P1.7 unblocks.

### `[ ]` P3.9 — Dedicated CI test pod for live PVO regression
**Why**: P2.16 (`catalog drift`) gives customers a tool to detect drift on their pod, but without a CI-accessible Fusion pod we can't catch drift between releases on the bundle's own side. Demo pod (`saasfademo1`) is shared, rate-limited, and unreliable for scheduled runs; customer pods must never be touched from CI. The right fix is an AIDP-side dedicated plugin-CI pod with stable creds, refreshed monthly, opt-in for the plugin to run a small live extract per PVO and assert schema fingerprint stability.
**Size**: 0 (blocker only — depends on AIDP infra)
**Depends on**: AIDP team provisioning a CI-accessible Fusion pod; P2.16 fingerprint command exists
**Accept**: GitHub Actions (or AIDP-internal CI) workflow runs nightly: extracts each `confirmed=True` PVO, diffs against stored fingerprint, opens an issue on drift. Same pod is reused for the saas-batch live test (P2.11) so it covers two blockers at once.

---

# Summary table — execution order recommendation

If you're picking from the top, here's the suggested first 10 sessions:

| # | Item | Class | Size | Why this order |
|---|---|---|---|---|
| 1 | P0.1 — CHANGELOG date stamp | P0 | XS | 30 sec; instant credibility |
| 2 | P0.3 — STATUS+BACKLOG git decision | P0 | XS | 1 min; clears repo state |
| 3 | P0.4 + P0.5 — README phase callouts | P0 | XS | 15 min; stops misleading users |
| 4 | P2.4 — `make test` target | P2 | XS | 15 min; fixes today's pytest pain |
| 5 | P0.6 — README references STATUS/BACKLOG | P0 | XS | 5 min; closes P0 |
| 6 | P1.1 — `dim_supplier` | P1 | S | 2-4h; smallest dim, prototyped |
| 7 | P1.2 — `gold.supplier_spend` | P1 | S | 2-4h; productize TC8 SQL |
| 8 | P1.3 — `dim_account` | P1 | S | needed for P1.8 |
| 9 | P1.4 — `dim_calendar` | P1 | S | needed for P1.8, P1.11 |
| 10 | P1.5 — `orchestrator` + notebook | P1 | M | wire it all; closes P0.2 |

After that the pattern is established and the rest of P1 falls into place; interleave P2 quality items as natural breaks between P1 features.

---

## Cross-references

- Status snapshot: [`STATUS.md`](STATUS.md)
- Plugin reference: `/Users/oussamalakrafi/Workspace/Claude-Context/claude-code-plugins-ahmed/07-fusion-bundle-plugin.md`
- Cross-cutting reference set: `/Users/oussamalakrafi/Workspace/Claude-Context/claude-code-plugins-ahmed/`
- Live evidence trail: [`tests/live/`](tests/live/)
- CHANGELOG (decision history): [`CHANGELOG.md`](CHANGELOG.md)
