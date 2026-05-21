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

### `[~]` P1.5 — orchestrator/__init__.py + notebooks/run_orchestrator.ipynb (Phase α shipped in commits 9e15d79 → 7f57d38; live TC26 pending)
**Status (2026-05-17)**: Phase α implementation **shipped** across five atomic commits on `oussama-dev`:
- `9e15d79` P0  — catalog cleanup (bronze_table_name rename, SAAS_BATCH, ar_aging/ap_aging cleanup)
- `c6f4ace` Phase 2 — orchestrator package (errors, registry, runtime, state) + Bundle versioning
- `f113fb2` Phase 3 — orchestrator/__init__.py run loop + resolve_plan + two-phase cascade
- `2df8cc3` Phase 4 — module retrofit: 6 modules gain run_id kwarg + silver_run_id/gold_run_id audit cols (closes P1.5α-fix9)
- `7f57d38` Phase 5 — CLI integration: --inline calls orchestrator.run, migrate-bundle scaffold, status() latest-per-dataset
482 unit tests pass (was 369 at session start). Plus Phase 6 (commit pending): `notebooks/run_orchestrator.ipynb` 3-cell demo + `tests/live/TC26_orchestrator_seed_run.md` procedure doc with expected outputs + failure-mode probes. Live execution on `fusion_bundle_dev` is the closing gate — see TC26 doc for the procedure.

**Why**: Public entry point that wires extract → bronze → silver → gold sequence and persists state to `fusion_bundle_state` Delta table. Resolves P0.2 fully.
**Size**: M
**Depends on**: P1.1–P1.4 (shipped).
**Accept**:
- ✅ `orchestrator.run(bundle_path, *, spark=None, mode='seed', datasets=None, layers=None, dry_run=False) → RunSummary`.
- 🟡 Incremental watermarking — Phase β (separate plan); `read_last_watermark` stubbed for α.
- ✅ Notebook at `notebooks/run_orchestrator.ipynb` (3 cells: import, seed run, state-table + audit-col verification).
- ✅ `cli.py` `run` command: `--inline` calls orchestrator directly; REST dispatch path is a stub today (BACKLOG P1.5ε — empirically validated, not wired).
- ✅ Removed the TODO from `commands/run.py:175` (closes P0.2).
- 🟡 Live TC26 evidence on a real tenant — procedure doc shipped (`tests/live/TC26_orchestrator_seed_run.md`); execution pending operator-side credential setup.
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
**Depends on**: P1.3 (`dim_account`) ✅; P1.4 (`dim_calendar`) ✅ — but **dim_calendar dep was nominal**, not used in the SQL (grain mismatch: daily dim vs period fact; period context comes from fact's `period_year`/`period_num` directly). See the dim_calendar grain-mismatch note in the live evidence TCs for the deviation rationale.
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

### `[x]` P1.5α-fix1 — PLAN §4.4 review corrections (closed 2026-05-15)
**Why**: Read-through of the §4.4 (the `_execute_node` + run-loop pseudocode) surfaced two correctness bugs in the as-drafted code. Both reflected in the plan BEFORE α implementation starts. Single trackable item so the corrections don't get lost between drafting and committing.

**Bug 1 — BICC double-pull on bronze count** (PLAN line 525). `[FIXED in plan 2026-05-15]`
- **Problem**: bronze branch did `df.write...saveAsTable(target); return RunStep.success(..., row_count=df.count())`. `df` is the lazy `extract_pvo()` (`reader.load()`) wrapped with audit columns — calling `.count()` after the write actions the plan a SECOND time, triggering a second BICC HTTP fetch against Fusion. BICC extracts are not idempotent (each call opens a new `_extract_ts` window), so the count could differ from what was just written, and every bronze extract doubles Fusion load on the customer's tenant.
- **Fix**: count from the materialized Delta target — `row_count=spark.table(target).count()`. Applied to PLAN §4.4 lines 525-537. Acceptance-criteria checklist updated with the unit-test contract: fake-Spark stub records every method call on the `extract_pvo` return; assert exactly one action terminator (`saveAsTable`) and zero `.count()` / `.collect()` / `.show()` calls. Silver/gold branches exempt: module contract is that `build()` writes the target inside the call and returns `spark.table(<resolved>)`, so `.count()` is a cheap Delta read.

**Bug 2 — Failure cascade never runs** (PLAN line 477). `[FIXED in plan 2026-05-15 — Option C applied]`
- **Problem**: The success-path branch checks `if step.status == "failed" and node.is_required_upstream(): _skip_dependents(...)`, but `_execute_node` only ever returned `RunStep.success(...)` or **raised** — there was no return path producing `status="failed"`. So that branch was dead code. The exception-path branch caught, wrote a failed step, then `break`d — **without calling `_skip_dependents`**. Net: failed upstreams produced 1 `failed` row + 0 `skipped` rows, contradicting §4.7 and the acceptance criterion that mandates downstream `status="skipped"` cascade rows.
- **Decision (Option C)**: chosen over A and B because it's the only option that respects the data-error vs infrastructure-error distinction — a cardinal data-engineering principle. Pattern aligns with Airflow / Dagster / Databricks Workflows (narrow catch at the unit-of-work boundary, infra exceptions propagate). Option A over-catches (state-write bugs masked as "module failures" — bookkeeping fraud against the operator). Option B is acceptable but maintains two loop paths and requires editing §4.7 prose; Option C matches §4.7 literally.
- **Fix applied to PLAN §4.4**:
  - `_execute_node` body wrapped in try/except; module-dispatch exceptions return `RunStep.failed(node, run_id, mode, exc)`. The unknown-spec `raise TypeError` sits OUTSIDE the try/except so it propagates as an orchestrator bug.
  - Run loop collapsed to a single branch — no separate orchestrator-level try/except around `_execute_node`. Cascade is triggered uniformly via `step.status == "failed"` check after every step.
  - Boundary comment in `_execute_node` docstring documents what belongs inside the try and what doesn't, citing the unit-test invariant.
- **Two new acceptance-criteria tests added to PLAN**:
  - `test_failed_bronze_cascades_to_skipped_silver_and_gold` — stub `extract_pvo` to raise; plan `ap_invoices → dim_supplier → supplier_spend`; assert 1 `failed` + 2 `skipped` rows with correct cross-references, exactly 3 `write_state_row` calls, no later nodes attempted.
  - `test_state_write_failure_propagates_as_uncaught_exception` + `test_unknown_spec_type_raises_typeerror` — orchestrator-infrastructure failures must crash with their real stack trace, NOT get absorbed as a misleading `failed` step.

**Size**: S — plan edits only, no production code. ~45 min total (both bugs).
**Depends on**: nothing. Landed before any α implementation commit.
**Accept** (all met 2026-05-15):
- ✅ Bug 1: PLAN §4.4 + acceptance criteria reflect target-table counting.
- ✅ Bug 2: PLAN §4.4 pseudocode rewritten per Option C; §4.7 prose stays correct as-written ("`_execute_node` caught the exception"); two new tests added to acceptance criteria.
- ✅ Both bugs traceable from the canonical PLAN back to this BACKLOG entry for audit.

### `[x]` P1.5α-fix2 — Drop `--mode full` from CLI surface (shipped 2026-05-17)
**Why**: `cli.py:112` used to accept `--mode full` via `click.Choice(["full","incremental","seed"])`. The orchestrator's `Literal["seed","incremental"]` is a type-hint, not runtime-enforced — so `--mode full` would reach `orchestrator.run(...)` unchallenged, pass the `if mode == "incremental"` guard (because `"full" != "incremental"`), and land rows in `fusion_bundle_state` with `mode="full"` — a value outside the documented enum. Worst kind of bug: no exception, no log, silent state-table contract pollution.
**Done** (Option A surface + Option D defense-in-depth):
- ✅ `cli.py:113`: `Choice(["seed", "incremental"])`, default `"seed"`, help text mentions the retired alias.
- ✅ `commands/run.py`: default `"seed"`; type-hint `Literal["seed","incremental"]`.
- ✅ `orchestrator/__init__.py:433-440`: `_VALID_MODES = frozenset({"seed","incremental"})`; entry-point validation raises `UnsupportedModeError` with retired-alias hint (validation runs BEFORE `load_bundle` — zero filesystem / Spark / state side effects on bad mode).
- ✅ `orchestrator/errors.py:53`: `UnsupportedModeError(OrchestratorConfigError, ValueError)` — multi-inherits `ValueError` so legacy callers that catch `ValueError` still trap mode errors (P1.5α-fix6 marker-pattern back-compat).
- ✅ `test_run_cli_rejects_mode_full_at_parse_time` (`test_commands.py:278`): Click parses `--mode full`, exit code 2, `orchestrator.run` patched and `assert_not_called()` confirms parse-time rejection — orchestrator never invoked.
- ✅ `test_mode_full_raises_before_any_io` (`test_orchestrator_run.py:126`): `pytest.raises(UnsupportedModeError, match="full")` + `"retired" in str(exc)` (breadcrumb preservation) + `isinstance(exc, ValueError)` (marker-pattern contract) + `load_bundle` NOT called.

### `[x]` P1.5α-fix3 — State-table failure semantics: hard `ensure`, soft per-step write (shipped 2026-05-17)
**Why**: Read-through of the canonical PLAN surfaced a direct contradiction between §4.4 (after Option C was applied for the cascade bug, state writes propagate uncaught) and §4.7 line 767 ("State-table write failure: log + continue"). Both can't be right. The deeper question is whether `fusion_bundle_state` is observability (logs-like; may fail without consequence) or data contract (rows read by future runs and must be reliable). Answer: **both** — Phase α uses it mostly for `status()` human-readable output, but Phase β reads `last_watermark` from it to drive incremental `MERGE INTO`. So pure "log + continue" misses the watermark concern; pure "halt always" kills 45-min bronze re-extracts on 2-second network blips.
**Approach** (Option 4 — hard `ensure`, soft per-step write):
- **Layer 1 (hard)**: `state.ensure_state_table(spark, paths)` at orchestrator start (§4.4 step 5). Creates the table if absent AND probes writeability (INSERT a sentinel row + DELETE; catches "create succeeded but write denied" on tenants with split DDL/DML grants). On any failure: raises uncaught — halts BEFORE any module dispatch so no bronze extract burns Fusion-side load against a structurally inaccessible state table.
- **Layer 2 (soft)**: `_safe_write_state_row(spark, paths, step, console)` in `orchestrator/runtime.py`. Wraps `state.write_state_row` in try/except. On exception: logs WARN with `dataset_id`, `layer`, `status`, `repr(exc)`; returns `False`; does NOT raise. Caller continues. Cascade decisions in the run loop are made from in-memory `step.status`, never from whether the row landed — so state-write failures never affect in-run correctness.
- `_skip_dependents` uses `_safe_write_state_row` internally for the `skipped` rows it writes.
- Update §4.4 run loop, `_execute_node` docstring boundary comment, §4.7 line 767 (make the precondition explicit), file layout in §4.1 (add `_safe_write_state_row` to `runtime.py` listing). **All four edits applied 2026-05-15.**
- Phase β's `read_last_watermark` must handle missing rows gracefully (NULL → full extract, idempotent in seed mode). Documented as a forward-looking constraint; no Phase β code touches yet.
**Acceptance-test changes** (applied to PLAN 2026-05-15):
- **Removed** (was added under Option C): `test_state_write_failure_propagates_as_uncaught_exception` — no longer valid; per-step writes don't halt the run.
- **Added** `test_state_write_failure_logged_and_continues`: stub `state.write_state_row` to raise `OSError("transient")` on second call; assert `_safe_write_state_row` returns `False` on that call but `True` on others; WARN log emitted exactly once with all four fields; loop continues; all in-memory steps produced; `RunSummary` returned normally; `state.write_state_row` called `len(steps)` times (wrapper attempts every write).
- **Added** `test_ensure_state_table_failure_halts_run_before_dispatch`: stub `state.ensure_state_table` to raise `PermissionError("Delta DDL denied")`; assert `orchestrator.run(...)` raises `PermissionError`; `_execute_node` called zero times.
- **Updated** `test_failed_bronze_cascades_to_skipped_silver_and_gold` (the Option C cascade test) to assert calls go through `_safe_write_state_row`, with `state.write_state_row` underneath called 3 times when all writes succeed.
**Size**: S — ~10 LOC of wrapper code + 4 plan edits + 2 unit test changes + 1 unit test update. ~45 min. Plan edits already applied.
**Depends on**: P1.5α-fix1 (Option C cascade refactor, already applied to plan). The split contract builds on Option C's cascade decoupling — cascades use in-memory state, so soft per-step writes don't affect cascade correctness.
**Done**:
- ✅ PLAN §4.4 run loop calls `_safe_write_state_row(...)`, not `state.write_state_row(...)` directly.
- ✅ PLAN §4.4 `_execute_node` boundary comment describes the soft-vs-hard split.
- ✅ PLAN §4.7 line 767 makes the `ensure`-passed precondition explicit and links to DECISION doc.
- ✅ PLAN §4.1 file layout lists `_safe_write_state_row` under `runtime.py`.
- ✅ PLAN acceptance criteria has the two new tests + updated cascade test.
- ✅ `state.ensure_state_table` writeability probe (INSERT sentinel + DELETE) shipped at `state.py:77-116`.
- ✅ `_safe_write_state_row` SOFT wrapper shipped at `runtime.py:585-612` — try `state.write_state_row` / except `Exception` → `logger.warning(...)` with the 4 required fields + `return False`.
- ✅ `TestStateWriteFailureSemantics.test_state_write_failure_logged_and_continues` (`test_orchestrator_run.py`) — flaky `state.write_state_row` on the 2nd call surfaces 1 WARN with all 4 fields; loop completes; wrapper attempted every step's write; in-memory `RunStep` sequence intact.
- ✅ `TestStateWriteFailureSemantics.test_ensure_state_table_failure_halts_run_before_dispatch` — `PermissionError` from `ensure_state_table` propagates; `_execute_node` patched + `assert_not_called()` confirms zero dispatch attempts.
- ✅ `TestRunCascadeAndAbort.test_failed_bronze_cascades_to_skipped_silver_and_gold` updated with `wraps=`-style patches that count both wrapper invocations and underlying `state.write_state_row` calls — both equal `len(steps)`, proving the run loop persists every step through the SOFT wrapper.

### `[~]` P1.5α-fix4 — Layer/dataset filter semantics: intra-plan vs extra-plan dependencies (impl + tests shipped 2026-05-17; awaiting live evidence for `[x]`)
**Why**: the §4.2 advertises `layers=["gold"]` as the iterating-on-gold-SQL workflow ("only rebuild gold without re-extracting bronze"). §4.7 simultaneously says any consumer whose dependency is filtered out of the current run hard-fails with `MissingDependencyError`. The two contradict: running `orchestrator.run(layers=["gold"])` would crash on every gold mart's bronze prerequisite.
**Approach** (Option 4 — distinguish intra-plan from extra-plan dependencies):
- **Intra-plan** deps (both consumer and provider in current plan): standard topo-sort + cascade-on-failure as today.
- **Extra-plan** deps (provider filtered out by `datasets=`/`layers=`): preflight via `spark.catalog.tableExists(...)` BEFORE any module dispatch. Missing → `PrerequisiteError` with redirect message naming what's missing and how to fix it.
- **Two distinct error classes**:
  - `MissingDependencyError`: logical — consumer references a `dataset_id` that exists nowhere in the registry. Bundle.yaml typo or registry inconsistency. Raised at `resolve_plan` time.
  - `PrerequisiteError`: data — extra-plan provider's Delta table doesn't exist on disk. User skipped a materialization step. Raised at `_preflight_external_deps` time. Message: *"Prerequisite tables not found: `<catalog>.bronze.ap_invoices` (needed by `'supplier_spend'`). Either: include layer(s) ['bronze'] in --layers, OR run with --datasets ap_invoices first to materialize."*
- New types in `orchestrator/runtime.py`: `ExternalDep` dataclass, `MissingDependencyError`, `PrerequisiteError`, `_preflight_external_deps(spark, deps) → None`.
- New helper in `orchestrator/__init__.py`: `resolve_plan(bundle, datasets, layers, all_nodes, paths) → (list[Node], list[ExternalDep])` — does the topo-sort + intra/extra split atomically.
- §4.4 run loop gains a new step 5.5 (`_preflight_external_deps`) between `ensure_state_table` (step 5, hard) and the dispatch loop (step 6).
- `dry_run` returns both `plan` and `extra_deps` in the RunSummary so customers can see what would dispatch + what they need on disk.
**Plan edits applied 2026-05-15**:
- ✅ §4.2 docstring: `layers=`/`datasets=` clarified — filtered-out deps are external prerequisites, verified by preflight.
- ✅ §4.4 run loop: step 2 calls `resolve_plan(...)` instead of three separate `filter_enabled_*` calls; new step 5.5 calls `_preflight_external_deps(...)`.
- ✅ §4.7 bullet split: `MissingDependencyError` (logical) and `PrerequisiteError` (data) documented as distinct error classes with distinct remediation paths.
- ✅ §4.1 file layout: `ExternalDep` + `MissingDependencyError` + `PrerequisiteError` + `_preflight_external_deps` added to `runtime.py` listing.
- ✅ §5 step-by-step: `runtime.py` task gains the new types + 3 preflight tests; `__init__.py` task gains the `resolve_plan` topology + 2 layer-filter tests + 1 missing-dep test.
- ✅ Acceptance criteria: new "Layer/dataset filter — intra-plan vs extra-plan dependency split" item with three test specs.
**Staleness — out of scope (deferred)**: extra-plan deps that exist on disk but are stale (e.g. bronze last run 3 weeks ago) are NOT detected by `tableExists()`. Tracked as a follow-up (potential P1.X): read `fusion_bundle_state.last_run_at` per extra-plan dep, emit WARN if older than configurable `max_dep_age_days` threshold. Default behavior: no failure, operator visibility only. Rationale for deferral: depends on state-table contract being live-verified (P1.5α-fix3); threshold belongs in bundle.yaml as policy.
**Size**: M — ~30 LOC of `resolve_plan` logic + ~15 LOC of `_preflight_external_deps` + 6 unit tests across two files + the plan edits already applied. ~1h.
**Depends on**: P1.5α-fix1 (Option C cascade refactor) for the cascade-correctness foundation; P1.5α-fix3 (state-table split contract) for the dispatch-order pattern (`ensure_state_table` → `_preflight_external_deps` → loop). Nothing else.
**Done**:
- ✅ PLAN §4.2, §4.4, §4.7, §4.1, §5, acceptance criteria all updated (2026-05-15).
- ✅ Implementation: `ExternalDep` dataclass + `MissingDependencyError(OrchestratorConfigError)` + `PrerequisiteError(OrchestratorConfigError)` + `resolve_plan` + `_preflight_external_deps` shipped in commits `c6f4ace` (Phase 2) + `f113fb2` (Phase 3). Run-loop dispatch order is `ensure_state_table` → `_preflight_external_deps` → loop (`orchestrator/__init__.py:469-475`).
- ✅ `TestLayerFilterPreflight.test_layers_gold_with_prereqs_present_dispatches_only_gold` — full run with `layers=['gold']`, fake catalog pre-seeded with all extra-plan dep table paths → preflight passes silently → only 3 gold marts dispatch; bronze + silver builders never invoked.
- ✅ `TestLayerFilterPreflight.test_layers_gold_with_missing_prereq_raises_prerequisite_error` — same setup with empty fake catalog → `PrerequisiteError` raised with missing-table list + `--datasets`/`--layers` redirect hint; `_execute_node` patched and `assert_not_called()`.
- ✅ `TestResolvePlan.test_inplan_consumer_with_unknown_dependency_raises_missing_dependency` — registry-inconsistency guardrail (Branch B of `_check_dep_exists_or_raise` at `__init__.py:168`). Patches `GOLD_MARTS["supplier_spend"]` with `depends_on_bronze=("nonexistent_pvo",)`; asserts `MissingDependencyError` names the missing dep AND is NOT a `PrerequisiteError` (load-bearing — bad reference must NOT leak to disk-state-checking).
- ✅ `TestResolvePlan.test_typo_in_dim_raises_missing_dependency` — Branch A coverage (bundle.yaml typo → unknown REQUESTED name). Distinct contract from Branch B above; both branches keep their own test.

**Remaining gate for `[x]` flip**:
- **Live evidence**: `aidp-fusion-bundle run --inline --mode seed --layers gold` against `fusion_bundle_dev` (after a full seed run materialized bronze/silver) producing a RunSummary with only gold marts dispatched. Blocked on BICC credential refresh — same blocker as TC26 full happy-path.

### `[x]` P1.5α-fix5 — Plan-doc nomenclature: `password_ref` → `password` (closed 2026-05-15)
**Why**: Two spots in the canonical PLAN still referred to `fusion.password_ref` even though the Pydantic schema field is `fusion.password` (`scripts/.../schema/bundle.py:73`) and §4.4's pseudocode + §4.9's resolver helper already use `bundle.fusion.password`:
- Line 147 — §3.3 bundle-config table row.
- Line 903 — §6 open-question "How does the orchestrator obtain BICC credentials?" answer.
`password_ref` was likely an early draft name from before the unified-sigil pattern (one `str` field accepting literal / `${vault:OCID}` / `${env:VAR}` via `_resolve_password()`) was adopted. The stale references would have misled implementers — `bundle.fusion.password_ref` raises `AttributeError`, but since the references were in prose tables and not in code, the bug stayed dormant.
**Fix applied 2026-05-15** (plan-only, no code change):
- Line 147 — replaced `password_ref` with `password` and added a description of the three accepted value shapes (literal, `${vault:OCID}`, `${env:VAR}`), noting that all dispatch through `_resolve_password(...)` to a `pydantic.SecretStr`.
- Line 903 — rewrote the open-question answer to reference `fusion.password` with the sigil dispatch, explicitly calling out "there is no separate `password_ref` field — earlier plan drafts referred to one, but the schema uses the unified name."
**Size**: XS — two single-paragraph edits. ~5 min.
**Depends on**: nothing.
**Accept**:
- ✅ `grep password_ref` against the canonical PLAN returns only the explanatory "there is no separate `password_ref` field" sentences (deliberate).
- ✅ Line 147 + line 903 align with §4.4 pseudocode (`bundle.fusion.password`) and §4.9 resolver semantics.

### `[x]` P1.5α-fix6 — CLI exit-2 contract via `OrchestratorConfigError` marker (shipped 2026-05-17)
**Why**: the §4.5 `_run_inline` pseudocode caught only `NotImplementedError`, but the exit-code table said exit-2 covers "config error, NotImplementedError, or unsupported execution path." After P1.5α-fix2 (mode-validation `ValueError`), §4.4a (`BundleLoadError`), and P1.5α-fix4 (`MissingDependencyError` + `PrerequisiteError`) landed, the catch list became dangerously incomplete — any of those four raising would propagate as a raw Python traceback to the user instead of a clean exit-2 with a redacted message.
**Approach** — marker base class pattern (preferred over flat enumeration):
- Define `OrchestratorConfigError(Exception)` in `orchestrator/runtime.py` as a marker for "user-facing config / pre-dispatch error; CLI prints `str(exc)` and exits 2 without traceback."
- All existing user-facing config errors inherit from it:
  - `BundleLoadError(OrchestratorConfigError)` — was `(Exception)`.
  - `UnsupportedModeError(OrchestratorConfigError, ValueError)` — new (P1.5α-fix2 had said "raise `ValueError(...)`"); multiple-inherits `ValueError` so legacy callers that catch `ValueError` still work.
  - `MissingDependencyError(OrchestratorConfigError)` — was `(Exception)` per P1.5α-fix4.
  - `PrerequisiteError(OrchestratorConfigError)` — was `(Exception)` per P1.5α-fix4.
- `_run_inline` catches `(OrchestratorConfigError, NotImplementedError)`. Single catch site; new error classes just inherit and the CLI never changes.
- Each subclass's `__str__` must be self-explanatory — the CLI prints `str(exc)` directly with no extra framing. PrerequisiteError already includes redirect text; UnsupportedModeError lists valid modes + retired-alias hint; BundleLoadError names the missing env var; MissingDependencyError points at bundle.yaml / registry.
**Why marker over flat-except-list**:
- Source of truth is the class hierarchy, not the CLI catch tuple. Adding a new exit-2 error type is a one-line subclass declaration; the CLI doesn't change.
- Canonical Python idiom (`OSError`, `LookupError`, Click's `UsageError`).
- Lint test catches accidental `class XError(Exception)` regressions at PR time.
**Plan edits applied 2026-05-15**:
- ✅ §4.5 `_run_inline` pseudocode rewritten to catch `(OrchestratorConfigError, NotImplementedError)` with a per-class explanation comment.
- ✅ §4.5 exit-code table expanded — `2` now lists all six covered cases (BundleLoadError, UnsupportedModeError, MissingDependencyError, PrerequisiteError, NotImplementedError, unsupported execution path).
- ✅ §4.4a `BundleLoadError` definition: inherits from `OrchestratorConfigError`; marker class is defined just above with docstring explaining the contract.
- ✅ §4.1 file layout: `runtime.py` listing adds `OrchestratorConfigError` + `BundleLoadError` + `UnsupportedModeError` + `MissingDependencyError` + `PrerequisiteError` + `ExternalDep` + `_preflight_external_deps`.
- ✅ §5 step-by-step `runtime.py` task: ~1 new marker-class lint test added (1 test).
- ✅ Acceptance criteria: new "Exit-2 contract via `OrchestratorConfigError` marker" item with parametrized test (5 error classes), no-traceback assertion, "bug propagates" counter-test, marker-subclass lint.
**Implementation TODOs for P1.5α-fix2 + P1.5α-fix4** (now retroactively constrained by this fix):
- P1.5α-fix2's `UnsupportedModeError`: multi-inherit `(OrchestratorConfigError, ValueError)` — orchestrator entry guard raises it; CLI catches via marker.
- P1.5α-fix4's `MissingDependencyError` and `PrerequisiteError`: both inherit from `OrchestratorConfigError`.
**Size**: XS — ~10 LOC (marker class + 4 inheritance edits) + 1 lint test + 1 parametrized test in `test_commands.py`. ~30 min.
**Depends on**: P1.5α-fix2 (UnsupportedModeError implementation), P1.5α-fix4 (MissingDependencyError + PrerequisiteError implementations). Plan ordering: this fix's plan edits land first (now); the four subclass `(OrchestratorConfigError, ...)` lines land in code when fix2 + fix4 are implemented.
**Done**:
- ✅ Plan edits applied (above).
- ✅ DECISION doc — not needed; small enough that the BACKLOG entry + plan comments are the contract. `errors.py` contains zero `DECISION_` / `DESIGN_` / `RESEARCH_` filename references (verified by `grep -n "DECISION\|DESIGN\|RESEARCH" scripts/.../orchestrator/errors.py` → no output). Fix6 has no audit-trail dependency on P1.5α-fix11.
- ✅ Implementation shipped in commit `c6f4ace` (Phase 2 — marker class hierarchy in `errors.py:17-93`) + `7f57d38` (Phase 5 — CLI catch at `commands/run.py:123`).
- ✅ Marker class + 6 concrete subclasses (`BundleLoadError`, `BundleVersionMismatchError(BundleLoadError)`, `UnsupportedModeError(OrchestratorConfigError, ValueError)`, `MissingDependencyError`, `PrerequisiteError`, `CredentialResolutionError`) all inherit `OrchestratorConfigError` directly or transitively.
- ✅ `_run_inline` catches `(OrchestratorConfigError, NotImplementedError)` — single catch site, new error classes just inherit and the CLI never changes.
- ✅ 5-case parametrized exit-2 test (`test_run_inline_exits_2_on_orchestrator_config_error`) covers `BundleLoadError` / `UnsupportedModeError` / `MissingDependencyError` / `CredentialResolutionError` / `PrerequisiteError` — each raised from a patched `orchestrator.run`, exit 2 + message printed + no traceback.
- ✅ `test_run_inline_exits_2_on_not_implemented` — `NotImplementedError` case (the explicit second leg of the CLI catch).
- ✅ `test_run_inline_propagates_non_config_bugs_with_traceback` — counter-test: bare `RuntimeError` from `orchestrator.run` does NOT silently exit 2; it propagates via `result.exception` so the operator gets a real traceback. Guards against future `except Exception` broadening.
- ✅ `TestExceptionHierarchy.test_subclass_of_orchestrator_config_error` (`test_orchestrator_runtime.py`) parametrized over 6 cases (`BundleLoadError`, `BundleVersionMismatchError`, `MissingDependencyError`, `CredentialResolutionError`, `PrerequisiteError`, `UnsupportedModeError`) — every direct subclass asserted via `issubclass(cls, OrchestratorConfigError)`.
- ✅ `test_every_public_error_class_inherits_marker` — self-maintaining lint that loops `errors.__all__` and asserts each non-marker class has `OrchestratorConfigError` in MRO. New error classes added to `__all__` are automatically subject to the contract.

### `[~]` P1.5α-fix7 — CLI wiring: thread `bundle_path`, pass `datasets=None` by default (impl + tests shipped 2026-05-17; awaiting live evidence for `[x]`)
**Why**: the §4.5 `_run_inline` pseudocode had three coupled bugs the reviewer caught:
1. **`bundle_path` not threaded.** Pseudocode signature `_run_inline(bundle_data, mode, dataset_ids)` took the parsed YAML *dict*, but `orchestrator.run(bundle_path=...)` (§4.2 public API) needs the *Path*. The orchestrator re-reads the YAML internally because `_render_env_vars` (§4.4a) must run on raw text BEFORE Pydantic validation. Passing a parsed dict would skip env-var rendering entirely.
2. **Default `datasets=` is over-restrictive bronze-only.** `commands/run.py:47` calls `_resolve_datasets(bundle_data, datasets)` which, when `--datasets` is omitted, returns the full list of enabled `datasets[*].id` from bundle.yaml — those are BICC PVO names (`ap_invoices`, `gl_period_balances`, etc.), all bronze. Silver dim names (`dim_supplier`, `dim_account`) and gold mart names (`supplier_spend`, `ap_aging`) are NOT in `bundle.datasets[]`. Passing that list as `datasets=` to the orchestrator filters silver + gold out. **Worst-kind-of-bug**: `aidp-fusion-bundle run --inline --mode seed` (no `--datasets`) returns exit 0, RunSummary shows 11 bronze success rows, customer thinks everything materialized — silver + gold never dispatched.
3. **`--datasets foo,bar` should pass raw to the orchestrator** for cross-layer registry classification, not get pre-resolved. `--datasets ap_aging` (gold mart name) and `--datasets dim_supplier` (silver dim name) must work — the orchestrator's `resolve_plan` (per P1.5α-fix4) classifies user-named identifiers across all three registries.
**Approach** — apply the reviewer's prescription plus the natural cleanups that fall out:
- **Thread `Path`, not parsed dict.** `_run_inline(bundle_path: Path, mode: str, datasets: list[str] | None, console)` is the new signature. `commands/run.py:run` passes `bundle_path` directly; `orchestrator.run` re-reads the YAML.
- **`datasets=None` is the documented "no filter" sentinel.** CLI parses `--datasets` via inline CSV split: omitted → `None`, present → `[s.strip() for s in datasets.split(",") if s.strip()]`. Raw list passes through unchanged.
- **Inline `_resolve_datasets`, don't rename.** After validation moves to `resolve_plan` (P1.5α-fix4), the helper is a two-line CSV split with no logic. Keeping a function for that is over-engineering; future wildcards/expansion is YAGNI. The two-line expression at the call site reads cleanly.
- **Remove dead-code `if not requested_ids: return 1` branch.** After the fix, `datasets` is either `None` or a non-empty validated list. Empty-plan is an orchestrator concern (returns `RunSummary.empty(...)` → exit 0); typo'd dataset names raise `MissingDependencyError` (exit 2 via `OrchestratorConfigError` marker per P1.5α-fix6).
**Plan edits applied 2026-05-15**:
- ✅ §4.5: pseudocode rewritten with new `_run_inline` signature; CLI flow (`commands/run.py:run` body) added showing the inline CSV split, conditional `dataset_filter` construction, removed `_resolve_datasets` call.
- ✅ Acceptance criteria: new "CLI wires `bundle_path` and `datasets` correctly" item with two test specs (`test_default_inline_passes_bundle_path_and_datasets_none` + `test_inline_passes_datasets_csv_split_raw` with whitespace + empty-segment cases).
**Done**:
- ✅ PLAN §4.5 pseudocode + acceptance criteria updated (2026-05-15).
- ✅ `commands/run.py:85-88`: inline CSV split with whitespace trim + empty-segment drop. `_resolve_datasets` helper deleted; dead-code `if not requested_ids: return 1` branch removed.
- ✅ `_run_inline(bundle_path: Path, mode: str, datasets: list[str] | None, console)` — new signature threads `Path` (not parsed dict) so `_render_env_vars` runs on raw YAML text before Pydantic validation, per §4.4a.
- ✅ `_run_via_aidp_dispatch` signature also takes `bundle_path: Path` + `datasets: list[str] | None` consistently.
- ✅ `datasets=None` is the documented no-filter sentinel; raw user-typed list passes through to `orchestrator.run` unchanged (cross-layer registry classification happens in `resolve_plan`, per P1.5α-fix4 / P1.5α-fix12).
- ✅ `TestRun.test_run_inline_invokes_orchestrator_run` (`test_commands.py:170`) — verifies the call shape: `bundle_path` is a `Path`, `mode="seed"`, `datasets=None` when `--datasets` is omitted. Covers the BACKLOG-spec contract `test_default_inline_passes_bundle_path_and_datasets_none` under the shipped name.
- ✅ `TestRun.test_run_inline_passes_datasets_csv_as_raw_list` (`test_commands.py:202`) — verifies `--datasets " ap_aging , dim_supplier ,,"` parses to `["ap_aging", "dim_supplier"]` (whitespace trimmed, empty segments dropped) and threads as a raw list. Covers the BACKLOG-spec contract `test_inline_passes_datasets_csv_split_raw` under the shipped name.

**Remaining gate for `[x]` flip**:
- **Live evidence (TC26)**: `aidp-fusion-bundle run --inline --mode seed` (no `--datasets`) on `fusion_bundle_dev` produces a RunSummary with all 11 bronze + 3 silver + 3 gold success rows (the actual bug Bug 2 would have hidden — silver/gold rows MUST be present). Blocked on BICC credential refresh — same blocker as fix4's live-evidence gate.

### `[~]` P1.5α-fix9 — Module retrofit: `run_id` kwarg + `<layer>_run_id` audit column (impl + tests shipped 2026-05-17 in commit `2df8cc3`; awaiting live evidence for `[x]`)
**Why**: PLAN §3.1 widens every silver/gold `build()` signature to accept `run_id: str | None = None`, and §3.5a adds `silver_run_id` / `gold_run_id` audit columns. PLAN §4.4 `_execute_node` calls `node.builder(spark, paths=paths, run_id=run_id)` — but the six shipped modules' live signatures don't accept `run_id` today. Without this retrofit, the first silver/gold dispatch in P1.5α will TypeError on `unexpected keyword argument 'run_id'`. The old §8 "Modules untouched" acceptance criterion has been replaced (in-plan) by an explicit Module-retrofit criterion — this entry tracks the mechanical work.
**Files touched** (6 modules + their tests):
- `scripts/.../dimensions/dim_supplier.py` (build + SQL builder + `SOURCE_BRONZE_TABLE` consumers)
- `scripts/.../dimensions/dim_account.py`
- `scripts/.../dimensions/dim_calendar.py`
- `scripts/.../transforms/gold/supplier_spend.py`
- `scripts/.../transforms/gold/gl_balance.py`
- `scripts/.../transforms/gold/ap_aging.py`
- Paired unit test files in `tests/unit/test_<module>.py` (column-list expectations + new `run_id` parametrized tests)
**Per-module change** (mechanical, ~10 LOC + 1 test edit each):
1. Add `run_id: str | None = None` keyword-only kwarg to `build()` and `build_<mart>_sql()` signatures.
2. In the SQL builder, append `, {run_id_sql} AS <layer>_run_id` to the SELECT list — where `run_id_sql = f"'{run_id}'"` if set, `"NULL"` otherwise.
3. Update existing column-list assertion tests to include the new column.
4. Add `test_<module>_emits_layer_run_id_when_set` + `test_<module>_emits_null_layer_run_id_when_unset` per-module.
**Size**: M — ~60 LOC of code + ~12 new tests + ~6 existing-test edits. ~1h total.
**Depends on**: §3.1 / §3.5a / §4.4 (B3) — landed in plan, awaiting implementation in the same commit as `orchestrator/__init__.py`.
**Done**:
- ✅ All 6 shipped silver/gold modules (`dim_supplier`, `dim_account`, `dim_calendar`, `supplier_spend`, `gl_balance`, `ap_aging`) accept `run_id: str | None = None` on both `build()` and `build_<mart>_sql()` without TypeError. Per-module `_run_id_audit_sql(run_id)` helper emits `'<run_id>' AS <layer>_run_id` when set, `NULL AS <layer>_run_id` when not.
- ✅ `tests/unit/test_module_run_id_audit.py` — dedicated test file with 18+ tests covering: `test_with_run_id_embeds_literal` (6 modules) + `test_without_run_id_emits_null` (6 modules) + `test_build_signature_has_run_id` (per module). Plus `test_uuid_run_id_embeds_safely` (parametrized) and `test_quote_in_run_id_is_escaped` — SQL-injection-safety guards via run_id.
- ✅ All existing column-list expectation tests in `test_<module>.py` updated to include the new audit column; no other test regresses (496/496 unit tests pass).
- ✅ CLAUDE.md `"Audit columns are non-negotiable"` rule satisfied — bronze (`_extract_ts` etc.), silver (`silver_run_id`), gold (`gold_run_id`) all listed.
- ✅ SOX-trail JOIN silver↔state validated live for `dim_calendar` (4018 rows) in TC26 redacted commits `7889e64` + `35aa5ec`.

**Remaining gate for `[x]` flip**:
- **Live evidence**: `SELECT silver_run_id, gold_run_id FROM silver.dim_supplier UNION ... FROM gold.ap_aging` returns non-NULL run_ids matching `fusion_bundle_state.run_id` for every row across **all 6 shipped modules** (TC26 captured `dim_calendar` only; the rest are blocked on BICC credential refresh — same blocker as fix4 + fix7).

**Cross-ref**: §3.1 (signatures), §3.5a (audit-col contract), §4.4 (dispatch threading), §8 module-retrofit acceptance criterion (supersedes "Modules untouched"). Shipped in commit `2df8cc3` (P1.5α Phase 4 — module retrofit).

### `[ ]` P1.5α-fix10 — Move `_LITERAL_WARN_EMITTED` flag out of module-level state (Blocker-1.3 follow-up, 2026-05-17)
**Why**: `_resolve_password` in `orchestrator/runtime.py` uses a module-level `bool` flag to ensure the literal-password WARN fires exactly once per run. This is correct for α (one process, one orchestrator run), but it's brittle for two future scenarios:
- **Long-running processes** (REST jobRuns dispatch via P1.5ε, or Airflow-style scheduling): one process executes many runs back-to-back; the flag never resets after the first run, so subsequent runs with literal passwords surface zero WARNs.
- **Parallel orchestrator invocations** in the same process (post-P3.10 parallel execution): two runs share the module-level state; one run silences the other's WARN.

Today an autouse pytest fixture handles test isolation (see PLAN §4.9 "Test isolation for the literal-warn flag"); production correctness is preserved by the one-process-one-run assumption that holds for the `--inline` and laptop-terminal-REST surfaces.

**When to revisit**: any of (a) P1.5ε REST dispatch becomes a long-running process pattern, (b) P3.10 parallel execution lands, (c) a contributor reports the post-first-run silence as a real customer issue.

**Fix sketch**: introduce a `RunContext` (or extend `RunSummary` with builder-pattern state) that carries per-run state including `warned_about_literal: bool`. Thread through `_resolve_password(value, *, ctx: RunContext)`. The module-level flag goes away.

**Size**: S — ~30 LOC orchestrator refactor + signature change at 2 call sites + remove autouse fixture (replaced by per-test `RunContext()` construction). ~1h.
**Depends on**: nothing today; revisit when triggered.
**Accept**:
- `_resolve_password` no longer references module-level state.
- The autouse `_reset_literal_warn_flag` fixture in `tests/unit/conftest.py` is removed (each test instantiates its own `RunContext`).
- Two new tests: (a) two `RunContext` instances in the same process produce independent WARN counts; (b) long-lived process with three sequential `RunContext`s + literal passwords emits exactly 3 WARNs total (one per context).
**Cross-ref**: PLAN §4.9 (`_LITERAL_WARN_EMITTED` definition + autouse fixture); R3 (the WARN-once requirement that motivated the flag).

### `[x]` P1.5α-fix11 — Scrub references to local-only working notes (shipped 2026-05-17)
**Why**: PLAN / DECISION / DESIGN / RESEARCH / BOOTSTRAP files at the repo root were originally written as committable working notes. The team chose to keep them local-only (moved to `dev/`, gitignored) — they contain step-level identifiers and internal commentary not suitable for the public repo. That left tracked code/docs with ~40 references to filenames that don't exist in the public repo — broken cross-links for any contributor browsing the source.

**Done**:
- ✅ All public-facing references to local-only working-note files scrubbed from tracked code, docstrings, test prose, live-evidence docs, and BACKLOG entries.
- ✅ Operator-facing error messages (e.g. the retired-mode hint in `UnsupportedModeError`) no longer cite local-only filenames; the substantive hints stay.
- ✅ Production docstrings (`state.py`, `runtime.py`, `__init__.py`) keep their inline contract descriptions but drop citation filenames.
- ✅ `commands/run.py` REST-stub message rewritten to describe the status without citing internal-only research notes.
- ✅ Live-evidence docs under `tests/live/` (TC22/TC23/TC26) updated to describe the internal bootstrap / research procedure without filename links.
- ✅ BACKLOG entries' Why/Done blocks dropped citation filenames; the substantive rationale stays inline in each entry.
- ✅ `dev/` was added to `.gitignore` in a prior step; the local-only files live there.
- ✅ Post-scrub verification: the canonical `git grep -nE '<file-pattern set>' -- scripts/ tests/ BACKLOG.md CLAUDE.md CONTRIBUTING.md README.md` returns 0 hits.
- ✅ BACKLOG fix2 + fix3 entries flipped from `[~]` → `[x]` in the same commit (audit-trail gate closed by reference scrub). fix4 stays `[~]` (live-evidence gate still open). fix6 was independent and shipped at `[x]` in its own close-out.

### `[x]` P1.5α-fix12 — Validate `--datasets` / `--layers` filter inputs against bundle plan (post-α blocking bug, shipped 2026-05-17)
**Why**: `resolve_plan` at `orchestrator/__init__.py:126-135` filtered names already in `all_specs` via `_matches_filter` — but requested `datasets=` names absent from the bundle plan were never validated or rejected. Impact: `aidp-fusion-bundle run --inline --datasets ap_invoies` (typo of `ap_invoices`) returned an empty plan and exited 0 via `RunSummary.empty(...)` — an operator could believe a scoped refresh ran while no table changed. This violated the canonical PLAN (typoed filter names hard-fail) and `commands/run.py:78-84` (CLI docstring promises `MissingDependencyError` for unknown names).
**Done**:
- ✅ `resolve_plan` validates `set(datasets) - set(all_specs)` BEFORE the existing `_matches_filter` loop; raises `MissingDependencyError` listing the unknown name(s) + available bundle names (`__init__.py` step 1a).
- ✅ Same guardrail for `layers=`: `set(layers) - _VALID_LAYERS` → `MissingDependencyError` listing offenders + valid layer enum (`{"bronze", "silver", "gold"}`).
- ✅ `_VALID_LAYERS` imported from `.registry` (already exported at `registry.py:51, 293`); no new symbol introduced.
- ✅ `TestResolvePlan.test_typoed_datasets_filter_raises_missing_dependency` — `datasets=["ap_invoies"]` → `MissingDependencyError` with the typo + available names listed.
- ✅ `TestResolvePlan.test_typoed_datasets_filter_with_mixed_valid_and_invalid` — `["dim_supplier", "bogus_name_1", "bogus_name_2"]` → both unknown names surface (presence of a valid name doesn't excuse invalids).
- ✅ `TestResolvePlan.test_typoed_layers_filter_raises_missing_dependency` — `layers=["gols"]` → `MissingDependencyError` with the typo + valid layer enum.
- ✅ `TestRun.test_run_inline_typoed_datasets_exits_2_no_traceback` (CLI integration) — `aidp-fusion-bundle run --inline --mode seed --datasets ap_invoies` exits 2, output contains `"ap_invoies"`, no traceback (the OrchestratorConfigError marker catch works for this case).
- ✅ Existing `test_datasets_filter_targets_specific_names` + `test_layer_filter_creates_extra_deps` unchanged — happy-path filter behavior is preserved.
**Audit-trail status**: no DECISION-doc dependency. `errors.py` doesn't reference any working-note file; the fix is a pure validation tightening. Flips directly to `[x]` (no P1.5α-fix11 gate).
**Cross-ref**: reviewer-flagged blocking bug at `__init__.py:126-135`; the typoed-filter contract; `commands/run.py:78-84` CLI docstring.

### `[x]` P1.5α-fix13 — Wire `--layers` through the CLI (shipped 2026-05-21)

**Done**:
- ✅ `cli.py:117` gains `@click.option("--layers", default=None, …)` mirroring `--datasets`.
- ✅ `cli.py:121` callback signature accepts `layers: str | None` and threads to `commands/run.py::run`.
- ✅ `commands/run.py::run` accepts `layers` parameter, parses CSV the same shape as `datasets`, threads to both `_run_inline` and `_run_via_aidp_dispatch`.
- ✅ `_run_inline` passes `layers=layers` to `orchestrator.run(...)`. `_run_via_aidp_dispatch` plumbs the parameter through its (stub) signature so future P1.5ε wiring inherits it.
- ✅ 2 new tests in `tests/unit/test_commands.py::TestRun`:
  - `test_run_inline_with_layers_filter_passes_through` — `--inline --layers gold` reaches `orchestrator.run` with `layers=["gold"]`, `datasets=None`.
  - `test_run_inline_with_layers_and_datasets_combined` — both filters mutually compatible (`--layers bronze, silver --datasets ap_invoices` → both reach the orchestrator).
- ✅ No new validation in the CLI layer — `resolve_plan` already handles unknown layer names via `MissingDependencyError` (P1.5α-fix12).

**Cross-ref**: closes the P1.5α-fix4 live-evidence gate (a `--layers gold` run against `fusion_bundle_dev` is now command-line addressable; one live dispatch closes both fix4 and fix13).

---

**Original spec follows (preserved for the rationale):**

**Why**: `orchestrator.run(...)` accepts `layers=` (`orchestrator/__init__.py:102, 421, 440`), `resolve_plan` validates `--layers` typos via P1.5α-fix12 (`__init__.py:140-146`), `runtime.py:325` cites `--datasets / --layers` in its `PrerequisiteError` redirect, and `test_orchestrator_run.py:375, 594` exercise `--layers` end-to-end. But `cli.py:111-132` declares only `--mode` / `--datasets` / `--inline` — and `commands/run.py:117-121` calls `orchestrator.run(..., datasets=datasets)` with no `layers=` parameter at all. The path `cli.py → commands/run.py → orchestrator.run(...)` hardcodes `layers=None` end-to-end. Today the only way to do a "rebuild gold from existing bronze+silver" run is to import `orchestrator.run` from Python, defeating the "CLI is the contract" principle (CLAUDE.md).

**Repro**: `aidp-fusion-bundle run --inline --mode seed --layers gold` → `Error: No such option: --layers` (Click rejects unknown options before reaching `commands/run.py`). This is exactly why P1.5α-fix4's `[x]` flip is stuck (see that entry's §"Remaining gate"): the live-evidence command invokes a flag that doesn't exist.
**Size**: XS — one Click option + thread the value through two call sites + one unit test. ~30 min.
**Depends on**: P1.5α-fix12 (filter-input validation already lives in `resolve_plan`, so the new flag inherits the typo-guard for free).
**Accept**:
- `cli.py:117` gains `@click.option("--layers", default=None, help="Comma-separated layer names to filter (bronze, silver, gold). Mutually compatible with --datasets — both apply.")`. Type signature mirrors `--datasets` (CSV → `list[str] | None`).
- `commands/run.py:run(...)` accepts `layers: str | None = None`, parses the same way as `datasets` (CSV split + strip, empty → `None`), and threads to `_run_inline(...)` / `_run_via_aidp_dispatch(...)`.
- `_run_inline` passes `layers=layer_filter` to `orchestrator.run(...)`. No new validation in the CLI layer — `resolve_plan` already handles typos via `MissingDependencyError`.
- New test `TestRun.test_run_inline_with_layers_filter_passes_through` in `test_commands.py` (or `test_orchestrator_run.py`): `runner.invoke(["run", "--inline", "--mode", "seed", "--layers", "gold"])` reaches `orchestrator.run` with `layers=["gold"]` — assert via `mock.patch` on `orchestrator.run`.
- Existing `--datasets` tests unchanged.
- Live evidence — once shipped, run the P1.5α-fix4 gate command against `fusion_bundle_dev`: `aidp-fusion-bundle run --inline --mode seed --layers gold` produces a RunSummary with only gold marts dispatched. That single live run closes both fix4 and fix13 simultaneously.
**Cross-ref**: P1.5α-fix4 §"Remaining gate" (BACKLOG:299-300); P1.5α-fix12 (the typo-guard fix13 inherits); `cli.py:117`; `commands/run.py:40-97`; `orchestrator/__init__.py:102, 440`.

### `[x]` P1.5α-fix14 — `resolve_plan` rejects undeclared upstreams (shipped 2026-05-21)

**Done**:
- ✅ `resolve_plan` at `orchestrator/__init__.py:207-262` now distinguishes (A) declared-but-filtered (legitimate `ExternalDep`, preserved) from (B) never-declared-at-all (rejected). For each consumer-upstream pair, if `dep_name not in all_specs`, the offender is collected to `undeclared_deps`; after the consumer loop, a single consolidated `MissingDependencyError` is raised listing every offender with per-line remediation:
  ```
  bundle.yaml is missing N upstream declaration(s) — refusing to run with
  undeclared dependencies (which would silently rebuild from stale on-disk
  tables or trigger a misleading PrerequisiteError):
    • bronze 'ap_invoices' (required by 'ap_aging') — add it to bundle.datasets
    • silver 'dim_supplier' (required by 'supplier_spend') — add it to bundle.dimensions.build
  ```
- ✅ 5 new `TestResolvePlan` unit tests in `tests/unit/test_orchestrator_run.py` covering all three undeclared-upstream call sites + case-A regression + multi-offender accumulation:
  - `test_undeclared_bronze_upstream_raises_missing_dependency` — `GoldMartSpec.depends_on_bronze` call site
  - `test_undeclared_silver_upstream_raises_missing_dependency` — `GoldMartSpec.depends_on_silver` call site (reviewer catch #1)
  - `test_undeclared_bronze_upstream_for_silver_dim_raises_missing_dependency` — `SilverDimSpec.depends_on_bronze` call site (reviewer catch #2 — a future refactor could break this branch alone; without this test the regression would slip through)
  - `test_declared_bronze_filtered_out_becomes_external_dep` — locks in case (A) preservation (the `--layers gold` `ExternalDep` path must NOT break)
  - `test_multiple_undeclared_upstreams_accumulated_in_one_error` — accumulate-vs-fail-fast contract
- ✅ Full unit suite green: 503 passed (was 498, +5 regression tests).

**Cross-ref**: P1.5α-fix15 (depends on fix14 — shares the `all_specs` construction site) can now proceed against a clean upstream-declaration contract.

**Original spec follows (preserved for the rationale + reviewer-flagged failure modes):**

**Why**: `resolve_plan` treats every upstream not in `in_plan_names` as an `ExternalDep` to be preflighted on disk (`orchestrator/__init__.py:207-222`) — but `in_plan_names` is derived from `all_specs`, which only contains names declared in `bundle.{datasets, dimensions.build, gold.marts}`. The implementation conflates two scenarios:
- **(A) Declared but filtered out**: operator declared the upstream in bundle.yaml, then excluded it for this run via `--datasets`/`--layers`. Legitimate `ExternalDep` — preflight against the on-disk Delta table is the correct contract (P1.5α-fix4's design intent at BACKLOG:271-273).
- **(B) Never declared at all**: operator forgot to declare the upstream in bundle.yaml. Today this becomes an `ExternalDep` and silently passes preflight whenever a stale Delta table happens to exist on disk — gold rebuilds from undeclared, possibly-weeks-old bronze with **no warning**.

Concrete failure modes (live-confirmed by reviewer):

```yaml
# Bundle A — minimal misconfig
datasets: []
dimensions:
  build: []
gold:
  marts: [supplier_spend]   # depends on ap_invoices (bronze) + dim_supplier (silver)
```
`resolve_plan` returns plan `['supplier_spend']` with `extra_deps=[ap_invoices, dim_supplier]`. Two paths from here, both broken:

- **Stale tables on disk**: `_preflight_external_deps` passes via `tableExists(...)`, gold rebuilds from undeclared, possibly-weeks-old data. **Zero warnings, zero auditability.** Operator's intent — "I forgot to declare upstream, please refuse" — is silently inverted into "use whatever's lying around".
- **Tables missing on disk**: `PrerequisiteError` fires with redirect message "*include layer(s) ['bronze'] in --layers, OR run with --datasets ap_invoices first*" (`runtime.py:325`). But following that hint loops: `aidp-fusion-bundle run --inline --datasets ap_invoices` hits P1.5α-fix12's typo-guard at `__init__.py:129` and exits 2 with `--datasets contains name(s) not in the bundle plan: ['ap_invoices']`. The error message tells the operator to do something fix12 then explicitly rejects. Embarrassing UX loop — the two contracts contradict each other when the upstream is undeclared.

This is distinct from the deferred staleness concern at P1.5α-fix4 §"Staleness — out of scope" (BACKLOG:288), which covers declared-but-old bronze. fix14 closes the upstream-declaration gate; the staleness watermark is a separate follow-up.

**Approach**: In `resolve_plan`, before `_add_extra(...)` at `__init__.py:213, 218, 222`, check `if dep_name not in all_specs:` → `raise MissingDependencyError(...)`. Message names the consumer, the missing upstream, and tells the operator to declare it in `bundle.datasets` / `bundle.dimensions.build`. The existing `_check_dep_exists_or_raise` (lines 191-205) covers a different contract (registry consistency — "is this name knowable to the orchestrator?"); fix14 covers the bundle-declaration contract ("did the operator opt in?"). Both must pass.

**Size**: S — ~10 LOC of conditional in `resolve_plan` + 2 unit tests + message-shape decision (single error vs. accumulated list of all undeclared deps). ~1h.
**Depends on**: P1.5α-fix13 (need `--layers` wired to construct the test scenario for case A — declared-but-filtered — at the CLI layer); P1.5α-fix4 (the `ExternalDep` plumbing).
**Accept**:
- New branch in `resolve_plan` (`__init__.py:207-222`): for each consumer's upstream, if `dep_name not in all_specs`, raise `MissingDependencyError` with message naming consumer, upstream layer, upstream name, and the remediation (which bundle.yaml section to add it to). Accumulate across all consumers if multiple are missing — one error listing every offender beats N separate raises.
- Existing `ExternalDep` path preserved for the legitimate case: `dep_name in all_specs and dep_name not in in_plan_names`.
- `TestResolvePlan.test_undeclared_bronze_upstream_raises_missing_dependency` — `bundle.gold.marts=["ap_aging"]` + `bundle.datasets=[]` + no filter → `MissingDependencyError` naming `ap_invoices` AND the bundle section to add it to. Distinct from existing `test_typoed_datasets_filter_raises_missing_dependency` (P1.5α-fix12 — covers filter-input typos, not bundle-omission).
- `TestResolvePlan.test_declared_bronze_filtered_out_becomes_external_dep` — `bundle.datasets=[ap_invoices]` + `bundle.gold.marts=[ap_aging]` + `layers=["gold"]` → `ExternalDep("ap_invoices", "bronze", "ap_aging")` in the plan; no error. Locks in case (A) preservation.
- `TestResolvePlan.test_multiple_undeclared_upstreams_accumulated_in_one_error` — gold mart declares two bronze deps; neither in `bundle.datasets` → single `MissingDependencyError` names both. Operator shouldn't have to fix-rerun-fix-rerun.
**Cross-ref**: reviewer-flagged latent bug at `orchestrator/__init__.py:207-222`; P1.5α-fix4 §"Approach" (BACKLOG:271-273) — design intent of "extra-plan = filtered out", which fix14 enforces; P1.5α-fix4 §"Staleness" (BACKLOG:288) — separate deferred concern (declared-but-old, not undeclared); P1.5α-fix12 (`__init__.py:129`) — the typo-guard that closes the embarrassing loop in this entry's "Tables missing on disk" failure mode once fix14 declines the offending bundle upfront.

### `[x]` P1.5α-fix15 — Honor `DatasetSpec.enabled` + disabled-specific error wording (shipped 2026-05-21)

**Done** (Option A: honor the flag; grew beyond the original ~3 LOC to ~25 LOC across two error builders + 4-axis `undeclared_deps` tuple after plan-review rounds caught misleading-remediation gaps):

- ✅ `orchestrator/__init__.py:117-129` skips disabled datasets from `all_specs` and tracks their ids in a sibling `disabled_datasets: set[str]`.
- ✅ `undeclared_deps` widened to a 4-axis tuple `(consumer, consumer_layer, dep_layer, dep_name)` — the consumer's layer (derived via `_layer_for_spec(all_specs[consumer])`) is required for the disabled-state error wording to name the correct `bundle.yaml` section.
- ✅ **fix14's consumer-upstream error builder** (the consolidated raise at `__init__.py:280-305`) gets a two-branch `if dep_name in disabled_datasets:` check. Disabled branch emits: `"bronze 'ap_invoices' is disabled in bundle.datasets (required by 'supplier_spend') — set enabled: true or remove 'supplier_spend' from bundle.gold.marts"`. Plain undeclared branch (existing fix14 wording) is preserved for truly-missing names.
- ✅ **fix12's `--datasets` filter validator** (`__init__.py:131-170`) gets a parallel `disabled_in_filter` vs `truly_unknown` split. Disabled filter ids get their own message: `"--datasets references disabled name(s): ['ap_invoices']. Either set `enabled: true` in bundle.datasets, or remove them from --datasets."`
- ✅ 4 new `TestResolvePlan` tests in `tests/unit/test_orchestrator_run.py` covering all three disabled-state surfaces:
  - `test_disabled_dataset_excluded_from_plan` — happy path (disabled dataset absent from plan + extra_deps)
  - `test_disabled_dataset_with_gold_consumer_raises_disabled_specific_error` — gold-consumer wording (`bundle.gold.marts`); rejects misleading `"add it to bundle.datasets"`
  - `test_disabled_dataset_with_silver_consumer_raises_disabled_specific_error` — silver-consumer wording (`bundle.dimensions.build`); rejects wrong section `bundle.gold.marts` (reviewer catch — locks the `consumer_layer` derivation contract)
  - `test_datasets_filter_with_disabled_id_raises_disabled_specific_error` — filter-input wording; rejects generic `"not in the bundle plan"`
- ✅ 1 schema round-trip test in `tests/unit/test_bundle_schema.py::test_dataset_enabled_false_roundtrips_cleanly` — confirms `enabled: false` parses cleanly AND survives `model_dump → model_validate`. Without this, a silent schema regression would break fix15's honor-check at `resolve_plan`.
- ✅ All 5 disabled-state tests use **fully-explicit minimal YAML** (every `bundle.*` section listed, with empty `[]` where there's no consumer) — suppresses `DimensionsSpec.build` + `GoldSpec.marts` defaults that would otherwise inject undeclared upstreams and pollute error-message assertions for reasons unrelated to the disabled-state contract.
- ✅ Full unit suite green: **510 passed** (was 503 post-fix14, +7 regression tests).

**Cross-ref**: depends on P1.5α-fix14 (shares the `all_specs` construction site; fix14's error builder is the integration point for the disabled-specific wording branch). Reviewer caught two UX traps before code landed: (i) consumer-upstream needed `consumer_layer` axis so silver consumers get pointed at `bundle.dimensions.build` not `bundle.gold.marts`; (ii) filter-input path needed its own disabled-specific branch in fix12's validator — without it, `--datasets disabled-name` would dead-end with the misleading "edit bundle.yaml" message even though the entry IS in the bundle.

---

**Original spec follows (preserved for the rationale + reviewer-flagged dead-field bug):**

**Why**: `schema/bundle.py:104` declares `enabled: bool = True` on `DatasetSpec` — implying operators can opt a single bronze extract out of a run by setting `enabled: false`. But `orchestrator/__init__.py:118` builds `all_specs` by iterating `bundle.datasets` with **no `enabled` check**: every declared dataset becomes part of `in_plan_names` regardless. The field is dead weight that silently advertises a feature the orchestrator doesn't implement.

**Repro**:
```yaml
datasets:
  - id: ap_invoices
    enabled: false              # operator's intent: skip this bronze for now
gold:
  marts: [supplier_spend]
```
`resolve_plan(bundle, datasets=None, layers=None)` → plan still contains `ap_invoices`, BICC extract runs, bronze table is rebuilt. The operator's opt-out is silently ignored. Worse, there's no signal that the field was even read — no WARN, no validation error.

**Two acceptable resolutions** (pick one; defer to operator-experience):

(A) **Honor it**: in the `for ds in bundle.datasets` loop at `__init__.py:118`, skip entries where `ds.enabled is False`. Same treatment for `DimensionsSpec`/`GoldSpec` if/when they grow an `enabled` shape (they don't today — `dimensions.build` and `gold.marts` are `list[str]`, no per-entry knobs). Add a parametrized test in `test_orchestrator_run.py` confirming the disabled dataset never appears in the resolved plan. Operator gets a clean per-dataset opt-out without rewriting `--datasets` CSVs.

(B) **Remove it**: drop `enabled` from `DatasetSpec`. Add a `validate_legacy_enabled_field` model_validator that catches old bundles using the field and raises with a migration message ("use `--datasets <ids>` to scope a single run; bundle.yaml is the durable declaration of *what exists*, the CLI flag is the per-run filter"). Cleaner separation of "declared" vs "runnable today" — bundle.yaml stays declarative.

**Recommendation**: (A). The cost is ~3 LOC; the alternative requires a deprecation cycle and customer migrations for a feature that's already part of the schema contract. Honoring matches operator intuition (set it in YAML, it stays disabled across runs without remembering a CLI flag every time).

**Size**: XS — ~5 LOC orchestrator + 1 schema-validator test + 2 plan-resolution tests. ~30 min.
**Depends on**: P1.5α-fix14 (the all_specs construction loop is where both fixes intersect; fix14 lands first to establish the all_specs / in_plan_names contract cleanly).
**Accept (Option A)**:
- `orchestrator/__init__.py:118` loop becomes `for ds in bundle.datasets: if ds.enabled: all_specs[ds.id] = _resolve_bronze(ds.id)`. Disabled datasets never enter `all_specs`, so downstream consumers that depend on them trigger fix14's `MissingDependencyError` ("declared upstream `ap_invoices` is disabled — re-enable it or remove `supplier_spend` from `bundle.gold.marts`").
- New test `TestResolvePlan.test_disabled_dataset_excluded_from_plan` — bundle with one `enabled: true` and one `enabled: false` dataset → plan contains only the enabled id; the disabled id is absent from both `plan` and `extra_deps`.
- New test `TestResolvePlan.test_disabled_dataset_with_dependent_consumer_raises` — `ap_invoices.enabled: false` + `gold.marts: [supplier_spend]` (which depends on `ap_invoices`) → `MissingDependencyError`. Wording differentiates from "never declared" — names the disabled status explicitly so the operator knows to flip the flag, not add a new datasets entry.
- `test_bundle_schema.py` gets a round-trip test confirming `enabled: false` parses cleanly and the field survives a re-serialize.
**Cross-ref**: reviewer-flagged dead-field bug at `orchestrator/__init__.py:118` + `schema/bundle.py:104`; P1.5α-fix14 (shares the `all_specs` construction site).

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

**Status (2026-05-17)**: **Empirical probe complete end-to-end against `amitV2` / `playground` / `fusion_bundle_dev`.** All four phases of the §11 retry checklist pass (auth, upload, job submission, fetchOutput). Auth model CONFIRMED as OCI request signing — the §395 "load-bearing prerequisite" is satisfied. All implementer-facing schema corrections live in the internal REST-probe notes — read those notes as the source of truth for client code, NOT the `Schema facts` block below (some of which has been empirically falsified — see notes inline). Implementable now; gated only on P1.5α shipping the orchestrator notebook.

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
- **Output** *(empirically corrected 2026-05-17 — see probe doc §10.4–§10.6)*: `fetchOutput` requires body `{"outputKey": ""}` (empty string, **not** `"main"` as initially assumed). Response shape is `data[].type` + `data[].**value**` (NOT `content`). Only the `NOTEBOOK` type was observed; other enum values speculated here were not seen in practice. **`oidlUtils.notebook.exit()` is NOT a reliable surfacing primitive** — on the probed cluster the module was unavailable and the call raised. **The stdout-marker pattern (`AIDP_LIVE_TEST_RESULT_BEGIN/END`) is the cross-cluster-reliable channel**: the marker line surfaces inside the executed notebook's `cells[*].outputs[*].data["text/plain"]` strings, returned in `data[0].value`. Implementer: keep the dual-channel pattern (`try: notebook.exit(...); except: print(marker)`) and parse the embedded notebook on the REST side — see probe doc §10.8 for the final primitive signatures.
- **Auth** *(empirically CONFIRMED 2026-05-17)*: OCI request signing works against the data-plane endpoints. One signed `GET /workspaces` returned 200 with valid JSON; the full round-trip (upload → create job → submit run → poll → fetchOutput) completed cleanly using `oci.signer.Signer` (here via `oci raw-request` for the probe; production code uses `requests` with `auth=signer`). Data-plane endpoints under `/workspaces/{wk}/...` are still **not wrapped by the OCI Python SDK** — `AiDataPlatformClient` exposes control-plane methods only.
- **`datalake-tenant-id` header**: required only on `/notebook/api/sessions` and `/notebook/api/contents/{contentPath}` (the Jupyter passthrough); **not on `/jobs`, `/jobRuns`, or `fetchOutput`**. Origin of the value is undocumented; if the upload step needs it, probe.

**Implementation sketch**:
- Build an `aidp_rest` client module: `requests` with `auth=oci.signer.Signer(...)`, or alternatively shell out to `oci raw-request` (CLI does the signing). Resource-principal / instance-principal signers when running in-cloud.
- New file: `scripts/.../dispatch/aidp_rest.py` — `upload_notebook(path) → workspace_path`, `create_job(notebook_path, cluster_ref) → job_key`, `submit_run(job_key, parameters) → run_key`, `poll_run(run_key) → terminal_status`, `fetch_output(task_run_key) → RunSummary`.
- `commands/run.py:_run_via_aidp_dispatch()` becomes a real implementation that threads `bundle_path` + cluster reference from `aidp-deploy.config.json`.
- Add `notebook.exit(json.dumps(summary.to_dict()))` cell to `notebooks/run_orchestrator.ipynb` (1 LOC + 1 import).

**Size**: S–M (~½ to 1 day, down from M). Auth empirical work is DONE (probe doc); lion's share is now just the `aidp_rest` client wrapper.
**Depends on**: P1.5α shipped (notebook + orchestrator exist). The empirical-probe prerequisite is **satisfied** (see Status note above).
**Accept**:
- ~~Empirical evidence file `tests/live/TC28_rest_auth_probe.md` showing a signed request to AIDP returning 200 (not 401/403).~~ **Satisfied 2026-05-17** — evidence captured in the internal REST-probe notes §10.
- `aidp-fusion-bundle run --mode seed` (no `--inline`) against `fusion_bundle_dev` from a laptop terminal returns exit code 0 and prints the RunSummary. Live evidence at `tests/live/TC29_rest_dispatch.md`.
- Unit tests cover the four `aidp_rest` primitives with `responses`-mocked HTTP. Use the response shapes from probe doc §10 as fixture data (not the speculative shapes from this entry's pre-2026-05-17 schema-facts block).
- `_run_via_aidp_dispatch()` error message removed (function does real work now).
- Client-side validates body shape **before** sending `POST /jobs` (mandatory fields per probe doc §10.1: `name`, `path`, `maxConcurrentRuns`, plus task-level requirements). One malformed request trips the workflow CircuitBreaker for ~15 min — defense-in-depth here is load-bearing.

**File upstream issue if blocked**: if OCI signing turns out NOT to be the right scheme, OR if `datalake-tenant-id` is required on `/notebook/api/contents` and the origin is non-discoverable, file an issue with the AIDP team to get the auth-and-headers spec published in the `aiwap` doc tree (current gap: `swagger.json` has empty `securityDefinitions`).

### `[ ]` P1.Xb — Schema preflight before `CREATE OR REPLACE TABLE` *(orchestrator-evolution design item A — elevate to ship-with-α candidate)*
**Why**: Today each mart module validates its own kwargs and (in ap_aging's case) hard-gates on the currency column. But required bronze / silver column existence isn't checked uniformly — a missing column failures inside Spark with a cryptic `UNRESOLVED_COLUMN` analysis error. A unified preflight that runs before `spark.sql(CREATE OR REPLACE)` gives customers a clear, actionable error. Single biggest "Fusion release breaks us" insurance policy — DESIGN doc §2 argues for elevating to α-mandatory.
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

### `[ ]` P2.4a — `make test` default interpreter prefers `.venv/bin/python` when present (follow-up to P2.4, 2026-05-17)
**Why**: P2.4 shipped `Makefile` with `PYTHON ?= python` (`Makefile:6`). On macOS the default `python` is Homebrew's `/opt/homebrew/opt/python@3.13/libexec/bin/python` — a bare interpreter with no project dependencies. A fresh contributor cloning the repo and running `make test` (per `README.md:34` and `CONTRIBUTING.md:25`'s "canonical entry point" framing) hits `ModuleNotFoundError: No module named 'oracle_ai_data_platform_fusion_bundle'` unless they remembered to activate a venv first. The smoke test in this checkout reproduced exactly that: `make test` failed under bare Homebrew python; `make test PYTHON=.venv/bin/python` passed (496 tests).

**Repro**: `git clean -fdx && python -m venv .venv && .venv/bin/pip install -e '.[test]' && make test` (no `source .venv/bin/activate`). Result: `ModuleNotFoundError`. Documented "canonical" path doesn't actually work without an extra activation step the docs don't mandate.

**Two acceptable resolutions**:

(A) **Auto-detect a project-local venv** in the Makefile. Conditional default:
```makefile
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python)
```
Pros: `make test` "just works" from a fresh checkout after `pip install -e '.[test]'` regardless of activation. Matches the implicit contract of CONTRIBUTING.md's quick-start (which doesn't mention activation). Cons: a contributor with a non-`.venv`-named venv (e.g. `.env`, `venv`, conda) still has to override `PYTHON=` — same friction as today, but the docs now match. Mitigation: a single sentence in CONTRIBUTING.md naming the convention.

(B) **Update the docs to require activation**. README + CONTRIBUTING grow an explicit `source .venv/bin/activate` step before `make test`. Pros: zero Makefile change; convention is documented. Cons: more onboarding ceremony; the "canonical command works regardless of activation" framing in CONTRIBUTING.md:25 is then false (the whole point of P2.4) and would need to be rewritten.

**Recommendation**: (A). P2.4's headline benefit was "works regardless of shell PATH" — (A) extends that promise to "works regardless of shell activation state too", which is what contributors actually want. The conditional is 1 LOC and falls back to the current behavior if `.venv/bin/python` is missing.

**Size**: XS — 1 LOC Makefile + 1 sentence CONTRIBUTING.md ("we look for `.venv/bin/python` first; override with `make test PYTHON=…` for any other venv layout"). ~10 min.
**Depends on**: nothing.
**Accept (Option A)**:
- `Makefile:6` becomes `PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python)`.
- CONTRIBUTING.md "Quick start" §"`make test` is the canonical entry point" sentence gains a one-line note: "we look for `.venv/bin/python` first; override with `make test PYTHON=...` for non-standard venv paths".
- Smoke-verified on macOS zsh in two modes: (i) fresh clone + `python -m venv .venv && .venv/bin/pip install -e '.[test]' && make test` → 496 tests pass without activation; (ii) no `.venv/` dir + venv activated globally → falls through to bare `python`, uses the activated interpreter, still passes.
- Override path still works: `make test PYTHON=/some/other/path/python` overrides the conditional.
**Cross-ref**: P2.4 (BACKLOG:629 — the shipped Makefile); `Makefile:6` (current default); `README.md:34` + `CONTRIBUTING.md:25` (where the canonical-command framing lives that this fix backfills).

### `[x]` P2.4 — Add `make test` target so pytest works regardless of shell PATH (shipped 2026-05-17)
**Why**: This recon session: `pytest` not on PATH → confusing failure. `python -m pytest` works regardless of activation state.
**Size**: XS
**Depends on**: nothing
**Accept**: `Makefile` (or `tasks.py`) has `test` target running `python -m pytest tests/unit -q`. README's quick-start mentions `make test`.
**Done**: `Makefile` ships with `PYTHON ?= python` override + `test` (acceptance: `tests/unit -q`) + `test-all` (full `tests/`, live still env-gated) targets. README Quickstart §1a now shows `pip install -e '.[test]' && make test`. CONTRIBUTING.md:15 flipped from unquoted `.[dev]` (silently zsh-broken since macOS Catalina) to quoted `'.[dev,test]'`, picking up `pytest` deps that the `[dev]` extra lacked. Smoke-verified on zsh: 496 unit tests pass via both `make test` (activated venv) and `make test PYTHON=.venv/bin/python` (override, no activation).

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

### `[x]` P2.18 — Hoist decimal casts in `gl_balance` into a CTE (shipped 2026-05-17)
**Why**: `transforms/gold/gl_balance.py:262-272` cast the same four `decimal(38,30)` amount columns to `DECIMAL(28, 2)` twice each — once in the surfaced projection (`begin_balance_dr`, `begin_balance_cr`, `period_net_dr`, `period_net_cr`) and again inside the `closing_balance` formula's `COALESCE(CAST(...))` wrappers. Catalyst doesn't reliably CSE across `CAST` boundaries on high-precision decimals; at 11M rows this is measurable CPU. `ap_aging` already got this right via the `open_invoices` CTE (`ap_aging.py:431-445`) — cast once, outer SELECT operates on cast values.
**Done**: `build_gl_balance_sql` emits a `WITH balances AS (...)` CTE that performs each `CAST(... AS DECIMAL(28, 2))` exactly once (audit verified: 1/1/1/1); outer SELECT projects the four amount columns from the CTE without re-casting (`b.begin_balance_dr AS begin_balance_dr`, etc.); `closing_balance` is `ROUND(COALESCE(b.begin_balance_dr, 0) - COALESCE(b.begin_balance_cr, 0) + COALESCE(b.period_net_dr, 0) - COALESCE(b.period_net_cr, 0), 2)` — the `COALESCE(..., 0)` NULL-safety wrap stays on every term. LEFT JOIN preserved-fact-side maintained: `FROM balances b LEFT JOIN {silver_dim} da`. `tests/unit/test_gl_balance.py` 41 tests green; 2 tests updated to assert the new CTE shape (`test_uses_left_join_not_inner` split into "FROM gl_period_balances exists" + "FROM balances LEFT JOIN dim_account"; `test_closing_balance_formula` references CTE columns) while still enforcing the original invariants.

### `[x]` P2.19 — Project `currency_code` once in `supplier_spend` CTE (shipped 2026-05-17)
**Why**: `transforms/gold/supplier_spend.py:105, 122-123` emitted `UPPER(CAST(inv.{currency_col} AS STRING))` in both the SELECT projection and the GROUP BY — same expression twice. Spark usually CSEs this but with `UPPER(CAST(...))` chains it sometimes doesn't, and it prevents the shuffle from using a precomputed partition column. `ap_aging` already projects `currency_code` once in its `open_invoices` CTE.
**Done**: `build_supplier_spend_sql` emits a `WITH invoices AS (...)` CTE that projects `UPPER(CAST(inv.{currency_col} AS STRING)) AS currency_code` exactly once AND `CAST(inv.ApInvoicesVendorId AS BIGINT) AS vendor_id` exactly once (audit verified). Outer SELECT, JOIN ON, and GROUP BY all reference `inv.currency_code` and `inv.vendor_id`. NULL-safe amount aggregation preserved: `SUM(COALESCE(CAST(inv.ApInvoicesInvoiceAmount AS DECIMAL(20, 2)), 0))` and same for `AmountPaid` (amount casts intentionally kept inline because they only run inside `SUM(COALESCE(CAST(...)))` — pulling them into the CTE wouldn't save work). LEFT JOIN preserved-fact-side maintained: `FROM invoices inv LEFT JOIN {silver_dim} ds`. `WHERE inv.ApInvoicesVendorId IS NOT NULL` moved into CTE body (vendor-id presence filter preserved). `tests/unit/test_supplier_spend.py` 33 tests green; 3 tests updated to assert the new CTE shape (`test_uses_left_join_not_inner` split; `test_grouping_uses_invoice_vendor_id` verifies CTE projection + `inv.vendor_id` in GROUP BY; `test_currency_code_in_group_by` verifies CTE UPPER+CAST + `inv.currency_code` in GROUP BY) while still enforcing the original invariants.

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
**Why**: P1.5α ships `SecretStr` wrapping (`_resolve_password()` in `orchestrator/runtime.py` — see the §4.9) so resolved credentials don't leak through `repr`/`str`/`debug` accidents. But the schema-level footgun is still open: `schema/bundle.py:73` declares `password: str` and accepts a literal value equally with `${vault:OCID}` / `${env:VAR}` — Pydantic does not reject `password: hunter2`. In dev phase this is acceptable (1 user, both example bundles use the sigil, demo-pod creds, `_resolve_password()` logs a WARN on literals). At first non-`saasfademo1` customer onboarding, this becomes a real "creds in git history" risk and must be closed before the customer's `bundle.yaml` lands in a repo. Four hardening items, each cheap individually, sized together because they share the secret-resolution code path.
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

### `[ ]` P3.10 — Orchestrator parallel execution *(orchestrator-evolution design item E)*
**Why**: P1.5α explicitly chose sequential execution (the §7). Rationale at the time: saasfademo1 seed run finishes in <2 min and parallelism complicates failure-mode semantics. Trigger to revisit: any tenant where the seed run exceeds ~5 min wall-clock, OR where multiple bronze extracts could run concurrently against independent PVOs. The orchestrator's DAG already encodes dependencies (`depends_on_bronze`, `depends_on_silver`) — parallelism is a scheduler swap, not a re-architecture (e.g. `concurrent.futures` thread pool driving `graphlib.TopologicalSorter`'s ready-set).
**Size**: M — swap the topo executor for a ready-set scheduler; preserve fail-fast semantics; bounded worker count (config knob, default 4).
**Depends on**: P1.5α shipped; live evidence on at least one tenant where sequential runtime is the bottleneck.
**Accept**:
- `orchestrator.run()` gains `max_workers: int = 1` kwarg (default keeps today's sequential behavior).
- Independent bronze extracts (no shared PVO) and independent dim builds run concurrently up to `max_workers`.
- Fail-fast preserved: a failed step still skips dependents and halts new dispatches.
- Live evidence: TC<N> showing wall-clock reduction on a tenant with ≥4 enabled datasets.

### `[ ]` P3.11 — Orchestrator step-level retries *(orchestrator-evolution design item S)*
**Why**: P1.5α explicitly chose fail-fast (the §7) — re-run the CLI if a step fails. Trigger to revisit: transient BICC failures (rate-limit 429s, network blips, OAC connection timeouts) observed in real customer runs. Distinct from P2.1 (BICC API-key bootstrap exp backoff, one-shot at install time) — this is per-step retry at run time. Should be scoped to *transient* errors only (network, rate-limit), not data-correctness errors (schema mismatch, NULL currency hard-gate); the orchestrator must classify before retrying or it will mask real bugs.
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

## Theme: Orchestrator evolution menu (2026-05-15)

> Compact tracker entries for items from the orchestrator design-doc menu. Full analysis (problem framing, sizing rationale, hypotheticals considered) lives in the maintainer's orchestrator-evolution design notes — the letter at the end of each title is the cross-reference key. Items already tracked elsewhere have been annotated above: **A** → P1.Xb (elevate to α), **E** → P3.10, **S** → P3.11. **L** (bundle schema versioning) was elevated to ship in α and lives in the §4.4d, not in this section.
>
> When elevating one of these from "menu entry" to "real work item," expand into a full plan-analysis entry (problem statement + options + tradeoffs + chosen approach + plan edits + acceptance criteria) following the P1.5α-fix1..fix7 pattern. A DECISION doc is warranted when the tradeoff axis is non-obvious (data-correctness vs performance vs UX).

### β prerequisite — mandatory for P1.5β

#### `[ ]` P3.20 — Watermark window bounds for incremental extraction (DESIGN item K)
**Why**: Fusion BICC has max-window constraints on some PVOs (>90 days fails). If a tenant runs incremental after a 3-month gap (vacation, freeze period), passing a single 90+ day window crashes deep in `extract_pvo()` with a Fusion-side error. Helper chunks the window into ≤85-day spans.
**Size**: S — `_chunk_watermark_window(start, end, max_window_days=85) → list[(start, end)]` + tests. ~45 min.
**Depends on**: P1.5β incremental implementation.
**Accept**: any incremental extract spanning >85 days is automatically chunked; live evidence on one tenant showing a multi-chunk extract completes.

### β — early post-α (UX + quality wins)

#### `[ ]` P3.21 — Idempotency contract test per module (DESIGN item B)
**Why**: Each silver/gold `build()` claims `CREATE OR REPLACE TABLE` semantics. Nothing enforces it. A `current_timestamp()` baked into a non-audit column, or a side-effect to an external system, would slip through single-run unit tests.
**Size**: XS — one ~3-LOC property test per module ("run twice, assert row count + value checksum match").
**Depends on**: P1.5α shipped.
**Accept**: every shipped silver dim + gold mart has a `test_<name>_is_idempotent` test that runs `build()` twice and asserts byte-equivalent outputs.

#### `[ ]` P3.22 — Bronze freshness rendered in `status()` output (DESIGN item D)
**Why**: Operators have a "do I need to re-extract?" question every iteration. Today the answer requires reading `fusion_bundle_state` raw rows. A "X days stale" line in the dashboard is operator-UX gold for ~10 LOC.
**Size**: XS — formatting tweak in the existing `status()` renderer using `fusion_bundle_state.last_run_at` per dataset.
**Depends on**: P1.5α shipped (state table populated).
**Accept**: `aidp-fusion-bundle status` shows "bronze.ap_invoices: 14 days stale" / "bronze.gl_period_balances: fresh (2h ago)" per dataset.

#### `[ ]` P3.23 — Step-level timing breakdown (DESIGN item R)
**Why**: `RunStep.duration_seconds` today is one number. When `ap_invoices` runs slow, operator can't tell whether to call OCI support (Delta slow), Oracle support (BICC slow), or accept the cost (genuinely large extract). Four numbers (extract / enrich / write / count) diagnose.
**Size**: XS — ~10 LOC of timing wrappers + state-table columns + tests.
**Depends on**: P1.5α state-table schema (treat as a minor schema evolution).
**Accept**: `fusion_bundle_state` gains `extract_seconds`, `enrich_seconds`, `write_seconds`, `count_seconds`; `status()` surfaces the breakdown when `--verbose`.

#### `[ ]` P3.24 — Checkpoint-resume on partial failure (DESIGN item F)
**Why**: A 45-minute bronze extract followed by a 2-minute gold SQL fix that fails is expensive to iterate on. Today the operator re-runs from scratch — eats the 45 minutes again. With `--resume`, iteration drops to 2 minutes. Single biggest "iterating-on-gold" workflow UX improvement.
**Size**: M — ~40 LOC of resume logic + new `RunStep.status="resumed_skipped"` (distinct from cascade-skip) + state-table read-most-recent query + tests.
**Depends on**: P1.5α-fix3 state-table contract live-verified.
**Accept**: `aidp-fusion-bundle run --resume` reads the most recent run_id, skips steps with `status='success'`, only re-runs failed/skipped/missing. Unit test pins that a fixture with one failed gold step + everything else succeeded → `--resume` only re-runs the failed step.

#### `[ ]` P3.25 — `aidp-fusion-bundle dry-run-probe` CLI verb (DESIGN item O)
**Why**: Saves a 45-minute "wait for bronze to fail at mart 7" feedback loop. Customer hits a new tenant, runs `dry-run-probe`, sees "ap_invoices: 1 row sampled, schema OK; gl_period_balances: schema mismatch — expected `GL_PERIOD_NAME`, found `GL_PeriodName`" in under 30 seconds. They fix bundle config, then run for real. Distinct from existing `--dry-run` (plan-only, no extract).
**Size**: S — ~30 LOC reusing existing extractors with a `limit=1` kwarg + new CLI verb + tests.
**Depends on**: P1.5α bronze extractors stable.
**Accept**: `dry-run-probe` does one-row sample per enabled PVO, verifies connectivity + schema + audit columns, exits in <30s; doesn't materialize anything; doesn't touch state.

#### `[ ]` P3.26 — Structured JSON logging alongside Rich console output (DESIGN item Q)
**Why**: `console.print(...)` is great for the CLI surface. For AIDP cluster cron / REST jobRuns / any non-CLI surface, also emit structured logs ingestible by Datadog/Splunk/OCI Logging. A customer running from cluster cron loses all Rich formatting anyway; they need parseable events.
**Size**: S — `_log_event(event, **kwargs)` helper + ~10 call sites + tests asserting JSON parseability.
**Depends on**: P1.5α shipped.
**Accept**: every orchestrator state transition (run_started, step_started, step_completed, step_failed, run_ended) emits a structured JSON log line in addition to Rich output; unit test parses lines from `caplog` and asserts schema.

#### `[ ]` P3.27 — Data-quality assertions as first-class step status (DESIGN item C)
**Why**: SQL succeeds, but produces garbage. Today: `dim_supplier` builds with all NULLs because upstream bronze had a schema-detection miss; the build succeeds; gold marts join against all-NULL surrogate keys; mart shows zero rows; customer thinks they have no AP invoices. dbt does this with `tests:` blocks in YAML; we mirror in Python.
**Size**: M — ~50 LOC of assertion runner + per-module assertion lists + `RunStep.quality_check: bool | None` field. **DO NOT** pre-add the field shape in α (DESIGN §7 suggests this; we rejected — state-schema mutation pre-α is free).
**Depends on**: P1.5α shipped + first live tenant evidence on what's worth asserting.
**Accept**: each silver/gold module declares assertions (row count > 0 unless empty-source-declared, no NULL in natural_key, referential integrity); failures emit step-level WARN + populate `RunStep.quality_check=False`; orchestrator continues but the run summary surfaces the failures.

#### `[ ]` P3.28 — Cross-run locking via state-table sentinel (DESIGN item G)
**Why**: Two operators running `aidp-fusion-bundle run` simultaneously against the same tenant would clobber each other silently. Delta has table-level locking; orchestrator-layer "fail fast on concurrent run" is the safer pattern.
**Size**: S — ~30 LOC of lock acquire/release + stale-lock reclamation + tests.
**Depends on**: P1.5α state-table contract.
**Accept**: orchestrator writes a `running` sentinel row at start; refuses to start if one exists and is <N minutes old; reclaims stale locks (>N minutes); test pins both happy path and reclamation.

### Later — demand-driven

#### `[ ]` P3.29 — Cache shared bronze across gold marts (DESIGN item I)
**Why**: `ap_aging` and `supplier_spend` both read `bronze.ap_invoices` — within one Spark session that's two full scans. Detect shared bronze tables across the plan; `.cache()` before first consumer; `.unpersist()` after last. 2× gold-layer speedup with ~10 LOC.
**Size**: XS.
**Depends on**: P1.5α-fix4 `resolve_plan` (provides the dependency graph); live evidence the gold layer is bottlenecked on bronze reads (not before).
**Accept**: shared-dep detection in `resolve_plan`; `.cache()`/`.unpersist()` wired into the run loop; live evidence shows gold layer wall-clock improves by >30% on a tenant with mid-size bronze.

#### `[ ]` P3.30 — Idempotent `run_id` derivation (DESIGN item H)
**Why**: Today `run_id = _new_run_id()` is fresh per invocation. When P1.5ε REST auto-retry surfaces (not in scope today — REST dispatch just submits jobs), two retries with different `run_id`s claim the same logical work, audit trail confuses. Deferred from α because retry-layer-above-orchestrator isn't on the roadmap.
**Size**: XS — one-line derivation change + test.
**Depends on**: a real retry-layer-above-orchestrator surfacing (REST auto-retry, MCP retry, scheduler-driven retry).
**Accept**: `run_id` derived from `hash(bundle_path_content + mode + timestamp_rounded_to_minute)`; retries within the rounding window collide; unit test pins both same-window collision and different-window uniqueness.

#### `[ ]` P3.31 — Module versioning persisted in state rows (DESIGN item P)
**Why**: When `dim_supplier` v0.2 changes normalization rules in v0.3, forensics needs to know which version produced which row. Today `fusion_bundle_state` says "dim_supplier ran on 2026-05-15" — doesn't say which build. Deferred from α because state-table schema is mutable pre-α (no migration cost to add later).
**Size**: XS — ~5 LOC per module + one state-table column + one test.
**Depends on**: P1.5α shipped + a real forensics need (post-customer-onboarding).
**Accept**: every module declares `MODULE_VERSION: Final[str] = "0.2.0"`; threaded into `RunStep.module_version`; persisted as a new state-table column; bumped on breaking output changes.

#### `[ ]` P3.32 — Pluggable extractor protocol (DESIGN item M)
**Why**: BICC is the right primary for SaaS Fusion, but real customers eventually have hybrid scenarios (BICC + Fusion REST + Object Storage CSV + on-prem Oracle DB). A Protocol interface lets the plugin grow without rewriting the bronze layer. **Two phases**: (1) define the Protocol shape (annotation-only, no concrete additions); (2) ship concrete other-extractors per customer demand.
**Size**: S for phase 1 (Protocol declaration + wrap existing `extractors/bicc.py`); M for each concrete extractor.
**Depends on**: P1.5α shipped.
**Accept (phase 1)**: `Extractor(Protocol)` with `extract(spark, dataset_id, *, watermark=None) → DataFrame`; `BICCExtractor` is the sole concrete; `BronzeExtractSpec.extractor: str = "bicc"` selects via a registry; no behavior change for existing flows. **(phase 2)**: a customer asks; we add a concrete implementation matching their need.

#### `[ ]` P3.33 — Broadcast hints from row-count metadata (DESIGN item J)
**Why**: Small dims (`dim_calendar` ~4k, `dim_supplier` likely <50k) should be broadcast-joined to large bronze. Spark AQE handles this dynamically but the warmup eats real time (10-20s per join). Explicit `/*+ BROADCAST(silver_dim_alias) */` hints from build-time row counts skip the warmup. Premature without live evidence AQE warmup is the bottleneck.
**Size**: S — ~5 LOC per gold module + threshold config + tests.
**Depends on**: live evidence AQE warmup is dominant on a real customer.
**Accept**: gold-mart SQL builders inject broadcast hints when the joined dim's `RunStep.row_count` is below `bundle.run.broadcast_threshold: int = 100_000`; toggle off via config.

#### `[ ]` P3.34 — Configurable on-failure policy: continue-independent-branches vs abort-remaining
**Why**: P1.5α ships with **abort-remaining** semantics (every plan node not yet attempted gets a `status='skipped'` row with `error_message='run aborted on prior failure of <X>'`). This is the "audit-completeness" choice: state table has exactly `len(plan)` rows per run, `status()` never falls back to stale prior-run data. The trade-off is wasted work — a failing AP-branch bronze blocks the GL refresh too even though GL is independent. Industry tools (dbt, Airflow `trigger_rule="all_done"`, Spark DAG scheduler) default to continue-independent-branches — independent branches run to completion regardless of sibling failures.
**Decision deferred to evidence**: first-customer evidence will say whether (a) operators want fail-fast-complete-audit (today's behavior) OR (b) they want continue-on-independent-failure to maximize useful work per run. Don't pre-empt.
**Size**: M — add `bundle.run.on_failure: Literal["continue", "abort"] = "abort"` config field; gate the `_abort_remaining(...)` call behind it; in `"continue"` mode, drop the `break` after `_skip_dependents` and let the loop iterate over remaining independent nodes (`_execute_node` runs them normally; `_skip_dependents` already prevents downstream dispatch through `step.status` checks).
**Depends on**: P1.5α shipped + first-customer evidence (≥1 run where a single failure blocked otherwise-completable work AND the operator complained).
**Accept**:
- `bundle.run.on_failure: Literal["continue", "abort"]` field on the bundle config schema (defaults to `"abort"` — preserves α behavior).
- `"continue"` mode: loop iterates over every plan node; `_skip_dependents` cascades only direct/transitive downstream of failures; independent branches complete normally; `RunSummary.steps` has one row per plan node with the natural status mix (`success` for independent successes, `failed` + `skipped`-cascade for the failed branch).
- New test `test_continue_on_failure_runs_independent_branches`: branch A's bronze fails; branch B's bronze succeeds; assert branch B's full chain (bronze + silver + gold) all `success`; assert branch A is `failed` + cascade-`skipped`; no abort-`skipped` rows anywhere.
- `"abort"` mode regression-tested to still match α behavior exactly.
- Bundle schema doc + README updated with the trade-off explanation; operator picks based on whether independent-branch business value > root-cause-clarity.
**Cross-ref**: §4.4 + §4.7 of the canonical PLAN (the α-shipped abort-remaining cascade); P3.24 (Checkpoint-resume) is the work-maximizing alternative for `"abort"` mode operators.

#### `[ ]` P3.35 — Delete `ar_aging` from `schema/fusion_catalog.py` (documentation-only PVO duplicate)
**Why**: `_AR_AGING` at `schema/fusion_catalog.py:153` declares `datastore="…ArBiccExtractAM.TransactionHeaderExtractPVO"` — **identical to `_AR_INVOICES`** (line 137). Its own description admits "Fusion BICC has no direct AR-Aging PVO. The aging gold mart is computed downstream from ArBiccExtractAM.TransactionHeader + ReceiptHeader." It exists only as documentation linking the gold AR-aging mart to its data origin. Problems this creates:
- Catalog readers assume `ar_aging` is a runnable bronze extract (it isn't).
- The catalog–registry invariant lint (PLAN §8 — Option C from this session) flags it as EXTRACT_PVO-kind-not-registered. Adding it to BRONZE_EXTRACTS would duplicate `ar_invoices`; adding it to KNOWN_DEFERRED_DATASETS would imply a future extractor (none planned).
- `test_datastore_names_mostly_unique` (tests/unit/test_fusion_catalog.py:89) currently allows 1 duplicate datastore name to accommodate this entry; deleting `ar_aging` removes the special case.

**Fix**: delete `_AR_AGING` PvoEntry block; remove from the `for e in (...)` list in `CATALOG` declaration; update `test_datastore_names_mostly_unique` to assert `len(dupes) == 0` (strict). Update any docstring or LIMITS.md note that referenced `ar_aging` as a catalog entry. The gold AR-aging mart (currently KNOWN_DEFERRED_MARTS["ar_aging"] → P1.10) continues to reference `ar_invoices` + `ar_receipts` directly — no orchestrator code change.

**Size**: XS — ~15 LOC delete + 1 test assertion tightening + grep-and-touch for any external references.
**Depends on**: nothing. Independent cleanup.
**Accept**:
- `_AR_AGING` block removed from `schema/fusion_catalog.py`; `CATALOG` no longer contains the `ar_aging` key (`catalog.get("ar_aging")` raises `KeyError`).
- `test_datastore_names_mostly_unique` tightened to assert no datastore duplicates (`len(dupes) == 0`).
- New test `test_ar_aging_not_in_catalog` confirms the deletion (regression guard against re-adding by accident).
- No other test fails — `_AR_INVOICES` remains the canonical entry for the shared datastore.
**Cross-ref**: §4.3 (catalog ↔ bronze-registry invariant lint), and the comment block in §4.3 BRONZE_EXTRACTS noting "documentation-only catalog entries are NOT wired here" — this entry removes the only such case so the invariant lint can stay strict.

#### `[ ]` P3.36 — Rename bronze PVO id `ap_aging` → `ap_aging_periods` (cross-layer namespace collision fix)
**Why**: `_AP_AGING` at `schema/fusion_catalog.py:185` declares `id="ap_aging"` for the `AgingPeriodHeaderExtractPVO` — but the entry's own `bronze_table_name` is `ap_aging_periods` (the PVO is bucket-period configs, not aged transactions). `GOLD_MARTS["ap_aging"]` (P1.9, shipped) is the actual AP-aging gold mart computed downstream. **Same string, two registries** → the orchestrator's single-namespace `resolve_plan(...)` (P1.5α-fix7) treats `--datasets ap_aging` as ambiguous: should it run the bronze deferred-spec or the gold mart? Today's plan §6 "remove from example" patch is documentation-by-omission; the collision lives in code. Renaming the bronze id to match its already-declared `bronze_table_name` fixes both bugs:
- Cross-layer name collision → resolved (gold keeps `ap_aging`; bronze becomes `ap_aging_periods`).
- Misleading bronze id → resolved (the PVO is aging *period configs*, naming should reflect that).
**Fix**: change `id="ap_aging"` → `id="ap_aging_periods"` at `schema/fusion_catalog.py:186`; update `§4.3 KNOWN_DEFERRED_DATASETS` key (already updated to `"ap_aging_periods"` in this session); grep for stray `"ap_aging"` references that mean the bronze (vs the gold mart) — `examples/full_finance.yaml` likely needs `datasets: [..., ap_aging, ...]` → `datasets: [..., ap_aging_periods, ...]` if it lists this dataset at all (probably doesn't today since the bronze is deferred). Update `tests/unit/test_fusion_catalog.py` `test_gl_trio_confirmed`-style tests if any reference `get("ap_aging")` for the bronze.
**Size**: XS — one PvoEntry id rename + grep-and-touch for references + one new test (`test_no_name_collisions_across_registries`, also tracked in PLAN §8). ~30 min.
**Depends on**: nothing. Mechanical cleanup; runs ahead of P1.5α implementation cleanly.
**Accept**:
- `schema.fusion_catalog.get("ap_aging_periods")` returns the `AgingPeriodHeaderExtractPVO` PvoEntry; `get("ap_aging")` raises `KeyError` (or — preferred — points to `GOLD_MARTS["ap_aging"]` via a helpful message in the resolver).
- `§4.3 KNOWN_DEFERRED_DATASETS["ap_aging_periods"]` is the only registry slot for this PVO.
- `BRONZE_EXTRACTS ∩ GOLD_MARTS == ∅`, `KNOWN_DEFERRED_DATASETS ∩ GOLD_MARTS == ∅`, and all other pairwise intersections across the six registries are empty. New pytest `test_no_name_collisions_across_registries` pins this.
- No bundle.yaml example or test fixture references the old `"ap_aging"` bronze id.
**Cross-ref**: PLAN §4.3 (KNOWN_DEFERRED_DATASETS post-rename), PLAN §8 (single-namespace registry lint), P3.35 (sibling catalog cleanup — deletes `ar_aging` documentation-only entry; both close catalog↔registry naming bugs).

### Explicitly declined — captured here so they don't get re-pitched

#### `[~]` ~~Customer-authored marts via dynamic loading~~ — **DECLINED** (DESIGN item N + §8)
**Why declined**: creates a "is this customer code or our code?" support nightmare (every bug report starts with "before I look, confirm your `custom_mart_dir` is empty"). Dynamic import fragility — customer code failing to import looks like our code failing. Encourages fork-pretending-not-to-be-a-fork. The honest alternative is to make forking the plugin ergonomic (narrow module interfaces — already there + a "how to extend" guide). Forks are honest about their fork-ness. **Revisit only if a credible use case appears that genuinely can't be met by forking + good docs.**

#### `[~]` ~~HMAC signing of audit rows for SOX tamper-evidence~~ — **DECLINED**
**Why declined**: crypto in audit logs adds key-management burden disproportionate to the threat model. Realistic threat is "operator accidentally deletes a row," not "malicious party forges audit history." Key rotation + recovery story when a tenant's vault rotates is real ops cost. **Defer until a customer or auditor specifically asks**; the requirement will then be concrete (which fields signed, which algorithm, which key store) — better to defer than build speculatively.

#### `[~]` ~~True multi-process concurrent scheduling~~ — **DECLINED**
**Why declined**: P3.28 (cross-run locking) prevents collision — sufficient. Full multi-process scheduling is what Airflow is for; we're a library function. The pressure to grow into a daemon is a signal you've outgrown the plugin model — at that point, fork to an Airflow-based deployment. **Build cross-run locking; do not build a scheduler.**

#### `[~]` ~~"Smart" auto-tuning / cluster-size recommendations~~ — **DECLINED**
**Why declined**: auto-tuning needs a feedback loop (workload → measurement → recommendation → measurement of recommendation effect); we don't have that loop and won't until we have many tenants. A wrong recommendation erodes trust faster than no recommendation. Spark's AQE already handles most cases. **Manual broadcast hints (P3.33) are the exception; not the rule.**

---

# Summary table — execution order recommendation

If you're picking from the top, here's the suggested first 10 sessions:

| # | Item | Class | Size | Why this order |
|---|---|---|---|---|
| 1 | P0.1 — CHANGELOG date stamp | P0 | XS | 30 sec; instant credibility |
| 2 | P0.3 — STATUS+BACKLOG git decision | P0 | XS | 1 min; clears repo state |
| 3 | P0.4 + P0.5 — README phase callouts | P0 | XS | 15 min; stops misleading users |
| ~~4~~ | ~~P2.4 — `make test` target~~ | ~~P2~~ | ~~XS~~ | shipped 2026-05-17 |
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
