# `oracle-ai-data-platform-fusion-bundle` — working principles

> These are the load-bearing principles for working on this plugin. They sit on top of (not instead of) the workspace-level `/Users/oussamalakrafi/Workspace/CLAUDE.md` AIDP rules. The mechanical PR checklist lives in [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Mission

The fusion-bundle plugin must run on **any Fusion ERP/HCM/SCM tenant**, not just the `saasfademo1` demo pod. Hardcoded tenant-specific assumptions are bugs. The plugin's value proposition is that a customer can clone the repo, point it at *their* Fusion + AIDP + OAC, and have bronze + silver + gold materialize without editing Python.

## v1 + v2 coexistence (migration state)

This plugin is **mid-migration to v2 content-pack architecture**. Both layers live in the tree simultaneously:

- **v1 runtime (active today)**: hardcoded silver/gold Python modules under `scripts/.../dimensions/dim_*.py` and `scripts/.../transforms/gold/*.py`. Drive every shipped `--mode seed` and `--mode incremental` run. Frozen as **reference implementations** during the migration; new functionality does NOT land here.
- **v2 content packs (shipped Phase 1, runner shipped Phase 2)**: `scripts/.../content_packs/fusion-finance-starter/` declares the same nodes as YAML + (future) SQL templates, with `implementation.type: python_legacy` + `migrationTarget` pointing at the SQL replacement. Schema layer (`schema/medallion_pack.py`, `schema/dashboard_pack.py`) and loader/validators (`orchestrator/content_pack*.py`) load these without changing runtime behaviour. Phase 2 added the generic SQL runner (`orchestrator/sql_runner.py`) behind the opt-in `--execution-backend content-pack` flag — default backend stays `legacy-python` until Phase 4's dual-runner parity gate proves equivalence.

**Where new work goes:**

- **New silver / gold nodes** → content pack YAML + SQL in `content_packs/<pack-id>/`. Never a new `dim_*.py` or `gold_*.py` module. The architectural test `tests/architectural/test_no_new_legacy_modules.py` enforces this; new entries to the legacy allowlist require a documented reason on the PR (see PLAN §15 Phase 0 step 9).
- **New tenant variation** → `columnAliases` / `semanticVariants` in `pack.yaml`, resolved at `bootstrap` per ADR-0014. Not a runtime probe in a Python module.
- **New error code** → register in PLAN §25 first, then reference the constant from `medallion_pack.py` / validators.
- **v1 maintenance fixes (bug fixes on existing dimensions/dim_*.py, transforms/gold/*.py)** are still valid — those modules are the active runtime through Phase 5. But anything new (new column, new mart, new refresh logic) belongs in v2.

Architectural authority for v2: [`dev/PLAN_plugin_engine_medallion_content_packs.md`](dev/PLAN_plugin_engine_medallion_content_packs.md) (gitignored working doc).

## What varies per tenant — and where it lives

v2 splits tenant variability across **three** declarative surfaces (not the two v1 had).

### Static policy → `bundle.yaml`

Connectivity + AIDP catalog identity. Customer declares once, never probed.

- Fusion service URL, BICC storage, credentials (vault refs).
- 3-part target names — `aidp.catalog`, `aidp.bronzeSchema`, `aidp.silverSchema`, `aidp.goldSchema`.
- Active content pack + active profile reference.
- Enabled dashboard list.
- Runtime settings (`watermarkSafetyWindowSeconds`, retry policy).

### Tenant customisation knobs → `profiles/<tenant>.yaml`

Authored at `bootstrap` (interactively, with skill assist when needed); frozen for subsequent runs. Captures customer-specific values the pack expects.

- Calendar settings (start/end dates, fiscal start month, FY naming).
- COA semantic-segment map (when the customer's COA differs from Fusion-conventional segment1/2/3).
- Resolved variation points (the column-alias and semantic-variant choices `bootstrap` picked for this tenant).
- Bronze schema fingerprint pinned at bootstrap (drives the §11.6 drift gate).

### Variation points (pack-declared candidates, bootstrap-resolved)

Things the pack doesn't know upfront but enumerates likely candidates for. Resolved once at `bootstrap`, frozen into the tenant profile. **Superseding the v1 runtime detection contract** (`KNOWN_*_ALIASES` priority lists in Python modules) — see ADR-0014.

- **`columnAliases`** — same logical column, different physical names. `invoice_currency_code` candidates: `ApInvoicesInvoiceCurrencyCode` / `ApInvoicesCurrencyCode`.
- **`semanticVariants`** — same logical concept, different SQL shape. `cancelled_status` candidates: `cancelled_date` (`ApInvoicesCancelledDate IS NULL`) / `cancelled_flag` (`COALESCE(...CancelledFlag, 'N') != 'Y'`).
- Auto-resolved when exactly one candidate matches the tenant. Operator chooses when multiple match. **Skill drafts an overlay** when zero match (tier-2 escalation per §9.5.5).
- SQL templates reference them as `{{ column.<name> }}` / `{{ semantic.<name> }}`. The renderer substitutes the frozen profile value; **no LLM call happens during `--mode seed` or `--mode incremental`**.

**Add knobs evidence-driven, not preemptively.** A candidate ships when a real tenant has hit the variant, or a published Oracle source documents it. Don't speculate. See PLAN §13.3.2 evidence-discipline rules.

## Architecture: the CLI is the contract

`aidp-fusion-bundle run --mode seed` must materialize bronze + silver + gold end-to-end. `aidp-fusion-bundle bootstrap` resolves tenant variation. If a customer has to open a notebook and call `build()` by hand, the architecture has drifted from the README.

- **The CLI is self-contained.** No LLM is invoked during seed / incremental — per ADR-0017. Skill help is customer-initiated via Claude Code, reading diagnostic artefacts the CLI writes on failure.
- **The orchestrator owns state.** Watermarks, `fusion_bundle_state`, run-IDs are the orchestrator's responsibility. v1 modules read Spark in, write Spark out, accept paths + kwargs. v2 SQL templates render through the content-pack renderer with declared variables only. A node implementation that touches `fusion_bundle_state` directly is a layering violation.
- **v1 module shape (legacy):** each `dim_*.py` / gold module exposes `build_<mart>_sql(...) → str` for unit testing without Spark, plus `build(spark, ...) → DataFrame` that executes it.
- **v2 node shape (active):** YAML metadata + SQL file (or `type: builtin` callable). Loaded by `orchestrator/content_pack.py::load_pack`. Validated by `orchestrator/content_pack_validators.py`.

## Medallion correctness invariants

These apply to **both** v1 modules and v2 SQL templates — they're SOX/finance invariants, not implementation details.

- **Refresh strategy per node**, declared explicitly:
  - **Row-grain nodes** (silver dims excl. `dim_calendar`, gold `gl_balance`) use `strategy: merge` with `MERGE INTO target USING (<source> WHERE <lineage_col> > <layer-local watermark>) AS src ON target.<natural_key> <=> src.<natural_key> WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` — NULL-safe `<=>` on composite keys (LIMITS.md P1.17-L8).
  - **Incremental-exempt marts** (`supplier_spend`, `ap_aging`, `dim_calendar`) use `strategy: replace` — `CREATE OR REPLACE TABLE` every cycle regardless of mode. `supplier_spend` because its grain mixes mutable `approval_status` (partial-MERGE leaves PENDING + APPROVED dupes on status flip; correct aggregate-MERGE ships as `aggregate_merge` post-v0.3 per PLAN §10.8). `ap_aging` because `CURRENT_DATE()`-anchored bucket assignments would freeze daily under MERGE. `dim_calendar` because it's parameter-driven (ADR-0011 — stays as `builtin`).
  - **Bronze layer** always MERGEs under incremental on the bronze natural key — safety-window overlap re-extracted each cycle dedupes, never replaces.
  - v2 declares all of this in node YAML (`refresh.{seed,incremental}.strategy`); v1 hardcodes it inside `build()`. The strategy taxonomy is enumerated in PLAN §10 and validated in PLAN §11.3 (R1–R13) — every node's choice has a documented reason.
- **Surrogate keys are deterministic — `xxhash64(natural_key)`, never `monotonically_increasing_id()`.** Non-deterministic surrogates break incrementality and Type-2 SCD. Wired on `dim_supplier.supplier_key` and `dim_account.account_key`.
- **`COALESCE(amount, 0)` around every arithmetic operation on Fusion amount columns.** Fusion legitimately emits NULL for absent period components, opening balances on new accounts, sparse dim attributes. Without `COALESCE`, a single NULL nullifies the entire expression (NULL propagation). Live-validated: ~20% of `gl_period_balances` sample rows hit at least one NULL component (2026-05-09).
- **Currency-in-grain is mandatory for any amount aggregate.** Cross-currency rollup is the consumer's responsibility, never the mart's. Hard-fail the build if currency col is missing — no single-currency-summed marts on a multi-currency tenant.
- **Prefer one financially-correct SQL shape (single LEFT JOIN, fact preserved) over runtime path-selection.** Runtime decisions belong in data-quality gates (real-vs-proxy), not join topology. The round-6 audit killed the `id_populated_pct >= 0.5` picker for exactly this reason.
- **Audit columns are non-negotiable.** Bronze: `_extract_ts`, `_source_pvo`, `_run_id`, `_watermark_used`. Silver: `bronze_extract_ts`, `bronze_source_pvo`, `silver_built_at`, `silver_run_id`. Gold: `gold_built_at`, `gold_run_id`. SOX trail — the `<layer>_run_id` columns join silver/gold rows to `fusion_bundle_state.run_id`, so a row in `gold.ap_aging` can be traced to the exact orchestrator run that produced it. v1 modules accept `run_id: str | None = None` kwarg; v2 templates use `{{ run_id_literal }}` and the renderer escapes the value.
- **PII classification is mandatory** on every v2 `outputSchema.columns` entry (`pii: high | medium | low | none`). Missing → `AIDPF-2030`. High-PII columns must not appear in dashboard `requires.columns` / `security.allowedColumns` (§12.6 OAC MCP exposure).

## Testing discipline

- **Live evidence is required for any plugin-portability claim.** Unit tests verify SQL shape; only a live run against a real tenant's BICC extract verifies the SQL actually works. A new mart isn't done when `pytest` passes — it's done when there's a `tests/live/TC<N>_<mart>_results.md` showing real numbers from a real pod.
- **Live evidence on at least one non-`saasfademo1` tenant before any "plugin-portable" claim ships.** Verifying on the demo pod proves it works on the demo pod, not that it's portable. Tracked as P3.7 / P3.9.
- **The empty-source case is part of the contract.** Every dim and mart produces a sensible empty result (zero rows, correct schema, audit columns populated) when bronze is empty. Don't crash, don't silently relabel — `ap_aging`'s "empty population → real mode" decision is the template.
- **v2 architectural tests are mandatory** — `tests/architectural/test_no_new_legacy_modules.py` runs on every PR. Adding a new `dim_*.py` / `gold_*.py` module without an allowlist entry fails CI.
- **Pack-version drift tests** — `test_pack_schema_json_matches_models` catches divergence between the Pydantic source of truth and the exported `pack.schema.json` artefact. Regenerate via the docstring's snippet.

## Fusion specifics — non-obvious

- **PVO names use the full AM-hierarchy from live BICC.** `FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO`, not pdf1's abbreviated `FscmTopModelAM.SupplierExtractPVO`. `catalog probe` is the source of truth; the curated catalog in [`schema/fusion_catalog.py`](scripts/oracle_ai_data_platform_fusion_bundle/schema/fusion_catalog.py) reflects live confirmation.
- **Bronze column conventions are inconsistent across PVOs.** `SupplierExtractPVO` uses UPPERCASE no prefix (`SEGMENT1`, `VENDORID`). `InvoiceHeaderExtractPVO` uses PascalCase with `ApInvoices` prefix (`ApInvoicesVendorId`). `BalanceExtractPVO` uses PascalCase with `Balance` prefix. v2 absorbs this through `columnAliases` resolved at bootstrap; v1 modules document the convention in their docstring. Don't assume uniformity.
- **OAC's REST validator does not bless AIDP's `idljdbc` connectionType.** Customer creates the OAC connection via the UI once (using the 6-key JSON from `--print-only`); subsequent `dashboard install` runs reuse via precheck. Documented in [`docs/oac_rest_api_setup.md`](docs/oac_rest_api_setup.md). Don't try to bypass with REST.
- **OAC `.bar` files are opaque binary content** — never parsed by the plugin (PLAN §12.3). The dashboard pack YAML carries the gold contract; OAC catches drift between `.bar` and YAML at import time.

## Cross-references

- v2 plan (gitignored working doc): [`dev/PLAN_plugin_engine_medallion_content_packs.md`](dev/PLAN_plugin_engine_medallion_content_packs.md)
- v1 backlog (v1 maintenance items): [`BACKLOG.md`](BACKLOG.md)
- v1 status snapshot: [`STATUS.md`](STATUS.md)
- Live evidence trail: [`tests/live/`](tests/live/)
- Limit registry (known L1/L2 caveats): [`LIMITS.md`](LIMITS.md)
- Plugin reference set: `/Users/oussamalakrafi/Workspace/Claude-Context/claude-code-plugins-ahmed/07-fusion-bundle-plugin.md`
- Workspace-level AIDP rules: `/Users/oussamalakrafi/Workspace/CLAUDE.md`
