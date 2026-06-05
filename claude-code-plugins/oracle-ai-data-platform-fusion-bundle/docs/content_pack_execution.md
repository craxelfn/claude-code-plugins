# Content-pack execution backend

Phase 2 (v0.3) ships the generic SQL runner that executes a content
pack's silver/gold nodes against Spark. The runner is **opt-in** —
the default backend stays `legacy-python` (the existing v1
`dim_*.py` / `gold_*.py` modules). To exercise the content-pack
runner, pass `--execution-backend content-pack` to `aidp-fusion-bundle
run`.

## Prerequisites

1. **`bundle.yaml` declares a `contentPack:` block.**

   ```yaml
   contentPack:
     name: phase2-test-pack            # required
     path: ../packs/phase2-test-pack   # optional override; omit for installed pack
     profile: phase2-fixture            # required when using content-pack backend
   ```

   Without the block, the CLI exits with `AIDPF-1031`. Without
   `profile`, `AIDPF-1030`.

2. **A tenant profile YAML beside `bundle.yaml`.**

   The CLI resolves the profile at
   `<bundle.yaml.parent>/profiles/<profile_name>.yaml`. Per PLAN
   §9.5.7, the profile lives BESIDE `bundle.yaml`, never inside the
   pack directory.

   ```yaml
   schemaVersion: 1
   tenant: acme-prod
   pinnedAt: 2026-06-01T00:00:00+00:00
   bronzeSchemaFingerprint: "sha256:..."
   resolved:
     column: {}      # variation-point picks (empty for packs with none)
     semantic: {}
   profile:
     calendar:
       fiscalStartMonth: 4
   ```

   Missing file → `AIDPF-1033`. Bootstrap-time profile creation is a
   later feature; for v0.3 the profile is hand-authored.

## CLI usage

```bash
# Seed mode against the content-pack backend (inline).
aidp-fusion-bundle run --inline --mode seed \
  --execution-backend content-pack \
  --bundle path/to/bundle.yaml

# Incremental run (resume is NOT supported under content-pack in v0.3 —
# AIDPF-1032 rejects --resume + --execution-backend content-pack).
aidp-fusion-bundle run --inline --mode incremental \
  --execution-backend content-pack
```

## What the runner does, per node

1. **Static schema validation** (Phase 1 loader; trusted from the
   `ResolvedPack`).
2. **Preflight** — verifies declared `requiredColumns` are present in
   the live bronze schema and the merge-strategy watermark column
   exists. Metadata + bronze `DESCRIBE TABLE` only; never renders SQL.
3. **Render SQL** — exactly once, via the parameter-marker-bearing
   renderer in `orchestrator/sql_renderer.py`. Profile values flow
   through Spark's `args=` parameter binding, never inline.
4. **Compute expected plan-hash** — mixes pack/profile identity +
   `rendered_sql_hash` + `output_schema_hash` + `profile_hash`.
5. **Plan-hash drift gate** (incremental only) — blocks resume on
   `AIDPF-4040` if the expected hash differs from the last successful
   state row's hash. Re-run `--mode seed` to clear the drift.
6. **Dispatch by strategy** — `replace` (CREATE OR REPLACE TABLE) or
   `merge` (NULL-safe MERGE INTO with empty-delta probe). Reuses the
   same `RenderedSql` object.
7. **Quality tests** — 4 fully implemented (`not_null`, `unique`,
   `accepted_values`, `row_count_min`); 5 deferred (`row_count_delta`,
   `freshness`, `reconcile_to`, `referential_integrity`, `custom`)
   are reported as `status='deferred'` and do NOT block cursor
   advancement.
8. **Materialized-schema assertion** — fail closed with `AIDPF-4070`
   if the Spark target's actual schema doesn't match
   `node.outputSchema`.
9. **Atomic state commit** — primary + every lookup row written as one
   Delta append. Failure preserves the prior watermark.

## REST dispatch (no `--inline`)

Same backend choice flows through. `commands/run.py` resolves the pack
and reads the profile YAML at the laptop; `dispatch/notebook_builder.py`
embeds the staged pack files + profile YAML as **base64-encoded JSON**
in the generated notebook source — no raw payload leaks into the cell
text. The cluster-side notebook reconstructs the `ResolvedPack` +
`TenantProfile` via `materialize_staged_pack` + `load_full_chain` (the
orchestrator-owned public helpers promoted in Step 12c.bis) and passes
them into `orchestrator.run(..., execution_backend="content-pack",
resolved_pack=..., tenant_profile=...)`, which dispatches to the
content-pack per-node runner.

## Default backend stays `legacy-python`

Phase 4's parity gate will compare both backends row-for-row on a
real tenant before any default flip is considered. Until then,
`legacy-python` runs unchanged — the v1 `dim_*.py` / `gold_*.py`
modules drive every default run.

## Error codes registered by Phase 2

| Code | Meaning |
|---|---|
| `AIDPF-1030` | `contentPack.profile` missing under content-pack backend |
| `AIDPF-1031` | `bundle.yaml` has no `contentPack` block |
| `AIDPF-1032` | `--resume` not supported under content-pack in v0.3 |
| `AIDPF-1033` | Profile YAML not found at resolved path |
| `AIDPF-1034` | `--datasets` references node id not in pack |
| `AIDPF-1037` | Installed content pack `<name>` not found |
| `AIDPF-1038` | Resolved pack root has no `pack.yaml` |
| `AIDPF-1039` | Pack SQL path escapes pack root (traversal rejected) |
| `AIDPF-1040` | Staging source root not in `chain_roots` (programmer error) |
| `AIDPF-1050` | Tenant profile YAML schema validation failed |
| `AIDPF-1051` | Unsupported tenant profile `schemaVersion` |
| `AIDPF-4030` | Strategy not supported in v0.3 (only `replace` and `merge`) |
| `AIDPF-4031` | Target identifier failed allowlist |
| `AIDPF-4040` | Plan-hash drift on resume |
| `AIDPF-4060` | State-row hard commit failure |
| `AIDPF-4061` | `output_watermark` regression (defensive) |
| `AIDPF-4070` | Materialised target schema does not match `node.outputSchema` |
| `AIDPF-5001` | Identifier allowlist violation |
| `AIDPF-5002` | Unknown template token |
| `AIDPF-5003` | Unresolved variation point |
| `AIDPF-5010` | Post-render check rejected SQL (comment markers, semicolons, subqueries) |
| `AIDPF-5011` | Disallowed parameter value type for `{{ profile.<key> }}` |
| `AIDPF-5013` | `profile.snapshotDate` present but not an ISO-8601 date (Phase 3) |
| `AIDPF-5014` | `type: builtin` node's `implementation.callable` not in registry (Phase 3) |
| `AIDPF-8010` | Quality test failed |
| `AIDPF-8011` | Quality test deferred to a later phase |

## Phase 3 additions

* New renderer token `{{ snapshot_date }}` — emits literal `CURRENT_DATE()`
  when `profile.profile.snapshotDate` is absent / empty; binds as
  `:snapshot_date` parameter when present + ISO-8601. Used by `ap_aging.sql`
  to anchor aging buckets deterministically in tests; production runs
  with `snapshotDate` unset fall back to `CURRENT_DATE()` semantics
  matching v1.
* `RunContext.active_profile_name: str` — required field carrying the
  bundle's `contentPack.profile` value. Builtin adapters (initial:
  `dim_calendar`) key off this into `pack.pack.profiles[<name>]` for
  default lookups.
* Content-pack `execute_node` now dispatches `implementation.type: builtin`
  through `orchestrator/builtins/` adapters via `_BUILTIN_REGISTRY`.
  Initial entry: `dim_calendar`. The §11.9 plan-hash drift gate stays
  uniform across SQL and builtin paths by substituting
  `sha256(callable_id:VERSION)` for `rendered_sql_hash`.
* **AP aging proxy-mode-only divergence** — the v2 `ap_aging.sql` ships
  only v1's `due_date_mode='proxy'` shape (`bucket_basis='invoice_date'`,
  `max_days_outstanding`). v1's auto/real-mode behaviour, which probes
  Terms/Due-date coverage at runtime and switches to a different
  output schema (`max_days_past_due` + three provenance counts), is
  out of scope for v0.3. Tenants whose live AP data would auto-route
  to real mode under v1 will see different `ap_aging` output under
  the content-pack backend. Documented in `LIMITS.md` as **P3-L1**
  and explained at length in
  `docs/v2-phase-3-variation-catalog.md` "AP aging — proxy mode only".

## Phase 3a additions — bootstrap variation-point resolution

`aidp-fusion-bundle bootstrap` now runs a **second phase** when
`bundle.content_pack` is non-None: it probes the tenant's bronze
schema, walks each variation point declared in `pack.yaml`'s
`columnAliases` / `semanticVariants`, pins resolved values to
`<bundle.yaml.parent>/profiles/<tenant>.yaml`, and writes an evidence
snapshot to `<bundle.yaml.parent>/evidence/<tenant>/<ISO-ts>.yaml`.
The phase is no-op for v1 bundles (no `contentPack:` block);
their existing `bootstrap` behaviour is unchanged.

Algorithm per PLAN §9.5.4:

1. **Identity gate** — resolve operator from `--operator` →
   `AIDP_OPERATOR` → `USER`. Empty / whitespace / unset → `AIDPF-1020`.
2. **Pack load + probe** — load the resolved pack (overlay chain
   included); run `DESCRIBE TABLE` once per bronze dataset.
3. **Walk** — for each `columnAliases.<name>` and
   `semanticVariants.<name>`, walk the candidate list in priority
   order. Three outcomes:
   - **Exactly one match** → auto-resolve, record `mechanism: auto_resolve`.
   - **Multiple matches** → terminal prompt (or scripted via
     `--resolutions`, or auto-pick first under `--non-interactive`).
   - **Zero matches with `required: true`** → write
     `AIDPF-2010__<vp-name>.json` / `AIDPF-2011__<vp-name>.json`
     diagnostic artifact. Bootstrap COLLECTS all failures before
     exiting — multiple unresolved variation points produce multiple
     artifact files in one run.
4. **Persist** — on success, write profile YAML +
   `bronzeSchemaFingerprint` + evidence snapshot; preserve all
   prior snapshots.

New CLI flags on `bootstrap`:

* `--refresh` — re-walk every variation point against the live bronze;
  resolves drift per §9.5.5 Tier-1. No-op when the fingerprint matches
  the pinned one byte-for-byte. NEVER emits `AIDPF-2012` (runtime
  preflight owns that error code in feature #4).
* `--operator <string>` — explicit identity override
  (highest precedence).
* `--non-interactive` — sandbox/CI mode: multi-match auto-picks first
  candidate; `--refresh` refuses changes to pinned values
  (`RefreshRequiresConfirmation`).
* `--resolutions <json-file>` — scripted multi-match resolution.
  Schema documented in `docs/diagnostic-artifact-contract.md`.
* `--skip-preonboarding-probes` — skip phase-1 BICC / AIDP probes;
  useful for `--refresh` after initial onboarding succeeded.

Diagnostic artifact contract for feature #3
(`v2-phase-3b-medallion-author-skill`) consumption:
[`docs/diagnostic-artifact-contract.md`](./diagnostic-artifact-contract.md).

## Phase 3b additions — `medallion-author` Tier-2 overlay skill

When bootstrap (Phase 3a) exits 1 with `AIDPF-2010` / `AIDPF-2011`,
the operator's Tier-2 recovery path is the
[`medallion-author` Claude Code plugin skill](v2-medallion-author-skill.md).

The skill:

- Reads diagnostic artifacts feature #2 wrote under
  `<bundle.yaml.parent>/.aidp/diagnostics/<run_id>/`.
- Proposes new variation-point candidates from the tenant's
  observed bronze schema (seeded with `known-deltas.yaml`
  Fusion-release patterns).
- After operator approval, drafts an overlay at
  `<bundle.yaml.parent>/overlays/<overlay-name>/pack.yaml` that
  `extends:` the starter pack and ADDS candidates to existing
  `columnAliases` / `semanticVariants` (per §9.5.6 #1 MAY-NOT,
  never authors net-new SQL templates).
- Drafts a backend-aware remediation runbook recommending
  **Option D (targeted re-seed of affected nodes)** as the v0.3
  default. Options A (no-action), B (surgical MERGE), E (full
  re-seed) also surfaced. Option C (watermark rewind) is **deferred
  to v0.4** — requires an `aidp-fusion-bundle rewind` verb.

The skill is **operator-initiated** per ADR-0017; the CLI does NOT
auto-invoke Claude Code. Bootstrap is still the only writer to
`profiles/` and `evidence/` per §9.5.7 #6.

When the operator commits via `bootstrap --refresh` (or just
`bootstrap` for initial-onboarding AutoResolved flows), feature #2
detects the skill-authored overlay (via
`provenance.skillId == "aidp-fusion-medallion-author"`) and records:

- `mechanism: skill_proposed` on resolutions driven by the overlay
  (including AutoResolved on a skill-added candidate — Phase 3b
  round-2 finding).
- `SnapshotProvenance.skill_version` populated from the overlay.
- Per-resolution `incremental_impact` mirroring the overlay's
  `provenance.incrementalImpact[vp]`.

See [v2-medallion-author-skill.md](v2-medallion-author-skill.md)
for the operator UX walkthrough.

## Reference fixtures

Two layers of test fixtures live in the tree:

* **Phase 2 minimal pack** — `tests/fixtures/content_packs/phase2_test_pack/` +
  `tests/fixtures/projects/phase2_project/`. Mocked-Spark unit tests in
  `tests/unit/test_orchestrator_run_content_pack.py` prove the CLI flag
  reaches `sql_runner.execute_node`.
* **Phase 3 starter pack + example bundle** — `examples/fusion-finance-starter.yaml`
  + `examples/profiles/finance-default.yaml`. Pairs with the shipped
  starter pack at `scripts/oracle_ai_data_platform_fusion_bundle/content_packs/fusion-finance-starter/`.
  Five migrated SQL templates + one builtin route the content-pack
  backend end-to-end; smoke tests live in
  `tests/unit/test_phase3_starter_bundle_example.py`.

## Row-grain parity harness (active)

`tests/parity/test_starter_pack_parity.py` is a **fully-enabled
direct-SQL parity harness**, not a skipped skeleton. Run with
`pytest -m parity`. It:

1. Seeds a shared bronze schema with hand-crafted fixtures in
   `tests/parity/bronze_fixtures.py` (3 supplier rows, 4 COA rows,
   7 balance rows including sub-cent fractional amounts that exercise
   the `DECIMAL(28,2)` rounding contract, 6 invoice rows spanning
   aging buckets + multi-currency + NULL cancelled-date).
2. For each migrated node, executes both v1's SQL (via the v1 module's
   `build_<name>_sql()` helper, normalising `USING DELTA` →
   `USING PARQUET`) and v2's SQL (via `render_node_sql`) against the
   same bronze fixture data, writing to per-backend table-name suffixes
   (`..._v1` / `..._v2`).
3. Asserts:
   * **Schema-type equality** between v1 and v2 (Spark
     `dataType.simpleString` match including `decimal(p,s)`
     precision/scale).
   * **Multiset row equality** in both directions; Decimal values
     compared as Decimal (no float coercion).
   * **Audit-column presence + type** in both backends.
   * **xxhash64 surrogate parity** for `dim_supplier.supplier_key`
     and `dim_account.account_key`.

Direct-SQL was chosen over the original PLAN §15 Step 10 spec of
`orchestrator.run(...)` for both backends for three reasons: tighter
equivalence contract (no state-table / plan-hash noise on top of the
SQL), reproducibility on workstations without Delta Lake, and a
single shared bronze schema (per-backend table-name suffixes prevent
cross-contamination without two state-table setups). The module
docstring expands on the trade-off. A future `orchestrator.run`-based
harness can layer on top once the Delta-local-mode story is solved.
