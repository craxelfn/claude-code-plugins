---
name: mart-author
description: "Author a new medallion node (gold mart, silver dim, or additive column) when the live gold layer AND the content pack both cannot serve a business need. Takes the user's business logic, inspects the Fusion PVO SOURCE schema (not bronze) for available raw fields, and authors the lowest-cost, additive, non-destructive change as content-pack YAML + SQL in an overlay pack — never touching already-materialized (possibly terabyte-scale) bronze/silver. Then validates and hands off to seed. Use when oac-dataset-advisor reports a true GAP, or the user says 'add a metric/dimension my gold layer doesn't have', 'create a new mart for <business logic>', 'I need a column that doesn't exist'. Does NOT seed, query live data, alter existing nodes' grain/keys, or write Python dim modules."
allowed-tools: Read, Write, Edit, Bash, Glob, Grep, mcp__oac-mcp-server__oracle_analytics-search_catalog, mcp__oac-mcp-server__oracle_analytics-describe_data
---

# mart-author — author a new medallion node, additively and cheaply

When the gold layer genuinely can't serve a dashboard, this skill extends the
medallion **as v2 content-pack artifacts** (YAML + SQL), authored like a
careful data engineer: the smallest, additive, lowest-cost change that serves
the need **without disturbing already-materialized delta**.

It is the GAP-handler of the skill family. It does **not** seed, query live
data, create OAC datasets, or write `dim_*.py` modules (v2 forbids the last —
content-pack YAML+SQL only; `tests/architectural/test_no_new_legacy_modules.py`
enforces it).

> Not to be confused with **`medallion-author`**, which resolves *tenant
> variation* (column aliases / semantic variants) into an overlay. This skill
> authors *new analytical content*. Both write to overlay packs.

## When to use
- `oac-dataset-advisor` reports a **true GAP** (neither live gold nor the pack's
  buildable menu can serve the request).
- "Add a metric/dimension my gold layer doesn't have", "create a mart for
  <business logic>", "I need a column that isn't there".

## When NOT to use
- The data exists but isn't materialized → `aidp-fusion-bundle run --mode seed`
  (or `/aidp-fusion-seed`); use `oac-dataset-advisor` to confirm.
- Resolving column-alias / semantic-variant tenant variation → `medallion-author`.
- Building the OAC dataset/workbook → `oac-dataset-advisor` + `workbook-authoring`.

## Non-negotiable safety rules
1. **Never disturb living delta.** Reading existing bronze/silver is fine;
   rewriting/altering/reprocessing them is forbidden (they may be terabytes).
2. **Additive only.** New nodes, or a new column on an existing node — never a
   change to an existing node's **grain or natural key**.
3. **New bronze extracts are additive** — a new `bronze_extract` node, never an
   edit to an existing one.
4. **Write to a persistent overlay pack beside the bundle**
   (`<bundle.yaml.parent>/overlays/<name>/`) — never `/tmp`, never the shipped
   installed `content_packs/` tree.
5. **Inspect the Fusion PVO source schema, not bronze**, to discover raw fields
   (metadata-only; cheap; authoritative for "what could be extracted").

## Helpers

| File | Role | Invoked via |
|---|---|---|
| `change_planner.py` | Given where each needed field is sourced (existing layer / PVO-only / missing), picks the lowest-cost rung on the change ladder and emits a node spec with audit columns + refresh strategy + currency-in-grain checks pre-stamped. | `Bash`, JSON in/out |
| `../oac-dataset-advisor/catalog_inventory.py` | Live materialized tables (what exists). | reuse |
| `../oac-dataset-advisor/pack_capability.py` | Pack's buildable menu (what nodes already exist). | reuse |

## The change-strategy ladder (cheapest first)

| Rung | Build | When |
|---|---|---|
| **3 — add column** | additive `outputSchema` + `SELECT` on an existing node | new field derives from columns already in that node's sources, **same grain** |
| **1 — new gold** | new aggregate/business mart over EXISTING bronze/silver | a new metric/grain from already-materialized data |
| **2 — new silver** | new conformed/typed node over EXISTING bronze | a conformed shape not yet in silver |
| **4 — new bronze + node** | additive `bronze_extract` + downstream node | a raw field isn't extracted yet (only at the PVO source) |

`change_planner.py` chooses the rung and refuses to mark any existing node for
alteration.

---

## Workflow

### 1 — Frame the gap
Restate the missing metric/dimension/grain (ideally from `oac-dataset-advisor`'s
GAP output) and the business logic. Confirm the intended output grain with the user.

### 2 — Inventory what already exists (read-only)
Run the advisor's helpers to find the closest existing data to build on:
- live materialized tables — `catalog_inventory.py` (what exists);
- existing pack nodes/columns — `pack_capability.py` (what's already declared).
Do not scan bronze data — you only need the column inventory.

### 3 — Probe the Fusion PVO SOURCE schema (metadata-only)
For any field the business logic needs that is NOT already in an existing
table, confirm it exists at source and get its real name/type **from the PVO,
not bronze**:
```bash
aidp-fusion-bundle catalog probe --pod <url>            # list/reconcile PVOs
aidp-fusion-bundle catalog probe-pvo <dataset_id> \      # one PVO's schema, metadata-only
  --datastore <DatastorePVO> --bicc-schema <Financial|HCM|SCM> \
  --emit-pack-yaml overlays/<name>/bronze/<id>.yaml   # persistent, beside bundle.yaml
```
`probe-pvo` does a schema-only roundtrip (no row pull) and emits a **draft
bronze YAML** — the additive extract for rung 4.

### 4 — Plan the change (pick the rung)
Build a field-resolution map — for each required field, where is it sourced?
`existing_gold` / `existing_silver` / `existing_bronze` / `pvo_only` / `missing`
— then:
```bash
python3 change_planner.py --input change_request.json
```
Returns `{decision, reason, blastRadius, requiresNewBronze, missingFields,
warnings, touchesLivingDelta, nodeSpecs}`. Act on it:
- **`hard_gap`** (a field exists nowhere, not even at the PVO) → stop and tell
  the user it can't be served as specified; name the missing field(s).
- otherwise → present the chosen rung + blast radius to the user before writing,
  and resolve any `warnings` (e.g. add `currency_code` to an aggregate's grain).

### 5 — Author the artifacts (correct-by-construction)
Write to a **persistent overlay pack beside the bundle**:
**`<bundle.yaml.parent>/overlays/<name>/`** (e.g. `overlays/fusion-finance-ar-ext/`),
with `pack.yaml` declaring `extends: fusion-finance-starter@<version>`. This is
the canonical home — mirrors `medallion-author`'s write boundary, survives
reboots, and is what the customer commits/points the bundle at.
**Never** write to a temp dir (`/tmp` is lost on reboot) and **never** to the
shipped installed `content_packs/` tree. For each `nodeSpec`:
- **`<id>.yaml`** — `implementation.type: sql` (or `bronze_extract`),
  `dependsOn`, the planner's `refresh` strategy **with its documented reason**,
  and `outputSchema.columns` with a **mandatory `pii` classification per column**
  (missing → AIDPF-2030). High-PII columns must not be exposed to dashboards.
- **`<id>.sql`** — Jinja template enforcing the medallion invariants:
  `COALESCE(...,0)` around every amount arithmetic; **currency in the grain** of
  any amount aggregate; deterministic **`xxhash64(natural_key)`** surrogate keys
  (never `monotonically_increasing_id`); audit columns (`{{ run_id_literal }}` →
  `*_run_id`, `*_built_at`); single financially-correct shape (LEFT JOIN, fact
  preserved) over runtime path-selection; variation-point refs
  `{{ column.<name> }}` / `{{ semantic.<name> }}` where the tenant may differ.
- (rung 4) the `bronze_extract` YAML from step 3's `probe-pvo` (additive).

### 6 — Validate
```bash
aidp-fusion-bundle content-pack validate <overlay>
```
Fix until clean — schema + content validators cover PII-missing (AIDPF-2030),
dependency/SQL integrity, and the no-new-legacy-module rule. Document new error
codes in `docs/aidpf-error-codes.md`.

### 7 — Wire the bundle for the client (one command), then hand off to seed
An overlay isn't seeded until the bundle points at it. **Do this FOR the
client** with the single wiring verb — don't hand-edit YAML:

```bash
aidp-fusion-bundle use-pack overlays/<name> --profile <tenant>
```

Use the default aligning behavior when the client wants the bundle scope to
match every silver/gold node in the resolved pack. For narrow customer bundles
or a one-mart SQL override, preserve the existing scope instead:

```bash
aidp-fusion-bundle use-pack overlays/<name> --profile <tenant> --no-align
```

When using `--no-align` for a new mart, add only the authored node to
`gold.marts` before seeding; otherwise `--datasets <new-id>` is outside the
bundle scope and the resolver fails with `AIDPF-1043`.

`use-pack` does the content-pack wiring in one step: sets
`contentPack: {name: <overlay-id>, path: overlays/<name>, profile}`, aligns
`dimensions.build` / `gold.marts` to the resolved pack's real nodes unless
`--no-align` is supplied (so stale v1 entries like `dim_org` / `po_backlog`
can't break the plan resolver in full-scope wiring), and
**normalizes a placeholder-vault `fusion.password` to `${FUSION_BICC_PASSWORD}`**
(the cluster loads it from the AIDP credential store; a placeholder vault ref
fails with `CredentialResolutionError`). It's comment-preserving and validates
the result. Then:

1. **Profile present** — `use-pack` warns if `profiles/<tenant>.yaml` is absent;
   run `bootstrap` (or reuse one) so it exists.
2. **Config coords** — if `aidp.config.yaml` coords are missing/placeholder,
   route to `/aidp-fusion-config` (don't make the client hand-copy OCIDs). Any
   remaining `${ENV}` ref in `bundle.yaml` must resolve **both** client-side
   (preflight) and cluster-side (literalize tenant values or set the env var).
3. **Hand to the seed step** — `/aidp-fusion-seed` (or
   `aidp-fusion-bundle run --mode seed --datasets <new-id> --layers gold`).
   `--layers gold` lets the bronze-readiness gate verify the existing bronze
   dep instead of re-extracting it from BICC (the plan still lists the bronze
   dep; it is read, not rebuilt).
5. **Re-run `oac-dataset-advisor`** — it now sees the new **live** table and
   recommends the OAC dataset; then `workbook-authoring` builds the viz.

> **Overlay-on-installed-base requires plugin ≥ the `chain_roots` staging fix**
> (`content_pack_staging.py`): before it, seeding any overlay raised
> `AIDPF-1040` because inherited base-pack nodes weren't staged. See LIMITS.md.

---

## Skill family
`oac-dataset-advisor` (GAP) → **`mart-author`** (this skill: author node) →
`aidp-fusion-seed` (materialize) → `oac-dataset-advisor` (now COVERED) →
`workbook-authoring` (visualize). New content always lands as content-pack
YAML+SQL in an overlay, per ADR-0021 / CLAUDE.md "where new work goes".

## Safety invariants (do not regress)
- Author content-pack YAML+SQL only — **never** a new `dim_*.py` / gold `.py`.
- **Additive, non-destructive** — new node or new column; never alter an
  existing node's grain/keys, never rewrite materialized tables.
- **PVO, not bronze**, for source-field discovery (metadata-only).
- **PII mandatory** on every authored column; keep high-PII out of dashboards.
- **Overlay pack only** — never edit the shipped starter pack.
- Don't seed, don't query live data, don't create OAC datasets — hand off.
