---
name: medallion-author
description: "Draft a content-pack overlay extending the starter pack's variation-point candidate lists when `aidp-fusion-bundle bootstrap` fails AIDPF-2010 / AIDPF-2011. Reads diagnostic artifacts feature #2 wrote under `.aidp/diagnostics/<run_id>/`, proposes new candidates from the observed bronze schema, presents a draft overlay for operator approval, and drafts a backend-aware remediation runbook (Option A/B/D/E; Option C deferred to v0.4). Use when the CLI exits 1 with AIDPF-2010 or AIDPF-2011 on a fresh tenant or after a Fusion-release upgrade. NOT for runtime drift detection (feature #4 owns that) or for authoring net-new silver/gold nodes (§9.5.6 #1 MAY-NOT forbids skill-authored SQL templates)."
---

# medallion-author — Tier-2 overlay-author skill

When `aidp-fusion-bundle bootstrap` cannot mechanically resolve a
variation point (no candidate the pack declares is present on the
tenant's bronze), it writes a diagnostic artifact and exits non-zero.
This skill is the recovery path: read the artifact, propose new
candidates from the observed bronze schema, present a draft overlay
for operator approval, and draft a remediation runbook. The operator
runs `bootstrap --refresh` with the overlay applied; the engine
(feature #2) commits the resolution.

**Architectural authority**:
`dev/PLAN_plugin_engine_medallion_content_packs.md`
§9.5.4.1 (CLI ↔ skill one-way interaction), §9.5.5 Tier-2 (overlay
drafting), §9.5.6 (7 MUSTs / 3 MAY-NOTs), §9.5.7 (storage rules),
§9.5.8 (loose version coupling), §9.5.9 (`skill_proposed` mechanism),
ADRs 0014 / 0017 / 0019 / 0021.

## When to use

- The CLI exited 1 with `AIDPF-2010` or `AIDPF-2011`.
- The operator wants to author a pack overlay extending the starter
  pack with new variation-point candidates observed on their tenant.
- Mid-Fusion-upgrade recovery: a column got renamed and the existing
  candidate list no longer matches.

## When NOT to use

- During `run --mode seed` / `run --mode incremental` — the engine
  has zero LLM dependency at runtime per ADR-0017. This skill is
  operator-initiated and runs between bootstraps.
- When the diagnostic artifact is missing — the skill refuses to
  invent context (no synthesised proposals from thin air).
- When `AIDPF-1020` is present — operator-identity gate is unrelated
  to overlay drafting; fix the identity environment first.
- To author NEW silver/gold nodes (a SQL template the pack doesn't
  declare). Per §9.5.6 #1 MAY-NOT, the skill only EXTENDS existing
  variation-point candidate lists.

## AIDPF-2012 read-only context (Phase 3c + 3d)

When a runtime drift artifact (`AIDPF-2012.json`) is present alongside
2010/2011 in the same `<run_id>/` directory, the reader exposes it via
`DiagnosticReadResult.schema_drift_failure`. The skill SURFACES this
to the operator as context but DOES NOT act on it — drift recovery is
`bootstrap --refresh`, not an overlay draft.

As of **Phase 3d** the artifact's
`schema_drift_failure.schema_drift.dataset_deltas` field is populated
whenever bootstrap pinned a `profiles/<tenant>.schema-snapshot.yaml`
file (Phase 3a-vintage bootstraps and earlier still emit empty
`dataset_deltas` and a one-time WARN on the operator's terminal —
remediation is the same `--refresh`). When `dataset_deltas` is
non-empty, each entry carries `addedColumns` / `removedColumns` /
`typeChangedColumns` lists with the operator-facing original casing
preserved. Surface those directly in any human hand-off message
instead of asking the operator to re-probe + diff manually against
the most recent evidence snapshot. The reader contract is unchanged
— the field just becomes useful.

## The seven §9.5.6 MUSTs (verbatim)

1. **Probe bronze before declaring**. Skill reads the diagnostic
   artifact's `observedBronzeSchema`; never invents columns.
2. **Identify variation points via neighbour-tenant + release-delta
   knowledge**. Skill consults `known-deltas.yaml` for documented
   Fusion-release patterns.
3. **Add declarations to the pack overlay, not the profile**. Skill
   writes `overlays/<name>/pack.yaml`; never `profiles/<tenant>.yaml`
   (that's bootstrap's domain).
4. **Use `{{ column.<name> }}` / `{{ semantic.<name> }}` tokens, never
   hardcoded column names**. The skill ADDS candidates to existing
   `columnAliases` / `semanticVariants`; the SQL templates that
   reference the tokens are unchanged.
5. **Run `bootstrap --refresh` for final commit**. Skill never invokes
   bootstrap itself — operator gates every commit. §9.5.7 #6
   preserved (bootstrap is the only writer to `profiles/` and
   `evidence/`).
6. **Never hand-write `profile.resolved.*`**. Skill emits an
   overlay; bootstrap pins the resolved values.
7. **Stamp every artifact with `provenance` metadata**. Overlay
   carries `skillId`, `skillVersion`, `modelId`, `generatedAt`,
   `diagnosticRunId`, `proposals`, `incrementalImpact`.

## The three §9.5.6 MAY-NOTs (verbatim)

1. **Never hardcode the winning candidate into a SQL template**.
   The skill's drafter rejects any overlay that introduces a node
   definition or override block (`OverlayValidationError`).
2. **Never add candidates without observed-tenant evidence**. The
   reasoner only scores columns present in the diagnostic artifact's
   `observedBronzeSchema`.
3. **Never skip the human-approval step**. The propose phase shows
   each candidate to the operator for explicit Y/N approval.

## The 8-step workflow

When invoked via `/medallion-author [<run_id>]`:

### 1. Discovery (read-only)

- Scan `<workdir>/.aidp/diagnostics/<run_id>/` via
  `medallion_author.reader.read_run`.
- If `run_id` is omitted, auto-discover the latest run.
- Surface the parsed `VariationPointDiagnosticV1` entries to the
  operator: which VPs failed, which candidates were tried, what's
  observed on bronze.

### 2. Refuse-to-proceed gates

- `AIDPF-1020.json` present → refuse: "identity gate must be fixed
  before overlay drafting".
- Unknown `schemaVersion` → refuse per §9.5.8.
- Diagnostics directory missing / empty → refuse.

### 3. Pack load + affected-nodes computation

- Load the resolved pack via
  `orchestrator.content_pack.load_full_chain` (overlay chain
  included if any).
- For each failing VP, compute affected silver/gold node IDs via
  `medallion_author.affected_nodes.compute_affected_nodes`.

### 4. Propose phase (LLM-driven)

- For each failing VP, call
  `medallion_author.reasoner.score_candidates(...)` to get a ranked
  top-3 of observed columns (with confidence labels + KB hints).
- For refresh promotions (when `priorPinned` is set), call
  `classify_incremental_risk(...)` for the risk label.
- Write a one-paragraph reasoning per proposal — surface the KB hint
  rationale when present.

### 5. Operator review

Present each proposal in this shape:

```
Proposed candidate for invoice_currency_code:
  + ApInvoicesXCurrCode  (confidence: high, KB-hint: currency-code-casing-variants)
Rationale: Fusion 25C renamed CurrencyCode → XCurrCode for multi-currency
tenants. The known-deltas KB confirms this pattern. Observed on the
tenant's bronze.ap_invoices schema.
Incremental impact: likely-different-semantics; Option D recommended.
Affected nodes: silver.supplier_spend, silver.ap_aging, gold.supplier_spend, gold.ap_aging.

Approve this proposal? [y/N/edit]:
```

Operator approves / edits / rejects each proposal independently.

### 6. Draft phase (writes)

After approval, call:

- `medallion_author.drafter.draft_overlay(...)` — assembles a
  validated `PackYaml` overlay.
- `medallion_author.drafter.write_overlay(...)` — writes to
  `<workdir>/overlays/<overlay_name>/pack.yaml`.
- `medallion_author.drafter.write_resolutions(...)` — **conditional**.
  Returns `None` for initial-onboarding AutoResolved picks (no
  resolutions file needed; bootstrap walks the extended candidate
  list and AutoResolves trivially). Emits the file for MultiMatch
  or RefreshChange picks.
- `medallion_author.runbook.draft_remediation(...)` — drafts
  `remediation.md` (always) + `remediation.sql` (Option B only).
- `medallion_author.drafter.write_skill_evidence(...)` — drafts
  `skill-evidence.json` (skill's audit trail).

### 7. Hand-off

Print one of FOUR templates per the conditional + backend matrix:

#### 7a. Initial-onboarding + content-pack backend

```
Overlay drafted: overlays/<overlay-name>/pack.yaml
Remediation:    overlays/<overlay-name>/remediation.md (Option D)

Next steps:
  1. Review the overlay + remediation.md.
  2. Wire the overlay into the pack chain (set
     bundle.yaml contentPack.path to the overlay dir).
  3. Re-run bootstrap (NO --resolutions flag needed — the
     extended candidate list AutoResolves):
       aidp-fusion-bundle bootstrap --operator "$USER"
  4. Apply Option D remediation per remediation.md (targets
     affected pack silver/gold node IDs):
       aidp-fusion-bundle run --mode seed \
         --execution-backend content-pack \
         --datasets <silver/gold-node-ids>
  5. Resume scheduled `aidp-fusion-bundle run --mode incremental`.
```

#### 7b. Initial-onboarding + legacy backend

Steps 1–3 identical to 7a; step 4 below:

```
  4. Apply Option D remediation per remediation.md:
       aidp-fusion-bundle run --mode seed --datasets <legacy-ids>
     (where <legacy-ids> are SILVER_DIMS / GOLD_MARTS keys for the
     affected pack nodes — see remediation.md. NO --execution-backend
     flag — legacy is the CLI default.)
```

#### 7c. MultiMatch / refresh-promotion + content-pack backend

```
Overlay drafted: overlays/<overlay-name>/pack.yaml
Resolutions:    overlays/<overlay-name>/resolutions.json
Remediation:    overlays/<overlay-name>/remediation.md (Option D)

Next steps:
  1. Review the overlay + resolutions.json + remediation.md.
  2. Commit the resolution:
       aidp-fusion-bundle bootstrap --refresh \
         --operator "$USER" \
         --resolutions overlays/<overlay-name>/resolutions.json
  3. Apply Option D remediation per remediation.md:
       aidp-fusion-bundle run --mode seed \
         --execution-backend content-pack \
         --datasets <silver/gold-node-ids>
  4. Resume scheduled `aidp-fusion-bundle run --mode incremental`.
```

#### 7d. MultiMatch / refresh-promotion + legacy backend

As 7c but step 3 omits `--execution-backend` and uses mapped legacy
IDs from `remediation.md`.

**Legacy-backend fallback**: when an affected pack node has no
`SILVER_DIMS` / `GOLD_MARTS` mapping, `runbook.draft_remediation`
raises `UnmappedLegacyNodeError`; the skill emits a remediation.md
recommending Option B (surgical MERGE) or Option E (full re-seed)
instead.

### 8. Provenance trail

Every artifact the skill emits carries:

- `skillId: aidp-fusion-medallion-author`
- `skillVersion: 0.1.0` (bumped on every `known-deltas.yaml` change)
- `modelId: <claude-model-id>` (from runtime)
- `generatedAt: <iso-8601-utc>`
- `diagnosticRunId: <run_id>`
- `proposals[<vp-name>]: {candidateAdded, confidence, reasoning}`
- `incrementalImpact[<vp-name>]`: change kind, prior pinned, new
  candidate, risk label, affected nodes, remediation choice.

Feature #2's bootstrap reads `provenance.skillId` on commit and:

- Records `mechanism: skill_proposed` on resolutions driven by the
  overlay (including AutoResolved on a skill-added candidate).
- Populates `SnapshotProvenance.skill_version` from the overlay.
- Mirrors `provenance.incrementalImpact[vp]` into the snapshot's
  per-resolution `incremental_impact` field.

## Decision: which option to recommend?

| Risk label | Recommendation | Notes |
|---|---|---|
| `likely-rename` | Option A | No silver/gold re-seed needed. |
| `likely-different-semantics` | **Option D** (v0.3 default) | Targeted re-seed of affected nodes via the engine's tested seed path. |
| `unknown` | Option D + operator confirmation | Surface the uncertainty; operator's call. |

Option B is an advanced opt-in when the affected tables are too
large for Option D AND the column substitution is genuinely
surgical (no derived columns / joins reference the VP).

Option C (watermark rewind) is **deferred to v0.4** — requires an
`aidp-fusion-bundle rewind` verb that knows both legacy and
content-pack state contracts. Round-2 review evidence:
`orchestrator/preflight.py:400` raises `IncrementalCursorMissingError`
on NULL silver/gold cursors; content-pack
`orchestrator/__init__.py:1591-1596` reads `output_watermark`
filtered by `source_role='primary'`. Until the rewind verb ships,
`draft_remediation(option="C", ...)` raises `OptionDeferredError`
with a redirect message.

Option E (full re-seed) is the audit-baseline reset — rare.

## Dependencies

- Reads `<workdir>/.aidp/diagnostics/<run_id>/*.json` (feature #2's
  artifacts). Schema in
  `scripts/.../schema/diagnostic_artifact.py`.
- Writes only to `<workdir>/overlays/<overlay-name>/`. All other
  paths are off-limits — `Write` tool calls outside this subtree
  are rejected by the path-segment validator.
- Imports `medallion_author.*` from
  `scripts/.../medallion_author/` (real Python package, pip-installable).
- Reads `known-deltas.yaml` from the package dir.

## Skill version policy

`SKILL_VERSION = "0.1.0"`. Bumped on every change to:

- `known-deltas.yaml` (new entry → patch; new VP family → minor).
- Schema of the overlay's `provenance` block (breaking → major).
- The 8-step workflow contract (breaking → major).

Skill version is loose-coupled per §9.5.8 — recorded in evidence
snapshots as audit metadata, NOT a plan-hash input.
