---
name: aidp-fusion-autopilot
description: "End-to-end conductor for the Fusion -> AIDP -> OAC dashboard journey. Takes one high-level goal ('I want a supplier-spend vs GL-balance dashboard from Fusion') and drives the whole chain — configure -> bootstrap -> seed -> advise dataset -> author mart if needed -> create OAC dataset -> author workbook -> set up MCP chat — by detecting current state and delegating each phase to the right sibling skill/command. Auto-advances on clean steps; pauses for real decisions (destructive seed, variation freeze, OAC dataset creation, ambiguous intent, gaps). Use when the user states a dashboard/analytics goal and wants it driven start-to-finish, OR on first run after installing the plugin — 'I just installed this, what now', 'get me started', 'set me up', 'set up Fusion analytics from scratch / end to end', 'autopilot this', 'take me from nothing to a dashboard'. This is the front door for a fresh install (Phase 1 scaffolds the bundle). NOT for a single known step (call that skill directly)."
allowed-tools: Read, Bash, Glob, Grep, mcp__oac-mcp-server__oracle_analytics-search_catalog, mcp__oac-mcp-server__oracle_analytics-find_matching_datasources, mcp__oac-mcp-server__oracle_analytics-describe_data
---

# aidp-fusion-autopilot — one goal, driven A-to-Z

Turns *"I want a CFO dashboard of supplier spend vs GL balance, by currency"*
into a finished workbook by **conducting the existing skill family** — it does
not re-implement any step. Its only jobs are: **detect where the user already
is**, **drive the next incomplete phase via the right skill**, and **stop at the
decisions a human must make**.

This is the single entry point so the user never has to know which of the
seven skills to invoke, or in what order.

## When to use
- The user states a goal and wants it taken end-to-end ("take me from nothing
  to a dashboard", "set up Fusion analytics for X", "autopilot this").
- Resuming a half-done journey ("finish setting up my supplier dashboard").

## When NOT to use
- A single known step → call that skill directly (`/aidp-fusion-seed`,
  `/oac-dataset-advisor`, `/mart-author`, `/workbook-authoring`, …). Autopilot
  is overhead when the user already knows the one thing they want.

## Operating principles
1. **Compose, never reimplement.** Each phase delegates to a sibling skill or a
   CLI command. Autopilot owns only sequencing + state.
2. **State-first — don't redo finished work.** Detect each phase's status before
   acting (a tenant with gold already seeded skips straight to advise).
3. **Inherit the sub-skills' guards.** The seed destructive guard, the advisor's
   live-evidence rule, bootstrap's variation-freeze surfacing — autopilot never
   weakens them. It auto-advances only on a clean result.
4. **Pause at human decisions** (see gates). Auto-advance otherwise. Always show
   the journey state + what it's about to do before a state-changing phase.

---

## The journey (phase state machine)

| # | Phase | Done when (detect) | Drive with | PAUSE before if |
|---|---|---|---|---|
| 1 | **Config** | `bundle.yaml` + `aidp.config.yaml` exist; coords non-placeholder | `aidp-fusion-bundle init` (scaffold if absent — fresh install) → `/aidp-fusion-config` for coords | missing `fusion:` connectivity (human-only) |
| 2 | **Bootstrap** | `profiles/<tenant>.yaml` present + fingerprint pinned | `aidp-fusion-bundle bootstrap` | multi-match variation needs a human pick (never `--non-interactive`); surface frozen picks |
| 3 | **Seed** | live gold has the needed tables (probe) | `/aidp-fusion-seed` | always confirm the destructive guard's CONFIRM outcome |
| 4 | **Advise** | — (always run for the goal) | `/oac-dataset-advisor` | — |
| 5 | **Author mart** (only on advisor GAP) | the gap node exists live after seed | `/mart-author` → `use-pack` → back to phase 3 | confirm the authored change before seeding it |
| 6 | **OAC dataset** | a dataset over the recommended table(s) exists (OAC MCP) | advisor's recommendation | **always** — dataset creation is an OAC UI action today (MCP can't create datasets); hand the exact spec to the user |
| 7 | **Workbook** | a workbook on that dataset exists/renders | `/workbook-authoring` | confirm overwrite if replacing an existing workbook |
| 8 | **MCP chat** | `.mcp.json` wired + connector staged | `aidp-fusion-bundle dashboard mcp-setup` (basic auth, works in Claude Code) | confirm the OAC user is **least-privilege** (v1.4 exposes write/delete/ACL tools) |

## First run (fresh install)
If `bundle.yaml` / `aidp.config.yaml` don't exist yet (brand-new install), Phase
1 starts from zero: run **`aidp-fusion-bundle init`** to scaffold them, then
`/aidp-fusion-config` to resolve the AIDP coords from names. Capture the user's
goal first (even a rough one) so the rest of the journey has a target; if they
have no goal yet, scaffold + configure and stop there with "what dashboard do
you want?". Don't ask for OCIDs by hand — that's what `init` + `/aidp-fusion-config`
are for.

## The loop
1. **Capture the goal** — restate the dashboard the user wants (metrics,
   dimensions, grain). Keep it; every phase serves it.
2. **Assess state** — run the cheap detectors (below) to find the **first
   incomplete phase**. Report the full journey status (✓ done / ▶ next / ⏸ pause).
3. **Drive that phase** via its skill/command.
4. **On clean success → advance** to the next phase and repeat. **On a pause
   gate → stop, present the decision, wait** for the user.
5. **Stop when** phase 7 (workbook) is done — and offer phase 8 (MCP chat) if the
   user wants end-user querying.

## State detection (reuse the family's helpers — no new probes)
- **Config / profile / cluster:** `python3 skills/aidp-fusion-seed/preconditions.py
  --bundle bundle.yaml --config aidp.config.yaml --env <env>` → `{ok, missing[],
  config_placeholders[], cluster_state}`.
- **Live gold (what's seeded):** the advisor's live-catalog probe
  (`tests/live/aidp_catalog_probe_live.py` → `skills/oac-dataset-advisor/catalog_inventory.py`).
  **Live AIDP catalog is the evidence — never pack YAMLs.**
- **What the pack could build (seed-vs-gap routing):**
  `python3 skills/oac-dataset-advisor/pack_capability.py`.
- **Existing OAC datasets / workbooks:** OAC MCP `search_catalog` /
  `find_matching_datasources` / `describe_data`.

## Pause gates (never auto-do these)
- **Destructive seed** — honour `/aidp-fusion-seed`'s fail-closed guard; if it
  says CONFIRM, surface the affected tables and wait.
- **Bootstrap variation freeze** — surface the resolved column-alias /
  semantic-variant picks; on a multi-match, let a human choose.
- **Mart authoring** — show `/mart-author`'s chosen change (rung, blast radius)
  before authoring/seeding it.
- **OAC dataset creation** — autopilot cannot create the dataset (MCP has no
  create-dataset tool); present the exact spec (tables, columns, join key) for
  the user to create in the OAC UI, then continue.
- **Missing `fusion:` connectivity / non-least-privilege MCP user** — stop and ask.
- **Anything overwriting populated data or external state.**

## Output
A compact journey ledger each turn — e.g.:
```
goal: supplier spend vs GL closing balance, by currency
[✓] config   [✓] bootstrap   [✓] seed (gl_balance, supplier_spend live)
[✓] advise → COVERED: dataset over gl_balance + supplier_spend on currency_code
[⏸] OAC dataset → create in OAC UI (spec below), then I continue
[ ] workbook   [ ] mcp chat
```
On a pause, state exactly what you need from the user and which phase resumes.

## Skill family (what autopilot conducts)
`/aidp-fusion-config` · `bootstrap` / `medallion-author` · `/aidp-fusion-seed` ·
`/oac-dataset-advisor` · `/mart-author` (+ `use-pack`) · `/workbook-authoring` ·
`dashboard mcp-setup`. Autopilot adds no mechanism — it sequences these and
holds the user's goal across them.

## Safety invariants (do not regress)
- Never weaken a sub-skill's guard to "keep moving."
- Never claim a phase is done without its detector confirming it (live evidence
  for seed/gold; OAC MCP for dataset/workbook).
- Never create the OAC dataset or seed populated data without the gate.
- Surface every irreversible/external action before doing it.
