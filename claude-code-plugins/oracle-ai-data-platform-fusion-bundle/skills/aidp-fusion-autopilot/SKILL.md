---
name: aidp-fusion-autopilot
description: "End-to-end conductor for the Fusion -> AIDP -> OAC dashboard journey. Takes one high-level goal ('I want a supplier-spend vs GL-balance dashboard from Fusion') and drives the whole chain — configure -> connect OAC MCP (prerequisite for the OAC phases) -> bootstrap -> seed -> advise dataset -> author mart if needed -> create OAC dataset -> author workbook -> (optional) enable end-user MCP chat — by detecting current state and delegating each phase to the right sibling skill/command. Auto-advances on clean steps; pauses for real decisions (destructive seed, variation freeze, OAC dataset creation, ambiguous intent, gaps). Use when the user states a dashboard/analytics goal and wants it driven start-to-finish, OR on first run after installing the plugin — 'I just installed this, what now', 'get me started', 'set me up', 'set up Fusion analytics from scratch / end to end', 'autopilot this', 'take me from nothing to a dashboard'. This is the front door for a fresh install (Phase 1 scaffolds the bundle). NOT for a single known step (call that skill directly)."
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
| **1b** | **OAC MCP connect** (front-loaded prerequisite — see §below) | `oac-mcp-server` tools answer a live `search_catalog` ping | `aidp-fusion-bundle dashboard mcp-setup` / `mcp-token`, then **restart/reconnect Claude Code** | **always when dead** — staging the connector/token needs a Claude Code restart before its tools work; PAUSE for the restart, then resume here |
| 2 | **Bootstrap** | `profiles/<tenant>.yaml` present + fingerprint pinned | `aidp-fusion-bundle bootstrap` | multi-match variation needs a human pick (never `--non-interactive`); surface frozen picks |
| 3 | **Seed** | live gold has the needed tables (probe) | `/aidp-fusion-seed` | always confirm the destructive guard's CONFIRM outcome |
| 4 | **Advise** | — (always run for the goal) | `/oac-dataset-advisor` | — |
| 5 | **Author mart** (only on advisor GAP) | the gap node exists live after seed | `/mart-author` → `use-pack` → back to phase 3 | confirm the authored change before seeding it |
| 6 | **OAC dataset** | a dataset over the recommended table(s) exists (OAC MCP) | advisor's recommendation | **always** — dataset creation is an OAC UI action today (MCP can't create datasets); hand the exact spec to the user |
| 7 | **Workbook** | a workbook on that dataset exists/renders | `/workbook-authoring` | confirm overwrite if replacing an existing workbook |
| 8 | **End-user MCP chat** (optional deliverable) | least-privilege OAC user handed the connect steps | `docs/oac_mcp_setup.md` hand-off | confirm the end-user OAC account is **least-privilege** (v1.4 exposes write/delete/ACL tools) |

> **Phases 6–7 read OAC through the `oac-mcp-server` tools** (autopilot's own
> detectors *and* `/workbook-authoring`'s required tools). That connection is
> **Phase 1b**, a prerequisite — not Phase 8. Phase 8 is a *different* thing:
> enabling *end users* to chat with their own clients. Don't conflate them.

## Phase 1b — OAC MCP connectivity (front-loaded prerequisite)

**Why front-loaded (not Phase 8).** `/workbook-authoring` and autopilot's own
phase-6/7 detectors *consume* the `oac-mcp-server` tools (`search_catalog`,
`describe_data`, save-validation). They are a **prerequisite** for the OAC half
of the journey. And establishing the connector/token (`dashboard mcp-setup` /
`mcp-token`) requires a **Claude Code restart/reconnect** before the tools come
alive (`docs/oac_mcp_setup.md` — MCP servers bind at session start; a mid-flow
setup can't transparently light them up). So the connection is set up **early,
once**, before the minutes-long Bootstrap/Seed — by the time those finish the
OAC tools are live and phases 6–7 flow with no further interruption.

**Run it as early as OAC coords exist.** It needs the OAC instance + creds, which
come from Config — so on a fresh install Phase 1b falls **right after Phase 1**,
before Bootstrap. On an already-configured tenant, probe it first thing.

**The gate:**
1. **Probe liveness** — a cheap `oracle_analytics-search_catalog` ping (or
   `claude mcp list` → expect `oac-mcp-server ✔ Connected`).
2. **Live → continue** to Bootstrap; record `[✓] mcp connect`.
3. **Dead → set up + restart.** Run `aidp-fusion-bundle dashboard mcp-setup`
   (or `mcp-token` for the non-interactive path — Claude Code can't do the
   browser elicitation; see the OAC-MCP-auth root cause). This stages the
   connector, writes the 0600 token file, and wires `.mcp.json`. Then **PAUSE
   with an explicit handoff**: *"OAC MCP staged — restart/reconnect Claude Code
   (`/mcp` → reconnect `oac-mcp-server`, or relaunch), then re-run autopilot; it
   resumes at the next phase."* Autopilot is state-first, so re-invoking it after
   the restart just continues. **Do not** try to proceed into phases 6–7 on a
   dead connection.

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
5. **Stop when** phase 7 (workbook) is done — and offer phase 8 (end-user MCP
   chat) if the user wants downstream clients to query OAC directly.

**The one hard ordering rule:** never enter phases 6–7 with a dead
`oac-mcp-server`. If the Phase 1b probe fails, set up + hand off the restart and
stop there — re-running autopilot after the restart resumes the journey.

## State detection (reuse the family's helpers — no new probes)
- **Config / profile / cluster:** `python3 skills/aidp-fusion-seed/preconditions.py
  --bundle bundle.yaml --config aidp.config.yaml --env <env>` → `{ok, missing[],
  config_placeholders[], cluster_state}`.
- **Live gold (what's seeded):** the advisor's live-catalog probe
  (`tests/live/aidp_catalog_probe_live.py` → `skills/oac-dataset-advisor/catalog_inventory.py`).
  **Live AIDP catalog is the evidence — never pack YAMLs.**
- **What the pack could build (seed-vs-gap routing):**
  `python3 skills/oac-dataset-advisor/pack_capability.py`.
- **OAC MCP liveness (Phase 1b gate):** a cheap `oracle_analytics-search_catalog`
  ping, or `claude mcp list` (expect `oac-mcp-server ✔ Connected`). A failure /
  auth error means **route to Phase 1b setup + restart** — it does **not** mean
  the catalog is empty.
- **Existing OAC datasets / workbooks:** OAC MCP `search_catalog` /
  `find_matching_datasources` / `describe_data`. **Only trust these once Phase 1b
  is green** — a dead connection returning nothing must never be read as "no
  dataset/workbook exists" (false negative → autopilot would wrongly try to
  re-author).

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
- **OAC MCP not connected (Phase 1b)** — stage it (`dashboard mcp-setup` /
  `mcp-token`), then **stop for the operator to restart/reconnect Claude Code**;
  the tools can't activate inside the current session. Resume on re-run.
- **Missing `fusion:` connectivity / non-least-privilege MCP user** — stop and ask.
- **Anything overwriting populated data or external state.**

## Output
A compact journey ledger each turn — e.g.:
```
goal: supplier spend vs GL closing balance, by currency
[✓] config   [✓] mcp connect   [✓] bootstrap   [✓] seed (gl_balance, supplier_spend live)
[✓] advise → COVERED: dataset over gl_balance + supplier_spend on currency_code
[⏸] OAC dataset → create in OAC UI (spec below), then I continue
[ ] workbook   [ ] end-user mcp chat (optional)
```
On a pause, state exactly what you need from the user and which phase resumes.

## Skill family (what autopilot conducts)
`/aidp-fusion-config` · `bootstrap` / `medallion-author` · `/aidp-fusion-seed` ·
`/aidp-fusion-status` · `/oac-dataset-advisor` · `/mart-author` (+ `use-pack`) ·
`/workbook-authoring` · `dashboard mcp-setup`. Day-2: `/aidp-fusion-incremental`
(deltas) with `/fusion-drift-doctor` as its drift precheck. **On any gate
failure (AIDPF-2072/4070/4071/2012/4040 — schema/PVO drift, plan-hash) route to
`/fusion-drift-doctor`**, which diagnoses and hands to `bootstrap --refresh` /
`/medallion-author` / re-seed. Autopilot adds no mechanism — it sequences these
and holds the user's goal across them.

## Safety invariants (do not regress)
- Never weaken a sub-skill's guard to "keep moving."
- Never claim a phase is done without its detector confirming it (live evidence
  for seed/gold; OAC MCP for dataset/workbook).
- Never enter phases 6–7 with a dead `oac-mcp-server`, and never read a dead/
  unauthenticated MCP connection as an empty catalog — set up Phase 1b + restart
  first.
- Never create the OAC dataset or seed populated data without the gate.
- Surface every irreversible/external action before doing it.
