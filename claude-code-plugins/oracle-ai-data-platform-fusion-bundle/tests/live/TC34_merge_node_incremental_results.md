# TC34 — MERGE-node content-pack incremental (mode-normalized plan-hash)

**Test case ID**: TC34
**Status**: ⏸️ **BLOCKED (2026-06-15)** — upstream Fusion BICC service returned
`CONNECTOR_0255` ("system level error from Fusion Applications … Service
Temporarily unavailable") on two consecutive dispatch attempts. The blocker is
an **upstream Fusion source outage**, *not* the code under test. Re-run when the
pod recovers.
**Tracks**: first live-green **row-grain MERGE-node** `--mode incremental`
(closes `LIMITS.md` P-incr-L1 — the mode-normalized plan-hash / Approach 3 fix +
the `--repin-plan-hash` break-glass). Sibling to TC33 (which proved the
*replace*-strategy incremental on `ar_invoice_summary`).

## What this will verify (acceptance)

- A row-grain **MERGE** node (`gl_balance`, `incremental.strategy: merge`,
  primary watermark `gl_period_balances._extract_ts`) **passes the AIDPF-4040
  plan-hash continuity gate** on its first `--mode incremental` after a seed —
  with **no** `--repin-plan-hash` — proving the seed↔incremental hash now
  matches because the watermark predicate is mode-normalized (`1=1`) in the
  hash input while the executable SQL stays mode-correct.
- The gold node executes a **MERGE** (delta), not a full `replace`.
- A **second** incremental also stays clean (cursor advances; the latest
  successful row is now the first incremental, not the seed).

## Fix is confirmed deployed (independent of the blocker)

The dispatched run cell on the cluster carries the new kwarg — the fixed wheel
(`0db10970c1772267`) is what executed:

```
summary = orchestrator.run(
    mode='seed', datasets=['gl_balance'], layers=['gold'],
    force_fingerprint_skip=True,
    repin_plan_hash=False,            # <-- this turn's threading, live on cluster
    strict_scope=False,
    execution_backend="content-pack", ...
)
```

Offline proof is complete and green (unit): seed-rendered vs incremental-rendered
plan-hash of a MERGE node are **equal**; SQL-body / profile / watermark-column
edits still **differ**; `--repin-plan-hash` bypasses + audit-rows. See
`tests/unit/test_sql_renderer.py::TestHashDeterminism`,
`tests/unit/test_plan_hash_phase2.py`,
`tests/unit/test_sql_runner.py::TestRepinPlanHashBreakGlass`.

## Blocker detail

Both attempts failed in ~12–15s — **before** any plan-hash code ran. The failure
is in the **AIDPF-2072 Fusion-PVO drift gate**, which probes the live bronze PVO
schema via BICC *before* `_run_content_pack_backend`:

```
orchestrator/__init__.py:559  _run_fusion_pvo_drift_gate(...)
orchestrator/__init__.py:738  probe_bronze_schemas(...)
builtins/bronze_extract_adapter.py:210  bicc_extractor.extract_pvo(...)
extractors/bicc.py  -> Py4JJavaError:
  com.oracle.dicom.connectivity.exception.ConnectorException:
  CONNECTOR_0255 - Received system level error from Fusion Applications …
  Possible cause: Service Temporarily unavailable
  (BiccUtil.getExternalStorages / getLatestExternalStorage)
```

So `gl_balance`'s bronze dependency (`gl_period_balances`) couldn't be probed
because the Fusion BICC endpoint itself is erroring. The plan-hash gate is never
reached.

## How to resume (when the pod is healthy)

```
# 1. Seed — re-pin the plan-hash with the fixed wheel.
aidp-fusion-bundle --config dev/aidp.config.yaml --env dev run \
    --mode seed --datasets gl_balance --layers gold \
    --force-fingerprint-skip --poll-timeout 3000

# 2. Incremental #1 — THE PROOF (no --repin-plan-hash; must NOT raise 4040).
aidp-fusion-bundle --config dev/aidp.config.yaml --env dev run \
    --mode incremental --datasets gl_balance --layers gold \
    --force-fingerprint-skip --poll-timeout 3000

# 3. Incremental #2 — cursor advances; still clean.
aidp-fusion-bundle --config dev/aidp.config.yaml --env dev run \
    --mode incremental --datasets gl_balance --layers gold \
    --force-fingerprint-skip --poll-timeout 3000
```

Confirm a healthy pod first with a dry-run (preflight only, no BICC):
`… run --mode seed --datasets gl_balance --layers gold --dry-run --force-fingerprint-skip`.

**Fallback if the pod is flaky but a prior seed's bronze/silver are
materialized**: the AIDPF-2072 gate only fires when bronze is in scope, so a
gold-only run over pre-existing bronze/silver would skip the live BICC probe —
but it needs `bronze.gl_period_balances` + `silver.dim_account` already present
and the strict-scope dependency set declared. Prefer the full happy-path above
once the pod recovers.

## Scope / notes

- `--force-fingerprint-skip` for the same reason as TC33 (dev bundle references
  datasets whose bronze isn't all materialized on this pod; dev/sandbox only).
- Demo-pod identifiers (service URL, OCIDs, job/run UUIDs) intentionally omitted
  per the repo redaction rule.
