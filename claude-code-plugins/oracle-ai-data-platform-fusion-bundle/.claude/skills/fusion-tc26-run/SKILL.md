---
name: fusion-tc26-run
description: "Dispatch the TC26 orchestrator end-to-end probe (narrow or full happy path) against a live AIDP cluster via OCI-signed REST. Builds the plugin wheel, inlines it into a self-contained notebook, uploads via the workspace contents API, creates a Job + JobRun, polls to terminal state, fetches the executed notebook, and parses the AIDP_LIVE_TEST_RESULT marker. Use when the user wants to validate orchestrator behavior on a real Fusion tenant — TC26 evidence capture, regression after a fix, plugin-portability claim on a new tenant. NOT for unit-test runs (use pytest) or for dataset-extract debugging (use /probe-bicc-pvo)."
---

# fusion-tc26-run — orchestrator live evidence capture

This skill dispatches the TC26 probe end-to-end. It encapsulates the full "build wheel → upload notebook → create job → submit run → poll → fetch output" dance that's painful to reconstruct each time.

**Depends on the sibling `aidp-rest` skill** for OCI signing + REST primitives (`AidpRestClient`). This skill owns only the TC26-specific concerns: bundle templates, notebook generation with embedded wheel, and result presentation.

## When to use

- After shipping a bug fix to the orchestrator or extractors — confirm it still produces a clean bronze→silver→gold cascade on a real tenant.
- When opening a PR that needs live evidence — captures the marker payload + state-table snapshot for inclusion in `tests/live/TC26_orchestrator_seed_run.md`.
- When validating the plugin against a new tenant (P3.7/P3.9 portability claim).
- When the user says "run TC26" or "test the orchestrator against the cluster" or similar.

**Do NOT use** for unit-test runs, dataset-extract smoke tests in isolation, or any flow that doesn't need a real BICC roundtrip.

## Prerequisites the user must provide

1. **OCI CLI authed** — `oci config` profile with read/write to the AIDP control plane. Confirm with `oci raw-request --target-uri "https://datalake.<region>.oci.oraclecloud.com/20260430/aiDataPlatforms/<aidpId>/workspaces" --http-method GET`.
2. **AIDP coordinates** — `aiDataPlatformId` (OCID), `workspaceKey` (UUID), `clusterKey` (UUID), cluster name (display name), region. Resolve workspace/cluster keys via the resolver helper in `dispatch.py` if only display names are known.
3. **BICC credential in the AIDP credential store** — named entry that the cluster can read via the runtime-injected `aidputils.secrets.get(name=..., key=...)` global. Default name: `fusion_bicc_password`, key: `password`.
4. **Cluster ACTIVE** — start it via `POST /clusters/<key>/actions/start` with body `{}` if STOPPED; wait for `state: ACTIVE` before dispatching.
5. **A bundle.yaml** — either narrow probe (auto-generated from `bundle_narrow.yaml.tpl`) or a custom one. The skill inlines the bundle into the notebook (no separate upload), referencing the password via `${FUSION_BICC_PASSWORD}` which is set in the notebook from the AIDP credential store.

## Scopes

| Scope | Bundle | Wall time | Use when |
|---|---|---|---|
| `narrow` | 2 bronze (erp_suppliers, ap_invoices) + 2 silver dims (dim_supplier, dim_calendar) + 1 gold (supplier_spend) | ~2-3 min | Quick credential/cascade validation. Validates 5 plan nodes. |
| `full` | 11 bronze + 5 dims + 5 gold marts (mirrors `examples/full_finance.yaml`) | ~10-15 min | Closing TC26 evidence. Validates 17 shipped + 4 deferred plan nodes. Long pole: `gl_period_balances` ~10M rows. |
| `custom` | User-supplied `bundle.yaml` path; inlined verbatim | varies | Reproducing a customer scenario, debugging a specific DAG shape. |

## Workflow

The skill invokes `dispatch.py` (sibling file in this skill folder). The script:

1. **Sanity** — GET `/clusters/<key>`, assert `state == ACTIVE`.
2. **Build wheel** — `python -m build --wheel --outdir <tmp>` from the plugin checkout. ~130KB.
3. **Generate notebook** — 4 cells: (a) base64-decode + `pip install --target <tmpdir>` + `sys.path.insert`, (b) load password from `aidputils.secrets`, write bundle.yaml, import orchestrator, (c) call `orchestrator.run(...)`, print per-step table, emit `AIDP_LIVE_TEST_RESULT_BEGIN <json> AIDP_LIVE_TEST_RESULT_END`, (d) query `fusion_bundle_state` + verify `silver_run_id` / `gold_run_id` audit columns.
4. **Upload** — PUT `/notebook/api/contents/<urlencoded-path>` with `{type: "notebook", format: "json", content: <nbformat dict>}`. Path lives under `/Workspace/Shared/fusion-bundle-tc26-{scope}/`.
5. **Create Job** — POST `/jobs` with `path: "jobs"` + `maxConcurrentRuns: 1` + jobClusters mirror + NOTEBOOK_TASK. Both `jobClusters[]` and `tasks[].cluster` must carry the real cluster UUID — see dispatch.py inline comments for the empirically-confirmed shape.
6. **Submit Run** — POST `/jobRuns` with `{jobKey, parameters: [], queue: {isEnabled: false}}`.
7. **Poll** — GET `/jobRuns/<key>` every 20s, watch `state.status` transition `PENDING → RUNNING → SUCCESS|FAILED|CANCELED|TIMED_OUT`. Tolerate transient read timeouts (retry after 20s sleep).
8. **Resolve taskRunKey** — from `taskToTaskRunMap[<taskKey>]` (or `next(iter(taskRunSummaryMap))` for the single-task case).
9. **Fetch output** — POST `/taskRuns/<key>/actions/fetchOutput` with `{"outputKey": ""}` (empty string — NOT `"main"`). The executed notebook lands in `data[0].value` as a JSON string.
10. **Parse marker** — walk `cells[*].outputs[*]` for the `AIDP_LIVE_TEST_RESULT_BEGIN ... END` block; JSON-decode.
11. **Present result** — per-step table with status/rows/duration/skip_reason; mismatch vs expected (deferred datasets/dims/marts per `KNOWN_DEFERRED_*` registries).

## REST quirks

All empirically-confirmed REST shapes live in the `aidp-rest` skill (SKILL.md + `client.py`). The TC26 dispatcher uses `AidpRestClient` so it never has to think about: `path:"jobs"`, `maxConcurrentRuns:1`, the redundant `jobClusters[]`/`tasks[].cluster` mirror, `fetchOutput` `outputKey:""`, `data[0].value`, URL-encoded contents paths, the contents-GET `?type=notebook&content=1` requirement, or `ReadTimeout` tolerance during polling.

## Invocation

```bash
# Narrow probe (recommended for first validation after a fix)
python .claude/skills/fusion-tc26-run/dispatch.py \
  --scope narrow \
  --aidp-id <ocid1.datalake...> \
  --workspace-key <workspace-uuid> \
  --cluster-key <cluster-uuid> \
  --cluster-name <cluster-display-name> \
  --region <oci-region> \
  --secret-name <aidp-credential-store-entry-name> \
  --fusion-service-url <https://fa-pod.ds-fa.oraclecloud.com> \
  --fusion-user <bicc-user> \
  --external-storage <bicc-external-storage-profile-name>

# Full happy path
python .claude/skills/fusion-tc26-run/dispatch.py --scope full [same flags...]

# Custom bundle
python .claude/skills/fusion-tc26-run/dispatch.py --scope custom --bundle-path /path/to/bundle.yaml [same flags...]
```

The script writes the executed notebook to `/tmp/tc26-{scope}-{timestamp}/run.executed.ipynb` for inspection and prints the parsed marker payload to stdout. Exit code is 0 on SUCCESS, 2 on any failure mode.

## Evidence capture

After a successful run, the operator can append a redacted section to `tests/live/TC26_orchestrator_seed_run.md` — see commit `44b4bda` for the template. Sensitive identifiers to redact: cluster key, workspace key, aiDataPlatformId, job/run/task keys. The orchestrator `run_id` (UUID) is safe to keep (internal correlation only).

## Diagnostic when it fails

If the orchestrator reports a `failed` step but `error_message` is empty (`AnalysisException()` problem), re-run with `--diagnose <dataset_id>` — the script injects a try/except wrapper that re-calls the failed extractor inline, captures the full `str(exc)` and traceback, and prints it. This is how we caught the `_watermark_used` schema-merge bug in commit `d9292f3`.

## Known follow-ups (not yet skill-encoded)

- **Non-saasfademo1 tenant** — same script works, but the `--external-storage` profile name will differ per tenant. Document the lookup procedure once we have a second tenant validated.
- **Incremental mode (Phase β)** — orchestrator currently rejects `mode=incremental` with NotImplementedError. When β ships, this skill grows a `--mode incremental` flag + watermark seeding.
- **Cascade probe** — when the user wants to deliberately fail one bronze and confirm the cascade contract (`status='skipped'`, `skip_reason='cascade'` for downstream; `'aborted'` for unrelated). Currently requires manual extractor monkeypatch in the notebook; could be a `--fail-step <dataset_id>` flag.
