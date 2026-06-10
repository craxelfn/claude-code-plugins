# Limits — `oracle-ai-data-platform-fusion-bundle`

> **Purpose**: register every known limitation of this plugin — both the ones we've decided to live with and the ones we're tracking toward resolution. A limit is a constraint we cannot make disappear with effort proportional to its impact.
>
> **Maintenance rule**:
> - When a new limit is discovered or accepted, add an entry under **§Active limits**.
> - When a limit is resolved (fully or substantially), move its entry to **§Resolved limits** with the resolution commit / date / how it was resolved.
> - When a limit's mitigation status changes (e.g. partial workaround landed), update the entry in place — don't move it until the limit itself is gone.
>
> Last updated: **2026-06-01**.

---

## Active limits

### L1 — PVO schema drift across Fusion releases requires patch releases

**What it is**: Gold marts and silver dims hardcode column names sourced from a one-time live probe of each Fusion BICC PVO (e.g. `CodeCombinationCodeCombinationId`, `BalanceCodeCombinationId`). When Oracle renames columns or changes value domains across Fusion releases, the bundle requires a patch release that updates affected SQL builders. There is no architecture that eliminates this — see "Why we can't fix this" below.

**Severity**: medium (loud, predictable, infrequent)
**Discovered**: 2026-05-08 (during P1.8 planning)
**Affects**: every gold mart and silver dim that joins on a PVO column (all of them).

**Why we can't fix this** — alternative architectures considered, all reduce to "someone updates something when Oracle changes something":

| Architecture | Why it doesn't eliminate patches |
|---|---|
| Schema-introspection + dynamic SQL | Can't guess semantics — `BalanceActualFlag='A'` vs `BalanceActivityType='POSTED'` mean the same thing but the filter must change. |
| Config-driven mapping (`column_map.yaml`) | Pushes the patch from us to the customer, who is worse-positioned to know which release renamed what. |
| LLM-assisted column resolution | Unreliable; silent wrong-data is worse than loud breakage; runtime LLM cost. |
| Pin to one Fusion release forever | Customers upgrade Fusion; pinning becomes unusable later. |
| Oracle freezes PVO schemas | Not our control — Oracle revs them quarterly. |

The fundamental property: gold marts encode business semantics (column names + value domains + cast precisions), Oracle owns all three, when they change *something* in our code must change.

**Mitigations** (all on backlog):

* **P2.16** — `catalog drift` command + schema fingerprint snapshots → drift becomes loud and pre-flight, not silent and mid-run.
* **P2.17** — Fusion release-version detection + support-matrix warning → customer knows at install time whether their release is verified.
* **P3.9** — CI test pod for live PVO regression (blocker on AIDP infra) → drift caught on bundle's side, not customer's.

**Realistic operational profile** (after P2.16+P2.17):

* Most quarters: zero breaking changes (Oracle adds columns; SQL ignores extras).
* 1–2 quarters/year: minor breaking change (1–3 PVOs touched).
* Rare (every few years): major restructuring.
* **Patch cadence**: 1–2 patch releases / year, ~half a day each.

**Customer-facing framing**:

> "Verified against specific Fusion releases. Patch releases ship within ~2 weeks of new Fusion releases that touch covered PVOs. `catalog drift` lets you confirm compatibility on your own pod before relying on it."

This matches the posture of every other packaged BI mart on top of Fusion (Oracle's own FAW, SAP packaged content, Informatica accelerators) — none ship "patch-free across all upstream upgrades."

---

### L2 — BICC V1 datasource encoder bug blocks any PVO with integer-valued NUMBER columns

**What it is**: The BICC V1 datasource (`format("aidataplatform")`, `type=FUSION_BICC`) declares Oracle `NUMBER` columns as Spark `DecimalType(38,0)` / `DecimalType(18,0)` etc., but at row-materialization time emits `java.lang.Long` for integer-valued cells. Spark's `ExpressionEncoder` strict validator rejects with `java.lang.Long is not a valid external type for schema of decimal(38,0)`. Affects any write path (`saveAsTable`, `format("noop").save()`, `df.collect()`, etc.).

**Severity**: high (hard-blocks `bronze.scm_items` extraction; will block any future PVO with the trigger conditions)
**Discovered**: 2026-05-07 (during P1.6 implementation)
**Affects**: `ItemExtractPVO` confirmed; any future PVO whose Oracle source has integer-valued NUMBER columns is at risk. `SupplierExtractPVO`, `InvoiceHeaderExtractPVO`, `CodeCombinationExtractPVO`, `ReceiptHeaderExtractPVO` all worked — but only by chance of column shape, not by design.

**Why we can't fix this in the bundle**: bug is JVM-side, in the `aidataplatform` connector's runtime emission path. Not language-bindable around (Scala doesn't help — encoder runs in JVM regardless). Five workaround variants attempted, all failed:

1. Catalyst cast projection — optimizer elides same-type casts.
2. RDD-level Python coercion — encoder fires at JVM→Python boundary, before Python.
3. `.schema(all_string_mirror)` override — connector emits `BigDecimal` instead.
4. `.schema(uniform_Decimal(38,30))` — connector emits `Long` regardless of declared scale.
5. `count()` looks like it works but is a false positive (CountStar optimization skips column projection).

Detail: [`BLOCKER_P1.6_dim_item.md`](BLOCKER_P1.6_dim_item.md) (5 FIX scripts on disk for reference).

**Mitigations**:

* **Upstream fix** — bug paragraph drafted, pending hand-off to AIDP / BICC connector team. Reference framing: connector should box `Long` into `BigDecimal` (or emit `InternalRow` Decimals) when declared column type is `DecimalType`.
* **Option A — read CSV staging files directly** from OCI Object Storage bucket configured via `fusion.external.storage`. Plugin-portable bypass; requires manifest discovery + own delta detection. Estimated 1–2 days of design work + new test surface. Becomes the bundle's standard bronze-loader pattern for affected PVOs.
* **Architectural posture**: don't break "one extraction modality" by adding REST for individual PVOs (rejected during P1.6 deep-dive — would fragment watermarking/error handling).

**Status**: P1.6 paused. P1.8 to attempt standard pattern first; if it hits the same bug, escalate to Option A.

---

### L3 — `dim_org` blocked indefinitely on HCM pod access

**What it is**: P1.7 `dimensions/dim_org.py` requires identifying the right HCM/HR PVO for organization hierarchy. Demo pod (`saasfademo1`) returns 404 on `/saas-batch/security/tokenrelay` (HCM-tier feature, paying customers only); BICC catalog probe also can't land the right PVO without HCM access.

**Severity**: low (one deferred dim; doesn't block v0.2.0 release)
**Discovered**: 2026-04-30 (during catalog audit)
**Affects**: `dim_org`, plus any future cross-module mart that needs HCM × Finance joins.

**Why we can't fix this**: requires customer pod with HCM tier; not buyable as a developer.

**Mitigations**: none until customer engagement (tracked as **P3.6**, **P3.8**).

**Status**: deferred indefinitely. Marked as out-of-scope for v0.2.0; bundle ships with 3/4 dims.

---

### L4 — CI cannot run live PVO regression without dedicated test pod

**What it is**: Without a CI-accessible Fusion pod, drift detection (P2.16) only runs on customer pods, not on the bundle's own side. We can't catch regressions between Fusion releases proactively.

**Severity**: medium (limits how proactive we can be on patches)
**Discovered**: 2026-05-08
**Affects**: maintenance discipline for L1.

**Why we can't fix this in the bundle**: demo pod (`saasfademo1`) is shared, rate-limited, and unreliable for scheduled CI; customer pods must never be used from CI. Real fix is AIDP-side infrastructure.

**Mitigations**: tracked as **P3.9** — dedicated CI test pod, blocking on AIDP team provisioning.

**Status**: tracked, not actionable on bundle side.

---

### L6 — Empty-delta + watermark-read soft-failure silently regresses progress

**What it is**: When `state.read_last_watermark` soft-fails (Spark/metastore transient exception swallowed → `None` return + structured WARN log) AND the bronze extract in the SAME run returns zero rows, `prior_watermark=None` is indistinguishable from "no prior state row at all" — both paths persist `last_watermark=NULL` via the empty-delta fallback (`new_wm = persisted_cursor if row_count > 0 else prior_watermark`). The next read returns `None`, the next incremental BICC call gets `watermark=None`, and the next run does a full re-extract (under P1.17's MERGE-by-natural-key, that re-extract is correctness-safe — only a cost/perf hit).

**Severity**: low (correctness-safe under P1.17 MERGE; cost hit is one wasted full extract until the regression is detected)
**Discovered**: 2026-05-28 (P1.5β.1 review correction for soft-fail semantics under Invariant 1)
**Affects**: incremental mode operators (once P1.17 enables it) on tenants where the metastore is occasionally flaky.

**Why we can't fix this in β.1**: a dedicated state-row signal (`watermark_read_failed` column) would require a `fusion_bundle_state` schema migration + writeability-probe payload change + migration story for tenants on the current schema — all to duplicate audit information the WARN log already carries. β.1 accepts the regression under Invariant 1's soft-fail contract; the audit channel is the structured WARN log with the stable marker `"watermark_read_soft_failed"`.

**Mitigations**:
* `read_last_watermark` emits exactly one structured WARN per soft-fail carrying `dataset_id`, `layer`, `repr(exc)`, and the marker `"watermark_read_soft_failed"`. The marker is part of the public API contract (`tests/unit/test_orchestrator_watermark_infra.py::TestReadLastWatermark::test_soft_fail_returns_none_with_warn_marker`).
* Operator playbook documented in `README.md` §"Incremental refresh" → "Empty-delta + soft-fail operator playbook" (shipped with P1.17). Configure a log alert on the marker; any subsequent `last_watermark=NULL` on a previously-advancing dataset, paired with the marker, is the documented regression — manual state-row repair (re-stamp `last_watermark` for the dataset's most-recent success row) is the intervention path before the next incremental dispatches.
* If SOX traceability later demands a persisted in-state signal, the column add is post-P1.17 paired with other state-schema work.

**Status**: tracked-by-design; the WARN marker is the audit channel until/unless a future requirement demands in-state encoding.

---

### §L-Resume — `fusion_bundle_state` is multi-row-per-`(run_id, dataset_id)` on resumed runs

**What**: P1.5α-fix21 introduced `aidp-fusion-bundle run --resume <run_id>`. On a resumed run the state table is append-only and may carry multiple rows per `(run_id, dataset_id)` — for example, a `failed` row from the original attempt + a `resumed_skipped` carry-forward + an eventual `success` under the resume can all coexist under the same `run_id`. This is intentional (preserves the CLAUDE.md medallion `<layer>_run_id` invariant — a gold row's `gold_run_id` still joins 1:1 to a single logical pipeline run, not split across resume attempts).

**Where it bites**: naïve consumer queries against the raw `fusion_bundle_state` table:
* `SELECT * FROM fusion_bundle_state WHERE status = 'failed'` — surfaces stale failed-attempt rows that have since succeeded under a resume; dashboards page on resolved issues.
* `SELECT COUNT(*) FROM fusion_bundle_state WHERE run_id = '<x>'` — overcounts datasets when the run was resumed at least once.
* `SELECT SUM(row_count) FROM fusion_bundle_state WHERE run_id = '<x>' AND status = 'success'` — fine in practice (failed rows have `row_count IS NULL`), but reasoning is fragile.

**Mitigation**: `ensure_state_table` creates a Delta VIEW `fusion_bundle_state_latest` projecting one row per `(run_id, dataset_id)` via `ROW_NUMBER() OVER (PARTITION BY run_id, dataset_id ORDER BY last_run_at DESC)`. Consumers SHOULD read from the VIEW unless they explicitly need the append-only history. Dashboards, alerts, ad-hoc queries — all default to the VIEW. The operator-facing `aidp-fusion-bundle status` command (`commands/run.py:308`) is a different aggregation (latest per `dataset_id` regardless of `run_id`) and stays inline.

**Status**: tracked-by-design — the multi-row shape is load-bearing for audit traceability; the VIEW is the contract every consumer should reach for first.

---

### P1.17-L2 — `incremental_capable=False` PVOs re-extract in full on every cycle

**What it is**: Three bronze PVOs carry `PvoEntry.incremental_capable=False` because BICC's `fusion.initial.extract-date` filter is not respected for them: `gl_period_balances` (monthly-snapshot semantics), `gl_coa` (chart-of-accounts; tiny), `ap_aging_periods` (bucket-config table). Under `--mode incremental` these PVOs still re-extract the FULL row set every cycle. The bronze MERGE deduplicates by natural key so the target row count doesn't grow, but BICC-side cost equals seed-mode cost.

**Severity**: low (cost, not correctness — V1 explicit acceptance per plan §B6b)
**Discovered**: 2026-05-30 (P1.17 plan §B5 + §B6b)
**Affects**: tenants whose daily incremental cost budget assumes BICC short-circuits on no-op cycles.

**Mitigation**: documented as expected behavior. P3.x follow-up flips the catalog flag if/when BICC adds cursor support for one of these PVOs.

**Status**: tracked-by-design.

---

### P1.17-L3 — Gold dim-attribute staleness on `gl_balance` until P1.17a

**What it is**: V1's `gl_balance` incremental MERGE source predicate is `WHERE b._extract_ts > <prior_silver_watermark>` — fact-side delta filter only. Dim-only changes (e.g. an account's hierarchy attribute renamed in `silver.dim_account` between two incremental runs) do NOT refresh `gl_balance`'s denormalized hierarchy columns until a fact-side delta hits the affected `account_id`. Until then, the gold row carries the dim attribute value frozen at the last fact-side update.

**Severity**: low (correctness-affecting on dim attributes, but business consumers typically use the joined dim view for hierarchy lookups — `gl_balance`'s denormalized columns are a perf optimization, not the source of truth)
**Discovered**: 2026-06-01 (P1.17 plan §B2 gl_balance row-level classification)
**Affects**: tenants whose dashboards read denormalized hierarchy columns straight from `gold.gl_balance`.

**Mitigation**: operator schedules a weekly `--mode seed` to refresh denormalized columns end-to-end. The full canonical fix (dim-delta UNION pattern) ships in **P1.17a**.

**Status**: tracked, P1.17a follow-up.

---

### P1.17-L4 — `supplier_spend` re-runs as seed-shape every incremental cycle

**What it is**: `supplier_spend.incremental_capable=False` because its 6-column GROUP BY grain mixes a mutable fact attribute (`approval_status`). A partial-MERGE pattern in V1 would leave both old (`PENDING`) and new (`APPROVED`) aggregate rows on an invoice status flip. The mart therefore always emits seed-shape SQL regardless of `--mode incremental`. Cost ≈ seed-mode cost on every cycle (~13s on saasfademo1 per TC28 evidence — trivial in absolute terms).

**Severity**: low (cost, not correctness — V1 explicit acceptance per plan §B2)
**Discovered**: 2026-06-01 (P1.17 plan §B2 supplier_spend classification)
**Affects**: incremental-mode operators expecting cost savings on this mart.

**Mitigation**: the correct aggregate-MERGE pattern (affected-keys + full-recompute + DELETE for grain-moves) ships in **P1.17b** as a coherent unit.

**Status**: tracked, P1.17b follow-up.

---

### P1.17-L8 — `gl_period_balances` composite natural key has a NULL component

**What it is**: `gl_period_balances`'s composite natural key includes `BalanceTranslatedFlag`, which is NULL on `fusion_bundle_dev` (TC23 evidence row 151). Standard SQL equality (`=`) does NOT match NULL=NULL — only NULL-safe `<=>` does. The P1.17 bronze MERGE template uses `<=>` on every key column for exactly this reason, so the NULL-NULL match works correctly today. The limit is documentation: tenants where one of the 7 key columns has a populated value mixed with NULLs in different rows could still hit edge cases where the MERGE produces unexpected dedupe behavior.

**Severity**: low (documented; NULL-safe operator handles the saasfademo1 case correctly)
**Discovered**: 2026-06-01 (P1.17 plan §A1 verification)
**Affects**: tenants whose `gl_period_balances` carries a mix of NULL + non-NULL on any composite-key column.

**Mitigation**: NULL-safe `<=>` in the MERGE ON predicate handles same-rows-with-NULL correctly. Tenants who observe MERGE row-matching surprises should escalate with a `DESCRIBE HISTORY` of the affected commit.

**Status**: tracked-by-design.

---

### P3-L1 — content-pack `ap_aging` ships proxy mode only

**What**: v1's `transforms/gold/ap_aging.py` defaults `due_date_mode='auto'`, probes `ApInvoicesTermsDate` / `ApInvoicesDueDate` coverage at runtime, and switches to real mode (buckets on due_date, emits `max_days_past_due` + provenance counts) when coverage exceeds the threshold. v2 content-pack `ap_aging.sql` ships **proxy mode only** (buckets on `invoice_date`, emits `max_days_outstanding`) as an intentional scope decision: runtime coverage probing is exactly what ADR-0014 removes, and the two modes have different output schemas which the v0.3 renderer cannot select between from a single template.

**Where it bites**: tenants whose live AP invoice data has Terms/Due-date coverage above v1's `DEFAULT_REAL_MODE_GATE_THRESHOLD` (10%) will see v1 auto-route to real-mode output, while v2 content-pack will always produce proxy-shape output. Row counts may agree but bucket assignments and column shapes diverge.

**Mitigation**: documented divergence — see `docs/v2-phase-3-variation-catalog.md` "AP aging — proxy mode only". Customers who need real-mode behaviour today stay on `--execution-backend legacy-python`. Auto/real follow-up needs (a) a renderer extension for two-schema variants, (b) declarative threshold config, and (c) live evidence of any saasfademo1-or-comparable tenant exceeding the threshold.

**Status**: tracked-by-design.

---

### §L-Resume-Concurrency — two operators `--resume`-ing the same `run_id` concurrently is unguarded

**What**: P1.5α-fix21 has no lock / leader-election guard around `--resume`. If two operators (or one operator + a stuck-but-still-running prior dispatch) both run `--resume <same_run_id>`, both will pass the drift gate (same bundle, same plan_hash) and both will write rows under the same `run_id`. Latest-per-`(run_id, dataset_id)` semantics still produces a coherent terminal view; intermediate state during the race is inconsistent.

**Where it bites**: an operator kicks off a long resume, leaves it running, then a second operator (thinking the first stalled) kicks off another. Both finish; the state table has interleaved rows from both runs.

**Mitigation**: L3 — operator-discipline issue. Don't run two `--resume` for the same `run_id` concurrently. Real concurrency control (Spark-level locks, etcd-style leader election, or a `running` status sentinel in the state table) is Phase γ scope.

**Status**: tracked, awaiting Phase γ.

---

### P3-L2 — content-pack `dim_account` COA role-positioning has two gaps

**What (gap 1 — three role aliases, not six)**: v0.3 declares `coa_*_segment` `columnAliases` for three of the six v1 Fusion COA roles (`balancing`, `cost_center`, `natural_account`). The other three roles in v1's `DEFAULT_SEMANTIC_SEGMENT_MAP` (`subaccount`, `product`, `intercompany`) have NO declared `columnAliases`; `dim_account.sql` emits them via positional hardcoded references (`CodeCombinationSegment4 AS subaccount`, `Segment5 AS product`, `Segment6 AS intercompany`).

**What (gap 2 — `columnAliases` existence-based resolution cannot disambiguate role-positioning)**: even for the three declared roles, the candidate list is a single conventional default per role (`coa_balancing_segment.candidates: [CodeCombinationSegment1]`, etc.). `columnAliases` resolves by physical column existence (PLAN §9.5.4 step 2). All six `CodeCombinationSegmentN` columns coexist on every Fusion `gl_coa` extract, so on a non-conventional tenant — e.g., one where the `balancing` role lives at `CodeCombinationSegment4` — bootstrap auto-resolves to `Segment1` because `Segment1` still exists, and silently binds the wrong source column.

**Where it bites**: a non-conventional COA tenant — one where any of `balancing`, `cost_center`, `natural_account`, `subaccount`, `product`, or `intercompany` lives at a position other than the conventional 1 / 2 / 3 / 4 / 5 / 6 mapping — will have those roles silently bound to the wrong `CodeCombinationSegmentN` source columns unless the operator intervenes BEFORE running `bootstrap`.

**Mitigation (manual today)**: for the three declared roles, pre-author `overlays/<name>/pack.yaml` extending `columnAliases.coa_<role>_segment.candidates` with the role's actual source column (or hand-edit `profile.resolved.column.coa_<role>_segment` directly). For the three undeclared roles (`subaccount` / `product` / `intercompany`), invoke the medallion-author skill (feature `v2-phase-3b-medallion-author-skill`) to draft a pack overlay declaring `coa_subaccount_segment` / `coa_product_segment` / `coa_intercompany_segment` AND extending `dim_account.sql` via overlay to substitute via `{{ column.* }}` tokens. Either intervention must happen BEFORE `bootstrap` runs.

**Architectural fix (future, out of v0.3)**: a new `{{ coa.<role> }}` renderer token consuming `profile.chartOfAccounts.<role>Segment` integers is the proper substitution mechanism — bootstrap prompts the operator at onboarding for each role's position (or reads from a structured `chartOfAccounts` profile block), and the renderer emits `CodeCombinationSegment<N>` based on the resolved integer. Makes role-positioning explicit rather than relying on existence-based resolution. Requires (a) new renderer vocabulary, (b) bootstrap UX changes, (c) live-tenant evidence justifying the work.

**Status**: tracked-by-design for v0.3; future renderer feature reserved.

### P3c-L1 — legacy tenant profile silently bypasses the drift gate

**What**: the Phase 3c runtime drift gate
(`orchestrator/preflight_evidence.py::check_bronze_fingerprint_drift`)
treats a `bronzeSchemaFingerprint` value that is `None`, the sentinel
`sha256:placeholder-*`, or a malformed `sha256:` string as a **legacy profile** and emits `PreflightOutcome(kind="skip_legacy_profile")`. A WARN log fires once per run; the run proceeds. The detection is regex-based — any value not matching `^sha256:[0-9a-f]{64}$` is treated as legacy.

**Where it bites**: tenants who onboarded before the Phase 3a bootstrap landed (no fingerprint ever written) or whose profile was hand-authored / copy-pasted from a fixture continue to run incrementals without the drift safety net. A bronze schema change goes undetected by the runtime gate, surfacing later as a MERGE-time SQL failure or — worse — a silent semantic regression (column renamed; variation point still resolves to the new name but the SQL was rendered with the old).

**Mitigation**: operator runs `aidp-fusion-bundle bootstrap --refresh` once. Phase 3a writes a real `sha256:<64-hex>` fingerprint; subsequent incrementals fire the gate normally.

**Status**: tracked-by-design for v0.3. Stricter mode (treat legacy profiles as a hard fail) would gate every legacy tenant from running incrementals until they refresh — too disruptive for v0.3 rollout. A future env-var `AIDP_REQUIRE_PINNED_FINGERPRINT=1` could flip the policy without a code change.

---

## Resolved limits

### AIDPF-1032 — `--resume` rejected on content-pack backend (RESOLVED 2026-06-08 by Phase 5 Step 9b)

`--resume` now works on `--execution-backend=content-pack`. The
content-pack backend's per-node atomic-commit model (preflight →
render → drift gate → execute → quality → state row) is the resume
unit; the orchestrator adopts the supplied `resume_run_id` as the
shared run identifier so the resumed run's state rows join with the
prior failed run's rows. The xfail-strict
`TestStep5_Resume::test_v2_resume_currently_rejected` parity test
inverted to `test_v2_resume_adopts_supplied_run_id` (asserts the
adopt-supplied-run_id contract).

### P5-L1 — `python_legacy` adapter ships seed-only (P5 follow-up if a customer needs incremental)

The `python_legacy` runtime adapter (Phase 5 Step 1) constructs v1-
conventional kwargs (`paths`, `bronze_<id>`, `silver_dim`,
`silver_table` / `gold_table`, `refresh_mode`, `watermark`, `run_id`)
and is exercised for `seed` mode by unit tests. Most v1 builders accept
the same kwarg shape for both seed and incremental, so the
incremental code path likely works as-is; verifying that requires a
customer-shipped `python_legacy` node and is deferred until one
materialises. Tracked here so the v0.3-Phase-5 surface is honest.

### P5-L2 — Top-level dispatcher: bronze-then-content-pack chaining via CLI is scope-deferred

Phase 5 Step 2b ships the `split_run_scope(...)` classifier and the
content-pack backend accepts a `shared_run_id` kwarg, but the
CLI-level chaining ("`aidp-fusion-bundle run --mode seed` extracts
bronze via the legacy loop THEN runs the content-pack backend with
the same run_id") still lands on direct callers (notebooks,
integration tests) rather than through the public CLI. The CLI
today routes through `_run_content_pack_backend` which assumes
bronze is pre-seeded. Full bronze-then-cp integration into the
public CLI is deferred to a follow-up; AIDPF-2071 (Phase 5 Step
2c) fires when an operator opts into the chaining manually and the
bronze isn't ready.

### P3c-L2 — drift artifact lacks per-dataset column-level diff (RESOLVED 2026-06-06 by Phase 3d)

Phase 3d adds a bootstrap-pinned per-dataset bronze-schema snapshot file at
`<bundle.yaml.parent>/profiles/<tenant>.schema-snapshot.yaml`. Runtime
preflight reads the snapshot on drift and populates
`SchemaDriftFailure.datasetDeltas` with per-dataset `addedColumns` /
`removedColumns` / `typeChangedColumns` lists — the exact column-level
signal the operator triage flow needs. `bootstrap --refresh` back-fills
the snapshot atomically when it's missing / unparseable / has a desynced
metadata fingerprint / has hand-edited contents. REST dispatch stages the
snapshot alongside the profile YAML so cluster-side preflight sees the
same snapshot the laptop pinned. Pre-3d profiles continue to emit empty
`datasetDeltas` + a one-time WARN log (graceful degrade); `bootstrap
--refresh` is the documented remediation. See
`docs/content_pack_execution.md` "Phase 3d additions" for the full
contract.

> *Pre-fix P3c-L2 (Phase 3c era): the `AIDPF-2012` artifact reported
> pinned vs. current bronze fingerprints + `affectedVariationPoints[]`
> (per-VP "is the pinned candidate still present?"), but no per-dataset,
> per-column diff. The pinned fingerprint was a one-way SHA-256 hash, so
> reconstructing the prior schema required a pinned-schema snapshot
> deferred to v2-phase-3d-pinned-schema-diff. Operators triaging drift
> cross-referenced the live evidence snapshot in `evidence/<tenant>/`
> manually.*

---

### L5 — `--mode incremental` gate (RESOLVED 2026-06-02 by P1.17 commits `5f644d7` + `f6d003a` + `76fec96`)

The β.1 `NotImplementedError` gate at `orchestrator/__init__.py:641-645` was removed atomically with the non-destructive write strategy: bronze `MERGE INTO` keyed on `PvoEntry.natural_key`, silver `MERGE INTO` keyed on the projected natural key, gold `gl_balance` row-level MERGE with NULL-safe `<=>` composite ON predicate, and the three incremental-exempt marts (`supplier_spend`, `ap_aging`, `dim_calendar`) routed through seed-shape regardless of mode. Live evidence: `tests/live/TC30a_p117_incremental_merge_proof.md` — 11/11 per-layer state-table assertions passed on `fusion_bundle_dev`. The original L5 entry sits below in italics for historical context.

> *Pre-fix L5 (Phase β.1 era — incremental gated by NotImplementedError): the user-facing surface raised at the mode-validation guard; removing it required the non-destructive write strategy because `extract_pvo(watermark=W)` returning delta rows + `mode("overwrite")` would have replaced the full bronze table with just the delta slice (medallion-table corruption). The fix was deliberately deferred from β.1 to P1.17 so the write-strategy + gate-removal shipped atomically.*

---

### P1.17-L5 — Dropped-target silent corruption guard (RESOLVED 2026-06-02 by P1.17c — pending PR merge)

P1.17 V1's preflight only checked silver/gold cursor presence (`IncrementalCursorMissingError`); it did NOT check target-table existence vs cursor presence. An operator who dropped a bronze/silver/gold Delta table outside the orchestrator without also clearing the matching `fusion_bundle_state` row would trigger silent history loss on the next `--mode incremental` run — the auto-created empty target + delta-only MERGE would permanently drop every row below the stale prior cursor.

P1.17c extends `_preflight_incremental_cursors` with a second pass: for every in-scope node whose layer-local cursor is non-NULL, `spark.catalog.tableExists(_resolve_target_table(spec, paths))` must return True. If any node fails, the preflight raises `IncrementalTargetMissingError(OrchestratorConfigError)` listing every affected `(dataset_id, layer, target)` + the clear-state-row → re-seed remediation. Bronze IS in scope (bronze has a safe NULL-cursor fallback but no safe fallback when its target is dropped under a non-NULL cursor).

P1.17c also tightens the silver/gold cursor preflight to fail-closed on transient state-read errors: state reads in preflight now go through `read_last_watermark_strict`, which raises `StateReadFailedError(OrchestratorConfigError)` instead of soft-failing to `None`. Operators who previously saw spurious `IncrementalCursorMissingError` on a metastore flake will now see the more accurate error class with remediation pointing at state-table accessibility. Dispatch-path callers keep using the soft variant — its swallow-and-continue contract remains load-bearing for transient-flake tolerance during a long medallion run.

Unit-test coverage: seven tests in `tests/unit/test_p117_orchestrator_dispatch.py::TestPreflightIncrementalCursors` — single dropped target, consolidated multi-layer, false-positive guard, cursor-precedence guard, strict-read bronze fail-closed, skip-list contract, and bronze-cursor-absent fresh-tenant fallback. No live evidence required (preflight is metadata-only).

> *Pre-fix P1.17-L5: see the original §Active entry that lived here before P1.17c — operator-discipline only (clear state row before dropping a target; re-seed before resuming incremental). Documented as the "DO NOT do this" path, with no enforcement.*

---

### P1.17-L7 — Downstream silver/gold MERGE re-applies every row when bronze is `incremental_capable=False` (RESOLVED 2026-06-02 by P1.17e — pending PR merge)

V1's bronze MERGE used unconditional `WHEN MATCHED THEN UPDATE SET *`, so on every cycle of an `incremental_capable=False` PVO (`gl_period_balances`, `gl_coa`, `ap_aging_periods`) every matched row's `_extract_ts` was rewritten — even when no data column changed. Downstream silver/gold MERGE's source predicate `WHERE bronze_extract_ts > <prior_silver_watermark>` then matched every row, causing silver `dim_account` to re-MERGE all 63K coa rows and gold `gl_balance` to re-MERGE all 10M+ rows on every incremental cycle for marts fed by these PVOs.

P1.17e replaces the unconditional clause with `WHEN MATCHED AND (<payload-diff>) THEN UPDATE SET *`, where the predicate is an OR-joined `target.<col> IS DISTINCT FROM src.<col>` over every non-audit DATA column. The four audit columns (`_extract_ts`, `_source_pvo`, `_run_id`, `_watermark_used`) are excluded by symbolic reference to the new `BRONZE_AUDIT_COLUMNS` frozenset in `orchestrator/runtime.py`. Helper `_payload_diff_predicate_sql` (`orchestrator/__init__.py`) mirrors `_natural_key_join_sql`'s shape; returns `None` on empty/all-audit input (defensive fallback to V1 unconditional shape). NULL-safe via `IS DISTINCT FROM` to handle composite keys with NULL components (mirrors the `<=>` NULL-safety in the ON predicate).

Unit-test coverage: 9 new tests across `tests/unit/test_p117_builder_merge_sql.py::TestPayloadDiffPredicateHelper` (6 tests pinning the helper's exclusion contract + source-order preservation + alias overrides + per-audit-column regression guards), `TestBronzeMergePayloadDiffSQLShape` (2 tests pinning the helper output for single + composite natural-key shapes), and `tests/unit/test_p117_orchestrator_dispatch.py::TestBronzeMergeSql::test_incremental_renders_payload_diff_predicate_excluding_audit_cols` (E4 — pins renderer wiring at the dispatch boundary). The pre-existing `test_incremental_emits_merge_into_with_natural_key` was updated to assert the new gated shape (V1 substring-match would now be a false negative). Live evidence: `tests/live/TC30b_p117e_payload_diff_results.md` — two-cycle probe (Run A seed, Run B incremental no-change) on a 5-node scope (`gl_coa` + `gl_period_balances` bronze; `dim_account` + `dim_calendar` silver; `gl_balance` gold), asserts bronze MERGE post-Run-B `numTargetRowsUpdated=0` and downstream silver/gold either skip the MERGE commit OR record zero-metrics commits.

> *Pre-fix P1.17-L7: see the original §Active entry that lived here before P1.17e — V1 documented "expected V1 cost" and operators absorbed the silver/gold seed-mode wall-time hit on every incremental cycle for the `gl_balance` chain.*

---

### P1.17-L6 — MERGE fails loud on bronze schema drift; manual ALTER required (RESOLVED 2026-06-02 by P1.17d — pending PR merge)

V1 incremental MERGE assumed exact source-target schema match. A BICC-side column addition between cycles (extension package activation, custom field addition, Fusion release upgrade) raised `AnalysisException` naming the new column; bronze table unchanged; operator had to run `ALTER TABLE <bronze.target> ADD COLUMNS (<new_col> <type>)` manually then re-run incremental. Same problem affected silver/gold under SELECT-projection changes.

P1.17d ships `_ensure_target_schema_for_merge` in `orchestrator/state.py` — runs as a pre-step before every incremental MERGE at all 4 layers (bronze + dim_supplier + dim_account + gl_balance). The helper composes 3 existing primitives (`_existing_state_columns_with_types`, `_build_add_columns_ddl`, `spark.catalog.tableExists`):

- **Source-wider** — emits `ALTER TABLE ... ADD COLUMNS (<new> <type>, ...)` so the subsequent MERGE proceeds with V1 `UPDATE SET *` / `INSERT *` shape unchanged.
- **Target-wider** — returns `SchemaReconcileResult.target_only_columns`; renderer switches to explicit-column-list MERGE (`UPDATE SET t.c = s.c, ...; INSERT (c, ...) VALUES (s.c, ...)`) over `common + source_only`, preserving target-only columns by exclusion.
- **Type-conflict** — raises `SchemaEvolutionTypeConflictError(OrchestratorConfigError)` listing every affected `(column, source_type, target_type)`. CLI exit code = 1 (failed step in `fusion_bundle_state` with the full conflict list in `error_message`); strict-abort cascade then cascade-skips remaining steps. `OrchestratorConfigError` inheritance preserved for catch-by-class callers — the exit-code surface just matches "any failed step → exit 1". Auto-promotion out of scope (operator decides between widening ALTER, drop & re-seed, or source-projection revert).

The 4 silver/gold builders gain optional `target_only_columns` + `source_columns` kwargs to `build_<X>_sql`. The bronze MERGE renderer is wired in `_do_bronze` between `_ensure_target_table_exists` and the MERGE; composes cleanly with P1.17e's payload-diff predicate. Shared explicit-list clause builders live in a new neutral module `orchestrator/merge_sql.py` to break the circular-import risk that would otherwise exist if they lived in `__init__.py` (registry imports the builders at module-load time; builders need the helpers).

Unit-test coverage: 17 new tests — `TestSchemaReconcileHelper` (7 helper-unit), `TestSchemaEvolution` (4 dispatch-runtime), `TestExplicitColumnListMergeSyntax` (3 builder-SQL-shape), plus 4 `test_import_graph.py` regression guards against future circular-import reintroductions. Pre-existing 770-test suite continues to pass. Live evidence: `tests/live/TC30c_p117d_schema_evolution_results.md` (3-phase probe — seed baseline → source-wider monkey-patch → target-wider ALTER ADD COLUMNS + sentinel-backfill).

> *Pre-fix P1.17-L6: see the original §Active entry that lived here before P1.17d — operator-discipline manual `ALTER TABLE` + retry, ~30s per affected PVO per cycle. Documented as expected V1 friction.*

---

When a limit is resolved, move its entry here with the resolution date, commit SHA, and a one-line summary of what shipped to resolve it.

---

## Cross-references

* [`BACKLOG.md`](BACKLOG.md) — items tracking limit mitigations (P2.16, P2.17, P3.6, P3.8, P3.9, P1.6 blocker)
* [`BLOCKER_P1.6_dim_item.md`](BLOCKER_P1.6_dim_item.md) — full detail on L2 (BICC encoder bug)
* [`STATUS.md`](STATUS.md) — current project state including which limits are biting now
