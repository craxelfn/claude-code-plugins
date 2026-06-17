# Limits — `oracle-ai-data-platform-fusion-bundle`

> **Purpose**: register every known limitation of this plugin that is **still
> live today** — the constraints we've decided to live with and the ones we're
> tracking toward resolution. A limit is a constraint we cannot make disappear
> with effort proportional to its impact.
>
> **Maintenance rule**:
> - When a new limit is discovered or accepted, add an entry below.
> - When a limit is **resolved**, **delete its entry** — the resolving commit /
>   PR + git history are the record. This file tracks *current* reality only;
>   it is not an archive of fixed issues.
> - When a limit's mitigation status changes (partial workaround lands), update
>   the entry in place.
>
> Everything here reflects the **v2 content-pack** architecture (Phase 9 deleted
> the v1 silver/gold modules, the `python_legacy` adapter, and the
> `--execution-backend` flag — content-pack is the only execution path).
>
> Last updated: **2026-06-15**.

---

## Active limits

### L1 — PVO schema drift across Fusion releases requires patch releases

**What it is**: silver dims and gold marts reference column names sourced from a
one-time live probe of each Fusion BICC PVO (e.g. `CodeCombinationCodeCombinationId`,
`BalanceCodeCombinationId`). When Oracle renames columns or changes value domains
across Fusion releases, the bundle requires a patch — a new `columnAlias` /
`semanticVariant` candidate in `pack.yaml`, or corrected SQL. There is no
architecture that eliminates this; see "Why we can't fix this".

**Severity**: medium (loud, predictable, infrequent)
**Affects**: every silver dim and gold mart that joins on a PVO column (all of them).

**Why we can't fix this** — alternative architectures considered, all reduce to
"someone updates something when Oracle changes something":

| Architecture | Why it doesn't eliminate patches |
|---|---|
| Schema-introspection + dynamic SQL | Can't guess semantics — `BalanceActualFlag='A'` vs `BalanceActivityType='POSTED'` mean the same thing but the filter must change. |
| Config-driven mapping | Pushes the patch from us to the customer, who is worse-positioned to know which release renamed what. (v2's `columnAliases` are bootstrap-resolved candidate *lists* — they absorb *known* variation, not novel renames.) |
| LLM-assisted column resolution | Unreliable; silent wrong-data is worse than loud breakage; runtime LLM cost (and ADR-0017 forbids an LLM in seed/incremental). |
| Pin to one Fusion release forever | Customers upgrade Fusion; pinning becomes unusable later. |
| Oracle freezes PVO schemas | Not our control — Oracle revs them quarterly. |

The fundamental property: marts encode business semantics (column names + value
domains + cast precisions), Oracle owns all three, when they change *something*
in our code/pack must change.

**What's shipped (drift is now loud + pre-flight, not silent + mid-run)** — this
is the "we handle it" half, but the underlying patch requirement remains:
- `bronzeSchemaFingerprint` pinned at `bootstrap`; the **AIDPF-2012** runtime
  gate fails closed (exit 14) on a divergent fingerprint and recommends
  `bootstrap --refresh`, with a per-dataset column-level diff (`datasetDeltas`).
- The **AIDPF-2072** PVO drift gate probes the live PVO before any state write;
  **AIDPF-4070 / 4071** catch source-schema mismatches at/after extract.
- Zero-match variation points escalate to `/medallion-author` (overlay a new
  candidate the pack never anticipated).

**Remaining mitigations (backlog)**: Fusion release-version detection +
support-matrix warning at install time; CI live-PVO regression (see **L4**).

**Customer-facing framing**:
> "Verified against specific Fusion releases. The drift gates confirm
> compatibility on your own pod before a run mutates anything; patch releases
> ship when a covered PVO changes." This matches every other packaged BI mart on
> Fusion (Oracle FAW, SAP packaged content, Informatica accelerators) — none
> ship "patch-free across all upstream upgrades."

---

### L2 — BICC datasource encoder bug blocks any PVO with integer-valued NUMBER columns

**What it is**: the BICC datasource (`format("aidataplatform")`, `type=FUSION_BICC`)
declares Oracle `NUMBER` columns as Spark `DecimalType(38,0)` etc., but at
row-materialization emits `java.lang.Long` for integer-valued cells. Spark's
`ExpressionEncoder` strict validator rejects with `java.lang.Long is not a valid
external type for schema of decimal(38,0)`. Affects any write path
(`saveAsTable`, `format("noop").save()`, `df.collect()`, …).

**Severity**: high (hard-blocks the affected PVO's bronze extract)
**Affects**: confirmed on `ItemExtractPVO` (why `bronze.scm_items` still fails to
materialize — see also **P3-L3**). Any future PVO whose Oracle source has
integer-valued NUMBER columns is at risk; the finance PVOs (`SupplierExtractPVO`,
`InvoiceHeaderExtractPVO`, `CodeCombinationExtractPVO`, …) work by chance of
column shape, not by design. The extract path lives in
`extractors/bicc.py::extract_pvo` (invoked by
`orchestrator/builtins/bronze_extract_adapter.py`).

**Why we can't fix this in the bundle**: the bug is JVM-side, in the
`aidataplatform` connector's runtime emission path — not language-bindable
around. Five workaround variants were attempted (Catalyst cast projection;
RDD-level coercion; all-string schema mirror; uniform `Decimal(38,30)`;
`count()` false-positive) — all failed. Detail:
[`BLOCKER_P1.6_dim_item.md`](BLOCKER_P1.6_dim_item.md).

**Mitigations**: upstream connector fix (drafted, pending AIDP/BICC team
hand-off — connector should box `Long` into `BigDecimal` when the declared type
is `DecimalType`). Bypass option: read the CSV staging files directly from the
`fusion.externalStorage` Object Storage bucket (plugin-portable; needs manifest
discovery + own delta detection).

**Status**: tracked; affected PVOs (notably `scm_items`) deferred.

---

### L4 — CI cannot run live PVO regression without a dedicated test pod

**What it is**: without a CI-accessible Fusion pod, the L1 drift gates only run
on customer pods, not on the bundle's own side. We can't catch regressions
between Fusion releases proactively.

**Severity**: medium (caps how proactive we can be on L1 patches)
**Affects**: maintenance discipline for L1.

**Why we can't fix this in the bundle**: the demo pod (`saasfademo1`) is shared,
rate-limited, and unreliable for scheduled CI (the TC34 attempt hit a transient
`CONNECTOR_0255` outage — exactly the flakiness that disqualifies it); customer
pods must never be used from CI. The real fix is AIDP-side infrastructure.

**Status**: tracked, not actionable on the bundle side.

---

### §L-Resume — `fusion_bundle_state` is multi-row-per-`(run_id, dataset_id)` on resumed runs

**What**: `aidp-fusion-bundle run --resume <run_id>` (content-pack backend). On a
resumed run the state table is append-only and may carry multiple rows per
`(run_id, dataset_id)` — e.g. a `failed` row from the original attempt + a
`resumed_skipped` carry-forward + an eventual `success` can coexist under one
`run_id`. This is intentional: it preserves the medallion `<layer>_run_id`
invariant (a gold row's `gold_run_id` still joins 1:1 to a single logical run,
not split across resume attempts).

**Where it bites**: naïve consumer queries against the raw table — e.g.
`WHERE status='failed'` surfaces stale rows that since succeeded;
`COUNT(*) WHERE run_id=…` overcounts when the run was resumed.

**Mitigation**: a Delta VIEW `fusion_bundle_state_latest` projects one row per
`(run_id, dataset_id)` via `ROW_NUMBER() OVER (PARTITION BY run_id, dataset_id
ORDER BY last_run_at DESC)`. Consumers SHOULD read the VIEW unless they
explicitly need append-only history. The operator-facing
`aidp-fusion-bundle status` is a different aggregation (latest per `dataset_id`
regardless of `run_id`) and stays inline.

**Status**: tracked-by-design — the multi-row shape is load-bearing for audit
traceability; the VIEW is the contract consumers reach for first.

---

### P1.17-L2 — `incremental_capable=False` PVOs re-extract in full every cycle

**What it is**: `gl_period_balances` (`BalanceExtractPVO`, monthly-snapshot
semantics) carries `incremental_capable=False` because BICC's
`fusion.initial.extract-date` filter is not respected for it. Under
`--mode incremental` it still re-extracts the FULL row set every cycle. The bronze
MERGE dedupes by natural key so the target row count doesn't grow, but BICC-side
cost equals seed-mode cost.

**Update (2026-06-16)** — all three originally-listed PVOs were live-probed
(`tests/live/TC35_gl_coa_incremental_results.md`):
- `gl_period_balances` (`BalanceExtractPVO`) — **CONFIRMED IGNORED**: full =
  12,101,410 rows; a recent-watermark extract (2026-12-31, after the max LUD of
  2026-05-27) STILL returned the full 12.1M. Native delta genuinely unavailable —
  the lever is the period-window datastore filter (`bicc-period-window-extract`
  feature), not the native cursor. This is the limit's remaining real case.
- `gl_coa` (`CodeCombinationExtractPVO`) — **REMOVED**: BICC honors the lineage
  delta (recent-watermark = 0 rows vs full 69,578). Now `incremental_capable=True`
  on `CodeCombinationLastUpdateDate`. The prior `false` was inherited by analogy.
- `ap_aging_periods` (`AgingPeriodHeaderExtractPVO`) — **probed HONORED, flipped
  to `incremental_capable=True`** (recent-watermark = 0 vs unfiltered = 2). This is
  catalog-truth only: there is no shipped content-pack bronze node for it, so
  nothing extracts it today; a future node must declare watermark
  `ApAgingPeriodsLastUpdateDate`. Probe discrimination was weak (max LUD 2023
  precedes the test watermarks) but honored-vs-ignored is unambiguous — an ignored
  PVO returns the FULL set under a watermark, not 0.

**Severity**: low (cost, not correctness)
**Affects**: tenants whose daily incremental cost budget assumes BICC
short-circuits on no-op cycles.

**Mitigation**: documented as expected behavior; flip the catalog flag if/when a
live probe confirms BICC honors the cursor for one of the remaining PVOs (as was
done for `gl_coa`).

**Status**: tracked-by-design (narrowed from 3 PVOs to 1 real case — only
`gl_period_balances`, addressed by the period-window feature; `gl_coa` and
`ap_aging_periods` both flipped to `incremental_capable=True` after live probes).

---

### P1.17-L4 — `supplier_spend` rebuilds every incremental cycle (replace strategy)

**What it is**: `supplier_spend` ships `refresh.incremental.strategy: replace`
(CLAUDE.md medallion invariants) — its aggregate grain mixes a mutable fact
attribute (`approval_status`), and a partial-MERGE would leave both old
(`PENDING`) and new (`APPROVED`) rows on a status flip. So it `CREATE OR REPLACE`s
every cycle regardless of `--mode`. Cost ≈ seed-mode cost each run (~13s on
saasfademo1 — trivial in absolute terms).

**Severity**: low (cost, not correctness)
**Affects**: incremental-mode operators expecting per-cycle cost savings on this
mart.

**Mitigation**: the correct aggregate-MERGE pattern (affected-keys +
full-recompute + DELETE for grain-moves) is a post-v0.3 follow-up (PLAN §10.8).

**Status**: tracked-by-design.

---

### P1.17-L8 — `gl_period_balances` composite natural key has a NULL component

**What it is**: `gl_period_balances`'s composite natural key includes
`BalanceTranslatedFlag`, which is NULL on `fusion_bundle_dev`. Standard SQL `=`
does not match `NULL=NULL`; only NULL-safe `<=>` does. The v2 bronze MERGE
template uses `<=>` on every key column for exactly this reason, so the NULL-NULL
match works correctly today. The residual limit is documentation: a tenant where
one of the key columns mixes NULL + non-NULL across rows could still see
surprising dedupe behavior.

**Severity**: low (documented; the NULL-safe operator handles the known case)
**Affects**: tenants whose `gl_period_balances` carries mixed NULL / non-NULL on
any composite-key column.

**Mitigation**: NULL-safe `<=>` in the MERGE ON predicate. Operators who observe
row-matching surprises should escalate with a `DESCRIBE HISTORY` of the affected
commit.

**Status**: tracked-by-design.

---

### P3-L1 — content-pack `ap_aging` ships proxy mode only

**What**: the content-pack `ap_aging.sql` ships **proxy mode only** (buckets on
`invoice_date`, emits `max_days_outstanding`). It does not offer a real-mode
variant (buckets on due_date, emits `max_days_past_due`). This is an intentional
scope decision: runtime coverage-probing is exactly what ADR-0014 removes, and
the two modes have different output schemas which the current renderer cannot
select between from a single template.

**Where it bites**: tenants whose AP data has high Terms/Due-date coverage would,
under a real-mode mart, see due-date-based buckets — the content pack always
produces proxy-shape output instead. Row counts may agree but bucket assignments
and column shapes differ.

**Mitigation**: documented divergence (`docs/v2-phase-3-variation-catalog.md` "AP
aging — proxy mode only"). A real-mode follow-up needs (a) a renderer extension
for two-schema variants, (b) declarative threshold config, and (c) live evidence
of a tenant that actually needs it.

**Status**: tracked-by-design.

---

### §L-Resume-Concurrency — two operators `--resume`-ing the same `run_id` concurrently is unguarded

**What**: there is no lock / leader-election around `--resume`. If two operators
(or one operator + a stuck-but-running prior dispatch) both `--resume <same_run_id>`,
both pass the drift gate (same bundle, same plan_hash) and both write rows under
the same `run_id`. Latest-per-`(run_id, dataset_id)` still yields a coherent
terminal view; intermediate state during the race is inconsistent.

**Mitigation**: operator discipline — don't run two `--resume` for the same
`run_id` concurrently. Real concurrency control (Spark locks, a `running` status
sentinel in the state table) is future scope.

**Status**: tracked, awaiting a future concurrency-control phase.

---

### P3-L2 — content-pack `dim_account` COA role-positioning has two gaps

**What (gap 1 — three role aliases, not six)**: the pack declares `coa_*_segment`
`columnAliases` for three of the six Fusion COA roles (`balancing`, `cost_center`,
`natural_account`). The other three (`subaccount`, `product`, `intercompany`)
have no declared `columnAliases`; `dim_account.sql` emits them via positional
hardcoded references (`CodeCombinationSegment4 AS subaccount`, etc.).

**What (gap 2 — existence-based resolution can't disambiguate role-positioning)**:
even for the three declared roles, the candidate list is a single conventional
default per role (`coa_balancing_segment.candidates: [CodeCombinationSegment1]`).
`columnAliases` resolves by physical column *existence*. All six
`CodeCombinationSegmentN` columns coexist on every `gl_coa` extract, so on a
non-conventional tenant — e.g. one where `balancing` lives at `Segment4` —
bootstrap auto-resolves to `Segment1` (it still exists) and silently binds the
wrong source column.

**Where it bites**: a non-conventional-COA tenant gets roles silently bound to
the wrong `CodeCombinationSegmentN` unless the operator intervenes **before**
`bootstrap`.

**Mitigation (manual today)**: pre-author an `overlays/<name>/pack.yaml`
extending the role's `candidates` with the actual source column (or hand-edit
`profile.resolved.column.coa_<role>_segment`); for the three undeclared roles,
use `/medallion-author` to draft an overlay declaring them and extending
`dim_account.sql` via `{{ column.* }}` tokens. Either way, before `bootstrap`.

**Architectural fix (future)**: a `{{ coa.<role> }}` renderer token consuming
`profile.chartOfAccounts.<role>Segment` integers — bootstrap prompts for each
role's position and the renderer emits `CodeCombinationSegment<N>`. Makes
role-positioning explicit rather than existence-based.

**Status**: tracked-by-design; future renderer feature reserved.

---

### P3c-L1 — legacy tenant profile silently bypasses the drift gate

**What**: the runtime drift gate
(`orchestrator/preflight_evidence.py::check_bronze_fingerprint_drift`) treats a
`bronzeSchemaFingerprint` that is `None`, the sentinel `sha256:placeholder-*`, or
a malformed `sha256:` string as a **legacy profile** and emits
`PreflightOutcome(kind="skip_legacy_profile")` — a one-time WARN, then the run
proceeds. Detection is regex-based: anything not matching `^sha256:[0-9a-f]{64}$`
is treated as legacy.

**Where it bites**: tenants who onboarded before fingerprint pinning, or whose
profile was hand-authored / copied from a fixture, run incrementals without the
drift safety net — a bronze schema change goes undetected by the gate, surfacing
later as a MERGE-time failure or a silent semantic regression.

**Mitigation**: run `aidp-fusion-bundle bootstrap --refresh` once to write a real
`sha256:<64-hex>` fingerprint; subsequent incrementals fire the gate normally.

**Status**: tracked-by-design. A future `AIDP_REQUIRE_PINNED_FINGERPRINT=1` env
flag could flip the policy to hard-fail without a code change.

---

### P3-L3 — some starter-pack bronze nodes ship never-live-validated column names

**What**: seven `fusion-finance-starter` bronze nodes — `ap_payments`,
`ar_invoices`, `ar_receipts`, `gl_journal_lines`, `po_orders`, `po_receipts`,
`scm_items` — were authored from guessed prefix conventions and never
live-validated against the BICC PVO (only the four finance datasets —
`erp_suppliers`, `ap_invoices`, `gl_coa`, `gl_period_balances` — were exercised).
The mismatches are varied per node (e.g. `RaCustTrxAll*` → live `RaCustomerTrx*`;
`PoHeadersAll*` → live bare `PoHeaderId`), not a uniform rename.

**Where it bites**: a seed including any unfixed node fails the `AIDPF-4071`
pre-ingest gate. **Nothing in the starter pack consumes these seven** (no
downstream silver/gold), so the live finance medallion is not blocked.

**Status (partially fixed + live-verified 2026-06-11)**: 5 of 7 had names/types
corrected to the live PVO; 4 of those are live-verified materialized on
saasfademo1 (`ap_payments` 3.48M rows, `ar_invoices` 187,970, `ar_receipts`
64,007, `po_orders` 16,769). Residual:
- `scm_items` — name fix is correct (passes AIDPF-4071) but the node **fails to
  materialize**; this is the **L2** BICC encoder bug, not a name issue.
- `gl_journal_lines`, `po_receipts` — natural-key column has no clean live
  counterpart; under investigation.

The `AIDPF-4071` gate diagnoses each automatically
(`.aidp/diagnostics/<run_id>/AIDPF-4071__<node>.json` with the full live PVO
schema). Correcting them is per-column pack-authoring work — a *pack defect fix*,
not a per-tenant `columnAlias`. Do not trust automated name-matching (difflib
collides across the varied prefixes); use core-exact / semantic matching.

---

## Resolved limits

### §Resolved-FreshTenant — seed assumed medallion schemas + a clean state-table location already existed

**Symptom (fresh tenant / newly created catalog):** the first `run --mode seed`
died at cluster-side state-table init (notebook cell 4) — first with
`InvalidObjectException: There is no database <catalog>.bronze`, then, once the
schema existed, with `DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION` on
`fusion_catalog.bronze.fusion_bundle_state`.

**Root cause:** nothing in the shipped bundle provisioned the inside-the-catalog
prerequisites. The original dev catalog only worked because
`dev/BOOTSTRAP_fusion_catalog.py` (a manual paste-into-notebook script) had
created the bronze/silver/gold schemas once by hand. On a fresh catalog the
schemas were absent, and a prior aborted run could leave an orphaned Delta
location (a `_delta_log` with no metastore entry) at the state-table path.

**Fix (`orchestrator/state.py`):** seed now self-heals the *inside-the-catalog*
prerequisites at run start, idempotently:

* `ensure_schemas()` runs first inside `ensure_state_table()` —
  `CREATE SCHEMA IF NOT EXISTS` for bronze/silver/gold (deduped).
* `_create_or_adopt_state_table()` catches the non-empty-location error and
  **adopts** an orphaned-but-valid Delta location in place
  (`CREATE TABLE ... USING DELTA LOCATION`). Only the non-adoptable garbage
  case raises (**AIDPF-4021**) — the bundle never auto-deletes storage.

**Boundary preserved:** the **catalog** itself remains the operator's one manual
prerequisite (it needs storage/governance config); everything inside it is now
provisioned by seed. Pattern: *missing → create, present → use, orphaned →
adopt.* Verified live on `fusion_bundle_dev` (2026-06-17): the orphaned
`fusion_bundle_state` location was a valid empty Delta table and was adopted
cleanly.

---

## Cross-references

* [`BACKLOG.md`](BACKLOG.md) — items tracking limit mitigations.
* [`BLOCKER_P1.6_dim_item.md`](BLOCKER_P1.6_dim_item.md) — full detail on **L2** (BICC encoder bug).
* [`STATUS.md`](STATUS.md) — current project state including which limits are biting now.
