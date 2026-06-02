"""Orchestrator exception classes.

Single home for every error class the orchestrator raises, separated from
``runtime.py`` to avoid an import cycle: ``runtime.py`` defines RunStep
factories that take spec types from ``registry.py``; ``registry.py`` defines
resolvers that raise ``MissingDependencyError``. Both modules import
``errors.py`` and neither imports the other transitively.

All user-facing config / pre-dispatch errors inherit from
``OrchestratorConfigError`` so the §4.5 CLI exit-2 path catches them all via
a single marker — adding a new error class never edits the CLI's except clause.
"""

from __future__ import annotations


class OrchestratorConfigError(Exception):
    """Marker base class for user-facing config / pre-dispatch errors.

    The CLI's ``_run_inline`` (§4.5) catches ``(OrchestratorConfigError,
    NotImplementedError)`` and exits 2 with ``str(exc)`` — no traceback.
    Subclasses must produce a self-explanatory ``__str__`` (the CLI prints
    it verbatim without extra framing).

    Subclasses (as of P1.5α):
      - BundleLoadError          — bundle.yaml load failures (§4.4b)
      - BundleVersionMismatchError(BundleLoadError) — version-specific (§4.4d)
      - UnsupportedModeError     — mode not in {seed, incremental} (§4.4c)
      - MissingDependencyError   — logical missing dep (registry typo, §4.4)
      - PrerequisiteError        — extra-plan table missing (§4.7)
      - CredentialResolutionError — bundle.fusion.password unresolvable (§4.9)
    """


class OrchestratorRuntimeError(Exception):
    """Base class for failures DURING orchestrator dispatch (P1.5β.1).

    Distinct from :class:`OrchestratorConfigError`, which signals problems
    gating the run BEFORE dispatch (bundle load, identifier validation,
    credential resolution, missing dependency, etc.). Runtime errors are
    raised by per-step execution paths and are caught by the
    ``_execute_node`` outer try/except → ``RunStep.failed`` → cascade.

    Subclasses (as of P1.5β.1):
      - WatermarkMonotonicityError — captured bronze cursor regressed
        below the prior-success row's persisted cursor
      - MultipleUpstreamWatermarkError — resolver hit a spec with
        ``len(depends_on_bronze) >= 2`` and no per-upstream policy
        is shipped yet (P1.17's decision)

    Inherits directly from ``Exception``, NOT from
    ``OrchestratorConfigError``: the CLI exit-2 catch is for
    pre-dispatch errors only; runtime errors surface through the
    normal per-step state-row path so the operator sees them as
    ``status='failed'`` rows with the exception ``repr`` in
    ``error_message``.
    """


class WatermarkMonotonicityError(OrchestratorRuntimeError):
    """Captured bronze cursor is strictly less than the prior-success
    row's persisted cursor (clock regression / bronze-clock-skew bug).

    Phase β.1 captures the persisted cursor as
    ``extract_started_at - WATERMARK_SAFETY_WINDOW`` where
    ``extract_started_at = datetime.now(timezone.utc)`` immediately
    before the BICC extract. Under normal forward-time conditions
    the cursor strictly increases each run, so this error never
    fires; it exists as a defensive invariant for clock-jumping
    VMs (NTP correction, suspend/resume warp) and for a future
    change that re-introduces a non-wall-clock cursor source.

    Carries the prior and new watermarks plus the offending
    ``dataset_id`` in the message so the operator can correlate
    with the state-table row directly.
    """

    def __init__(
        self,
        *,
        prior: object,
        new: object,
        dataset_id: str,
    ) -> None:
        super().__init__(
            f"watermark monotonicity violation for dataset_id={dataset_id!r}: "
            f"new={new!r} < prior={prior!r}. The captured bronze cursor "
            f"regressed below the prior-success row's persisted cursor. "
            f"This usually indicates AIDP clock regression (NTP correction "
            f"or VM clock warp on suspend/resume) larger than "
            f"WATERMARK_SAFETY_WINDOW; investigate orchestrator-host clock "
            f"before re-running."
        )
        self.prior = prior
        self.new = new
        self.dataset_id = dataset_id


class MultipleUpstreamWatermarkError(OrchestratorRuntimeError):
    """A silver/gold spec has more than one upstream bronze
    dependency, and the per-upstream watermark policy hasn't been
    decided yet (P1.17 picks one).

    No shipped silver dim or gold mart hits this today; the resolver
    raises eagerly so any future registry entry with
    ``len(depends_on_bronze) >= 2`` surfaces the policy gap at
    dispatch time rather than silently picking ``depends_on_bronze[0]``.
    """


class BundleLoadError(OrchestratorConfigError):
    """Wraps every bundle.yaml load failure into one class so the CLI's
    exit-2 path catches them uniformly. Five failure modes (§4.4b):
    file unreadable, YAML parse error, env-var missing, Pydantic
    schema violation, bad ``aidp.*`` SQL identifier.
    """


class BundleVersionMismatchError(BundleLoadError):
    """Raised when ``bundle.version`` is unknown to this plugin build.
    The message names the offending version + the supported set + the
    ``aidp-fusion-bundle migrate-bundle`` remediation command (§4.4d).
    Inherits from ``BundleLoadError`` because it is a load failure
    with a specific remediation; the §4.5 catch picks it up via the
    transitive ``OrchestratorConfigError`` ancestor.
    """


class UnsupportedModeError(OrchestratorConfigError, ValueError):
    """Mode value not in ``_VALID_MODES`` (§4.4c). Multi-inherits ``ValueError``
    so legacy callers that ``except ValueError:`` still work. Carries the
    retired-alias hint (``"full"`` → ``"seed"``) in the message so operators
    see the remediation without grepping the decision doc.
    """


class MissingDependencyError(OrchestratorConfigError):
    """Logical missing dependency — a bundle.yaml name that doesn't resolve
    in any registry (BRONZE_EXTRACTS / SILVER_DIMS / GOLD_MARTS /
    KNOWN_DEFERRED_*). Raised by the ``_resolve_*`` functions in registry.py
    AND by ``resolve_plan(...)`` when a dataset_id has no provider.
    """


class PrerequisiteError(OrchestratorConfigError):
    """Extra-plan provider's Delta table doesn't exist on disk. Raised by
    ``_preflight_external_deps`` before any module dispatch. The message
    includes a redirect ("include layer X" / "--datasets Y first").
    """


class CredentialResolutionError(OrchestratorConfigError):
    """``bundle.fusion.password`` could not be resolved — missing env var,
    inaccessible vault OCID, OCI SDK auth/network/ServiceError. Surfaced
    at exit-2 via the §4.4 step-3.5 preflight, NOT as a per-step ``failed``
    row at first bronze dispatch (§4.9 + B5).
    """


class DiscoveryProbeError(OrchestratorConfigError):
    """The ``/biacm/rest/meta/datastores`` probe itself failed — HTTP 5xx,
    auth, network. Used by ``orchestrator/discovery.py`` so preflight can
    decide whether to surface this directly OR fall back to the original
    schema-not-found classification (P1.5α-fix19).

    DISTINCT from :class:`BronzeSchemaProbeError` (which is the
    ``inferSchema``-on-a-PVO failure). Different remediation:

    - BronzeSchemaProbeError → fix bundle.yaml / catalog
    - DiscoveryProbeError    → retry / check network / check creds

    Today's preflight catches this internally and surfaces it as a
    *modifier* on BronzeSchemaProbeError (so the operator sees BOTH "the
    original schema is wrong AND we tried to auto-discover but the BICC
    probe also failed"). The dedicated class keeps the two failure modes
    distinguishable for programmatic callers.
    """


class ResumeRunNotFoundError(OrchestratorConfigError):
    """``--resume <run_id>`` referenced a run with zero rows in
    ``fusion_bundle_state``. Operator typo on the run_id or a state-
    table truncate between original run and resume. Message includes
    the offending run_id and a hint about how to find a valid run_id
    (the ``status`` command surfaces recent runs).
    """


class ResumeRunNotResumableError(OrchestratorConfigError):
    """``--resume <run_id>`` referenced a run whose rows lack the
    drift-gate metadata (``plan_hash`` or ``plan_snapshot`` is NULL).

    Two structural subcases, both rejected by the same gate:
      1. **Legacy row** — original run completed under a plugin build
         that didn't write ``plan_hash``; the drift gate has nothing
         to compare against.
      2. **Partially-migrated row** — a write path skipped the
         ``plan_snapshot`` even though ``plan_hash`` was written.
         Without the snapshot the resume flow can't reconstruct scope
         from bare ``--resume`` and can't diff identity for drift
         diagnostics.

    There is no "resume with degraded metadata" path; both subcases
    require re-running from scratch. Message names the structural
    reason so the operator doesn't conflate this with
    ``ResumeRunNotFoundError`` (the run exists; it's just not
    resumable).
    """


class ResumeBundleMismatchError(OrchestratorConfigError):
    """The bundle being resumed against differs from the bundle that
    started the original run. The drift gate computes the current
    plan hash + identity and compares to the stored values; any
    divergence in any of:

      * plan shape (dataset_id / layer / mode / effective_schema)
      * Fusion pod + storage + principal
        (``fusion.serviceUrl`` / ``fusion.externalStorage`` /
        ``fusion.username``)
      * AIDP target paths (``aidp.{catalog, bronzeSchema,
        silverSchema, goldSchema}``)
      * plugin code version (``__version__``)

    ...raises this. Message renders:
      1. **Identity diff first** — one line per changed identity
         field (``aidp.silverSchema: "silver_v1" → "silver_v2"``,
         etc.). This is the high-frequency case (bumped a schema,
         upgraded the plugin, switched principal) and the operator
         wants to see it before scrolling.
      2. **Dataset diff second** — added / removed dataset_ids +
         per-dataset deltas derived from ``plan_snapshot.nodes``.
      3. **Hash echo last** — ``stored_hash`` and ``current_hash``
         so the operator can correlate with the state-table row.

    The renderer assumes ``plan_snapshot`` is non-NULL —
    ``read_resumable_state`` is the gatekeeper and guarantees it. The
    "no snapshot available" fallback exists only as a defense-in-depth
    branch for direct renderer calls outside the resume flow (e.g. a
    future debug command); it is NOT reachable from ``--resume``.
    """


class IncrementalCursorMissingError(OrchestratorConfigError):
    """One or more silver/gold datasets lack a prior ``last_watermark`` in
    ``fusion_bundle_state`` and cannot run in ``--mode incremental``.

    Raised by ``_preflight_incremental_cursors`` (P1.17 B4b) at run-level,
    AFTER ``ensure_state_table`` and BEFORE the dispatch loop. Single
    consolidated error lists every affected dataset so the operator sees
    the full remediation list at once instead of fix-rerun-fix-rerun.

    The error inherits from :class:`OrchestratorConfigError` so the CLI's
    ``_run_inline`` exit-2 catch fires it cleanly — no traceback, no
    partial dispatch, no half-materialized state.

    Bronze nodes are NOT checked: bronze tolerates a null prior cursor
    (full extract → MERGE inserts every row → fresh-tenant bronze
    succeeds without prior cursor). ``dim_calendar`` and any
    ``GoldMartSpec`` with ``incremental_capable=False`` (e.g.
    ``supplier_spend``, ``ap_aging``) are also skipped — they route
    through seed-shape regardless of mode.
    """

    def __init__(self, *, missing: list[tuple[str, str]]) -> None:
        self.missing = missing
        bullets = "\n".join(f"  - {ds} ({layer})" for ds, layer in missing)
        super().__init__(
            f"{len(missing)} silver/gold dataset(s) lack a prior "
            f"last_watermark in fusion_bundle_state and cannot run in "
            f"--mode incremental:\n"
            f"{bullets}\n"
            f"--mode incremental requires a prior --mode seed run to have "
            f"populated each layer's cursor. Run `aidp-fusion-bundle run "
            f"--mode seed` (full bundle) OR `aidp-fusion-bundle run --mode "
            f"seed --datasets <listed_above>` (scoped). After seed completes "
            f"successfully, re-run incremental.\n\n"
            f"Note: if you just ran a seed and still see this error, check "
            f"the orchestrator logs for the marker `watermark_read_soft_failed` "
            f"— a transient metastore failure may have prevented the cursor "
            f"read. Re-running incremental usually clears it; if the WARN "
            f"persists, escalate per LIMITS.md L6."
        )


class IncrementalTargetMissingError(OrchestratorConfigError):
    """One or more in-scope plan nodes have a non-NULL ``last_watermark``
    in ``fusion_bundle_state`` but their target Delta table is missing
    on disk — running ``--mode incremental`` would silently lose
    history below the prior cursor.

    Raised by ``_preflight_incremental_cursors`` (P1.17c) at run-level,
    AFTER the cursor-presence check passes and BEFORE the dispatch
    loop. Single consolidated error lists every affected
    ``(dataset_id, layer, target_table)`` so the operator sees the
    full remediation list at once.

    Failure scenario: operator drops a bronze/silver/gold target
    (recovery, schema reset, accidental ``DROP TABLE`` via Spark SQL
    outside the orchestrator) without clearing the matching state
    row. The next incremental run would (1) auto-create the empty
    target via ``CREATE TABLE IF NOT EXISTS`` (per B6c), (2) MERGE
    only the delta slice (BICC filter / silver-gold source predicate
    excludes everything older than the still-non-NULL prior cursor),
    (3) lose every row whose lineage timestamp is below the cursor —
    permanently, without any "failed" status in state.

    The check covers bronze, silver, and gold; honors the same
    skip-list as the cursor check (``DeferredSpec``, ``dim_calendar``,
    ``GoldMartSpec`` with ``incremental_capable=False``). Bronze IS
    in scope here even though it's skipped by the cursor check —
    bronze has a safe NULL-cursor fallback (full extract) but NO
    safe fallback when its target is dropped under a non-NULL cursor.

    Inherits from :class:`OrchestratorConfigError` so the CLI's
    ``_run_inline`` exit-2 catch fires it cleanly — no traceback, no
    partial dispatch, no half-materialized state.

    See LIMITS.md §P1.17-L5 (resolved by P1.17c) for the full
    failure-mode write-up + the historical interim mitigation.
    """

    def __init__(self, *, missing: list[tuple[str, str, str]]) -> None:
        self.missing = missing
        bullets = "\n".join(
            f"  - {ds} ({layer}): {target}" for ds, layer, target in missing
        )
        super().__init__(
            f"{len(missing)} target Delta table(s) are missing on disk "
            f"but fusion_bundle_state still carries a non-NULL "
            f"last_watermark — running --mode incremental would silently "
            f"lose history below the prior cursor:\n"
            f"{bullets}\n"
            f"Remediation: for each (dataset_id, layer) above, clear the "
            f"matching state row via `DELETE FROM <bronze_schema>."
            f"fusion_bundle_state WHERE dataset_id = '<X>' AND layer = "
            f"'<Y>'`, then re-run `aidp-fusion-bundle run --mode seed "
            f"--datasets <X>` to recreate the target. Only then is "
            f"--mode incremental safe to resume. See LIMITS.md "
            f"§P1.17-L5 for the full sequence."
        )


class MultipleNaturalKeyError(OrchestratorConfigError):
    """A spec's natural_key was overridden in a way that conflicts with
    the catalog's natural_key for the same upstream PVO.

    Defensive — no shipped customer override path triggers this today;
    introduced under P1.17 for forward-compat with a hypothetical
    bundle.yaml customer override that names a different natural key
    than the catalog. If exercised, message names the dataset_id and
    both candidate keys so the operator can reconcile.
    """


class BronzeSchemaProbeError(OrchestratorConfigError):
    """At least one bronze PVO's BICC schema/PVO-name probe failed before
    any data write. Surfaced at exit-2 via the §4.4 step-5.6 preflight, NOT
    as per-step ``failed`` rows at first bronze dispatch. Catches:

    - ``DATA_ACCESS_LAYER_0031: Schema X not found`` — catalog declares a
      BICC offering schema that doesn't exist on this tenant (P1.5α-fix17
      origin story: ``schema="SCM"`` on saasfademo1).
    - PVO renamed / removed since the catalog was confirmed.
    - BICC server unreachable / credential rejected at the BICC reader layer.

    Message lists every offending PVO + the underlying short error so the
    operator can fix the catalog or the bundle without re-dispatching a long
    run. ``failures`` carries structured detail for programmatic callers.
    """

    def __init__(self, message: str, failures: list[dict] | None = None) -> None:
        super().__init__(message)
        self.failures = failures or []


class StateReadFailedError(OrchestratorConfigError):
    """A preflight-context read of ``fusion_bundle_state`` raised
    underneath ``read_last_watermark_strict``.

    Distinct from :class:`IncrementalCursorMissingError`: that error
    fires when the read succeeds but no ``status='success'`` row exists
    (or the row exists but ``last_watermark IS NULL``). This error fires
    when the read itself raised — metastore unreachable, catalog ACL
    drift, Delta-log corruption, schema rename out from under the table
    path, etc. We cannot determine whether a prior cursor exists, so
    preflight refuses to run ``--mode incremental``.

    Why preflight needs a strict variant (P1.17c):
        :func:`oracle_ai_data_platform_fusion_bundle.orchestrator.state.read_last_watermark`
        intentionally soft-fails (logs ``watermark_read_soft_failed``
        WARN + returns ``None``) so a transient metastore flake during
        a long medallion run doesn't cascade-skip downstream nodes.
        That contract is load-bearing for the dispatch path. But the
        same swallow-and-continue semantics in a preflight gate would
        mask the dropped-target check on bronze nodes (None looks like
        "no cursor → skip target check") and let the silent-corruption
        sequence through. Preflight switches to
        :func:`read_last_watermark_strict`, which raises this error.

    Remediation: investigate state-table accessibility before
    re-running. From a notebook:
    ``spark.sql("DESCRIBE <state_table>").show()``. Do NOT run
    ``--mode seed`` blind — if the state table itself is unreadable,
    seed's own ``read_last_watermark_strict`` call (via preflight)
    will surface the same error class. Fix the root cause (catalog
    ACL, metastore connectivity, etc.) first.

    The original exception is chained via ``raise ... from cause`` so
    operators see both the high-level reason and the Spark/Hive root
    cause in the traceback.
    """

    def __init__(
        self,
        *,
        dataset_id: str,
        layer: str,
        table_path: str,
        cause: BaseException,
    ) -> None:
        self.dataset_id = dataset_id
        self.layer = layer
        self.table_path = table_path
        self.cause = cause
        # Best-effort short summary of the underlying exception — first
        # line only, truncated, since Spark/Hive tracebacks are often
        # multi-paragraph and the full thing reaches the operator
        # through Python's __cause__ chain anyway.
        cause_summary = str(cause).splitlines()[0][:300] if str(cause) else repr(cause)
        super().__init__(
            f"could not read fusion_bundle_state for "
            f"dataset_id={dataset_id!r}, layer={layer!r} "
            f"(table_path={table_path!r}); preflight cannot determine "
            f"whether a prior cursor exists, refusing to run --mode "
            f"incremental. Underlying error: "
            f"{type(cause).__name__}: {cause_summary}. Investigate "
            f"state-table accessibility (try "
            f"`spark.sql('DESCRIBE {table_path}').show()` from a "
            f"notebook) before re-running."
        )


__all__ = [
    "OrchestratorConfigError",
    "BundleLoadError",
    "BundleVersionMismatchError",
    "UnsupportedModeError",
    "DiscoveryProbeError",
    "MissingDependencyError",
    "PrerequisiteError",
    "CredentialResolutionError",
    "BronzeSchemaProbeError",
    "IncrementalCursorMissingError",
    "IncrementalTargetMissingError",
    "StateReadFailedError",
    "MultipleNaturalKeyError",
    # Resume failure modes
    "ResumeRunNotFoundError",
    "ResumeRunNotResumableError",
    "ResumeBundleMismatchError",
    "OrchestratorRuntimeError",
    "WatermarkMonotonicityError",
    "MultipleUpstreamWatermarkError",
]
