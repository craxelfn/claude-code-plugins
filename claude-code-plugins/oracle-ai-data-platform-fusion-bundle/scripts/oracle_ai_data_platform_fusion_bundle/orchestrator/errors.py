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
    # Resume failure modes
    "ResumeRunNotFoundError",
    "ResumeRunNotResumableError",
    "ResumeBundleMismatchError",
]
