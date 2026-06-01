"""Orchestrator runtime: RunStep / RunSummary dataclasses + factories, bundle
loading, env-var rendering, credential resolution, bronze audit-column
enrichment, state-write wrapper, external-dep preflight.

Single home for the orchestrator's helper infrastructure. The ``run()``
function in ``__init__.py`` is the only consumer; ``registry.py`` provides
spec types + resolvers; ``state.py`` provides the state-table contract.

All public exception classes live in ``errors.py`` (separate module to avoid
a registry ↔ runtime import cycle).
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Literal
from uuid import uuid4

import yaml
from pydantic import SecretStr, ValidationError

from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle
from oracle_ai_data_platform_fusion_bundle.schema.refs import render_vars

from .errors import (
    BundleLoadError,
    BundleVersionMismatchError,
    CredentialResolutionError,
    MissingDependencyError,
    OrchestratorConfigError,
    PrerequisiteError,
    UnsupportedModeError,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

    from .registry import (
        BronzeExtractSpec,
        DeferredSpec,
        GoldMartSpec,
        SilverDimSpec,
    )

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mode validation (§4.4c) + helpers
# ---------------------------------------------------------------------------

_VALID_MODES: Final[frozenset[str]] = frozenset({"seed", "incremental"})


# ---------------------------------------------------------------------------
# P1.5β.1 — watermark safety window
# ---------------------------------------------------------------------------
#
# The bronze closure captures the orchestrator wall clock immediately
# before ``extract_pvo`` as ``extract_started_at``, then persists the
# state-table cursor as ``extract_started_at - WATERMARK_SAFETY_WINDOW``.
# The overlap absorbs AIDP-vs-Fusion clock skew — the next incremental
# run's BICC filter is evaluated against Fusion's clock, not AIDP's,
# and the next run re-extracts the overlap. P1.17's MERGE-by-natural-key
# write strategy dedupes the re-extracted rows.
#
# β.1 uses a **hardcoded module-level constant** (no ``bundle.yaml`` knob,
# no env override). Per-tenant configurability lands in P1.17 paired
# with the actual ``extract_pvo(watermark=...)`` threading — the cursor
# is captured here but NOT consumed by BICC in this PR (the
# ``NotImplementedError`` gate stays). If a tenant's observed AIDP-vs-
# Fusion clock skew exceeds 1 hour, widening the constant before
# enabling P1.17 is the only intervention.
#
# Industry-standard CDC pattern (Debezium / Kafka Connect / Airbyte all
# use safety-windowed cursors for cross-system incremental extraction).
WATERMARK_SAFETY_WINDOW: Final[timedelta] = timedelta(hours=1)


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _new_run_id() -> str:
    """One UUID4 per orchestrator invocation. Joins back to
    ``fusion_bundle_state.run_id`` and (post-B3) silver_run_id / gold_run_id
    audit columns on the materialized tables.
    """
    return str(uuid4())


# ---------------------------------------------------------------------------
# Skip-reason message templates (B1.1 — structured cascade/abort discrimination)
# ---------------------------------------------------------------------------
# Centralized so downstream consumers branch on RunStep.skip_reason (typed enum)
# rather than substring-matching error_message. Future contributors changing
# wording MUST update these constants — the §8 test asserts factory output
# matches.

_CASCADE_MSG_TMPL: Final[str] = "cascade: upstream {upstream!r} failed"
_ABORT_MSG_TMPL:   Final[str] = "aborted: run halted on prior failure of {failed!r}"
# Carries forward a node that already succeeded under this run_id
# (or was carried-forward under a prior resume) — the table is on
# disk, nothing to do.
_RESUME_SKIP_MSG_TMPL: Final[str] = "resume-skip: succeeded under run {run_id!r}"


# ---------------------------------------------------------------------------
# RunStep + RunSummary dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunStep:
    """One row of orchestrator telemetry. Mirrors the
    ``fusion_bundle_state`` schema (§3.2). Constructed via classmethod
    factories (``success`` / ``failed`` / ``skipped_cascade`` /
    ``skipped_aborted`` / ``deferred``) — never instantiated directly.
    """

    run_id: str
    dataset_id: str
    layer: Literal["bronze", "silver", "gold"]
    mode: Literal["seed", "incremental"]
    # ``resumed_skipped`` is the third "skipped" flavor — set when a
    # resume carries forward a node whose latest terminal status is
    # ``success`` (or a prior ``resumed_skipped``). Distinct from
    # cascade-skip and abort-skip so SOX/audit consumers can tell
    # "carried forward from a prior run" apart from "pre-empted by
    # an upstream failure".
    status: Literal["success", "failed", "skipped", "deferred", "resumed_skipped"]
    row_count: int | None
    duration_seconds: float
    error_message: str | None
    # ``watermark_used`` is the INPUT — the watermark resolved at
    # dispatch time. In β.1 it stays in-memory only (debug/logs,
    # ``__repr__``, test assertions) and is NOT written to any state
    # column. The prior success row's ``last_watermark`` is the
    # implicit input audit (see B0 of the P1.5β plan).
    watermark_used: datetime | None
    # ``last_watermark`` is the OUTPUT — the value persisted to the
    # ``last_watermark`` column. Bronze closures capture
    # ``extract_started_at - WATERMARK_SAFETY_WINDOW`` here (or fall
    # back to ``prior_watermark`` on an empty delta to preserve
    # progress). Silver/gold rows leave it at ``None`` until P1.17
    # ships per-layer lineage capture.
    last_watermark: datetime | None = None
    # Structured discriminator for the three `skipped` flavors and
    # the ``resumed_skipped`` carry-forward. Persisted to
    # fusion_bundle_state.skip_reason (§3.2). NULL for non-skipped /
    # non-resumed-skipped rows.
    skip_reason: Literal["cascade", "aborted", "resume-skip"] | None = None
    # Plan hash + canonical snapshot persisted on every row of a
    # run, threaded through every factory call so the frozen
    # instance carries the right metadata at construction (direct
    # post-construction assignment would raise ``FrozenInstanceError``).
    # Legacy rows from earlier plugin builds land NULL on both;
    # ``read_resumable_state`` rejects those as non-resumable.
    plan_hash: str | None = None
    plan_snapshot: str | None = None

    # --- Factories ---------------------------------------------------------

    @classmethod
    def success(
        cls,
        spec: Any,  # BronzeExtractSpec | SilverDimSpec | GoldMartSpec
        run_id: str,
        mode: str,
        *,
        row_count: int,
        duration_seconds: float,
        watermark_used: datetime | None = None,
        last_watermark: datetime | None = None,
        plan_hash: str | None = None,
        plan_snapshot: str | None = None,
    ) -> "RunStep":
        """Step ran and produced rows. ``row_count`` is the materialized
        target's count (per §4.4 — never ``df.count()`` on a lazy plan
        for the bronze branch).

        ``last_watermark`` (P1.5β.1) is the OUTPUT cursor for bronze
        rows — ``extract_started_at - WATERMARK_SAFETY_WINDOW`` on a
        non-empty run, or the preserved ``prior_watermark`` on an
        empty-delta run. Silver/gold leave it at ``None`` (capture
        deferred to P1.17).
        """
        from .registry import _layer_for_spec
        return cls(
            run_id=run_id,
            dataset_id=spec.dataset_id,
            layer=_layer_for_spec(spec),
            mode=mode,  # type: ignore[arg-type]
            status="success",
            row_count=row_count,
            duration_seconds=duration_seconds,
            error_message=None,
            watermark_used=watermark_used,
            last_watermark=last_watermark,
            plan_hash=plan_hash,
            plan_snapshot=plan_snapshot,
        )

    @classmethod
    def failed(
        cls,
        spec: Any,
        run_id: str,
        mode: str,
        *,
        exc: BaseException,
        duration_seconds: float,
        plan_hash: str | None = None,
        plan_snapshot: str | None = None,
    ) -> "RunStep":
        """Module dispatch raised. ``error_message`` carries ``repr(exc)`` so
        the state-row preserves the type + args for post-mortem grepping
        (distinguishing BICC 503 from Spark AnalysisException without
        re-running)."""
        from .registry import _layer_for_spec
        return cls(
            run_id=run_id,
            dataset_id=spec.dataset_id,
            layer=_layer_for_spec(spec),
            mode=mode,  # type: ignore[arg-type]
            status="failed",
            row_count=None,
            duration_seconds=duration_seconds,
            error_message=repr(exc),
            watermark_used=None,
            last_watermark=None,
            plan_hash=plan_hash,
            plan_snapshot=plan_snapshot,
        )

    @classmethod
    def skipped_cascade(
        cls,
        spec: Any,
        run_id: str,
        mode: str,
        *,
        upstream_dataset_id: str,
        plan_hash: str | None = None,
        plan_snapshot: str | None = None,
    ) -> "RunStep":
        """Cascade-skip: an upstream dependency of ``spec`` failed. Called
        by ``_skip_dependents``. Sets ``skip_reason='cascade'``."""
        from .registry import _layer_for_spec
        return cls(
            run_id=run_id,
            dataset_id=spec.dataset_id,
            layer=_layer_for_spec(spec),
            mode=mode,  # type: ignore[arg-type]
            status="skipped",
            row_count=None,
            duration_seconds=0.0,
            error_message=_CASCADE_MSG_TMPL.format(upstream=upstream_dataset_id),
            watermark_used=None,
            last_watermark=None,
            skip_reason="cascade",
            plan_hash=plan_hash,
            plan_snapshot=plan_snapshot,
        )

    @classmethod
    def skipped_aborted(
        cls,
        spec: Any,
        run_id: str,
        mode: str,
        *,
        failed_dataset_id: str,
        plan_hash: str | None = None,
        plan_snapshot: str | None = None,
    ) -> "RunStep":
        """Abort-skip: the run halted on ``failed_dataset_id``'s failure and
        ``spec`` is an unattempted independent-branch node. Called by
        ``_abort_remaining``. Sets ``skip_reason='aborted'``."""
        from .registry import _layer_for_spec
        return cls(
            run_id=run_id,
            dataset_id=spec.dataset_id,
            layer=_layer_for_spec(spec),
            mode=mode,  # type: ignore[arg-type]
            status="skipped",
            row_count=None,
            duration_seconds=0.0,
            error_message=_ABORT_MSG_TMPL.format(failed=failed_dataset_id),
            watermark_used=None,
            last_watermark=None,
            skip_reason="aborted",
            plan_hash=plan_hash,
            plan_snapshot=plan_snapshot,
        )

    @classmethod
    def deferred(
        cls,
        spec: Any,  # DeferredSpec
        run_id: str,
        mode: str,
        *,
        error_message: str,
        plan_hash: str | None = None,
        plan_snapshot: str | None = None,
    ) -> "RunStep":
        """Spec is a ``DeferredSpec`` — module not yet shipped. Layer comes
        from ``spec.layer`` directly (DeferredSpec carries it).
        ``duration_seconds=0.0`` (no work was done)."""
        return cls(
            run_id=run_id,
            dataset_id=spec.dataset_id,
            layer=spec.layer,
            mode=mode,  # type: ignore[arg-type]
            status="deferred",
            row_count=None,
            duration_seconds=0.0,
            error_message=error_message,
            watermark_used=None,
            last_watermark=None,
            plan_hash=plan_hash,
            plan_snapshot=plan_snapshot,
        )

    @classmethod
    def resumed_skip(
        cls,
        spec: Any,  # BronzeExtractSpec | SilverDimSpec | GoldMartSpec | DeferredSpec
        run_id: str,
        mode: str,
        *,
        row_count: int | None = None,
        last_watermark: datetime | None = None,
        plan_hash: str | None = None,
        plan_snapshot: str | None = None,
    ) -> "RunStep":
        """Resume-skip — node already succeeded under the original
        ``run_id`` (or was carried-forward by a prior resume). Sets
        ``status='resumed_skipped'``, ``skip_reason='resume-skip'``,
        ``duration_seconds=0.0``. Distinct from cascade/abort skips so
        SOX-audit consumers can tell "this row was carried forward
        from a prior run" apart from "this row never got a chance to
        run".

        ``row_count`` carries forward the original successful row's
        count so the latest-per-(run_id, dataset_id) projection (and
        the ``fusion_bundle_state_latest`` VIEW) preserve the logical
        row count instead of showing NULL. Caller passes
        ``resume_context.succeeded_row_counts[(node.dataset_id, layer)]``.

        ``last_watermark`` (P1.5β.1) carries forward the original
        bronze run's persisted cursor so a resumed_skipped row does
        not regress the watermark to ``NULL`` on the
        ``fusion_bundle_state_latest`` projection. Caller passes
        ``resume_context.succeeded_last_watermarks[(node.dataset_id,
        layer)]``. Silver/gold + nodes that did not advance a
        watermark in the original run pass ``None`` (default).

        ``error_message`` is informational, not an error: it names
        the original ``run_id`` so the audit chain stays visible from
        the carry-forward row itself.
        """
        from .registry import BronzeExtractSpec, DeferredSpec, GoldMartSpec, SilverDimSpec, _layer_for_spec
        # DeferredSpec carries .layer directly; the other three derive it.
        if isinstance(spec, DeferredSpec):
            layer = spec.layer
        elif isinstance(spec, (BronzeExtractSpec, SilverDimSpec, GoldMartSpec)):
            layer = _layer_for_spec(spec)
        else:  # pragma: no cover — defensive
            raise TypeError(f"resumed_skip: unsupported spec type {type(spec)!r}")
        return cls(
            run_id=run_id,
            dataset_id=spec.dataset_id,
            layer=layer,
            mode=mode,  # type: ignore[arg-type]
            status="resumed_skipped",
            row_count=row_count,
            duration_seconds=0.0,
            error_message=_RESUME_SKIP_MSG_TMPL.format(run_id=run_id),
            watermark_used=None,
            last_watermark=last_watermark,
            skip_reason="resume-skip",
            plan_hash=plan_hash,
            plan_snapshot=plan_snapshot,
        )


@dataclass(frozen=True)
class RunSummary:
    """Aggregate result of one ``orchestrator.run(...)`` invocation.

    Normal runs leave ``plan`` and ``prereqs`` at None — the per-step
    ``RunStep`` rows in ``steps`` are the canonical "what was attempted"
    record. The two optional fields are populated only by the
    ``.empty(...)`` classmethod for paths that didn't dispatch (empty
    bundle or ``dry_run=True``).
    """

    run_id: str
    started_at: datetime
    finished_at: datetime
    bundle_project: str
    mode: str
    steps: tuple[RunStep, ...]
    plan: tuple[object, ...] | None = None
    prereqs: tuple[object, ...] | None = None

    # P1.5α-fix19: operator-actionable strings emitted by preflight
    # auto-discovery. Each entry is a recommendation the CLI renders in
    # the summary footer (e.g. "consider adding schemaOverrides.po_receipts:
    # Financial to bundle.yaml to stabilize across runs"). Empty on a clean
    # run with no auto-corrections.
    recommendations: tuple[str, ...] = ()

    # Counter properties — sum to len(steps).

    @property
    def succeeded(self) -> int:
        return sum(1 for s in self.steps if s.status == "success")

    @property
    def failed(self) -> int:
        return sum(1 for s in self.steps if s.status == "failed")

    @property
    def skipped(self) -> int:
        return sum(1 for s in self.steps if s.status == "skipped")

    @property
    def deferred(self) -> int:
        return sum(1 for s in self.steps if s.status == "deferred")

    @property
    def resumed_skipped(self) -> int:
        """Count of carry-forward steps under ``--resume``. Zero on a
        normal (non-resumed) run.
        """
        return sum(1 for s in self.steps if s.status == "resumed_skipped")

    @property
    def total_duration_seconds(self) -> float:
        return sum(s.duration_seconds for s in self.steps)

    @classmethod
    def empty(
        cls,
        bundle_project: str,
        mode: str,
        *,
        plan: tuple[object, ...] | None = None,
        prereqs: tuple[object, ...] | None = None,
    ) -> "RunSummary":
        """Construct a zero-step RunSummary for paths that didn't dispatch.

        Two callers (R1 fix):
          - Empty-bundle path: ``plan`` is empty after ``resolve_plan``.
          - dry_run path: show what *would* have run — populate ``plan`` +
            ``prereqs`` for the CLI renderer.

        Synthetic ``run_id = 'empty-<uuid>'`` since no actual run occurred.
        """
        now = _utc_now()
        return cls(
            run_id=f"empty-{uuid4()}",
            started_at=now,
            finished_at=now,
            bundle_project=bundle_project,
            mode=mode,
            steps=(),
            plan=plan,
            prereqs=prereqs,
        )


# ---------------------------------------------------------------------------
# ExternalDep + _preflight_external_deps (§4.7 layer/dataset filter contract)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExternalDep:
    """An extra-plan dependency — a dataset/dim/mart that was filtered out
    of the current run's plan (via ``--datasets`` or ``--layers``) but is
    required by an in-plan consumer. The preflight asserts it exists on
    disk before any module dispatch.
    """

    dataset_id: str
    layer: Literal["bronze", "silver", "gold"]
    consumer: str  # the in-plan node that requires it
    table_path: str  # 3-part Delta name


def _preflight_external_deps(
    spark: "SparkSession",
    deps: tuple[ExternalDep, ...],
) -> None:
    """For each external dep, assert the Delta table exists. Raises
    ``PrerequisiteError`` naming every missing table + a redirect to
    widen the filter. Microseconds-per-dep cost via ``catalog.tableExists``.
    """
    if not deps:
        return
    missing: list[ExternalDep] = []
    for dep in deps:
        if not spark.catalog.tableExists(dep.table_path):
            missing.append(dep)
    if missing:
        lines = "\n".join(
            f"  - {d.dataset_id} ({d.layer}) at {d.table_path} — required by {d.consumer}"
            for d in missing
        )
        # Recommend widening either by dataset_id or by layer, whichever
        # is more selective; mention both so the operator picks.
        hint_datasets = ",".join(sorted({d.dataset_id for d in missing}))
        hint_layers = sorted({d.layer for d in missing})
        raise PrerequisiteError(
            f"Extra-plan dependencies missing on disk:\n{lines}\n\n"
            f"Either re-run with broader scope (e.g. --datasets {hint_datasets}) "
            f"or include the upstream layer(s): {hint_layers}."
        )


# ---------------------------------------------------------------------------
# Bronze audit-column enrichment (§3.5)
# ---------------------------------------------------------------------------


def enrich_bronze_audit_cols(
    df: "DataFrame",
    *,
    source_pvo: str,
    run_id: str,
    watermark: datetime | None,
    extract_ts: datetime,
) -> "DataFrame":
    """Add the four mandatory bronze audit columns to every row.

    Called by ``_execute_node`` between extract and write. Keeps the
    extractor a pure I/O primitive (CLAUDE.md "modules are stateless");
    ``run_id`` and ``source_pvo`` are orchestrator-owned.

    ``extract_ts`` (P1.5β.1) is the caller-supplied orchestrator wall
    clock captured immediately before the BICC extract — stamped as
    the literal ``_extract_ts`` audit column on every row. Replaces
    the Phase α ``F.current_timestamp()`` self-stamp, which evaluated
    at Spark action time (strictly LATER than the extract instant the
    audit column claims to record). The orchestrator already needs
    this value to compute the state-table cursor
    (``extract_started_at - WATERMARK_SAFETY_WINDOW``), so passing it
    through here keeps the audit column and the cursor strictly
    consistent: ``_extract_ts == extract_started_at`` and
    ``last_watermark == extract_started_at - WATERMARK_SAFETY_WINDOW``,
    with a known gap of exactly one window.

    Distinct from ``watermark`` — that kwarg controls the
    ``_watermark_used`` audit column (records the watermark INPUT
    consumed by the extract). In β.1 the dispatch site passes
    ``watermark=None`` since the ``NotImplementedError`` gate stays
    and the BICC call doesn't consume a watermark; that audit
    column is wired in P1.17.
    """
    from pyspark.sql import functions as F

    return (
        df.withColumn("_extract_ts", F.lit(extract_ts).cast("timestamp"))
        .withColumn("_source_pvo", F.lit(source_pvo))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn(
            "_watermark_used",
            F.lit(watermark).cast("timestamp") if watermark is not None else F.lit(None).cast("timestamp"),
        )
    )


# ---------------------------------------------------------------------------
# Credential resolution (§4.9 + B5)
# ---------------------------------------------------------------------------

_VAULT_SIGIL = re.compile(r"^\$\{vault:(?P<ocid>[A-Za-z0-9._\-]+)\}$")
_ENV_SIGIL   = re.compile(r"^\$\{env:(?P<var>[A-Z_][A-Z0-9_]*)\}$")

# Module-level flag for R3 — flipped by _resolve_password on first
# literal-path hit. Reset to False at module import; tests MUST reset
# between cases via the autouse fixture in tests/unit/conftest.py.
_LITERAL_WARN_EMITTED: bool = False


def _resolve_password(value: str) -> SecretStr:
    """Resolve a bundle.fusion.password value to a SecretStr.

    Accepts (in α):
      - ``${vault:OCID}`` → fetched via ``aidputils.secrets.get(ocid)``
      - ``${env:VAR}`` → ``os.environ[VAR]``
      - literal string → wrapped as-is (WARN-once-per-run; rejected
        entirely in P2.23).

    Failure-mode wrapping (B5):
      - ``${env:X}`` missing → ``CredentialResolutionError`` naming X
      - ``${vault:OCID}`` inaccessible → ``CredentialResolutionError``
        naming the OCID + the underlying SDK message
      The bare exception chain is preserved via ``raise ... from e``.
    """
    if m := _VAULT_SIGIL.match(value):
        ocid = m["ocid"]
        try:
            # Lazy import — aidputils is an AIDP-runtime package; not
            # available in standalone test environments.
            from aidputils import secrets as _aidp_secrets  # type: ignore[import-not-found]
            return SecretStr(_aidp_secrets.get(ocid))
        except Exception as e:
            raise CredentialResolutionError(
                f"Vault secret {ocid!r} could not be resolved for "
                f"bundle.fusion.password: {e}. Check the OCID is valid, "
                f"the vault exists, and the runtime identity has "
                f"`SECRET_FAMILY_READ` on it."
            ) from e
    if m := _ENV_SIGIL.match(value):
        var = m["var"]
        try:
            return SecretStr(os.environ[var])
        except KeyError as e:
            raise CredentialResolutionError(
                f"Env var {var!r} referenced by bundle.fusion.password "
                f"is not set. Export it before running, or switch the "
                f"password to a ${{vault:OCID}} reference."
            ) from e
    # Dev-phase: accept literal but warn ONCE per run (R3).
    global _LITERAL_WARN_EMITTED
    if not _LITERAL_WARN_EMITTED:
        logger.warning(
            "fusion.password is a literal; will be rejected by P2.23. "
            "Migrate to ${vault:OCID} or ${env:VAR}. (This warning "
            "fires once per run regardless of how many times "
            "_resolve_password is called.)"
        )
        _LITERAL_WARN_EMITTED = True
    return SecretStr(value)


# ---------------------------------------------------------------------------
# Env-var rendering + load_bundle (§4.4a + §4.4b)
# ---------------------------------------------------------------------------


def _render_env_vars(node: Any) -> Any:
    """Recursively expand ``${VAR}`` env-var refs in a parsed-YAML structure.

    Leaves ``${vault:OCID}`` references untouched (the regex in
    ``schema/refs.py`` has a ``(?!vault:)`` negative-lookahead). Raises
    ``BundleLoadError`` naming the missing variable when an env-var ref
    cannot be resolved — bare ``KeyError`` never bubbles through.
    """
    if isinstance(node, dict):
        return {k: _render_env_vars(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_render_env_vars(v) for v in node]
    if isinstance(node, str):
        try:
            return render_vars(node)
        except KeyError as e:
            raise BundleLoadError(
                f"Missing env var {e.args[0]!r} referenced in bundle.yaml. "
                f"Set it before running, or override on the CLI."
            ) from e
    return node  # int, float, bool, None — pass through


def load_bundle(bundle_path: Path) -> tuple[Bundle, TablePaths]:
    """Load and validate a bundle.yaml, returning the parsed model + resolved paths.

    Single entry point that wraps EVERY config-load failure mode into
    ``BundleLoadError`` so the CLI's exit-2 path catches them all (no
    bare tracebacks for malformed YAML, missing env var, schema
    violations, or bad ``aidp.*`` identifiers).

    Failure modes (§4.4b):
      1. File-not-found / permission / IsADirectoryError / OSError
      2. yaml.YAMLError (malformed YAML)
      3. _render_env_vars KeyError (missing env var) — already wrapped
      4. pydantic.ValidationError (schema violation) — version-specific
         re-raised as ``BundleVersionMismatchError``
      5. TypeError/ValueError from TablePaths._validate_identifier

    Exception chain preserved via ``raise ... from e``.
    """
    bundle_path = Path(bundle_path)

    # 1. File read.
    try:
        text = bundle_path.read_text(encoding="utf-8")
    except FileNotFoundError as e:
        raise BundleLoadError(f"Bundle file not found: {bundle_path}") from e
    except IsADirectoryError as e:
        raise BundleLoadError(
            f"Bundle path is a directory, not a file: {bundle_path}"
        ) from e
    except PermissionError as e:
        raise BundleLoadError(
            f"Cannot read bundle {bundle_path}: permission denied"
        ) from e
    except OSError as e:
        raise BundleLoadError(
            f"Cannot read bundle {bundle_path}: {e.strerror or e}"
        ) from e

    # 2. YAML parse.
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        loc = f" at line {mark.line + 1} col {mark.column + 1}" if mark else ""
        problem = getattr(e, "problem", str(e))
        raise BundleLoadError(
            f"Malformed YAML in {bundle_path}{loc}: {problem}"
        ) from e

    if not isinstance(raw, dict):
        raise BundleLoadError(
            f"Bundle {bundle_path} must be a YAML mapping at the top level, "
            f"got {type(raw).__name__}"
        )

    # 3. Env-var expansion.
    rendered = _render_env_vars(raw)

    # 4. Pydantic validation — hoist version errors into the specific class.
    try:
        bundle = Bundle.model_validate(rendered)
    except ValidationError as e:
        version_errs = [err for err in e.errors() if err["loc"] == ("version",)]
        if version_errs:
            offending = version_errs[0].get("input", "<unknown>")
            raise BundleVersionMismatchError(
                f"Bundle {bundle_path} declares version={offending!r}; "
                f"this plugin supports version='0.2.0'. "
                f"Run `aidp-fusion-bundle migrate-bundle "
                f"--from {offending} --to 0.2.0`."
            ) from e
        details = "\n".join(
            f"  - {'.'.join(str(p) for p in err['loc'])}: {err['msg']}"
            for err in e.errors()
        )
        raise BundleLoadError(
            f"Bundle {bundle_path} failed schema validation:\n{details}"
        ) from e

    # 5. TablePaths identifier validation.
    try:
        paths = TablePaths.from_bundle(bundle.model_dump(by_alias=True))
    except (TypeError, ValueError) as e:
        raise BundleLoadError(
            f"Bundle {bundle_path} has invalid aidp.* identifier: {e}"
        ) from e

    return bundle, paths


# ---------------------------------------------------------------------------
# State-write wrapper (§4.7 — soft, log + continue)
# ---------------------------------------------------------------------------


def _safe_write_state_row(
    spark: "SparkSession",
    paths: TablePaths,
    step: RunStep,
) -> bool:
    """Best-effort per-step state-row write. Logs WARN via the stdlib
    logger and returns False on any exception; does NOT raise.

    The per-step write is SOFT (transient persistence flakes shouldn't
    kill a 45-minute medallion run); the structural ``ensure_state_table``
    check at run start is HARD. Cascade decisions read in-memory
    ``step.status``, decoupled from whether this write succeeded.
    """
    from . import state

    try:
        state.write_state_row(spark, paths, step)
        return True
    except Exception as e:
        logger.warning(
            "state-write failed: dataset_id=%s layer=%s status=%s exc=%r",
            step.dataset_id,
            step.layer,
            step.status,
            e,
        )
        return False


__all__ = [
    # Constants
    "_VALID_MODES",
    "_CASCADE_MSG_TMPL",
    "_ABORT_MSG_TMPL",
    "_RESUME_SKIP_MSG_TMPL",
    "WATERMARK_SAFETY_WINDOW",
    # Helpers
    "_utc_now",
    "_new_run_id",
    "_resolve_password",
    "_render_env_vars",
    "load_bundle",
    "enrich_bronze_audit_cols",
    "_safe_write_state_row",
    "_preflight_external_deps",
    # Dataclasses
    "RunStep",
    "RunSummary",
    "ExternalDep",
    # Re-exported exceptions
    "OrchestratorConfigError",
    "BundleLoadError",
    "BundleVersionMismatchError",
    "UnsupportedModeError",
    "MissingDependencyError",
    "PrerequisiteError",
    "CredentialResolutionError",
]
