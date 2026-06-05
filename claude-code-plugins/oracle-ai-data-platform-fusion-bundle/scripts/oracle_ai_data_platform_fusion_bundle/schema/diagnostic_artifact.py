"""Diagnostic artifact schema (PLAN §9.5.4.1).

Bootstrap writes one file per failing concern under
``<workdir>/.aidp/diagnostics/<run_id>/`` when mechanical resolution
cannot proceed. Feature #3 (``v2-phase-3b-medallion-author-skill``)
consumes these files to draft overlays; other future tools (custom
recovery scripts, Web UIs, alternate LLMs) can consume the same
contract because Pydantic models + a documented schema version are
the public surface.

Path-naming uses a per-failure discriminator so a single bootstrap run
can produce multiple no-match artifacts without collision:

```
.aidp/diagnostics/<run_id>/
  AIDPF-1020.json                     # identity gate (one per run)
  AIDPF-2010__<vp-name>.json          # one per failing columnAlias
  AIDPF-2011__<vp-name>.json          # one per failing semanticVariant
```

Bootstrap collects ALL failures across the walk loop before exiting
(no early-exit on first failure); skill reads the whole directory to
assemble full recovery context.

**Out of scope**: ``AIDPF-2012`` / ``SchemaDriftFailure``. Runtime
preflight (feature #4) is the only emitter of 2012, and it owns its
own diagnostic-artifact model in that feature's PR. Bootstrap's
``--refresh`` resolves drift, emitting 2010 / 2011 only when re-walk
fails.

Schema-version forward-compatibility per PLAN §9.5.8: consumers ignore
unknown top-level fields; a future schemaVersion=2 model adds fields
without breaking v1 consumers.
"""

from __future__ import annotations

import errno
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Error codes (PLAN §25)
# ---------------------------------------------------------------------------

AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED = "AIDPF-1020"
"""Operator identity cannot be resolved from --operator / AIDP_OPERATOR / $USER."""

AIDPF_2010_COLUMN_ALIAS_UNRESOLVED = "AIDPF-2010"
"""``required: true`` ``columnAliases.<name>`` has no matching candidate on the tenant's bronze."""

AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED = "AIDPF-2011"
"""``required: true`` ``semanticVariants.<name>`` has no matching detect clause on the tenant's bronze."""

AIDPF_2012_SCHEMA_DRIFT_DETECTED = "AIDPF-2012"
"""Bronze schema fingerprint drift detected at runtime preflight (Phase 3c).
Live bronze fingerprint differs from the value pinned in the tenant profile;
the run blocks until the operator runs ``aidp-fusion-bundle bootstrap --refresh``."""


# ---------------------------------------------------------------------------
# Failure payload sub-models
# ---------------------------------------------------------------------------


class CandidateProbeOutcome(BaseModel):
    """Per-candidate probe result captured during a walker no-match.

    Skill (feature #3) reads each outcome to understand WHY the candidate
    failed — was the column simply absent, or did its detect-clause fail
    for a semantic variant?
    """

    model_config = ConfigDict(extra="forbid")

    candidate: str
    """The candidate's logical id (column name for columnAliases; candidate
    id like ``cancelled_date`` for semanticVariants)."""

    outcome: Literal["column_not_found", "detect_clause_failed"]
    """Why this candidate was rejected. ``column_not_found`` is the
    columnAlias case (physical column doesn't exist). ``detect_clause_failed``
    is the semanticVariant case where the detect clause's required column
    is absent."""

    detail: str | None = None
    """Human-readable extension — e.g. ``"detect.columnExists=ApInvoicesCancelledFlag"``
    when the failing candidate was a semantic variant."""


class ObservedColumn(BaseModel):
    """One column observed on the tenant's bronze schema."""

    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    nullable: bool = True


class VariationPointFailure(BaseModel):
    """The structured failure context for ``AIDPF-2010`` / ``AIDPF-2011``.

    Skill reads this to author an overlay extending the candidate list,
    or to surface the failure to the operator in human-readable form.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    """Variation-point name (e.g. ``invoice_currency_code``)."""

    kind: Literal["columnAliases", "semanticVariants"]
    """Which variation-point family this name belongs to."""

    applies_to: str = Field(alias="appliesTo")
    """The bronze table the variation point targets (e.g. ``bronze.ap_invoices``)."""

    candidates_tried: list[CandidateProbeOutcome] = Field(alias="candidatesTried")
    """Per-candidate walker result, in priority order."""

    observed_bronze_schema: list[ObservedColumn] = Field(alias="observedBronzeSchema")
    """Columns present in the tenant's bronze table at probe time. Skill uses
    these to suggest a candidate to add to an overlay."""

    prior_pinned: str | None = Field(default=None, alias="priorPinned")
    """Value from the prior profile when running ``--refresh``; ``None`` on
    initial onboarding."""


class IdentityProbeFailure(BaseModel):
    """Structured failure context for ``AIDPF-1020``.

    Records what env-var lookups bootstrap probed; skill can advise the
    operator which one to set.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    probed_sources: list[str] = Field(alias="probedSources")
    """Identity sources probed in §9.5.9 precedence order
    (``"--operator"``, ``"AIDP_OPERATOR"``, ``"USER"``)."""

    non_empty_sources: list[str] = Field(alias="nonEmptySources", default_factory=list)
    """Subset of ``probed_sources`` that were set to a non-empty / non-whitespace
    value but were still rejected (currently always empty — bootstrap accepts
    any non-empty value; reserved for future stricter validation)."""


# ---------------------------------------------------------------------------
# Artifact models
# ---------------------------------------------------------------------------


class DiagnosticArtifactBase(BaseModel):
    """Shared header for every diagnostic artifact."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_version: Literal[1] = Field(default=1, alias="schemaVersion")
    """Bootstrap-diagnostic schema version. Bumped on breaking changes."""

    run_id: str = Field(alias="runId")
    """Bootstrap-run identifier; matches the
    ``.aidp/diagnostics/<run_id>/`` directory."""

    tenant: str | None
    """Tenant identifier. ``None`` only on ``AIDPF-1020`` — identity gate
    fires before tenant context is loaded."""

    error_code: str = Field(alias="errorCode")
    """One of ``AIDPF-1020`` / ``AIDPF-2010`` / ``AIDPF-2011``."""

    error_message: str = Field(alias="errorMessage")
    """Human-readable explanation of the failure."""

    generated_at: datetime = Field(alias="generatedAt")
    """UTC timestamp of artifact creation."""


class VariationPointDiagnosticV1(DiagnosticArtifactBase):
    """Diagnostic artifact for one unresolved variation point."""

    error_code: Literal["AIDPF-2010", "AIDPF-2011"] = Field(alias="errorCode")
    variation_point: VariationPointFailure = Field(alias="variationPoint")


class IdentityDiagnosticV1(DiagnosticArtifactBase):
    """Diagnostic artifact for an unresolved operator identity (AIDPF-1020)."""

    error_code: Literal["AIDPF-1020"] = Field(alias="errorCode")
    tenant: None = None
    identity_probe: IdentityProbeFailure = Field(alias="identityProbe")


# ---------------------------------------------------------------------------
# Phase 3c — schema-drift artifact (AIDPF-2012)
# ---------------------------------------------------------------------------


class ColumnTypeChange(BaseModel):
    """One observed column whose type changed since pin time.

    Phase 3c v0.3 does NOT populate this — it requires a separate
    pinned-schema snapshot file (deferred to
    ``v2-phase-3d-pinned-schema-diff``). The model ships in v0.3 so
    feature #3d can extend the artifact without a schemaVersion bump.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    prior_type: str = Field(alias="priorType")
    current_type: str = Field(alias="currentType")


class DatasetSchemaDelta(BaseModel):
    """Per-dataset description of what changed since pin time.

    All three lists are optional / default-empty in Phase 3c v0.3 —
    only ``affectedVariationPoints`` is computable from the pinned
    profile + live observation. ``datasetDeltas`` lands when the
    follow-up pinned-schema-snapshot feature ships.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    dataset_id: str = Field(alias="datasetId")
    added_columns: list[ObservedColumn] = Field(
        default_factory=list, alias="addedColumns"
    )
    removed_columns: list[ObservedColumn] = Field(
        default_factory=list, alias="removedColumns"
    )
    type_changed_columns: list[ColumnTypeChange] = Field(
        default_factory=list, alias="typeChangedColumns"
    )


class AffectedVariationPoint(BaseModel):
    """Per-pinned-VP impact summary for the drift artifact.

    Computed by diffing each ``profile.resolved.{column,semantic}.<name>``
    pinned candidate against the live observed bronze schema. ``True``
    means the pinned column still exists on bronze (drift cause is
    elsewhere); ``False`` means the pinned column was dropped /
    renamed (skill recovery target).
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str
    kind: Literal["columnAliases", "semanticVariants"]
    pinned_candidate: str = Field(alias="pinnedCandidate")
    still_exists_on_bronze: bool = Field(alias="stillExistsOnBronze")


class SchemaDriftFailure(BaseModel):
    """Structured drift-context payload for the AIDPF-2012 artifact."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    prior_fingerprint: str = Field(alias="priorFingerprint")
    current_fingerprint: str = Field(alias="currentFingerprint")
    pinned_at: datetime = Field(alias="pinnedAt")
    """When bootstrap pinned the prior fingerprint (from the profile)."""

    dataset_deltas: list[DatasetSchemaDelta] = Field(
        default_factory=list, alias="datasetDeltas"
    )
    """Per-dataset column-level deltas. Empty in Phase 3c v0.3 — the
    fingerprint hash is one-way; reconstructing per-column diff needs a
    separate pinned-schema snapshot deferred to
    ``v2-phase-3d-pinned-schema-diff``."""

    affected_variation_points: list[AffectedVariationPoint] = Field(
        default_factory=list, alias="affectedVariationPoints"
    )
    """Per-pinned-VP impact. Computable from pinned profile + live
    observation; populated unconditionally on drift. Skill (Phase 3b)
    consumes this to decide which VPs need re-resolution."""


class SchemaDriftDiagnosticV1(DiagnosticArtifactBase):
    """Diagnostic artifact for AIDPF-2012 schema-fingerprint drift
    (Phase 3c).

    One file per drifted run at
    ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2012.json`` — no
    discriminator (one fingerprint per run, not per-VP).
    """

    error_code: Literal["AIDPF-2012"] = Field(alias="errorCode")
    tenant: str
    """Drift always happens in a known-tenant context (unlike 1020
    where tenant is unknown at gate-fire time). Override the base's
    ``str | None`` to require a value."""

    schema_drift: SchemaDriftFailure = Field(alias="schemaDrift")


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


class DiagnosticArtifactAlreadyExistsError(FileExistsError):
    """Raised when two bootstrap calls reuse the same ``run_id`` and
    target the same artifact path.

    Inherits from ``FileExistsError`` so callers can also catch the
    stdlib-typed exception (e.g. broad exception handlers in test
    harnesses)."""


def _diagnostics_dir(workdir: Path, run_id: str) -> Path:
    return workdir / ".aidp" / "diagnostics" / run_id


def _atomic_write_json(path: Path, payload: str) -> None:
    """Write ``payload`` to ``path`` atomically.

    Refuses to overwrite an existing file — two bootstrap runs reusing
    the same ``run_id`` indicates a caller bug or operator error, and a
    silent overwrite would destroy the prior run's evidence.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise DiagnosticArtifactAlreadyExistsError(
            errno.EEXIST,
            f"refusing to overwrite existing diagnostic artifact",
            str(path),
        )
    # Write to a sibling temp file in the same directory so os.replace is atomic.
    fd, tmp_path = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp_path, path)
    except BaseException:
        # Best-effort cleanup of the temp file if anything went wrong.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def write_variation_diagnostic(
    workdir: Path,
    run_id: str,
    artifact: VariationPointDiagnosticV1,
) -> Path:
    """Write a variation-point diagnostic artifact.

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/<errorCode>__<vpName>.json``.
    The ``<vpName>`` discriminator ensures multiple no-match failures in
    one bootstrap run produce distinct files.

    Args:
        workdir: persistence-root anchor; bootstrap passes
            ``bundle_path.resolve().parent``.
        run_id: bootstrap-run identifier.
        artifact: the diagnostic payload.

    Returns:
        The absolute path the artifact was written to.

    Raises:
        DiagnosticArtifactAlreadyExistsError: a file already exists at
            the target path (the ``run_id``/``vpName`` combination has
            been used before).
        UnsafePathSegmentError: ``run_id`` or
            ``artifact.variation_point.name`` is not a safe filesystem
            segment, or the resolved target escapes
            ``<workdir>/.aidp/diagnostics/<run_id>/``.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    validate_path_segment(
        artifact.variation_point.name, field="variationPoint.name"
    )
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / (
        f"{artifact.error_code}__{artifact.variation_point.name}.json"
    )
    assert_within_root(target, diag_dir, field="variationPoint.name")
    payload = artifact.model_dump_json(by_alias=True, indent=2) + "\n"
    _atomic_write_json(target, payload)
    return target


def write_identity_diagnostic(
    workdir: Path,
    run_id: str,
    artifact: IdentityDiagnosticV1,
) -> Path:
    """Write an identity-gate diagnostic artifact.

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-1020.json``.
    Only one ``AIDPF-1020`` artifact per run (no discriminator); identity
    gate fires once.

    Raises:
        UnsafePathSegmentError: ``run_id`` is not a safe filesystem
            segment.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / "AIDPF-1020.json"
    assert_within_root(target, diag_dir, field="run_id")
    payload = artifact.model_dump_json(by_alias=True, indent=2) + "\n"
    _atomic_write_json(target, payload)
    return target


def write_schema_drift_diagnostic(
    workdir: Path,
    run_id: str,
    artifact: SchemaDriftDiagnosticV1,
) -> Path:
    """Write a Phase 3c schema-drift diagnostic artifact (AIDPF-2012).

    Path = ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2012.json``.
    Only one ``AIDPF-2012`` artifact per run (no discriminator); drift
    is detected once at preflight.

    Raises:
        UnsafePathSegmentError: ``run_id`` is not a safe filesystem
            segment.
    """
    from .path_segment import assert_within_root, validate_path_segment

    validate_path_segment(run_id, field="run_id")
    diag_dir = _diagnostics_dir(workdir, run_id).resolve()
    target = diag_dir / "AIDPF-2012.json"
    assert_within_root(target, diag_dir, field="run_id")
    payload = artifact.model_dump_json(by_alias=True, indent=2) + "\n"
    _atomic_write_json(target, payload)
    return target


__all__ = [
    "AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED",
    "AIDPF_2010_COLUMN_ALIAS_UNRESOLVED",
    "AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED",
    "AIDPF_2012_SCHEMA_DRIFT_DETECTED",
    "AffectedVariationPoint",
    "CandidateProbeOutcome",
    "ColumnTypeChange",
    "DatasetSchemaDelta",
    "DiagnosticArtifactAlreadyExistsError",
    "DiagnosticArtifactBase",
    "IdentityDiagnosticV1",
    "IdentityProbeFailure",
    "ObservedColumn",
    "SchemaDriftDiagnosticV1",
    "SchemaDriftFailure",
    "VariationPointDiagnosticV1",
    "VariationPointFailure",
    "write_identity_diagnostic",
    "write_schema_drift_diagnostic",
    "write_variation_diagnostic",
]
