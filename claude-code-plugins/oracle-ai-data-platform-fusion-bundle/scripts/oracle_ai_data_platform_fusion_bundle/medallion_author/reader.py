"""Diagnostic-artifact reader (Phase 3b skill).

Reads the artifact files feature #2's bootstrap writes under
``<workdir>/.aidp/diagnostics/<run_id>/`` and surfaces them as parsed
Pydantic models the skill can reason over.

Refuse-to-proceed gates per PLAN §9.5.8:

* ``AIDPF-1020.json`` present → identity gate failed; overlay drafting
  is the wrong response.
* Unknown ``schemaVersion`` → forward-compat rule says readers ignore
  unknown fields but MUST refuse on unknown major versions.
* Diagnostics directory missing / empty → nothing to draft.

The reader does NOT modify the artifacts; it's read-only.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from ..schema.diagnostic_artifact import (
    AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
    AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
    AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED,
    IdentityDiagnosticV1,
    VariationPointDiagnosticV1,
)

SUPPORTED_SCHEMA_VERSIONS: tuple[int, ...] = (1,)
"""Major schemaVersions the reader can parse. Bumped when the artifact
contract changes incompatibly (skill version bumps in lockstep)."""


@dataclass(frozen=True)
class DiagnosticReadResult:
    """Result of scanning a single ``<run_id>`` diagnostics directory.

    The skill reads this dataclass to decide:

    * Should we refuse? (identity gate, unknown version, empty dir)
    * If proceeding: which variation-point failures to draft for?
    """

    run_id: str
    """Bootstrap-run id parsed from the directory name."""

    run_dir: Path
    """Absolute path to ``<workdir>/.aidp/diagnostics/<run_id>/``."""

    variation_failures: list[VariationPointDiagnosticV1] = field(default_factory=list)
    """One entry per ``AIDPF-2010__<vp>.json`` or
    ``AIDPF-2011__<vp>.json`` found in the directory."""

    identity_failure: IdentityDiagnosticV1 | None = None
    """Set if ``AIDPF-1020.json`` is present. Skill refuses to draft
    when this is non-None — identity gate must be fixed first."""

    unknown_schema_paths: list[Path] = field(default_factory=list)
    """Artifact files whose ``schemaVersion`` is not in
    :data:`SUPPORTED_SCHEMA_VERSIONS`. Skill refuses to draft when
    non-empty per §9.5.8 forward-compat rule on major versions."""

    malformed_paths: list[Path] = field(default_factory=list)
    """Artifact files that failed JSON parse or Pydantic validation.
    Surface to the operator with the path so they can inspect."""

    @property
    def has_identity_failure(self) -> bool:
        """``AIDPF-1020`` present → refuse to draft."""
        return self.identity_failure is not None

    @property
    def has_unknown_schema_version(self) -> bool:
        """Any artifact at an unsupported schemaVersion → refuse."""
        return bool(self.unknown_schema_paths)

    @property
    def has_malformed_artifacts(self) -> bool:
        return bool(self.malformed_paths)

    @property
    def is_empty(self) -> bool:
        """No variation-point failures + no identity failure → nothing
        to draft (the operator may have pointed at the wrong run_id, or
        feature #2 exited cleanly on this run)."""
        return (
            not self.variation_failures
            and self.identity_failure is None
            and not self.unknown_schema_paths
            and not self.malformed_paths
        )

    def can_proceed(self) -> bool:
        """``True`` iff the skill should proceed to the propose phase."""
        return (
            bool(self.variation_failures)
            and not self.has_identity_failure
            and not self.has_unknown_schema_version
            and not self.has_malformed_artifacts
        )


def read_run(
    diagnostics_root: Path,
    run_id: str | None = None,
) -> DiagnosticReadResult:
    """Scan a single bootstrap-run's diagnostics directory.

    Args:
        diagnostics_root: ``<workdir>/.aidp/diagnostics/``. The skill
            resolves ``workdir`` from the bundle path before calling.
        run_id: bootstrap-run id (matches the directory name). If
            ``None``, the reader auto-discovers the most-recent run
            (lexicographic max of subdirectory names — feature #2's
            run-ids are ISO-prefixed so this is chronological).

    Returns:
        :class:`DiagnosticReadResult`. Always returns — the gates
        surface as fields, not exceptions, so the skill can present a
        coherent refusal message to the operator instead of crashing.
    """
    if not diagnostics_root.is_dir():
        return DiagnosticReadResult(
            run_id=run_id or "",
            run_dir=diagnostics_root / (run_id or ""),
        )

    resolved_run_id, run_dir = _resolve_run_dir(diagnostics_root, run_id)
    if run_dir is None or not run_dir.is_dir():
        return DiagnosticReadResult(run_id=resolved_run_id, run_dir=diagnostics_root)

    variation_failures: list[VariationPointDiagnosticV1] = []
    identity_failure: IdentityDiagnosticV1 | None = None
    unknown_schema_paths: list[Path] = []
    malformed_paths: list[Path] = []

    for artifact_path in sorted(run_dir.glob("*.json")):
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            malformed_paths.append(artifact_path)
            continue

        schema_version = payload.get("schemaVersion")
        if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
            unknown_schema_paths.append(artifact_path)
            continue

        error_code = payload.get("errorCode")
        try:
            if error_code == AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED:
                identity_failure = IdentityDiagnosticV1.model_validate(payload)
            elif error_code in (
                AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
                AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED,
            ):
                variation_failures.append(
                    VariationPointDiagnosticV1.model_validate(payload)
                )
            else:
                malformed_paths.append(artifact_path)
        except Exception:  # noqa: BLE001 — Pydantic raises a variety of types
            malformed_paths.append(artifact_path)

    return DiagnosticReadResult(
        run_id=resolved_run_id,
        run_dir=run_dir,
        variation_failures=variation_failures,
        identity_failure=identity_failure,
        unknown_schema_paths=unknown_schema_paths,
        malformed_paths=malformed_paths,
    )


def _resolve_run_dir(
    diagnostics_root: Path,
    run_id: str | None,
) -> tuple[str, Path | None]:
    """Pick the target run directory.

    With an explicit ``run_id``, point at that directory (don't probe
    other runs — operator's intent is specific). With ``None``,
    auto-discover the latest run: feature #2's run-ids are formatted
    ``YYYYMMDDTHHMMSSZ-<uuid8>`` so lexicographic max == chronological
    latest.
    """
    if run_id is not None:
        return run_id, diagnostics_root / run_id

    candidates = [p for p in diagnostics_root.iterdir() if p.is_dir()]
    if not candidates:
        return "", None
    latest = max(candidates, key=lambda p: p.name)
    return latest.name, latest


__all__ = [
    "SUPPORTED_SCHEMA_VERSIONS",
    "DiagnosticReadResult",
    "read_run",
]
