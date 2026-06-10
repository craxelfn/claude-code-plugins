"""Overlay drafter for the medallion-author skill.

Emits:

* ``overlays/<overlay-name>/pack.yaml`` — content-pack overlay with
  ``extends:`` pointing at the starter pack and ``columnAliases`` /
  ``semanticVariants`` extended with operator-approved candidates.
  Full provenance block stamped per the Phase 3b contract.
* ``overlays/<overlay-name>/resolutions.json`` — **conditional**;
  only emitted when MultiMatch picks or refresh-AutoResolved-change
  picks need scripted operator approval at commit time. Initial
  AIDPF-2010 onboarding skips this file (would fail feature #2's
  validator).
* ``overlays/<overlay-name>/skill-evidence.json`` — the skill's own
  audit trail (model id, reasoning per proposal, cost estimates,
  operator decisions).

Per PLAN §9.5.6 #1 MAY-NOT, the drafter NEVER emits SQL templates —
``validate_overlay`` rejects any overlay that introduces a node or
override block. Per §9.5.7 #6 ("bootstrap is the only writer"),
``write_overlay`` confines all I/O to ``<workdir>/overlays/<overlay-name>/``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import yaml

from ..schema.incremental_impact import IncrementalImpact
from ..schema.medallion_pack import (
    ColumnAlias,
    PackProvenance,
    PackYaml,
    SemanticVariant,
    SemanticVariantCandidate,
    SemanticVariantDetect,
    SkillProposalRecord,
)
from ..schema.path_segment import (
    UnsafePathSegmentError,
    assert_within_root,
    validate_path_segment,
)
from ..schema.resolutions_input import ResolutionsInputV1
from . import SKILL_ID, SKILL_VERSION


PickOutcome = Literal["AutoResolved", "MultiMatch", "RefreshChange"]


@dataclass(frozen=True)
class ProposedCandidate:
    """One operator-approved candidate to add to a VP's list."""

    vp_name: str
    kind: Literal["columnAliases", "semanticVariants"]
    applies_to: str
    """``bronze.<dataset_id>`` (from the diagnostic artifact's
    ``variationPoint.appliesTo``)."""

    candidate: str
    """For columnAliases: the column name. For semanticVariants: the
    candidate ID (e.g. ``cancelled_date_short``)."""

    confidence: str | None = None
    reasoning: str | None = None

    # semanticVariants-only — required to make the overlay valid.
    detect_column: str | None = None
    """The ``detect.columnExists`` value (semanticVariants only)."""

    fragment: str | None = None
    """SQL fragment substituted at render (semanticVariants only)."""

    outcome: PickOutcome = "AutoResolved"
    """Walker outcome label — drives whether ``write_resolutions`` emits
    an entry for this pick."""

    incremental_impact: IncrementalImpact | None = None
    """Per-VP impact analysis (refresh promotions only)."""


@dataclass(frozen=True)
class OverlayDraft:
    """In-memory overlay representation prior to disk write."""

    overlay_name: str
    base_pack_id: str
    base_pack_version: str
    diagnostic_run_id: str
    model_id: str

    proposed: tuple[ProposedCandidate, ...]
    """Operator-approved proposals to write into the overlay."""

    pack_yaml: PackYaml
    """Pydantic-validated PackYaml ready to serialise."""

    skill_evidence: dict[str, Any] = field(default_factory=dict)
    """Skill's own audit-trail payload (model id, reasoning per
    proposal, cost estimates). Written separately to
    ``skill-evidence.json``."""


class OverlayValidationError(ValueError):
    """Raised when a drafted overlay fails the §9.5.6 MAY-NOT checks
    (e.g. tries to add a SQL template, hardcodes a column, missing
    provenance)."""


# ---------------------------------------------------------------------------
# draft_overlay
# ---------------------------------------------------------------------------


def draft_overlay(
    *,
    overlay_name: str,
    base_pack_id: str,
    base_pack_version: str,
    base_column_aliases: dict[str, ColumnAlias],
    base_semantic_variants: dict[str, SemanticVariant],
    proposed: list[ProposedCandidate],
    diagnostic_run_id: str,
    model_id: str,
    skill_evidence: dict[str, Any] | None = None,
) -> OverlayDraft:
    """Assemble an :class:`OverlayDraft` from operator-approved proposals.

    Each ``proposed`` candidate appends to the base pack's existing
    candidate list — overlay's ``columnAliases.<vp>.candidates`` is
    ``[<base candidates...>, <new candidate>]``. Validated via
    :func:`validate_overlay` before return; raises
    :class:`OverlayValidationError` on any §9.5.6 MAY-NOT.
    """
    validate_path_segment(overlay_name, field="overlay.name")
    validate_path_segment(diagnostic_run_id, field="overlay.diagnosticRunId")

    if not proposed:
        raise OverlayValidationError(
            "draft_overlay called with empty `proposed` list — nothing to draft."
        )

    column_aliases: dict[str, dict[str, Any]] = {}
    semantic_variants: dict[str, dict[str, Any]] = {}

    proposals_map: dict[str, SkillProposalRecord] = {}
    impact_map: dict[str, IncrementalImpact] = {}

    for p in proposed:
        # Provenance entries (per VP) for the operator-facing audit.
        proposals_map[p.vp_name] = SkillProposalRecord(
            candidateAdded=p.candidate,
            confidence=p.confidence,
            reasoning=p.reasoning,
        )
        if p.incremental_impact is not None:
            impact_map[p.vp_name] = p.incremental_impact

        if p.kind == "columnAliases":
            base = base_column_aliases.get(p.vp_name)
            inherited = list(base.candidates) if base is not None else []
            column_aliases[p.vp_name] = {
                "appliesTo": p.applies_to,
                "required": base.required if base is not None else True,
                "candidates": [*inherited, p.candidate],
            }
        else:  # semanticVariants
            if p.detect_column is None or p.fragment is None:
                raise OverlayValidationError(
                    f"semanticVariants proposal for {p.vp_name!r} missing "
                    f"detect_column or fragment; both are required."
                )
            base = base_semantic_variants.get(p.vp_name)
            inherited: list[dict[str, Any]] = []
            if base is not None:
                inherited = [c.model_dump(by_alias=True) for c in base.candidates]
            semantic_variants[p.vp_name] = {
                "appliesTo": p.applies_to,
                "required": base.required if base is not None else True,
                "candidates": [
                    *inherited,
                    {
                        "id": p.candidate,
                        "detect": {"columnExists": p.detect_column},
                        "fragment": p.fragment,
                    },
                ],
            }

    pack_data: dict[str, Any] = {
        "id": overlay_name,
        "version": "0.1.0",
        "extends": f"{base_pack_id}@{base_pack_version}",
        "compatibility": {
            "pluginMinVersion": "0.3.0",
            "fusionFamilies": ["ERP"],
            "aidp": {"requiresDelta": True},
        },
        "provenance": _build_provenance(
            diagnostic_run_id=diagnostic_run_id,
            model_id=model_id,
            proposals=proposals_map,
            incremental_impact=impact_map,
        ),
    }
    if column_aliases:
        pack_data["columnAliases"] = column_aliases
    if semantic_variants:
        pack_data["semanticVariants"] = semantic_variants

    pack_yaml = PackYaml.model_validate(pack_data)
    draft = OverlayDraft(
        overlay_name=overlay_name,
        base_pack_id=base_pack_id,
        base_pack_version=base_pack_version,
        diagnostic_run_id=diagnostic_run_id,
        model_id=model_id,
        proposed=tuple(proposed),
        pack_yaml=pack_yaml,
        skill_evidence=skill_evidence or {},
    )
    validate_overlay(draft)
    return draft


def validate_overlay(draft: OverlayDraft) -> None:
    """Enforce the §9.5.6 MAY-NOTs.

    * Overlay MUST NOT introduce any silver/gold node definitions
      (skill never authors SQL templates).
    * Overlay MUST NOT use ``overrides`` to change an existing node
      (out of scope for v0.3; would require SQL template authoring).
    * Overlay MUST carry a non-empty ``provenance.skill_id``,
      ``skill_version``, ``model_id``, ``diagnostic_run_id``.
    """
    pack = draft.pack_yaml
    if pack.overrides:
        raise OverlayValidationError(
            "Skill-authored overlay declared `overrides` — §9.5.6 #1 "
            "MAY-NOT forbids skill-authored SQL templates."
        )
    if pack.provenance is None:
        raise OverlayValidationError("Overlay missing `provenance` block.")
    missing = [
        n
        for n, v in (
            ("skill_id", pack.provenance.skill_id),
            ("skill_version", pack.provenance.skill_version),
            ("model_id", pack.provenance.model_id),
            ("diagnostic_run_id", pack.provenance.diagnostic_run_id),
        )
        if not v
    ]
    if missing:
        raise OverlayValidationError(
            f"Overlay provenance missing required fields: {missing}"
        )
    if pack.provenance.skill_id != SKILL_ID:
        raise OverlayValidationError(
            f"Overlay provenance.skill_id={pack.provenance.skill_id!r} does "
            f"not match the medallion-author skill_id ({SKILL_ID!r})."
        )


def _build_provenance(
    *,
    diagnostic_run_id: str,
    model_id: str,
    proposals: dict[str, SkillProposalRecord],
    incremental_impact: dict[str, IncrementalImpact],
) -> dict[str, Any]:
    return {
        "skillId": SKILL_ID,
        "skillVersion": SKILL_VERSION,
        "modelId": model_id,
        "generatedAt": datetime.now(tz=timezone.utc).isoformat(),
        "diagnosticRunId": diagnostic_run_id,
        "proposals": {
            name: {
                "candidateAdded": p.candidate_added,
                "confidence": p.confidence,
                "reasoning": p.reasoning,
            }
            for name, p in proposals.items()
        },
        "incrementalImpact": {
            name: i.model_dump(by_alias=True)
            for name, i in incremental_impact.items()
        }
        if incremental_impact
        else None,
    }


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def write_overlay(
    draft: OverlayDraft,
    *,
    workdir: Path,
    overwrite: bool = False,
) -> Path:
    """Write the validated overlay YAML to
    ``<workdir>/overlays/<overlay_name>/pack.yaml``.

    Args:
        draft: validated :class:`OverlayDraft`.
        workdir: persistence-root anchor; the skill passes
            ``bundle_path.resolve().parent``.
        overwrite: when ``False`` (default), refuse to write into an
            existing overlay directory (operator's prior draft must
            be reviewed / removed before overwriting).

    Returns:
        Absolute path to the written ``pack.yaml``.
    """
    workdir_resolved = workdir.resolve()
    overlays_root = workdir_resolved / "overlays"
    overlay_dir = overlays_root / draft.overlay_name
    assert_within_root(overlay_dir, overlays_root, field="overlay.name")

    if overlay_dir.exists() and not overwrite:
        raise FileExistsError(
            f"overlay directory already exists: {overlay_dir}. "
            f"Pass overwrite=True to replace it."
        )

    overlay_dir.mkdir(parents=True, exist_ok=True)
    target = overlay_dir / "pack.yaml"
    assert_within_root(target, overlay_dir, field="overlay.pack.yaml")

    payload = draft.pack_yaml.model_dump(mode="json", by_alias=True, exclude_none=True)
    rendered = yaml.safe_dump(payload, sort_keys=False, default_flow_style=False)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(rendered, encoding="utf-8")
    tmp.replace(target)
    return target


def write_resolutions(
    draft: OverlayDraft,
    *,
    workdir: Path,
    tenant: str,
) -> Path | None:
    """Conditionally write ``resolutions.json``.

    Emits ONLY for picks whose ``outcome`` is ``MultiMatch`` or
    ``RefreshChange``. Initial AutoResolved picks would fail feature
    #2's validator (Rule 7 — extraneous entry) so this function
    returns ``None`` when no such picks exist.

    Args:
        draft: the validated overlay draft.
        workdir: persistence-root anchor.
        tenant: ``bundle.contentPack.profile``. Stamped into the
            resolutions file so feature #2's validator accepts it.

    Returns:
        Absolute path to the written file, OR ``None`` if no
        scripted entries are needed.
    """
    scripted = [
        p
        for p in draft.proposed
        if p.outcome in ("MultiMatch", "RefreshChange")
    ]
    if not scripted:
        return None

    validate_path_segment(tenant, field="resolutions.tenant")

    resolutions = ResolutionsInputV1.model_validate(
        {
            "schemaVersion": 1,
            "tenant": tenant,
            "resolutions": [
                {
                    "name": p.vp_name,
                    "kind": p.kind,
                    "chosenCandidate": p.candidate,
                }
                for p in scripted
            ],
        }
    )

    workdir_resolved = workdir.resolve()
    overlays_root = workdir_resolved / "overlays"
    overlay_dir = overlays_root / draft.overlay_name
    assert_within_root(overlay_dir, overlays_root, field="overlay.name")
    target = overlay_dir / "resolutions.json"
    assert_within_root(target, overlay_dir, field="overlay.resolutions")

    overlay_dir.mkdir(parents=True, exist_ok=True)
    payload = resolutions.model_dump_json(by_alias=True, indent=2) + "\n"
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(target)
    return target


def write_skill_evidence(
    draft: OverlayDraft,
    *,
    workdir: Path,
) -> Path:
    """Write the skill's own audit trail to ``skill-evidence.json``.

    Separate from feature #2's evidence snapshot — this file captures
    the skill's reasoning + operator decisions for ops-side review;
    feature #2 only records the final pinned values + the
    `mechanism: skill_proposed` mechanism + the `skill_version` for
    correlation.
    """
    workdir_resolved = workdir.resolve()
    overlay_dir = workdir_resolved / "overlays" / draft.overlay_name
    assert_within_root(
        overlay_dir, workdir_resolved / "overlays", field="overlay.name"
    )
    target = overlay_dir / "skill-evidence.json"

    payload: dict[str, Any] = {
        "schemaVersion": 1,
        "skillId": SKILL_ID,
        "skillVersion": SKILL_VERSION,
        "modelId": draft.model_id,
        "diagnosticRunId": draft.diagnostic_run_id,
        "generatedAt": datetime.now(tz=timezone.utc).isoformat(),
        "overlayName": draft.overlay_name,
        "basedOnPack": f"{draft.base_pack_id}@{draft.base_pack_version}",
        "proposed": [
            {
                "vpName": p.vp_name,
                "kind": p.kind,
                "candidate": p.candidate,
                "confidence": p.confidence,
                "reasoning": p.reasoning,
                "outcome": p.outcome,
            }
            for p in draft.proposed
        ],
        "extras": draft.skill_evidence,
    }
    overlay_dir.mkdir(parents=True, exist_ok=True)
    import json

    rendered = json.dumps(payload, indent=2, sort_keys=False) + "\n"
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(rendered, encoding="utf-8")
    tmp.replace(target)
    return target


__all__ = [
    "OverlayDraft",
    "OverlayValidationError",
    "PickOutcome",
    "ProposedCandidate",
    "draft_overlay",
    "validate_overlay",
    "write_overlay",
    "write_resolutions",
    "write_skill_evidence",
]
