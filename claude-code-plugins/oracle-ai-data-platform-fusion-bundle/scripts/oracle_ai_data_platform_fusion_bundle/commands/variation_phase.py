"""Bootstrap's variation-resolution phase (PLAN §9.5.4 + §9.5.5).

Runs after the existing pre-onboarding probes (or in place of them when
``--skip-preonboarding-probes`` is set) iff ``bundle.content_pack`` is
non-None. v1 bundles (no ``contentPack:`` block) skip this phase
entirely; their existing ``bootstrap`` behaviour is unchanged.

Responsibilities:

1. Resolve operator identity (Step 5) or raise ``AIDPF-1020``.
2. Load the resolved pack (overlay chain included).
3. Acquire a Spark session, probe bronze once into an ``observed`` dict.
4. Compute ``bronzeSchemaFingerprint`` from the observation (Step 2).
5. For each ``columnAliases.<name>`` and ``semanticVariants.<name>``:
   walk → collect outcome. Never exit early.
6. After both loops complete:

   * If any required no-match outcome was collected → write one
     diagnostic artifact per failure (Step 3), exit non-zero.
   * Otherwise → assemble profile + evidence snapshot, write them
     (Step 4), exit 0.

``--refresh`` semantics (Step 9): re-walk-all every variation point;
no-op only when fingerprints match byte-for-byte. Never emits
``AIDPF-2012``.
"""

from __future__ import annotations

import contextlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import yaml
from rich.console import Console

from ..orchestrator.content_pack import ResolvedPack, load_full_chain, load_pack
from ..schema.bronze_fingerprint import ColumnInfo, compute_bronze_fingerprint
from ..schema.bundle import Bundle, resolve_content_pack_root
from ..schema.diagnostic_artifact import (
    AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
    AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
    AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED,
    CandidateProbeOutcome,
    IdentityDiagnosticV1,
    IdentityProbeFailure,
    ObservedColumn,
    VariationPointDiagnosticV1,
    VariationPointFailure,
    write_identity_diagnostic,
    write_variation_diagnostic,
)
from ..schema.evidence_snapshot import (
    ApprovedBy,
    CandidateConsidered,
    EvidenceContainer,
    EvidenceSnapshotV1,
    ResolvedVariationPoint,
    SnapshotEntry,
    SnapshotProvenance,
    write_evidence_snapshot,
)
from ..schema.incremental_impact import IncrementalImpact
from ..schema.path_segment import UnsafePathSegmentError, validate_path_segment
from ..schema.resolutions_input import (
    ResolutionsFileError,
    ResolutionsInputV1,
    validate_against_pack,
)
from ..schema.tenant_profile import (
    TenantProfile,
    load_tenant_profile,
    resolve_profile_path,
)
from .bronze_probe import describe_bronze
from .operator_identity import OperatorIdentityUnresolved, resolve_operator
from .resolution_prompt import PromptResult, prompt_multi_match
from .variation_resolver import (
    AutoResolved,
    CandidateAttempt,
    CandidateWalkResult,
    MultiMatch,
    NoMatch,
    walk_column_alias,
    walk_semantic_variant,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Inputs / outputs
# ---------------------------------------------------------------------------


@dataclass
class VariationPhaseOptions:
    """All knobs the CLI passes into the variation phase."""

    refresh: bool = False
    operator: str | None = None
    non_interactive: bool = False
    resolutions_path: Path | None = None
    spark_session: Any | None = None
    """Caller-provided Spark session. ``None`` → acquire local-mode session."""

    spark_factory: Callable[[], Any] | None = None
    """Test injection — replace local-Spark acquisition. The CLI never sets
    this directly; tests use it to inject a mock without monkeypatching."""

    input_fn: Callable[[str], str] | None = None
    """Test injection for the interactive y/N confirmation prompt during
    ``--refresh`` when a pinned value would change. ``None`` falls back
    to stdlib ``input()``. Tests pass a lambda to drive accept/decline."""


@dataclass
class VariationPhaseOutcome:
    """Result the CLI uses to decide its exit code."""

    exit_code: int
    """0 on success / drift-no-op; non-zero on any failure."""

    profile_path: Path | None = None
    """Set when a profile was (re-)written."""

    evidence_path: Path | None = None
    """Set when a new evidence snapshot was written."""

    diagnostic_paths: list[Path] = field(default_factory=list)
    """Diagnostic artifacts written (any combination of 1020 / 2010 / 2011)."""

    summary: str = ""
    """One-line summary for the CLI to print."""


class RefreshRequiresConfirmation(Exception):
    """Raised in ``--refresh --non-interactive`` mode when re-walk would
    change a pinned variation-point value. The §9.5.5 rule is that
    pinned values may not change silently; non-interactive runs must
    refuse and direct the operator to re-run interactively (or supply
    ``--resolutions``)."""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_variation_phase(
    bundle: Bundle,
    bundle_path: Path,
    *,
    options: VariationPhaseOptions,
    console: Console | None = None,
) -> VariationPhaseOutcome:
    """Run the variation-resolution phase end-to-end.

    Args:
        bundle: parsed bundle. Caller guarantees
            ``bundle.content_pack is not None``.
        bundle_path: path to the bundle YAML — drives ``workdir``
            (``bundle_path.resolve().parent``).
        options: CLI flag values.
        console: Rich console for logging (test injection).

    Returns:
        :class:`VariationPhaseOutcome` describing what was written + the
        exit code to return.
    """
    console = console or Console()
    if bundle.content_pack is None:
        # Defensive — caller should have guarded.
        raise ValueError("run_variation_phase requires bundle.content_pack to be set")

    workdir = bundle_path.resolve().parent
    run_id = _generate_run_id()

    # --- Path-traversal hard-fail (defence-in-depth) ---
    # The bundle's contentPack.profile is a free-form string. A malformed
    # or malicious bundle could pass `../../outside`, which after .resolve()
    # would land profiles/evidence/diagnostics OUTSIDE the bundle's
    # persistence root. Validate up-front so the failure surfaces with a
    # clear AIDPF-style message rather than as a vague write failure (or
    # silent arbitrary-write success). The writers also re-validate as
    # defence-in-depth.
    tenant_name = bundle.content_pack.profile or bundle.content_pack.name
    try:
        validate_path_segment(tenant_name, field="contentPack.profile")
    except UnsafePathSegmentError as exc:
        console.print(f"[red]{exc}[/red]")
        return VariationPhaseOutcome(
            exit_code=1, summary=f"unsafe contentPack.profile: {tenant_name!r}"
        )

    # --- Step 5: operator identity gate ---
    try:
        operator = resolve_operator(options.operator)
    except OperatorIdentityUnresolved as exc:
        artifact = IdentityDiagnosticV1(
            runId=run_id,
            tenant=None,
            errorCode=AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
            errorMessage=str(exc),
            generatedAt=_now(),
            identityProbe=IdentityProbeFailure(probedSources=exc.probed_sources),
        )
        diag_path = write_identity_diagnostic(workdir, run_id, artifact)
        console.print(
            f"[red]{AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED}: operator identity "
            f"unresolved. Diagnostic at {diag_path}.[/red]"
        )
        return VariationPhaseOutcome(
            exit_code=1,
            diagnostic_paths=[diag_path],
            summary="AIDPF-1020 operator identity unresolved",
        )

    # --- Pack load + Spark probe ---
    pack_root = resolve_content_pack_root(bundle_path, bundle.content_pack)
    pack: ResolvedPack = load_full_chain(pack_root)
    # Phase 3b: also load the unmerged entry overlay (if any) to access
    # its untouched ``provenance`` block. ``merge_overlay`` discards
    # overlay-level provenance (see content_pack.py:486), so we re-read
    # the entry root to detect skill-authored overlays.
    entry_overlay_pack = _load_entry_overlay_provenance(pack_root)

    dataset_ids = _bronze_dataset_ids(pack)
    catalog = bundle.aidp.catalog
    bronze_schema = bundle.aidp.bronze_schema

    spark = _resolve_spark(options)
    try:
        observed = describe_bronze(
            spark,
            catalog=catalog,
            bronze_schema=bronze_schema,
            dataset_ids=dataset_ids,
        )
    finally:
        _close_spark_if_owned(spark, options)

    fingerprint = compute_bronze_fingerprint(observed=observed)

    # --- --refresh drift detection (Step 9) ---
    profile_path = resolve_profile_path(bundle_path, tenant_name)
    prior_profile: TenantProfile | None = None
    if options.refresh and profile_path.exists():
        prior_profile = load_tenant_profile(profile_path)
        if prior_profile.bronze_schema_fingerprint == fingerprint:
            console.print(
                f"[green]No drift detected — fingerprint matches "
                f"{fingerprint[:24]}... — profile unchanged.[/green]"
            )
            return VariationPhaseOutcome(
                exit_code=0,
                summary="bootstrap --refresh: no drift detected",
            )

    # --- Steps 6/7: walk every variation point ---
    walker_results: dict[tuple[str, str], CandidateWalkResult] = {}
    column_alias_specs = pack.pack.column_aliases
    semantic_variant_specs = pack.pack.semantic_variants

    for name, spec in column_alias_specs.items():
        cols = _columns_for_applies_to(observed, spec.appliesTo)
        walker_results[(name, "columnAliases")] = walk_column_alias(spec, cols)
    for name, spec in semantic_variant_specs.items():
        cols = _columns_for_applies_to(observed, spec.appliesTo)
        walker_results[(name, "semanticVariants")] = walk_semantic_variant(spec, cols)

    # --- Step 8: aggregate failures, write one artifact per failing VP ---
    failure_paths: list[Path] = []
    for (name, kind), outcome in walker_results.items():
        if isinstance(outcome, NoMatch):
            required = _is_required(name, kind, pack)
            if required:
                artifact = _build_variation_artifact(
                    run_id=run_id,
                    tenant=tenant_name,
                    name=name,
                    kind=kind,
                    outcome=outcome,
                    applies_to=_applies_to_for(name, kind, pack),
                    observed=observed,
                    prior_pinned=_prior_pinned(prior_profile, name, kind),
                )
                failure_paths.append(
                    write_variation_diagnostic(workdir, run_id, artifact)
                )

    if failure_paths:
        for path in failure_paths:
            console.print(f"[red]Diagnostic written: {path}[/red]")
        return VariationPhaseOutcome(
            exit_code=1,
            diagnostic_paths=failure_paths,
            summary=f"{len(failure_paths)} variation point(s) unresolved",
        )

    # --- Build the walker-outcome maps the resolutions validator uses ---
    multi_match_outcomes: dict[tuple[str, str], list[str]] = {
        key: outcome.matched
        for key, outcome in walker_results.items()
        if isinstance(outcome, MultiMatch)
    }
    # Under --refresh, AutoResolved outcomes whose chosen value differs
    # from the prior profile's pinned value are eligible for scripted
    # acceptance via --resolutions (mechanism: cli_flag). The validator
    # accepts such entries; the chosen_candidate MUST equal the
    # walker's value (the candidate that actually exists on bronze).
    accepted_autoresolved: dict[tuple[str, str], str] = {}
    if options.refresh and prior_profile is not None:
        for (name, kind), outcome in walker_results.items():
            if not isinstance(outcome, AutoResolved):
                continue
            prior = _prior_pinned(prior_profile, name, kind)
            if prior is not None and prior != outcome.chosen:
                accepted_autoresolved[(name, kind)] = outcome.chosen

    # Single validated load — strict on unknown names / kind / duplicates,
    # permissive on AutoResolved changes under --refresh.
    scripted = _load_resolutions(
        options,
        expected_tenant=tenant_name,
        walker_outcomes=multi_match_outcomes,
        accepted_autoresolved=accepted_autoresolved,
        pack=pack,
    )

    # --- Multi-match resolution (interactive / scripted / non-interactive) ---
    picks: dict[tuple[str, str], PromptResult] = {}
    for key in sorted(multi_match_outcomes):
        if scripted is not None and key in scripted:
            picks[key] = PromptResult(
                chosen=scripted[key], mechanism="cli_flag"
            )
            continue
        result = prompt_multi_match(
            variation_point_name=key[0],
            kind=key[1],
            matched=multi_match_outcomes[key],
            non_interactive=options.non_interactive,
            console=console,
        )
        picks[key] = result

    # --- Step 9 (cont.): if --refresh changes a pinned value, prompt confirm ---
    # Per §9.5.5: no silent change to a previously-pinned value. Three
    # acceptance paths:
    #   1. ``--resolutions`` (scripted) — operator supplied an entry
    #      that names this VP; the validator above confirmed it matches
    #      either a MultiMatch outcome or an AutoResolved-change
    #      outcome. Record ``mechanism: cli_flag`` and skip the prompt.
    #   2. ``--non-interactive`` (no resolutions file) — refuses to
    #      make a silent decision; raises ``RefreshRequiresConfirmation``.
    #   3. Interactive y/N prompt — must read a real answer and abort
    #      on no/default. The prior print-only branch fell through and
    #      wrote the profile silently — that was the round-2 blocking bug.
    if options.refresh and prior_profile is not None:
        for (name, kind), outcome in walker_results.items():
            chosen = _chosen_value(outcome, picks.get((name, kind)))
            if chosen is None:
                continue  # NoMatch (optional VP) — skip silently.
            prior = _prior_pinned(prior_profile, name, kind)
            if prior is not None and prior != chosen:
                # Path 1: scripted via ``--resolutions``. The validator
                # already confirmed the entry is valid for this VP.
                if scripted is not None and (name, kind) in scripted:
                    picks[(name, kind)] = PromptResult(
                        chosen=chosen, mechanism="cli_flag"
                    )
                    continue
                # Path 2: --non-interactive without scripted approval → abort.
                if options.non_interactive:
                    raise RefreshRequiresConfirmation(
                        f"refresh would change pinned {kind}.{name} from "
                        f"{prior!r} to {chosen!r}; re-run without "
                        f"--non-interactive to confirm, or supply "
                        f"--resolutions with an entry for ({name!r}, {kind!r})."
                    )
                # Path 3: interactive y/N prompt — actually read input.
                if not _prompt_confirm_change(
                    name=name,
                    kind=kind,
                    prior=prior,
                    chosen=chosen,
                    console=console,
                    input_fn=options.input_fn or input,
                ):
                    console.print(
                        f"[yellow]Operator declined to change pinned "
                        f"{kind}.{name}; refresh aborted. Profile + evidence "
                        f"unchanged.[/yellow]"
                    )
                    return VariationPhaseOutcome(
                        exit_code=1,
                        summary=(
                            f"refresh aborted — operator declined to change "
                            f"pinned {kind}.{name}"
                        ),
                    )
                # Operator confirmed — record mechanism as terminal_prompt
                # so the evidence trail reflects the y/N decision.
                picks[(name, kind)] = PromptResult(
                    chosen=chosen, mechanism="terminal_prompt"
                )

    # --- Profile + evidence ---
    resolutions, snapshot_entry_resolutions, mechanism_record = _assemble_resolutions(
        walker_results=walker_results,
        picks=picks,
        operator=operator,
        entry_overlay_pack=entry_overlay_pack,
    )

    now = _now()
    profile = _build_profile(
        tenant=tenant_name,
        pinned_at=now,
        bronze_schema_fingerprint=fingerprint,
        resolutions=resolutions,
        operator=operator,
        mechanism=mechanism_record,
        existing_profile=prior_profile,
        run_id=run_id,
    )
    _write_profile_yaml(profile_path, profile)

    # Phase 3b: thread skill_version from the entry overlay (when
    # skill-authored) into the snapshot's top-level provenance so audit
    # tooling can correlate evidence files with the skill version that
    # produced them.
    skill_version_for_snapshot: str | None = None
    if _is_skill_authored_overlay(entry_overlay_pack):
        prov = entry_overlay_pack.pack.provenance  # type: ignore[union-attr]
        if prov is not None:
            skill_version_for_snapshot = prov.skill_version

    snapshot = EvidenceSnapshotV1(
        tenant=tenant_name,
        generatedAt=now,
        runId=run_id,
        bronzeSchemaFingerprint=fingerprint,
        provenance=SnapshotProvenance(
            approvedBy=ApprovedBy(
                operator=operator,
                timestamp=now,
                mechanism=mechanism_record,  # type: ignore[arg-type]
            ),
            skillVersion=skill_version_for_snapshot,
            evidence=EvidenceContainer(
                snapshots=[
                    SnapshotEntry(
                        snapshotId=run_id,
                        capturedAt=now,
                        resolutions=snapshot_entry_resolutions,
                    )
                ],
            ),
        ),
    )
    evidence_path = write_evidence_snapshot(workdir, snapshot)

    console.print(
        f"[green]bootstrap variation phase complete — profile "
        f"{profile_path}, evidence {evidence_path}.[/green]"
    )
    return VariationPhaseOutcome(
        exit_code=0,
        profile_path=profile_path,
        evidence_path=evidence_path,
        summary="variation phase resolved",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _generate_run_id() -> str:
    return _now().strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]


def _bronze_dataset_ids(pack: ResolvedPack) -> list[str]:
    """Extract bronze dataset ids from the pack's bronze.yaml."""
    bronze = pack.bronze_yaml or {}
    datasets = bronze.get("datasets", [])
    out: list[str] = []
    for entry in datasets:
        if isinstance(entry, dict) and "id" in entry:
            out.append(str(entry["id"]))
    return out


def _columns_for_applies_to(
    observed: dict[str, list[ColumnInfo]],
    applies_to: str,
) -> set[str]:
    """``applies_to`` is ``bronze.<dataset_id>`` → return that dataset's
    column-name set (uppercase variant preserved; walker normalises case)."""
    if "." not in applies_to:
        return set()
    _, dataset = applies_to.split(".", 1)
    return {c.name for c in observed.get(dataset, [])}


def _is_required(name: str, kind: str, pack: ResolvedPack) -> bool:
    if kind == "columnAliases":
        spec = pack.pack.column_aliases.get(name)
    else:
        spec = pack.pack.semantic_variants.get(name)
    return bool(spec and spec.required)


def _applies_to_for(name: str, kind: str, pack: ResolvedPack) -> str:
    if kind == "columnAliases":
        return pack.pack.column_aliases[name].appliesTo
    return pack.pack.semantic_variants[name].appliesTo


def _prior_pinned(
    profile: TenantProfile | None,
    name: str,
    kind: str,
) -> str | None:
    if profile is None:
        return None
    if kind == "columnAliases":
        return profile.resolved.column.get(name)
    return profile.resolved.semantic.get(name)


def _chosen_value(
    outcome: CandidateWalkResult, pick: PromptResult | None
) -> str | None:
    if isinstance(outcome, AutoResolved):
        return outcome.chosen
    if isinstance(outcome, MultiMatch):
        return pick.chosen if pick else None
    return None


def _build_variation_artifact(
    *,
    run_id: str,
    tenant: str,
    name: str,
    kind: str,
    outcome: NoMatch,
    applies_to: str,
    observed: dict[str, list[ColumnInfo]],
    prior_pinned: str | None,
) -> VariationPointDiagnosticV1:
    _, dataset = applies_to.split(".", 1)
    observed_cols = observed.get(dataset, [])
    return VariationPointDiagnosticV1(
        runId=run_id,
        tenant=tenant,
        errorCode=(
            AIDPF_2010_COLUMN_ALIAS_UNRESOLVED
            if kind == "columnAliases"
            else AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED
        ),
        errorMessage=(
            f"{kind}.{name} has no matching candidate on the tenant's bronze. "
            f"appliesTo={applies_to}"
        ),
        generatedAt=_now(),
        variationPoint=VariationPointFailure(
            name=name,
            kind=kind,  # type: ignore[arg-type]
            appliesTo=applies_to,
            candidatesTried=[
                CandidateProbeOutcome(
                    candidate=a.candidate,
                    outcome=a.outcome,  # type: ignore[arg-type]
                    detail=a.detail,
                )
                for a in outcome.candidates_tried
            ],
            observedBronzeSchema=[
                ObservedColumn(name=col.name, type=col.type, nullable=col.nullable)
                for col in observed_cols
            ],
            priorPinned=prior_pinned,
        ),
    )


def _assemble_resolutions(
    *,
    walker_results: dict[tuple[str, str], CandidateWalkResult],
    picks: dict[tuple[str, str], PromptResult],
    operator: str,
    entry_overlay_pack=None,
) -> tuple[dict[tuple[str, str], str], list[ResolvedVariationPoint], str]:
    """Turn walker outcomes + operator picks into:

    * ``resolutions``: ``{(name, kind): chosen}`` map for the profile writer.
    * ``snapshot_entries``: the per-resolution audit list for the evidence snapshot.
    * ``mechanism_record``: the strongest mechanism applied across all picks
      (used in the profile's approvedBy block).

    Phase 3b: when ``entry_overlay_pack`` is a skill-authored overlay,
    stamp ``mechanism: skill_proposed`` on resolutions whose chosen
    candidate matches the overlay's ``provenance.proposals[vp].candidate_added``,
    and copy ``provenance.incremental_impact[vp]`` into the resolved
    variation point. Without this, the initial-onboarding flow
    silently records ``auto_resolve`` even though the skill drafted
    the candidate that resolved — audit trail can't tell skill-driven
    from pack-author-driven resolutions.
    """
    skill_authored = _is_skill_authored_overlay(entry_overlay_pack)
    skill_proposals: dict[str, str] = {}
    skill_incremental_impacts: dict[str, IncrementalImpact] = {}
    if skill_authored:
        prov = entry_overlay_pack.pack.provenance  # type: ignore[union-attr]
        if prov is not None and prov.proposals:
            skill_proposals = {
                vp_name: rec.candidate_added
                for vp_name, rec in prov.proposals.items()
            }
        if prov is not None and prov.incremental_impact:
            skill_incremental_impacts = dict(prov.incremental_impact)

    resolutions: dict[tuple[str, str], str] = {}
    snapshot_resolutions: list[ResolvedVariationPoint] = []
    mechanisms: list[str] = []

    for key, outcome in sorted(walker_results.items()):
        name, kind = key
        if isinstance(outcome, AutoResolved):
            # For AutoResolved outcomes the picks map normally has no
            # entry — but the --refresh confirmation path stamps one
            # when the operator explicitly approves a pinned-value
            # change. Prefer that mechanism over the bare auto_resolve.
            chosen = outcome.chosen
            override = picks.get(key)
            if override is not None and override.chosen == chosen:
                mechanism = override.mechanism
            elif skill_authored and skill_proposals.get(name) == chosen:
                # Phase 3b: AutoResolved on a skill-proposed candidate —
                # record skill_proposed instead of bare auto_resolve so
                # the audit trail attributes the resolution to the skill.
                mechanism = "skill_proposed"
            else:
                mechanism = "auto_resolve"
            considered = [
                CandidateConsidered(candidate=chosen, outcome="matched")
            ]
        elif isinstance(outcome, MultiMatch):
            pick = picks[key]
            chosen = pick.chosen
            mechanism = pick.mechanism
            # Phase 3b: cli_flag picks driven by a skill-authored overlay
            # become skill_proposed.
            if skill_authored and mechanism == "cli_flag" and name in skill_proposals:
                mechanism = "skill_proposed"
            considered = [
                CandidateConsidered(candidate=c, outcome="matched")
                for c in outcome.matched
            ]
        else:  # NoMatch — only reached for required=False
            continue

        resolutions[key] = chosen
        mechanisms.append(mechanism)
        snapshot_resolutions.append(
            ResolvedVariationPoint(
                name=name,
                kind=kind,  # type: ignore[arg-type]
                chosenCandidate=chosen,
                candidatesConsidered=considered,
                incrementalImpact=skill_incremental_impacts.get(name),
            )
        )

    # Record the profile-level mechanism per §9.5.9 audit-floor semantics:
    #   1. ``auto_resolve`` is the baseline — any operator-touched
    #      mechanism takes precedence (an operator-touched profile
    #      should NOT look identical to an all-auto profile in the
    #      audit trail).
    #   2. Among operator-touched mechanisms, the WEAKEST wins — a
    #      single ``non_interactive`` choice taints the whole profile.
    # Order (weakest → strongest among operator-touched):
    #   non_interactive < cli_flag < skill_proposed < terminal_prompt.
    operator_touched = [m for m in mechanisms if m != "auto_resolve"]
    if not operator_touched:
        mechanism_record = "auto_resolve"
        # Phase 3b: if all resolutions are auto_resolve but ANY came
        # via a skill-proposed candidate, the run is skill-driven.
        if skill_authored and "skill_proposed" in mechanisms:
            mechanism_record = "skill_proposed"
    else:
        precedence_among_operator = [
            "non_interactive",
            "cli_flag",
            "skill_proposed",
            "terminal_prompt",
        ]
        mechanism_record = operator_touched[0]
        for m in precedence_among_operator:
            if m in operator_touched:
                mechanism_record = m
                break

    return resolutions, snapshot_resolutions, mechanism_record


def _load_entry_overlay_provenance(pack_root: Path):
    """Re-load the entry overlay pack (unmerged) to access its untouched
    ``provenance`` block.

    Phase 3b: ``merge_overlay`` discards overlay-level provenance (see
    ``orchestrator/content_pack.py:486``), so the merged pack returned
    by ``load_full_chain`` always reflects the BASE's provenance. To
    detect skill-authored overlays we need to re-read the entry root
    via ``load_pack`` (which performs no overlay merging).

    Returns ``None`` when the entry root does not declare ``extends:``
    (i.e. the bundle points directly at a base pack — no overlay layer,
    nothing to thread).
    """
    try:
        entry = load_pack(pack_root)
    except Exception:  # noqa: BLE001 — defensive: any load failure → skip
        return None
    if entry.pack.extends is None:
        # The entry root IS the base pack (no overlay layer).
        return None
    return entry


def _is_skill_authored_overlay(entry_overlay_pack) -> bool:
    """Return True iff the entry overlay's provenance carries the
    medallion-author skill_id."""
    if entry_overlay_pack is None:
        return False
    prov = getattr(entry_overlay_pack.pack, "provenance", None)
    if prov is None:
        return False
    return getattr(prov, "skill_id", None) == "aidp-fusion-medallion-author"


def _build_profile(
    *,
    tenant: str,
    pinned_at: datetime,
    bronze_schema_fingerprint: str,
    resolutions: dict[tuple[str, str], str],
    operator: str,
    mechanism: str,
    existing_profile: TenantProfile | None,
    run_id: str,
) -> TenantProfile:
    column_map: dict[str, str] = {}
    semantic_map: dict[str, str] = {}
    for (name, kind), value in resolutions.items():
        if kind == "columnAliases":
            column_map[name] = value
        else:
            semantic_map[name] = value

    # Preserve the existing profile's free-form `profile:` block on refresh.
    free_form: dict[str, Any] = {}
    if existing_profile is not None:
        free_form = dict(existing_profile.profile)

    return TenantProfile(
        schemaVersion=1,
        tenant=tenant,
        pinnedAt=pinned_at,
        bronzeSchemaFingerprint=bronze_schema_fingerprint,
        resolved={  # type: ignore[arg-type]
            "column": column_map,
            "semantic": semantic_map,
        },
        profile=free_form,
        provenance={
            "approvedBy": {
                "operator": operator,
                "timestamp": pinned_at.isoformat(),
                "mechanism": mechanism,
            },
            "runId": run_id,
        },
    )


def _write_profile_yaml(path: Path, profile: TenantProfile) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = profile.model_dump(mode="json", by_alias=True, exclude_none=True)
    rendered = yaml.safe_dump(payload, sort_keys=False, default_flow_style=False)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(rendered, encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Spark session management
# ---------------------------------------------------------------------------


def _resolve_spark(options: VariationPhaseOptions):
    if options.spark_session is not None:
        return options.spark_session
    if options.spark_factory is not None:
        return options.spark_factory()
    return _acquire_local_spark()


def _acquire_local_spark():  # pragma: no cover — exercised in integration tests
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "bootstrap variation phase requires PySpark. Install with "
            "`pip install pyspark` or pass --skip-bootstrap if v1-only."
        ) from exc
    return (
        SparkSession.builder.master("local[1]")
        .appName("aidp-fusion-bundle-bootstrap")
        .getOrCreate()
    )


def _close_spark_if_owned(
    spark, options: VariationPhaseOptions
) -> None:
    if options.spark_session is None and options.spark_factory is None:
        # We acquired it ourselves — best-effort close.
        with contextlib.suppress(Exception):
            spark.stop()


# ---------------------------------------------------------------------------
# Resolutions-file loading
# ---------------------------------------------------------------------------


def _load_resolutions(
    options: VariationPhaseOptions,
    *,
    expected_tenant: str,
    walker_outcomes: dict[tuple[str, str], list[str]],
    accepted_autoresolved: dict[tuple[str, str], str],
    pack: ResolvedPack,
) -> dict[tuple[str, str], str] | None:
    """Parse + validate the ``--resolutions`` file if provided.

    Returns ``{(name, kind): chosen}`` covering BOTH the multi-match
    picks loop AND the refresh-change acceptance path. The validator
    runs once with the full set of permitted entries — entries for
    multi-matches, plus (when ``--refresh`` is in play) entries for
    AutoResolved outcomes whose value differs from the prior profile.

    ``None`` when the flag was not supplied.
    """
    if options.resolutions_path is None:
        return None

    with options.resolutions_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    input_data = ResolutionsInputV1.model_validate(raw)

    validate_against_pack(
        input_data=input_data,
        expected_tenant=expected_tenant,
        column_alias_names=set(pack.pack.column_aliases.keys()),
        semantic_variant_names=set(pack.pack.semantic_variants.keys()),
        walker_outcomes=walker_outcomes,
        accepted_autoresolved=accepted_autoresolved,
    )

    return {
        (entry.name, entry.kind): entry.chosen_candidate
        for entry in input_data.resolutions
    }


def _prompt_confirm_change(
    *,
    name: str,
    kind: str,
    prior: str,
    chosen: str,
    console: Console,
    input_fn: Callable[[str], str],
) -> bool:
    """Prompt the operator to confirm a pinned-value change during
    ``--refresh``. Default is **no** — operator must explicitly type
    ``y`` / ``yes`` to accept.

    Returns ``True`` on accept, ``False`` on decline / default.
    """
    console.print(
        f"[yellow]Variation {name!r} would change from "
        f"{prior!r} → {chosen!r}.[/yellow]"
    )
    raw = input_fn(f"Confirm change to {kind}.{name}? (y/N): ").strip().lower()
    return raw in ("y", "yes")


__all__ = [
    "RefreshRequiresConfirmation",
    "VariationPhaseOptions",
    "VariationPhaseOutcome",
    "run_variation_phase",
]
