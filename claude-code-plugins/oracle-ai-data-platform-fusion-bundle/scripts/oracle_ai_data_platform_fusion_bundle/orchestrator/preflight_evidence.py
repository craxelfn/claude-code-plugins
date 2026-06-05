"""Phase 3c runtime preflight — bronze schema fingerprint drift detection.

Runs at the start of every ``aidp-fusion-bundle run --mode
incremental`` under ``--execution-backend content-pack``, AFTER
``_run_content_pack_backend`` mints Spark + ``run_id`` but BEFORE
``state.ensure_state_table`` and any state-row write.

Compares the live bronze schema fingerprint against
``profile.bronze_schema_fingerprint`` (pinned at bootstrap by
Phase 3a). On mismatch:

* Writes ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2012.json``
  with the structured drift context (``SchemaDriftFailure`` + per-
  pinned-VP ``affectedVariationPoints``).
* RETURNS ``PreflightOutcome(kind="drift", ...)``. The caller
  (``_run_content_pack_backend``) is the ONLY place that raises
  :class:`schema.errors.SchemaDriftDetectedError` so the CLI's
  catch arm can map to exit 14.

Skip cases (per PLAN §11.6 / round-1 + round-3 findings):

* ``--mode seed`` → seed is the new baseline; skip.
* ``--force-fingerprint-skip`` → probe + record both fingerprints
  in the outcome (caller writes audit row via
  :func:`state.write_fingerprint_skip_row`); skip comparison.
* Legacy / placeholder fingerprint → WARN log once; skip.

The probe is the same ``commands.bronze_probe.describe_bronze``
feature #2 ships; the fingerprint is the same
``schema.bronze_fingerprint.compute_bronze_fingerprint`` — single
source of truth across bootstrap (Phase 3a) and this preflight.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from ..commands.bronze_probe import describe_bronze
from ..schema.bronze_fingerprint import compute_bronze_fingerprint
from ..schema.diagnostic_artifact import (
    AIDPF_2012_SCHEMA_DRIFT_DETECTED,
    AffectedVariationPoint,
    SchemaDriftDiagnosticV1,
    SchemaDriftFailure,
    write_schema_drift_diagnostic,
)

if TYPE_CHECKING:  # pragma: no cover — type-only
    from pyspark.sql import SparkSession

    from ..orchestrator.content_pack import ResolvedPack
    from ..schema.bundle import Bundle
    from ..schema.tenant_profile import TenantProfile


logger = logging.getLogger(__name__)


# A well-formed pinned fingerprint per
# ``schema.bronze_fingerprint.compute_bronze_fingerprint``:
# "sha256:<64-hex>". Anything else (legacy/placeholder sentinels
# like ``sha256:placeholder-finance-default-2026-06-05``) → legacy
# graceful-degrade path.
_VALID_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

_LEGACY_WARN_EMITTED = False
"""Module-level flag so the legacy-profile WARN log fires once per
process. Reset by tests via :func:`_reset_legacy_warn` if needed."""


PreflightKind = Literal[
    "match", "drift", "skip_seed", "skip_legacy_profile", "skip_force_flag"
]


@dataclass(frozen=True)
class PreflightOutcome:
    """Result of :func:`check_bronze_fingerprint_drift`.

    Caller (``_run_content_pack_backend``) inspects ``kind`` to
    decide what to do. The helper itself NEVER raises
    :class:`schema.errors.SchemaDriftDetectedError` — that's the
    CLI-mapping boundary, raised by the caller from the outcome's
    fields.
    """

    kind: PreflightKind
    prior_fingerprint: str | None = None
    current_fingerprint: str | None = None
    diagnostic_path: Path | None = None
    """Set when ``kind == "drift"`` — path to the written
    ``AIDPF-2012.json``."""

    summary: str = ""
    """Human-readable hand-off message; used for stderr printing
    on drift. Empty for non-drift outcomes."""


def check_bronze_fingerprint_drift(
    *,
    spark: "SparkSession",
    bundle: "Bundle",
    bundle_path: Path,
    pack: "ResolvedPack",
    profile: "TenantProfile",
    run_id: str,
    mode: str,
    workdir: Path,
    force_skip: bool = False,
) -> PreflightOutcome:
    """Probe live bronze, compute fingerprint, compare to pinned.

    Args:
        spark: active Spark session (caller owns; preflight does NOT
            create one).
        bundle: loaded ``Bundle`` (carries ``aidp.catalog`` +
            ``aidp.bronze_schema`` for the probe).
        bundle_path: path to ``bundle.yaml`` (unused today but
            threaded for forward-compat with future per-tenant
            checks).
        pack: resolved content pack — the bronze dataset list comes
            from ``pack.bronze_yaml["datasets"]`` (the SAME source
            bootstrap uses; round-1 finding pinned this).
        profile: loaded ``TenantProfile`` — read
            ``bronze_schema_fingerprint`` + ``resolved.*`` for the
            affected-VP diff.
        run_id: the SAME run_id ``_run_content_pack_backend``
            minted. Threads through to the diagnostic artifact path
            so the run, the drift artifact, and any force-skip
            audit row all correlate.
        mode: ``"seed"`` | ``"incremental"``.
        workdir: persistence root (``bundle_path.resolve().parent``).
        force_skip: ``--force-fingerprint-skip`` operator flag.

    Returns:
        :class:`PreflightOutcome`. Never raises drift-typed
        exceptions; caller is responsible for that.
    """
    # ─── Skip: --mode seed ─────────────────────────────────────────
    if mode == "seed":
        logger.debug("Phase 3c drift gate skipped in seed mode")
        return PreflightOutcome(kind="skip_seed")

    # ─── Skip: legacy / placeholder fingerprint ────────────────────
    prior_fingerprint = profile.bronze_schema_fingerprint
    if _is_legacy_fingerprint(prior_fingerprint):
        _emit_legacy_warn_once()
        return PreflightOutcome(kind="skip_legacy_profile")

    # ─── Probe + compute current fingerprint (UNCONDITIONAL) ───────
    # Even under --force-fingerprint-skip we run the probe so the
    # outcome carries a real `current_fingerprint` for the audit row
    # (round-1 finding — earlier draft returned skip_force_flag
    # BEFORE probing, leaving the audit row's `current` undefined).
    dataset_ids = _bronze_dataset_ids(pack)
    observed = describe_bronze(
        spark,
        catalog=bundle.aidp.catalog,
        bronze_schema=bundle.aidp.bronze_schema,
        dataset_ids=dataset_ids,
    )
    current_fingerprint = compute_bronze_fingerprint(observed=observed)

    # ─── Skip: --force-fingerprint-skip ────────────────────────────
    if force_skip:
        return PreflightOutcome(
            kind="skip_force_flag",
            prior_fingerprint=prior_fingerprint,
            current_fingerprint=current_fingerprint,
            summary="--force-fingerprint-skip bypassed comparison",
        )

    # ─── Compare ───────────────────────────────────────────────────
    if prior_fingerprint == current_fingerprint:
        return PreflightOutcome(
            kind="match",
            prior_fingerprint=prior_fingerprint,
            current_fingerprint=current_fingerprint,
        )

    # ─── Drift: write artifact + return outcome ────────────────────
    affected_vps = _compute_affected_variation_points(
        profile=profile, observed=observed
    )
    artifact = SchemaDriftDiagnosticV1(
        runId=run_id,
        tenant=profile.tenant,
        errorCode=AIDPF_2012_SCHEMA_DRIFT_DETECTED,
        errorMessage=(
            "Live bronze fingerprint differs from the value pinned in the "
            "tenant profile. Re-run `aidp-fusion-bundle bootstrap --refresh` "
            "to repin."
        ),
        generatedAt=datetime.now(tz=timezone.utc),
        schemaDrift=SchemaDriftFailure(
            priorFingerprint=prior_fingerprint,  # type: ignore[arg-type]
            currentFingerprint=current_fingerprint,
            pinnedAt=profile.pinned_at,
            datasetDeltas=[],  # v0.3 — see feature #3d for per-dataset diffs
            affectedVariationPoints=affected_vps,
        ),
    )
    diagnostic_path = write_schema_drift_diagnostic(workdir, run_id, artifact)
    summary = _build_handoff_message(
        run_id=run_id,
        prior=prior_fingerprint,  # type: ignore[arg-type]
        current=current_fingerprint,
        affected_vps=affected_vps,
        diagnostic_path=diagnostic_path,
    )
    return PreflightOutcome(
        kind="drift",
        prior_fingerprint=prior_fingerprint,
        current_fingerprint=current_fingerprint,
        diagnostic_path=diagnostic_path,
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_legacy_fingerprint(value: str | None) -> bool:
    """Detect a missing / placeholder / malformed pinned fingerprint.

    Three flavors all map to "no real pin" (legacy graceful degrade):

    1. ``None`` — the ``TenantProfile`` field is Optional post-Phase-3c
       schema change; legacy profiles parse without the field.
    2. ``sha256:placeholder-...`` — the existing
       ``examples/profiles/finance-default.yaml`` placeholder shape.
    3. Anything that doesn't match ``sha256:[0-9a-f]{64}$`` — defensive
       catch-all for malformed values.
    """
    if value is None:
        return True
    return not bool(_VALID_FINGERPRINT_RE.match(value))


def _emit_legacy_warn_once() -> None:
    global _LEGACY_WARN_EMITTED
    if _LEGACY_WARN_EMITTED:
        return
    logger.warning(
        "Phase 3c drift gate skipped — tenant profile has no real "
        "bronze fingerprint pinned (legacy / pre-Phase-3a profile). "
        "Run `aidp-fusion-bundle bootstrap --refresh` to pin a real "
        "fingerprint and enable drift detection from the next run on."
    )
    _LEGACY_WARN_EMITTED = True


def _reset_legacy_warn() -> None:
    """Test helper — reset the module-level flag between cases."""
    global _LEGACY_WARN_EMITTED
    _LEGACY_WARN_EMITTED = False


def _bronze_dataset_ids(pack: "ResolvedPack") -> list[str]:
    """Extract bronze dataset ids from ``pack.bronze_yaml``.

    Mirrors ``commands.variation_phase._bronze_dataset_ids`` — the
    pin source bootstrap uses. NEVER reads ``bundle.datasets``
    (round-1 finding — bundle.datasets is a legitimate subset of
    pack bronze sources, and using it here would produce a
    different fingerprint than bootstrap pinned).
    """
    bronze = pack.bronze_yaml or {}
    datasets = bronze.get("datasets", [])
    return [
        str(entry["id"])
        for entry in datasets
        if isinstance(entry, dict) and "id" in entry
    ]


def _compute_affected_variation_points(
    *,
    profile: "TenantProfile",
    observed: dict[str, list[Any]],
) -> list[AffectedVariationPoint]:
    """For each pinned VP in ``profile.resolved.*``, check whether
    the pinned candidate column still exists anywhere in the live
    observation.

    Returns a flat list — skill (Phase 3b) reads it for recovery
    context. The check is column-existence-only (matches the
    walker's columnAlias semantics); SemanticVariant pinned values
    are ``id``s like ``cancelled_date``, NOT bronze columns —
    those can't be diff'd against bronze directly (the detect-clause
    column might still exist while a different detect-clause
    candidate now matches), so semantic VPs always surface as
    ``stillExistsOnBronze: True``. Operator + skill resolve any
    real semantic drift via re-bootstrap.
    """
    # Flatten observed columns across all datasets — the walker
    # checks per-dataset existence, but for the drift gate we only
    # need "does this column exist somewhere on bronze?"
    all_observed_columns: set[str] = set()
    for cols in observed.values():
        for col in cols:
            all_observed_columns.add(col.name.lower())

    result: list[AffectedVariationPoint] = []
    for vp_name, pinned in profile.resolved.column.items():
        result.append(
            AffectedVariationPoint(
                name=vp_name,
                kind="columnAliases",
                pinnedCandidate=pinned,
                stillExistsOnBronze=pinned.lower() in all_observed_columns,
            )
        )
    for vp_name, pinned in profile.resolved.semantic.items():
        # SemanticVariants pin a candidate ``id`` (not a column) —
        # column-existence on the id is meaningless; report True
        # so the skill can show the operator the VP didn't drop
        # from bronze but the fingerprint shifted (likely an
        # adjacent column delta).
        result.append(
            AffectedVariationPoint(
                name=vp_name,
                kind="semanticVariants",
                pinnedCandidate=pinned,
                stillExistsOnBronze=True,
            )
        )
    return result


def _build_handoff_message(
    *,
    run_id: str,
    prior: str,
    current: str,
    affected_vps: list[AffectedVariationPoint],
    diagnostic_path: Path,
) -> str:
    """Build the multi-line §9.5.5 hand-off message for stderr."""
    affected_lines = [
        f"      - {vp.name} (pinned '{vp.pinned_candidate}' "
        f"{'still exists on bronze' if vp.still_exists_on_bronze else 'NO LONGER EXISTS on bronze'})"
        for vp in affected_vps
    ]
    affected_block = (
        "\n".join(affected_lines)
        if affected_lines
        else "      (no pinned variation points to inspect)"
    )
    return (
        f"✗ AIDPF-2012  bronze schema fingerprint drift detected\n"
        f"    Prior fingerprint (pinned at bootstrap):   {prior}\n"
        f"    Current fingerprint:                       {current}\n"
        f"    Affected variation points:\n{affected_block}\n"
        f"\n"
        f"    To recover, run:\n"
        f"      aidp-fusion-bundle bootstrap --refresh\n"
        f"\n"
        f"    If --refresh cannot resolve mechanically (Tier 1), open\n"
        f"    Claude Code in this project and ask the\n"
        f"    aidp-fusion-medallion-author skill to draft an overlay.\n"
        f"\n"
        f"    Diagnostic artifact: {diagnostic_path}\n"
        f"    Documentation:       PLAN.md §9.5.5\n"
        f"    run_id:              {run_id}"
    )


__all__ = [
    "PreflightOutcome",
    "PreflightKind",
    "check_bronze_fingerprint_drift",
]
