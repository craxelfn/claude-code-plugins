"""Fast-fail preflight checks for laptop-CLI REST dispatch (P1.5ε §Step 5).

Split into two phases so the REST client is never constructed against a
malformed config:

- :func:`run_local_preflight` (Phase A) — bundle.yaml schema, dispatch-coord
  presence, OCI profile load + session-token validation. No HTTP. Runs first
  and must return PASS for every check before the client is built.
- :func:`run_remote_preflight` (Phase B) — AIDP control plane reachability,
  cluster state (with optional auto-start), BICC credential-store presence
  (P1.5ε-fix1 — added 2026-06-03 once the endpoint shape was empirically
  confirmed against ``playground``; see ``dev/RESEARCH_aidp_rest_api_probe_results.md``
  §11). Requires a constructed :class:`AidpRestClient`.
"""

from __future__ import annotations

import logging
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import oci

from ..schema.bundle import AidpConfig, EnvSpec, load_bundle
from ..schema.errors import BundleLoadError
from .rest_client import AidpRestClient, AidpRestError

logger = logging.getLogger(__name__)


PreflightStatus = Literal["PASS", "FAIL", "SKIP"]


@dataclass(frozen=True)
class PreflightResult:
    """One check's outcome. ``remediation`` is the single-line hint the
    operator sees when ``status == FAIL`` — should be copy-pasteable."""

    name: str
    status: PreflightStatus
    detail: str
    remediation: str | None = None


# ---------------------------------------------------------------------------
# Phase A — local checks (no REST client, no HTTP)
# ---------------------------------------------------------------------------


def _check_bundle_yaml(bundle_path: Path) -> PreflightResult:
    try:
        load_bundle(bundle_path)
    except BundleLoadError as e:
        return PreflightResult(
            name="bundle.yaml",
            status="FAIL",
            detail=str(e).splitlines()[0],
            remediation="run `aidp-fusion-bundle validate` for the full schema error",
        )
    return PreflightResult(
        name="bundle.yaml",
        status="PASS",
        detail=f"loaded {bundle_path}",
    )


def _check_dispatch_coords(env: EnvSpec, env_name: str) -> PreflightResult:
    missing: list[str] = []
    if not env.ai_data_platform_id:
        missing.append("aiDataPlatformId")
    if not env.cluster_key:
        missing.append("clusterKey")
    if not env.cluster_name:
        missing.append("clusterName")
    if missing:
        return PreflightResult(
            name="aidp.config.yaml dispatch coords",
            status="FAIL",
            detail=f"missing field(s) under environments.{env_name}: {', '.join(missing)}",
            remediation=(
                f"add {', '.join(missing)} under environments.{env_name} in "
                "aidp.config.yaml; see examples/aidp.config.example.yaml"
            ),
        )
    # P1.5ε scope guard — vault auth mode is rejected in this PR; tracked as
    # follow-up P1.5ε-fix6 (cloud-side signers).
    if env.auth.mode == "vault":
        return PreflightResult(
            name="aidp.config.yaml dispatch coords",
            status="FAIL",
            detail=f"environments.{env_name}.auth.mode='vault' is not supported in P1.5ε",
            remediation=(
                "set auth.mode: profile + populate ociProfile, OR wait for "
                "P1.5ε-fix6 (vault / resource-principal signer support)"
            ),
        )
    return PreflightResult(
        name="aidp.config.yaml dispatch coords",
        status="PASS",
        detail=f"all dispatch coords present for env={env_name!r}",
    )


def _check_oci_profile_and_session(env: EnvSpec) -> PreflightResult:
    profile_name = env.oci_profile or "DEFAULT"

    # 3a — config-file probe (does NOT prove a session token is valid).
    try:
        cfg = oci.config.from_file(profile_name=profile_name)
    except oci.exceptions.ConfigFileNotFound as e:
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=str(e),
            remediation="check ~/.oci/config exists",
        )
    except oci.exceptions.ProfileNotFound as e:
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=str(e),
            remediation=(
                f"add a [{profile_name}] section to ~/.oci/config, or change "
                "environments.<env>.ociProfile to a profile that exists"
            ),
        )
    except oci.exceptions.InvalidConfig as e:
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=f"invalid OCI profile {profile_name!r}: {e}",
            remediation="check ~/.oci/config — required fields missing or malformed",
        )

    # 3b — session-token validation (session-token profiles only).
    token_file = cfg.get("security_token_file")
    if not token_file:
        # API-key profile — signature is end-to-end verified by the AIDP
        # plane in Phase B check 4. Nothing to validate locally.
        return PreflightResult(
            name="OCI profile",
            status="PASS",
            detail=f"API-key profile {profile_name!r} loaded",
        )

    try:
        proc = subprocess.run(
            ["oci", "session", "validate", "--profile", profile_name],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except FileNotFoundError:
        # oci CLI not on PATH. For a session-token profile this is a hard
        # FAIL — we can't validate the token and Phase B would misclassify
        # an expired session as "AIDP plane unreachable".
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=(
                "session-token profile but `oci` CLI not on PATH; cannot "
                "validate session token locally"
            ),
            remediation=(
                "install/configure the OCI CLI "
                "(https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm), "
                "OR switch ociProfile to an API-key profile"
            ),
        )
    except subprocess.TimeoutExpired:
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=f"`oci session validate --profile {profile_name}` timed out after 5s",
            remediation=f"run `oci session validate --profile {profile_name}` interactively to investigate",
        )

    if proc.returncode != 0:
        stderr_summary = (proc.stderr or proc.stdout or "").strip().splitlines()
        detail_tail = stderr_summary[-1] if stderr_summary else "(no stderr)"
        return PreflightResult(
            name="OCI profile",
            status="FAIL",
            detail=f"session token for profile {profile_name!r} is invalid or expired: {detail_tail}",
            remediation=f"run `oci session refresh --profile {profile_name}`",
        )

    return PreflightResult(
        name="OCI profile",
        status="PASS",
        detail=f"session-token profile {profile_name!r} valid",
    )


def run_local_preflight(
    *,
    bundle_path: Path,
    config: AidpConfig,
    env_name: str,
    env: EnvSpec,
) -> list[PreflightResult]:
    """Run all three local-phase checks in cheapest-first order.

    Returns the full list (one entry per check) so the caller can render
    every result, but short-circuits subsequent checks to ``SKIP`` once a
    FAIL is hit — there's no value in probing OCI if the bundle.yaml is
    malformed, and the operator should fix one thing at a time.
    """
    results: list[PreflightResult] = []

    bundle_result = _check_bundle_yaml(bundle_path)
    results.append(bundle_result)
    if bundle_result.status != "PASS":
        results.append(
            PreflightResult(
                name="aidp.config.yaml dispatch coords",
                status="SKIP",
                detail="skipped — bundle.yaml check failed",
            )
        )
        results.append(
            PreflightResult(
                name="OCI profile",
                status="SKIP",
                detail="skipped — bundle.yaml check failed",
            )
        )
        return results

    coords_result = _check_dispatch_coords(env, env_name)
    results.append(coords_result)
    if coords_result.status != "PASS":
        results.append(
            PreflightResult(
                name="OCI profile",
                status="SKIP",
                detail="skipped — dispatch-coord check failed",
            )
        )
        return results

    results.append(_check_oci_profile_and_session(env))
    return results


# ---------------------------------------------------------------------------
# Phase B — remote checks (require a constructed client)
# ---------------------------------------------------------------------------


def _check_aidp_control_plane(
    client: AidpRestClient,
) -> tuple[PreflightResult, list]:
    """Probe ``list_clusters`` to confirm the AIDP plane is reachable.
    Returns ``(result, clusters_or_empty)`` so check 5 can reuse the list."""
    try:
        clusters = client.list_clusters()
    except AidpRestError as e:
        # First 200 chars of the underlying HTTP excerpt — enough to
        # diagnose region/IAM/wrong-workspace without flooding the terminal.
        detail = str(e).splitlines()[0][:200]
        return (
            PreflightResult(
                name="AIDP control plane",
                status="FAIL",
                detail=detail,
                remediation=(
                    "verify region + workspaceKey + aiDataPlatformId in "
                    "aidp.config.yaml, then check OCI IAM grants for the "
                    "current profile on the target workspace"
                ),
            ),
            [],
        )
    return (
        PreflightResult(
            name="AIDP control plane",
            status="PASS",
            detail=f"reachable; {len(clusters)} cluster(s) visible",
        ),
        clusters,
    )


def _check_cluster_state(
    client: AidpRestClient,
    cluster_key: str,
    clusters: list,
    *,
    auto_start: bool,
    log: Callable[[str], None],
) -> PreflightResult:
    target = next((c for c in clusters if c.key == cluster_key), None)
    if target is None:
        return PreflightResult(
            name="cluster state",
            status="FAIL",
            detail=f"clusterKey {cluster_key!r} not found in workspace",
            remediation=(
                "verify clusterKey under environments.<env> in aidp.config.yaml — "
                "the UUID must match a cluster visible to this workspace"
            ),
        )

    state = target.state
    if state == "ACTIVE":
        return PreflightResult(
            name="cluster state",
            status="PASS",
            detail=f"cluster {cluster_key!r} ACTIVE",
        )

    if state == "STOPPED" and not auto_start:
        return PreflightResult(
            name="cluster state",
            status="FAIL",
            detail=f"cluster {cluster_key!r} is STOPPED",
            remediation=(
                "start it manually via the AIDP UI, or invoke dispatch with "
                "auto-start enabled"
            ),
        )

    if state == "STOPPED" and auto_start:
        log(f"cluster {cluster_key!r} STOPPED — auto-starting (~5 min)…")
        try:
            client.start_cluster(cluster_key)
            client.wait_cluster_active(cluster_key, timeout_s=600)
        except AidpRestError as e:
            return PreflightResult(
                name="cluster state",
                status="FAIL",
                detail=f"cluster {cluster_key!r} auto-start failed: {str(e).splitlines()[0][:200]}",
                remediation="check the AIDP console for the failure reason",
            )
        return PreflightResult(
            name="cluster state",
            status="PASS",
            detail=f"cluster {cluster_key!r} auto-started to ACTIVE",
        )

    # FAILED / CREATING / UNKNOWN / etc — no auto-recovery.
    return PreflightResult(
        name="cluster state",
        status="FAIL",
        detail=f"cluster {cluster_key!r} state={state!r}, expected ACTIVE",
        remediation="check the AIDP console for the cluster's current state",
    )


def _check_bicc_credential(
    client: AidpRestClient,
    secret_name: str,
) -> PreflightResult:
    """P1.5ε-fix1 — Phase B check 6.

    The cluster-side notebook's creds-cell at
    ``notebook_builder._build_creds_cell`` unconditionally calls
    ``aidputils.secrets.get(name=env.bicc_secret_name, key=...)`` BEFORE
    writing the bundle or importing the orchestrator. A missing
    credential surfaces mid-notebook ~4 min into dispatch (wheel build
    + upload + job submit + cluster ramp). This check fast-fails the
    same condition in ~300ms.

    Per §Technical Decisions row 5: **always check, regardless of
    bundle.fusion.password shape**. The notebook's secret fetch is
    independent of how the password is referenced.
    """
    try:
        exists = client.check_credential_exists(secret_name)
    except AidpRestError as exc:
        detail = str(exc).splitlines()[0][:200]
        return PreflightResult(
            name="BICC credential",
            status="FAIL",
            detail=(
                f"credential-store check failed (transport / IAM): {detail}"
            ),
            remediation=(
                "verify IAM grants for `use aiDataPlatformCredentials` on "
                "the current OCI profile; if AIDP is degraded, retry"
            ),
        )
    if not exists:
        return PreflightResult(
            name="BICC credential",
            status="FAIL",
            detail=(
                f"AIDP credential entry {secret_name!r} not found in the "
                "data-lake credential store"
            ),
            remediation=(
                f"add a credential named {secret_name!r} (key 'password') "
                "via the AIDP UI before running, OR change "
                f"environments.<env>.biccSecretName in aidp.config.yaml to "
                "match an existing entry"
            ),
        )
    return PreflightResult(
        name="BICC credential",
        status="PASS",
        detail=f"credential {secret_name!r} present in AIDP store",
    )


def run_remote_preflight(
    *,
    client: AidpRestClient,
    env: EnvSpec,
    auto_start_cluster: bool = True,
    log: Callable[[str], None] = lambda msg: None,
) -> list[PreflightResult]:
    """Run Phase-B checks that require an AIDP control-plane round-trip.

    Order (cheapest-first, each gate independent of the next):
      4. AIDP control plane reachable (list_clusters probe)
      5. Cluster state ACTIVE (or auto-start if STOPPED) — can take ~5 min
      6. BICC credential entry exists in AIDP credential store
         (P1.5ε-fix1) — ~300ms

    Check 6 runs even when check 5 SKIPped/FAILed: the credential check
    is independent of cluster state and giving the operator both pieces
    of information up front is more useful than gating on the most
    expensive check.
    """
    results: list[PreflightResult] = []
    plane_result, clusters = _check_aidp_control_plane(client)
    results.append(plane_result)
    if plane_result.status != "PASS":
        results.append(
            PreflightResult(
                name="cluster state",
                status="SKIP",
                detail="skipped — control-plane check failed",
            )
        )
        results.append(
            PreflightResult(
                name="BICC credential",
                status="SKIP",
                detail="skipped — control-plane check failed",
            )
        )
        return results

    assert env.cluster_key is not None  # Phase A coords check guarantees this
    results.append(
        _check_cluster_state(
            client,
            env.cluster_key,
            clusters,
            auto_start=auto_start_cluster,
            log=log,
        )
    )
    # Check 6 fires regardless of cluster-state outcome — fast (~300ms)
    # and operator-actionable even if the cluster check failed.
    results.append(_check_bicc_credential(client, env.bicc_secret_name))
    return results


# ---------------------------------------------------------------------------
# Convenience
# ---------------------------------------------------------------------------


def any_failed(results: list[PreflightResult]) -> bool:
    """Return True if any check in ``results`` is FAIL."""
    return any(r.status == "FAIL" for r in results)


def render(results: list[PreflightResult]) -> str:
    """One-line-per-check rendering for plain-text logs. The CLI renders
    via Rich; this is a fallback for non-Rich consumers."""
    lines: list[str] = []
    for r in results:
        lines.append(f"[preflight] {r.status} {r.name}: {r.detail}")
        if r.status == "FAIL" and r.remediation:
            lines.append(f"             → {r.remediation}")
    return "\n".join(lines)


__all__ = [
    "PreflightResult",
    "PreflightStatus",
    "any_failed",
    "render",
    "run_local_preflight",
    "run_remote_preflight",
]
