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
# no env override). P1.17 keeps the constant as the **default** but adds
# a per-tenant override via ``bundle.incremental.watermark_safety_window_seconds``;
# :func:`_resolve_safety_window` reads the bundle field and falls back
# to this module-level default when the bundle hasn't declared one.
#
# Industry-standard CDC pattern (Debezium / Kafka Connect / Airbyte all
# use safety-windowed cursors for cross-system incremental extraction).
WATERMARK_SAFETY_WINDOW: Final[timedelta] = timedelta(hours=1)


def _resolve_safety_window(bundle: "Bundle") -> timedelta:
    """Return the per-run watermark safety window from ``bundle.incremental``.

    Reads ``bundle.incremental.watermark_safety_window_seconds`` (Pydantic
    field, ``gt=0`` validated; default 3600 — see
    :class:`oracle_ai_data_platform_fusion_bundle.schema.bundle.IncrementalConfig`).
    Pure function — no I/O, trivially unit-testable.

    The bronze closure captures the resolved timedelta via closure-scope
    binding at ``_execute_node`` setup; subsequent within-run retries
    re-use the same value (the bundle isn't reloaded mid-run).
    """
    return timedelta(seconds=bundle.incremental.watermark_safety_window_seconds)


def _new_run_id() -> str:
    """One UUID4 per orchestrator invocation. Joins back to
    ``fusion_bundle_state.run_id`` and (post-B3) silver_run_id / gold_run_id
    audit columns on the materialized tables.
    """
    return str(uuid4())


# ---------------------------------------------------------------------------
# RunStep + RunSummary moved to schema/run_summary.py (P1.5ε §Step 1b)
# ---------------------------------------------------------------------------
# Re-exported here so every existing in-package import path keeps working;
# identity is preserved (orchestrator.runtime.RunStep is
# schema.run_summary.RunStep). Skip-reason templates + _utc_now also live
# in the schema module — re-exported below for back-compat.

from ..schema.run_summary import (  # noqa: E402, F401
    MARKER_SCHEMA_VERSION,
    PlanNode,
    RunStep,
    RunSummary,
    _ABORT_MSG_TMPL,
    _CASCADE_MSG_TMPL,
    _RESUME_SKIP_MSG_TMPL,
    _utc_now,
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


BRONZE_AUDIT_COLUMNS: frozenset[str] = frozenset({
    "_extract_ts",
    "_source_pvo",
    "_run_id",
    "_watermark_used",
})
"""Canonical name set for the four bronze audit columns added by
``enrich_bronze_audit_cols``. Single source of truth — consumed by the
P1.17e bronze MERGE payload-diff predicate generator
(``orchestrator/__init__.py::_payload_diff_predicate_sql``) to exclude
audit columns from the ``IS DISTINCT FROM`` clause, and by this
module's enrichment assertion. Order is irrelevant (set semantics);
``enrich_bronze_audit_cols`` controls the column-add order via its
explicit ``withColumn`` chain.
"""


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

    The canonical name set is :data:`BRONZE_AUDIT_COLUMNS` (see module
    constant above); this function's ``withColumn`` chain materializes
    those four names + values. The constant exists so the P1.17e
    payload-diff predicate generator can exclude audit columns by
    symbolic reference rather than a duplicated hardcoded list.

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

    out = (
        df.withColumn("_extract_ts", F.lit(extract_ts).cast("timestamp"))
        .withColumn("_source_pvo", F.lit(source_pvo))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn(
            "_watermark_used",
            F.lit(watermark).cast("timestamp") if watermark is not None else F.lit(None).cast("timestamp"),
        )
    )
    # Defensive: catches a future refactor that adds/removes an audit column
    # without updating BRONZE_AUDIT_COLUMNS. Set difference, not equality, so
    # original payload columns (which are not in BRONZE_AUDIT_COLUMNS) don't
    # trip the check.
    assert BRONZE_AUDIT_COLUMNS.issubset(set(out.columns)), (
        f"enrich_bronze_audit_cols failed to add all of "
        f"{sorted(BRONZE_AUDIT_COLUMNS)}; got {sorted(set(out.columns) - set(df.columns))}"
    )
    return out


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


# P1.5ε §Step 1d — ``load_bundle`` and its ``_render_env_vars`` helper
# moved to ``schema/bundle.py``. Re-exported here so existing in-package
# imports (orchestrator/__init__.py, commands/*.py, ~15 unit-test files)
# keep working unchanged. Identity is preserved.
from ..schema.bundle import (  # noqa: E402, F401
    _render_env_vars,
    load_bundle,
)



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
    "_resolve_safety_window",
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
