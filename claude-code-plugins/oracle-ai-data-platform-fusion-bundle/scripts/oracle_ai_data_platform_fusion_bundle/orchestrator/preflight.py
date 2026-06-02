"""Pre-run BICC bronze-PVO schema probe (P1.5α-fix17, extended by fix19).

Validates every bronze PVO in the plan with a cheap Spark ``inferSchema``
roundtrip BEFORE the orchestrator commits to the full extract loop. Catches
schema-name mismatches, missing PVOs, and BICC-layer auth issues in ~1-2s
per PVO — vs. discovering them ~5-20 minutes into a multi-PVO run when one
of the later extracts crashes.

Surfaced live by TC26 full-happy-path (2026-05-21, run_id=3f9b0648): the
catalog declared ``schema="SCM"`` for ``po_receipts`` and ``scm_items``, but
BICC on saasfademo1 only publishes a ``"Financial"`` offering. The 32-minute
run got 9 successful bronze pulls (including the 10M-row ``gl_period_balances``)
then died on the 10th PVO with ``DATA_ACCESS_LAYER_0031 - Schema: SCM not
found``.

**P1.5α-fix19** turns "fail loud" into "self-correct silently for ~80% of
cases". Three-tier resolution evaluated per PVO:

  1. Override (``bundle.fusion.schema_overrides[node.dataset_id]``) — wins.
  2. Catalog default (``pvo.schema``) — fix17's existing path.
  3. Auto-discovery on ``DATA_ACCESS_LAYER_0031`` — hit
     ``/biacm/rest/meta/datastores`` once (cached), retry with discovered
     schema. Emits a WARN + recommendation for the operator to make the
     fix permanent via tier 1.

The returned :class:`PreflightResult` carries ``effective_schemas`` so the
orchestrator threads the resolved schema into the REAL bronze dispatch —
without that, overrides + auto-discovery would be cosmetic-only (preflight
passes, real run still crashes with the same error).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable

from oracle_ai_data_platform_fusion_bundle import extractors
from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog

from .discovery import discover_pvo_schemas
from .errors import BronzeSchemaProbeError, DiscoveryProbeError
from .registry import BronzeExtractSpec

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
    from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle


_LOG = logging.getLogger("oracle_ai_data_platform_fusion_bundle.orchestrator.preflight")


# Heuristic classification of common BICC failures — used to give the operator
# a remediation hint instead of just the raw Java exception class.
_DATA_ACCESS_LAYER_SCHEMA_RE = "DATA_ACCESS_LAYER_0031"  # Schema X not found
_DATA_ACCESS_LAYER_PVO_RE = "DATA_ACCESS_LAYER_0032"     # PVO not found (inferred — variant of _0031)


# Tier 3 auto-discovery → per-PVO bundle.yaml section the operator should
# remove the consumer from when ambiguous. Today every fix19 user-facing
# remediation points at bundle.fusion.schemaOverrides regardless of layer.
_BUNDLE_SCHEMA_OVERRIDES_SECTION = "bundle.fusion.schemaOverrides"


@dataclass(frozen=True)
class PreflightResult:
    """Structured return value from :func:`preflight_bronze_schemas`.

    Two channels:

    - ``recommendations``: operator-facing strings the CLI renders in the
      summary footer (e.g. ``"consider adding schemaOverrides.po_receipts:
      Financial to bundle.yaml"``). Emitted only on SUCCESSFUL auto-
      correction.
    - ``effective_schemas``: ``dataset_id`` → resolved schema. The
      orchestrator threads this into ``_execute_node`` so the REAL bronze
      dispatch uses the same schema preflight validated. **CRITICAL** —
      without this, overrides + auto-discovery would be cosmetic-only.
      Keyed by ``dataset_id`` (customer-facing bundle id), NOT ``pvo_id``
      (catalog-internal); see ``BronzeExtractSpec`` docstring for the
      alias caveat.
    """

    recommendations: tuple[str, ...]
    effective_schemas: dict[str, str]


def _classify(exc: BaseException) -> str:
    """Return a short remediation hint based on the exception message."""
    msg = str(exc)
    if _DATA_ACCESS_LAYER_SCHEMA_RE in msg:
        return "BICC offering schema not found on this tenant — check catalog schema field is tenant-correct"
    if _DATA_ACCESS_LAYER_PVO_RE in msg:
        return "PVO not found — catalog datastore name may have drifted from BICC"
    if "401" in msg or "Unauthorized" in msg or "authentication" in msg.lower():
        return "BICC credential rejected — check bundle.fusion.username + password resolver"
    if "Connection refused" in msg or "UnknownHost" in msg or "timed out" in msg.lower():
        return "BICC unreachable — check bundle.fusion.serviceUrl + network egress from cluster"
    return "uncategorized BICC reader failure — see full exception in run logs"


def _try_schema(spark, pvo, bundle, password, schema):
    """Invoke extract_pvo + .schema with an explicit schema kwarg.

    Returns None on success; raises whatever extract_pvo / .schema raise.
    Factored out so the override-default-discovery flow has a single
    "try this schema" primitive.
    """
    df = extractors.bicc.extract_pvo(
        spark, pvo,
        fusion_service_url=bundle.fusion.service_url,
        username=bundle.fusion.username,
        password=password,
        fusion_external_storage=bundle.fusion.external_storage,
        schema=schema,
    )
    # Trigger inferSchema (metadata-only — no data rows transferred).
    _ = df.schema


def preflight_bronze_schemas(
    spark: "SparkSession",
    bundle: "Bundle",
    plan: Iterable[object],
    resolved_password: str,
) -> PreflightResult:
    """Probe every bronze PVO in ``plan`` for schema-inference success.

    Lazily calls ``extract_pvo()`` then ``df.schema`` for each
    :class:`BronzeExtractSpec` in the plan. Spark's ``.schema`` triggers
    BICC's ``inferSchema`` (a metadata-only roundtrip — no extract files
    are written, no rows pulled). Deferred specs are skipped.

    On ``DATA_ACCESS_LAYER_0031`` (BICC offering schema not found),
    triggers auto-discovery (P1.5α-fix19): hit ``/biacm/rest/meta/datastores``
    once per run (cached across PVOs), retry with the discovered schema if
    unique. Override (``bundle.fusion.schema_overrides``) wins over both
    catalog default and auto-discovery.

    Returns:
        :class:`PreflightResult` with:
        - ``recommendations``: operator-facing footer strings for the
          summary (one per auto-corrected PVO).
        - ``effective_schemas``: ``dataset_id → resolved schema`` for
          every successful bronze probe. Orchestrator threads this into
          ``_execute_node`` so the real dispatch uses the same schema.

    Raises:
        BronzeSchemaProbeError: At least one PVO probe failed (after
            auto-discovery had its chance). No Spark side effects.
    """
    failures: list[dict] = []
    recommendations: list[str] = []
    effective_schemas: dict[str, str] = {}

    # Per-run discovery cache: None = not yet probed; dict = probe result
    # (may be empty if BICC returned no datastores); _DISC_PROBE_FAILED =
    # probe itself failed, don't retry within this run.
    discovery_cache: dict[str, set[str]] | None = None
    discovery_failed: DiscoveryProbeError | None = None

    def _get_discovery() -> dict[str, set[str]]:
        """Memoize the BICC /biacm/rest/meta/datastores probe across PVOs."""
        nonlocal discovery_cache, discovery_failed
        if discovery_failed is not None:
            raise discovery_failed
        if discovery_cache is not None:
            return discovery_cache
        try:
            discovery_cache = discover_pvo_schemas(
                bundle.fusion.service_url,
                bundle.fusion.username,
                resolved_password,
            )
        except DiscoveryProbeError as exc:
            discovery_failed = exc
            raise
        return discovery_cache

    for node in plan:
        if not isinstance(node, BronzeExtractSpec):
            continue
        try:
            pvo = fusion_catalog.get(node.pvo_id)
        except Exception as exc:
            # Catalog miss — registry vs catalog drift. Surface under the
            # same preflight banner so the operator gets one consolidated
            # failure list.
            failures.append({
                "dataset_id": node.dataset_id, "pvo_id": node.pvo_id,
                "stage": "catalog_lookup",
                "exception_class": type(exc).__name__,
                "message": str(exc)[:300],
                "hint": "catalog entry missing for this PVO id",
            })
            continue

        # Tier 1: override consultation. Keyed by dataset_id (customer-facing
        # bundle id) — see PreflightResult docstring + BronzeExtractSpec note.
        override = bundle.fusion.schema_overrides.get(node.dataset_id)
        from_override = override is not None
        effective_schema = override if from_override else pvo.schema

        try:
            _try_schema(spark, pvo, bundle, resolved_password, effective_schema)
            effective_schemas[node.dataset_id] = effective_schema
            continue
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            # Override failure must NOT trigger discovery — operator's
            # explicit choice was wrong; surface directly so the operator
            # sees their own typo. (Discovery cascading on override
            # failures would mask user mistakes.)
            if from_override:
                failures.append({
                    "dataset_id": node.dataset_id, "pvo_id": node.pvo_id,
                    "catalog_schema": pvo.schema,
                    "attempted_schema": effective_schema,
                    "datastore": pvo.datastore,
                    "stage": "override_failed",
                    "exception_class": type(exc).__name__,
                    "message": str(exc).split("\n")[0][:300],
                    "hint": (
                        f"override schemaOverrides.{node.dataset_id}="
                        f"{effective_schema!r} did not work on this tenant. "
                        f"Check the offering name in BICC's "
                        f"/biacm/rest/meta/datastores or remove the override "
                        f"to fall back to catalog + auto-discovery."
                    ),
                })
                continue

            # Tier 3: auto-discovery (only on schema-not-found from tier 2).
            if _DATA_ACCESS_LAYER_SCHEMA_RE not in str(exc):
                # Not a schema-not-found error; auto-discovery doesn't apply.
                failures.append({
                    "dataset_id": node.dataset_id, "pvo_id": node.pvo_id,
                    "catalog_schema": pvo.schema,
                    "datastore": pvo.datastore,
                    "stage": "schema_infer",
                    "exception_class": type(exc).__name__,
                    "message": str(exc).split("\n")[0][:300],
                    "hint": _classify(exc),
                })
                continue

            try:
                all_schemas = _get_discovery()
            except DiscoveryProbeError as disc_exc:
                # Discovery probe itself failed — surface BOTH errors so the
                # operator can debug either side.
                failures.append({
                    "dataset_id": node.dataset_id, "pvo_id": node.pvo_id,
                    "catalog_schema": pvo.schema,
                    "datastore": pvo.datastore,
                    "stage": "schema_infer_then_discovery_failed",
                    "exception_class": type(exc).__name__,
                    "message": str(exc).split("\n")[0][:300],
                    "discovery_error": str(disc_exc)[:200],
                    "hint": (
                        "BICC returned 'schema not found' AND the "
                        "/biacm/rest/meta/datastores probe also failed. "
                        "Check both the catalog schema field AND BICC server "
                        "health / credentials."
                    ),
                })
                continue

            candidates = all_schemas.get(pvo.datastore, set())
            if len(candidates) == 1:
                discovered = next(iter(candidates))
                try:
                    _try_schema(spark, pvo, bundle, resolved_password, discovered)
                except Exception as retry_exc:
                    failures.append({
                        "dataset_id": node.dataset_id, "pvo_id": node.pvo_id,
                        "catalog_schema": pvo.schema,
                        "datastore": pvo.datastore,
                        "discovered_schema_also_failed": discovered,
                        "stage": "discovered_schema_failed",
                        "exception_class": type(retry_exc).__name__,
                        "message": str(retry_exc).split("\n")[0][:300],
                        "hint": (
                            f"catalog schema {pvo.schema!r} failed AND "
                            f"auto-discovered schema {discovered!r} also "
                            f"failed. Likely BICC-side bug or stale metadata."
                        ),
                    })
                    continue
                # Auto-correction succeeded.
                effective_schemas[node.dataset_id] = discovered
                _LOG.warning(
                    "auto-corrected %s (pvo=%s): catalog=%r → discovered=%r",
                    node.dataset_id, node.pvo_id, pvo.schema, discovered,
                )
                # User-facing remediation MUST name the bundle.yaml key the
                # operator types — that's dataset_id, NOT pvo_id.
                recommendations.append(
                    f"consider adding schemaOverrides.{node.dataset_id}: "
                    f"{discovered} to bundle.yaml to stabilize across runs"
                )
                continue
            elif len(candidates) >= 2:
                failures.append({
                    "dataset_id": node.dataset_id, "pvo_id": node.pvo_id,
                    "catalog_schema": pvo.schema,
                    "datastore": pvo.datastore,
                    "candidates": sorted(candidates),
                    "stage": "discovery_ambiguous",
                    "hint": (
                        f"PVO {pvo.datastore!r} present in multiple offering "
                        f"schemas: {sorted(candidates)}. Auto-pick is unsafe "
                        f"— set schemaOverrides.{node.dataset_id} in "
                        f"bundle.yaml to disambiguate."
                    ),
                })
                continue
            else:  # not found anywhere
                failures.append({
                    "dataset_id": node.dataset_id, "pvo_id": node.pvo_id,
                    "catalog_schema": pvo.schema,
                    "datastore": pvo.datastore,
                    "stage": "discovery_not_found",
                    "hint": (
                        f"PVO {pvo.datastore!r} not found in any BICC "
                        f"offering on this tenant. Either the catalog "
                        f"datastore name has drifted from BICC, the PVO "
                        f"has been renamed, or the tenant's BICC "
                        f"subscription doesn't include it."
                    ),
                })
                continue

    if not failures:
        return PreflightResult(
            recommendations=tuple(recommendations),
            effective_schemas=effective_schemas,
        )

    # Build a multi-line message that gives the operator everything they need
    # without re-running the dispatch.
    lines = [
        f"BICC bronze-schema preflight failed for {len(failures)} PVO(s) — "
        f"no extracts were started. Fix the catalog or bundle and re-run:"
    ]
    for f in failures:
        if f["stage"] == "catalog_lookup":
            lines.append(
                f"  • {f['dataset_id']}: catalog_lookup failed — {f['message']}"
            )
        elif f["stage"] == "discovery_ambiguous":
            lines.append(
                f"  • {f['dataset_id']} (datastore={f['datastore']}): "
                f"PVO present in multiple BICC offerings ({f['candidates']}). "
                f"Add `schemaOverrides.{f['dataset_id']}: <one of "
                f"{f['candidates']}>` to bundle.yaml to disambiguate."
            )
        elif f["stage"] == "discovery_not_found":
            lines.append(
                f"  • {f['dataset_id']} (datastore={f['datastore']}): "
                f"{f['hint']}"
            )
        elif f["stage"] == "override_failed":
            lines.append(
                f"  • {f['dataset_id']} (override={f['attempted_schema']!r}): "
                f"{f['hint']}"
            )
            lines.append(f"      └─ {f['exception_class']}: {f['message']}")
        elif f["stage"] == "discovered_schema_failed":
            lines.append(
                f"  • {f['dataset_id']} (datastore={f['datastore']}): "
                f"{f['hint']}"
            )
            lines.append(f"      └─ {f['exception_class']}: {f['message']}")
        elif f["stage"] == "schema_infer_then_discovery_failed":
            lines.append(
                f"  • {f['dataset_id']} (datastore={f['datastore']}, "
                f"schema={f['catalog_schema']!r}): {f['hint']}"
            )
            lines.append(f"      └─ {f['exception_class']}: {f['message']}")
            lines.append(f"      └─ discovery probe: {f['discovery_error']}")
        else:
            # schema_infer (no schema-not-found classification → no discovery attempted)
            lines.append(
                f"  • {f['dataset_id']} (datastore={f['datastore']}, "
                f"schema={f['catalog_schema']!r}): {f['hint']}"
            )
            lines.append(f"      └─ {f['exception_class']}: {f['message']}")

    raise BronzeSchemaProbeError("\n".join(lines), failures=failures)


# ---------------------------------------------------------------------------
# P1.17 — incremental cursor preflight
# ---------------------------------------------------------------------------


def _preflight_incremental_cursors(
    spark: "SparkSession",
    plan: "list",
    paths: "TablePaths",
) -> None:
    """Two-check incremental preflight gate (P1.17 B4b + P1.17c).

    Runs at run-level (caller: :func:`orchestrator.run`) AFTER
    ``ensure_state_table`` + BEFORE the dispatch loop, ONLY when
    ``mode == "incremental"``. Two consolidated checks, evaluated in
    order:

    1. **Cursor-presence check** (P1.17 V1) — every silver/gold node
       in ``plan`` must have a prior ``last_watermark`` in
       ``fusion_bundle_state``. A NULL cursor means the layer has
       never run a successful seed, and its MERGE source predicate
       ``WHERE bronze_extract_ts > <cursor>`` would become empty →
       permanently-stuck mart. Bronze tolerates a NULL prior cursor
       (full extract fallback), so it's exempt from this check.
       Raises :class:`IncrementalCursorMissingError` listing every
       affected ``(dataset_id, layer)``.

    2. **Target-existence check** (P1.17c — silent-corruption guard) —
       for every in-scope node whose cursor is non-NULL, the target
       Delta table must still exist on disk
       (``spark.catalog.tableExists`` returns True). If an operator
       drops a target out from under a non-NULL cursor, the next
       incremental run would silently lose history below the cursor
       (auto-CREATE empty target + MERGE only delta slice; rows below
       the cursor are filtered out by BICC / the silver-gold source
       predicate and never written back). Bronze IS in scope for this
       check: bronze has a safe NULL-cursor fallback but NO safe
       fallback when its target is dropped under a non-NULL cursor.
       Raises :class:`IncrementalTargetMissingError` listing every
       affected ``(dataset_id, layer, target)``.

    The two checks run in order so a fresh tenant sees the cursor
    message ("run seed") instead of a target-missing message
    ("clear state row + re-seed") — the former is the right
    remediation when seed has never run, the latter when the
    operator has dropped a table out from under a working state.

    Strict state reads (P1.17c). State-row presence is read via
    :func:`state.read_last_watermark_strict`, NOT the soft variant.
    Preflight is a gate; gates fail closed. A transient metastore
    flake during the read raises :class:`StateReadFailedError`
    (also an :class:`OrchestratorConfigError` → CLI exit 2) instead
    of being absorbed into a misleading cursor-missing message —
    operators get the accurate remediation (investigate state-table
    accessibility) instead of being told to re-run seed against an
    unreadable state table.

    Skipped node classes (universal across both checks):
      * :class:`DeferredSpec` — never dispatched.
      * ``dim_calendar`` :class:`SilverDimSpec` — parameter-driven,
        resolver returns ``None``, no source watermark.
      * :class:`GoldMartSpec` with ``incremental_capable=False`` —
        always emits seed-shape regardless of mode (supplier_spend,
        ap_aging).

    :class:`BronzeExtractSpec` is selectively exempt: included in
    the target check (bronze can be silently corrupted by a dropped
    target under non-NULL cursor) but excluded from the cursor-
    presence check (bronze tolerates NULL cursor via full extract).
    The selective exemption is recorded per-node via the
    ``bronze_tolerates_null_cursor`` flag in the single-pass loop
    below.
    """
    # Local imports keep this helper's module-import cost zero outside
    # the incremental codepath, and avoid a hard dependency cycle
    # against ``runtime.py`` (which imports preflight transitively via
    # the orchestrator package's ``__init__``).
    from . import state
    from .errors import IncrementalCursorMissingError, IncrementalTargetMissingError
    from .registry import (
        BronzeExtractSpec,
        DeferredSpec,
        GoldMartSpec,
        SilverDimSpec,
        _layer_for_spec,
        _resolve_target_table,
    )

    # Single-pass collection of per-node (cursor, target-applicability)
    # so the two checks below can iterate over the same pre-computed
    # list without re-reading state. The cursor read uses the strict
    # variant: a transient state-read failure raises StateReadFailedError
    # immediately, before any node-level decisions are made.
    missing_cursors: list[tuple[str, str]] = []
    node_cursors: list[tuple[object, str, "datetime | None", bool]] = []

    for node in plan:
        if isinstance(node, DeferredSpec):
            continue
        if isinstance(node, SilverDimSpec) and node.dataset_id == "dim_calendar":
            continue
        if isinstance(node, GoldMartSpec) and not getattr(node, "incremental_capable", True):
            continue
        layer = _layer_for_spec(node)
        # Strict read — raises StateReadFailedError on metastore
        # exception (P1.17c), bubbling up to the CLI exit-2 path.
        cursor = state.read_last_watermark_strict(
            spark, paths, node.dataset_id, layer,
        )
        bronze_tolerates_null_cursor = isinstance(node, BronzeExtractSpec)
        if cursor is None and not bronze_tolerates_null_cursor:
            missing_cursors.append((node.dataset_id, layer))
        node_cursors.append((node, layer, cursor, bronze_tolerates_null_cursor))

    if missing_cursors:
        raise IncrementalCursorMissingError(missing=missing_cursors)

    # P1.17c target-existence pass. Bronze IS in scope; the
    # ``cursor is None`` early-continue preserves the documented
    # bronze fresh-tenant fallback (NULL cursor + missing target is
    # the normal first-seed state, not silent corruption).
    missing_targets: list[tuple[str, str, str]] = []
    for node, layer, cursor, _bronze in node_cursors:
        if cursor is None:
            continue
        target = _resolve_target_table(node, paths)
        if not spark.catalog.tableExists(target):
            missing_targets.append((node.dataset_id, layer, target))

    if missing_targets:
        raise IncrementalTargetMissingError(missing=missing_targets)


__all__ = [
    "preflight_bronze_schemas",
    "PreflightResult",
    "_preflight_incremental_cursors",
]
