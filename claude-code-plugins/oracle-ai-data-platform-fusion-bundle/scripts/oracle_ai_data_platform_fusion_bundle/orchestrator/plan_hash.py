"""Plan-shape + execution-identity hashing for resume drift detection.

A resume operation needs to prove that the bundle being resumed against is
materially the same as the bundle that started the run. "Same" is decomposed
into two axes:

1. **Plan shape** — which (dataset_id, layer, mode, effective_schema) tuples
   the orchestrator is about to dispatch. Schema is the post-preflight
   ``effective_schema`` (the value runtime threads into ``_execute_node``),
   NOT the raw ``schemaOverrides`` — so an auto-discovered schema flip
   between original and resume is detected.

2. **Execution identity** — non-plan environmental knobs that change the
   semantic meaning of "same plan": Fusion pod (`serviceUrl`), BICC storage
   profile (`externalStorage`), Fusion principal (`username`), AIDP target
   paths (`catalog` / `bronzeSchema` / `silverSchema` / `goldSchema`), and
   plugin code version. Secrets (`password`, vault OCIDs) are deliberately
   excluded — the hash is persisted to ``fusion_bundle_state`` and surfaced
   in error messages; identity ≠ credentials.

Both axes feed a single SHA256 hash AND a JSON ``plan_snapshot`` of the
canonical shape ``{"identity": {...}, "nodes": [...]}``. The snapshot is
kept for two reasons:

  * Diagnostic — ``ResumeBundleMismatchError`` diffs the stored snapshot
    against the current shape to tell the operator *which* identity field
    or dataset diverged, instead of an opaque "hashes differ".
  * Scope reconstruction — a bare ``--resume <run_id>`` (no
    ``--datasets`` / ``--layers``) rebuilds the original dispatch scope
    from ``snapshot.nodes``.

The hash and snapshot are computed once per run and persisted alongside
every state-table row written under that run_id (see ``state.py``).
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Mapping, Sequence

    from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
    from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle

    from .registry import (
        BronzeExtractSpec,
        DeferredSpec,
        GoldMartSpec,
        SilverDimSpec,
    )

    PlanNode = (
        BronzeExtractSpec | SilverDimSpec | GoldMartSpec | DeferredSpec
    )


# Sentinel used in node tuples for layers where schema is N/A (silver,
# gold, deferred). Picked as the empty string so the JSON projection
# stays compact and the SHA256 input is deterministic.
_NO_SCHEMA = ""


def _identity_dict(
    bundle: "Bundle",
    paths: "TablePaths",
    plugin_version: str,
) -> dict[str, str]:
    """Extract the 8-field execution identity from bundle + paths + version.

    Centralized so the hash and the snapshot agree on field ordering and
    field set. Secrets (``bundle.fusion.password``, vault OCIDs) are
    excluded; ``fusion.username`` is non-secret by Oracle convention and
    pins the principal (mixed-authorization guard).
    """
    return {
        "fusion.serviceUrl": bundle.fusion.service_url,
        "fusion.externalStorage": bundle.fusion.external_storage,
        "fusion.username": bundle.fusion.username,
        "aidp.catalog": paths.catalog,
        "aidp.bronzeSchema": paths.bronze_schema,
        "aidp.silverSchema": paths.silver_schema,
        "aidp.goldSchema": paths.gold_schema,
        "plugin_version": plugin_version,
    }


def _node_tuple(
    node: "PlanNode",
    mode: str,
    effective_schemas: "Mapping[str, str]",
) -> tuple[str, str, str, str]:
    """Project a plan node to its canonical ``(dataset_id, layer, mode,
    effective_schema)`` tuple.

    Schema is the runtime ``effective_schema`` for bronze nodes — the
    value runtime threads into ``_execute_node`` at
    ``orchestrator/__init__.py``. For non-bronze nodes (silver, gold,
    deferred) schema is N/A and normalized to the empty string.

    ``effective_schemas`` is a partial mapping ``dataset_id -> schema``;
    bronze nodes without an entry fall back to ``""`` so a missing
    preflight entry doesn't crash the hash (it'll surface as drift
    when the resume run computes its own effective_schemas).
    """
    from .registry import BronzeExtractSpec, _layer_for_spec

    layer = _layer_for_spec(node)
    if isinstance(node, BronzeExtractSpec):
        effective_schema = effective_schemas.get(node.dataset_id, _NO_SCHEMA)
    else:
        effective_schema = _NO_SCHEMA
    return (node.dataset_id, layer, mode, effective_schema)


def _canonical_payload(
    plan: "Sequence[PlanNode]",
    effective_schemas: "Mapping[str, str]",
    mode: str,
    identity: "Mapping[str, str]",
) -> dict[str, object]:
    """Build the canonical ``{"identity": {...}, "nodes": [...]}`` shape.

    Nodes are sorted by ``dataset_id`` so plan-order changes don't flip
    the hash. Identity keys are sorted by the field-name spelling so
    JSON serialization is byte-stable across Python versions.
    """
    nodes = sorted(
        _node_tuple(node, mode, effective_schemas) for node in plan
    )
    return {
        "identity": dict(sorted(identity.items())),
        "nodes": [
            {
                "dataset_id": ds,
                "layer": layer,
                "mode": m,
                "effective_schema": schema,
            }
            for (ds, layer, m, schema) in nodes
        ],
    }


def hash_resolved_plan(
    plan: "Sequence[PlanNode]",
    effective_schemas: "Mapping[str, str]",
    mode: str,
    *,
    bundle: "Bundle",
    paths: "TablePaths",
    plugin_version: str,
) -> str:
    """SHA256 of the canonical plan-shape + execution-identity payload.

    Stability properties (pinned by ``tests/orchestrator/test_plan_hash.py``):
      * Identical plans + identical effective_schemas + identical
        identity → identical hash.
      * Reordered ``plan`` argument → identical hash (sort-first).
      * Any single change in any of the 8 identity fields, any
        ``effective_schema`` value, the mode, or the dataset_id /
        layer set → hash flips.

    Returned as a lowercase 64-char hex digest.
    """
    identity = _identity_dict(bundle, paths, plugin_version)
    payload = _canonical_payload(plan, effective_schemas, mode, identity)
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def serialize_plan_snapshot(
    plan: "Sequence[PlanNode]",
    effective_schemas: "Mapping[str, str]",
    mode: str,
    *,
    bundle: "Bundle",
    paths: "TablePaths",
    plugin_version: str,
) -> str:
    """JSON serialization of the canonical snapshot, persisted to
    ``fusion_bundle_state.plan_snapshot``.

    Same shape as the hash input; reading it back gives the
    ``ResumeBundleMismatchError`` renderer + the scope-reconstruction
    path everything they need. Bounded size: each node is ~80 bytes,
    identity is ~300 bytes → ≤2KB for a 20-dataset bundle.
    """
    identity = _identity_dict(bundle, paths, plugin_version)
    payload = _canonical_payload(plan, effective_schemas, mode, identity)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def build_current_diagnostics(
    plan: "Sequence[PlanNode]",
    effective_schemas: "Mapping[str, str]",
    mode: str,
    *,
    bundle: "Bundle",
    paths: "TablePaths",
    plugin_version: str,
) -> "tuple[dict[str, str], list[dict[str, str]]]":
    """Helper for the drift renderer: returns
    ``(identity_dict, node_tuples_list)`` so
    :func:`orchestrator.resume.render_drift_error` can diff the
    current shape against the stored snapshot without re-implementing
    the canonical-payload code path.

    Splits the canonical payload into its two halves so the renderer
    can label each half separately ("Identity changes" vs "Dataset
    changes") without re-parsing the JSON.
    """
    identity = _identity_dict(bundle, paths, plugin_version)
    payload = _canonical_payload(plan, effective_schemas, mode, identity)
    # ``payload["nodes"]`` is already a list of dicts in canonical
    # order; cast for type-checker.
    return identity, list(payload["nodes"])  # type: ignore[list-item]


__all__ = [
    "hash_resolved_plan",
    "serialize_plan_snapshot",
    "build_current_diagnostics",
    # Phase 2 content-pack plan-hash additions.
    "compute_output_schema_hash",
    "compute_content_pack_plan_hash",
]


# ---------------------------------------------------------------------------
# Phase 2 — content-pack backend plan-hash (PLAN §11.9)
# ---------------------------------------------------------------------------
#
# These functions add new plan-hash inputs that exist ONLY under
# --execution-backend content-pack. Legacy-python runs continue to use
# hash_resolved_plan() unchanged — Phase 2 does NOT modify the v1
# plan-hash, so in-flight v1 runs remain resumable.
#
# The content-pack plan-hash mixes:
#
#   * pack identity (pack_id, pack_version)
#   * node identity (node_id, node_version, node_implementation_type)
#   * rendered SQL hash (template + profile-resolved params; from
#     orchestrator/sql_renderer.py::compute_rendered_sql_hash)
#   * declared output_schema hash (this module)
#   * profile_hash (from schema/tenant_profile.py::compute_profile_hash)
#   * tenant fingerprint + bronze-schema fingerprint (from profile)
#   * refresh strategy + natural key + partition columns + watermark spec
#
# Drift on any of these blocks the resume (AIDPF-4040).


def compute_output_schema_hash(node: "NodeYaml") -> str:  # noqa: F821 — forward
    """Deterministic hash of a node's declared ``outputSchema``.

    Canonical serialisation: ``name|type|nullable|pii`` per column in
    declared order (column reorder shifts the hash — declared order is
    significant; downstream consumers rely on it). Joined with newlines,
    sha256.

    Computed pre-dispatch from ``node.outputSchema`` — NOT from a
    Spark probe. The materialised-schema assertion in Step 11
    (``_assert_materialized_matches_declared``) is a separate post-
    execution check that catches SQL-induced drift; this hash catches
    YAML-author-induced drift.

    Cosmetic YAML whitespace doesn't shift the hash; semantic flips
    (add a column / change a type / flip nullable / change pii level)
    do.

    Args:
        node: the validated NodeYaml whose outputSchema to hash.

    Returns:
        Hex sha256 string.
    """
    columns = []
    for col in node.output_schema.columns:
        columns.append(
            f"{col.name}|{col.type}|{int(col.nullable)}|{col.pii}"
        )
    canonical = "\n".join(columns)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_content_pack_plan_hash(
    *,
    pack: "ResolvedPack",  # noqa: F821 — forward
    node: "NodeYaml",  # noqa: F821
    profile: "TenantProfile",  # noqa: F821
    rendered_sql_hash: str,
    output_schema_hash: str,
    profile_hash: str,
) -> str:
    """Assemble the content-pack plan-hash from its constituent inputs.

    Per PLAN §11.9, the content-pack plan-hash includes every input that
    can semantically change "what this node would produce":

    * Pack identity (id + version) — catches "we re-resolved to a
      different pack".
    * Node identity (id + implementation type + declared version).
    * Refresh strategy + natural key + partition columns + watermark
      spec — catches a node-yaml edit that didn't change the SQL but
      changed the refresh shape.
    * Rendered SQL hash — catches a SQL template edit (or a profile-
      value flip the template substituted).
    * Output schema hash — catches a YAML outputSchema edit.
    * Profile hash — catches a variation-point pick flip or any
      tenant-profile value change.
    * Tenant fingerprint + bronze schema fingerprint — catches "tenant
      identity or bronze schema drifted since the last successful run".

    Args:
        pack: assembled ResolvedPack (provides pack.id + pack.version).
        node: validated NodeYaml.
        profile: validated TenantProfile (provides tenant fingerprint +
            bronze schema fingerprint).
        rendered_sql_hash: from ``compute_rendered_sql_hash(rendered)``.
        output_schema_hash: from :func:`compute_output_schema_hash`.
        profile_hash: from ``compute_profile_hash(profile)``.

    Returns:
        Hex sha256 string. Comparing this against the prior successful
        state row's ``plan_hash`` is the resume drift gate (PLAN §11.9
        / AIDPF-4040).
    """
    inc = node.refresh.incremental
    payload = {
        # Pack identity.
        "pack_id": pack.pack.id,
        "pack_version": pack.pack.version,
        # Node identity.
        "node_id": node.id,
        "node_implementation_type": node.implementation.type,
        # Refresh shape.
        "refresh_seed_strategy": node.refresh.seed.strategy,
        "refresh_incremental_strategy": inc.strategy if inc else None,
        "natural_key": list(inc.natural_key) if inc and inc.natural_key else [],
        "partition_columns": list(inc.partition_columns) if inc and inc.partition_columns else [],
        "watermark_source": inc.watermark.source if inc and inc.watermark else None,
        "watermark_column": inc.watermark.column if inc and inc.watermark else None,
        # Hashes (computed by other modules; mixed in by reference here).
        "rendered_sql_hash": rendered_sql_hash,
        "output_schema_hash": output_schema_hash,
        "profile_hash": profile_hash,
        # Identity fingerprints (PLAN §11.6 Gate 4 inputs).
        "tenant": profile.tenant,
        "bronze_schema_fingerprint": profile.bronze_schema_fingerprint,
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()
