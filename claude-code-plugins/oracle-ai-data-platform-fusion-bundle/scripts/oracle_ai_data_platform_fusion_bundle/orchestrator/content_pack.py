"""Content pack loader and overlay merger.

Reads ``pack.yaml`` files from disk or installed package data, validates them
against the Pydantic models in ``schema.medallion_pack``, and merges overlay
packs with their base pack. Operator-facing behavior is documented in
``docs/content_pack_execution.md`` and ``docs/mart_overlay_authoring.md``.

Public API
----------

* :func:`load_pack` — read a single pack.yaml + its silver/gold/dashboard
  per-node YAML files. Returns a :class:`ResolvedPack`.
* :func:`resolve_overlay_chain` — walk an overlay's ``extends:`` chain to
  the root base pack, rejecting cycles.
* :func:`merge_overlay` — apply overlay merge rules to combine a base pack with
  one or more overlays.

Each function raises a ``PackLoaderError`` subclass with the appropriate
AIDPF code in the message; the CLI ``content-pack validate`` surfaces these
to the operator.

The pack hash (sha256 of canonical merged YAML) is computed by
:meth:`ResolvedPack.compute_hash` and used by the plan-hash drift gate.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from oracle_ai_data_platform_fusion_bundle.schema.dashboard_pack import DashboardYaml
from oracle_ai_data_platform_fusion_bundle.schema.medallion_pack import (
    AIDPF_2001_ORPHAN_OVERRIDE,
    NodeYaml,
    OutputSchemaOverride,
    PackOverlayRef,
    PackYaml,
    # ResolvedPack lives in schema/medallion_pack.py to honor the
    # dispatch import boundary. Re-exported here for compatibility with
    # existing consumers.
    ResolvedPack,
    _canonicalise,
)

# Error codes used by this module.
AIDPF_2001 = AIDPF_2001_ORPHAN_OVERRIDE  # orphan override / extends cycle
AIDPF_2004_EXTENDS_VERSION_MISMATCH = "AIDPF-2004"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class PackLoaderError(Exception):
    """Base class for content-pack load / merge errors.

    Carries an AIDPF code so the CLI can surface it with a remediation pointer.
    """

    code: str = "AIDPF-2000"

    def __init__(self, message: str) -> None:
        super().__init__(message)


class OrphanOverrideError(PackLoaderError):
    code = AIDPF_2001


class OverlayCycleError(PackLoaderError):
    code = AIDPF_2001


class ExtendsVersionMismatchError(PackLoaderError):
    code = AIDPF_2004_EXTENDS_VERSION_MISMATCH


class MissingPackFileError(PackLoaderError):
    code = "AIDPF-2000"


# ---------------------------------------------------------------------------
# ResolvedPack lives in schema/medallion_pack.py to honor the dispatch
# import boundary. Re-exported above for compatibility.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Public helper functions
# ---------------------------------------------------------------------------
#
# The generated REST notebook imports load_full_chain from this module.
# Keeping it here makes it available to both CLI code and the cluster-side
# notebook body without crossing the dispatch import boundary.


def make_filesystem_base_resolver(pack_path: Path):
    """Build a base resolver for :func:`resolve_overlay_chain`.

    Looks up referenced base packs in two places, in order:

    1. Sibling directory of ``pack_path`` with the matching pack id —
       the common workflow during development.
    2. Under the installed-pack directory (Oracle-shipped packs).

    Returns a Callable[[PackOverlayRef], Path]. On miss, raises
    ``FileNotFoundError`` so :func:`resolve_overlay_chain` surfaces
    a clean error.

    Used by both the CLI's content-pack verbs and the inline runner.
    The cluster-side staging path passes a different closure over the
    staged tempdir layers so the cluster reconstructs the overlay chain
    from the embedded layer subdirs.
    """
    # Lazy import to avoid commands -> orchestrator -> commands cycle.
    from ..commands.content_pack import INSTALLED_CONTENT_PACKS_DIR
    from ..schema.medallion_pack import PackOverlayRef

    def resolver(ref: PackOverlayRef) -> Path:
        sibling = pack_path.parent / ref.name
        if sibling.exists() and (sibling / "pack.yaml").exists():
            return sibling.resolve()
        installed = INSTALLED_CONTENT_PACKS_DIR / ref.name
        if installed.exists() and (installed / "pack.yaml").exists():
            return installed.resolve()
        raise FileNotFoundError(
            f"base pack {ref.name!r} (referenced as `extends: {ref.to_string()}`) "
            f"not found beside {pack_path} or in {INSTALLED_CONTENT_PACKS_DIR}"
        )

    return resolver


def load_full_chain(pack_path: Path, *, base_resolver=None) -> ResolvedPack:
    """Load a pack and resolve any ``extends:`` chain.

    For a base pack (no ``extends:``), returns it unmerged. For an
    overlay, resolves the chain via :func:`resolve_overlay_chain` +
    :func:`merge_overlay`, yielding the fully-assembled ``ResolvedPack``
    that validators and the runner expect.

    Args:
        pack_path: filesystem path to the pack root (the overlay root
            for chains; the base root for non-overlay packs).
        base_resolver: callable mapping a :class:`PackOverlayRef` to a
            ``Path``. Required when the pack uses ``extends:`` — overlay
            resolution will raise without it. CLI / inline callers
            typically pass ``make_filesystem_base_resolver(pack_path)``.
            The cluster-side staging passes a closure over staged
            layer subdirs.

    Returns:
        Fully-merged ``ResolvedPack`` with ``chain_roots`` populated.
    """
    if base_resolver is None:
        # Default to the filesystem resolver — the common CLI / inline
        # case. Cluster-side callers MUST pass a staged resolver
        # explicitly (the filesystem default won't find the layers
        # cluster-side).
        base_resolver = make_filesystem_base_resolver(pack_path)

    chain_paths = resolve_overlay_chain(pack_path, base_resolver=base_resolver)
    packs = [load_pack(p) for p in chain_paths]
    merged = packs[0]
    for overlay in packs[1:]:
        merged = merge_overlay(merged, overlay)
    return merged


# ---------------------------------------------------------------------------
# load_pack
# ---------------------------------------------------------------------------


def _read_yaml(path: Path) -> Any:
    if not path.exists():
        raise MissingPackFileError(f"pack file missing: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_pack(root: Path) -> ResolvedPack:
    """Load a single pack from a filesystem directory.

    Reads:
        * ``<root>/pack.yaml`` — required.
        * ``<root>/bronze.yaml`` — optional.
        * ``<root>/silver/*.yaml`` — optional, each a :class:`NodeYaml`.
        * ``<root>/gold/*.yaml`` — optional, each a :class:`NodeYaml`.
        * ``<root>/dashboards/*.yaml`` — optional, each a :class:`DashboardYaml`.

    Does **not** resolve ``extends:`` — that's the job of
    :func:`resolve_overlay_chain` + :func:`merge_overlay`. ``load_pack`` is
    a leaf operation.
    """
    root = Path(root).resolve()
    pack_yaml_path = root / "pack.yaml"
    raw = _read_yaml(pack_yaml_path) or {}
    pack = PackYaml.model_validate(raw)

    bronze_yaml: dict[str, Any] = {}
    if (root / "bronze.yaml").exists():
        bronze_yaml = _read_yaml(root / "bronze.yaml") or {}

    def _scan_nodes(subdir: str) -> dict[str, NodeYaml]:
        nodes: dict[str, NodeYaml] = {}
        d = root / subdir
        if not d.exists():
            return nodes
        for p in sorted(d.glob("*.yaml")):
            raw_node = _read_yaml(p) or {}
            node = NodeYaml.model_validate(raw_node)
            # Filename stem must equal node.id. The loader keys nodes by id, so
            # a mismatched filename would silently mis-target — in particular an
            # overlay's same-id replacement file `bronze/<id>.yaml` carrying a
            # different `id` would become a new node and leave the base node
            # untouched (a silent no-op). Fail closed.
            if p.stem != node.id:
                raise PackLoaderError(
                    f"{AIDPF_2001}: node file {p.name!r} declares id "
                    f"{node.id!r} — the filename stem must equal the node id "
                    f"(rename the file to {node.id}.yaml or fix the id)."
                )
            nodes[node.id] = node
        return nodes

    bronze_nodes = _scan_nodes("bronze")
    silver = _scan_nodes("silver")
    gold = _scan_nodes("gold")

    dashboards: dict[str, DashboardYaml] = {}
    dashboards_dir = root / "dashboards"
    if dashboards_dir.exists():
        for p in sorted(dashboards_dir.glob("*.yaml")):
            raw_d = _read_yaml(p) or {}
            d = DashboardYaml.model_validate(raw_d)
            dashboards[d.id] = d

    source_roots: dict[str, Path] = {}
    if bronze_yaml:
        source_roots["bronze.yaml"] = root
    for nid in bronze_nodes:
        source_roots[f"bronze/{nid}"] = root
    for nid in silver:
        source_roots[f"silver/{nid}"] = root
    for nid in gold:
        source_roots[f"gold/{nid}"] = root
    for did in dashboards:
        source_roots[f"dashboards/{did}"] = root

    return ResolvedPack(
        root=root,
        pack=pack,
        bronze=bronze_nodes,
        silver=silver,
        gold=gold,
        dashboards=dashboards,
        bronze_yaml=bronze_yaml,
        chain=(pack.id,),
        source_roots=source_roots,
        chain_roots=(root,),
    )


# ---------------------------------------------------------------------------
# Overlay chain resolution
# ---------------------------------------------------------------------------


def resolve_overlay_chain(
    overlay_path: Path,
    *,
    base_resolver: "callable[[PackOverlayRef], Path] | None" = None,
) -> list[Path]:
    """Walk an overlay's ``extends:`` chain to the root base pack.

    Returns a list of pack-root paths in **load order** (base first, overlays
    after). For a pack with no ``extends:``, returns ``[overlay_path]``.

    ``base_resolver`` is a callable that maps a :class:`PackOverlayRef` to
    a filesystem path. Tests pass an in-memory resolver; the CLI passes a
    resolver that walks the installed ``content_packs/`` directory.

    After the resolver returns a candidate base path, this function loads
    the candidate's ``pack.yaml`` and **verifies that the resolved pack's
    ``id`` and ``version`` match the ``extends:`` ref**. A mismatch raises
    :class:`ExtendsVersionMismatchError` (``AIDPF-2004``). This guards
    against the failure mode where a name-only resolver returns the wrong
    version (e.g., an overlay declaring ``extends: foo@9.9.9`` silently
    resolving to ``foo@0.1.0``).

    Raises:
        :class:`OverlayCycleError` — ``extends:`` chain contains a cycle.
        :class:`ExtendsVersionMismatchError` — resolved base pack's
            ``id`` or ``version`` does not match the ``extends:`` ref.
    """
    overlay_path = Path(overlay_path).resolve()
    chain: list[Path] = []
    seen: set[Path] = set()
    current: Path | None = overlay_path

    while current is not None:
        current = current.resolve()
        if current in seen:
            cycle_repr = " -> ".join(str(p) for p in chain + [current])
            raise OverlayCycleError(
                f"{AIDPF_2001}: extends: cycle detected: {cycle_repr}"
            )
        seen.add(current)
        chain.insert(0, current)  # prepend; we want base-first order

        raw = _read_yaml(current / "pack.yaml") or {}
        pack = PackYaml.model_validate(raw)
        if pack.extends is None:
            break

        ref = PackOverlayRef.parse(pack.extends)
        if base_resolver is None:
            raise PackLoaderError(
                f"pack {pack.id!r} declares extends: {pack.extends!r} but no "
                "base_resolver was provided to resolve_overlay_chain."
            )
        candidate = base_resolver(ref).resolve()

        # Verify the resolved candidate actually matches the ref's id+version.
        # Resolvers commonly look up by name only (directory match); without
        # this gate, a wrong-version base could silently merge in.
        candidate_raw = _read_yaml(candidate / "pack.yaml") or {}
        candidate_pack = PackYaml.model_validate(candidate_raw)
        if candidate_pack.id != ref.name or candidate_pack.version != ref.version:
            raise ExtendsVersionMismatchError(
                f"{AIDPF_2004_EXTENDS_VERSION_MISMATCH}: overlay "
                f"{pack.id!r} declares `extends: {ref.to_string()}` but the "
                f"base_resolver returned a pack at {candidate} with "
                f"`id={candidate_pack.id!r}, version={candidate_pack.version!r}`. "
                f"Expected `id={ref.name!r}, version={ref.version!r}`."
            )

        current = candidate

    return chain


# ---------------------------------------------------------------------------
# Overlay merge
# ---------------------------------------------------------------------------


def merge_overlay(base: ResolvedPack, overlay: ResolvedPack) -> ResolvedPack:
    """Merge ``overlay`` on top of ``base``.

    Rules applied:

    * ``columnAliases.<vp>.candidates``: list-extend, with the literal
      ``inherit`` keyword preserving base candidates in position.
    * ``semanticVariants.<vp>.candidates``: same as columnAliases.
    * ``overrides.<node-id>``: applied to ``base.silver`` / ``base.gold``:
        - ``sql:`` — full-file replace (validators check the new SQL file
          exists; resolution happens at validation time).
        - ``quality.tests:`` — list-extend.
        - ``profile:`` — scalar replace.
        - Any other key — scalar replace.
    * ``profiles.<name>``: deep merge (overlay nested keys override base
      keys; absent keys keep base values).
    * ``defaults.*``: scalar replace.

    Orphan overrides (overlay overrides a node not present in base) raise
    :class:`OrphanOverrideError` (AIDPF-2001).
    """
    if overlay.pack.extends is None:
        raise PackLoaderError(
            f"merge_overlay called with overlay.pack.extends == None; "
            f"pack {overlay.pack.id!r} is not an overlay."
        )

    # ----- Validate orphan overrides ----------------------------------
    base_node_ids = set(base.bronze) | set(base.silver) | set(base.gold)
    base_qualified_ids = (
        base_node_ids
        | {f"bronze/{nid}" for nid in base.bronze}
        | {f"silver/{nid}" for nid in base.silver}
        | {f"gold/{nid}" for nid in base.gold}
    )
    for override_target in overlay.pack.overrides:
        normalized = override_target.replace("bronze/", "").replace(
            "silver/", ""
        ).replace("gold/", "")
        if normalized not in base_node_ids and override_target not in base_qualified_ids:
            raise OrphanOverrideError(
                f"{AIDPF_2001}: overlay {overlay.pack.id!r} overrides node "
                f"{override_target!r} which does not exist in base pack "
                f"{base.pack.id!r}. Known base nodes: {sorted(base_node_ids)!r}."
            )

    # ----- Merge column aliases / semantic variants -------------------
    merged_column_aliases = _merge_variation_points(
        base.pack.column_aliases, overlay.pack.column_aliases
    )
    merged_semantic_variants = _merge_variation_points(
        base.pack.semantic_variants, overlay.pack.semantic_variants
    )

    # ----- Merge profiles (deep) --------------------------------------
    merged_profiles = dict(base.pack.profiles)
    for name, overlay_profile in overlay.pack.profiles.items():
        if name in merged_profiles:
            merged_profiles[name] = _deep_merge_models(
                merged_profiles[name], overlay_profile
            )
        else:
            merged_profiles[name] = overlay_profile

    # ----- Build merged pack.yaml top-level ---------------------------
    merged_pack_data = base.pack.model_dump(mode="python", by_alias=True)
    merged_pack_data["columnAliases"] = {
        name: ca.model_dump(by_alias=True) if hasattr(ca, "model_dump") else ca
        for name, ca in merged_column_aliases.items()
    }
    merged_pack_data["semanticVariants"] = {
        name: sv.model_dump(by_alias=True) if hasattr(sv, "model_dump") else sv
        for name, sv in merged_semantic_variants.items()
    }
    merged_pack_data["profiles"] = {
        name: p.model_dump(by_alias=True) if hasattr(p, "model_dump") else p
        for name, p in merged_profiles.items()
    }
    # The merged pack inherits base identity but records the overlay chain.
    # We do NOT change `id` / `version` — those remain the base's identity.
    merged_pack_data["extends"] = None
    merged_pack_data["overrides"] = {}

    merged_pack = PackYaml.model_validate(merged_pack_data)

    # ----- Merge node overrides + track source-root provenance ---------
    # source_roots starts from base (every inherited node + dashboard +
    # bronze.yaml entry comes from base.root). Overridden nodes and any
    # overlay-only additions are then reassigned to overlay.root below.
    merged_source_roots: dict[str, Path] = dict(base.source_roots)

    merged_bronze = _apply_node_overrides(base.bronze, overlay, "bronze/")
    merged_silver = _apply_node_overrides(base.silver, overlay, "silver/")
    merged_gold = _apply_node_overrides(base.gold, overlay, "gold/")

    # Reassign source root to the overlay ONLY for `sql:` overrides — the new
    # SQL file lives in the overlay, so `root_for` must resolve there. A pure
    # metadata/schema override (outputSchema / quality / profile) keeps the base
    # root so the node's inherited `implementation.sql` still resolves in
    # validate_sql_paths (a relocated root would raise a spurious AIDPF-2003).
    for override_key, override_entry in overlay.pack.overrides.items():
        if override_entry.sql is None:
            continue
        normalized = override_key.replace("bronze/", "").replace(
            "silver/", ""
        ).replace("gold/", "")
        if normalized in base.bronze:
            merged_source_roots[f"bronze/{normalized}"] = overlay.root
        elif normalized in base.silver:
            merged_source_roots[f"silver/{normalized}"] = overlay.root
        elif normalized in base.gold:
            merged_source_roots[f"gold/{normalized}"] = overlay.root

    # Node ids the overlay block-overrides (for the file/block conflict guard).
    block_overridden = {
        k.replace("bronze/", "").replace("silver/", "").replace("gold/", "")
        for k in overlay.pack.overrides
    }

    # Overlay's own bronze nodes: a brand-new id is an addition; a same id as a
    # base node is a full-node *replacement* (bronze only), guarded.
    for nid, node in overlay.bronze.items():
        if nid not in base.bronze:
            merged_bronze[nid] = node
            merged_source_roots[f"bronze/{nid}"] = overlay.root
            continue
        # Same-id replacement.
        if nid in block_overridden:
            raise OrphanOverrideError(
                f"{AIDPF_2001}: node {nid!r} is overridden two ways — a same-id "
                f"file `bronze/{nid}.yaml` AND a `pack.yaml` overrides entry. "
                f"The two mechanisms are mutually exclusive; declare only one."
            )
        _validate_same_id_bronze_replacement(base.bronze[nid], node)
        merged_bronze[nid] = node
        merged_source_roots[f"bronze/{nid}"] = overlay.root

    # Silver/gold: a brand-new id is an addition; a same id is rejected
    # (full same-id replacement of a shipped mart is not supported).
    for layer, overlay_nodes, base_nodes in (
        ("silver", overlay.silver, base.silver),
        ("gold", overlay.gold, base.gold),
    ):
        target = merged_silver if layer == "silver" else merged_gold
        for nid, node in overlay_nodes.items():
            if nid not in base_nodes:
                target[nid] = node
                merged_source_roots[f"{layer}/{nid}"] = overlay.root
                continue
            raise OrphanOverrideError(
                f"{AIDPF_2001}: same-id {layer} file `{layer}/{nid}.yaml` would "
                f"replace shipped node {nid!r}, which is not supported. Use "
                f"`overrides: {{ {layer}/{nid}: {{ sql: ... }} }}` for a SQL "
                f"change, or create a new mart id for a structural change."
            )

    # Dashboards: overlay can add or replace (replace-only,
    # no field-level merge). Inherited dashboards keep base root; overlay
    # dashboards (whether new or replacing a base one) get overlay root.
    merged_dashboards = dict(base.dashboards)
    for did, dash in overlay.dashboards.items():
        merged_dashboards[did] = dash
        merged_source_roots[f"dashboards/{did}"] = overlay.root

    # bronze.yaml: base wins unless overlay provides one.
    if overlay.bronze_yaml:
        merged_source_roots["bronze.yaml"] = overlay.root

    return ResolvedPack(
        root=overlay.root,
        pack=merged_pack,
        bronze=merged_bronze,
        silver=merged_silver,
        gold=merged_gold,
        dashboards=merged_dashboards,
        bronze_yaml=overlay.bronze_yaml if overlay.bronze_yaml else base.bronze_yaml,
        is_merged=True,
        chain=tuple(list(base.chain) + [overlay.pack.id]),
        source_roots=merged_source_roots,
        # Accumulate like `chain` (ids) — NOT (base.root, overlay.root), which
        # would drop middle/base layers in an overlay-of-overlay chain because a
        # merged base's `.root` is already the top overlay root.
        chain_roots=tuple(list(base.chain_roots) + [overlay.root]),
    )


def _merge_variation_points(base: dict, overlay: dict) -> dict:
    """Merge variation point dicts, applying `inherit` keyword in candidates."""
    out = dict(base)
    for name, overlay_vp in overlay.items():
        base_vp = out.get(name)
        if base_vp is None:
            # Brand-new variation point introduced by overlay.
            out[name] = overlay_vp
            continue
        # Extend candidates with `inherit` handling.
        merged_candidates = _merge_candidate_list(
            base_vp.candidates, overlay_vp.candidates
        )
        # Rebuild the variation-point object via model_validate.
        new_data = overlay_vp.model_dump(by_alias=True)
        # `candidates` may be list[str] for ColumnAlias or list[dict] for
        # SemanticVariant; the merge function handles both.
        if merged_candidates and not isinstance(merged_candidates[0], dict):
            new_data["candidates"] = merged_candidates
        else:
            # SemanticVariant candidates: serialise base ones too.
            new_data["candidates"] = [
                c if isinstance(c, dict) else c.model_dump(by_alias=True)
                for c in merged_candidates
            ]
        out[name] = type(base_vp).model_validate(new_data)
    return out


def _merge_candidate_list(base: list, overlay: list) -> list:
    """Apply the `inherit` keyword convention in an overlay candidate list."""
    result: list = []
    for cand in overlay:
        if cand == "inherit":
            result.extend(base)
        else:
            result.append(cand)
    return result


def _deep_merge_models(base, overlay):
    """Deep-merge two Pydantic models (or dicts) of the same type."""
    base_data = base.model_dump(by_alias=True) if hasattr(base, "model_dump") else dict(base)
    overlay_data = (
        overlay.model_dump(by_alias=True, exclude_unset=True)
        if hasattr(overlay, "model_dump")
        else dict(overlay)
    )

    def _merge(a: Any, b: Any) -> Any:
        if isinstance(a, dict) and isinstance(b, dict):
            out = dict(a)
            for k, v in b.items():
                out[k] = _merge(out.get(k), v) if k in out else v
            return out
        return b

    merged = _merge(base_data, overlay_data)
    return type(base).model_validate(merged) if hasattr(base, "model_validate") else merged


def _apply_node_overrides(
    base_nodes: dict[str, NodeYaml],
    overlay: ResolvedPack,
    prefix: str,
) -> dict[str, NodeYaml]:
    """Apply overlay's `overrides:` entries to a layer (silver/gold) of base nodes."""
    out = {k: v for k, v in base_nodes.items()}
    for override_key, override_entry in overlay.pack.overrides.items():
        # Override keys may be `silver/dim_supplier` or just `dim_supplier`.
        node_id = override_key.replace(prefix, "")
        if node_id not in base_nodes:
            continue  # Belongs to a different layer; skip.

        # Only `profile`, `sql`, and `quality.tests` extension are supported
        # at the schema level. SQL override is a path-replace; validators
        # confirm the new SQL file exists.
        base_node = base_nodes[node_id]
        node_data = base_node.model_dump(by_alias=True)

        if override_entry.profile is not None:
            # Profile is metadata, not a NodeYaml field; we record it on the
            # override but it's surfaced through the merged pack's profiles
            # block. No NodeYaml change needed for v0.3.
            pass

        if override_entry.sql is not None:
            node_data["implementation"] = {
                "type": "sql",
                "sql": override_entry.sql,
            }

        if override_entry.quality is not None and "tests" in override_entry.quality:
            existing_tests = list(node_data.get("quality", {}).get("tests", []))
            new_tests = override_entry.quality.get("tests", [])
            node_data.setdefault("quality", {})["tests"] = existing_tests + new_tests

        if override_entry.output_schema is not None:
            # Bronze-only: a silver/gold outputSchema override is out of scope
            # (those are SQL nodes with exact-match post-write assertions).
            if prefix != "bronze/":
                raise OrphanOverrideError(
                    f"{AIDPF_2001}: outputSchema override on node "
                    f"{override_key!r} is bronze-only. Silver/gold schema "
                    f"changes go through `overrides: {{ sql }}` or a new mart id."
                )
            node_data["outputSchema"]["columns"] = _merge_output_schema_columns(
                node_data["outputSchema"]["columns"],
                override_entry.output_schema,
                node_id,
            )

        out[node_id] = NodeYaml.model_validate(node_data)
    return out


def _validate_same_id_bronze_replacement(
    base_node: NodeYaml, new_node: NodeYaml
) -> None:
    """Guard a same-id bronze full-file replacement.

    The file may differ from base ONLY in ``outputSchema`` and ``quality.tests``
    — a whitelist, so a new/unanticipated extraction field can't slip through.
    Identity fields (layer/grain, target, datastore/pvo, refresh incl.
    naturalKey, requiredColumns, …) must equal base → else a new node id.
    ``outputSchema`` is retain-only (every base column kept; retype/append only)
    and ``quality.tests`` is superset-only (extend, never drop) — neither may
    silently narrow the contract. Fail closed (AIDPF-2001 family).
    """
    b = base_node.model_dump(by_alias=True)
    n = new_node.model_dump(by_alias=True)
    allowed = {"outputSchema", "quality"}
    for key in sorted(set(b) | set(n)):
        if key in allowed:
            continue
        if b.get(key) != n.get(key):
            raise OrphanOverrideError(
                f"{AIDPF_2001}: same-id bronze file for {base_node.id!r} changes "
                f"{key!r} (identity field). Only `outputSchema` and "
                f"`quality.tests` may differ; for an identity change create a "
                f"new node id. base={b.get(key)!r} overlay={n.get(key)!r}."
            )
    # outputSchema retain-only (no contract narrowing; subset assertion wouldn't catch a drop).
    base_cols = {c["name"].lower(): c["name"] for c in b["outputSchema"]["columns"]}
    new_cols = {c["name"].lower() for c in n["outputSchema"]["columns"]}
    dropped = [orig for low, orig in base_cols.items() if low not in new_cols]
    if dropped:
        raise OrphanOverrideError(
            f"{AIDPF_2001}: same-id bronze file for {base_node.id!r} drops base "
            f"outputSchema column(s) {sorted(dropped)!r}. A replacement must "
            f"retain every base column (retype/append only), incl. audit columns."
        )
    # quality.tests superset-only.
    base_tests = (b.get("quality") or {}).get("tests", []) or []
    new_tests = (n.get("quality") or {}).get("tests", []) or []
    for t in base_tests:
        if t not in new_tests:
            raise OrphanOverrideError(
                f"{AIDPF_2001}: same-id bronze file for {base_node.id!r} drops "
                f"base quality test {t!r}. quality.tests may extend but not drop."
            )


def _merge_output_schema_columns(
    base_columns: list[dict],
    override: "OutputSchemaOverride",
    node_id: str,
) -> list[dict]:
    """Name-keyed (case-insensitive) partial merge of override columns into base.

    * Matched column → override only the provided `type`/`nullable`/`pii`;
      the rest inherit from base. Position preserved.
    * New column + `extendColumns: true` → appended; full `type` + `pii`
      required (no column may enter outputSchema without a PII level).
    * New column without `extendColumns` → orphan-column override, fail closed.

    Base columns not mentioned are retained (no narrowing). The re-validation
    via `NodeYaml` then enforces the no-duplicate-name invariant on the result.
    """
    by_lower = {c["name"].lower(): i for i, c in enumerate(base_columns)}
    merged = [dict(c) for c in base_columns]
    for ov in override.columns:
        key = ov.name.lower()
        if key in by_lower:
            col = merged[by_lower[key]]
            if ov.type is not None:
                col["type"] = ov.type
            if ov.nullable is not None:
                col["nullable"] = ov.nullable
            if ov.pii is not None:
                col["pii"] = ov.pii
        else:
            if not override.extend_columns:
                raise OrphanOverrideError(
                    f"{AIDPF_2001}: outputSchema override for node {node_id!r} "
                    f"names column {ov.name!r} which is absent from the base "
                    f"node. Set `extendColumns: true` to append a new column, "
                    f"or fix the name. Known base columns: "
                    f"{[c['name'] for c in base_columns]!r}."
                )
            if ov.type is None or ov.pii is None:
                raise OrphanOverrideError(
                    f"{AIDPF_2001}: appended column {ov.name!r} on node "
                    f"{node_id!r} must declare both `type` and `pii` "
                    f"(no column may enter outputSchema without a PII level)."
                )
            merged.append(
                {
                    "name": ov.name,
                    "type": ov.type,
                    "nullable": ov.nullable if ov.nullable is not None else True,
                    "pii": ov.pii,
                }
            )
    return merged
