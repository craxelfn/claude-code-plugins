"""Content pack loader + overlay merger.

Reads ``pack.yaml`` files from disk (or installed package data), validates
them against the Pydantic models in ``schema.medallion_pack``, and merges
overlay packs with their base per the rules in PLAN §8.7.

References:
    * dev/PLAN_plugin_engine_medallion_content_packs.md §6 (repo structure, paths)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §7.3 (path resolution)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §8.7 (pack overlays)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §25 (error codes — AIDPF-2001)

Public API
----------

* :func:`load_pack` — read a single pack.yaml + its silver/gold/dashboard
  per-node YAML files. Returns a :class:`ResolvedPack`.
* :func:`resolve_overlay_chain` — walk an overlay's ``extends:`` chain to
  the root base pack, rejecting cycles.
* :func:`merge_overlay` — apply §8.7 merge rules to combine a base pack
  with one or more overlays.

Each function raises a ``PackLoaderError`` subclass with the appropriate
AIDPF code in the message; the CLI ``content-pack validate`` surfaces these
to the operator.

The pack hash (sha256 of canonical merged YAML) is computed by
:meth:`ResolvedPack.compute_hash` and used by PLAN §11.9 plan-hash drift
detection.
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
    PackOverlayRef,
    PackYaml,
)

# Error codes used by this module (registered in PLAN §25).
AIDPF_2001 = AIDPF_2001_ORPHAN_OVERRIDE  # orphan override / extends cycle


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class PackLoaderError(Exception):
    """Base class for content-pack load / merge errors.

    Carries an AIDPF code so the CLI can surface it with a remediation
    pointer (PLAN §25).
    """

    code: str = "AIDPF-2000"

    def __init__(self, message: str) -> None:
        super().__init__(message)


class OrphanOverrideError(PackLoaderError):
    code = AIDPF_2001


class OverlayCycleError(PackLoaderError):
    code = AIDPF_2001


class MissingPackFileError(PackLoaderError):
    code = "AIDPF-2000"


# ---------------------------------------------------------------------------
# ResolvedPack dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedPack:
    """A fully-loaded content pack (post-overlay-merge).

    Attributes:
        root: filesystem path to the pack root directory. For merged packs,
            this is the overlay root (top of the chain). Use ``source_roots``
            for per-node path resolution.
        pack: parsed ``pack.yaml`` (top-level).
        silver: per-node-id mapping of silver nodes (parsed from
            ``silver/*.yaml``).
        gold: per-node-id mapping of gold nodes (parsed from ``gold/*.yaml``).
        dashboards: per-dashboard-id mapping (parsed from
            ``dashboards/*.yaml``).
        bronze_yaml: parsed contents of ``bronze.yaml`` (unstructured dict
            for now; Phase 1 doesn't validate the schema — Phase 2 will).
        is_merged: True if this is the result of a merge_overlay call.
        chain: list of pack ids in load order (base first, overlays after).
        source_roots: per-artifact pack-root provenance. Keys are qualified
            ids (``"silver/<id>"``, ``"gold/<id>"``, ``"dashboards/<id>"``,
            and the literal ``"bronze.yaml"``). Values are the pack-root
            paths the artifact's files actually live under. For a non-merged
            pack, every key maps to ``root``. For a merged pack, inherited
            base artifacts keep their base root; overlay-added or
            overlay-overridden artifacts use the overlay root.

            Validators use this to resolve relative file paths (e.g. a
            silver node's ``implementation.sql``) against the correct
            filesystem location. The single ``root`` field is insufficient
            for merged packs because inherited base SQL lives under
            ``base.root``, not under ``overlay.root``.
    """

    root: Path
    pack: PackYaml
    silver: dict[str, NodeYaml] = field(default_factory=dict)
    gold: dict[str, NodeYaml] = field(default_factory=dict)
    dashboards: dict[str, DashboardYaml] = field(default_factory=dict)
    bronze_yaml: dict[str, Any] = field(default_factory=dict)
    is_merged: bool = False
    chain: tuple[str, ...] = ()
    source_roots: dict[str, Path] = field(default_factory=dict)

    def all_nodes(self) -> dict[str, NodeYaml]:
        """Convenience: silver and gold nodes combined."""
        return {**self.silver, **self.gold}

    def root_for(self, qualified_id: str) -> Path:
        """Return the source-pack root for an artifact id, falling back to ``root``.

        ``qualified_id`` examples: ``"silver/dim_supplier"``,
        ``"gold/gl_balance"``, ``"dashboards/executive_cfo"``, ``"bronze.yaml"``.
        """
        return self.source_roots.get(qualified_id, self.root)

    def compute_hash(self) -> str:
        """Stable sha256 of the pack's canonical serialised form.

        Used by PLAN §11.9 plan-hash drift detection. Deterministic across
        runs: keys sorted, no unstable ordering.
        """
        # We hash the pack.yaml's model_dump plus the node/dashboard contents.
        payload: dict[str, Any] = {
            "pack": self.pack.model_dump(mode="json", by_alias=True),
            "silver": {k: v.model_dump(mode="json", by_alias=True) for k, v in sorted(self.silver.items())},
            "gold": {k: v.model_dump(mode="json", by_alias=True) for k, v in sorted(self.gold.items())},
            "dashboards": {
                k: v.model_dump(mode="json", by_alias=True)
                for k, v in sorted(self.dashboards.items())
            },
            "bronze_yaml": _canonicalise(self.bronze_yaml),
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()


def _canonicalise(value: Any) -> Any:
    """Recursively sort dict keys for deterministic hashing."""
    if isinstance(value, dict):
        return {k: _canonicalise(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [_canonicalise(v) for v in value]
    return value


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
            nodes[node.id] = node
        return nodes

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
    for nid in silver:
        source_roots[f"silver/{nid}"] = root
    for nid in gold:
        source_roots[f"gold/{nid}"] = root
    for did in dashboards:
        source_roots[f"dashboards/{did}"] = root

    return ResolvedPack(
        root=root,
        pack=pack,
        silver=silver,
        gold=gold,
        dashboards=dashboards,
        bronze_yaml=bronze_yaml,
        chain=(pack.id,),
        source_roots=source_roots,
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
    resolver that walks the installed `content_packs/` directory.

    Raises :class:`OverlayCycleError` if a cycle is detected.
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
        current = base_resolver(ref)

    return chain


# ---------------------------------------------------------------------------
# Overlay merge
# ---------------------------------------------------------------------------


def merge_overlay(base: ResolvedPack, overlay: ResolvedPack) -> ResolvedPack:
    """Merge ``overlay`` on top of ``base`` per PLAN §8.7 rules.

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
    base_node_ids = set(base.silver) | set(base.gold)
    base_qualified_ids = base_node_ids | {f"silver/{nid}" for nid in base.silver} | {
        f"gold/{nid}" for nid in base.gold
    }
    for override_target in overlay.pack.overrides:
        normalized = override_target.replace("silver/", "").replace("gold/", "")
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

    merged_silver = _apply_node_overrides(base.silver, overlay, "silver/")
    merged_gold = _apply_node_overrides(base.gold, overlay, "gold/")

    # Mark every override target's source root as the overlay root,
    # since the override declared by the overlay points at overlay-side files.
    for override_key in overlay.pack.overrides:
        normalized = override_key.replace("silver/", "").replace("gold/", "")
        if normalized in base.silver:
            merged_source_roots[f"silver/{normalized}"] = overlay.root
        elif normalized in base.gold:
            merged_source_roots[f"gold/{normalized}"] = overlay.root

    # Overlay's own silver/gold (not declared as overrides) are additions.
    for nid, node in overlay.silver.items():
        if nid not in merged_silver:
            merged_silver[nid] = node
            merged_source_roots[f"silver/{nid}"] = overlay.root
    for nid, node in overlay.gold.items():
        if nid not in merged_gold:
            merged_gold[nid] = node
            merged_source_roots[f"gold/{nid}"] = overlay.root

    # Dashboards: overlay can add or replace (Phase 1 scope — replace-only,
    # no field-level merge). Inherited dashboards keep base root; overlay
    # dashboards (whether new or replacing a base one) get overlay root.
    merged_dashboards = dict(base.dashboards)
    for did, dash in overlay.dashboards.items():
        merged_dashboards[did] = dash
        merged_source_roots[f"dashboards/{did}"] = overlay.root

    # bronze.yaml: base wins unless overlay provides one (rare; Phase 2 feature).
    if overlay.bronze_yaml:
        merged_source_roots["bronze.yaml"] = overlay.root

    return ResolvedPack(
        root=overlay.root,
        pack=merged_pack,
        silver=merged_silver,
        gold=merged_gold,
        dashboards=merged_dashboards,
        bronze_yaml=overlay.bronze_yaml if overlay.bronze_yaml else base.bronze_yaml,
        is_merged=True,
        chain=tuple(list(base.chain) + [overlay.pack.id]),
        source_roots=merged_source_roots,
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

        # Phase 1: only `profile` and `sql` and `quality.tests` extension are
        # supported at the schema level. SQL override is a path-replace;
        # validators (Step 6) confirm the new SQL file exists.
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

        out[node_id] = NodeYaml.model_validate(node_data)
    return out
