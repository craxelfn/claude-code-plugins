"""CLI verbs for content pack management.

Wires ``aidp-fusion-bundle content-pack {validate,list,info}`` against the
loader (orchestrator.content_pack) and validators
(orchestrator.content_pack_validators).

Operator-facing behavior is documented in ``docs/content_pack_execution.md``.

Exit codes:
    0 — success
    1 — I/O or unexpected error
    2 — validation errors found
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

import oracle_ai_data_platform_fusion_bundle as _pkg
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import (
    PackLoaderError,
    ResolvedPack,
    load_pack,
    merge_overlay,
    resolve_overlay_chain,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack_validators import (
    ValidationReport,
    validate_pack_full,
)
from oracle_ai_data_platform_fusion_bundle.schema.medallion_pack import PackOverlayRef

INSTALLED_CONTENT_PACKS_DIR = Path(_pkg.__file__).parent / "content_packs"


# ---------------------------------------------------------------------------
# Pack discovery
# ---------------------------------------------------------------------------


def discover_installed_packs(root: Path = INSTALLED_CONTENT_PACKS_DIR) -> list[Path]:
    """Return paths to all packs shipped under the installed content_packs dir."""
    if not root.exists():
        return []
    return sorted(p for p in root.iterdir() if p.is_dir() and (p / "pack.yaml").exists())


def resolve_pack_path(spec: str) -> Path:
    """Resolve a pack reference to a directory path.

    Accepts:
        - A pack name (e.g., ``fusion-finance-starter``) — looked up under
          the installed ``content_packs/`` directory.
        - A filesystem path to a pack root directory.
    """
    candidate = Path(spec)
    if candidate.exists() and (candidate / "pack.yaml").exists():
        return candidate.resolve()
    # Try installed packs by name.
    by_name = INSTALLED_CONTENT_PACKS_DIR / spec
    if by_name.exists() and (by_name / "pack.yaml").exists():
        return by_name.resolve()
    raise FileNotFoundError(
        f"could not resolve content pack {spec!r} — not a directory and not "
        f"an installed pack under {INSTALLED_CONTENT_PACKS_DIR}"
    )


# ---------------------------------------------------------------------------
# Overlay base resolution for the CLI
# ---------------------------------------------------------------------------


def _make_cli_base_resolver(overlay_root: Path):
    """Build a base resolver for `resolve_overlay_chain`.

    Looks up referenced base packs in two places, in order:

    1. As a sibling directory of the overlay root with the matching pack id.
       This is the common workflow during development — the customer's
       overlay sits next to the base it extends.
    2. Under the installed ``content_packs/`` directory (Oracle-shipped packs).

    On miss, raises ``FileNotFoundError`` so ``resolve_overlay_chain`` surfaces
    a clean error.
    """

    def resolver(ref: "PackOverlayRef") -> Path:
        # Sibling-directory lookup.
        sibling = overlay_root.parent / ref.name
        if sibling.exists() and (sibling / "pack.yaml").exists():
            return sibling.resolve()
        # Installed-packs lookup.
        installed = INSTALLED_CONTENT_PACKS_DIR / ref.name
        if installed.exists() and (installed / "pack.yaml").exists():
            return installed.resolve()
        raise FileNotFoundError(
            f"base pack {ref.name!r} (referenced as `extends: {ref.to_string()}`) "
            f"not found beside {overlay_root} or in {INSTALLED_CONTENT_PACKS_DIR}"
        )

    return resolver


def _load_full_chain(pack_path: Path) -> "ResolvedPack":
    """Backwards-compat alias for the orchestrator-owned loader.

    The generated REST notebook imports
    :func:`orchestrator.content_pack.load_full_chain` without crossing the
    dispatch import boundary. This alias preserves the older CLI-private import
    path for internal callers and matches the behavior 1:1: same callable
    shape, same default base resolver.
    """
    from ..orchestrator.content_pack import load_full_chain
    return load_full_chain(pack_path, base_resolver=_make_cli_base_resolver(pack_path))


# ---------------------------------------------------------------------------
# Verb: content-pack list
# ---------------------------------------------------------------------------


def list_packs(*, json_output: bool, console) -> int:
    """Enumerate installed content packs."""
    packs_info: list[dict[str, str]] = []
    for pack_dir in discover_installed_packs():
        try:
            pack = load_pack(pack_dir)
            packs_info.append(
                {
                    "name": pack.pack.id,
                    "version": pack.pack.version,
                    "path": str(pack_dir),
                }
            )
        except (PackLoaderError, ValidationError) as exc:
            packs_info.append(
                {
                    "name": pack_dir.name,
                    "version": "<load-error>",
                    "path": str(pack_dir),
                    "error": str(exc),
                }
            )

    if json_output:
        print(json.dumps({"packs": packs_info}, indent=2))
        return 0

    if not packs_info:
        console.print("[yellow]No installed content packs found.[/yellow]")
        return 0

    # Pretty table
    name_w = max(len(p["name"]) for p in packs_info)
    ver_w = max(len(p["version"]) for p in packs_info)
    console.print(f"[bold]{'NAME':<{name_w}}  {'VERSION':<{ver_w}}  PATH[/bold]")
    for p in packs_info:
        console.print(f"{p['name']:<{name_w}}  {p['version']:<{ver_w}}  {p['path']}")
    return 0


# ---------------------------------------------------------------------------
# Verb: content-pack info
# ---------------------------------------------------------------------------


def info_pack(name: str, *, json_output: bool, console) -> int:
    """Print details about an installed pack (overlay-aware)."""
    try:
        pack_path = resolve_pack_path(name)
        pack = _load_full_chain(pack_path)
    except (FileNotFoundError, PackLoaderError, ValidationError) as exc:
        if json_output:
            print(json.dumps({"error": str(exc)}, indent=2))
        else:
            console.print(f"[red]error:[/red] {exc}")
        return 1

    info: dict[str, Any] = {
        "id": pack.pack.id,
        "version": pack.pack.version,
        "description": pack.pack.description,
        "compatibility": {
            "pluginMinVersion": pack.pack.compatibility.plugin_min_version,
            "fusionFamilies": pack.pack.compatibility.fusion_families,
            "requiresDelta": pack.pack.compatibility.aidp.requires_delta,
        },
        "path": str(pack.root),
        "nodes": {
            "bronze": list(sorted(pack.bronze)),
            "silver": list(sorted(pack.silver)),
            "gold": list(sorted(pack.gold)),
            "builtin_count": sum(
                1 for n in pack.silver.values() if n.implementation.type == "builtin"
            )
            + sum(1 for n in pack.gold.values() if n.implementation.type == "builtin"),
        },
        "variation_points": {
            "columnAliases": list(sorted(pack.pack.column_aliases)),
            "semanticVariants": list(sorted(pack.pack.semantic_variants)),
        },
        "dashboards": list(sorted(pack.dashboards)),
        "pack_hash": pack.compute_hash(),
    }

    if json_output:
        print(json.dumps(info, indent=2))
        return 0

    console.print(f"[bold]Pack:[/bold]            {info['id']}")
    console.print(f"[bold]Version:[/bold]         {info['version']}")
    console.print(f"[bold]Path:[/bold]            {info['path']}")
    console.print(
        f"[bold]Compatibility:[/bold]   pluginMinVersion >= {info['compatibility']['pluginMinVersion']}"
    )
    console.print(
        f"                  fusionFamilies: {info['compatibility']['fusionFamilies']}"
    )
    console.print(
        f"                  aidp.requiresDelta: {info['compatibility']['requiresDelta']}"
    )
    console.print(
        f"[bold]Nodes:[/bold]           {len(info['nodes']['bronze'])} bronze, "
        f"{len(info['nodes']['silver'])} silver, "
        f"{len(info['nodes']['gold'])} gold "
        f"({info['nodes']['builtin_count']} builtin)"
    )
    console.print(
        f"[bold]Variation points:[/bold] {len(info['variation_points']['columnAliases'])} columnAliases, "
        f"{len(info['variation_points']['semanticVariants'])} semanticVariants"
    )
    console.print(
        f"[bold]Dashboards:[/bold]      {len(info['dashboards'])}"
        + (" (" + ", ".join(info["dashboards"]) + ")" if info["dashboards"] else "")
    )
    return 0


# ---------------------------------------------------------------------------
# Verb: content-pack validate
# ---------------------------------------------------------------------------


def validate_pack_cli(name: str, *, json_output: bool, console) -> int:
    """Run schema + content validation; surface AIDPF codes.

    Resolves the full ``extends:`` chain via ``resolve_overlay_chain`` +
    ``merge_overlay`` before validating, so overlay failures (orphan
    overrides → AIDPF-2001, inherited dashboard / SQL errors, etc.) are
    surfaced to the operator.
    """
    try:
        pack_path = resolve_pack_path(name)
    except FileNotFoundError as exc:
        if json_output:
            print(json.dumps({"error": str(exc)}, indent=2))
        else:
            console.print(f"[red]error:[/red] {exc}")
        return 1

    try:
        pack = _load_full_chain(pack_path)
    except PackLoaderError as exc:
        if json_output:
            print(json.dumps({"errors": [{"code": exc.code, "message": str(exc)}]}, indent=2))
        else:
            console.print(f"[red]{exc.code}:[/red] {exc}")
        return 2
    except FileNotFoundError as exc:
        # Base pack referenced by `extends:` not found.
        if json_output:
            print(json.dumps({"errors": [{"code": "AIDPF-2000", "message": str(exc)}]}, indent=2))
        else:
            console.print(f"[red]error:[/red] {exc}")
        return 2
    except ValidationError as exc:
        if json_output:
            print(
                json.dumps(
                    {
                        "errors": [
                            {
                                "code": "AIDPF-2000",
                                "message": str(e),
                                "loc": list(e.get("loc", ())),
                            }
                            for e in exc.errors()
                        ]
                    },
                    indent=2,
                )
            )
        else:
            console.print("[red]Pack schema validation failed:[/red]")
            for e in exc.errors():
                console.print(f"  - {e.get('msg', '')} (at {e.get('loc')})")
        return 2

    report: ValidationReport = validate_pack_full(pack)
    if json_output:
        print(
            json.dumps(
                {
                    "pack": pack.pack.id,
                    "version": pack.pack.version,
                    "ok": report.ok,
                    "errors": [
                        {"code": e.code, "message": e.message, "location": e.location}
                        for e in report.errors
                    ],
                    "warnings": [
                        {"code": w.code, "message": w.message, "location": w.location}
                        for w in report.warnings
                    ],
                },
                indent=2,
            )
        )
    else:
        if report.ok:
            console.print(
                f"[green]✓[/green] {pack.pack.id}@{pack.pack.version} validates clean."
            )
        else:
            console.print(
                f"[red]✗[/red] {pack.pack.id}@{pack.pack.version} has "
                f"{len(report.errors)} validation error(s):"
            )
            for e in report.errors:
                loc_part = f" [{e.location}]" if e.location else ""
                console.print(f"  [red]{e.code}[/red]{loc_part}: {e.message}")

    return 0 if report.ok else 2
