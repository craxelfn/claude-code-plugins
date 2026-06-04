"""CLI verbs for content pack management.

Wires ``aidp-fusion-bundle content-pack {validate,list,info}`` against the
loader (orchestrator.content_pack) and validators
(orchestrator.content_pack_validators).

References:
    * dev/PLAN_plugin_engine_medallion_content_packs.md §14.2 (content-pack validate)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §14.3 (content-pack list)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §14.3a (content-pack info)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §14.0 (global flags: --json)

Exit codes per PLAN §25 convention:
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
    load_pack,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack_validators import (
    ValidationReport,
    validate_pack_full,
)

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
    """Print details about an installed pack."""
    try:
        pack_path = resolve_pack_path(name)
        pack = load_pack(pack_path)
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
        f"[bold]Nodes:[/bold]           {len(info['nodes']['silver'])} silver, "
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
    """Run schema + content validation; surface AIDPF codes."""
    try:
        pack_path = resolve_pack_path(name)
    except FileNotFoundError as exc:
        if json_output:
            print(json.dumps({"error": str(exc)}, indent=2))
        else:
            console.print(f"[red]error:[/red] {exc}")
        return 1

    try:
        pack = load_pack(pack_path)
    except PackLoaderError as exc:
        if json_output:
            print(json.dumps({"errors": [{"code": exc.code, "message": str(exc)}]}, indent=2))
        else:
            console.print(f"[red]{exc.code}:[/red] {exc}")
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
