"""Implementation of ``aidp-fusion-bundle init``.

Scaffolds ``bundle.yaml`` + ``aidp.config.yaml`` in the current directory by
copying one of the bundled customer-project templates.
"""

from __future__ import annotations

import shutil
from importlib import resources
from pathlib import Path

from rich.console import Console

TEMPLATES: dict[str, tuple[str, str]] = {
    "minimal-bundle": ("minimal-bundle/bundle.yaml", "minimal-bundle/aidp.config.yaml"),
    "minimal": ("minimal_gl_only.yaml", "aidp.config.example.yaml"),
    "full-finance": ("full_finance.yaml", "aidp.config.example.yaml"),
}


def init(template: str, *, force: bool, console: Console | None = None) -> int:
    """Copy templates into ./bundle.yaml and ./aidp.config.yaml.

    Returns process exit code (0 on success, 1 on collision without --force).
    """
    console = console or Console()
    if template not in TEMPLATES:
        console.print(f"[red]unknown template: {template}[/red]; pick one of {list(TEMPLATES)}")
        return 2

    bundle_target = Path("bundle.yaml")
    config_target = Path("aidp.config.yaml")

    if not force and (bundle_target.exists() or config_target.exists()):
        console.print(
            f"[red]existing files found:[/red] "
            f"{[p.name for p in (bundle_target, config_target) if p.exists()]}; "
            f"pass --force to overwrite."
        )
        return 1

    examples_dir = _examples_dir()
    bundle_source, config_source = TEMPLATES[template]
    shutil.copy(examples_dir / bundle_source, bundle_target)
    shutil.copy(examples_dir / config_source, config_target)

    console.print(f"[green]wrote[/green] {bundle_target}  ([dim]{bundle_source}[/dim])")
    console.print(f"[green]wrote[/green] {config_target}  ([dim]{config_source}[/dim])")
    console.print(
        "\n[bold]Next steps:[/bold]\n"
        "  1. Fill in [cyan]variables.team[/cyan] and the Fusion/OAC values in [cyan]bundle.yaml[/cyan]\n"
        "     (${FUSION_*}, ${OAC_URL}, schemas, and dataSourceName as needed).\n"
        "  2. Run [cyan]aidp-fusion-bundle init-config[/cyan] with the AIDP OCID plus workspace/cluster names\n"
        "     to write [cyan]workspaceKey, aiDataPlatformId, clusterKey, clusterName[/cyan] in [cyan]aidp.config.yaml[/cyan].\n"
        "  3. Run [cyan]aidp-fusion-bundle validate[/cyan] to schema-check the bundle.\n"
        "  4. Run [cyan]aidp-fusion-bundle dashboard mcp-setup[/cyan] before OAC workbook phases.\n"
        "  5. Run [cyan]aidp-fusion-bundle bootstrap[/cyan] to probe live prereqs.\n"
    )
    return 0


def _examples_dir() -> Path:
    """Locate the bundled examples directory.

    When installed via pip, examples ship as package data. For editable
    installs (and test runs), they're at ``../../../examples/`` relative to
    this file.
    """
    # Editable install: ../../../examples relative to this module
    here = Path(__file__).resolve()
    candidate = here.parent.parent.parent.parent / "examples"
    if candidate.exists():
        return candidate
    # Future: package-data fallback once pyproject.toml ships examples in the wheel.
    raise FileNotFoundError(f"examples directory not found at {candidate}")


__all__ = ["init", "TEMPLATES"]
