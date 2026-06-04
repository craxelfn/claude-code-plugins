"""Static content validators for content packs.

Distinct from the Pydantic schema validation in :mod:`schema.medallion_pack`:
these validators need access to the filesystem (SQL files), the assembled
pack (after overlay merge), and cross-references between packs and
dashboards.

References:
    * dev/PLAN_plugin_engine_medallion_content_packs.md §9.1 (SQL template variables)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §9.5 (variation points)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §11.3 (strategy validation)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §12 (dashboard pack contract)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §25 (error codes)

Validators implemented (one error code per failure mode):

    * :func:`validate_sql_paths` → AIDPF-2003
    * :func:`validate_template_variables` → AIDPF-5002, AIDPF-5003
    * :func:`validate_dag` → AIDPF-2040, AIDPF-2041
    * :func:`validate_dashboard_requires` → AIDPF-7001, AIDPF-7003

:func:`validate_pack_full` aggregates the above into a single
:class:`ValidationReport`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable

from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import ResolvedPack
from oracle_ai_data_platform_fusion_bundle.schema.dashboard_pack import DashboardYaml

# Error codes (registered in PLAN §25).
AIDPF_2003_SQL_FILE_MISSING = "AIDPF-2003"
AIDPF_2040_DAG_CYCLE = "AIDPF-2040"
AIDPF_2041_UNRESOLVED_DEPENDENCY = "AIDPF-2041"
AIDPF_5002_UNKNOWN_TEMPLATE_VAR = "AIDPF-5002"
AIDPF_5003_UNDECLARED_VARIATION_POINT = "AIDPF-5003"
AIDPF_7001_DASHBOARD_MISSING_NODE = "AIDPF-7001"
AIDPF_7003_DASHBOARD_TYPE_MISMATCH = "AIDPF-7003"


# ---------------------------------------------------------------------------
# Validation report dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ValidationError:
    code: str
    message: str
    location: str | None = None  # e.g., "silver/dim_supplier" or "dashboard/executive_cfo"


@dataclass
class ValidationReport:
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def extend(self, other: "ValidationReport") -> None:
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)

    def merge_errors(self, errors: Iterable[ValidationError]) -> None:
        self.errors.extend(errors)


# ---------------------------------------------------------------------------
# Allowlisted SQL template variables (PLAN §9.1)
# ---------------------------------------------------------------------------

_BASE_TEMPLATE_VARS = {
    "catalog",
    "bronze_schema",
    "silver_schema",
    "gold_schema",
    "run_id_literal",
    "watermark_predicate",
}

# `{{ profile.<key> }}` / `{{ column.<name> }}` / `{{ semantic.<name> }}`
# are parsed and the suffix is validated against pack content.
_NAMESPACED_PREFIXES = ("profile", "column", "semantic")

_TEMPLATE_TOKEN_RE = re.compile(r"\{\{\s*([^}\s]+(?:\.[^}\s]+)*)\s*\}\}")


# ---------------------------------------------------------------------------
# validate_sql_paths (AIDPF-2003)
# ---------------------------------------------------------------------------


def validate_sql_paths(pack: ResolvedPack) -> list[ValidationError]:
    """For every node with `implementation.type: sql`, confirm the SQL file exists."""
    errors: list[ValidationError] = []
    for layer_name, nodes in (("silver", pack.silver), ("gold", pack.gold)):
        for node_id, node in nodes.items():
            if node.implementation.type != "sql":
                continue
            sql_path = pack.root / node.implementation.sql
            if not sql_path.exists():
                errors.append(
                    ValidationError(
                        code=AIDPF_2003_SQL_FILE_MISSING,
                        message=(
                            f"{AIDPF_2003_SQL_FILE_MISSING}: node "
                            f"`{layer_name}/{node_id}` declares "
                            f"`implementation.sql: {node.implementation.sql}` but "
                            f"file does not exist at {sql_path}."
                        ),
                        location=f"{layer_name}/{node_id}",
                    )
                )
    return errors


# ---------------------------------------------------------------------------
# validate_template_variables (AIDPF-5002, AIDPF-5003)
# ---------------------------------------------------------------------------


def validate_template_variables(pack: ResolvedPack) -> list[ValidationError]:
    """Confirm every `{{ ... }}` token in pack SQL files is allowed and declared.

    Allowlisted tokens (PLAN §9.1):
        * Bare names in `_BASE_TEMPLATE_VARS`.
        * `profile.<key>` — resolved against `pack.profiles[<active>].<key>`
          at render time. Phase 1 doesn't check the inner key chain (depth
          can vary), only that the namespace exists in the pack.
        * `column.<name>` — must match a declared `columnAliases.<name>`.
        * `semantic.<name>` — must match a declared `semanticVariants.<name>`.
    """
    errors: list[ValidationError] = []
    declared_columns = set(pack.pack.column_aliases)
    declared_semantics = set(pack.pack.semantic_variants)
    has_profiles = bool(pack.pack.profiles)

    for layer_name, nodes in (("silver", pack.silver), ("gold", pack.gold)):
        for node_id, node in nodes.items():
            if node.implementation.type != "sql":
                continue
            sql_path = pack.root / node.implementation.sql
            if not sql_path.exists():
                # validate_sql_paths surfaces the AIDPF-2003 error; skip token
                # scanning to avoid noisy duplicate errors.
                continue
            content = sql_path.read_text(encoding="utf-8")
            tokens = _TEMPLATE_TOKEN_RE.findall(content)
            for token in tokens:
                parts = token.split(".")
                head = parts[0]
                if head in _BASE_TEMPLATE_VARS and len(parts) == 1:
                    continue
                if head == "profile":
                    if not has_profiles:
                        errors.append(
                            ValidationError(
                                code=AIDPF_5003_UNDECLARED_VARIATION_POINT,
                                message=(
                                    f"{AIDPF_5003_UNDECLARED_VARIATION_POINT}: "
                                    f"node `{layer_name}/{node_id}` references "
                                    f"`{{{{ {token} }}}}` but pack declares no profiles."
                                ),
                                location=f"{layer_name}/{node_id}",
                            )
                        )
                    continue
                if head == "column":
                    name = parts[1] if len(parts) > 1 else ""
                    if name not in declared_columns:
                        errors.append(
                            ValidationError(
                                code=AIDPF_5003_UNDECLARED_VARIATION_POINT,
                                message=(
                                    f"{AIDPF_5003_UNDECLARED_VARIATION_POINT}: "
                                    f"node `{layer_name}/{node_id}` references "
                                    f"`{{{{ {token} }}}}` but `columnAliases.{name}` "
                                    f"is not declared. Known: {sorted(declared_columns)!r}."
                                ),
                                location=f"{layer_name}/{node_id}",
                            )
                        )
                    continue
                if head == "semantic":
                    name = parts[1] if len(parts) > 1 else ""
                    if name not in declared_semantics:
                        errors.append(
                            ValidationError(
                                code=AIDPF_5003_UNDECLARED_VARIATION_POINT,
                                message=(
                                    f"{AIDPF_5003_UNDECLARED_VARIATION_POINT}: "
                                    f"node `{layer_name}/{node_id}` references "
                                    f"`{{{{ {token} }}}}` but `semanticVariants.{name}` "
                                    f"is not declared. Known: {sorted(declared_semantics)!r}."
                                ),
                                location=f"{layer_name}/{node_id}",
                            )
                        )
                    continue
                # Unknown top-level namespace.
                errors.append(
                    ValidationError(
                        code=AIDPF_5002_UNKNOWN_TEMPLATE_VAR,
                        message=(
                            f"{AIDPF_5002_UNKNOWN_TEMPLATE_VAR}: node "
                            f"`{layer_name}/{node_id}` references unknown "
                            f"template variable `{{{{ {token} }}}}`. "
                            f"Allowed: {sorted(_BASE_TEMPLATE_VARS) + ['profile.<key>', 'column.<name>', 'semantic.<name>']}."
                        ),
                        location=f"{layer_name}/{node_id}",
                    )
                )
    return errors


# ---------------------------------------------------------------------------
# validate_dag (AIDPF-2040, AIDPF-2041)
# ---------------------------------------------------------------------------


def validate_dag(pack: ResolvedPack) -> list[ValidationError]:
    """Confirm node `dependsOn` references resolve and form a DAG."""
    errors: list[ValidationError] = []

    # Build the set of declared source ids:
    #   - bronze datasets from bronze.yaml
    #   - silver nodes by id
    declared_bronze = set()
    for ds in pack.bronze_yaml.get("datasets", []) or []:
        if isinstance(ds, dict) and "id" in ds:
            declared_bronze.add(ds["id"])
    declared_silver = set(pack.silver)
    declared_gold = set(pack.gold)

    # Build adjacency: node -> set(node ids it depends on, restricted to
    # silver/gold-shape nodes for cycle detection — bronze deps are leaves).
    graph: dict[str, set[str]] = {}

    all_nodes = {
        f"silver/{nid}": node for nid, node in pack.silver.items()
    } | {f"gold/{nid}": node for nid, node in pack.gold.items()}

    for full_id, node in all_nodes.items():
        deps: set[str] = set()

        for src in node.depends_on.bronze:
            if src.id not in declared_bronze:
                errors.append(
                    ValidationError(
                        code=AIDPF_2041_UNRESOLVED_DEPENDENCY,
                        message=(
                            f"{AIDPF_2041_UNRESOLVED_DEPENDENCY}: node "
                            f"`{full_id}` depends on bronze `{src.id}` which "
                            f"is not declared in bronze.yaml. Known bronze "
                            f"datasets: {sorted(declared_bronze)!r}."
                        ),
                        location=full_id,
                    )
                )
        for src in node.depends_on.silver:
            if src.id not in declared_silver:
                errors.append(
                    ValidationError(
                        code=AIDPF_2041_UNRESOLVED_DEPENDENCY,
                        message=(
                            f"{AIDPF_2041_UNRESOLVED_DEPENDENCY}: node "
                            f"`{full_id}` depends on silver `{src.id}` which "
                            f"is not a declared silver node. Known: "
                            f"{sorted(declared_silver)!r}."
                        ),
                        location=full_id,
                    )
                )
                continue
            # Map silver dependency id to its full qualified name.
            deps.add(f"silver/{src.id}")
        graph[full_id] = deps

    # Cycle detection via DFS.
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {n: WHITE for n in graph}

    def dfs(node: str, path: list[str]) -> None:
        color[node] = GRAY
        path.append(node)
        for nxt in graph.get(node, set()):
            if color.get(nxt, WHITE) == GRAY:
                cycle_repr = " -> ".join(path[path.index(nxt):] + [nxt])
                errors.append(
                    ValidationError(
                        code=AIDPF_2040_DAG_CYCLE,
                        message=(
                            f"{AIDPF_2040_DAG_CYCLE}: dependency cycle in pack "
                            f"DAG: {cycle_repr}."
                        ),
                        location=node,
                    )
                )
                return
            if color.get(nxt, WHITE) == WHITE:
                dfs(nxt, path)
        path.pop()
        color[node] = BLACK

    for node in graph:
        if color[node] == WHITE:
            dfs(node, [])

    return errors


# ---------------------------------------------------------------------------
# validate_dashboard_requires (AIDPF-7001, AIDPF-7003)
# ---------------------------------------------------------------------------


def validate_dashboard_requires(
    pack: ResolvedPack, dashboard: DashboardYaml
) -> list[ValidationError]:
    """Confirm dashboard requires.{tables,columns} resolve against pack gold nodes."""
    errors: list[ValidationError] = []

    # Build a lookup from `gold.<table>` -> gold node.
    gold_by_target = {f"gold.{node.target}": node for node in pack.gold.values()}

    for table_ref in dashboard.requires.tables:
        if table_ref not in gold_by_target:
            errors.append(
                ValidationError(
                    code=AIDPF_7001_DASHBOARD_MISSING_NODE,
                    message=(
                        f"{AIDPF_7001_DASHBOARD_MISSING_NODE}: dashboard "
                        f"`{dashboard.id}` requires `{table_ref}` which is not "
                        f"declared as a gold node in the pack. Known gold "
                        f"tables: {sorted(gold_by_target)!r}."
                    ),
                    location=f"dashboard/{dashboard.id}",
                )
            )

    for table_ref, columns in dashboard.requires.columns.items():
        if table_ref not in gold_by_target:
            # Already reported above; skip column-level checks.
            continue
        node = gold_by_target[table_ref]
        node_columns_by_name = {c.name: c for c in node.output_schema.columns}
        for required_col in columns:
            if required_col.name not in node_columns_by_name:
                errors.append(
                    ValidationError(
                        code=AIDPF_7001_DASHBOARD_MISSING_NODE,
                        message=(
                            f"{AIDPF_7001_DASHBOARD_MISSING_NODE}: dashboard "
                            f"`{dashboard.id}` requires column `{required_col.name}` "
                            f"on `{table_ref}` which is not in the gold "
                            f"node's `outputSchema.columns`."
                        ),
                        location=f"dashboard/{dashboard.id}",
                    )
                )
                continue
            actual = node_columns_by_name[required_col.name]
            if actual.type != required_col.type:
                errors.append(
                    ValidationError(
                        code=AIDPF_7003_DASHBOARD_TYPE_MISMATCH,
                        message=(
                            f"{AIDPF_7003_DASHBOARD_TYPE_MISMATCH}: dashboard "
                            f"`{dashboard.id}` requires `{table_ref}.{required_col.name}: "
                            f"{required_col.type}` but the gold node declares "
                            f"`{actual.type}`."
                        ),
                        location=f"dashboard/{dashboard.id}",
                    )
                )

    return errors


# ---------------------------------------------------------------------------
# validate_pack_full
# ---------------------------------------------------------------------------


def validate_pack_full(pack: ResolvedPack) -> ValidationReport:
    """Run every validator over the assembled pack; aggregate into a report."""
    report = ValidationReport()
    report.merge_errors(validate_sql_paths(pack))
    report.merge_errors(validate_template_variables(pack))
    report.merge_errors(validate_dag(pack))
    for dashboard in pack.dashboards.values():
        report.merge_errors(validate_dashboard_requires(pack, dashboard))
    return report
