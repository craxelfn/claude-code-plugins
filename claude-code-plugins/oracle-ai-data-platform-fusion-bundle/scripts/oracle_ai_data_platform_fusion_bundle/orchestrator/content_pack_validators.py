"""Static content validators for content packs.

Distinct from the Pydantic schema validation in :mod:`schema.medallion_pack`:
these validators need access to the filesystem (SQL files), the assembled
pack after overlay merge, and cross-references between packs and dashboards.
Operator-facing behavior is documented in ``docs/content_pack_execution.md``.

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

# Error codes surfaced by content-pack validation.
AIDPF_2003_SQL_FILE_MISSING = "AIDPF-2003"
AIDPF_2040_DAG_CYCLE = "AIDPF-2040"
AIDPF_2041_UNRESOLVED_DEPENDENCY = "AIDPF-2041"
AIDPF_5002_UNKNOWN_TEMPLATE_VAR = "AIDPF-5002"
AIDPF_5003_UNDECLARED_VARIATION_POINT = "AIDPF-5003"
AIDPF_7001_DASHBOARD_MISSING_NODE = "AIDPF-7001"
AIDPF_7003_DASHBOARD_TYPE_MISMATCH = "AIDPF-7003"
AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE = "AIDPF-7004"
AIDPF_7005_ALLOWED_COLUMNS_NOT_REQUIRED = "AIDPF-7005"
AIDPF_8002_PII_HIGH_DASHBOARD_EXPOSURE = "AIDPF-8002"

# Bronze extract nodes are validated against the Fusion catalog when possible.
AIDPF_2080_BRONZE_EXTRACT_PVO_NOT_IN_CATALOG = "AIDPF-2080"


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
# Allowlisted SQL template variables.
# ---------------------------------------------------------------------------

_BASE_TEMPLATE_VARS = {
    "catalog",
    "bronze_schema",
    "silver_schema",
    "gold_schema",
    "run_id_literal",
    "watermark_predicate",
    "snapshot_date",  # Dedicated ISO-date token (AIDPF-5013).
}

# `{{ profile.<key> }}` / `{{ column.<name> }}` / `{{ semantic.<name> }}`
# are parsed and the suffix is validated against pack content.
_NAMESPACED_PREFIXES = ("profile", "column", "semantic")

_TEMPLATE_TOKEN_RE = re.compile(r"\{\{\s*([^}\s]+(?:\.[^}\s]+)*)\s*\}\}")


# ---------------------------------------------------------------------------
# validate_sql_paths (AIDPF-2003)
# ---------------------------------------------------------------------------


def validate_sql_paths(pack: ResolvedPack) -> list[ValidationError]:
    """For every node with `implementation.type: sql`, confirm the SQL file exists.

    Uses ``pack.root_for(qualified_id)`` so inherited base nodes resolve against
    the base pack's root (not the overlay's), and overlay-overridden / overlay-
    added nodes resolve against the overlay's root.
    """
    errors: list[ValidationError] = []
    for layer_name, nodes in (("silver", pack.silver), ("gold", pack.gold)):
        for node_id, node in nodes.items():
            if node.implementation.type != "sql":
                continue
            qualified = f"{layer_name}/{node_id}"
            sql_path = pack.root_for(qualified) / node.implementation.sql
            if not sql_path.exists():
                errors.append(
                    ValidationError(
                        code=AIDPF_2003_SQL_FILE_MISSING,
                        message=(
                            f"{AIDPF_2003_SQL_FILE_MISSING}: node "
                            f"`{qualified}` declares "
                            f"`implementation.sql: {node.implementation.sql}` but "
                            f"file does not exist at {sql_path}."
                        ),
                        location=qualified,
                    )
                )
    return errors


# ---------------------------------------------------------------------------
# validate_template_variables (AIDPF-5002, AIDPF-5003)
# ---------------------------------------------------------------------------


def validate_template_variables(pack: ResolvedPack) -> list[ValidationError]:
    """Confirm every `{{ ... }}` token in pack SQL files is allowed and declared.

    Allowlisted tokens:
        * Bare names in `_BASE_TEMPLATE_VARS`.
        * `profile.<key>` — resolved against `pack.profiles[<active>].<key>`
          at render time. This validator checks only that the namespace exists
          in the pack because profile key depth can vary.
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
            qualified = f"{layer_name}/{node_id}"
            sql_path = pack.root_for(qualified) / node.implementation.sql
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
                                    f"node `{qualified}` references "
                                    f"`{{{{ {token} }}}}` but pack declares no profiles."
                                ),
                                location=qualified,
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
                                    f"node `{qualified}` references "
                                    f"`{{{{ {token} }}}}` but `columnAliases.{name}` "
                                    f"is not declared. Known: {sorted(declared_columns)!r}."
                                ),
                                location=qualified,
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
                                    f"node `{qualified}` references "
                                    f"`{{{{ {token} }}}}` but `semanticVariants.{name}` "
                                    f"is not declared. Known: {sorted(declared_semantics)!r}."
                                ),
                                location=qualified,
                            )
                        )
                    continue
                # Unknown top-level namespace.
                errors.append(
                    ValidationError(
                        code=AIDPF_5002_UNKNOWN_TEMPLATE_VAR,
                        message=(
                            f"{AIDPF_5002_UNKNOWN_TEMPLATE_VAR}: node "
                            f"`{qualified}` references unknown "
                            f"template variable `{{{{ {token} }}}}`. "
                            f"Allowed: {sorted(_BASE_TEMPLATE_VARS) + ['profile.<key>', 'column.<name>', 'semantic.<name>']}."
                        ),
                        location=qualified,
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
    declared_bronze: set[str] = set()
    # Per-file pack.bronze is the source of truth; legacy pack.bronze_yaml is
    # retained for backwards compatibility.
    declared_bronze.update(pack.bronze.keys())
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

    # Tables that appear in requires.tables were checked in the loop above.
    # Tables that appear in requires.columns must ALSO resolve to a gold node;
    # a typo present only in requires.columns (e.g. `gold.gl_balnace` instead
    # of `gold.gl_balance`) would otherwise slip through silently.
    required_tables_set = set(dashboard.requires.tables)
    for table_ref, columns in dashboard.requires.columns.items():
        if table_ref not in gold_by_target:
            # Distinguish two flavours of failure for clearer remediation:
            #   * Already reported via requires.tables loop above → silent skip
            #     would be acceptable, but we still want the dashboard author
            #     to see one error per failing table so we report it again here.
            #   * Table only in requires.columns (typo / forgot requires.tables):
            #     must surface explicitly; otherwise the rest of the column
            #     checks and the PII firewall skip the table altogether.
            already_reported = table_ref in required_tables_set
            extra_hint = (
                "" if already_reported else
                " (this table is referenced only by `requires.columns`; "
                "every column-table key must also appear in `requires.tables` "
                "AND resolve to a declared gold node)"
            )
            errors.append(
                ValidationError(
                    code=AIDPF_7001_DASHBOARD_MISSING_NODE,
                    message=(
                        f"{AIDPF_7001_DASHBOARD_MISSING_NODE}: dashboard "
                        f"`{dashboard.id}` references `{table_ref}` in "
                        f"`requires.columns` which is not declared as a gold "
                        f"node in the pack{extra_hint}. Known gold tables: "
                        f"{sorted(gold_by_target)!r}."
                    ),
                    location=f"dashboard/{dashboard.id}",
                )
            )
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
# validate_dashboard_security_and_compat (AIDPF-7004, AIDPF-7005, AIDPF-8002)
# ---------------------------------------------------------------------------


def _semver_tuple(v: str) -> tuple[int, ...]:
    """Best-effort SemVer to comparable tuple. Ignores pre-release / build."""
    core = v.split("-")[0].split("+")[0]
    try:
        return tuple(int(p) for p in core.split("."))
    except ValueError:
        return ()


def validate_dashboard_security_and_compat(
    pack: ResolvedPack, dashboard: DashboardYaml
) -> list[ValidationError]:
    """Pack-version compatibility + PII firewall + allowedColumns subset check.

    Three rules:

    * **AIDPF-7004** — ``requires.pack.id`` must equal ``pack.pack.id``; if
      ``requires.pack.minVersion`` is set, ``pack.pack.version`` must be
      >= it; if ``requires.pack.maxVersion`` is set, ``pack.pack.version``
      must be <= it.
    * **AIDPF-7005** — every entry in ``security.allowedColumns[table]``
      must already appear in ``requires.columns[table]`` for the same
      table. Prevents "I allow X for display but never required it" drift.
    * **AIDPF-8002** — any column in ``requires.columns`` OR
      ``security.allowedColumns`` whose gold ``outputSchema`` declares
      ``pii: high`` is rejected. High-PII columns must not be reachable
      via OAC dataset/RPD.
    """
    errors: list[ValidationError] = []
    where = f"dashboard/{dashboard.id}"

    # --- AIDPF-7004: pack compatibility ----------------------------------
    req_pack = dashboard.requires.pack
    if req_pack.id != pack.pack.id:
        errors.append(
            ValidationError(
                code=AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE,
                message=(
                    f"{AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE}: dashboard "
                    f"`{dashboard.id}` requires pack `{req_pack.id}` but "
                    f"active pack is `{pack.pack.id}`."
                ),
                location=where,
            )
        )
    else:
        pack_v = _semver_tuple(pack.pack.version)
        min_v = _semver_tuple(req_pack.min_version) if req_pack.min_version else None
        max_v = _semver_tuple(req_pack.max_version) if req_pack.max_version else None
        if min_v and pack_v and pack_v < min_v:
            errors.append(
                ValidationError(
                    code=AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE,
                    message=(
                        f"{AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE}: dashboard "
                        f"`{dashboard.id}` requires pack `{req_pack.id}` "
                        f">= {req_pack.min_version} but active pack is "
                        f"{pack.pack.version}."
                    ),
                    location=where,
                )
            )
        if max_v and pack_v and pack_v > max_v:
            errors.append(
                ValidationError(
                    code=AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE,
                    message=(
                        f"{AIDPF_7004_DASHBOARD_PACK_INCOMPATIBLE}: dashboard "
                        f"`{dashboard.id}` requires pack `{req_pack.id}` "
                        f"<= {req_pack.max_version} but active pack is "
                        f"{pack.pack.version}."
                    ),
                    location=where,
                )
            )

    # Look up gold nodes keyed by qualified `gold.<target>` for column ↔ PII checks.
    gold_by_target = {f"gold.{node.target}": node for node in pack.gold.values()}

    # --- AIDPF-7005: allowed_columns ⊆ requires.columns ------------------
    required_cols_by_table: dict[str, set[str]] = {
        table: {c.name for c in cols}
        for table, cols in dashboard.requires.columns.items()
    }
    for table, allowed_names in dashboard.security.allowed_columns.items():
        required_set = required_cols_by_table.get(table, set())
        unrequired = [name for name in allowed_names if name not in required_set]
        if unrequired:
            errors.append(
                ValidationError(
                    code=AIDPF_7005_ALLOWED_COLUMNS_NOT_REQUIRED,
                    message=(
                        f"{AIDPF_7005_ALLOWED_COLUMNS_NOT_REQUIRED}: dashboard "
                        f"`{dashboard.id}` declares `allowedColumns[{table}]` "
                        f"entries that are not present in `requires.columns[{table}]`: "
                        f"{sorted(unrequired)!r}."
                    ),
                    location=where,
                )
            )

    # --- AIDPF-8002: PII high firewall -----------------------------------
    # Collect every (table, column) pair the dashboard references via
    # requires.columns OR security.allowedColumns, then check the gold
    # node's outputSchema for pii=='high' on each.
    references: dict[str, set[str]] = {}
    for table, cols in dashboard.requires.columns.items():
        references.setdefault(table, set()).update(c.name for c in cols)
    for table, allowed_names in dashboard.security.allowed_columns.items():
        references.setdefault(table, set()).update(allowed_names)

    for table, col_names in references.items():
        node = gold_by_target.get(table)
        if node is None:
            # validate_dashboard_requires reports the missing-table error;
            # don't surface a duplicate PII complaint here.
            continue
        cols_by_name = {c.name: c for c in node.output_schema.columns}
        for col_name in col_names:
            col = cols_by_name.get(col_name)
            if col is None:
                # Missing column already reported by validate_dashboard_requires.
                continue
            if col.pii == "high":
                errors.append(
                    ValidationError(
                        code=AIDPF_8002_PII_HIGH_DASHBOARD_EXPOSURE,
                        message=(
                            f"{AIDPF_8002_PII_HIGH_DASHBOARD_EXPOSURE}: dashboard "
                            f"`{dashboard.id}` references `{table}.{col_name}` "
                            f"which is declared `pii: high` in the gold node's "
                            f"`outputSchema`. High-PII columns must not be "
                            f"reachable via OAC dataset/RPD. "
                            f"Remove from `requires.columns` / `allowedColumns` "
                            f"or downgrade the column's pii classification."
                        ),
                        location=where,
                    )
                )

    return errors


# ---------------------------------------------------------------------------
# validate_pack_full
# ---------------------------------------------------------------------------


def validate_bronze_pvo_catalog(pack: ResolvedPack) -> list[ValidationError]:
    """WARN when a bronze_extract node's ``pvo_id`` is not in the catalog.

    WARN-only: pack loads cleanly; the BICC drift gate (``AIDPF-2072``)
    catches typo'd PVOs at extract-preflight time. This preserves the customer
    extension story: customers can author overlay-pack YAMLs for new PVOs
    without a plugin release.

    Missing ``pvo_id`` entirely produces NO WARN — there is nothing to
    cross-reference.
    """
    from ..schema.fusion_catalog import CATALOG

    warnings: list[ValidationError] = []
    curated_pvo_ids = {entry.datastore for entry in CATALOG.values()}
    for node_id, node in pack.bronze.items():
        impl = node.implementation
        if impl.type != "bronze_extract":
            continue
        pvo_id = getattr(impl, "pvo_id", None)
        if pvo_id is None:
            continue
        # Cross-reference against either the curated PvoEntry.datastore
        # (full AM-hierarchy) or the curated id keys themselves.
        if pvo_id in curated_pvo_ids or pvo_id in CATALOG:
            continue
        warnings.append(
            ValidationError(
                code=AIDPF_2080_BRONZE_EXTRACT_PVO_NOT_IN_CATALOG,
                message=(
                    f"{AIDPF_2080_BRONZE_EXTRACT_PVO_NOT_IN_CATALOG}: bronze "
                    f"node `bronze/{node_id}` references pvo_id "
                    f"{pvo_id!r} which is not in the curated fusion_catalog. "
                    f"Pack loads cleanly; the BICC drift gate "
                    f"(AIDPF-2072) catches typos at extract-preflight time. "
                    f"Customer overlay packs commonly hit this WARN."
                ),
                location=f"bronze/{node_id}",
            )
        )
    return warnings


def validate_pack_full(pack: ResolvedPack) -> ValidationReport:
    """Run every validator over the assembled pack; aggregate into a report."""
    report = ValidationReport()
    report.merge_errors(validate_sql_paths(pack))
    report.merge_errors(validate_template_variables(pack))
    report.merge_errors(validate_dag(pack))
    # AIDPF-2080 is WARN-only.
    report.warnings.extend(validate_bronze_pvo_catalog(pack))
    for dashboard in pack.dashboards.values():
        report.merge_errors(validate_dashboard_requires(pack, dashboard))
        report.merge_errors(validate_dashboard_security_and_compat(pack, dashboard))
    return report
