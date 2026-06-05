"""Pydantic models for content pack YAML (pack.yaml + silver/gold node YAML).

This module is the schema half of the v2 content-pack contract. It defines
the typed representation that ``aidp-fusion-bundle content-pack validate``
parses and the engine consumes at run time.

References:
    * dev/PLAN_plugin_engine_medallion_content_packs.md §8 (pack contract)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §9.5 (variation points)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §11.3 (strategy validation)
    * dev/PLAN_plugin_engine_medallion_content_packs.md §25 (error codes)

Error codes raised here are registered in PLAN §25. v0.3 codes used in this
module's top-level models:

    * AIDPF-2002 -- pack version not SemVer-valid

Node-level validation rules (R1-R13) and their codes are implemented in the
``NodeYaml`` model and validators (Step 3 of v2-phase-1-content-pack-schema).

State-table migration note (informational, not implemented here): the
state-table additive migration for pack / profile / tenant / source-level
cursor columns is declared by the schema (per ADR-0018, PLAN §11.9) but
the engine's runtime path does not yet write the new columns. Phase 2 of
the v2 migration consumes the schema; Phase 4 (parity gate) wires the
columns through the runtime.
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Literal

from .incremental_impact import IncrementalImpact

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

# ---------------------------------------------------------------------------
# Error code constants
# ---------------------------------------------------------------------------
# Centralised here so module-level validators reference symbols rather than
# bare strings. The authoritative registry lives in PLAN §25.

AIDPF_2002_INVALID_SEMVER = "AIDPF-2002"
AIDPF_2001_ORPHAN_OVERRIDE = "AIDPF-2001"  # used by overlay merger (Step 5)

# Node-level validation rule codes (PLAN §11.3 R1-R13).
AIDPF_2020_MERGE_NO_NATURAL_KEY = "AIDPF-2020"      # R1
AIDPF_2030_OUTPUT_SCHEMA_NO_PII = "AIDPF-2030"      # R12
AIDPF_2050_MERGE_NO_WATERMARK = "AIDPF-2050"        # R2
AIDPF_2051_MERGE_ZERO_PRIMARY = "AIDPF-2051"        # R3
AIDPF_2052_MERGE_MULTI_PRIMARY = "AIDPF-2052"       # R4
AIDPF_2053_MERGE_MULTI_BRONZE_NO_ROLE = "AIDPF-2053"  # R5
AIDPF_2054_REPLACE_PARTITION_NO_COLUMNS = "AIDPF-2054"  # R6
AIDPF_2055_REPLACE_PARTITION_MULTI_PRIMARY = "AIDPF-2055"  # R7
AIDPF_2056_APPEND_UNIQUE_NO_KEY = "AIDPF-2056"      # R8
AIDPF_2057_AGGREGATE_MERGE_DEFERRED = "AIDPF-2057"  # R9
AIDPF_2058_SNAPSHOT_NO_UNIQUE_TEST = "AIDPF-2058"   # R10
AIDPF_2059_SCD2_NO_TRACKED_COLUMNS = "AIDPF-2059"   # R11
AIDPF_2060_PYTHON_LEGACY_NO_DEPRECATED = "AIDPF-2060"  # R13


# ---------------------------------------------------------------------------
# SemVer validation
# ---------------------------------------------------------------------------
# Canonical SemVer 2.0.0 regex (https://semver.org/). Accepts:
#   1.0.0
#   0.1.0-alpha.1
#   1.2.3-rc.1+build.42
# Rejects bare numbers, leading zeros, trailing whitespace.

_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)


def _validate_semver(value: str) -> str:
    """Validate a SemVer string; raise ValueError with AIDPF-2002 on failure."""
    if not isinstance(value, str) or not _SEMVER_RE.match(value):
        raise ValueError(
            f"{AIDPF_2002_INVALID_SEMVER}: pack version not SemVer-valid: {value!r}. "
            f"Use a SemVer string in pack.yaml's `version:` field (e.g., `0.1.0`)."
        )
    return value


SemVerStr = Annotated[str, Field(description="SemVer 2.0.0 version string (e.g., 0.1.0)")]


# ---------------------------------------------------------------------------
# Identity, compatibility, defaults
# ---------------------------------------------------------------------------


class PackCompatibilityAidp(BaseModel):
    """AIDP runtime capability requirements declared by the pack."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    requires_delta: bool = Field(default=True, alias="requiresDelta")
    """Whether the pack requires Delta Lake tables. v0.3 packs always do."""


class PackCompatibility(BaseModel):
    """Minimum environment requirements declared by the pack."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    plugin_min_version: SemVerStr = Field(alias="pluginMinVersion")
    """Minimum installed plugin version that satisfies this pack."""

    fusion_families: list[Literal["ERP", "HCM", "SCM"]] = Field(
        default_factory=list, alias="fusionFamilies"
    )
    """Fusion module families this pack targets (informational)."""

    aidp: PackCompatibilityAidp = Field(default_factory=PackCompatibilityAidp)

    @field_validator("plugin_min_version")
    @classmethod
    def _check_plugin_min_version(cls, v: str) -> str:
        return _validate_semver(v)


class RunIdColumnDefaults(BaseModel):
    """Audit column names per medallion layer (per PLAN §8.1)."""

    model_config = ConfigDict(extra="forbid")

    bronze: str = "_run_id"
    silver: str = "silver_run_id"
    gold: str = "gold_run_id"


class PackDefaults(BaseModel):
    """Pack-wide defaults applied to every node unless overridden."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    sql_dialect: Literal["spark-sql"] = Field(default="spark-sql", alias="sqlDialect")
    """SQL flavour the templates target. v0.3 supports spark-sql only."""

    identifier_policy: Literal["validated-three-part"] = Field(
        default="validated-three-part", alias="identifierPolicy"
    )
    """Identifier validation policy for `{{ catalog }}.{{ schema }}.<table>` substitutions."""

    run_id_column: RunIdColumnDefaults = Field(
        default_factory=RunIdColumnDefaults, alias="runIdColumn"
    )


# ---------------------------------------------------------------------------
# Tenant profile defaults (per PLAN §8.1)
# ---------------------------------------------------------------------------


class CalendarProfile(BaseModel):
    """Default calendar dimensions for the dim_calendar builtin (ADR-0011)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    start_date: str = Field(alias="startDate")
    """ISO-8601 date (YYYY-MM-DD)."""

    end_date: str = Field(alias="endDate")

    fiscal_start_month: int = Field(default=1, alias="fiscalStartMonth", ge=1, le=12)


class ChartOfAccountsProfile(BaseModel):
    """Default COA segment role mapping. Overridden per tenant in profiles/<tenant>.yaml."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    balancing_segment: str = Field(alias="balancingSegment")
    cost_center_segment: str = Field(alias="costCenterSegment")
    natural_account_segment: str = Field(alias="naturalAccountSegment")


class PackProfileDefaults(BaseModel):
    """One named profile's defaults declared by the pack.

    The pack ships sensible defaults; tenant profiles (``profiles/<tenant>.yaml``)
    override per-key during bootstrap. Schema is intentionally extensible --
    arbitrary typed knobs can be added under nested dicts.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    calendar: CalendarProfile | None = None
    chart_of_accounts: ChartOfAccountsProfile | None = Field(
        default=None, alias="chartOfAccounts"
    )


# ---------------------------------------------------------------------------
# Variation points (PLAN §9.5)
# ---------------------------------------------------------------------------


class ColumnAlias(BaseModel):
    """Same logical column, different physical names across tenants.

    PLAN §9.5.1. Bootstrap walks ``candidates`` in priority order and pins
    the first that exists on the tenant.
    """

    model_config = ConfigDict(extra="forbid")

    appliesTo: str
    """Fully-qualified bronze table this variation point applies to (e.g. `bronze.ap_invoices`)."""

    required: bool = True
    """If true and zero candidates match, bootstrap fails with AIDPF-2010."""

    candidates: list[str] = Field(min_length=1)
    """Priority-ordered list of physical column names. May include the literal `inherit`
    in overlay packs to extend the base pack's candidates (resolved at overlay merge)."""


class SemanticVariantDetect(BaseModel):
    """How to recognise that this candidate applies to a tenant."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    column_exists: str = Field(alias="columnExists")
    """Column that must exist on the tenant for this candidate to match."""


class SemanticVariantCandidate(BaseModel):
    """One semantic-shape candidate (PLAN §9.5.1 / §9.5.2)."""

    model_config = ConfigDict(extra="forbid")

    id: str
    """Stable identifier for this candidate (e.g., `cancelled_date`, `cancelled_flag`)."""

    detect: SemanticVariantDetect

    fragment: str
    """SQL boolean fragment substituted into `{{ semantic.<name> }}`.
    Must conform to the semantic-fragment grammar (PLAN §9.5.2)."""


class SemanticVariant(BaseModel):
    """Same logical concept, different SQL **shape** across tenants (PLAN §9.5.1)."""

    model_config = ConfigDict(extra="forbid")

    appliesTo: str
    required: bool = True
    candidates: list[SemanticVariantCandidate] = Field(min_length=1)


# ---------------------------------------------------------------------------
# Overlay reference + override entries
# ---------------------------------------------------------------------------


_OVERLAY_REF_RE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*?)@([^\s]+)$")


class PackOverlayRef(BaseModel):
    """Parsed ``extends: <pack-id>@<semver>`` reference.

    Constructed from a plain string by ``PackYaml.extends``; serialises back
    to the same string form. Provides typed access to ``name`` and ``version``
    for the overlay merger.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    version: SemVerStr

    @classmethod
    def parse(cls, raw: str) -> "PackOverlayRef":
        m = _OVERLAY_REF_RE.match(raw or "")
        if not m:
            raise ValueError(
                f"{AIDPF_2002_INVALID_SEMVER}: `extends:` must be in the form "
                f"`<pack-id>@<semver>`; got {raw!r}."
            )
        name, version = m.group(1), m.group(2)
        _validate_semver(version)  # may raise AIDPF-2002
        return cls(name=name, version=version)

    def to_string(self) -> str:
        return f"{self.name}@{self.version}"


class OverrideEntry(BaseModel):
    """Per-node override declared by an overlay pack.

    PLAN §8.7 merge rules:

    * ``profile:`` -- scalar replace.
    * ``sql:`` -- full-file replace; the named SQL path lives in the overlay.
    * ``quality:`` -- nested ``tests:`` list extends base.
    * ``extendColumns: true`` -- the overlay extends the base node's
      ``outputSchema.columns`` rather than replacing it (Phase-1 stubs use
      this for ``python_legacy`` migration-period stubs).

    Unknown keys default to scalar-replace per §8.7.
    """

    model_config = ConfigDict(extra="allow")

    profile: str | None = None
    sql: str | None = None
    quality: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Provenance (ADR-0019)
# ---------------------------------------------------------------------------


class SkillProposalRecord(BaseModel):
    """One per-VP proposal the medallion-author skill captured at
    overlay-draft time (Phase 3b).

    Bootstrap reads ``candidate_added`` to detect AutoResolved-on-skill-
    proposed-candidate at commit time (Phase 3b round-2 finding —
    initial-onboarding flow must record ``mechanism: skill_proposed``
    when the walker AutoResolves on a candidate the skill added).
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    candidate_added: str = Field(alias="candidateAdded")
    """The candidate value the skill appended to the VP's list (e.g.
    ``ApInvoicesXCurrCode``)."""

    confidence: str | None = None
    """``high`` | ``medium`` | ``low`` — LLM's confidence in the
    proposal. Optional for audit only."""

    reasoning: str | None = None
    """Operator-facing rationale paragraph from the propose phase."""


class PackProvenance(BaseModel):
    """Optional provenance block stamped by the medallion-author skill (ADR-0019).

    Skill-authored packs (overlays, in particular) record the skill version,
    model identity, generation timestamp, and reason. Hand-authored packs may
    omit this block entirely.

    Phase 3b extends the schema with skill-specific fields
    (``skill_id``, ``diagnostic_run_id``, ``proposals``,
    ``incremental_impact``). **Every new field declares an explicit
    camelCase ``Field(alias=...)``** so overlay YAML's camelCase keys
    parse — round-3 plan-review finding (without aliases the bootstrap
    detection silently never fires).
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    generated_by: str | None = Field(default=None, alias="generatedBy")
    skill_version: str | None = Field(default=None, alias="skillVersion")
    model_id: str | None = Field(default=None, alias="modelId")
    generated_at: str | None = Field(default=None, alias="generatedAt")
    reason: str | None = None
    evidence: dict[str, Any] | None = None

    # --- Phase 3b extensions (additive, default-None) ---

    skill_id: str | None = Field(default=None, alias="skillId")
    """Stable identifier of the skill that drafted the overlay. The
    medallion-author skill stamps ``aidp-fusion-medallion-author``;
    bootstrap detects skill-authored overlays via this field and
    records ``mechanism: skill_proposed`` on the resolutions they
    drive."""

    diagnostic_run_id: str | None = Field(default=None, alias="diagnosticRunId")
    """The bootstrap run_id whose diagnostic artifacts triggered the
    skill invocation. Threads the audit trail from failure → draft →
    commit."""

    proposals: dict[str, SkillProposalRecord] | None = Field(
        default=None, alias="proposals"
    )
    """Per-VP candidate proposals the skill captured at draft time.
    Keyed by VP name (e.g. ``invoice_currency_code``). Bootstrap
    reads this to detect AutoResolved-on-skill-added-candidate at
    commit time."""

    incremental_impact: dict[str, IncrementalImpact] | None = Field(
        default=None, alias="incrementalImpact"
    )
    """Per-VP impact analysis (change kind, risk label, affected
    nodes, remediation choice). Bootstrap mirrors this into the
    per-resolution evidence snapshot on commit. See
    :class:`schema.incremental_impact.IncrementalImpact`."""


# ---------------------------------------------------------------------------
# Top-level pack.yaml
# ---------------------------------------------------------------------------


class PackYaml(BaseModel):
    """Top-level schema for ``pack.yaml``.

    Node definitions live in separate per-node YAML files under
    ``silver/`` and ``gold/`` and are validated by :class:`NodeYaml`
    (Step 3 of v2-phase-1-content-pack-schema -- not yet implemented).
    ``PackYaml`` covers pack identity, compatibility, defaults, profile
    defaults, variation-point declarations, overlay coordination, and
    provenance.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    # Identity
    id: str
    """Stable pack identifier (e.g., ``fusion-finance-starter``).
    Dashes are allowed and conventional; the Python package data directory
    uses an underscore equivalent (``content_packs/<id-with-underscores>/``)."""

    version: SemVerStr
    description: str | None = None

    # Compatibility constraints
    compatibility: PackCompatibility

    # Pack-wide defaults
    defaults: PackDefaults = Field(default_factory=PackDefaults)

    # Tenant-customisation knobs
    profiles: dict[str, PackProfileDefaults] = Field(default_factory=dict)

    # Variation points (§9.5)
    column_aliases: dict[str, ColumnAlias] = Field(
        default_factory=dict, alias="columnAliases"
    )
    semantic_variants: dict[str, SemanticVariant] = Field(
        default_factory=dict, alias="semanticVariants"
    )

    # Overlay coordination
    extends: str | None = None
    """For overlay packs, the base pack reference in the form ``<id>@<version>``.
    ``None`` for base packs themselves."""

    overrides: dict[str, OverrideEntry] = Field(default_factory=dict)
    """For overlay packs: per-node-id override entries. Empty for base packs."""

    # Provenance (skill-authored packs)
    provenance: PackProvenance | None = None

    @field_validator("version")
    @classmethod
    def _check_version(cls, v: str) -> str:
        return _validate_semver(v)

    @field_validator("extends")
    @classmethod
    def _check_extends_syntax(cls, v: str | None) -> str | None:
        if v is None:
            return None
        # Parse and validate; discard the result (we store the string form).
        PackOverlayRef.parse(v)
        return v

    @model_validator(mode="after")
    def _base_packs_have_no_overrides(self) -> "PackYaml":
        """A base pack (``extends: null``) must not have ``overrides:`` content.

        Overrides are an overlay-only concept; populating them on a base pack
        is a logic error in pack authoring.
        """
        if self.extends is None and self.overrides:
            raise ValueError(
                f"{AIDPF_2001_ORPHAN_OVERRIDE}: base packs (with no `extends:`) "
                f"must not declare `overrides:`. Found overrides for: "
                f"{sorted(self.overrides.keys())!r}."
            )
        return self

    def parsed_extends(self) -> PackOverlayRef | None:
        """Return the parsed ``extends:`` reference, or ``None`` for base packs."""
        return PackOverlayRef.parse(self.extends) if self.extends else None


# ---------------------------------------------------------------------------
# Node-level models (silver / gold YAML per-node files)
# ---------------------------------------------------------------------------


# Dependency source reference (one entry in dependsOn.bronze or dependsOn.silver).

PiiLevel = Literal["high", "medium", "low", "none"]
Role = Literal["primary", "lookup"]
SeedStrategy = Literal["replace", "merge", "append", "replace_partition", "custom"]
IncrementalStrategy = Literal[
    "replace",
    "merge",
    "append",
    "replace_partition",
    "custom",
    # Deferred strategies; reject at validate time per PLAN §11.3.
    "aggregate_merge",
    "snapshot",
    "scd2",
]
NodeLayer = Literal["silver", "gold"]
NodeImplType = Literal["sql", "builtin", "python_legacy"]


class WatermarkSpec(BaseModel):
    """Per-source watermark column for incremental filtering."""

    model_config = ConfigDict(extra="forbid")

    column: str
    """Bronze/silver column whose monotonic values drive the watermark predicate."""


class SourceRef(BaseModel):
    """One entry in ``dependsOn.bronze[]`` or ``dependsOn.silver[]``.

    PLAN §11.10 (primary/lookup contract).
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    """Dataset/node id; must resolve in bronze.yaml or the pack's silver node set."""

    role: Role | None = None
    """Optional in YAML for single-source nodes (default applies); required for multi-bronze."""

    watermark: WatermarkSpec | None = None


class DependsOn(BaseModel):
    """Per-node upstream dependencies."""

    model_config = ConfigDict(extra="forbid")

    bronze: list[SourceRef] = Field(default_factory=list)
    silver: list[SourceRef] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Refresh strategy specs
# ---------------------------------------------------------------------------


class RefreshSeed(BaseModel):
    """Seed-mode refresh strategy. Typically `replace` for all v0.3 nodes."""

    model_config = ConfigDict(extra="forbid")

    strategy: SeedStrategy


class IncrementalWatermark(BaseModel):
    """Watermark configuration for incremental-mode merge / replace_partition."""

    model_config = ConfigDict(extra="forbid")

    source: str
    """ID of the primary source whose watermark drives this node."""

    column: str


class AffectedPartitionsFrom(BaseModel):
    """Maps source delta to target partitions (replace_partition strategy)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    source: str
    source_columns: list[str] = Field(alias="sourceColumns", min_length=1)


class RefreshIncremental(BaseModel):
    """Incremental-mode refresh strategy (PLAN §10, §11.3)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    strategy: IncrementalStrategy
    watermark: IncrementalWatermark | None = None
    natural_key: list[str] = Field(default_factory=list, alias="naturalKey")
    partition_columns: list[str] = Field(default_factory=list, alias="partitionColumns")
    affected_partitions_from: AffectedPartitionsFrom | None = Field(
        default=None, alias="affectedPartitionsFrom"
    )
    tracked_columns: list[str] = Field(default_factory=list, alias="trackedColumns")
    """For scd2: columns whose changes close a record (deferred — schema only)."""
    reason: str | None = None


class RefreshSpec(BaseModel):
    """Both seed and incremental refresh strategies."""

    model_config = ConfigDict(extra="forbid")

    seed: RefreshSeed
    incremental: RefreshIncremental | None = None


# ---------------------------------------------------------------------------
# Output schema (PLAN §8.5)
# ---------------------------------------------------------------------------


class OutputSchemaColumn(BaseModel):
    """One column in a node's declared output schema."""

    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    """Spark SQL type string (e.g., `bigint`, `string`, `decimal(28,8)`)."""

    nullable: bool = True
    pii: PiiLevel
    """REQUIRED per PLAN §8.5. Missing → AIDPF-2030."""


class OutputSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    columns: list[OutputSchemaColumn] = Field(min_length=1)


# ---------------------------------------------------------------------------
# Quality tests (discriminated union over `type`)
# ---------------------------------------------------------------------------


class _QualityTestBase(BaseModel):
    """Common base for quality test entries. Field shapes vary by type."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class QualityTestNotNull(_QualityTestBase):
    type: Literal["not_null"] = "not_null"
    columns: list[str] = Field(min_length=1)


class QualityTestUnique(_QualityTestBase):
    type: Literal["unique"] = "unique"
    columns: list[str] = Field(min_length=1)


class QualityTestAcceptedValues(_QualityTestBase):
    type: Literal["accepted_values"] = "accepted_values"
    column: str
    values: list[Any] = Field(min_length=1)


class QualityTestRowCountMin(_QualityTestBase):
    type: Literal["row_count_min"] = "row_count_min"
    min: int = Field(ge=0)
    when_source_non_empty: str | None = Field(default=None, alias="whenSourceNonEmpty")


class QualityTestRowCountDelta(_QualityTestBase):
    type: Literal["row_count_delta"] = "row_count_delta"
    tolerance_pct: float = Field(alias="tolerancePct", ge=0)


class QualityTestFreshness(_QualityTestBase):
    type: Literal["freshness"] = "freshness"
    column: str
    max_age_hours: int = Field(alias="maxAgeHours", ge=1)


class QualityTestReconcileTo(_QualityTestBase):
    type: Literal["reconcile_to"] = "reconcile_to"
    source: str
    aggregate: str
    tolerance: float = Field(default=0.0, ge=0)


class QualityTestReferentialIntegrity(_QualityTestBase):
    type: Literal["referential_integrity"] = "referential_integrity"
    column: str
    references: str


class QualityTestCustom(_QualityTestBase):
    """Third-party quality test (PLAN §8.6.1)."""

    type: Literal["custom"] = "custom"
    implementation: str
    args: dict[str, Any] = Field(default_factory=dict)


QualityTest = Annotated[
    (
        QualityTestNotNull
        | QualityTestUnique
        | QualityTestAcceptedValues
        | QualityTestRowCountMin
        | QualityTestRowCountDelta
        | QualityTestFreshness
        | QualityTestReconcileTo
        | QualityTestReferentialIntegrity
        | QualityTestCustom
    ),
    Field(discriminator="type"),
]


class QualitySection(BaseModel):
    """Per-node `quality:` block."""

    model_config = ConfigDict(extra="forbid")

    tests: list[QualityTest] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Node implementation (discriminated union over `type`)
# ---------------------------------------------------------------------------


class SqlImpl(BaseModel):
    """`implementation.type: sql` — a SQL template file."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["sql"] = "sql"
    sql: str
    """Pack-relative path to the SQL template file (e.g., `silver/dim_supplier.sql`)."""


class BuiltinImpl(BaseModel):
    """`implementation.type: builtin` — engine-owned helper (e.g., `dim_calendar`, ADR-0011)."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["builtin"] = "builtin"
    callable: str
    """Importable Python callable, `<module>:<func>` form."""


class PythonLegacyImpl(BaseModel):
    """`implementation.type: python_legacy` — v1 module bridged for the migration period.

    Per the v2 plan, this type is permitted only during the migration window:

    * ``deprecated: true`` — v1 module replaced by SQL, kept for parity testing.
    * ``deprecated: false`` — v1 module still active runtime; ``migrationTarget``
      points at the pack-relative SQL path that will replace it in Phase 3.

    The discriminator validates the field is **present**. The model_validator
    below enforces the ``deprecated=false → migrationTarget`` invariant.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type: Literal["python_legacy"] = "python_legacy"
    callable: str
    """Importable Python callable, `<module>:<func>` form."""

    deprecated: bool
    """Required field. Missing → AIDPF-2060.

    Pydantic treats `bool` fields without a default as required; if YAML omits
    the key entirely, validation fails with a generic missing-field error. We
    wrap that case in a model_validator below to surface AIDPF-2060 cleanly.
    """

    migration_target: str | None = Field(default=None, alias="migrationTarget")
    """Pack-relative SQL path that will replace this module in Phase 3.
    Required when ``deprecated=False``; ignored otherwise."""

    @model_validator(mode="after")
    def _check_deprecated_invariant(self) -> "PythonLegacyImpl":
        if self.deprecated is False and not self.migration_target:
            raise ValueError(
                f"{AIDPF_2060_PYTHON_LEGACY_NO_DEPRECATED}: "
                "python_legacy node with deprecated=false must declare "
                "migrationTarget pointing at the pack-relative SQL path that "
                "will replace it in Phase 3."
            )
        return self


NodeImplementation = Annotated[
    SqlImpl | BuiltinImpl | PythonLegacyImpl,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# NodeYaml (silver/gold per-node YAML file)
# ---------------------------------------------------------------------------


class NodeYaml(BaseModel):
    """Schema for ``silver/<name>.yaml`` and ``gold/<name>.yaml`` files.

    Enforces the full v0.3 strategy validation matrix (PLAN §11.3 R1-R13).
    Each rule violation raises a `ValueError` with a specific AIDPF code so
    the CLI's `content-pack validate` surfaces actionable remediation.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    layer: NodeLayer
    implementation: NodeImplementation
    target: str
    """Target table name (resolved against `{{ silver_schema }}` / `{{ gold_schema }}`)."""

    depends_on: DependsOn = Field(default_factory=DependsOn, alias="dependsOn")

    refresh: RefreshSpec

    required_columns: dict[str, list[str]] = Field(
        default_factory=dict, alias="requiredColumns"
    )
    """Per-source required column lists. Keys are source IDs, values are column lists."""

    output_schema: OutputSchema = Field(alias="outputSchema")

    quality: QualitySection = Field(default_factory=QualitySection)

    # ----- v0.3 strategy validation matrix (PLAN §11.3 R1-R13) -----------

    @model_validator(mode="after")
    def _validate_strategy_matrix(self) -> "NodeYaml":
        inc = self.refresh.incremental
        if inc is None:
            # No incremental block declared; nothing to validate against §11.3.
            return self

        bronze_sources = self.depends_on.bronze
        primary_sources = [s for s in bronze_sources if s.role == "primary"]
        # Determine "primary set" — treat single-bronze nodes with no explicit
        # role as implicit primary (per §11.10 defaults).
        implicit_primary = (
            len(bronze_sources) == 1
            and bronze_sources[0].role is None
            and not self.depends_on.silver
        )
        primary_count = (
            len(primary_sources) if not implicit_primary else 1
        )

        s = inc.strategy

        # R9: aggregate_merge is deferred to v0.4+.
        if s == "aggregate_merge":
            raise ValueError(
                f"{AIDPF_2057_AGGREGATE_MERGE_DEFERRED}: strategy `aggregate_merge` "
                "is deferred to v0.4+. Use `replace` in v0.3; see PLAN §10.8."
            )

        # R10: snapshot itself is deferred; if declared, must have a
        # unique-on-(natural_key, snapshot_date) quality test.
        if s == "snapshot":
            has_unique_snapshot_test = any(
                isinstance(t, QualityTestUnique)
                and "snapshot_date" in t.columns
                and any(k in t.columns for k in (inc.natural_key or []))
                for t in self.quality.tests
            )
            if not has_unique_snapshot_test:
                raise ValueError(
                    f"{AIDPF_2058_SNAPSHOT_NO_UNIQUE_TEST}: strategy `snapshot` "
                    "(deferred) requires a `unique` quality test on "
                    "(natural_key, snapshot_date). See PLAN §10.7."
                )

        # R11: scd2 itself is deferred; if declared, must have trackedColumns.
        if s == "scd2" and not inc.tracked_columns:
            raise ValueError(
                f"{AIDPF_2059_SCD2_NO_TRACKED_COLUMNS}: strategy `scd2` "
                "(deferred) requires `trackedColumns:`. See PLAN §10.6."
            )

        # R1: merge requires naturalKey.
        if s == "merge" and not inc.natural_key:
            raise ValueError(
                f"{AIDPF_2020_MERGE_NO_NATURAL_KEY}: strategy `merge` requires "
                "`naturalKey:` on the node's `refresh.incremental` block."
            )

        # R2: merge requires watermark config.
        if s == "merge" and inc.watermark is None:
            raise ValueError(
                f"{AIDPF_2050_MERGE_NO_WATERMARK}: strategy `merge` requires "
                "`incremental.watermark.{source,column}`."
            )

        # R5: merge with multiple bronze deps and no explicit role classification.
        if s == "merge" and len(bronze_sources) > 1:
            unroled = [src.id for src in bronze_sources if src.role is None]
            if unroled:
                raise ValueError(
                    f"{AIDPF_2053_MERGE_MULTI_BRONZE_NO_ROLE}: strategy `merge` "
                    "with multiple bronze deps requires explicit `role:` "
                    f"(primary|lookup) on every source. Missing role on: {unroled!r}."
                )

        # R3: merge with zero role:primary.
        if s == "merge" and primary_count == 0:
            raise ValueError(
                f"{AIDPF_2051_MERGE_ZERO_PRIMARY}: strategy `merge` requires "
                "exactly one source marked `role: primary` (PLAN §11.10)."
            )

        # R4: merge with multiple role:primary (multi-primary deferred).
        if s == "merge" and primary_count > 1:
            raise ValueError(
                f"{AIDPF_2052_MERGE_MULTI_PRIMARY}: strategy `merge` with multiple "
                "`role: primary` sources is deferred to v0.4+ (PLAN §11.11). "
                "Collapse to a single primary or switch to `replace`."
            )

        # R6: replace_partition requires partitionColumns or affectedPartitionsFrom.
        if s == "replace_partition" and not (
            inc.partition_columns or inc.affected_partitions_from
        ):
            raise ValueError(
                f"{AIDPF_2054_REPLACE_PARTITION_NO_COLUMNS}: strategy "
                "`replace_partition` requires `partitionColumns:` or "
                "`affectedPartitionsFrom:`. See PLAN §10.4."
            )

        # R7: replace_partition with multi-source primary.
        if s == "replace_partition" and primary_count > 1:
            raise ValueError(
                f"{AIDPF_2055_REPLACE_PARTITION_MULTI_PRIMARY}: strategy "
                "`replace_partition` requires a single `role: primary`. "
                "Partition derivation requires an unambiguous primary."
            )

        # R8: append with `unique` quality test but no naturalKey.
        if s == "append" and not inc.natural_key:
            has_unique_test = any(isinstance(t, QualityTestUnique) for t in self.quality.tests)
            if has_unique_test:
                raise ValueError(
                    f"{AIDPF_2056_APPEND_UNIQUE_NO_KEY}: strategy `append` with "
                    "a `unique` quality test must declare `naturalKey:` (or "
                    "remove the test)."
                )

        return self
