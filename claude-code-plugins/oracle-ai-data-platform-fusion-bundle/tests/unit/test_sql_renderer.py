"""Unit tests for ``orchestrator/sql_renderer.py`` (Phase 2 Step 3).

The renderer is the security boundary between untrusted profile values and
Spark SQL execution. Tests cover:

* Happy path with every substitution kind.
* Identifier allowlist (AIDPF-5001).
* Profile-string injection contained inside parameter markers (no raw
  ``"DROP TABLE"`` ever appears in ``rendered.sql``).
* Semantic-fragment grammar rejection (AIDPF-5010).
* Dotted profile lookups across depth.
* Unknown token (AIDPF-5002).
* Unresolved variation point (AIDPF-5003).
* Hash determinism — cosmetic whitespace changes don't shift the hash;
  profile-value flips do.
* Disallowed param value type (AIDPF-5011).
"""

from __future__ import annotations

import pathlib

import pytest

from oracle_ai_data_platform_fusion_bundle.orchestrator.content_pack import load_pack
from oracle_ai_data_platform_fusion_bundle.orchestrator.sql_renderer import (
    AIDPF_5001_IDENTIFIER_ALLOWLIST,
    AIDPF_5002_UNKNOWN_TOKEN,
    AIDPF_5003_UNRESOLVED_VARIATION,
    AIDPF_5010_POST_RENDER_REJECTED,
    AIDPF_5011_DISALLOWED_PARAM_TYPE,
    DisallowedParamTypeError,
    IdentifierAllowlistError,
    PostRenderRejectedError,
    RenderedSql,
    RunContext,
    UnknownTokenError,
    UnresolvedVariationPointError,
    _format_profile_value_for_params,
    compute_rendered_sql_hash,
    render_node_sql,
)
from oracle_ai_data_platform_fusion_bundle.schema.tenant_profile import (
    load_tenant_profile_from_string,
)


# ---------------------------------------------------------------------------
# Fixture pack builder — minimal pack on disk that the renderer can read.
# ---------------------------------------------------------------------------


PACK_YAML_BASE = """
id: phase2-renderer-test
version: 1.0.0
description: Phase 2 renderer test pack (no variants)
compatibility:
  pluginMinVersion: 0.3.0
"""


PACK_YAML_WITH_VARIANTS = """
id: phase2-renderer-test
version: 1.0.0
description: Phase 2 renderer test pack with variation points
compatibility:
  pluginMinVersion: 0.3.0
columnAliases:
  invoice_currency:
    appliesTo: silver.dim_thing
    required: true
    candidates:
      - ApInvoicesInvoiceCurrencyCode
      - ApInvoicesCurrencyCode
semanticVariants:
  cancelled_status:
    appliesTo: silver.dim_thing
    required: true
    candidates:
      - id: cancelled_date
        detect:
          columnExists: ApInvoicesCancelledDate
        fragment: "{table}.ApInvoicesCancelledDate IS NULL"
      - id: cancelled_flag
        detect:
          columnExists: CancelledFlag
        fragment: "COALESCE({table}.CancelledFlag, 'N') != 'Y'"
"""


NODE_YAML_NO_VARIANTS = """
id: dim_thing
layer: silver
implementation:
  type: sql
  sql: silver/dim_thing.sql
target: dim_thing
outputSchema:
  columns:
    - name: thing_id
      type: string
      nullable: false
      pii: none
    - name: thing_name
      type: string
      nullable: true
      pii: low
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
      watermark:
        column: _extract_ts
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    naturalKey: [thing_id]
    watermark:
      source: erp_thing
      column: _extract_ts
"""


NODE_YAML_WITH_VARIANTS = """
id: dim_thing
layer: silver
implementation:
  type: sql
  sql: silver/dim_thing.sql
target: dim_thing
outputSchema:
  columns:
    - name: thing_id
      type: string
      nullable: false
      pii: none
dependsOn:
  bronze:
    - id: erp_thing
      role: primary
      watermark:
        column: _extract_ts
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    naturalKey: [thing_id]
    watermark:
      source: erp_thing
      column: _extract_ts
"""


PROFILE_YAML = """
schemaVersion: 1
tenant: test-tenant
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:abc"
resolved:
  column:
    invoice_currency: ApInvoicesInvoiceCurrencyCode
  semantic:
    cancelled_status: cancelled_date
profile:
  calendar:
    fiscalStartMonth: 4
    startDate: "2024-01-01"
  chartOfAccounts:
    balancingSegment: segment1
"""


def _build_pack(tmp_path: pathlib.Path, *, with_variants: bool, sql_template: str):
    """Materialise a minimal content pack on disk and return the ResolvedPack."""
    pack_root = tmp_path / "pack"
    pack_root.mkdir(parents=True, exist_ok=True)
    pack_yaml = PACK_YAML_WITH_VARIANTS if with_variants else PACK_YAML_BASE
    (pack_root / "pack.yaml").write_text(pack_yaml, encoding="utf-8")

    silver = pack_root / "silver"
    silver.mkdir()
    node_yaml = NODE_YAML_WITH_VARIANTS if with_variants else NODE_YAML_NO_VARIANTS
    (silver / "dim_thing.yaml").write_text(node_yaml, encoding="utf-8")
    (silver / "dim_thing.sql").write_text(sql_template, encoding="utf-8")

    return load_pack(pack_root)


def _default_ctx(mode: str = "seed", **overrides) -> RunContext:
    base = dict(
        catalog="fusion_catalog",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        run_id="run-2026-06-06-001",
        prior_watermark={},
        mode=mode,
        bronze_table_for_source={"erp_thing": "fusion_catalog.bronze.erp_thing"},
    )
    base.update(overrides)
    return RunContext(**base)


def _profile():
    return load_tenant_profile_from_string(PROFILE_YAML)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_simple_template_renders(self, tmp_path: pathlib.Path) -> None:
        sql_template = """
        SELECT {{ column.invoice_currency }} AS currency
        FROM {{ catalog }}.{{ bronze_schema }}.erp_thing
        WHERE {{ watermark_predicate }}
        """
        pack = _build_pack(tmp_path, with_variants=True, sql_template=sql_template)
        rendered = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(), _default_ctx()
        )
        # Identifier substitutions inlined.
        assert "fusion_catalog" in rendered.sql
        assert "bronze.erp_thing" in rendered.sql
        assert "ApInvoicesInvoiceCurrencyCode" in rendered.sql
        # Seed-mode watermark renders as always-true.
        assert "1=1" in rendered.sql

    def test_run_id_uses_parameter_marker(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT '{{ run_id_literal }}' AS run_id FROM {{ catalog }}.t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        rendered = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(), _default_ctx()
        )
        # Marker used (not the raw value).
        assert ":run_id" in rendered.sql
        assert "run-2026-06-06-001" not in rendered.sql
        assert rendered.params["run_id"] == "run-2026-06-06-001"

    def test_incremental_watermark_uses_parameter_marker(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT * FROM t WHERE {{ watermark_predicate }}"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        ctx = _default_ctx(mode="incremental", prior_watermark={"erp_thing": "2026-06-01T00:00:00"})
        rendered = render_node_sql(pack.silver["dim_thing"], pack, _profile(), ctx)
        # Column inlined; value via marker.
        assert "_extract_ts > :watermark_erp_thing" in rendered.sql
        assert rendered.params["watermark_erp_thing"] == "2026-06-01T00:00:00"
        # The raw watermark value must not appear next to the column in the SQL.
        assert "_extract_ts > '2026-06-01" not in rendered.sql

    def test_semantic_variant_renders_with_table_substitution(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT * FROM t WHERE {{ semantic.cancelled_status }}"
        pack = _build_pack(tmp_path, with_variants=True, sql_template=sql_template)
        rendered = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(), _default_ctx()
        )
        # cancelled_date fragment selected; {table} substituted with the
        # bronze table identifier.
        assert "fusion_catalog.bronze.erp_thing.ApInvoicesCancelledDate IS NULL" in rendered.sql

    def test_profile_lookup_dotted_key(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT {{ profile.calendar.fiscalStartMonth }} AS m FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        rendered = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(), _default_ctx()
        )
        assert ":profile_calendar__fiscalStartMonth" in rendered.sql
        assert rendered.params["profile_calendar__fiscalStartMonth"] == 4

    def test_returns_rendered_sql_dataclass(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT 1 AS x FROM {{ catalog }}.t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        rendered = render_node_sql(
            pack.silver["dim_thing"], pack, _profile(), _default_ctx()
        )
        assert isinstance(rendered, RenderedSql)
        assert rendered.sql
        assert rendered.hash_input


# ---------------------------------------------------------------------------
# Identifier allowlist (AIDPF-5001)
# ---------------------------------------------------------------------------


class TestIdentifierAllowlist:
    def test_catalog_with_unsafe_chars_rejected(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT * FROM {{ catalog }}.t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        ctx = _default_ctx(catalog='evil"; DROP TABLE')
        with pytest.raises(IdentifierAllowlistError) as exc_info:
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), ctx)
        assert AIDPF_5001_IDENTIFIER_ALLOWLIST in str(exc_info.value)

    def test_column_resolution_to_unsafe_identifier_rejected(
        self, tmp_path: pathlib.Path
    ) -> None:
        sql_template = "SELECT {{ column.invoice_currency }} FROM t"
        pack = _build_pack(tmp_path, with_variants=True, sql_template=sql_template)
        # Override the profile to resolve to an unsafe value.
        evil_profile_yaml = PROFILE_YAML.replace(
            "ApInvoicesInvoiceCurrencyCode",
            'evil"; DROP TABLE; --',
        )
        profile = load_tenant_profile_from_string(evil_profile_yaml)
        with pytest.raises(IdentifierAllowlistError):
            render_node_sql(pack.silver["dim_thing"], pack, profile, _default_ctx())


# ---------------------------------------------------------------------------
# Profile injection contained inside parameter markers
# ---------------------------------------------------------------------------


class TestProfileInjectionContainment:
    def test_drop_table_string_never_inlined_into_sql(self, tmp_path: pathlib.Path) -> None:
        """A profile-string value containing SQL injection MUST land in
        ``params`` only — never as a substring of ``rendered.sql``.
        """
        sql_template = "SELECT {{ profile.calendar.startDate }} FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        malicious_profile_yaml = PROFILE_YAML.replace(
            'startDate: "2024-01-01"',
            "startDate: \"'; DROP TABLE x; --\"",
        )
        profile = load_tenant_profile_from_string(malicious_profile_yaml)
        rendered = render_node_sql(pack.silver["dim_thing"], pack, profile, _default_ctx())

        # The malicious string must NOT appear in the SQL.
        assert "DROP TABLE" not in rendered.sql
        # It MUST appear in params (where Spark binds it as a literal).
        assert any("DROP TABLE" in str(v) for v in rendered.params.values())


# ---------------------------------------------------------------------------
# Semantic-fragment grammar (AIDPF-5010)
# ---------------------------------------------------------------------------


class TestSemanticFragmentGrammar:
    def test_subquery_in_fragment_rejected(self, tmp_path: pathlib.Path) -> None:
        bad_pack_yaml = PACK_YAML_WITH_VARIANTS.replace(
            'fragment: "{table}.ApInvoicesCancelledDate IS NULL"',
            'fragment: "(SELECT 1 FROM other_table)"',
        )
        pack_root = tmp_path / "pack"
        pack_root.mkdir()
        (pack_root / "pack.yaml").write_text(bad_pack_yaml, encoding="utf-8")
        silver = pack_root / "silver"
        silver.mkdir()
        (silver / "dim_thing.yaml").write_text(NODE_YAML_WITH_VARIANTS, encoding="utf-8")
        (silver / "dim_thing.sql").write_text(
            "SELECT * FROM t WHERE {{ semantic.cancelled_status }}", encoding="utf-8"
        )
        pack = load_pack(pack_root)
        with pytest.raises(PostRenderRejectedError) as exc_info:
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())
        assert AIDPF_5010_POST_RENDER_REJECTED in str(exc_info.value)

    def test_semicolon_in_fragment_rejected(self, tmp_path: pathlib.Path) -> None:
        bad_pack_yaml = PACK_YAML_WITH_VARIANTS.replace(
            'fragment: "{table}.ApInvoicesCancelledDate IS NULL"',
            'fragment: "1=1; DROP TABLE x"',
        )
        pack_root = tmp_path / "pack"
        pack_root.mkdir()
        (pack_root / "pack.yaml").write_text(bad_pack_yaml, encoding="utf-8")
        silver = pack_root / "silver"
        silver.mkdir()
        (silver / "dim_thing.yaml").write_text(NODE_YAML_WITH_VARIANTS, encoding="utf-8")
        (silver / "dim_thing.sql").write_text(
            "SELECT * FROM t WHERE {{ semantic.cancelled_status }}", encoding="utf-8"
        )
        pack = load_pack(pack_root)
        with pytest.raises(PostRenderRejectedError):
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())

    def test_comment_marker_in_fragment_rejected(self, tmp_path: pathlib.Path) -> None:
        bad_pack_yaml = PACK_YAML_WITH_VARIANTS.replace(
            'fragment: "{table}.ApInvoicesCancelledDate IS NULL"',
            'fragment: "1=1 -- comment"',
        )
        pack_root = tmp_path / "pack"
        pack_root.mkdir()
        (pack_root / "pack.yaml").write_text(bad_pack_yaml, encoding="utf-8")
        silver = pack_root / "silver"
        silver.mkdir()
        (silver / "dim_thing.yaml").write_text(NODE_YAML_WITH_VARIANTS, encoding="utf-8")
        (silver / "dim_thing.sql").write_text(
            "SELECT * FROM t WHERE {{ semantic.cancelled_status }}", encoding="utf-8"
        )
        pack = load_pack(pack_root)
        with pytest.raises(PostRenderRejectedError):
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())


# ---------------------------------------------------------------------------
# Unknown / unresolved tokens
# ---------------------------------------------------------------------------


class TestTokenResolutionFailures:
    def test_unknown_token_raises_5002(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT {{ frobnicate }} FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        with pytest.raises(UnknownTokenError) as exc_info:
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())
        assert AIDPF_5002_UNKNOWN_TOKEN in str(exc_info.value)

    def test_undeclared_column_variation_raises_5003(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT {{ column.undeclared }} FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        with pytest.raises(UnresolvedVariationPointError) as exc_info:
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())
        assert AIDPF_5003_UNRESOLVED_VARIATION in str(exc_info.value)

    def test_undeclared_semantic_variation_raises_5003(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT * FROM t WHERE {{ semantic.undeclared }}"
        pack = _build_pack(tmp_path, with_variants=True, sql_template=sql_template)
        with pytest.raises(UnresolvedVariationPointError):
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())

    def test_profile_dotted_lookup_to_dict_rejected(self, tmp_path: pathlib.Path) -> None:
        """``{{ profile.calendar }}`` resolves to a nested dict, not a leaf
        scalar — AIDPF-5011 (disallowed type)."""
        sql_template = "SELECT {{ profile.calendar }} FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        with pytest.raises(DisallowedParamTypeError):
            render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())


# ---------------------------------------------------------------------------
# Hash determinism
# ---------------------------------------------------------------------------


class TestHashDeterminism:
    def test_cosmetic_whitespace_does_not_shift_hash(self, tmp_path: pathlib.Path) -> None:
        sql_template_a = "SELECT 1 FROM {{ catalog }}.t WHERE {{ watermark_predicate }}"
        sql_template_b = "SELECT   1\nFROM   {{ catalog }}.t   WHERE   {{ watermark_predicate }}"
        pack_a = _build_pack(tmp_path / "a", with_variants=False, sql_template=sql_template_a)
        pack_b = _build_pack(tmp_path / "b", with_variants=False, sql_template=sql_template_b)
        r_a = render_node_sql(pack_a.silver["dim_thing"], pack_a, _profile(), _default_ctx())
        r_b = render_node_sql(pack_b.silver["dim_thing"], pack_b, _profile(), _default_ctx())
        assert compute_rendered_sql_hash(r_a) == compute_rendered_sql_hash(r_b)

    def test_profile_value_change_shifts_hash(self, tmp_path: pathlib.Path) -> None:
        sql_template = "SELECT {{ profile.calendar.fiscalStartMonth }} FROM t"
        pack = _build_pack(tmp_path, with_variants=False, sql_template=sql_template)
        r_a = render_node_sql(pack.silver["dim_thing"], pack, _profile(), _default_ctx())

        # Profile with a different fiscal start month.
        b_profile = load_tenant_profile_from_string(
            PROFILE_YAML.replace("fiscalStartMonth: 4", "fiscalStartMonth: 7")
        )
        r_b = render_node_sql(pack.silver["dim_thing"], pack, b_profile, _default_ctx())
        assert compute_rendered_sql_hash(r_a) != compute_rendered_sql_hash(r_b)


# ---------------------------------------------------------------------------
# Disallowed param value types
# ---------------------------------------------------------------------------


class TestDisallowedParamTypes:
    def test_list_param_value_rejected(self) -> None:
        with pytest.raises(DisallowedParamTypeError) as exc_info:
            _format_profile_value_for_params([1, 2, 3])
        assert AIDPF_5011_DISALLOWED_PARAM_TYPE in str(exc_info.value)

    def test_none_param_value_rejected(self) -> None:
        with pytest.raises(DisallowedParamTypeError):
            _format_profile_value_for_params(None)

    def test_allowed_scalar_types_pass(self) -> None:
        # str / int / float / bool / date / datetime all pass.
        from datetime import date, datetime
        for v in ("x", 1, 1.5, True, date(2026, 1, 1), datetime(2026, 1, 1)):
            assert _format_profile_value_for_params(v) == v
