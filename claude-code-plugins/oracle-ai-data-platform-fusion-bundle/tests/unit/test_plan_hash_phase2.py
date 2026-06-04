"""Unit tests for the Phase 2 content-pack plan-hash extension.

The pre-existing v1 plan-hash (``hash_resolved_plan``) must remain
unchanged — Phase 2 adds new functions but doesn't modify the old API.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import yaml

from oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash import (
    compute_content_pack_plan_hash,
    compute_output_schema_hash,
)
from oracle_ai_data_platform_fusion_bundle.schema.medallion_pack import NodeYaml
from oracle_ai_data_platform_fusion_bundle.schema.tenant_profile import (
    load_tenant_profile_from_string,
)


NODE_YAML = """
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


PROFILE_YAML = """
schemaVersion: 1
tenant: acme-prod
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:abc"
resolved:
  column: {}
  semantic: {}
"""


def _node(yaml_text: str = NODE_YAML) -> NodeYaml:
    return NodeYaml.model_validate(yaml.safe_load(yaml_text))


def _pack(pack_id: str = "phase2-test", version: str = "1.0.0") -> MagicMock:
    """Stand-in ResolvedPack — the hash function only reads pack.pack.id/version."""
    pack = MagicMock()
    pack.pack.id = pack_id
    pack.pack.version = version
    return pack


def _profile():
    return load_tenant_profile_from_string(PROFILE_YAML)


# ---------------------------------------------------------------------------
# compute_output_schema_hash
# ---------------------------------------------------------------------------


class TestComputeOutputSchemaHash:
    def test_deterministic_across_runs(self) -> None:
        node = _node()
        assert compute_output_schema_hash(node) == compute_output_schema_hash(node)

    def test_adding_column_shifts_hash(self) -> None:
        a = compute_output_schema_hash(_node())
        # Add a column to the YAML.
        modified = NODE_YAML.replace(
            "    - name: thing_name",
            "    - name: extra\n      type: int\n      nullable: true\n      pii: none\n    - name: thing_name",
        )
        b = compute_output_schema_hash(_node(modified))
        assert a != b

    def test_changing_type_shifts_hash(self) -> None:
        a = compute_output_schema_hash(_node())
        b_yaml = NODE_YAML.replace(
            "    - name: thing_id\n      type: string",
            "    - name: thing_id\n      type: bigint",
        )
        b = compute_output_schema_hash(_node(b_yaml))
        assert a != b

    def test_flipping_nullable_shifts_hash(self) -> None:
        a = compute_output_schema_hash(_node())
        b_yaml = NODE_YAML.replace(
            "    - name: thing_name\n      type: string\n      nullable: true",
            "    - name: thing_name\n      type: string\n      nullable: false",
        )
        b = compute_output_schema_hash(_node(b_yaml))
        assert a != b

    def test_changing_pii_shifts_hash(self) -> None:
        a = compute_output_schema_hash(_node())
        b_yaml = NODE_YAML.replace(
            "    - name: thing_name\n      type: string\n      nullable: true\n      pii: low",
            "    - name: thing_name\n      type: string\n      nullable: true\n      pii: high",
        )
        b = compute_output_schema_hash(_node(b_yaml))
        assert a != b

    def test_column_reorder_shifts_hash(self) -> None:
        """Declared column order is significant (downstream consumers
        rely on it). Reordering MUST shift the hash."""
        a = compute_output_schema_hash(_node())
        # Swap thing_id and thing_name positions.
        reordered = """
id: dim_thing
layer: silver
implementation:
  type: sql
  sql: silver/dim_thing.sql
target: dim_thing
outputSchema:
  columns:
    - name: thing_name
      type: string
      nullable: true
      pii: low
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
"""
        b = compute_output_schema_hash(_node(reordered))
        assert a != b


# ---------------------------------------------------------------------------
# compute_content_pack_plan_hash
# ---------------------------------------------------------------------------


class TestComputeContentPackPlanHash:
    def _baseline(self):
        return compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )

    def test_deterministic_across_runs(self) -> None:
        assert self._baseline() == self._baseline()

    def test_different_pack_version_shifts_hash(self) -> None:
        a = self._baseline()
        b = compute_content_pack_plan_hash(
            pack=_pack(version="1.1.0"),
            node=_node(),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )
        assert a != b

    def test_different_rendered_sql_hash_shifts(self) -> None:
        a = self._baseline()
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(),
            profile=_profile(),
            rendered_sql_hash="sql-hash-DIFFERENT",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )
        assert a != b

    def test_different_output_schema_hash_shifts(self) -> None:
        a = self._baseline()
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-DIFFERENT",
            profile_hash="profile-hash-1",
        )
        assert a != b

    def test_different_profile_hash_shifts(self) -> None:
        a = self._baseline()
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-DIFFERENT",
        )
        assert a != b

    def test_different_bronze_schema_fingerprint_shifts(self) -> None:
        a = self._baseline()
        profile_b = load_tenant_profile_from_string(
            PROFILE_YAML.replace("sha256:abc", "sha256:DIFFERENT")
        )
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(),
            profile=profile_b,
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )
        assert a != b

    def test_changing_natural_key_shifts_hash(self) -> None:
        a = self._baseline()
        b_yaml = NODE_YAML.replace(
            "naturalKey: [thing_id]", "naturalKey: [thing_id, thing_name]"
        )
        b = compute_content_pack_plan_hash(
            pack=_pack(),
            node=_node(b_yaml),
            profile=_profile(),
            rendered_sql_hash="sql-hash-1",
            output_schema_hash="schema-hash-1",
            profile_hash="profile-hash-1",
        )
        assert a != b


# ---------------------------------------------------------------------------
# Backwards compatibility — v1 plan-hash API unaffected
# ---------------------------------------------------------------------------


class TestV1PlanHashUnchanged:
    def test_v1_plan_hash_function_still_imports(self) -> None:
        """Phase 2 must not remove or modify the v1 ``hash_resolved_plan``
        callable — in-flight resume runs depend on it."""
        from oracle_ai_data_platform_fusion_bundle.orchestrator.plan_hash import (
            build_current_diagnostics,
            hash_resolved_plan,
            serialize_plan_snapshot,
        )
        # Just importing the names is the regression check — any signature
        # change would surface in the v1 resume tests.
        assert callable(hash_resolved_plan)
        assert callable(serialize_plan_snapshot)
        assert callable(build_current_diagnostics)
