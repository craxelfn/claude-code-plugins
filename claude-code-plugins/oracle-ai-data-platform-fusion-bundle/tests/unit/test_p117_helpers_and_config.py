"""P1.17 Stage D — helpers + IncrementalConfig tests.

Covers:
  - D8: `bundle.yaml` without `incremental:` declares
        `watermark_safety_window_seconds=3600` by default.
  - D9: `bundle.yaml` with `incremental.watermark_safety_window_seconds:
        7200` resolves to `timedelta(hours=2)` through
        `_resolve_safety_window`.
  - Helper-level pinning:
      * `_to_bicc_iso(datetime) → ISO-8601 with trailing Z`
      * `_natural_key_join_sql(...)` produces NULL-safe `<=>` predicates
        for single + composite keys.
      * `_resolve_target_table(spec, paths)` resolves each spec class.
      * `IncrementalConfig` rejects ≤0 + non-int values.
      * `Bundle.incremental` field defaults via default_factory.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from oracle_ai_data_platform_fusion_bundle.config.paths import DEFAULT_PATHS
from oracle_ai_data_platform_fusion_bundle.orchestrator import (
    _natural_key_join_sql,
    _to_bicc_iso,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.registry import (
    BRONZE_EXTRACTS,
    DeferredSpec,
    GOLD_MARTS,
    SILVER_DIMS,
    _resolve_target_table,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.runtime import (
    WATERMARK_SAFETY_WINDOW,
    _resolve_safety_window,
)
from oracle_ai_data_platform_fusion_bundle.schema.bundle import (
    Bundle,
    DatasetSpec,
    FusionConn,
    IncrementalConfig,
)


# ---------------------------------------------------------------------------
# _to_bicc_iso — UTC ISO-8601 with trailing 'Z'
# ---------------------------------------------------------------------------


class TestToBiccIso:
    def test_utc_datetime_renders_with_z_suffix(self) -> None:
        wm = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert _to_bicc_iso(wm) == "2026-04-01T00:00:00Z"

    def test_non_utc_timezone_is_converted_to_utc(self) -> None:
        # 12:00 in UTC-5 == 17:00 in UTC. The helper must normalize so
        # BICC's filter (evaluated against Fusion's UTC clock) sees the
        # right instant.
        from datetime import timezone as _tz
        tz_minus5 = _tz(timedelta(hours=-5))
        wm = datetime(2026, 4, 1, 12, 0, 0, tzinfo=tz_minus5)
        assert _to_bicc_iso(wm) == "2026-04-01T17:00:00Z"

    def test_microseconds_preserved(self) -> None:
        wm = datetime(2026, 4, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)
        assert _to_bicc_iso(wm) == "2026-04-01T00:00:00.123456Z"


# ---------------------------------------------------------------------------
# _natural_key_join_sql — NULL-safe MERGE ON predicate
# ---------------------------------------------------------------------------


class TestNaturalKeyJoinSql:
    def test_single_key_uses_null_safe_operator(self) -> None:
        # NULL-safe `<=>` so NULL components don't break row matching.
        assert _natural_key_join_sql("SEGMENT1") == "target.SEGMENT1 <=> src.SEGMENT1"

    def test_composite_key_AND_joined(self) -> None:
        # AND-joined per-column predicates; same NULL-safe operator.
        result = _natural_key_join_sql(("k1", "k2", "k3"))
        assert result == "target.k1 <=> src.k1 AND target.k2 <=> src.k2 AND target.k3 <=> src.k3"

    def test_custom_aliases(self) -> None:
        result = _natural_key_join_sql("k1", target_alias="t", src_alias="s")
        assert result == "t.k1 <=> s.k1"

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError, match="natural_key is empty"):
            _natural_key_join_sql("")

    def test_empty_tuple_raises(self) -> None:
        with pytest.raises(ValueError, match="natural_key is empty tuple"):
            _natural_key_join_sql(())


# ---------------------------------------------------------------------------
# _resolve_target_table — spec → 3-part Delta table identifier
# ---------------------------------------------------------------------------


class TestResolveTargetTable:
    def test_bronze_spec_resolves_to_bronze_path(self) -> None:
        # bronze_table_name comes from the catalog (PvoEntry.bronze_table_name).
        result = _resolve_target_table(BRONZE_EXTRACTS["erp_suppliers"], DEFAULT_PATHS)
        assert result == "fusion_catalog.bronze.erp_suppliers"

    def test_silver_spec_uses_dataset_id_for_table_name(self) -> None:
        result = _resolve_target_table(SILVER_DIMS["dim_supplier"], DEFAULT_PATHS)
        assert result == "fusion_catalog.silver.dim_supplier"

    def test_gold_spec_uses_dataset_id_for_table_name(self) -> None:
        result = _resolve_target_table(GOLD_MARTS["gl_balance"], DEFAULT_PATHS)
        assert result == "fusion_catalog.gold.gl_balance"

    def test_deferred_spec_raises(self) -> None:
        # Deferred specs never materialize a Delta target — calling
        # _resolve_target_table on one is a caller bug.
        spec = DeferredSpec("dim_org", layer="silver", reason="BACKLOG P1.7")
        with pytest.raises(TypeError, match="deferred spec"):
            _resolve_target_table(spec, DEFAULT_PATHS)

    def test_unknown_spec_type_raises(self) -> None:
        class _Bogus:
            dataset_id = "bogus"
        with pytest.raises(TypeError, match="unknown spec type"):
            _resolve_target_table(_Bogus(), DEFAULT_PATHS)


# ---------------------------------------------------------------------------
# D8 + D9 — IncrementalConfig defaults + override threading
# ---------------------------------------------------------------------------


def _min_bundle(**overrides) -> Bundle:
    """Build a minimal valid Bundle. Pass `incremental=...` to override."""
    fields = dict(
        apiVersion="aidp-fusion-bundle/v1",
        project="p1.17-test",
        fusion=FusionConn(
            serviceUrl="https://example.fa.oraclecloud.com",
            username="u",
            password="p",
            externalStorage="s",
        ),
        datasets=[DatasetSpec(id="ap_invoices")],
    )
    fields.update(overrides)
    return Bundle(**fields)


class TestIncrementalConfigDefaults:
    """D8 — a bundle WITHOUT an `incremental:` section loads with the
    default watermark_safety_window_seconds=3600.
    """

    def test_omitted_section_defaults_to_3600s(self) -> None:
        b = _min_bundle()
        assert b.incremental.watermark_safety_window_seconds == 3600
        assert _resolve_safety_window(b) == timedelta(hours=1)
        # Sanity — matches the module-level default constant.
        assert _resolve_safety_window(b) == WATERMARK_SAFETY_WINDOW

    def test_default_factory_yields_independent_instances(self) -> None:
        # Important: shared mutable defaults are a classic Pydantic
        # footgun. default_factory must produce a fresh instance per
        # bundle so a future field with a mutable default doesn't leak
        # state across loads.
        b1 = _min_bundle()
        b2 = _min_bundle()
        assert b1.incremental is not b2.incremental


class TestIncrementalConfigValidation:
    """gt=0 validation — a non-positive value would either erase the
    safety buffer (zero) or send a future-dated cursor to BICC (negative).
    Both silently corrupt incremental extraction.
    """

    def test_negative_seconds_rejected(self) -> None:
        with pytest.raises(ValidationError, match="greater than 0"):
            IncrementalConfig(watermark_safety_window_seconds=-60)

    def test_zero_seconds_rejected(self) -> None:
        with pytest.raises(ValidationError, match="greater than 0"):
            IncrementalConfig(watermark_safety_window_seconds=0)

    def test_string_seconds_rejected(self) -> None:
        with pytest.raises(ValidationError):
            IncrementalConfig.model_validate({"watermark_safety_window_seconds": "1h"})

    def test_extra_fields_forbidden(self) -> None:
        # `extra="forbid"` so typos in customer bundles surface early.
        with pytest.raises(ValidationError):
            IncrementalConfig.model_validate(
                {"watermark_safety_window_seconds": 3600, "typo": "x"}
            )


class TestIncrementalConfigOverride:
    """D9 — operator override via bundle.yaml flows through to
    _resolve_safety_window. Documented use case: a tenant with observed
    AIDP-vs-Fusion skew > 1h widens the window to absorb it.
    """

    def test_override_threads_to_safety_window(self) -> None:
        b = _min_bundle(
            incremental=IncrementalConfig(watermark_safety_window_seconds=7200),
        )
        assert _resolve_safety_window(b) == timedelta(hours=2)

    def test_camelcase_alias_loads(self) -> None:
        # populate_by_name=True — YAML can use either snake_case or
        # camelCase. Customer bundles in the wild use camelCase.
        cfg = IncrementalConfig.model_validate(
            {"watermarkSafetyWindowSeconds": 5400}
        )
        assert cfg.watermark_safety_window_seconds == 5400
