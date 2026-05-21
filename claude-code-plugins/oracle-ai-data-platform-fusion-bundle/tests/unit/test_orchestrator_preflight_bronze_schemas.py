"""Unit tests for preflight_bronze_schemas — P1.5α-fix17 regression.

Validates:
  - Clean probes → no raise, no Spark side effects beyond .schema reads.
  - Any single failure → BronzeSchemaProbeError listing every failure.
  - Failures are classified (schema-not-found vs auth vs unreachable).
  - Deferred specs are skipped (they don't probe BICC).
  - The preflight runs BEFORE any saveAsTable would be attempted.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_bundle.orchestrator.errors import (
    BronzeSchemaProbeError,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.preflight import (
    preflight_bronze_schemas,
)
from oracle_ai_data_platform_fusion_bundle.orchestrator.registry import (
    BronzeExtractSpec,
    DeferredSpec,
    GoldMartSpec,
    SilverDimSpec,
)


@pytest.fixture
def stub_bundle():
    b = MagicMock()
    b.fusion.service_url = "https://example.fa.test"
    b.fusion.username = "test.user"
    b.fusion.external_storage = "test_external_storage"
    # P1.5α-fix19: explicit empty dict — without this, MagicMock makes
    # `.schema_overrides.get(...)` return another truthy MagicMock and
    # every test silently takes the override-tier branch.
    b.fusion.schema_overrides = {}
    return b


@pytest.fixture
def stub_spark():
    return MagicMock(name="spark")


def _stub_df(success: bool = True, exc: Exception | None = None):
    df = MagicMock(name="df")
    if not success and exc is not None:
        type(df).schema = property(lambda self: (_ for _ in ()).throw(exc))
    else:
        df.schema = "<stub-schema>"
    return df


def test_clean_run_does_not_raise(stub_spark, stub_bundle):
    plan = [
        BronzeExtractSpec("erp_suppliers", "erp_suppliers"),
        BronzeExtractSpec("ap_invoices", "ap_invoices"),
    ]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=True),
    ) as extract:
        preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    assert extract.call_count == 2


def test_skips_deferred_specs(stub_spark, stub_bundle):
    plan = [
        BronzeExtractSpec("erp_suppliers", "erp_suppliers"),
        DeferredSpec(dataset_id="dim_org", layer="silver", reason="P1.7 deferred"),
        DeferredSpec(dataset_id="ar_aging", layer="gold", reason="P1.10 deferred"),
    ]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=True),
    ) as extract:
        preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    # Only the bronze extract should be probed; deferreds skipped.
    assert extract.call_count == 1


def test_skips_silver_and_gold_specs(stub_spark, stub_bundle):
    _stub_builder = lambda *a, **k: None  # noqa: E731
    plan = [
        BronzeExtractSpec("erp_suppliers", "erp_suppliers"),
        SilverDimSpec(dataset_id="dim_supplier", builder=_stub_builder,
                      depends_on_bronze=("erp_suppliers",)),
        GoldMartSpec(dataset_id="supplier_spend", builder=_stub_builder,
                     depends_on_bronze=("ap_invoices",),
                     depends_on_silver=("dim_supplier",)),
    ]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=True),
    ) as extract:
        preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    assert extract.call_count == 1


def test_schema_not_found_classified_and_raised(stub_spark, stub_bundle):
    """The exact failure mode caught live by TC26 (P1.5α-fix17 origin)."""
    exc = RuntimeError(
        "An error occurred while calling o577.load.\n"
        "DATA_ACCESS_LAYER_0031 - Schema: SCM not found. Please provide the right schema name"
    )
    plan = [BronzeExtractSpec("po_receipts", "po_receipts")]
    # P1.5α-fix19: schema-not-found now triggers auto-discovery. Mock the
    # discovery helper to return an empty mapping (PVO not present in any
    # offering) → routes through the discovery_not_found branch which is
    # the fix17-origin "tenant doesn't have the schema" classification.
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=False, exc=exc),
    ), patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "discover_pvo_schemas",
        return_value={},  # PVO not found anywhere on this tenant
    ):
        with pytest.raises(BronzeSchemaProbeError) as exc_info:
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")

    err = exc_info.value
    assert len(err.failures) == 1
    f = err.failures[0]
    assert f["dataset_id"] == "po_receipts"
    assert f["stage"] == "discovery_not_found"
    assert "tenant" in f["hint"].lower(), "hint should mention tenant"
    # The message must name the offending PVO so the operator can act
    assert "po_receipts" in str(err)


def test_credential_failure_classified(stub_spark, stub_bundle):
    # Auth failure is NOT DATA_ACCESS_LAYER_0031 → auto-discovery does NOT
    # trigger; the original schema_infer classification still fires (no
    # mock for discover_pvo_schemas needed).
    exc = RuntimeError("HTTP 401 Unauthorized — authentication failed")
    plan = [BronzeExtractSpec("erp_suppliers", "erp_suppliers")]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=False, exc=exc),
    ):
        with pytest.raises(BronzeSchemaProbeError) as exc_info:
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    assert "credential" in exc_info.value.failures[0]["hint"].lower()


def test_unreachable_classified(stub_spark, stub_bundle):
    exc = RuntimeError("Connection refused: example.fa.test:443")
    plan = [BronzeExtractSpec("erp_suppliers", "erp_suppliers")]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=False, exc=exc),
    ):
        with pytest.raises(BronzeSchemaProbeError) as exc_info:
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    assert "unreachable" in exc_info.value.failures[0]["hint"].lower()


def test_aggregates_all_failures(stub_spark, stub_bundle):
    """Operator gets ONE consolidated error, not N separate exceptions."""
    schema_exc = RuntimeError("DATA_ACCESS_LAYER_0031 - Schema: SCM not found")

    def fake_extract(spark, pvo, **kwargs):
        if pvo.id in ("po_receipts", "scm_items"):
            return _stub_df(success=False, exc=schema_exc)
        return _stub_df(success=True)

    plan = [
        BronzeExtractSpec("erp_suppliers", "erp_suppliers"),
        BronzeExtractSpec("po_receipts", "po_receipts"),
        BronzeExtractSpec("scm_items", "scm_items"),
        BronzeExtractSpec("ap_invoices", "ap_invoices"),
    ]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        side_effect=fake_extract,
    ):
        with pytest.raises(BronzeSchemaProbeError) as exc_info:
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")

    failures = exc_info.value.failures
    failed_ids = {f["dataset_id"] for f in failures}
    assert failed_ids == {"po_receipts", "scm_items"}, (
        "preflight must collect ALL failures, not stop at first"
    )


def test_preflight_makes_no_writes(stub_spark, stub_bundle):
    """The probe must never call saveAsTable / write — only schema inference."""
    df = MagicMock(name="df")
    df.schema = "<stub>"
    plan = [BronzeExtractSpec("erp_suppliers", "erp_suppliers")]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=df,
    ):
        preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    # The MagicMock will track every method call. Anything write-shaped would fail the run.
    write_calls = [c for c in df.mock_calls if "write" in str(c) or "saveAsTable" in str(c)]
    assert not write_calls, f"preflight wrote data: {write_calls}"


def test_keyboard_interrupt_propagates_not_caught(stub_spark, stub_bundle):
    """Reviewer catch: operator Ctrl-C during ``df.schema`` must propagate,
    not be swallowed as a probe failure that keeps probing the rest of the
    plan. Same for SystemExit. Originally caught by ``except BaseException``
    which is wrong — narrowed to ``except Exception`` + explicit re-raise
    of KeyboardInterrupt / SystemExit.
    """
    df_kb = _stub_df(success=False, exc=KeyboardInterrupt())
    df_se = _stub_df(success=False, exc=SystemExit())
    df_ok = _stub_df(success=True)

    # KeyboardInterrupt → must propagate
    plan = [
        BronzeExtractSpec("erp_suppliers", "erp_suppliers"),
        BronzeExtractSpec("ap_invoices", "ap_invoices"),  # would be probed if KB was swallowed
    ]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        side_effect=[df_kb, df_ok],
    ) as extract:
        with pytest.raises(KeyboardInterrupt):
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    # Critically: only ONE extract was attempted — preflight stopped, didn't loop.
    assert extract.call_count == 1, (
        "KeyboardInterrupt must stop the probe loop; getting >1 extract calls "
        "means BaseException was caught and we kept probing"
    )

    # SystemExit → must propagate too
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        side_effect=[df_se, df_ok],
    ) as extract:
        with pytest.raises(SystemExit):
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    assert extract.call_count == 1


# ---------------------------------------------------------------------------
# P1.5α-fix19 — 3-tier resolution (override → catalog → auto-discovery)
# plus dispatch-contract via PreflightResult.effective_schemas.
# ---------------------------------------------------------------------------


def test_preflight_returns_preflight_result_with_effective_schemas(stub_spark, stub_bundle):
    """Clean preflight returns PreflightResult, not None (P1.5α-fix19 contract).
    effective_schemas covers every bronze in the plan, keyed by dataset_id."""
    from oracle_ai_data_platform_fusion_bundle.orchestrator.preflight import (
        PreflightResult,
    )
    plan = [
        BronzeExtractSpec("erp_suppliers", "erp_suppliers"),
        BronzeExtractSpec("ap_invoices", "ap_invoices"),
    ]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=True),
    ):
        result = preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    assert isinstance(result, PreflightResult)
    assert result.recommendations == ()
    # Catalog defaults — for these PVO ids the catalog says "Financial"
    from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog
    assert result.effective_schemas == {
        "erp_suppliers": fusion_catalog.get("erp_suppliers").schema,
        "ap_invoices": fusion_catalog.get("ap_invoices").schema,
    }


def test_override_skips_probe_entirely(stub_spark, stub_bundle):
    """Tier 1 — `schemaOverrides.po_receipts: Custom` → extract_pvo gets
    schema='Custom', NEVER the catalog value. Discovery NEVER called."""
    stub_bundle.fusion.schema_overrides = {"po_receipts": "Custom"}
    plan = [BronzeExtractSpec("po_receipts", "po_receipts")]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=True),
    ) as extract, patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "discover_pvo_schemas",
    ) as mock_discover:
        result = preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")

    # The override value flows to extract_pvo's schema kwarg
    extract.assert_called_once()
    call_kwargs = extract.call_args.kwargs
    assert call_kwargs["schema"] == "Custom", (
        f"override schema must be passed to extract_pvo; got {call_kwargs.get('schema')!r}"
    )
    # Discovery was never triggered (override = no probe)
    mock_discover.assert_not_called()
    # effective_schemas carries the override value, keyed by dataset_id
    assert result.effective_schemas == {"po_receipts": "Custom"}


def test_override_failure_does_not_trigger_discovery(stub_spark, stub_bundle):
    """Override = explicit operator choice. If wrong, surface the failure
    directly — discovery cascading on override failure would mask user typos."""
    stub_bundle.fusion.schema_overrides = {"po_receipts": "Bogus"}
    exc = RuntimeError("DATA_ACCESS_LAYER_0031 - Schema: Bogus not found")
    plan = [BronzeExtractSpec("po_receipts", "po_receipts")]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=False, exc=exc),
    ), patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "discover_pvo_schemas",
    ) as mock_discover:
        with pytest.raises(BronzeSchemaProbeError) as exc_info:
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    # Discovery NOT called — override-failure short-circuit
    mock_discover.assert_not_called()
    err = exc_info.value
    assert err.failures[0]["stage"] == "override_failed"
    assert "Bogus" in str(err)
    assert "remove the override" in str(err).lower() or "remove the override" in err.failures[0]["hint"]


def test_auto_discovery_unique_match_succeeds_with_warn_and_recommendation(
    stub_spark, stub_bundle, caplog,
):
    """Tier 3 unique match — first extract fails with DATA_ACCESS_LAYER_0031,
    discovery returns ONE candidate, second extract with discovered schema
    succeeds. Recommendation + WARN emitted."""
    import logging
    from oracle_ai_data_platform_fusion_bundle.orchestrator.preflight import (
        PreflightResult,
    )
    from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog

    pvo = fusion_catalog.get("po_receipts")
    schema_not_found = RuntimeError(
        f"DATA_ACCESS_LAYER_0031 - Schema: {pvo.schema} not found"
    )

    # First call (catalog): fails. Second call (discovered): succeeds.
    extract_results = [
        _stub_df(success=False, exc=schema_not_found),
        _stub_df(success=True),
    ]
    plan = [BronzeExtractSpec("po_receipts", "po_receipts")]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        side_effect=extract_results,
    ) as extract, patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "discover_pvo_schemas",
        return_value={pvo.datastore: {"Financial"}},
    ), caplog.at_level(logging.WARNING):
        result = preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")

    # Required assertions on PreflightResult
    assert isinstance(result, PreflightResult)
    assert len(result.recommendations) == 1
    assert "schemaOverrides.po_receipts: Financial" in result.recommendations[0]
    # Load-bearing: dispatch-contract value is the discovered schema, NOT catalog
    assert result.effective_schemas["po_receipts"] == "Financial"
    # Two extract_pvo calls — first with catalog, second with discovered
    assert extract.call_count == 2
    assert extract.call_args_list[0].kwargs["schema"] == pvo.schema  # original catalog
    assert extract.call_args_list[1].kwargs["schema"] == "Financial"  # discovered
    # WARN emitted with the auto-corrected pair
    assert any(
        "auto-corrected po_receipts" in r.message for r in caplog.records
    ), f"WARN log expected; got: {[r.message for r in caplog.records]}"


def test_auto_discovery_ambiguous_raises_with_candidate_list(stub_spark, stub_bundle):
    """Tier 3 ambiguous — discovery returns multiple candidates. Raise with
    full list + dataset_id-keyed remediation."""
    from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog
    pvo = fusion_catalog.get("po_receipts")
    schema_not_found = RuntimeError(
        f"DATA_ACCESS_LAYER_0031 - Schema: {pvo.schema} not found"
    )
    plan = [BronzeExtractSpec("po_receipts", "po_receipts")]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=False, exc=schema_not_found),
    ), patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "discover_pvo_schemas",
        return_value={pvo.datastore: {"Financial", "SCM"}},
    ):
        with pytest.raises(BronzeSchemaProbeError) as exc_info:
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    msg = str(exc_info.value)
    # Both candidates appear
    assert "Financial" in msg
    assert "SCM" in msg
    # Operator-actionable remediation pointing at schemaOverrides (dataset_id key)
    assert "schemaOverrides.po_receipts" in msg


def test_auto_discovery_not_found_raises_with_renamed_hint(stub_spark, stub_bundle):
    """Tier 3 not found anywhere — discovery returns empty mapping.
    Hint mentions catalog drift / PVO renamed / not in subscription."""
    from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog
    pvo = fusion_catalog.get("po_receipts")
    schema_not_found = RuntimeError(
        f"DATA_ACCESS_LAYER_0031 - Schema: {pvo.schema} not found"
    )
    plan = [BronzeExtractSpec("po_receipts", "po_receipts")]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=False, exc=schema_not_found),
    ), patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "discover_pvo_schemas",
        return_value={},
    ):
        with pytest.raises(BronzeSchemaProbeError) as exc_info:
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    err = exc_info.value
    assert err.failures[0]["stage"] == "discovery_not_found"
    hint = err.failures[0]["hint"]
    assert any(s in hint.lower() for s in ("renamed", "subscription", "drift"))


def test_discovery_probe_failure_falls_back_to_original_error(stub_spark, stub_bundle):
    """Discovery probe itself fails (HTTP 5xx etc) → surface BOTH the
    schema-not-found AND the discovery-probe failure so operator can debug
    either side."""
    from oracle_ai_data_platform_fusion_bundle.orchestrator.errors import (
        DiscoveryProbeError,
    )
    schema_not_found = RuntimeError("DATA_ACCESS_LAYER_0031 - Schema not found")
    plan = [BronzeExtractSpec("po_receipts", "po_receipts")]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=False, exc=schema_not_found),
    ), patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "discover_pvo_schemas",
        side_effect=DiscoveryProbeError("HTTP 503 Service Unavailable"),
    ):
        with pytest.raises(BronzeSchemaProbeError) as exc_info:
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")
    err = exc_info.value
    assert err.failures[0]["stage"] == "schema_infer_then_discovery_failed"
    # Both error pieces surface
    msg = str(err)
    assert "DATA_ACCESS_LAYER_0031" in msg or "schema not found" in msg.lower()
    assert "503" in msg


def test_discovery_cache_called_once_for_multiple_failures(stub_spark, stub_bundle):
    """Memoization contract — 2 PVOs both trip auto-discovery → discover_pvo_schemas
    is called ONCE (cached). Option A from plan.md (patch helper, assert call_count)."""
    from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog
    pvo1 = fusion_catalog.get("erp_suppliers")
    pvo2 = fusion_catalog.get("ap_invoices")
    _DISCOVERED = "Discovered_Schema_Not_In_Catalog"  # MUST differ from pvo.schema

    def fake_extract(spark, pvo, **kwargs):
        # Fail on catalog schema; succeed on the discovered schema. Different
        # values required — if discovery returned the catalog string the
        # retry would re-fail and we'd never hit the success path.
        if kwargs.get("schema") == _DISCOVERED:
            return _stub_df(success=True)
        return _stub_df(
            success=False,
            exc=RuntimeError(
                f"DATA_ACCESS_LAYER_0031 - Schema: {kwargs.get('schema')!r} not found"
            ),
        )

    plan = [
        BronzeExtractSpec("erp_suppliers", "erp_suppliers"),
        BronzeExtractSpec("ap_invoices", "ap_invoices"),
    ]
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        side_effect=fake_extract,
    ), patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "discover_pvo_schemas",
        return_value={pvo1.datastore: {_DISCOVERED}, pvo2.datastore: {_DISCOVERED}},
    ) as mock_discover:
        result = preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")

    # ONE discovery call across BOTH PVO failures — memoization contract
    assert mock_discover.call_count == 1, (
        f"discover_pvo_schemas must be called once and cached; "
        f"got {mock_discover.call_count} calls"
    )
    # Both PVOs auto-corrected to the discovered schema
    assert result.effective_schemas["erp_suppliers"] == _DISCOVERED
    assert result.effective_schemas["ap_invoices"] == _DISCOVERED
