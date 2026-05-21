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
    with patch(
        "oracle_ai_data_platform_fusion_bundle.orchestrator.preflight."
        "extractors.bicc.extract_pvo",
        return_value=_stub_df(success=False, exc=exc),
    ):
        with pytest.raises(BronzeSchemaProbeError) as exc_info:
            preflight_bronze_schemas(stub_spark, stub_bundle, plan, resolved_password="pw")

    err = exc_info.value
    assert len(err.failures) == 1
    f = err.failures[0]
    assert f["dataset_id"] == "po_receipts"
    assert f["stage"] == "schema_infer"
    assert "tenant" in f["hint"].lower(), "hint should mention tenant"
    # The message must name the offending PVO so the operator can act
    assert "po_receipts" in str(err)


def test_credential_failure_classified(stub_spark, stub_bundle):
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
