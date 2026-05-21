"""Pre-run BICC bronze-PVO schema probe (P1.5α-fix17).

Validates every bronze PVO in the plan with a cheap Spark ``inferSchema``
roundtrip BEFORE the orchestrator commits to the full extract loop. Catches
schema-name mismatches, missing PVOs, and BICC-layer auth issues in ~1-2s
per PVO — vs. discovering them ~5-20 minutes into a multi-PVO run when one
of the later extracts crashes.

Surfaced live by TC26 full-happy-path (2026-05-21, run_id=3f9b0648): the
catalog declared ``schema="SCM"`` for ``po_receipts`` and ``scm_items``, but
BICC on saasfademo1 only publishes a ``"Financial"`` offering. The 32-minute
run got 9 successful bronze pulls (including the 10M-row ``gl_period_balances``)
then died on the 10th PVO with ``DATA_ACCESS_LAYER_0031 - Schema: SCM not
found``. The fix (P1.5α-fix18) corrects the catalog; this preflight ensures
the same class of bug never burns that much compute again.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

from oracle_ai_data_platform_fusion_bundle import extractors
from oracle_ai_data_platform_fusion_bundle.schema import fusion_catalog

from .errors import BronzeSchemaProbeError
from .registry import BronzeExtractSpec

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle


# Heuristic classification of common BICC failures — used to give the operator
# a remediation hint instead of just the raw Java exception class.
_DATA_ACCESS_LAYER_SCHEMA_RE = "DATA_ACCESS_LAYER_0031"  # Schema X not found
_DATA_ACCESS_LAYER_PVO_RE = "DATA_ACCESS_LAYER_0032"     # PVO not found (inferred — variant of _0031)


def _classify(exc: BaseException) -> str:
    """Return a short remediation hint based on the exception message."""
    msg = str(exc)
    if _DATA_ACCESS_LAYER_SCHEMA_RE in msg:
        return "BICC offering schema not found on this tenant — check catalog schema field is tenant-correct"
    if _DATA_ACCESS_LAYER_PVO_RE in msg:
        return "PVO not found — catalog datastore name may have drifted from BICC"
    if "401" in msg or "Unauthorized" in msg or "authentication" in msg.lower():
        return "BICC credential rejected — check bundle.fusion.username + password resolver"
    if "Connection refused" in msg or "UnknownHost" in msg or "timed out" in msg.lower():
        return "BICC unreachable — check bundle.fusion.serviceUrl + network egress from cluster"
    return "uncategorized BICC reader failure — see full exception in run logs"


def preflight_bronze_schemas(
    spark: "SparkSession",
    bundle: "Bundle",
    plan: Iterable[object],
    resolved_password: str,
) -> None:
    """Probe every bronze PVO in ``plan`` for schema-inference success.

    Lazily calls ``extract_pvo()`` then ``df.schema`` for each
    :class:`BronzeExtractSpec` in the plan. Spark's ``.schema`` triggers
    BICC's ``inferSchema`` (a metadata-only roundtrip — no extract files
    are written, no rows pulled). Deferred specs are skipped.

    Args:
        spark: Active SparkSession.
        bundle: Loaded :class:`Bundle` — provides ``fusion.serviceUrl``,
            ``fusion.username``, ``fusion.externalStorage``.
        plan: Iterable of plan-node specs (mixed BronzeExtractSpec /
            SilverDimSpec / GoldMartSpec / DeferredSpec). Non-bronze and
            deferred specs are skipped.
        resolved_password: Plaintext password already resolved from the
            credential preflight (§4.4 step-3.5). Never logged.

    Raises:
        BronzeSchemaProbeError: At least one PVO probe failed. The message
            lists every failing PVO with its classification + short error.
            ``.failures`` carries structured detail for programmatic callers.
            No Spark side effects (no saveAsTable, no state-table writes).
    """
    failures: list[dict] = []

    for node in plan:
        if not isinstance(node, BronzeExtractSpec):
            continue
        try:
            pvo = fusion_catalog.get(node.pvo_id)
        except Exception as exc:
            # Catalog miss is its own class of bug (registry vs catalog drift);
            # surface it under the same preflight banner so the operator gets
            # one consolidated failure list.
            failures.append({
                "dataset_id": node.dataset_id, "pvo_id": node.pvo_id,
                "stage": "catalog_lookup",
                "exception_class": type(exc).__name__,
                "message": str(exc)[:300],
                "hint": "catalog entry missing for this PVO id",
            })
            continue

        try:
            df = extractors.bicc.extract_pvo(
                spark, pvo,
                fusion_service_url=bundle.fusion.service_url,
                username=bundle.fusion.username,
                password=resolved_password,
                fusion_external_storage=bundle.fusion.external_storage,
            )
            # Trigger inferSchema (metadata-only — no data rows transferred).
            # Forcing the property access lazily binds the schema; a successful
            # return here means BICC accepted the (schema, datastore, creds)
            # tuple and the reader's metadata roundtrip completed.
            _ = df.schema
        except BaseException as exc:  # noqa: BLE001 — Py4JJavaError is a base type variant
            failures.append({
                "dataset_id": node.dataset_id,
                "pvo_id": node.pvo_id,
                "catalog_schema": pvo.schema,
                "datastore": pvo.datastore,
                "stage": "schema_infer",
                "exception_class": type(exc).__name__,
                "message": str(exc).split("\n")[0][:300],
                "hint": _classify(exc),
            })

    if not failures:
        return

    # Build a multi-line message that gives the operator everything they need
    # without re-running the dispatch.
    lines = [
        f"BICC bronze-schema preflight failed for {len(failures)} PVO(s) — "
        f"no extracts were started. Fix the catalog or bundle and re-run:"
    ]
    for f in failures:
        if f["stage"] == "catalog_lookup":
            lines.append(
                f"  • {f['dataset_id']}: catalog_lookup failed — {f['message']}"
            )
        else:
            lines.append(
                f"  • {f['dataset_id']} (datastore={f['datastore']}, "
                f"schema={f['catalog_schema']!r}): {f['hint']}"
            )
            lines.append(f"      └─ {f['exception_class']}: {f['message']}")

    raise BronzeSchemaProbeError("\n".join(lines), failures=failures)


__all__ = ["preflight_bronze_schemas"]
