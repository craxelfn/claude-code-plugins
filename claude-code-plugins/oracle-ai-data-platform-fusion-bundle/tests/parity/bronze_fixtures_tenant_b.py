"""Phase 4 Step 6 — bronze fixtures for parity-tenant-b.

Variant of ``bronze_fixtures.py`` that populates ``ApInvoicesCancelledFlag``
(the alternate cancelled-status semantic) instead of
``ApInvoicesCancelledDate``. The other three datasets
(``erp_suppliers``, ``gl_coa``, ``gl_period_balances``) carry the same
rows as ``bronze_fixtures.py`` — only ``ap_invoices`` diverges, since
that's the only dataset the ``cancelled_status`` variation touches.

The fixture intentionally keeps the conventional six-segment COA shape;
non-conventional COA positioning is OUT OF SCOPE for Phase 4 per the
plan's Step 6 scope clarification.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

# Re-use the unchanged datasets from bronze_fixtures so we don't carry
# two copies of identical fixture data through future refactors.
from .bronze_fixtures import (
    bronze_pyspark_schemas as _base_pyspark_schemas,
    erp_suppliers_rows, gl_coa_rows, gl_period_balances_rows,
)


def ap_invoices_rows() -> list[dict[str, Any]]:
    """Tenant-B ap_invoices fixture: ApInvoicesCancelledFlag populated
    (alternate cancelled-status semantic). ApInvoicesCancelledDate is
    explicitly None on every row so the alternate fragment's
    ``COALESCE(...CancelledFlag, 'N') != 'Y'`` predicate is the only
    cancelled-status filter applied.

    Two cancelled invoices: one with CancelledFlag='Y' (filtered out
    of ap_aging + supplier_spend), one with CancelledFlag NULL/non-'Y'
    (kept). The cross-profile diff documented in
    ``docs/v2-phase-4-multi-tenant-coverage.md`` walks the math.
    """
    base = {
        "ApInvoicesAmountPaid": 0.0,
        "ApInvoicesApprovalStatus": "APPROVED",
        "ApInvoicesTermsDate": None,
        "ApInvoicesDueDate": None,
        "ApInvoicesCancelledDate": None,  # NEVER populated under tenant-b
        "_source_pvo": "parity-tenant-b",
        "_run_id": "parity-tenant-b-seed",
        "_watermark_used": None,
    }
    invoice_date_a = datetime(2026, 4, 15, tzinfo=timezone.utc)
    invoice_date_b = datetime(2026, 5, 1, tzinfo=timezone.utc)
    invoice_date_c = datetime(2026, 5, 20, tzinfo=timezone.utc)
    extract_ts_a = datetime(2026, 5, 5, tzinfo=timezone.utc)
    extract_ts_b = datetime(2026, 5, 25, tzinfo=timezone.utc)
    extract_ts_c = datetime(2026, 5, 28, tzinfo=timezone.utc)

    return [
        # Active invoice — kept by both v1 (date semantic) and v2
        # (flag semantic, CancelledFlag NULL → COALESCE→'N' → kept).
        {**base, "ApInvoicesVendorId": 101,
         "ApInvoicesInvoiceCurrencyCode": "USD",
         "ApInvoicesInvoiceAmount": 1000.00,
         "ApInvoicesInvoiceDate": invoice_date_a,
         "ApInvoicesCancelledFlag": None,
         "_extract_ts": extract_ts_a},
        # Cancelled by FLAG — v2 with cancelled_flag semantic filters
        # this out; v1 would NOT filter it (v1 uses cancelled_date
        # only, and date is None). The cross-profile diff records
        # the row-count divergence as expected.
        {**base, "ApInvoicesVendorId": 102,
         "ApInvoicesInvoiceCurrencyCode": "USD",
         "ApInvoicesInvoiceAmount": 500.00,
         "ApInvoicesInvoiceDate": invoice_date_b,
         "ApInvoicesCancelledFlag": "Y",
         "_extract_ts": extract_ts_b},
        # Non-Y flag, treated as not-cancelled by the COALESCE rule.
        {**base, "ApInvoicesVendorId": 103,
         "ApInvoicesInvoiceCurrencyCode": "EUR",
         "ApInvoicesInvoiceAmount": 250.00,
         "ApInvoicesInvoiceDate": invoice_date_c,
         "ApInvoicesCancelledFlag": "N",
         "_extract_ts": extract_ts_c},
    ]


def all_fixtures() -> dict[str, list[dict[str, Any]]]:
    return {
        "erp_suppliers": erp_suppliers_rows(),
        "gl_coa": gl_coa_rows(),
        "gl_period_balances": gl_period_balances_rows(),
        "ap_invoices": ap_invoices_rows(),
    }


def bronze_pyspark_schemas():
    """Tenant-B bronze schema is identical to default's EXCEPT the
    ``ap_invoices`` table carries ``ApInvoicesCancelledFlag`` (string)
    in addition to ``ApInvoicesCancelledDate``. Defining the extra
    column up front means the seed DataFrame matches the rendered
    SQL's expected column set regardless of which semantic fragment
    resolves.
    """
    from pyspark.sql.types import (  # type: ignore[import-not-found]
        StructField, StringType,
    )
    base = _base_pyspark_schemas()
    # Append ApInvoicesCancelledFlag to ap_invoices. The other tables
    # are untouched.
    ap_struct = base["ap_invoices"]
    fields = list(ap_struct.fields)
    # Insert the new column right after the existing CancelledDate
    # column so column order stays human-scannable.
    new_fields = []
    for f in fields:
        new_fields.append(f)
        if f.name == "ApInvoicesCancelledDate":
            new_fields.append(StructField("ApInvoicesCancelledFlag",
                                           StringType(), True))
    from pyspark.sql.types import StructType  # type: ignore[import-not-found]
    base["ap_invoices"] = StructType(new_fields)
    return base
