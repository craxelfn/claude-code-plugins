"""Hand-crafted bronze fixture rows for the Phase 3 parity harness.

Each builder returns a list of dict rows that match the v1 module's read
shape exactly. Rows cover the invariants the parity harness needs to
prove:

* `erp_suppliers` — NULL `PARTYID` (NULLIF assertion), sparse name cols
  (COALESCE assertion), multiple vendors.
* `gl_coa` — multi-segment COA with all six positions populated.
* `gl_period_balances` — multi-currency, NULL period components
  (COALESCE invariant), known closing-balance arithmetic.
* `ap_invoices` — multi-currency, NULL `ApInvoicesCancelledDate`
  (so the cancelled_status semantic variant picks `cancelled_date`),
  invoice dates spanning aging buckets, paid-vs-unpaid coverage,
  proxy-mode ratio of TermsDate/DueDate < 10% so v1 routes to proxy
  mode and stays aligned with the v2 proxy-only path (P3-L1 in
  LIMITS.md).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


# A frozen snapshot of "now" so audit timestamps are deterministic.
EXTRACT_TS = datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc)
SNAPSHOT_DATE_ISO = "2026-06-05"


def erp_suppliers_rows() -> list[dict[str, Any]]:
    """3 supplier rows — covers the invariants dim_supplier asserts."""
    base = {
        "AlternateNamePartyName": None,
        "AliasPartyName": None,
        "TaxReportingName": None,
        "BUSINESSRELATIONSHIP": "SUPPLIER",
        "ENDDATEACTIVE": None,
        "CREATIONDATE": EXTRACT_TS,
        "LASTUPDATEDATE": EXTRACT_TS,
        "PARTYID": 0,  # sentinel → NULLIF wraps to NULL
        "PARENTVENDORID": 0,
        "PARENTPARTYID": 0,
        "_extract_ts": EXTRACT_TS,
        "_source_pvo": "SupplierExtractPVO",
        "_run_id": "fixture-bronze",
        "_watermark_used": None,
    }
    return [
        {**base,
         "SEGMENT1": "ACME-001",
         "VENDORID": 101,
         "PARTYID": 5001,
         "TaxReportingName": "Acme Corp",
         "BUSINESSRELATIONSHIP": "SUPPLIER"},
        {**base,
         "SEGMENT1": "BETA-002",
         "VENDORID": 102,
         "PARTYID": 0,  # NULLIF sentinel — exercises the NULLIF wrap
         "AliasPartyName": "Beta Ltd",
         "BUSINESSRELATIONSHIP": "SUPPLIER_AND_CUSTOMER"},
        {**base,
         "SEGMENT1": "GAMMA-003",
         "VENDORID": 103,
         "PARTYID": 5003,
         "AlternateNamePartyName": "Gamma Inc",
         "BUSINESSRELATIONSHIP": "SUPPLIER"},
    ]


def gl_coa_rows() -> list[dict[str, Any]]:
    """4 COA rows — covers the six-segment positional + semantic emit."""
    base = {
        "CodeCombinationChartOfAccountsId": 101,
        "CodeCombinationAccountType": "A",
        "CodeCombinationEnabledFlag": "Y",
        "CodeCombinationSummaryFlag": "N",
        "CodeCombinationDetailPostingAllowedFlag": "Y",
        "CodeCombinationFinancialCategory": "ASSET",
        "CodeCombinationStartDateActive": EXTRACT_TS,
        "CodeCombinationEndDateActive": None,
        "_extract_ts": EXTRACT_TS,
        "_source_pvo": "GlCodeCombinationExtractPVO",
        "_run_id": "fixture-bronze",
        "_watermark_used": None,
    }
    return [
        {**base,
         "CodeCombinationCodeCombinationId": 10001,
         "CodeCombinationSegment1": "01",
         "CodeCombinationSegment2": "100",
         "CodeCombinationSegment3": "1100",
         "CodeCombinationSegment4": "0000",
         "CodeCombinationSegment5": "000",
         "CodeCombinationSegment6": "00"},
        {**base,
         "CodeCombinationCodeCombinationId": 10002,
         "CodeCombinationSegment1": "01",
         "CodeCombinationSegment2": "200",
         "CodeCombinationSegment3": "2100",
         "CodeCombinationSegment4": "0000",
         "CodeCombinationSegment5": "000",
         "CodeCombinationSegment6": "00"},
        {**base,
         "CodeCombinationCodeCombinationId": 10003,
         "CodeCombinationAccountType": "E",
         "CodeCombinationSegment1": "02",
         "CodeCombinationSegment2": "300",
         "CodeCombinationSegment3": "5100",
         "CodeCombinationSegment4": "0000",
         "CodeCombinationSegment5": "000",
         "CodeCombinationSegment6": "00"},
        {**base,
         "CodeCombinationCodeCombinationId": 10004,
         "CodeCombinationAccountType": "L",
         "CodeCombinationSegment1": "01",
         "CodeCombinationSegment2": "100",
         "CodeCombinationSegment3": "3100",
         "CodeCombinationSegment4": "0000",
         "CodeCombinationSegment5": "000",
         "CodeCombinationSegment6": "00"},
    ]


def gl_period_balances_rows() -> list[dict[str, Any]]:
    """6 balance rows — multi-currency, NULL components, multiple periods.

    Exercises the COALESCE invariant: one row has NULL period_net_dr to
    confirm the closing_balance formula still computes (rather than
    propagating NULL across the whole aggregate).
    """
    base = {
        "BalanceLedgerId": 1,
        "BalanceActualFlag": "A",
        "BalanceTranslatedFlag": "N",
        "_extract_ts": EXTRACT_TS,
        "_source_pvo": "BalanceExtractPVO",
        "_run_id": "fixture-bronze",
        "_watermark_used": None,
    }
    return [
        {**base,
         "BalanceCodeCombinationId": 10001,
         "BalancePeriodYear": 2026, "BalancePeriodNum": 1,
         "BalancePeriodName": "Jan-26", "BalanceCurrencyCode": "USD",
         "BalanceBeginBalanceDr": 1000.00, "BalanceBeginBalanceCr": 0.00,
         "BalancePeriodNetDr": 500.00, "BalancePeriodNetCr": 100.00},
        {**base,
         "BalanceCodeCombinationId": 10001,
         "BalancePeriodYear": 2026, "BalancePeriodNum": 2,
         "BalancePeriodName": "Feb-26", "BalanceCurrencyCode": "USD",
         "BalanceBeginBalanceDr": 1400.00, "BalanceBeginBalanceCr": 0.00,
         "BalancePeriodNetDr": None, "BalancePeriodNetCr": 50.00},  # NULL — COALESCE invariant
        {**base,
         "BalanceCodeCombinationId": 10002,
         "BalancePeriodYear": 2026, "BalancePeriodNum": 1,
         "BalancePeriodName": "Jan-26", "BalanceCurrencyCode": "EUR",
         "BalanceBeginBalanceDr": 0.00, "BalanceBeginBalanceCr": 200.00,
         "BalancePeriodNetDr": 75.00, "BalancePeriodNetCr": 0.00},
        {**base,
         "BalanceCodeCombinationId": 10003,
         "BalanceActualFlag": "B",  # budget — different actual_flag
         "BalancePeriodYear": 2026, "BalancePeriodNum": 1,
         "BalancePeriodName": "Jan-26", "BalanceCurrencyCode": "USD",
         "BalanceBeginBalanceDr": 0.00, "BalanceBeginBalanceCr": 0.00,
         "BalancePeriodNetDr": 250.00, "BalancePeriodNetCr": 0.00},
        {**base,
         "BalanceCodeCombinationId": 10004,
         "BalancePeriodYear": 2026, "BalancePeriodNum": 1,
         "BalancePeriodName": "Jan-26", "BalanceCurrencyCode": "USD",
         "BalanceBeginBalanceDr": 0.00, "BalanceBeginBalanceCr": 5000.00,
         "BalancePeriodNetDr": 100.00, "BalancePeriodNetCr": 0.00},
        {**base,
         "BalanceCodeCombinationId": 10001,
         "BalanceTranslatedFlag": None,  # NULL → COALESCE 'N'
         "BalancePeriodYear": 2026, "BalancePeriodNum": 3,
         "BalancePeriodName": "Mar-26", "BalanceCurrencyCode": "USD",
         "BalanceBeginBalanceDr": 1450.00, "BalanceBeginBalanceCr": 0.00,
         "BalancePeriodNetDr": 200.00, "BalancePeriodNetCr": 0.00},
        # Sub-cent fractional row — v1 rounds to DECIMAL(28,2); v2 must
        # do the same. If a future regression widens v2 to DECIMAL(28,8)
        # this row's closing_balance will differ between backends.
        {**base,
         "BalanceCodeCombinationId": 10001,
         "BalancePeriodYear": 2026, "BalancePeriodNum": 4,
         "BalancePeriodName": "Apr-26", "BalanceCurrencyCode": "USD",
         "BalanceBeginBalanceDr": 1650.00, "BalanceBeginBalanceCr": 0.00,
         "BalancePeriodNetDr": 100.12345678, "BalancePeriodNetCr": 0.00567},
    ]


def ap_invoices_rows() -> list[dict[str, Any]]:
    """5 invoice rows — multi-currency, varying invoice_date for aging
    buckets, cancelled_date NULL, some paid-in-full (zero open).

    Phase 3 LIMITS P3-L1: every row has NULL ApInvoicesTermsDate AND
    NULL ApInvoicesDueDate so v1 stays in proxy mode (coverage 0% <
    threshold 10%), aligning with v2's proxy-only path.
    """
    base = {
        "ApInvoicesCancelledDate": None,  # not cancelled
        "ApInvoicesApprovalStatus": "APPROVED",
        "ApInvoicesTermsDate": None,  # P3-L1: keep v1 in proxy mode
        "ApInvoicesDueDate": None,
        "_extract_ts": EXTRACT_TS,
        "_source_pvo": "ApInvoicesExtractPVO",
        "_run_id": "fixture-bronze",
        "_watermark_used": None,
    }
    return [
        # ACME — small open balance, NOT_DUE bucket (invoice yesterday).
        {**base, "ApInvoicesVendorId": 101,
         "ApInvoicesInvoiceCurrencyCode": "USD",
         "ApInvoicesInvoiceAmount": 1000.00,
         "ApInvoicesAmountPaid": 900.00,
         "ApInvoicesInvoiceDate": datetime(2026, 6, 4, tzinfo=timezone.utc)},
        # BETA — fully open, 0-30 bucket.
        {**base, "ApInvoicesVendorId": 102,
         "ApInvoicesInvoiceCurrencyCode": "EUR",
         "ApInvoicesInvoiceAmount": 500.00,
         "ApInvoicesAmountPaid": 0.00,
         "ApInvoicesInvoiceDate": datetime(2026, 5, 20, tzinfo=timezone.utc)},
        # GAMMA — partially paid, 31-60 bucket.
        {**base, "ApInvoicesVendorId": 103,
         "ApInvoicesInvoiceCurrencyCode": "USD",
         "ApInvoicesInvoiceAmount": 2000.00,
         "ApInvoicesAmountPaid": 800.00,
         "ApInvoicesInvoiceDate": datetime(2026, 4, 25, tzinfo=timezone.utc)},
        # ACME — credit memo (amount paid > invoice), OVER_90 bucket.
        {**base, "ApInvoicesVendorId": 101,
         "ApInvoicesInvoiceCurrencyCode": "USD",
         "ApInvoicesInvoiceAmount": 100.00,
         "ApInvoicesAmountPaid": 250.00,  # negative open_amount
         "ApInvoicesInvoiceDate": datetime(2026, 2, 15, tzinfo=timezone.utc)},
        # GAMMA — PENDING approval (different approval_status).
        {**base, "ApInvoicesVendorId": 103,
         "ApInvoicesApprovalStatus": "PENDING",
         "ApInvoicesInvoiceCurrencyCode": "USD",
         "ApInvoicesInvoiceAmount": 750.00,
         "ApInvoicesAmountPaid": None,  # NULL → COALESCE 0
         "ApInvoicesInvoiceDate": datetime(2026, 5, 10, tzinfo=timezone.utc)},
        # Sub-cent fractional amounts — v1's DECIMAL(28,2) rounds to
        # cents. Phase 3 round-3 regression guard: if a future change
        # widens v2 to DECIMAL(28,8) the open_amount / total_paid
        # aggregates here will diverge between backends.
        {**base, "ApInvoicesVendorId": 102,
         "ApInvoicesInvoiceCurrencyCode": "USD",
         "ApInvoicesInvoiceAmount": 1234.5678,
         "ApInvoicesAmountPaid": 100.1234,
         "ApInvoicesInvoiceDate": datetime(2026, 5, 25, tzinfo=timezone.utc)},
        # Unknown vendor — ApInvoicesVendorId 999999 has no matching
        # row in erp_suppliers, so the LEFT JOIN to dim_supplier leaves
        # supplier_number / supplier_name / business_relationship NULL.
        # Round-5 regression guard: round-4's ap_aging.yaml had
        # `not_null` on supplier_number, which would have converted
        # this v1-supported input into a quality_failed state row under
        # the content-pack runner. The fixture proves the NULL-supplier
        # path stays allowed end-to-end.
        {**base, "ApInvoicesVendorId": 999999,
         "ApInvoicesInvoiceCurrencyCode": "USD",
         "ApInvoicesInvoiceAmount": 500.00,
         "ApInvoicesAmountPaid": 0.00,
         "ApInvoicesInvoiceDate": datetime(2026, 5, 15, tzinfo=timezone.utc)},
        # Sub-cent residual — both amounts round to 100.00 at v1's
        # DECIMAL(28,2) precision, so the v1 open-invoice filter
        # excludes the row (100.00 - 100.00 = 0). Round-4 review caught
        # that the v2 filter predicate had stayed at DECIMAL(28,8) and
        # was including this row as open with a 0.004 residual that
        # rounded down to 0.00 in the output — inflating
        # open_invoice_count and emitting a zero-open ghost row. Both
        # filter predicates now use DECIMAL(28,2); this fixture row
        # MUST NOT appear in either v1 or v2 output.
        {**base, "ApInvoicesVendorId": 101,
         "ApInvoicesInvoiceCurrencyCode": "USD",
         "ApInvoicesInvoiceAmount": 100.004,
         "ApInvoicesAmountPaid": 100.000,
         "ApInvoicesInvoiceDate": datetime(2026, 5, 28, tzinfo=timezone.utc)},
    ]


def all_fixtures() -> dict[str, list[dict[str, Any]]]:
    return {
        "erp_suppliers": erp_suppliers_rows(),
        "gl_coa": gl_coa_rows(),
        "gl_period_balances": gl_period_balances_rows(),
        "ap_invoices": ap_invoices_rows(),
    }
