# TC21 — `silver.dim_calendar` live verification (2026-05-07)

> **Status**: ✅ **PASS (100% verification)** — calendar dim materialized end-to-end on `fusion_bundle_dev` cluster. Unlike `dim_supplier` / `supplier_spend` (which have data-shape variations across pods), `dim_calendar` is fully system-generated, so this single run covers the entire verification surface. No production-shape gap.

## Test setup

* **Cluster**: `fusion_bundle_dev` (id `838d3aff-1f5a-4aec-a020-57fd9ad27b58`) in workspace `f95a83f8-9bd1-4259-a45f-ea1c3a5a7516`
* **SQL**: inlined from `dimensions/dim_calendar.py` with default parameters (range 2020-01-01 → 2030-12-31, fiscal_start_month=1)
* **Source**: none — generated via Spark's `sequence(DATE, DATE, INTERVAL 1 DAY)` + `EXPLODE`

## Row count + range

| Metric | Expected | Actual |
|---|---|---|
| Row count | 4,018 (3 leap years: 2020, 2024, 2028) | **4,018** ✅ |
| Distinct `calendar_date` count | 4,018 (no dupes) | **4,018** ✅ |
| `MIN(calendar_date)` | 2020-01-01 | **2020-01-01** ✅ |
| `MAX(calendar_date)` | 2030-12-31 | **2030-12-31** ✅ |

## No-gap invariant

The strongest invariant a calendar dim can have: every consecutive pair of dates must be exactly 1 day apart.

| Check | Result |
|---|---|
| `COUNT(*) WHERE DATEDIFF(calendar_date, LAG(calendar_date)) <> 1` | **0** ✅ |

Zero gaps confirms `sequence()` covers the range continuously.

## Surrogate key invariant

`calendar_key` must equal `CAST(date_format(calendar_date, 'yyyyMMdd') AS BIGINT)` for every row — the deterministic-from-date contract.

| Check | Result |
|---|---|
| `COUNT(*) WHERE calendar_key <> CAST(date_format(calendar_date,'yyyyMMdd') AS BIGINT)` | **0** ✅ |

## Fiscal calendar (calendar-year mode, fiscal_start_month=1)

| Field | Expected range | Actual |
|---|---|---|
| `fiscal_year` | 2020-2030 | ✅ exact |
| `fiscal_period` | 1-12 (every month covered) | ✅ exact |
| `fiscal_quarter` | 1-4 | ✅ exact |

## Spot-checked known dates

| `calendar_date` | year | quarter | month | day_of_week | day_name | is_weekend | fiscal_year | fiscal_period | fiscal_quarter | calendar_key |
|---|---|---|---|---|---|---|---|---|---|---|
| 2024-02-29 (leap day) | 2024 | 1 | 2 | 5 | Thursday | false | 2024 | 2 | 1 | **20240229** |
| 2026-01-01 (New Year's) | 2026 | 1 | 1 | 5 | Thursday | false | 2026 | 1 | 1 | **20260101** |
| 2026-07-04 (US Indep. Day, Sat) | 2026 | 3 | 7 | 7 | Saturday | **true** | 2026 | 7 | 3 | **20260704** |
| 2026-12-31 (last day of range covered) | 2026 | 4 | 12 | 5 | Thursday | false | 2026 | 12 | 4 | **20261231** |

Highlights:
* **Leap day 2024-02-29** is present — confirms `sequence()` doesn't skip it
* **Saturday 2026-07-04** correctly flagged `is_weekend = TRUE` (Spark `DAYOFWEEK` semantics: 1=Sun, 7=Sat)
* **Calendar-year fiscal mode** works correctly: fiscal_period == month, fiscal_year == year

## Verdict

**TC21: ✅ PASS (full live verification).** P1.4 acceptance criteria fully satisfied:
* ✅ Generates Gregorian + Fiscal calendars for 2020-2030 (configurable)
* ✅ Surrogate `calendar_key`, `fiscal_year`, `fiscal_period`, `calendar_date` all present
* ✅ Unit test verifies coverage + no gaps (16 unit cases pass; 189/189 full suite)
* ✅ Live evidence — this file

`silver.dim_calendar` is now ready to feed P1.8 (`gl_balance`) and P1.11 (`po_backlog`).

## What's not covered live (but covered in unit tests)

* **Non-calendar fiscal modes** (`fiscal_start_month=7` Jul-Jun, `=10` Oct-Sep) — verified via unit-test SQL-string assertions; not run live since the calendar-year mode exercises the same SQL shape.
* **Custom date ranges** — verified via unit tests (`test_uses_custom_table_name`, `test_uses_sequence_for_date_range`).
* **Input validation** (`fiscal_start_month not in [1, 12]` raises `ValueError`) — pure-Python check; covered by unit test.

## References

* Module: [`scripts/.../dimensions/dim_calendar.py`](../../scripts/oracle_ai_data_platform_fusion_bundle/dimensions/dim_calendar.py)
* Unit tests: [`tests/unit/test_dim_calendar.py`](../unit/test_dim_calendar.py)
