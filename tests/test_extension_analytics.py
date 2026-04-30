"""Tests for the per-year extension-of-time aggregator."""

from datetime import date, datetime

from harness_analytics.extension_analytics import compute_extensions_by_year


def _grouped(**kwargs):
    """Tiny helper: wrap a single application's events in the grouped shape."""
    return {
        1: {
            "ctnf": kwargs.get("ctnf", []),
            "ctfr": kwargs.get("ctfr", []),
            "ctrs": kwargs.get("ctrs", []),
            "noa": kwargs.get("noa", []),
            "response": kwargs.get("response", []),
        }
    }


def _row(rows, year):
    for r in rows:
        if r["year"] == year:
            return r
    raise AssertionError(f"year {year} missing from {rows}")


def test_empty_input_returns_empty():
    out = compute_extensions_by_year({})
    assert out["byYear"] == []
    assert out["totals"] == {"ctnf": 0, "ctfr": 0, "restriction": 0, "total": 0}
    assert out["appsContributing"] == 0


def test_ctnf_response_within_3_months_not_extension():
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        response=[date(2024, 3, 31)],
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["total"] == 0
    assert out["byYear"] == []


def test_ctnf_response_just_over_3_months_counts():
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        response=[date(2024, 4, 2)],
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["ctnf"] == 1
    assert out["totals"]["total"] == 1
    assert _row(out["byYear"], 2024)["ctnf"] == 1
    assert out["appsContributing"] == 1


def test_ctfr_extension_bucketed_separately():
    g = _grouped(
        ctfr=[date(2023, 6, 1)],
        response=[date(2023, 11, 1)],
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["ctfr"] == 1
    assert out["totals"]["ctnf"] == 0
    assert _row(out["byYear"], 2023)["ctfr"] == 1


def test_ctrs_two_month_threshold():
    # 60 days exactly = NOT > 2 months; 70 days = extension.
    on_time = _grouped(
        ctrs=[date(2024, 1, 1)],
        response=[date(2024, 3, 1)],
    )
    late = _grouped(
        ctrs=[date(2024, 1, 1)],
        response=[date(2024, 3, 15)],
    )
    assert compute_extensions_by_year(on_time)["totals"]["restriction"] == 0
    out = compute_extensions_by_year(late)
    assert out["totals"]["restriction"] == 1
    assert _row(out["byYear"], 2024)["restriction"] == 1


def test_response_year_is_year_of_response_not_oa():
    # CTNF in Dec 2023, response in May 2024 — year taken = 2024.
    g = _grouped(
        ctnf=[date(2023, 12, 1)],
        response=[date(2024, 5, 1)],
    )
    out = compute_extensions_by_year(g)
    assert _row(out["byYear"], 2024)["ctnf"] == 1
    assert _row(out["byYear"], 2024)["total"] == 1


def test_year_gaps_zero_filled():
    g = {
        1: {
            "ctnf": [date(2020, 1, 1)],
            "ctfr": [],
            "ctrs": [],
            "noa": [],
            "response": [date(2020, 5, 1)],
        },
        2: {
            "ctnf": [date(2023, 1, 1)],
            "ctfr": [],
            "ctrs": [],
            "noa": [],
            "response": [date(2023, 5, 1)],
        },
    }
    out = compute_extensions_by_year(g)
    years = [r["year"] for r in out["byYear"]]
    assert years == [2020, 2021, 2022, 2023]
    assert _row(out["byYear"], 2021)["total"] == 0
    assert _row(out["byYear"], 2022)["total"] == 0


def test_response_after_next_oa_does_not_count():
    # Late response that lands AFTER the next CTNF should not be credited
    # to the first CTNF — that response belongs to a different cycle.
    g = _grouped(
        ctnf=[date(2024, 1, 1), date(2024, 6, 1)],
        response=[date(2024, 7, 1)],
    )
    out = compute_extensions_by_year(g)
    # The first CTNF (Jan) gets no qualifying response in its window
    # (the July response is after the June CTNF). The second CTNF (Jun)
    # has a July response — that's only 30 days, not late.
    assert out["totals"]["total"] == 0


def test_response_after_first_noa_not_counted():
    # NOA mailed Apr 1 closes the response window; an extension would have
    # had to land before that.
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        noa=[date(2024, 4, 1)],
        response=[date(2024, 4, 15)],
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["total"] == 0


def test_string_dates_are_parsed():
    g = _grouped(
        ctnf=["2024-01-01"],
        response=["2024-04-15"],
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["ctnf"] == 1


def test_datetime_inputs_normalize_to_date():
    g = _grouped(
        ctfr=[datetime(2023, 1, 1, 9, 30)],
        response=[datetime(2023, 6, 1, 14, 0)],
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["ctfr"] == 1


def test_multiple_extensions_same_app_distinct_oas():
    g = _grouped(
        ctnf=[date(2022, 1, 1)],
        ctfr=[date(2022, 9, 1)],
        # First response answers the CTNF (late), second answers the CTFR (late).
        response=[date(2022, 5, 15), date(2023, 1, 15)],
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["ctnf"] == 1
    assert out["totals"]["ctfr"] == 1
    assert _row(out["byYear"], 2022)["ctnf"] == 1
    assert _row(out["byYear"], 2023)["ctfr"] == 1
    assert out["appsContributing"] == 1


def test_apps_contributing_counts_unique_apps():
    g = {
        1: {
            "ctnf": [date(2024, 1, 1)],
            "ctfr": [], "ctrs": [], "noa": [],
            "response": [date(2024, 5, 1)],
        },
        2: {
            "ctnf": [], "ctfr": [], "noa": [],
            "ctrs": [date(2024, 1, 1)],
            "response": [date(2024, 4, 1)],
        },
        3: {
            "ctnf": [date(2024, 1, 1)],
            "ctfr": [], "ctrs": [], "noa": [],
            "response": [date(2024, 3, 15)],
        },
    }
    out = compute_extensions_by_year(g)
    assert out["appsContributing"] == 2
    assert out["totals"]["total"] == 2
