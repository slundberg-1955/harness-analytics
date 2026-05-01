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
            "rem": kwargs.get("rem", []),
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
    assert out["totals"] == {
        "ctnf": 0, "ctfr": 0, "restriction": 0, "total": 0,
        "oneMonth": 0, "twoMonth": 0, "threeMonth": 0, "fourPlus": 0,
    }
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


def test_duration_bucket_one_month_just_past_deadline():
    """Response 1 day past the 3-month CTNF deadline -> 1-month bucket."""
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        response=[date(2024, 4, 2)],
    )
    out = compute_extensions_by_year(g)
    row = _row(out["byYear"], 2024)
    assert row["oneMonth"] == 1
    assert row["twoMonth"] == 0
    assert row["threeMonth"] == 0
    assert row["fourPlus"] == 0
    assert out["totals"]["oneMonth"] == 1


def test_duration_bucket_two_months():
    """Response ~1 month + 1 day past the deadline -> 2-month bucket."""
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        response=[date(2024, 5, 2)],
    )
    out = compute_extensions_by_year(g)
    row = _row(out["byYear"], 2024)
    assert row["twoMonth"] == 1
    assert row["oneMonth"] == 0
    assert out["totals"]["twoMonth"] == 1


def test_duration_bucket_three_months():
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        response=[date(2024, 6, 2)],
    )
    out = compute_extensions_by_year(g)
    row = _row(out["byYear"], 2024)
    assert row["threeMonth"] == 1
    assert out["totals"]["threeMonth"] == 1


def test_duration_bucket_four_plus():
    """Response 4+ months past deadline -> fourPlus catch-all bucket."""
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        response=[date(2024, 8, 15)],
    )
    out = compute_extensions_by_year(g)
    row = _row(out["byYear"], 2024)
    assert row["fourPlus"] == 1
    assert row["threeMonth"] == 0
    assert out["totals"]["fourPlus"] == 1


def test_duration_bucket_ctrs_uses_2_month_deadline():
    """CTRS deadline is 2 months, so a 3-month response is 1-month past."""
    g = _grouped(
        ctrs=[date(2024, 1, 1)],
        response=[date(2024, 4, 2)],
    )
    out = compute_extensions_by_year(g)
    row = _row(out["byYear"], 2024)
    assert row["restriction"] == 1
    assert row["twoMonth"] == 1, "Apr 2 is just past Mar 1 (2-mo deadline) by 1 month + 1 day -> 2-month bucket"


def test_duration_buckets_sum_to_total():
    """Per-row bucket sums equal the type-bucket sums (= total)."""
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        ctfr=[date(2024, 9, 1)],  # follow-up after CTNF response (close enough not to be invalid)
        response=[date(2024, 4, 15), date(2025, 2, 1)],
    )
    out = compute_extensions_by_year(g)
    for row in out["byYear"]:
        type_total = row["ctnf"] + row["ctfr"] + row["restriction"]
        bucket_total = row["oneMonth"] + row["twoMonth"] + row["threeMonth"] + row["fourPlus"]
        assert type_total == bucket_total == row["total"]


# ---------------------------------------------------------------------------
# REM (Applicant Remarks) closes the CTRS response window. A REM mailed after
# a CTRS — whether responding to that CTRS, a later CTNF, or a later CTFR —
# caps the CTRS horizon so a far-later RESPONSE_NONFINAL doesn't get
# attributed back to the restriction and inflate the 4+mo bucket.
# ---------------------------------------------------------------------------


def test_rem_caps_ctrs_window_so_far_later_merits_response_isnt_attributed():
    """The user-facing motivation for the REM closer: a CTRS sits open
    while a later CTNF + RESPONSE_NONFINAL play out far past the 2-month
    SSP. Without the REM cap, the CTRS would credit the merits response
    as its "response" and land in the 4+mo duration bucket; with the cap,
    the CTRS attributes to the REM mailed shortly after the SSP and lands
    in the 1-month bucket instead.
    """
    g = _grouped(
        ctrs=[date(2024, 1, 1)],            # SSP deadline = Mar 1
        rem=[date(2024, 3, 15)],            # 14d past SSP -> 1-month bucket
        ctnf=[date(2024, 4, 1)],
        # Real merits response 6+ months after the CTRS SSP. Without the
        # REM cap, this date would be the CTRS "response" and the row
        # would land in fourPlus.
        response=[date(2024, 9, 1)],
    )
    out = compute_extensions_by_year(g)
    totals = out["totals"]
    assert totals["restriction"] == 1
    assert totals["fourPlus"] == 0, totals
    assert totals["oneMonth"] == 1, totals
    assert _row(out["byYear"], 2024)["restriction"] == 1
    assert _row(out["byYear"], 2024)["oneMonth"] == 1


def test_rem_alone_serves_as_ctrs_response_when_no_classified_response():
    """If prosecution_events doesn't classify a response (e.g., Election with
    traverse fell through the cracks), but a REM is in the IFW, the REM
    date alone produces a measurable CTRS lateness instead of silently
    dropping the row.
    """
    g = _grouped(
        ctrs=[date(2024, 1, 1)],
        # Mar 15 = 14 days past the Mar 1 deadline -> rounds up to 1-month
        # extension (USPTO sells extensions in whole-month blocks).
        rem=[date(2024, 3, 15)],
        response=[],
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["restriction"] == 1
    assert _row(out["byYear"], 2024)["oneMonth"] == 1


def test_rem_does_not_close_ctnf_or_ctfr_window():
    """REM only adjusts the CTRS horizon. CTNF/CTFR keep their original
    horizon (next OA / first NOA), so a late merits response still lands
    in the right bucket against the right OA.
    """
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        rem=[date(2024, 3, 15)],  # On-time CTNF response in REM form
        response=[date(2024, 7, 1)],  # Real (late) RESPONSE_NONFINAL
    )
    out = compute_extensions_by_year(g)
    # CTNF deadline = Apr 1; response = Jul 1 -> 3 months past -> threeMonth bucket.
    assert out["totals"]["ctnf"] == 1, out["totals"]
    assert out["totals"]["restriction"] == 0
    assert _row(out["byYear"], 2024)["threeMonth"] == 1


def test_rem_before_ctrs_does_not_close_window():
    """A REM mailed before the CTRS (e.g., preliminary remarks at filing) is
    ignored by the boundary check (`v <= t0` in _earliest_after).
    """
    g = _grouped(
        ctrs=[date(2024, 6, 1)],
        rem=[date(2024, 3, 1)],            # pre-CTRS, must be ignored
        response=[date(2024, 9, 30)],      # ~4 months past Aug 1 deadline -> 2mo
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["restriction"] == 1
    assert _row(out["byYear"], 2024)["twoMonth"] == 1
