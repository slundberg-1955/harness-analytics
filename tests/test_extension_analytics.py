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
            "elc": kwargs.get("elc", []),
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


# ---------------------------------------------------------------------------
# ELC. / ELECTION (Election filed in response to a Restriction Requirement)
# closes the CTRS response window the same way REM does. Election is the
# textbook applicant response to a restriction; classifier.EVENT_TYPE_MAP
# does not surface it as RESPONSE_NONFINAL/FINAL/RCE, so the calculator
# would otherwise miss the actual response entirely.
# ---------------------------------------------------------------------------


def test_elc_alone_serves_as_ctrs_response_when_no_classified_response():
    """An on-time-ish Election alone (no RESPONSE_NONFINAL/FINAL/RCE,
    no REM) must still produce a measurable lateness against the CTRS
    2-month deadline so we don't silently drop the row.
    """
    g = _grouped(
        ctrs=[date(2024, 1, 1)],
        # Mar 15 = 14 days past the Mar 1 deadline -> rounds up to 1-month.
        elc=[date(2024, 3, 15)],
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["restriction"] == 1
    assert _row(out["byYear"], 2024)["oneMonth"] == 1


def test_elc_caps_ctrs_window_so_far_later_merits_response_isnt_attributed():
    """Mirrors the REM test: a CTRS sits open while a later CTNF +
    RESPONSE_NONFINAL play out far past the 2-month SSP. With an Election
    on file shortly after the SSP, the CTRS attributes to the Election and
    lands in the 1-month bucket instead of fourPlus.
    """
    g = _grouped(
        ctrs=[date(2024, 1, 1)],
        elc=[date(2024, 3, 15)],            # 14d past SSP -> 1-month bucket
        ctnf=[date(2024, 4, 1)],
        response=[date(2024, 9, 1)],        # late merits response, NOT the CTRS response
    )
    out = compute_extensions_by_year(g)
    totals = out["totals"]
    assert totals["restriction"] == 1
    assert totals["fourPlus"] == 0, totals
    assert totals["oneMonth"] == 1, totals
    assert _row(out["byYear"], 2024)["restriction"] == 1
    assert _row(out["byYear"], 2024)["oneMonth"] == 1


def test_on_time_elc_does_not_count_as_extension():
    """An Election filed at-or-before the 2-month CTRS deadline is not an
    extension, even though it does close the response window.
    """
    g = _grouped(
        ctrs=[date(2024, 1, 1)],
        elc=[date(2024, 2, 28)],            # within 2 months
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["restriction"] == 0
    assert out["byYear"] == []


def test_elc_does_not_close_ctnf_or_ctfr_window():
    """ELC. only adjusts the CTRS horizon. CTNF/CTFR keep their original
    horizon (next OA / first NOA), so a late merits response still lands
    in the right bucket against the right OA.
    """
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        elc=[date(2024, 3, 15)],            # would NOT close a CTNF window
        response=[date(2024, 7, 1)],        # late RESPONSE_NONFINAL
    )
    out = compute_extensions_by_year(g)
    # CTNF deadline = Apr 1; response = Jul 1 -> 3 months past -> threeMonth.
    assert out["totals"]["ctnf"] == 1, out["totals"]
    assert out["totals"]["restriction"] == 0
    assert _row(out["byYear"], 2024)["threeMonth"] == 1


def test_elc_before_ctrs_does_not_close_window():
    """A pre-CTRS Election (rare but possible if the IFW carries an old
    code) is ignored by the boundary check.
    """
    g = _grouped(
        ctrs=[date(2024, 6, 1)],
        elc=[date(2024, 3, 1)],             # pre-CTRS, must be ignored
        response=[date(2024, 9, 30)],       # ~4 months past Aug 1 deadline -> 2mo
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["restriction"] == 1
    assert _row(out["byYear"], 2024)["twoMonth"] == 1


def test_compute_extensions_by_year_flags_current_year_partial():
    """The current calendar year row should be flagged ``isPartial=True`` so
    the frontend can overlay a YTD projection. Prior-year rows — including
    zero-filled gap rows — must stay ``isPartial=False``.
    """
    g = {
        1: {
            "ctnf": [date(2023, 1, 1)],
            "ctfr": [], "ctrs": [], "noa": [],
            "response": [date(2023, 5, 1)],
            "rem": [], "elc": [],
        },
        2: {
            "ctnf": [date(2026, 1, 1)],
            "ctfr": [], "ctrs": [], "noa": [],
            "response": [date(2026, 5, 1)],
            "rem": [], "elc": [],
        },
    }
    out = compute_extensions_by_year(g, today=date(2026, 5, 1))
    flags = {r["year"]: r["isPartial"] for r in out["byYear"]}
    assert flags == {2023: False, 2024: False, 2025: False, 2026: True}


def test_compute_extensions_by_year_default_today_uses_date_today(monkeypatch):
    """When ``today`` is not passed, the function falls back to
    ``date.today()`` — verify the current-year row is still flagged.
    """
    g = _grouped(
        ctnf=[date(2024, 1, 1)],
        response=[date(2024, 5, 1)],
    )
    out = compute_extensions_by_year(g)
    today_year = date.today().year
    flags = {r["year"]: r["isPartial"] for r in out["byYear"]}
    if today_year in flags:
        assert flags[today_year] is True
    for y, p in flags.items():
        if y != today_year:
            assert p is False


def test_earlier_of_elc_or_rem_is_the_ctrs_response():
    """If both an Election and a REM are on file after a CTRS, the earlier
    one is the response (boundary closes at min, candidates union takes
    the earliest). Verifies that adding ELC. doesn't accidentally let a
    later REM beat an earlier ELC..
    """
    g = _grouped(
        ctrs=[date(2024, 1, 1)],
        elc=[date(2024, 3, 15)],            # 14d past Mar 1 -> 1-month
        rem=[date(2024, 5, 1)],             # 2mo past Mar 1 -> would be 2-month
    )
    out = compute_extensions_by_year(g)
    assert out["totals"]["restriction"] == 1
    assert _row(out["byYear"], 2024)["oneMonth"] == 1
    assert _row(out["byYear"], 2024)["twoMonth"] == 0
