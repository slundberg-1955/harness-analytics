"""Pure-function KPI / chart aggregates for the Portfolio Explorer."""

from __future__ import annotations

from datetime import date

from harness_analytics.portfolio_aggregates import (
    CTNF_RESPONSE_BUCKETS,
    STATUS_PILL,
    apply_recency_window,
    compute_breakdowns,
    compute_charts,
    compute_cohort_trend,
    compute_ctnf_response_speed_to_noa,
    compute_family_yield,
    compute_first_action_allowance,
    compute_foreign_priority_share,
    compute_kpis,
    compute_pendency,
    compute_rce_intensity,
    compute_scope,
    compute_status_mix,
    compute_strategic_abandonment,
    compute_time_to_allowance,
    resolve_recency_window,
    status_label,
    status_tone,
)


def _row(
    app_no: str,
    *,
    status: int | None = 150,
    status_text: str | None = "Patented Case",
    days_to_noa: int | None = 500,
    nonfinal: int = 0,
    final: int = 0,
    interview: bool = False,
    interviews: int = 0,
    noa_within_90: bool = False,
    rce: int = 0,
    is_continuation: bool = False,
    is_jac: bool = False,
    has_child_continuation: bool = False,
    filing_date: date | None = None,
    disposal_date: date | None = None,
    noa_mailed_date: date | None = None,
    months_to_allowance: float | None = None,
    final_rejection_count: int = 0,
    family_root_app_no: str | None = None,
    has_foreign_priority: bool | None = False,
    art_unit: str | None = None,
    has_analytics_row: bool = True,
) -> dict:
    return {
        "application_number": app_no,
        "application_status_code": status,
        "application_status_text": status_text,
        "invention_title": f"Title {app_no}",
        "days_filing_to_noa": days_to_noa,
        "nonfinal_oa_count": nonfinal,
        "final_oa_count": final,
        "had_examiner_interview": interview,
        "interview_count": interviews,
        "noa_within_90_days_of_interview": noa_within_90,
        "rce_count": rce,
        "is_continuation": is_continuation,
        "is_jac": is_jac,
        "has_child_continuation": has_child_continuation,
        # Allowance Analytics v2 fields. Default-NULL so legacy tests that
        # don't pass them continue to test legacy behavior.
        "filing_date": filing_date,
        "disposal_date": disposal_date,
        "noa_mailed_date": noa_mailed_date,
        "months_to_allowance": months_to_allowance,
        "final_rejection_count": final_rejection_count,
        "family_root_app_no": family_root_app_no,
        "has_foreign_priority": has_foreign_priority,
        "group_art_unit": art_unit,
        # Data-quality flag for FAA. Defaults True so legacy tests behave
        # exactly like before (rce/final-rejection ints are trusted). New
        # tests pass has_analytics_row=False to exercise the exclusion path.
        "has_analytics_row": has_analytics_row,
    }


def test_status_label_and_tone_use_map() -> None:
    assert status_label(150, "anything") == "Patented"
    assert status_tone(150) == "emerald"
    # Unknown codes fall back to status text.
    assert status_label(999, "Custom Status") == "Custom Status"
    assert status_tone(999) == STATUS_PILL["_"]["tone"]


def test_kpis_empty_set_is_zeroed() -> None:
    k = compute_kpis([])
    assert k["totalApps"] == 0
    assert k["allowanceRatePct"] == 0.0
    assert k["avgDaysToNoa"] is None
    assert k["medianDaysToNoa"] is None
    assert k["appsWithAtLeastOneOa"] == 0
    assert k["interviewCount"] == 0
    assert k["chmAllowanceRatePct"] == 0.0
    assert k["chmAllowedNoRce"] == 0
    assert k["chmAllowedWithRce"] == 0
    assert k["chmAbandonedNoChild"] == 0


def test_kpis_chm_includes_noa_and_issue_fee_in_allowed() -> None:
    # All "Allowed"-set codes count toward CHM A/CA but not toward Traditional.
    rows = [
        _row("A", status=150, rce=0),                                # A
        _row("B", status=93, status_text="NOA Mailed", rce=0),       # A
        _row("C", status=159, status_text="Issue Fee Verified", rce=2),  # CA
        _row("D", status=161, status_text="Abandoned", has_child_continuation=False),  # AB
    ]
    k = compute_kpis(rows)
    assert k["chmAllowedNoRce"] == 2
    assert k["chmAllowedWithRce"] == 1
    assert k["chmAbandonedNoChild"] == 1
    # (A + CA) / (A + CA + AB) = 3 / 4 = 75.0
    assert k["chmAllowanceRatePct"] == 75.0
    # Traditional uses only Patented (150) over Patented + Abandoned: 1 / 2 = 50.0
    assert k["allowanceRatePct"] == 50.0


def test_kpis_chm_excludes_continued_abandons_from_denominator() -> None:
    # An abandoned matter with a CHM-qualifying child does NOT count as AB.
    rows = [
        _row("A", status=150, rce=0),                                          # A
        _row("B", status=161, status_text="Abandoned", has_child_continuation=True),
        _row("C", status=161, status_text="Abandoned", has_child_continuation=False),  # AB
    ]
    k = compute_kpis(rows)
    assert k["chmAllowedNoRce"] == 1
    assert k["chmAllowedWithRce"] == 0
    assert k["chmAbandonedNoChild"] == 1
    # 1 / (1 + 1) = 50.0
    assert k["chmAllowanceRatePct"] == 50.0
    # Traditional ignores the continuation flag: 1 patented / (1 + 2 abandoned) = 33.3
    assert k["allowanceRatePct"] == 33.3


def test_kpis_chm_zero_when_no_dispositions() -> None:
    # All-pending portfolio: both rates are 0.0 (divide-by-zero guarded).
    rows = [
        _row("A", status=41, status_text="Non-Final"),
        _row("B", status=42, status_text="Final"),
        _row("C", status=30, status_text="Published"),
    ]
    k = compute_kpis(rows)
    assert k["allowanceRatePct"] == 0.0
    assert k["chmAllowanceRatePct"] == 0.0
    assert k["chmAllowedNoRce"] == 0
    assert k["chmAllowedWithRce"] == 0
    assert k["chmAbandonedNoChild"] == 0


def test_kpis_allowance_rate_and_counts() -> None:
    rows = [
        _row("A", status=150),
        _row("B", status=150),
        _row("C", status=161, status_text="Abandoned"),
        _row("D", status=93, status_text="NOA Mailed"),
    ]
    k = compute_kpis(rows)
    assert k["totalApps"] == 4
    assert k["patentedCount"] == 2
    assert k["abandonedCount"] == 1
    assert k["pendingCount"] == 1  # NOA-mailed app
    # 2 patented / (2 + 1 abandoned) = 66.7
    assert k["allowanceRatePct"] == 66.7


def test_kpis_days_to_noa_ignores_nulls() -> None:
    rows = [
        _row("A", days_to_noa=100),
        _row("B", days_to_noa=200),
        _row("C", days_to_noa=None),
    ]
    k = compute_kpis(rows)
    assert k["avgDaysToNoa"] == 150
    assert k["medianDaysToNoa"] == 150


def test_kpis_interview_and_rce_rates() -> None:
    rows = [
        _row("A", interview=True, rce=0),
        _row("B", interview=True, rce=1),
        _row("C", interview=False, rce=2),
        _row("D", interview=False, rce=0),
    ]
    k = compute_kpis(rows)
    assert k["interviewCount"] == 2
    assert k["interviewRatePct"] == 50.0
    assert k["rceCount"] == 2
    assert k["rceRatePct"] == 50.0


def test_kpis_any_oa_flag_counts_either_kind() -> None:
    rows = [
        _row("A", nonfinal=0, final=0),
        _row("B", nonfinal=1, final=0),
        _row("C", nonfinal=0, final=2),
        _row("D", nonfinal=3, final=1),
    ]
    k = compute_kpis(rows)
    assert k["appsWithAtLeastOneOa"] == 3
    # avgOaCount is nonfinal + final combined
    assert k["avgOaCount"] == round((0 + 1 + 2 + 4) / 4, 2)


def test_status_mix_sorts_by_count_desc() -> None:
    rows = [
        _row("A", status=150),
        _row("B", status=150),
        _row("C", status=150),
        _row("D", status=93, status_text="NOA Mailed"),
    ]
    mix = compute_status_mix(rows)
    assert mix[0]["count"] == 3 and mix[0]["label"] == "Patented"
    assert mix[1]["count"] == 1 and mix[1]["label"] == "NOA Mailed"


def test_charts_days_to_noa_sorts_nulls_last() -> None:
    rows = [
        _row("A", days_to_noa=None),
        _row("B", days_to_noa=300),
        _row("C", days_to_noa=100),
    ]
    charts = compute_charts(rows)
    seq = [d["days"] for d in charts["daysToNoaByApp"]]
    assert seq == [100, 300, None]


def test_days_to_noa_histogram_buckets_and_stats() -> None:
    # Mix of values: tight cluster around 50–90 days plus one ~600d outlier.
    rows = [
        _row("A", days_to_noa=50),
        _row("B", days_to_noa=60),
        _row("C", days_to_noa=80),
        _row("D", days_to_noa=85),
        _row("E", days_to_noa=600),
        _row("F", days_to_noa=None),
    ]
    hist = compute_charts(rows)["daysToNoaHistogram"]
    assert hist["totalWithNoa"] == 5
    assert hist["totalWithoutNoa"] == 1
    assert sum(b["count"] for b in hist["bins"]) == 5
    # Bucket widths come from the curated set; for max=600 we expect 90d bins.
    assert hist["binDays"] in (60, 90, 180)
    # Each bin carries a label and a percentage that's a multiple of 1/total.
    for b in hist["bins"]:
        assert "label" in b and isinstance(b["label"], str)
        assert b["pct"] == round(100.0 * b["count"] / hist["totalWithNoa"], 1)
    # Median is the middle of the 5 sorted values: [50,60,80,85,600] -> 80.
    assert hist["median"] == 80


def test_days_to_noa_histogram_empty_when_no_noa_dates() -> None:
    rows = [_row("A", days_to_noa=None), _row("B", days_to_noa=None)]
    hist = compute_charts(rows)["daysToNoaHistogram"]
    assert hist["bins"] == []
    assert hist["totalWithNoa"] == 0
    assert hist["totalWithoutNoa"] == 2
    assert hist["median"] is None and hist["mean"] is None


def test_days_to_noa_histogram_uses_smaller_bins_for_short_ranges() -> None:
    # All values inside 0–90 days: should not collapse to one giant bar.
    rows = [_row(f"X{i}", days_to_noa=d) for i, d in enumerate([10, 20, 30, 60, 75, 88])]
    hist = compute_charts(rows)["daysToNoaHistogram"]
    # We expect at least 5 bars across this range.
    assert len(hist["bins"]) >= 5
    assert hist["binDays"] <= 30


def test_prosecution_signals_noa_within_90_pct() -> None:
    rows = [
        _row("A", interview=True, noa_within_90=True),
        _row("B", interview=True, noa_within_90=False),
        _row("C", interview=False),
    ]
    signals = compute_charts(rows)["prosecutionSignals"]
    # Only interviewed rows factor into the pct; 1 of 2 = 50%.
    assert signals["noaWithin90DaysOfInterviewPct"] == 50.0
    assert signals["continuationTotal"] == 3


# ---------------------------------------------------------------------------
# CTNF response-speed -> outcome chart
# ---------------------------------------------------------------------------


def _ev(days: int, outcome: str, *, response_to_next: int | None = None) -> dict:
    return {
        "daysToResponse": days,
        "outcome": outcome,
        "daysResponseToNext": response_to_next,
    }


def test_ctnf_chart_empty_input_returns_zeroed_skeleton() -> None:
    out = compute_ctnf_response_speed_to_noa([])
    assert out["totalEvents"] == 0
    assert out["overallAllowedPct"] == 0.0
    assert out["medianDaysToResponse"] is None
    # Always all 6 buckets present so the chart axis is stable.
    assert len(out["buckets"]) == len(CTNF_RESPONSE_BUCKETS)
    for b in out["buckets"]:
        assert b["responses"] == 0
        assert b["allowed"] == 0
        assert b["rejected"] == 0
        assert b["pending"] == 0
        assert b["allowedPct"] == 0.0
        assert b["medianDaysResponseToNoa"] is None


def test_ctnf_chart_buckets_by_days_to_response() -> None:
    events = [
        _ev(10, "allowed", response_to_next=40),    # 0-30
        _ev(30, "allowed", response_to_next=20),    # 0-30 (boundary)
        _ev(31, "rejected"),                        # 31-60 (boundary)
        _ev(60, "allowed", response_to_next=15),    # 31-60
        _ev(61, "rejected"),                        # 61-90
        _ev(90, "allowed", response_to_next=10),    # 61-90 (boundary)
        _ev(91, "pending"),                         # 91-120
        _ev(150, "rejected"),                       # 121-180
        _ev(200, "allowed", response_to_next=30),   # 181+
    ]
    out = compute_ctnf_response_speed_to_noa(events)
    by_label = {b["label"]: b for b in out["buckets"]}
    assert by_label["0\u201330d"]["responses"] == 2
    assert by_label["0\u201330d"]["allowed"] == 2
    assert by_label["0\u201330d"]["allowedPct"] == 100.0

    assert by_label["31\u201360d"]["responses"] == 2
    assert by_label["31\u201360d"]["allowed"] == 1
    assert by_label["31\u201360d"]["rejected"] == 1
    assert by_label["31\u201360d"]["allowedPct"] == 50.0

    # Pending events are counted but excluded from the rate denominator.
    assert by_label["91\u2013120d"]["responses"] == 1
    assert by_label["91\u2013120d"]["pending"] == 1
    assert by_label["91\u2013120d"]["allowedPct"] == 0.0

    assert by_label["181d+"]["responses"] == 1
    assert by_label["181d+"]["allowed"] == 1
    assert by_label["181d+"]["allowedPct"] == 100.0

    # Top-level rollups: 5 allowed, 3 rejected, 1 pending = 9 events.
    assert out["totalAllowed"] == 5
    assert out["totalRejected"] == 3
    assert out["totalPending"] == 1
    assert out["totalEvents"] == 9
    # Overall allowance rate excludes the pending event: 5 / (5+3) = 62.5%.
    assert out["overallAllowedPct"] == round(100.0 * 5 / 8, 1)


def test_ctnf_chart_median_response_to_noa_per_bucket() -> None:
    events = [
        _ev(10, "allowed", response_to_next=10),
        _ev(20, "allowed", response_to_next=30),
        _ev(25, "allowed", response_to_next=50),
        _ev(15, "rejected"),
    ]
    out = compute_ctnf_response_speed_to_noa(events)
    by_label = {b["label"]: b for b in out["buckets"]}
    # Median of [10, 30, 50] = 30.
    assert by_label["0\u201330d"]["medianDaysResponseToNoa"] == 30


def test_ctnf_chart_pending_only_keeps_zero_allowance_rate() -> None:
    """A bucket whose only entries are pending should not display an
    artificial 0% allowance rate ratio without context. The numeric field
    stays 0.0 (UI renders "—" when allowed+rejected == 0)."""
    events = [_ev(10, "pending"), _ev(20, "pending")]
    out = compute_ctnf_response_speed_to_noa(events)
    by_label = {b["label"]: b for b in out["buckets"]}
    assert by_label["0\u201330d"]["pending"] == 2
    assert by_label["0\u201330d"]["allowed"] == 0
    assert by_label["0\u201330d"]["rejected"] == 0
    assert by_label["0\u201330d"]["allowedPct"] == 0.0
    assert out["overallAllowedPct"] == 0.0
    assert out["totalEvents"] == 2


def test_ctnf_chart_drops_negative_days_defensively() -> None:
    """Bad ingest data shouldn't crash the chart -- skip silently."""
    events = [_ev(-5, "allowed", response_to_next=10), _ev(50, "allowed", response_to_next=20)]
    out = compute_ctnf_response_speed_to_noa(events)
    assert out["totalAllowed"] == 1
    assert out["totalEvents"] == 1


def test_ctnf_chart_skips_malformed_event_dicts() -> None:
    """Missing daysToResponse field -> drop the row, keep aggregating the rest."""
    events = [{"outcome": "allowed"}, _ev(10, "allowed", response_to_next=5)]
    out = compute_ctnf_response_speed_to_noa(events)
    assert out["totalAllowed"] == 1
    assert out["totalEvents"] == 1


def test_ctnf_chart_overall_pct_is_decided_only() -> None:
    """Overall % must use allowed / (allowed + rejected) -- pending does NOT
    dilute the rate. (Same convention as the per-bucket cells.)"""
    events = [
        _ev(10, "allowed", response_to_next=5),
        _ev(20, "rejected"),
        _ev(25, "pending"),
        _ev(30, "pending"),
    ]
    out = compute_ctnf_response_speed_to_noa(events)
    assert out["totalAllowed"] == 1
    assert out["totalRejected"] == 1
    assert out["totalPending"] == 2
    # 1 / (1+1) = 50% -- pending excluded.
    assert out["overallAllowedPct"] == 50.0


# ---------------------------------------------------------------------------
# Allowance Analytics v2 (spec §10).
# ---------------------------------------------------------------------------


def test_recency_filter_excludes_outside_window() -> None:
    """Spec §10 case 1: 4 inside, 2 outside a 5y window."""
    rows = [
        _row("A", filing_date=date(2024, 1, 1)),
        _row("B", filing_date=date(2023, 6, 1)),
        _row("C", filing_date=date(2022, 9, 1)),
        _row("D", filing_date=date(2021, 12, 1)),
        _row("E-OUTSIDE", filing_date=date(2018, 1, 1)),
        _row("F-OUTSIDE", filing_date=date(2017, 1, 1)),
    ]
    today = date(2026, 4, 29)
    window = resolve_recency_window("5y", None, None, today=today)
    out = apply_recency_window(rows, "filing", window)
    assert {r["application_number"] for r in out} == {"A", "B", "C", "D"}


def test_recency_filter_handles_null_cohort_date() -> None:
    """Spec §10 case 2: NOA-axis row with null noa_mailed_date is excluded."""
    rows = [
        _row("HAS-NOA", noa_mailed_date=date(2024, 1, 1)),
        _row("NO-NOA", noa_mailed_date=None),
    ]
    today = date(2026, 4, 29)
    window = resolve_recency_window("5y", None, None, today=today)
    out = apply_recency_window(rows, "noa", window)
    assert [r["application_number"] for r in out] == ["HAS-NOA"]


def test_recency_window_all_returns_unbounded() -> None:
    assert resolve_recency_window("all", None, None) == (None, None)
    # And `apply_recency_window` should be a no-op for unbounded:
    rows = [_row("A", filing_date=None), _row("B", filing_date=date(2010, 1, 1))]
    out = apply_recency_window(rows, "filing", (None, None))
    assert len(out) == 2


def test_recency_window_custom_uses_supplied_dates() -> None:
    today = date(2026, 4, 29)
    start, end = resolve_recency_window(
        "custom", date(2020, 1, 1), date(2022, 6, 30), today=today
    )
    assert start == date(2020, 1, 1)
    assert end == date(2022, 6, 30)
    # Empty end falls back to today.
    start, end = resolve_recency_window(
        "custom", date(2020, 1, 1), None, today=today
    )
    assert end == today


def test_first_action_allowance_rate_basic() -> None:
    """Spec §10 case 3 / §5.1: numerator counts clean allowances (status 150/93/159
    with rce=0 and final=0); denominator is "closed" = patented + abandoned only.
    Setup: 4 closed (3 patented + 1 abandoned), 2 of the patented are first-action.
    """
    rows = [
        _row("A", status=150, rce=0, final_rejection_count=0),   # FAA num + closed denom
        _row("B", status=150, rce=0, final_rejection_count=0),   # FAA num + closed denom
        _row("C", status=150, rce=1, final_rejection_count=0),   # closed, excluded from num (RCE)
        _row("D", status=161),                                    # closed, excluded from num
    ]
    out = compute_first_action_allowance(rows)
    assert out["count"] == 2
    assert out["denom"] == 4
    assert out["pct"] == 50.0
    assert out["excluded"] == 0


def test_first_action_allowance_excludes_final_rejection() -> None:
    """Spec §10 case 4: status allowed and rce=0 but final_rejection_count>=1
    is NOT a first-action allowance."""
    rows = [
        _row("A", status=150, rce=0, final_rejection_count=1),
        _row("B", status=161),
    ]
    out = compute_first_action_allowance(rows)
    assert out["count"] == 0
    assert out["pct"] == 0.0
    assert out["excluded"] == 0


def test_first_action_allowance_empty_window_returns_none() -> None:
    """Spec §9 empty-window rule: no closed rows -> pct is None, not 0."""
    out = compute_first_action_allowance([])
    assert out["pct"] is None
    assert out["denom"] == 0
    assert out["excluded"] == 0


def test_first_action_allowance_excludes_apps_without_analytics_row() -> None:
    """Data-quality guard: an allowed-class app with no application_analytics
    row (has_analytics_row=False) MUST NOT count toward the FAA numerator.
    Pre-fix the COALESCE coerced rce/final_rejection to 0 and these apps
    masqueraded as first-action allowances, which inflated the prod rate to
    ~75%. They still count toward the closed denominator because the status
    code on `applications` is reliable.
    """
    rows = [
        _row("A", status=150, rce=0, final_rejection_count=0, has_analytics_row=True),
        _row("B", status=150, rce=0, final_rejection_count=0, has_analytics_row=False),
        _row("C", status=150, rce=0, final_rejection_count=0, has_analytics_row=False),
        _row("D", status=161, has_analytics_row=False),  # abandoned: not in num pool anyway
    ]
    out = compute_first_action_allowance(rows)
    assert out["count"] == 1, "only A is a verified first-action allowance"
    assert out["denom"] == 4, "all 4 closed apps still count in the denominator"
    assert out["pct"] == 25.0
    assert out["excluded"] == 2, "B and C are allowed-class but had no analytics data"


def test_first_action_allowance_excluded_does_not_double_count_abandoned() -> None:
    """Abandoned apps without an analytics row should NOT be reported in
    `excluded` — they were never numerator candidates to begin with."""
    rows = [
        _row("A", status=161, has_analytics_row=False),
        _row("B", status=161, has_analytics_row=False),
    ]
    out = compute_first_action_allowance(rows)
    assert out["count"] == 0
    assert out["denom"] == 2
    assert out["excluded"] == 0


def test_first_action_allowance_missing_flag_treats_as_present() -> None:
    """Backward-compat: if a row dict has no `has_analytics_row` key (legacy
    callers, hand-rolled fixtures), treat it as present so the integer
    rce/final_rejection_count fields are trusted as-is."""
    legacy_row = {
        "application_number": "A",
        "application_status_code": 150,
        "rce_count": 0,
        "final_rejection_count": 0,
    }
    out = compute_first_action_allowance([legacy_row])
    assert out["count"] == 1
    assert out["denom"] == 1
    assert out["excluded"] == 0


def test_compute_kpis_surfaces_faa_excluded_count() -> None:
    """The FAA card on the Allowance Analytics tab reads `faaExcluded` from
    the kpis dict; verify it threads through compute_kpis."""
    rows = [
        _row("A", status=150, rce=0, final_rejection_count=0, has_analytics_row=True),
        _row("B", status=150, rce=0, final_rejection_count=0, has_analytics_row=False),
    ]
    k = compute_kpis(rows)
    assert k["faaCount"] == 1
    assert k["faaDenom"] == 2
    assert k["faaExcluded"] == 1


def test_cohort_trend_groups_by_filing_year() -> None:
    """Spec §10 case 5: rows across 2020-2023 -> 4 trend entries."""
    rows = [
        _row("A1", filing_date=date(2020, 3, 1), status=150),
        _row("A2", filing_date=date(2020, 5, 1), status=161),
        _row("B1", filing_date=date(2021, 6, 1), status=150),
        _row("C1", filing_date=date(2022, 7, 1), status=150),
        _row("D1", filing_date=date(2023, 8, 1), status=150),
    ]
    out = compute_cohort_trend(rows, "filing")
    assert [r["year"] for r in out] == [2020, 2021, 2022, 2023]
    assert [r["n"] for r in out] == [2, 1, 1, 1]
    # 2020: 1 patented + 1 abandoned -> Trad = 50%
    assert out[0]["traditionalPct"] == 50.0
    # 2021-2023: all-patented years -> Trad = 100%
    assert out[1]["traditionalPct"] == 100.0


def test_cohort_trend_flags_maturing_when_pending_present() -> None:
    """Spec §10 case 6: any pending row in a year flips maturing=True."""
    rows = [
        _row("A", filing_date=date(2024, 1, 1), status=150),
        _row("B", filing_date=date(2024, 6, 1), status=41, status_text="Non-Final"),
        _row("C", filing_date=date(2023, 1, 1), status=150),
        _row("D", filing_date=date(2023, 6, 1), status=161),
    ]
    out = compute_cohort_trend(rows, "filing")
    by_year = {r["year"]: r for r in out}
    assert by_year[2024]["maturing"] is True
    assert by_year[2023]["maturing"] is False


def test_time_to_allowance_median_p25_p75() -> None:
    """Spec §10 case 7: contrived month deltas -> known percentiles."""
    rows = [_row(f"A{i}", months_to_allowance=m) for i, m in enumerate([10, 20, 30, 40])]
    out = compute_time_to_allowance(rows)
    assert out["medianMonths"] == 25.0
    assert out["p25Months"] == 17.5
    assert out["p75Months"] == 32.5
    assert out["n"] == 4


def test_time_to_allowance_empty_returns_none() -> None:
    """Spec §9 empty-window rule."""
    rows = [_row("A", months_to_allowance=None)]
    out = compute_time_to_allowance(rows)
    assert out["medianMonths"] is None
    assert out["n"] == 0


def test_rce_intensity_among_allowed() -> None:
    """Spec §10 case 8: 5 allowed with RCE counts [0,0,0,1,2].
    avg = 0.6, pct_with_rce = 40.0."""
    rows = [
        _row("A", status=150, rce=0),
        _row("B", status=150, rce=0),
        _row("C", status=93, rce=0),
        _row("D", status=159, rce=1),
        _row("E", status=150, rce=2),
        _row("F-IGNORED", status=161, rce=99),  # Not allowed; ignored.
    ]
    out = compute_rce_intensity(rows)
    assert out["avgRceAmongAllowed"] == 0.6
    assert out["pctAllowancesWithRce"] == 40.0
    assert out["n"] == 5


def test_strategic_abandonment_rate_basic() -> None:
    """Spec §10 case 9: 10 abandoned, 3 with child continuation -> 30.0%."""
    rows = [
        *(_row(f"AB{i}", status=161, has_child_continuation=True) for i in range(3)),
        *(_row(f"AB{i+3}", status=161, has_child_continuation=False) for i in range(7)),
        _row("PAT", status=150),  # Should not affect numerator/denominator.
    ]
    out = compute_strategic_abandonment(rows)
    assert out["pct"] == 30.0
    assert out["withChild"] == 3
    assert out["totalAbandoned"] == 10


def test_strategic_abandonment_zero_abandoned_returns_none() -> None:
    """Spec §10 case 10: 0 abandoned -> pct=None (not zero, not error)."""
    rows = [_row("PAT", status=150)]
    out = compute_strategic_abandonment(rows)
    assert out["pct"] is None
    assert out["totalAbandoned"] == 0


def test_family_yield_counts_others_in_family() -> None:
    """Family of 3 patented siblings -> each contributes 2 OTHER, avg = 2.0."""
    rows = [
        _row("F1", status=150, family_root_app_no="ROOT"),
        _row("F2", status=150, family_root_app_no="ROOT"),
        _row("F3", status=150, family_root_app_no="ROOT"),
        _row("SOLO", status=150, family_root_app_no="SOLO"),  # contributes 0
    ]
    out = compute_family_yield(rows)
    # 3 rows * 2 others + 1 row * 0 others = 6, divide by 4 = 1.5
    assert out["avg"] == 1.5
    assert out["n"] == 4


def test_foreign_priority_share_basic() -> None:
    """Spec §10 case 11: 3 of 5 with foreign priority -> 60.0%."""
    rows = [
        _row("A", has_foreign_priority=True),
        _row("B", has_foreign_priority=True),
        _row("C", has_foreign_priority=True),
        _row("D", has_foreign_priority=False),
        _row("E", has_foreign_priority=None),  # Treated as False (mid-backfill).
    ]
    out = compute_foreign_priority_share(rows)
    assert out["pct"] == 60.0
    assert out["n"] == 3
    assert out["total"] == 5


def test_pendency_uses_open_cohort_only() -> None:
    """Spec §5.6 — pending only, median (today - filing_date) in months."""
    today = date(2025, 1, 1)
    rows = [
        # Pending: 12, 24, 36 months in prosecution.
        _row("P12", status=41, status_text="Non-Final", filing_date=date(2024, 1, 1)),
        _row("P24", status=41, status_text="Non-Final", filing_date=date(2023, 1, 1)),
        _row("P36", status=41, status_text="Non-Final", filing_date=date(2022, 1, 1)),
        # Closed (excluded).
        _row("CLOSED", status=150, filing_date=date(2010, 1, 1)),
    ]
    out = compute_pendency(rows, today=today)
    # Median of [365, 730, 1096] days / 30.44 = 730/30.44 = 23.98
    assert out["n"] == 3
    assert 23.0 < out["medianMonths"] < 25.0


def test_traditional_and_chm_unchanged_when_no_recency_filter() -> None:
    """Spec §10 case 12: regression — old fields stay byte-identical when
    ``recency_window`` is None."""
    rows = [
        _row("A", status=150, rce=0),
        _row("B", status=93, status_text="NOA Mailed", rce=0),
        _row("C", status=159, status_text="Issue Fee Verified", rce=2),
        _row("D", status=161, has_child_continuation=False),
    ]
    k_before = compute_kpis(rows)
    k_after = compute_kpis(rows, cohort_axis="filing", recency_window=None)
    for legacy in (
        "totalApps", "patentedCount", "pendingCount", "abandonedCount",
        "allowanceRatePct", "chmAllowanceRatePct",
        "chmAllowedNoRce", "chmAllowedWithRce", "chmAbandonedNoChild",
    ):
        assert k_before[legacy] == k_after[legacy], f"legacy field {legacy} drifted"


def test_cohort_trend_surfaces_closed_and_faa_excluded() -> None:
    """The cohort chart needs (a) closed-N per year for the n= sub-label
    that exposes survivorship bias, and (b) faaExcluded per year for the
    hover tooltip and color treatment."""
    rows = [
        # 2024: 1 patented w/ analytics, 1 patented w/o analytics, 1 pending.
        _row("A", filing_date=date(2024, 1, 1), status=150, has_analytics_row=True),
        _row("B", filing_date=date(2024, 2, 1), status=150, has_analytics_row=False),
        _row("C", filing_date=date(2024, 3, 1), status=41, status_text="Non-Final"),
    ]
    out = compute_cohort_trend(rows, "filing")
    assert len(out) == 1
    y = out[0]
    assert y["year"] == 2024
    assert y["n"] == 3, "n is total apps in cohort"
    assert y["closed"] == 2, "closed is patented + abandoned (B counts even w/o analytics row)"
    assert y["faaExcluded"] == 1, "B is allowed-class but no analytics row"
    # FAA = 1 (only A) / 2 closed = 50% — pre-fix this would have been 100%.
    assert y["faaPct"] == 50.0
    assert y["maturing"] is True


def test_breakdowns_path_excluded_surfaces_unknown_count() -> None:
    """Allowed-class apps with no analytics row should be reported as
    pathExcluded, NOT silently bucketed into firstAction."""
    rows = [
        _row("A", status=150, rce=0, final_rejection_count=0, has_analytics_row=True),
        _row("B", status=150, rce=0, final_rejection_count=0, has_analytics_row=False),
        _row("C", status=150, rce=1, final_rejection_count=0, has_analytics_row=True),
    ]
    out = compute_breakdowns(rows)
    paths = {b["key"]: b for b in out["byPathToAllowance"]}
    assert paths["firstAction"]["count"] == 1, "only A is verified first-action"
    assert paths["after1Rce"]["count"] == 1, "C had 1 RCE"
    assert out["pathExcluded"] == 1, "B is allowed but unknown path"
    assert out["pathTotalAllowed"] == 3
    # Shares are denominated against the classifiable subset (2), not 3.
    assert paths["firstAction"]["sharePct"] == 50.0
    assert paths["after1Rce"]["sharePct"] == 50.0


def test_breakdowns_art_unit_carries_faa_excluded_per_row() -> None:
    """byArtUnit rows must carry faaExcluded so the table can flag the
    art units whose FAA is built on incomplete data."""
    rows = [
        _row("A", art_unit="2444", status=150, has_analytics_row=True),
        _row("B", art_unit="2444", status=150, has_analytics_row=False),
        _row("C", art_unit="2444", status=161),
        _row("D", art_unit="3686", status=150, has_analytics_row=True),
    ]
    out = compute_breakdowns(rows)
    aus = {r["artUnit"]: r for r in out["byArtUnit"]}
    assert aus["2444"]["closed"] == 3
    assert aus["2444"]["faaExcluded"] == 1, "B in 2444 is allowed-class no-analytics"
    assert aus["3686"]["faaExcluded"] == 0


def test_breakdowns_art_unit_top_n_and_path_buckets() -> None:
    rows = [
        # Art unit 2444 has 3 closed: 2 patented + 1 abandoned -> trad ~66.7%.
        _row("A1", art_unit="2444", status=150),
        _row("A2", art_unit="2444", status=150),
        _row("A3", art_unit="2444", status=161),
        # Art unit 3686 has 1 closed (abandoned).
        _row("B1", art_unit="3686", status=161),
        # No-AU row should be dropped from byArtUnit.
        _row("NOAU", art_unit=None, status=150),
        # Path buckets: A1 (rce=0,fr=0) -> firstAction; A2 (rce=0,fr=1) -> afterOaNoRce.
        # We force the second case via overrides:
    ]
    rows[1]["final_rejection_count"] = 1  # A2 -> afterOaNoRce
    out = compute_breakdowns(rows)
    aus = {r["artUnit"]: r for r in out["byArtUnit"]}
    assert "2444" in aus
    assert aus["2444"]["closed"] == 3
    assert aus["2444"]["tradPct"] == round(100 * 2 / 3, 1)
    # No-AU row dropped from breakdown.
    assert all(r["artUnit"] for r in out["byArtUnit"])

    paths = {b["key"]: b for b in out["byPathToAllowance"]}
    assert paths["firstAction"]["count"] == 2  # A1 + NOAU (both rce=0,fr=0,allowed)
    assert paths["afterOaNoRce"]["count"] == 1  # A2 (rce=0,fr=1)
    assert paths["after1Rce"]["count"] == 0
    assert paths["after2PlusRce"]["count"] == 0


def test_compute_scope_counts_open_and_closed() -> None:
    rows = [
        _row("A", status=150),
        _row("B", status=161),
        _row("C", status=41, status_text="Non-Final"),
    ]
    s = compute_scope(rows)
    assert s == {"totalInWindow": 3, "closedInWindow": 2, "pendingInWindow": 1}


def test_kpis_secondary_metrics_surface_in_response() -> None:
    """Smoke test: every new KPI key from spec §5 appears on compute_kpis."""
    rows = [
        _row("A", status=150, rce=0, months_to_allowance=24.0,
             family_root_app_no="A", has_foreign_priority=True,
             filing_date=date(2024, 1, 1)),
        _row("B", status=161, has_child_continuation=False,
             filing_date=date(2023, 6, 1)),
    ]
    k = compute_kpis(rows)
    for key in (
        "faaPct", "faaCount", "faaDenom", "timeToAllowance",
        "rceIntensity", "strategicAbandonment", "familyYield",
        "pendency", "foreignPriority",
    ):
        assert key in k, f"missing KPI key {key}"
