"""Pure-function KPI / chart aggregates for the Portfolio Explorer."""

from __future__ import annotations

from harness_analytics.portfolio_aggregates import (
    CTNF_RESPONSE_BUCKETS,
    STATUS_PILL,
    compute_charts,
    compute_ctnf_response_speed_to_noa,
    compute_kpis,
    compute_status_mix,
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
