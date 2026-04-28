"""Pure-Python KPI and chart aggregates for the Portfolio Explorer.

All functions operate on the already-filtered row list (list of dicts with the
`patent_applications` view column names) so they can be unit-tested without a
database. Keep this module free of FastAPI / SQLAlchemy imports.
"""

from __future__ import annotations

import math
from statistics import mean, median
from typing import Any, Iterable, Optional


# Status code -> pill label/tone (spec §8). `_` is the fallback entry the UI
# falls back to when a status code is not in the map.
STATUS_PILL: dict[int | str, dict[str, str]] = {
    150: {"label": "Patented", "tone": "emerald"},
    93:  {"label": "NOA Mailed", "tone": "blue"},
    41:  {"label": "Non-Final", "tone": "amber"},
    42:  {"label": "Final", "tone": "rose"},
    161: {"label": "Abandoned", "tone": "slate"},
    30:  {"label": "Published", "tone": "violet"},
    "_": {"label": "Other", "tone": "slate"},
}


def status_label(code: int | None, text: str | None) -> str:
    if code is not None and code in STATUS_PILL:
        return STATUS_PILL[code]["label"]
    return (text or "").strip() or (f"Status {code}" if code is not None else "Unknown")


def status_tone(code: int | None) -> str:
    if code is not None and code in STATUS_PILL:
        return STATUS_PILL[code]["tone"]
    return STATUS_PILL["_"]["tone"]


def _get_int(row: dict[str, Any], key: str, default: int = 0) -> int:
    v = row.get(key)
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _days_to_noa_values(rows: Iterable[dict[str, Any]]) -> list[int]:
    return [
        int(r["days_filing_to_noa"])
        for r in rows
        if r.get("days_filing_to_noa") is not None
    ]


# Status codes treated as "Allowed" for the Carley-Hegde-Marco rate.
# 150 = Patented Case, 93 = NOA Mailed, 159 = Issue Fee Payment Verified.
# (The Traditional rate intentionally still uses Patented (150) only.)
_CHM_ALLOWED_STATUS_CODES: frozenset[int] = frozenset({150, 93, 159})


def _deadlines_within(rows: Iterable[dict[str, Any]], days: int) -> int:
    """Count rows whose next_deadline_date falls within ``days`` of today.

    Today's date is computed at call time (UTC date is fine for KPI counts —
    USPTO deadlines are date-only). Rows without a next_deadline_date are
    excluded. Past-due dates count for ``days >= 0`` so the "Due in 30d" KPI
    naturally absorbs overdue items into the same number — that's the design
    doc behavior.
    """
    from datetime import date as _date

    today = _date.today()
    cutoff_days = days
    out = 0
    for r in rows:
        nd = r.get("next_deadline_date")
        if nd is None:
            continue
        if hasattr(nd, "isoformat"):
            d = nd
        else:
            try:
                d = _date.fromisoformat(str(nd)[:10])
            except ValueError:
                continue
        delta = (d - today).days
        if delta <= cutoff_days:
            out += 1
    return out


def compute_kpis(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    patented = sum(1 for r in rows if r.get("application_status_code") == 150)
    abandoned = sum(1 for r in rows if r.get("application_status_code") == 161)
    # Pending = anything that is neither patented nor abandoned. Matches the
    # mockup's "13 patented · 2 pending" subtitle for a 15-row portfolio.
    pending = total - patented - abandoned

    # Traditional (#1) rate: Patented / (Patented + Abandoned). Pending matters
    # are excluded from both numerator and denominator.
    allowance_denom = patented + abandoned
    allowance_pct = round(100.0 * patented / allowance_denom, 1) if allowance_denom else 0.0

    # Carley-Hegde-Marco (#2) "true" allowance rate:
    #   (A + CA) / (A + CA + AB)
    #     A  = Allowed without ever filing an RCE
    #     CA = Allowed after one or more RCEs
    #     AB = Abandoned without a subsequent continuation/CIP/divisional
    chm_allowed_no_rce = 0
    chm_allowed_with_rce = 0
    chm_abandoned_no_child = 0
    for r in rows:
        code = r.get("application_status_code")
        if code in _CHM_ALLOWED_STATUS_CODES:
            if _get_int(r, "rce_count") >= 1:
                chm_allowed_with_rce += 1
            else:
                chm_allowed_no_rce += 1
        elif code == 161 and not r.get("has_child_continuation"):
            chm_abandoned_no_child += 1
    chm_num = chm_allowed_no_rce + chm_allowed_with_rce
    chm_den = chm_num + chm_abandoned_no_child
    chm_pct = round(100.0 * chm_num / chm_den, 1) if chm_den else 0.0

    days = _days_to_noa_values(rows)
    avg_days = round(mean(days), 0) if days else None
    med_days = round(median(days), 0) if days else None

    oa_counts = [
        _get_int(r, "nonfinal_oa_count") + _get_int(r, "final_oa_count") for r in rows
    ]
    avg_oa = round(mean(oa_counts), 2) if oa_counts else 0.0
    apps_with_any_oa = sum(1 for n in oa_counts if n > 0)

    interview_count = sum(1 for r in rows if r.get("had_examiner_interview"))
    interview_rate = round(100.0 * interview_count / total, 1) if total else 0.0

    rce_count = sum(1 for r in rows if _get_int(r, "rce_count") > 0)
    rce_rate = round(100.0 * rce_count / total, 1) if total else 0.0

    return {
        "totalApps": total,
        "patentedCount": patented,
        "pendingCount": pending,
        "abandonedCount": abandoned,
        "allowanceRatePct": allowance_pct,
        # Prior-period delta is not tracked yet; surfaced as 0.0 so the UI can
        # show a neutral indicator. Spec calls for it but v1 ships without it.
        "allowanceRateDeltaPctPts": 0.0,
        "chmAllowanceRatePct": chm_pct,
        "chmAllowedNoRce": chm_allowed_no_rce,
        "chmAllowedWithRce": chm_allowed_with_rce,
        "chmAbandonedNoChild": chm_abandoned_no_child,
        "avgDaysToNoa": int(avg_days) if avg_days is not None else None,
        "medianDaysToNoa": int(med_days) if med_days is not None else None,
        "avgOaCount": avg_oa,
        "appsWithAtLeastOneOa": apps_with_any_oa,
        "interviewRatePct": interview_rate,
        "interviewCount": interview_count,
        "rceRatePct": rce_rate,
        "rceCount": rce_count,
        # M7: Deadlines Due (30d) KPI. Pulls from the
        # patent_applications view's next_deadline_date column, which is
        # populated by a correlated subquery on computed_deadlines.
        "deadlinesDue30d": _deadlines_within(rows, 30),
        "overdueDeadlines": sum(
            (_get_int(r, "overdue_deadline_count")) for r in rows
        ),
        "openDeadlines": sum(
            (_get_int(r, "open_deadline_count")) for r in rows
        ),
    }


def compute_days_to_noa_by_app(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sorted ascending by days (nulls last so muted bars cluster on the right).

    Kept for backwards compatibility; UI now consumes ``daysToNoaHistogram``.
    """
    ordered = sorted(
        rows,
        key=lambda r: (
            r.get("days_filing_to_noa") is None,
            r.get("days_filing_to_noa") or 0,
        ),
    )
    return [
        {
            "applicationNumber": r.get("application_number"),
            "title": r.get("invention_title"),
            "days": r["days_filing_to_noa"] if r.get("days_filing_to_noa") is not None else None,
        }
        for r in ordered
    ]


# Bucket widths (in days) the histogram is allowed to use, in increasing order.
# All values are multiples of 30 days (or aligned to 365) so the auto-generated
# labels read as whole months / years.
_HISTOGRAM_BIN_CANDIDATES_DAYS: tuple[int, ...] = (15, 30, 60, 90, 180, 365, 730)


def _pick_histogram_bin_days(max_days: int) -> int:
    """Pick a bin width yielding ~5–12 bars across [0, max_days]."""
    for c in _HISTOGRAM_BIN_CANDIDATES_DAYS:
        if math.ceil((max_days + 1) / c) <= 12:
            chosen = c
            break
    else:
        chosen = _HISTOGRAM_BIN_CANDIDATES_DAYS[-1]
    # Step down to a smaller candidate when the chart would otherwise have
    # fewer than 5 bars (looks sparse and uninformative).
    while (
        math.ceil((max_days + 1) / chosen) < 5
        and chosen > _HISTOGRAM_BIN_CANDIDATES_DAYS[0]
    ):
        idx = _HISTOGRAM_BIN_CANDIDATES_DAYS.index(chosen)
        chosen = _HISTOGRAM_BIN_CANDIDATES_DAYS[idx - 1]
    return chosen


def _format_histogram_bin_label(lo: int, hi_exclusive: int, bin_days: int) -> str:
    if bin_days < 30:
        return f"{lo}\u2013{hi_exclusive - 1}d"
    if bin_days % 365 == 0 and lo % 365 == 0 and hi_exclusive % 365 == 0:
        return f"{lo // 365}\u2013{hi_exclusive // 365}y"
    lo_m = round(lo / 30)
    hi_m = round(hi_exclusive / 30)
    return f"{lo_m}\u2013{hi_m}mo"


def compute_days_to_noa_histogram(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Histogram of days-from-filing-to-NOA for the filtered set.

    Returns auto-sized bins, the median / mean for marker rendering, and counts
    of apps with vs. without an NOA so the UI can disclose what's missing.
    """
    days_values: list[int] = [
        int(r["days_filing_to_noa"])
        for r in rows
        if r.get("days_filing_to_noa") is not None
    ]
    no_noa_count = sum(1 for r in rows if r.get("days_filing_to_noa") is None)

    if not days_values:
        return {
            "bins": [],
            "binDays": 0,
            "median": None,
            "mean": None,
            "totalWithNoa": 0,
            "totalWithoutNoa": no_noa_count,
        }

    max_v = max(days_values)
    bin_days = _pick_histogram_bin_days(max_v)
    n_bins = max(1, math.ceil((max_v + 1) / bin_days))

    counts = [0] * n_bins
    for v in days_values:
        idx = min(n_bins - 1, v // bin_days)
        counts[idx] += 1

    total = len(days_values)
    bins: list[dict[str, Any]] = []
    for i, count in enumerate(counts):
        lo = i * bin_days
        hi_exclusive = (i + 1) * bin_days
        bins.append(
            {
                "label": _format_histogram_bin_label(lo, hi_exclusive, bin_days),
                "minDays": lo,
                "maxDays": hi_exclusive - 1,
                "count": count,
                "pct": round(100.0 * count / total, 1),
            }
        )

    return {
        "bins": bins,
        "binDays": bin_days,
        "median": int(median(days_values)),
        "mean": round(sum(days_values) / total, 1),
        "totalWithNoa": total,
        "totalWithoutNoa": no_noa_count,
    }


def compute_status_mix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tallies: dict[int | None, dict[str, Any]] = {}
    for r in rows:
        code = r.get("application_status_code")
        label = status_label(code, r.get("application_status_text"))
        key = code if code is not None else -1
        entry = tallies.setdefault(
            key,
            {"label": label, "code": code, "count": 0, "tone": status_tone(code)},
        )
        entry["count"] += 1
    # Largest slices first so the donut looks stable.
    return sorted(tallies.values(), key=lambda e: (-e["count"], e["label"]))


def compute_prosecution_signals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    if not total:
        return {
            "avgNonfinalOa": 0.0,
            "avgFinalOa": 0.0,
            "avgInterviews": 0.0,
            "noaWithin90DaysOfInterviewPct": 0.0,
            "continuationCount": 0,
            "continuationTotal": 0,
            "jacCount": 0,
        }
    avg_nonfinal = round(
        mean(_get_int(r, "nonfinal_oa_count") for r in rows), 2
    )
    avg_final = round(mean(_get_int(r, "final_oa_count") for r in rows), 2)
    avg_interviews = round(mean(_get_int(r, "interview_count") for r in rows), 2)

    interviewed = [r for r in rows if r.get("had_examiner_interview")]
    if interviewed:
        within_90 = sum(1 for r in interviewed if r.get("noa_within_90_days_of_interview"))
        noa_pct = round(100.0 * within_90 / len(interviewed), 1)
    else:
        noa_pct = 0.0

    continuations = sum(1 for r in rows if r.get("is_continuation"))
    jacs = sum(1 for r in rows if r.get("is_jac"))

    return {
        "avgNonfinalOa": avg_nonfinal,
        "avgFinalOa": avg_final,
        "avgInterviews": avg_interviews,
        "noaWithin90DaysOfInterviewPct": noa_pct,
        "continuationCount": continuations,
        "continuationTotal": total,
        "jacCount": jacs,
    }


# Bucket edges for the CTNF response-speed -> outcome chart, in days from
# CTNF mail to the applicant's response.
#
# 0-30 / 31-60 / 61-90 / 91-120 / 121-180 land on USPTO-meaningful month
# boundaries (1, 2, 3, 4, 6 months) so the bars line up with how a
# prosecutor actually thinks about the response window. Beyond 180d we
# collapse into a single "181+" bucket -- past the statutory cliff,
# response-speed effects are dominated by petition-revival mechanics, not
# examiner behavior, so finer granularity isn't informative.
CTNF_RESPONSE_BUCKETS: tuple[tuple[str, int, Optional[int]], ...] = (
    ("0\u201330d", 0, 30),
    ("31\u201360d", 31, 60),
    ("61\u201390d", 61, 90),
    ("91\u2013120d", 91, 120),
    ("121\u2013180d", 121, 180),
    ("181d+", 181, None),
)


def _bucket_index_for_days(days: int) -> int:
    for i, (_label, lo, hi) in enumerate(CTNF_RESPONSE_BUCKETS):
        if days < lo:
            continue
        if hi is None or days <= hi:
            return i
    return len(CTNF_RESPONSE_BUCKETS) - 1


def compute_ctnf_response_speed_to_noa(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Bucket per-CTNF outcome events by days-to-respond, compute allowance %.

    ``events`` is the list-of-dicts produced from
    ``ctnf_outcome.extract_outcomes_from_grouped_events`` (each dict has
    ``daysToResponse``, ``outcome``, optional ``daysResponseToNext``).

    Per bucket we emit:
      * ``responses``  total CTNFs whose response landed in this bucket
      * ``allowed``    next examiner action was an NOA
      * ``rejected``   next examiner action was another CTNF / CTFR
      * ``pending``    no successor examiner action yet
      * ``allowedPct`` allowed / (allowed + rejected) -- pending excluded
                       so a still-prosecuting cohort doesn't drag the rate
      * ``medianDaysResponseToNoa`` median days from response to NOA among
                       the ``allowed`` events in this bucket (handy for
                       "fast response also means fast allowance" stories)

    Top-level fields:
      ``buckets`` is the per-bucket list (always all 6 buckets, even when
      empty, so the chart axis stays stable across filter changes).
      ``totalEvents`` / ``totalAllowed`` / ``totalRejected`` /
      ``totalPending`` are corpus-wide.
      ``overallAllowedPct`` is allowed / (allowed + rejected) across the
      whole filtered set.
      ``medianDaysToResponse`` is the median days-to-respond across all
      events (sanity gut-check vs the chart).
    """
    buckets: list[dict[str, Any]] = [
        {
            "label": label,
            "minDays": lo,
            "maxDays": hi,
            "responses": 0,
            "allowed": 0,
            "rejected": 0,
            "pending": 0,
            "allowedPct": 0.0,
            "medianDaysResponseToNoa": None,
        }
        for label, lo, hi in CTNF_RESPONSE_BUCKETS
    ]
    response_to_noa_days_per_bucket: list[list[int]] = [
        [] for _ in CTNF_RESPONSE_BUCKETS
    ]
    days_to_response_all: list[int] = []

    total_allowed = 0
    total_rejected = 0
    total_pending = 0

    for ev in events:
        try:
            d = int(ev["daysToResponse"])
        except (KeyError, TypeError, ValueError):
            continue
        if d < 0:
            # Defensive: a response can't predate the CTNF in the real
            # world, but bad ingest data shouldn't crash the chart.
            continue
        idx = _bucket_index_for_days(d)
        b = buckets[idx]
        b["responses"] += 1
        days_to_response_all.append(d)
        outcome = ev.get("outcome")
        if outcome == "allowed":
            b["allowed"] += 1
            total_allowed += 1
            r2n = ev.get("daysResponseToNext")
            if isinstance(r2n, int) and r2n >= 0:
                response_to_noa_days_per_bucket[idx].append(r2n)
        elif outcome == "rejected":
            b["rejected"] += 1
            total_rejected += 1
        elif outcome == "pending":
            b["pending"] += 1
            total_pending += 1

    for i, b in enumerate(buckets):
        decided = b["allowed"] + b["rejected"]
        if decided:
            b["allowedPct"] = round(100.0 * b["allowed"] / decided, 1)
        if response_to_noa_days_per_bucket[i]:
            b["medianDaysResponseToNoa"] = int(
                median(response_to_noa_days_per_bucket[i])
            )

    decided_total = total_allowed + total_rejected
    overall_allowed_pct = (
        round(100.0 * total_allowed / decided_total, 1) if decided_total else 0.0
    )
    median_dtr = (
        int(median(days_to_response_all)) if days_to_response_all else None
    )

    return {
        "buckets": buckets,
        "totalEvents": total_allowed + total_rejected + total_pending,
        "totalAllowed": total_allowed,
        "totalRejected": total_rejected,
        "totalPending": total_pending,
        "overallAllowedPct": overall_allowed_pct,
        "medianDaysToResponse": median_dtr,
    }


def compute_charts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "daysToNoaHistogram": compute_days_to_noa_histogram(rows),
        # `daysToNoaByApp` retained for any external consumers; the in-app UI
        # now uses the histogram above.
        "daysToNoaByApp": compute_days_to_noa_by_app(rows),
        "statusMix": compute_status_mix(rows),
        "prosecutionSignals": compute_prosecution_signals(rows),
    }
