"""Pure-Python KPI and chart aggregates for the Portfolio Explorer.

All functions operate on the already-filtered row list (list of dicts with the
`patent_applications` view column names) so they can be unit-tested without a
database. Keep this module free of FastAPI / SQLAlchemy imports.
"""

from __future__ import annotations

from statistics import mean, median
from typing import Any, Iterable


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


def compute_kpis(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    patented = sum(1 for r in rows if r.get("application_status_code") == 150)
    abandoned = sum(1 for r in rows if r.get("application_status_code") == 161)
    # Pending = anything that is neither patented nor abandoned. Matches the
    # mockup's "13 patented · 2 pending" subtitle for a 15-row portfolio.
    pending = total - patented - abandoned

    allowance_denom = patented + abandoned
    allowance_pct = round(100.0 * patented / allowance_denom, 1) if allowance_denom else 0.0

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
        "avgDaysToNoa": int(avg_days) if avg_days is not None else None,
        "medianDaysToNoa": int(med_days) if med_days is not None else None,
        "avgOaCount": avg_oa,
        "appsWithAtLeastOneOa": apps_with_any_oa,
        "interviewRatePct": interview_rate,
        "interviewCount": interview_count,
        "rceRatePct": rce_rate,
        "rceCount": rce_count,
    }


def compute_days_to_noa_by_app(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sorted ascending by days (nulls last so muted bars cluster on the right)."""
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


def compute_charts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "daysToNoaByApp": compute_days_to_noa_by_app(rows),
        "statusMix": compute_status_mix(rows),
        "prosecutionSignals": compute_prosecution_signals(rows),
    }
