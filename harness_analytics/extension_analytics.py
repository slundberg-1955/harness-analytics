"""Per-year extension-of-time counts for CTNF / CTFR / CTRS office actions.

An "extension" here is a heuristic proxy: any qualifying applicant
response (``RESPONSE_NONFINAL`` / ``RESPONSE_FINAL`` / ``RCE`` / Notice
of Appeal) filed strictly more than:

* 3 calendar months after a Non-Final (CTNF) or Final (CTFR) Office Action
* 2 calendar months after a Restriction Requirement (CTRS)

Each extension is bucketed by the calendar year of the response (the
"year taken"). Per-OA we use the first qualifying response between the OA
mail date and the earlier of (next OA on this app, first NOA on this
app) — same horizon as ``extension_metrics`` / ``ctnf_outcome`` so the
three analytics agree on what counts as "the response".

This is a rough proxy, not a determination of formal USPTO extensions.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Optional

from dateutil.relativedelta import relativedelta


# Mirrors extension_metrics._RESPONSE_TYPES + Notice of Appeal so the user's
# "any response/RCE/Appeal" definition is honored. Notice-of-Appeal entries
# come in via the ``response`` bucket too (caller pulls them from the
# ``N.APP`` IFW document code so we don't depend on a missing
# prosecution_events classification).
_CTNF_CTFR_DEADLINE_MONTHS = 3
_CTRS_DEADLINE_MONTHS = 2


def _to_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v)[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _clean_dates(values: Iterable[Any]) -> list[date]:
    return sorted(d for d in (_to_date(v) for v in values) if d is not None)


def _earliest_after(values: Iterable[date], t0: date) -> Optional[date]:
    best: Optional[date] = None
    for v in values:
        if v <= t0:
            continue
        if best is None or v < best:
            best = v
    return best


def _horizon(next_boundary: Optional[date], first_noa: Optional[date]) -> Optional[date]:
    parts = [d for d in (next_boundary, first_noa) if d is not None]
    return min(parts) if parts else None


def _first_response_in_window(
    responses: list[date], t0: date, horizon: Optional[date]
) -> Optional[date]:
    """First response strictly after ``t0`` and at-or-before ``horizon``."""
    for r in responses:
        if r <= t0:
            continue
        if horizon is not None and r > horizon:
            return None
        return r
    return None


def _extension_for_oa(
    oa_dates: list[date],
    boundary_dates: list[date],
    first_noa: Optional[date],
    responses: list[date],
    deadline_months: int,
) -> list[date]:
    """Return the response dates that qualify as extensions for these OAs.

    ``boundary_dates`` is the set of "next examiner action" dates that
    close the response window (typically the same set as ``oa_dates`` for
    CTNF/CTFR, or just the CTRS list for restriction requirements). We use
    ``min(next boundary > t0, first NOA)`` as the horizon — matches
    ``extension_metrics`` so the two analytics agree on which response
    counts.
    """
    out: list[date] = []
    for t0 in oa_dates:
        next_boundary = _earliest_after(boundary_dates, t0)
        horizon = _horizon(next_boundary, first_noa)
        resp = _first_response_in_window(responses, t0, horizon)
        if resp is None:
            continue
        deadline = t0 + relativedelta(months=deadline_months)
        if resp > deadline:
            out.append(resp)
    return out


def _empty_year_row(year: int) -> dict[str, int]:
    return {"year": year, "ctnf": 0, "ctfr": 0, "restriction": 0, "total": 0}


def compute_extensions_by_year(
    grouped: dict[int, dict[str, list[Any]]],
) -> dict[str, Any]:
    """Aggregate per-year extension counts across many applications.

    ``grouped`` shape mirrors ``ctnf_outcome.extract_outcomes_from_grouped_events``::

        {
            application_id: {
                "ctnf":     [date|datetime|str, ...],   # CTNF mail dates
                "ctfr":     [...],                       # CTFR mail dates
                "ctrs":     [...],                       # CTRS mail dates
                "noa":      [...],                       # NOA mail dates
                "response": [...],                       # Applicant responses
            },
            ...
        }

    Returns::

        {
            "byYear":  [{"year": 2023, "ctnf": 5, "ctfr": 2,
                         "restriction": 1, "total": 8}, ...],
            "totals":  {"ctnf": ..., "ctfr": ..., "restriction": ..., "total": ...},
            "appsContributing": int,   # apps with >=1 extension
        }

    ``byYear`` is dense: every year between the min and max year with any
    extension is present (zero-filled gaps make the chart axis stable).
    """
    by_year: dict[int, dict[str, int]] = {}
    apps_contributing: set[int] = set()

    for app_id, buckets in grouped.items():
        ctnf = _clean_dates(buckets.get("ctnf", []))
        ctfr = _clean_dates(buckets.get("ctfr", []))
        ctrs = _clean_dates(buckets.get("ctrs", []))
        noa = _clean_dates(buckets.get("noa", []))
        responses = _clean_dates(buckets.get("response", []))
        first_noa = noa[0] if noa else None

        # CTNF/CTFR share a single "next examiner action" horizon — a CTFR
        # mailed after a CTNF closes the CTNF's response window even though
        # they're different doc codes. CTRS uses its own horizon (separate
        # restriction loops are independent of the merits-rejection loop).
        oa_boundaries = sorted(ctnf + ctfr)

        late_ctnf = _extension_for_oa(
            ctnf, oa_boundaries, first_noa, responses, _CTNF_CTFR_DEADLINE_MONTHS
        )
        late_ctfr = _extension_for_oa(
            ctfr, oa_boundaries, first_noa, responses, _CTNF_CTFR_DEADLINE_MONTHS
        )
        late_ctrs = _extension_for_oa(
            ctrs, ctrs, first_noa, responses, _CTRS_DEADLINE_MONTHS
        )

        if late_ctnf or late_ctfr or late_ctrs:
            apps_contributing.add(app_id)

        for resp in late_ctnf:
            row = by_year.setdefault(resp.year, _empty_year_row(resp.year))
            row["ctnf"] += 1
            row["total"] += 1
        for resp in late_ctfr:
            row = by_year.setdefault(resp.year, _empty_year_row(resp.year))
            row["ctfr"] += 1
            row["total"] += 1
        for resp in late_ctrs:
            row = by_year.setdefault(resp.year, _empty_year_row(resp.year))
            row["restriction"] += 1
            row["total"] += 1

    if by_year:
        y_min = min(by_year)
        y_max = max(by_year)
        for y in range(y_min, y_max + 1):
            by_year.setdefault(y, _empty_year_row(y))

    rows = [by_year[y] for y in sorted(by_year)]
    totals = {
        "ctnf": sum(r["ctnf"] for r in rows),
        "ctfr": sum(r["ctfr"] for r in rows),
        "restriction": sum(r["restriction"] for r in rows),
        "total": sum(r["total"] for r in rows),
    }
    return {
        "byYear": rows,
        "totals": totals,
        "appsContributing": len(apps_contributing),
    }
