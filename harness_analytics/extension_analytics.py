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

CTRS-specific tweak: an applicant Election (IFW codes ``ELC.`` /
``ELECTION``) or Remarks/Argument document (IFW ``REM``) mailed strictly
after a CTRS also closes the CTRS response window, in addition to the
next CTRS / first NOA closers. ``ELC.`` / ``ELECTION`` is the textbook
applicant response to a Restriction Requirement; ``REM`` catches the
common pattern where a Restriction Requirement is followed by an
Election filed alongside a later CTNF/CTFR response. Without these
closers, the late merits-response gets credited back to the CTRS and
inflates the 4+month duration bucket. Election and REM dates also serve
as candidate responses for the CTRS so an Election filed directly
against the restriction (which is not classified as
RESPONSE_NONFINAL/FINAL/RCE in prosecution_events) still produces a
measurable lateness against the 2-month deadline.

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


def _months_past_deadline(t0: date, resp: date, deadline_months: int) -> int:
    """How many additional 1-month USPTO extension blocks the response used.

    Maps to the formal extension model: if a response is filed any time
    after the statutory deadline, it bought at least one 1-month
    extension (even if only a day late). One month past the deadline =
    1-month extension, two months past = 2-month, etc. We round any
    fractional days *up* to the next month boundary because USPTO sells
    extensions in whole-month units.
    """
    deadline = t0 + relativedelta(months=deadline_months)
    if resp <= deadline:
        return 0
    delta = relativedelta(resp, deadline)
    months = delta.years * 12 + delta.months
    if delta.days > 0 or delta.hours > 0 or delta.minutes > 0:
        months += 1
    return max(1, months)


def _extension_for_oa(
    oa_dates: list[date],
    boundary_dates: list[date],
    first_noa: Optional[date],
    responses: list[date],
    deadline_months: int,
) -> list[tuple[date, int]]:
    """Return ``(response_date, months_past_deadline)`` pairs for late OAs.

    ``boundary_dates`` is the set of "next examiner action" dates that
    close the response window (typically the same set as ``oa_dates`` for
    CTNF/CTFR, or just the CTRS list for restriction requirements). We use
    ``min(next boundary > t0, first NOA)`` as the horizon — matches
    ``extension_metrics`` so the two analytics agree on which response
    counts.
    """
    out: list[tuple[date, int]] = []
    for t0 in oa_dates:
        next_boundary = _earliest_after(boundary_dates, t0)
        horizon = _horizon(next_boundary, first_noa)
        resp = _first_response_in_window(responses, t0, horizon)
        if resp is None:
            continue
        deadline = t0 + relativedelta(months=deadline_months)
        if resp > deadline:
            out.append((resp, _months_past_deadline(t0, resp, deadline_months)))
    return out


def _empty_year_row(year: int) -> dict[str, Any]:
    return {
        "year": year,
        "ctnf": 0,
        "ctfr": 0,
        "restriction": 0,
        "total": 0,
        # Duration buckets — number of additional 1-month extension blocks
        # the response consumed. ``fourPlus`` aggregates anything ≥ 4
        # months past the statutory deadline (rare in practice; USPTO
        # caps formal extensions at 5 months, but our heuristic catches
        # any abandonment-revival or other late-filed response).
        "oneMonth": 0,
        "twoMonth": 0,
        "threeMonth": 0,
        "fourPlus": 0,
        # Stamped after the per-year roll-up; default False so zero-filled
        # gap rows aren't accidentally flagged as YTD.
        "isPartial": False,
    }


def _empty_quarter_row(year: int, quarter: int) -> dict[str, Any]:
    return {
        "year": year,
        "quarter": quarter,
        "ctnf": 0,
        "ctfr": 0,
        "restriction": 0,
        "total": 0,
        "oneMonth": 0,
        "twoMonth": 0,
        "threeMonth": 0,
        "fourPlus": 0,
        "isPartial": False,
    }


def _empty_month_row(year: int, month: int) -> dict[str, Any]:
    return {
        "year": year,
        "month": month,
        "ctnf": 0,
        "ctfr": 0,
        "restriction": 0,
        "total": 0,
        "oneMonth": 0,
        "twoMonth": 0,
        "threeMonth": 0,
        "fourPlus": 0,
        "isPartial": False,
    }


def _quarter_of(d: date) -> int:
    return (d.month - 1) // 3 + 1


def _bucket_key(months: int) -> str:
    if months <= 1:
        return "oneMonth"
    if months == 2:
        return "twoMonth"
    if months == 3:
        return "threeMonth"
    return "fourPlus"


def compute_extensions_by_year(
    grouped: dict[int, dict[str, list[Any]]],
    *,
    today: Optional[date] = None,
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
            "byYear":    [{"year": 2023, "ctnf": 5, "ctfr": 2,
                           "restriction": 1, "total": 8,
                           "isPartial": False}, ...],
            "byQuarter": [{"year": 2023, "quarter": 1, ...}, ...],
            "byMonth":   [{"year": 2023, "month":   1, ...}, ...],
            "totals":  {"ctnf": ..., "ctfr": ..., "restriction": ..., "total": ...},
            "appsContributing": int,   # apps with >=1 extension
        }

    ``byYear`` / ``byQuarter`` / ``byMonth`` are dense: every period in
    the year span containing extensions is present (zero-filled gaps make
    the chart axis stable). The current calendar year / quarter / month
    (per ``today``, defaulting to ``date.today()``) is flagged
    ``isPartial=True`` so the renderer can overlay a YTD projection at
    whatever grain the user has drilled into.
    """
    by_year: dict[int, dict[str, Any]] = {}
    by_quarter: dict[tuple[int, int], dict[str, Any]] = {}
    by_month: dict[tuple[int, int], dict[str, Any]] = {}
    apps_contributing: set[int] = set()

    def _bump(rows: dict, key, factory, oa_kind: str, months: int) -> None:
        row = rows.setdefault(key, factory())
        row[oa_kind] += 1
        row["total"] += 1
        row[_bucket_key(months)] += 1

    for app_id, buckets in grouped.items():
        ctnf = _clean_dates(buckets.get("ctnf", []))
        ctfr = _clean_dates(buckets.get("ctfr", []))
        ctrs = _clean_dates(buckets.get("ctrs", []))
        noa = _clean_dates(buckets.get("noa", []))
        responses = _clean_dates(buckets.get("response", []))
        rem = _clean_dates(buckets.get("rem", []))
        elc = _clean_dates(buckets.get("elc", []))
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
        # CTRS horizon adds Election (ELC. / ELECTION) and REM (Applicant
        # Remarks/Argument) mail dates as additional closers. Election is
        # the textbook applicant response to a Restriction Requirement —
        # prosecution_events doesn't classify Elections as
        # RESPONSE_NONFINAL/FINAL/RCE, so without this the calculator would
        # never see the actual response. REM catches the related pattern
        # where the Election is filed alongside a later CTNF/CTFR REM
        # response. Either one filed strictly after a CTRS caps the CTRS
        # response window so a far-later merits response can't be credited
        # back to the restriction. Both also land in the CTRS response
        # candidate list so an Election or REM filed in direct response to
        # the CTRS still produces a measurable lateness against the 2-month
        # deadline (instead of silently dropping when no
        # RESPONSE_NONFINAL classification exists).
        ctrs_boundaries = sorted(ctrs + rem + elc)
        ctrs_responses = sorted(set(responses + rem + elc))
        late_ctrs = _extension_for_oa(
            ctrs, ctrs_boundaries, first_noa, ctrs_responses, _CTRS_DEADLINE_MONTHS
        )

        if late_ctnf or late_ctfr or late_ctrs:
            apps_contributing.add(app_id)

        for kind, late in (
            ("ctnf", late_ctnf),
            ("ctfr", late_ctfr),
            ("restriction", late_ctrs),
        ):
            for resp, months in late:
                q = _quarter_of(resp)
                _bump(by_year, resp.year, lambda y=resp.year: _empty_year_row(y), kind, months)
                _bump(
                    by_quarter,
                    (resp.year, q),
                    lambda y=resp.year, qq=q: _empty_quarter_row(y, qq),
                    kind,
                    months,
                )
                _bump(
                    by_month,
                    (resp.year, resp.month),
                    lambda y=resp.year, m=resp.month: _empty_month_row(y, m),
                    kind,
                    months,
                )

    if by_year:
        y_min = min(by_year)
        y_max = max(by_year)
        for y in range(y_min, y_max + 1):
            by_year.setdefault(y, _empty_year_row(y))
            for q in range(1, 5):
                by_quarter.setdefault((y, q), _empty_quarter_row(y, q))
            for m in range(1, 13):
                by_month.setdefault((y, m), _empty_month_row(y, m))

    today_d = today or date.today()
    cur_year = today_d.year
    cur_quarter = _quarter_of(today_d)
    cur_month = today_d.month
    for y, row in by_year.items():
        row["isPartial"] = (y == cur_year)
    for (y, q), row in by_quarter.items():
        row["isPartial"] = (y == cur_year and q == cur_quarter)
    for (y, m), row in by_month.items():
        row["isPartial"] = (y == cur_year and m == cur_month)

    rows = [by_year[y] for y in sorted(by_year)]
    quarter_rows = [by_quarter[k] for k in sorted(by_quarter)]
    month_rows = [by_month[k] for k in sorted(by_month)]
    totals = {
        "ctnf": sum(r["ctnf"] for r in rows),
        "ctfr": sum(r["ctfr"] for r in rows),
        "restriction": sum(r["restriction"] for r in rows),
        "total": sum(r["total"] for r in rows),
        "oneMonth": sum(r["oneMonth"] for r in rows),
        "twoMonth": sum(r["twoMonth"] for r in rows),
        "threeMonth": sum(r["threeMonth"] for r in rows),
        "fourPlus": sum(r["fourPlus"] for r in rows),
    }
    return {
        "byYear": rows,
        "byQuarter": quarter_rows,
        "byMonth": month_rows,
        "totals": totals,
        "appsContributing": len(apps_contributing),
    }
