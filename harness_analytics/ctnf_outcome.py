"""CTNF response-speed -> next-action-was-NOA outcome extraction.

Pure-Python event-stream walker, no DB / FastAPI imports. Used by the
Portfolio Explorer "Response Speed -> Allowance" chart.

Per-CTNF event model
--------------------
For each non-final office action (IFW ``CTNF``) on an application:

  t0           = CTNF mail date
  next_oa_or_noa = earliest mail date strictly after t0 of any of:
                   the next CTNF, next CTFR, or next NOA on this app.
  response_t   = first prosecution event (RESPONSE_NONFINAL,
                 RESPONSE_FINAL, RCE) with t0 < transaction_date AND
                 (next_oa_or_noa is None OR transaction_date <= next_oa_or_noa)

Each CTNF emits at most one outcome event:

  - "allowed"   if next_oa_or_noa came from an NOA mail event
  - "rejected"  if next_oa_or_noa came from another CTNF / CTFR
  - "pending"   if next_oa_or_noa is None (no successor examiner action yet)
                AND a response has been filed -- counted separately, excluded
                from the per-bucket allowance rate denominator
  - dropped     if no qualifying response was filed (these are abandonment
                or dead-file situations and would skew the chart)

This keeps the analytic apples-to-apples: every counted CTNF has both a
known response time AND a known examiner verdict on that response.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, Optional


# Set of prosecution_events.event_type values that count as a "response"
# closing the CTNF response window. Mirrors extension_metrics._RESPONSE_TYPES
# intentionally so the two analytics agree on what "the applicant responded"
# means.
_RESPONSE_EVENT_TYPES: frozenset[str] = frozenset(
    {"RESPONSE_NONFINAL", "RESPONSE_FINAL", "RCE"}
)


@dataclass(frozen=True)
class CtnfOutcome:
    """One per CTNF that produced a usable outcome.

    ``days_to_response`` is response_t - t0 (calendar days, can be 0).
    ``outcome`` is one of "allowed" | "rejected" | "pending".
    ``days_response_to_next`` is next_oa_or_noa_t - response_t when the next
    examiner action is known (only set on allowed/rejected).
    """

    application_id: int
    ctnf_date: date
    response_date: date
    days_to_response: int
    outcome: str  # "allowed" | "rejected" | "pending"
    next_action_date: Optional[date]
    days_response_to_next: Optional[int]


def _to_date(v: object) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date):
        # date is a base of datetime; trim datetimes to their date portion.
        return v if not hasattr(v, "hour") else v.date()  # type: ignore[union-attr]
    s = str(v)[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _earliest_after(values: Iterable[Optional[date]], t0: date) -> Optional[date]:
    """Return min(v for v in values if v > t0), or None."""
    best: Optional[date] = None
    for v in values:
        if v is None or v <= t0:
            continue
        if best is None or v < best:
            best = v
    return best


def extract_outcomes_for_application(
    application_id: int,
    ctnf_dates: list[date],
    ctfr_dates: list[date],
    noa_dates: list[date],
    response_dates: list[date],
) -> list[CtnfOutcome]:
    """Walk one application's events, emit one CtnfOutcome per CTNF.

    All inputs are lists of ``date`` (already de-nulled). Order does not
    matter; this routine takes ``min(... > t0)`` for each successor.
    """
    if not ctnf_dates:
        return []
    out: list[CtnfOutcome] = []
    # Sort so we have a stable iteration order; downstream tests rely on it.
    for t0 in sorted(ctnf_dates):
        # Earliest of: next CTNF, next CTFR, next NOA after this CTNF.
        next_ctnf = _earliest_after(ctnf_dates, t0)
        next_ctfr = _earliest_after(ctfr_dates, t0)
        next_noa = _earliest_after(noa_dates, t0)
        candidates: list[tuple[date, str]] = []
        if next_ctnf is not None:
            candidates.append((next_ctnf, "rejected"))
        if next_ctfr is not None:
            candidates.append((next_ctfr, "rejected"))
        if next_noa is not None:
            candidates.append((next_noa, "allowed"))
        candidates.sort(key=lambda x: x[0])
        next_action_date: Optional[date]
        verdict: Optional[str]
        if candidates:
            next_action_date, verdict = candidates[0]
        else:
            next_action_date, verdict = None, None

        # First response strictly after t0; if a verdict landed, it must
        # also be at-or-before the verdict date so we don't credit a
        # response that was filed AFTER the next examiner action.
        response: Optional[date] = None
        for r in sorted(response_dates):
            if r <= t0:
                continue
            if next_action_date is not None and r > next_action_date:
                break
            response = r
            break

        if response is None:
            # No qualifying response in this window -- abandonment or dead
            # file. Drop from the analytic so the bucket rates aren't
            # diluted by non-respondents.
            continue

        days_to_response = (response - t0).days
        if verdict is None:
            outcome = "pending"
            days_response_to_next: Optional[int] = None
        else:
            outcome = verdict
            assert next_action_date is not None  # for type checker
            days_response_to_next = (next_action_date - response).days

        out.append(
            CtnfOutcome(
                application_id=application_id,
                ctnf_date=t0,
                response_date=response,
                days_to_response=days_to_response,
                outcome=outcome,
                next_action_date=next_action_date,
                days_response_to_next=days_response_to_next,
            )
        )
    return out


def extract_outcomes_from_grouped_events(
    grouped: dict[int, dict[str, list[object]]],
) -> list[CtnfOutcome]:
    """Convenience entry point for SQL fetchers.

    ``grouped`` shape::

        {
            application_id: {
                "ctnf": [date|datetime|str, ...],
                "ctfr": [...],
                "noa":  [...],
                "response": [...],
            },
            ...
        }

    Missing inner keys are treated as empty lists. Non-date values that
    can't be parsed are silently dropped.
    """
    out: list[CtnfOutcome] = []
    for app_id, buckets in grouped.items():
        ctnf = [d for d in (_to_date(v) for v in buckets.get("ctnf", [])) if d]
        if not ctnf:
            continue
        ctfr = [d for d in (_to_date(v) for v in buckets.get("ctfr", [])) if d]
        noa = [d for d in (_to_date(v) for v in buckets.get("noa", [])) if d]
        responses = [
            d for d in (_to_date(v) for v in buckets.get("response", [])) if d
        ]
        out.extend(
            extract_outcomes_for_application(
                application_id=app_id,
                ctnf_dates=ctnf,
                ctfr_dates=ctfr,
                noa_dates=noa,
                response_dates=responses,
            )
        )
    return out


# Re-exported for callers that want to filter prosecution_events server-side
# without re-deriving the set.
RESPONSE_EVENT_TYPES = _RESPONSE_EVENT_TYPES
