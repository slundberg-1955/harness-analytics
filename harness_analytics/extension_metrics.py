"""Heuristic extension-of-time counts from IFW OA/CTRS mail dates vs prosecution responses.

Rules (see README):
- CTNF / CTFR: assumed 3 calendar months from IFW mail date; first classified
  RESPONSE_NONFINAL / RESPONSE_FINAL / RCE after that OA and before the earlier of the
  next OA mail or first NOA mail counts as the response date. Counts are split by
  non-final (CTNF) vs final (CTFR).
- CTRS: assumed 2 calendar months; first qualifying response after that CTRS and before
  the earlier of the next CTRS mail or first NOA mail.
- If response is after the deadline, lateness = response_date - deadline (whole days);
  buckets: 1–30, 31–60, 61–90 days only; lateness beyond 90 days is not counted.

This is a rough proxy, not a determination of formal USPTO extensions.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

from dateutil.relativedelta import relativedelta

from harness_analytics.classifier import IFW_CTRS_DOC_CODE

_RESPONSE_TYPES = frozenset({"RESPONSE_NONFINAL", "RESPONSE_FINAL", "RCE"})


def _ifw_mail_date(d: Any) -> Optional[date]:
    if not d.mail_room_date:
        return None
    mrd = d.mail_room_date
    return mrd.date() if isinstance(mrd, datetime) else mrd


def _ifw_doc_code(d: Any) -> str:
    return (d.document_code or "").strip().upper()


def _deadline_plus_months(mail: date, months: int) -> date:
    return mail + relativedelta(months=months)


def _horizon(next_boundary: Optional[date], first_noa: Optional[date]) -> Optional[date]:
    parts = [d for d in (next_boundary, first_noa) if d is not None]
    if not parts:
        return None
    return min(parts)


def _first_response_date(events: list[Any], t0: date, horizon: Optional[date]) -> Optional[date]:
    for e in events:
        et = e.transaction_date
        if et <= t0:
            continue
        if getattr(e, "event_type", None) not in _RESPONSE_TYPES:
            continue
        if horizon is not None and et >= horizon:
            continue
        return et
    return None


def _inc_late_bucket(counters: dict[str, int], key: str) -> None:
    counters[key] = counters.get(key, 0) + 1


def _bucket_late_days(late_days: int, prefix: str, counters: dict[str, int]) -> None:
    if late_days <= 0 or late_days > 90:
        return
    if late_days <= 30:
        _inc_late_bucket(counters, f"{prefix}_1mo")
    elif late_days <= 60:
        _inc_late_bucket(counters, f"{prefix}_2mo")
    else:
        _inc_late_bucket(counters, f"{prefix}_3mo")


def _ctrs_docs_before_noa(ifw_docs: list[Any], first_noa_date: Optional[date]) -> list[tuple[date, int]]:
    rows: list[tuple[date, int]] = []
    for d in sorted(ifw_docs, key=lambda x: (_ifw_mail_date(x) or date.min, x.id)):
        if _ifw_doc_code(d) != IFW_CTRS_DOC_CODE.upper():
            continue
        dd = _ifw_mail_date(d)
        if dd is None:
            continue
        if first_noa_date is not None and dd >= first_noa_date:
            continue
        rows.append((dd, d.id))
    return rows


def compute_extension_time_counts(
    nonfinal_ifw: list[Any],
    final_ifw: list[Any],
    ifw_docs: list[Any],
    events: list[Any],
    first_noa_date: Optional[date],
) -> dict[str, int]:
    """Return keys ctnf_1mo..ctfr_3mo, ctrs_1mo..ctrs_3mo (int counts)."""
    events = sorted(events, key=lambda e: (e.transaction_date, e.seq_order or 0))

    counters: dict[str, int] = {
        "ctnf_1mo": 0,
        "ctnf_2mo": 0,
        "ctnf_3mo": 0,
        "ctfr_1mo": 0,
        "ctfr_2mo": 0,
        "ctfr_3mo": 0,
        "ctrs_1mo": 0,
        "ctrs_2mo": 0,
        "ctrs_3mo": 0,
    }

    oa_rows: list[tuple[date, int, str]] = []
    for d in nonfinal_ifw:
        md = _ifw_mail_date(d)
        if md is not None:
            oa_rows.append((md, d.id, "CTNF"))
    for d in final_ifw:
        md = _ifw_mail_date(d)
        if md is not None:
            oa_rows.append((md, d.id, "CTFR"))
    oa_rows.sort(key=lambda x: (x[0], x[1]))

    for i, (t0, _oid, code) in enumerate(oa_rows):
        next_boundary = oa_rows[i + 1][0] if i + 1 < len(oa_rows) else None
        horizon = _horizon(next_boundary, first_noa_date)
        resp = _first_response_date(events, t0, horizon)
        if resp is None:
            continue
        deadline = _deadline_plus_months(t0, 3)
        late_days = (resp - deadline).days
        prefix = "ctnf" if code == "CTNF" else "ctfr"
        _bucket_late_days(late_days, prefix, counters)

    ctrs_rows = _ctrs_docs_before_noa(ifw_docs, first_noa_date)
    for i, (t0, _cid) in enumerate(ctrs_rows):
        next_ctrs = ctrs_rows[i + 1][0] if i + 1 < len(ctrs_rows) else None
        horizon = _horizon(next_ctrs, first_noa_date)
        resp = _first_response_date(events, t0, horizon)
        if resp is None:
            continue
        deadline = _deadline_plus_months(t0, 2)
        late_days = (resp - deadline).days
        _bucket_late_days(late_days, "ctrs", counters)

    return counters
