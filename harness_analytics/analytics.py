"""Compute per-application analytics after ingestion."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from sqlalchemy.orm import Session

from harness_analytics.classifier import ifw_document_suggests_interview
from harness_analytics.models import (
    Application,
    ApplicationAnalytics,
    ApplicationAttorney,
    FileWrapperDocument,
    ProsecutionEvent,
)

JAC_REG = "35094"

_DEFAULT_OFFICE_CONFIG: dict[str, Any] = {
    "uspto_customer_number_to_office": {},
    "area_code_to_office": {"703": "DC", "571": "DC"},
}


def _default_office_map_path() -> Path:
    return Path(__file__).resolve().parent.parent / "config" / "office_map.json"


def load_office_config(path: Optional[Path] = None) -> dict[str, Any]:
    cfg = json.loads(json.dumps(_DEFAULT_OFFICE_CONFIG))
    p = path or _default_office_map_path()
    if p.exists():
        with p.open(encoding="utf-8") as fh:
            user = json.load(fh)
        if isinstance(user, dict):
            cust = user.get("uspto_customer_number_to_office") or {}
            area = user.get("area_code_to_office") or {}
            if isinstance(cust, dict):
                cfg["uspto_customer_number_to_office"].update(
                    {str(k): str(v) for k, v in cust.items() if not str(k).startswith("_")}
                )
            if isinstance(area, dict):
                cfg["area_code_to_office"].update({str(k): str(v) for k, v in area.items()})
    return cfg


def _area_code_from_phone(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) >= 10:
        return digits[:3]
    if len(digits) >= 3:
        return digits[:3]
    return None


def _resolve_office_name(
    customer_number: Optional[str],
    billing_phone: Optional[str],
    office_cfg: dict[str, Any],
) -> str:
    cmap: dict[str, str] = office_cfg.get("uspto_customer_number_to_office", {})
    amap: dict[str, str] = office_cfg.get("area_code_to_office", {})
    if customer_number:
        key = str(customer_number).strip()
        if key in cmap:
            return cmap[key]
    ac = _area_code_from_phone(billing_phone)
    if ac and ac in amap:
        return amap[ac]
    return "UNKNOWN"


def _first_date_of_type(events: list[ProsecutionEvent], types: set[str]) -> Optional[date]:
    for e in events:
        if e.event_type in types:
            return e.transaction_date
    return None


def _events_of_type(events: list[ProsecutionEvent], types: set[str]) -> list[ProsecutionEvent]:
    return [e for e in events if e.event_type in types]


def _events_before_noa(
    events: list[ProsecutionEvent], types: set[str], first_noa_date: Optional[date]
) -> list[ProsecutionEvent]:
    out: list[ProsecutionEvent] = []
    for e in events:
        if e.event_type not in types:
            continue
        if first_noa_date is None:
            out.append(e)
        elif e.transaction_date and e.transaction_date < first_noa_date:
            out.append(e)
    return out


def _interview_signal_dates(
    events: list[ProsecutionEvent], ifw_docs: list[FileWrapperDocument]
) -> set[date]:
    dates: set[date] = set()
    for e in events:
        if e.event_type == "INTERVIEW" and e.transaction_date:
            dates.add(e.transaction_date)
    for d in ifw_docs:
        if not ifw_document_suggests_interview(d.document_code, d.document_description):
            continue
        if not d.mail_room_date:
            continue
        mrd = d.mail_room_date
        dd = mrd.date() if isinstance(mrd, datetime) else mrd
        dates.add(dd)
    return dates


def compute_analytics(
    db: Session,
    *,
    interview_window_days: int = 90,
    office_map_path: Optional[Path] = None,
) -> None:
    """Compute and upsert application_analytics for every application."""
    office_cfg = load_office_config(office_map_path)

    apps = db.query(Application).order_by(Application.id).all()

    for app in apps:
        events = (
            db.query(ProsecutionEvent)
            .filter(ProsecutionEvent.application_id == app.id)
            .order_by(ProsecutionEvent.transaction_date, ProsecutionEvent.seq_order)
            .all()
        )
        ifw_docs = (
            db.query(FileWrapperDocument)
            .filter(FileWrapperDocument.application_id == app.id)
            .order_by(FileWrapperDocument.mail_room_date, FileWrapperDocument.id)
            .all()
        )

        first_noa_date = _first_date_of_type(events, {"NOA"})
        nonfinal_oa_events = _events_before_noa(events, {"NONFINAL_OA"}, first_noa_date)
        final_oa_events = _events_before_noa(events, {"FINAL_OA"}, first_noa_date)
        interview_events = _events_of_type(events, {"INTERVIEW"})
        rce_events = _events_of_type(events, {"RCE"})

        interview_dates = _interview_signal_dates(events, ifw_docs)
        first_interview_date = min(interview_dates) if interview_dates else None
        first_rce_date = rce_events[0].transaction_date if rce_events else None
        first_oa_date = _first_date_of_type(events, {"NONFINAL_OA", "FINAL_OA"})

        interview_before_noa = bool(
            first_interview_date and first_noa_date and first_interview_date < first_noa_date
        )
        days_int_to_noa: Optional[int] = None
        interview_led_to_noa = False
        if first_interview_date and first_noa_date and interview_before_noa:
            days_int_to_noa = (first_noa_date - first_interview_date).days
            interview_led_to_noa = days_int_to_noa <= interview_window_days

        billing_atty = (
            db.query(ApplicationAttorney)
            .filter(
                ApplicationAttorney.application_id == app.id,
                ApplicationAttorney.attorney_role == "POA",
                ApplicationAttorney.is_first_attorney.is_(True),
            )
            .first()
        )
        billing_reg = billing_atty.registration_number if billing_atty else None
        billing_name = (
            f"{billing_atty.first_name or ''} {billing_atty.last_name or ''}".strip()
            if billing_atty
            else None
        )
        billing_phone = billing_atty.phone if billing_atty else None
        is_jac = billing_reg == JAC_REG

        office_name = _resolve_office_name(app.customer_number, billing_phone, office_cfg)

        def days_between(d1: Optional[date], d2: Optional[date]) -> Optional[int]:
            if d1 and d2:
                return (d2 - d1).days
            return None

        existing = db.query(ApplicationAnalytics).filter_by(application_id=app.id).first()
        if not existing:
            existing = ApplicationAnalytics(application_id=app.id)
            db.add(existing)

        existing.nonfinal_oa_count = len(nonfinal_oa_events)
        existing.final_oa_count = len(final_oa_events)
        existing.total_substantive_oas = len(nonfinal_oa_events) + len(final_oa_events)
        existing.first_oa_date = first_oa_date
        existing.first_nonfinal_oa_date = (
            nonfinal_oa_events[0].transaction_date if nonfinal_oa_events else None
        )
        existing.first_final_oa_date = final_oa_events[0].transaction_date if final_oa_events else None
        existing.first_noa_date = first_noa_date
        existing.had_examiner_interview = bool(interview_dates)
        existing.interview_count = len(interview_dates)
        existing.interview_before_noa = interview_before_noa
        existing.interview_led_to_noa = interview_led_to_noa
        existing.days_interview_to_noa = days_int_to_noa
        existing.rce_count = len(rce_events)
        existing.first_rce_date = first_rce_date
        existing.days_filing_to_first_oa = days_between(app.filing_date, first_oa_date)
        existing.days_filing_to_noa = days_between(app.filing_date, first_noa_date)
        existing.days_filing_to_issue = days_between(app.filing_date, app.issue_date)
        existing.billing_attorney_reg = billing_reg
        existing.billing_attorney_name = billing_name or None
        existing.is_jac = is_jac
        existing.office_name = office_name
