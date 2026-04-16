"""Compute per-application analytics after ingestion."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

from sqlalchemy.orm import Session

from harness_analytics.classifier import IFW_A_NE_DOC_CODE, ifw_document_suggests_interview
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


def _events_of_type(events: list[ProsecutionEvent], types: set[str]) -> list[ProsecutionEvent]:
    return [e for e in events if e.event_type in types]


def _ifw_mail_date(d: FileWrapperDocument) -> Optional[date]:
    if not d.mail_room_date:
        return None
    mrd = d.mail_room_date
    return mrd.date() if isinstance(mrd, datetime) else mrd


def _ifw_doc_code(d: FileWrapperDocument) -> str:
    return (d.document_code or "").strip().upper()


def _first_noa_date_from_ifw(ifw_docs: list[FileWrapperDocument]) -> Optional[date]:
    dates: list[date] = []
    for d in ifw_docs:
        if _ifw_doc_code(d) != "NOA":
            continue
        dd = _ifw_mail_date(d)
        if dd is not None:
            dates.append(dd)
    return min(dates) if dates else None


def _ifw_docs_code_before_noa(
    ifw_docs: list[FileWrapperDocument],
    doc_code: str,
    first_noa_date: Optional[date],
) -> list[FileWrapperDocument]:
    want = doc_code.strip().upper()
    out: list[FileWrapperDocument] = []
    for d in ifw_docs:
        if _ifw_doc_code(d) != want:
            continue
        dd = _ifw_mail_date(d)
        if dd is None:
            continue
        if first_noa_date is not None and dd >= first_noa_date:
            continue
        out.append(d)
    out.sort(key=lambda x: (_ifw_mail_date(x) or date.min, x.id))
    return out


def _interview_signal_dates_from_ifw(ifw_docs: list[FileWrapperDocument]) -> set[date]:
    dates: set[date] = set()
    for d in ifw_docs:
        if not ifw_document_suggests_interview(d.document_code, d.document_description):
            continue
        dd = _ifw_mail_date(d)
        if dd is not None:
            dates.add(dd)
    return dates


def _days_between(d1: Optional[date], d2: Optional[date]) -> Optional[int]:
    if d1 and d2:
        return (d2 - d1).days
    return None


def compute_analytics_for_application(
    db: Session,
    app: Application,
    *,
    interview_window_days: int,
    office_cfg: dict[str, Any],
) -> None:
    """Compute and upsert application_analytics for one application."""
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

    first_noa_date = _first_noa_date_from_ifw(ifw_docs)
    nonfinal_ifw = _ifw_docs_code_before_noa(ifw_docs, "CTNF", first_noa_date)
    final_ifw = _ifw_docs_code_before_noa(ifw_docs, "CTFR", first_noa_date)
    rce_events = _events_of_type(events, {"RCE"})

    interview_dates = _interview_signal_dates_from_ifw(ifw_docs)
    last_interview_before_noa: Optional[date] = None
    if first_noa_date and interview_dates:
        prior = [d for d in interview_dates if d < first_noa_date]
        if prior:
            last_interview_before_noa = max(prior)

    first_rce_date = rce_events[0].transaction_date if rce_events else None

    oa_mail_dates: list[date] = []
    for d in nonfinal_ifw + final_ifw:
        dd = _ifw_mail_date(d)
        if dd is not None:
            oa_mail_dates.append(dd)
    first_oa_date = min(oa_mail_dates) if oa_mail_dates else None

    interview_before_noa = last_interview_before_noa is not None
    days_int_to_noa: Optional[int] = None
    interview_led_to_noa = False
    if last_interview_before_noa is not None and first_noa_date is not None:
        days_int_to_noa = (first_noa_date - last_interview_before_noa).days
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

    existing = db.query(ApplicationAnalytics).filter_by(application_id=app.id).first()
    if not existing:
        existing = ApplicationAnalytics(application_id=app.id)
        db.add(existing)

    existing.nonfinal_oa_count = len(nonfinal_ifw)
    existing.final_oa_count = len(final_ifw)
    existing.total_substantive_oas = len(nonfinal_ifw) + len(final_ifw)
    existing.first_oa_date = first_oa_date
    existing.first_nonfinal_oa_date = _ifw_mail_date(nonfinal_ifw[0]) if nonfinal_ifw else None
    existing.first_final_oa_date = _ifw_mail_date(final_ifw[0]) if final_ifw else None
    existing.first_noa_date = first_noa_date
    existing.had_examiner_interview = bool(interview_dates)
    existing.interview_count = len(interview_dates)
    existing.interview_before_noa = interview_before_noa
    existing.interview_led_to_noa = interview_led_to_noa
    existing.days_interview_to_noa = days_int_to_noa
    existing.rce_count = len(rce_events)
    existing.first_rce_date = first_rce_date
    existing.days_filing_to_first_oa = _days_between(app.filing_date, first_oa_date)
    existing.days_filing_to_noa = _days_between(app.filing_date, first_noa_date)
    existing.days_filing_to_issue = _days_between(app.filing_date, app.issue_date)
    existing.billing_attorney_reg = billing_reg
    existing.billing_attorney_name = billing_name or None
    existing.is_jac = is_jac
    existing.office_name = office_name
    existing.ifw_a_ne_count = _count_ifw_doc_code(ifw_docs, IFW_A_NE_DOC_CODE)


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
        compute_analytics_for_application(
            db,
            app,
            interview_window_days=interview_window_days,
            office_cfg=office_cfg,
        )
