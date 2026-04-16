"""Authenticated web portal: Excel download + per-matter HTML/XML.

All /portal routes except /portal/login and /portal/logout require either a valid
signed session cookie (after HTML form sign-in) or HTTP Basic credentials.
"""

from __future__ import annotations

import base64
import binascii
import os
import re
import secrets
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.security import HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware

from harness_analytics._debug_agent_log import agent_log
from harness_analytics.db import get_db
from harness_analytics.models import Application, ApplicationAnalytics, FileWrapperDocument, ProsecutionEvent

router = APIRouter(prefix="/portal", tags=["portal"])

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

PORTAL_USER_DEFAULT = "viewer"
EVENTS_LIMIT = 500
DOCS_LIMIT = 500
SESSION_KEY = "portal_authenticated"


def _portal_password() -> str | None:
    p = os.environ.get("PORTAL_PASSWORD")
    return p if p else None


def _expected_username() -> str:
    return os.environ.get("PORTAL_USER", PORTAL_USER_DEFAULT)


def _session_signing_secret() -> str:
    """Secret for signed session cookies; must be stable for the process lifetime."""
    return (
        os.environ.get("SECRET_KEY")
        or os.environ.get("PORTAL_PASSWORD")
        or "dev-only-insecure-session-key-32chars!!"
    )


def _basic_credentials_from_request(request: Request) -> HTTPBasicCredentials | None:
    header = request.headers.get("Authorization")
    if not header or not header.startswith("Basic "):
        return None
    try:
        raw = base64.b64decode(header[6:].strip()).decode("utf-8")
    except (ValueError, binascii.Error, UnicodeDecodeError):
        return None
    if ":" not in raw:
        return None
    username, _, password = raw.partition(":")
    return HTTPBasicCredentials(username=username, password=password)


def _basic_credentials_valid(credentials: HTTPBasicCredentials) -> bool:
    expected = _portal_password()
    if not expected:
        return False
    if credentials.username != _expected_username():
        return False
    return secrets.compare_digest(credentials.password, expected)


def _portal_authenticated(request: Request) -> bool:
    if request.session.get(SESSION_KEY) is True:
        return True
    creds = _basic_credentials_from_request(request)
    return bool(creds and _basic_credentials_valid(creds))


class PortalAuthMiddleware(BaseHTTPMiddleware):
    """Require password (session or Basic) for every /portal path except login/logout."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        path = request.url.path
        # #region agent log
        if path == "/health" or path.startswith("/health"):
            agent_log(
                "portal.py:PortalAuthMiddleware.dispatch",
                "health_through_mw",
                data={"path": path, "method": request.method},
                hypothesis_id="H2_MW",
            )
        # #endregion

        if not path.startswith("/portal"):
            return await call_next(request)

        if path in ("/portal/login", "/portal/login/"):
            return await call_next(request)

        if path in ("/portal/logout", "/portal/logout/"):
            return await call_next(request)

        expected_pw = _portal_password()
        if not expected_pw:
            if path.startswith("/portal"):
                return JSONResponse(
                    {"detail": "Portal is not configured. Set PORTAL_PASSWORD on the service."},
                    status_code=503,
                )
            return await call_next(request)

        if _portal_authenticated(request):
            return await call_next(request)

        accept = request.headers.get("accept", "")
        if "text/html" in accept and "application/xml" not in accept:
            return RedirectResponse(url="/portal/login", status_code=303)

        return JSONResponse(
            {"detail": "Not authenticated"},
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Harness analytics portal"'},
        )


def install_portal_security(app) -> None:
    """Register session cookies + portal gate (Session must run before PortalAuth on each request)."""
    app.add_middleware(PortalAuthMiddleware)
    app.add_middleware(
        SessionMiddleware,
        secret_key=_session_signing_secret(),
        max_age=14 * 24 * 60 * 60,
        same_site="lax",
        https_only=os.environ.get("RAILWAY_ENVIRONMENT") == "production",
    )


def _format_value(val: object) -> object:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    return val


def _application_field_pairs(app: Application) -> list[tuple[str, object]]:
    pairs: list[tuple[str, object]] = [
        ("Application number", app.application_number),
        ("Filing date", _format_value(app.filing_date)),
        ("Issue date", _format_value(app.issue_date)),
        ("Patent number", app.patent_number),
        ("Status code", app.application_status_code),
        ("Status text", app.application_status_text),
        ("Status date", _format_value(app.application_status_date)),
        ("Customer number", app.customer_number),
        ("HDP customer number", app.hdp_customer_number),
        ("Attorney docket", app.attorney_docket_number),
        ("Confirmation", app.confirmation_number),
        ("GAU", app.group_art_unit),
        ("Class / subclass", f"{app.patent_class or ''} / {app.patent_subclass or ''}".strip(" /") or None),
        ("Examiner", f"{app.examiner_first_name or ''} {app.examiner_last_name or ''}".strip() or None),
        ("Examiner phone", app.examiner_phone),
        ("Assignee", app.assignee_name),
        ("Imported at", _format_value(app.imported_at)),
    ]
    return [(k, v) for k, v in pairs if v is not None or k in ("Application number",)]


def _analytics_field_pairs(aa: ApplicationAnalytics) -> list[tuple[str, object]]:
    pairs: list[tuple[str, object]] = [
        ("Non-final OA count", aa.nonfinal_oa_count),
        ("Final OA count", aa.final_oa_count),
        ("Total substantive OAs", aa.total_substantive_oas),
        ("First OA date", _format_value(aa.first_oa_date)),
        ("First non-final OA date", _format_value(aa.first_nonfinal_oa_date)),
        ("First final OA date", _format_value(aa.first_final_oa_date)),
        ("First NOA date", _format_value(aa.first_noa_date)),
        ("Had examiner interview", aa.had_examiner_interview),
        ("Interview count", aa.interview_count),
        ("Interview before NOA", aa.interview_before_noa),
        ("Interview led to NOA (≤window)", aa.interview_led_to_noa),
        ("Days interview → NOA", aa.days_interview_to_noa),
        ("RCE count", aa.rce_count),
        ("First RCE date", _format_value(aa.first_rce_date)),
        ("Days filing → first OA", aa.days_filing_to_first_oa),
        ("Days filing → NOA", aa.days_filing_to_noa),
        ("Days filing → issue", aa.days_filing_to_issue),
        ("Billing attorney reg", aa.billing_attorney_reg),
        ("Billing attorney name", aa.billing_attorney_name),
        ("Is JAC", aa.is_jac),
        ("Office name", aa.office_name),
        ("Updated at", _format_value(aa.updated_at)),
    ]
    return pairs


def _normalize_lookup_key(raw: str) -> str:
    return "".join(raw.strip().split())


def _find_application(db: Session, raw_key: str) -> Application | None:
    key = _normalize_lookup_key(raw_key)
    if not key:
        return None
    app = db.scalar(select(Application).where(Application.application_number == key))
    if app:
        return app
    digits = re.sub(r"\D", "", key)
    if digits and digits != key:
        return db.scalar(select(Application).where(Application.application_number == digits))
    return None


@router.get("/login", response_class=HTMLResponse)
def portal_login_get(request: Request, invalid: int = 0) -> HTMLResponse:
    pw = _portal_password()
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "default_user": _expected_username(),
            "invalid": bool(invalid),
            "not_configured": not pw,
            "show_sign_out": False,
        },
    )


@router.post("/login")
def portal_login_post(
    request: Request,
    username: str = Form(),
    password: str = Form(),
) -> RedirectResponse:
    expected = _portal_password()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="Portal is not configured. Set PORTAL_PASSWORD on the service.",
        )
    if username != _expected_username() or not secrets.compare_digest(password, expected):
        return RedirectResponse(url="/portal/login?invalid=1", status_code=303)
    request.session[SESSION_KEY] = True
    return RedirectResponse(url="/portal/", status_code=303)


@router.get("/logout")
def portal_logout(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse(url="/portal/login", status_code=303)


@router.get("/", response_class=HTMLResponse)
def portal_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "index.html",
        {"show_sign_out": True},
    )


@router.get("/matter/lookup")
def matter_lookup(q: str) -> RedirectResponse:
    key = _normalize_lookup_key(q)
    if not key:
        raise HTTPException(status_code=400, detail="Missing application number")
    enc = quote(key, safe="")
    return RedirectResponse(url=f"/portal/matter/{enc}", status_code=302)


@router.get("/report.xlsx")
def download_report(db: Session = Depends(get_db)) -> StreamingResponse:
    # Lazy import: pandas/openpyxl are heavy; keep app import fast for Railway healthchecks.
    from harness_analytics.excel_builder import build_excel_workbook, workbook_to_bytesio

    wb = build_excel_workbook(db)
    buf = workbook_to_bytesio(wb)
    data = buf.getvalue()
    headers = {
        "Content-Disposition": 'attachment; filename="harness_analytics_report.xlsx"',
    }
    return StreamingResponse(
        iter([data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.get("/matter/{application_number:path}/xml")
def matter_xml(application_number: str, db: Session = Depends(get_db)) -> StreamingResponse:
    app = _find_application(db, application_number)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    if not app.xml_raw:
        raise HTTPException(status_code=404, detail="XML was not stored for this application")

    def body() -> list[bytes]:
        return [app.xml_raw.encode("utf-8")]

    safe = re.sub(r"[^\w.\-]+", "_", app.application_number)[:80]
    filename = f"biblio_{safe}.xml"
    return StreamingResponse(
        iter(body()),
        media_type="application/xml",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@router.get("/matter/{application_number:path}", response_class=HTMLResponse)
def matter_detail(
    request: Request,
    application_number: str,
    db: Session = Depends(get_db),
) -> HTMLResponse:
    app = _find_application(db, application_number)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")

    aa = db.scalar(
        select(ApplicationAnalytics).where(ApplicationAnalytics.application_id == app.id)
    )

    ev_stmt = (
        select(ProsecutionEvent)
        .where(ProsecutionEvent.application_id == app.id)
        .order_by(ProsecutionEvent.transaction_date, ProsecutionEvent.seq_order.nulls_last())
        .limit(EVENTS_LIMIT + 1)
    )
    events = list(db.scalars(ev_stmt).all())
    events_truncated = len(events) > EVENTS_LIMIT
    if events_truncated:
        events = events[:EVENTS_LIMIT]

    doc_stmt = (
        select(FileWrapperDocument)
        .where(FileWrapperDocument.application_id == app.id)
        .order_by(FileWrapperDocument.mail_room_date.nulls_last(), FileWrapperDocument.id)
        .limit(DOCS_LIMIT + 1)
    )
    documents = list(db.scalars(doc_stmt).all())
    docs_truncated = len(documents) > DOCS_LIMIT
    if docs_truncated:
        documents = documents[:DOCS_LIMIT]

    has_xml = bool(app.xml_raw)
    xml_url = f"/portal/matter/{quote(app.application_number, safe='')}/xml"

    return templates.TemplateResponse(
        request,
        "matter.html",
        {
            "app": app,
            "has_xml": has_xml,
            "xml_url": xml_url,
            "app_fields": _application_field_pairs(app),
            "analytics_fields": _analytics_field_pairs(aa) if aa else [],
            "events": events,
            "events_truncated": events_truncated,
            "documents": documents,
            "docs_truncated": docs_truncated,
            "show_sign_out": True,
        },
    )
