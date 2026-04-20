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

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.security import HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware

from harness_analytics import app_settings
from harness_analytics.analytics import compute_analytics_for_application, load_office_config
from harness_analytics.auth import (
    SESSION_COOKIE,
    SESSION_TTL,
    authenticate,
    bootstrap_owner_from_env,
    current_user_optional,
    has_any_owner,
    issue_session,
    lookup_session,
    revoke_session,
)
from harness_analytics.db import get_db, get_session_factory
from harness_analytics.models import Application, ApplicationAnalytics, FileWrapperDocument, ProsecutionEvent
from harness_analytics.portfolio_api import (
    SETTING_KEY_AGG_ROW_CAP,
    _aggregate_row_cap,
    _DEFAULT_AGG_ROW_CAP,
)
from harness_analytics.reports import (
    ANALYTICS_REPORT_HEADER_LABELS,
    analytics_column_header,
    report_spreadsheet_row_for_application,
)

router = APIRouter(prefix="/portal", tags=["portal"])

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def _compute_static_version() -> str:
    """Cache-buster for /static asset URLs.

    Uses RAILWAY_DEPLOYMENT_ID when present (changes on every deploy) and
    otherwise falls back to the max mtime of files under ``static/`` at import
    time. Either way the resulting string changes on each new deploy, which
    forces browsers off any stale cached portfolio.js / portfolio.css.
    """
    rid = os.environ.get("RAILWAY_DEPLOYMENT_ID") or os.environ.get("RAILWAY_GIT_COMMIT_SHA")
    if rid:
        return rid[:12]
    static_dir = Path(__file__).resolve().parent / "static"
    try:
        latest = max(p.stat().st_mtime for p in static_dir.rglob("*") if p.is_file())
        return str(int(latest))
    except (ValueError, OSError):
        return "1"


STATIC_VERSION = _compute_static_version()
templates.env.globals["static_version"] = STATIC_VERSION

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


def _db_session_user(request: Request):
    """Look up the DB-backed session token if present. Returns CurrentUser or None.

    Opens a short-lived DB session — middleware doesn't get FastAPI deps.
    """
    sid = request.cookies.get(SESSION_COOKIE)
    if not sid:
        return None
    try:
        SessionLocal = get_session_factory()
    except RuntimeError:
        return None
    db = SessionLocal()
    try:
        return lookup_session(db, sid)
    finally:
        db.close()


def _portal_authenticated(request: Request) -> bool:
    # Real DB-backed session wins.
    cu = _db_session_user(request)
    if cu is not None:
        request.state.current_user = cu
        return True
    # Legacy starlette signed-cookie flag (transitional).
    if request.session.get(SESSION_KEY) is True:
        return True
    # Basic auth still works during rollout.
    creds = _basic_credentials_from_request(request)
    return bool(creds and _basic_credentials_valid(creds))


class PortalAuthMiddleware(BaseHTTPMiddleware):
    """Require password (session or Basic) for every /portal path except login/logout."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        path = request.url.path

        if not path.startswith("/portal"):
            return await call_next(request)

        if path in ("/portal/login", "/portal/login/"):
            return await call_next(request)

        if path in ("/portal/logout", "/portal/logout/"):
            return await call_next(request)

        # Authentication is configured if EITHER PORTAL_PASSWORD is set OR a
        # real user exists in the DB.
        if not _portal_password():
            try:
                SessionLocal = get_session_factory()
                with SessionLocal() as db:
                    if not has_any_owner(db):
                        return JSONResponse(
                            {
                                "detail": (
                                    "Portal is not configured. Set PORTAL_PASSWORD or "
                                    "create a user via `python -m harness_analytics users add`."
                                )
                            },
                            status_code=503,
                        )
            except RuntimeError:
                return JSONResponse(
                    {"detail": "Portal is not configured (no DATABASE_URL)."},
                    status_code=503,
                )

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
        (
            "Is continuation (prior US parent)",
            "Yes" if app.continuity_child_of_prior_us else "No",
        ),
        ("Imported at", _format_value(app.imported_at)),
    ]
    return [
        (k, v)
        for k, v in pairs
        if v is not None or k in ("Application number", "Is continuation (prior US parent)")
    ]


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
        ("IFW A.NE count", aa.ifw_a_ne_count),
        (ANALYTICS_REPORT_HEADER_LABELS["ifw_ctrs_count"], aa.ifw_ctrs_count),
        ("Interview before NOA", aa.interview_before_noa),
        (ANALYTICS_REPORT_HEADER_LABELS["interview_led_to_noa"], aa.interview_led_to_noa),
        (ANALYTICS_REPORT_HEADER_LABELS["days_interview_to_noa"], aa.days_interview_to_noa),
        ("RCE count", aa.rce_count),
        (ANALYTICS_REPORT_HEADER_LABELS["ctnf_ext_1mo_count"], aa.ctnf_ext_1mo_count),
        (ANALYTICS_REPORT_HEADER_LABELS["ctnf_ext_2mo_count"], aa.ctnf_ext_2mo_count),
        (ANALYTICS_REPORT_HEADER_LABELS["ctnf_ext_3mo_count"], aa.ctnf_ext_3mo_count),
        (ANALYTICS_REPORT_HEADER_LABELS["ctfr_ext_1mo_count"], aa.ctfr_ext_1mo_count),
        (ANALYTICS_REPORT_HEADER_LABELS["ctfr_ext_2mo_count"], aa.ctfr_ext_2mo_count),
        (ANALYTICS_REPORT_HEADER_LABELS["ctfr_ext_3mo_count"], aa.ctfr_ext_3mo_count),
        (ANALYTICS_REPORT_HEADER_LABELS["ctrs_ext_1mo_count"], aa.ctrs_ext_1mo_count),
        (ANALYTICS_REPORT_HEADER_LABELS["ctrs_ext_2mo_count"], aa.ctrs_ext_2mo_count),
        (ANALYTICS_REPORT_HEADER_LABELS["ctrs_ext_3mo_count"], aa.ctrs_ext_3mo_count),
        ("First RCE date", _format_value(aa.first_rce_date)),
        ("Days filing → first OA", aa.days_filing_to_first_oa),
        ("Days filing → NOA", aa.days_filing_to_noa),
        ("Days filing → issue", aa.days_filing_to_issue),
        ("Is JAC", aa.is_jac),
        ("Office name", aa.office_name),
        ("Updated at", _format_value(aa.updated_at)),
    ]
    return pairs


def _matter_analytics_field_pairs(app: Application, aa: ApplicationAnalytics) -> list[tuple[str, object]]:
    """Matter page: lead with continuation / restriction so they are not buried in the list."""
    ctrs = aa.ifw_ctrs_count or 0
    summary = [
        ("Has restriction (CTRS in IFW)", "Yes" if ctrs > 0 else "No"),
    ]
    return summary + _analytics_field_pairs(aa)


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


def _portal_interview_window_days() -> int:
    raw = os.environ.get("INTERVIEW_WINDOW_DAYS", "90")
    try:
        return max(1, int(raw))
    except ValueError:
        return 90


@router.get("/login", response_class=HTMLResponse)
def portal_login_get(request: Request, invalid: int = 0) -> HTMLResponse:
    pw = _portal_password()
    SessionLocal = None
    try:
        SessionLocal = get_session_factory()
    except RuntimeError:
        SessionLocal = None
    has_owner = False
    if SessionLocal is not None:
        with SessionLocal() as db:
            has_owner = has_any_owner(db)
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "default_user": _expected_username(),
            "invalid": bool(invalid),
            "not_configured": not (pw or has_owner),
            "has_users": has_owner,
            "show_sign_out": False,
        },
    )


@router.post("/login")
def portal_login_post(
    request: Request,
    username: str = Form(),
    password: str = Form(),
) -> RedirectResponse:
    # Try DB user first when a database is available.
    SessionLocal = None
    try:
        SessionLocal = get_session_factory()
    except RuntimeError:
        SessionLocal = None

    if SessionLocal is not None:
        with SessionLocal() as db:
            user = authenticate(db, username, password)
            if user is None and "@" not in username:
                user = authenticate(db, f"{username}@harness.local", password)
            if user is not None:
                sid = issue_session(
                    db,
                    user,
                    user_agent=request.headers.get("user-agent"),
                    ip=(request.client.host if request.client else None),
                )
                resp = RedirectResponse(url="/portal/", status_code=303)
                resp.set_cookie(
                    SESSION_COOKIE,
                    sid,
                    max_age=int(SESSION_TTL.total_seconds()),
                    httponly=True,
                    samesite="lax",
                    secure=os.environ.get("RAILWAY_ENVIRONMENT") == "production",
                    path="/",
                )
                return resp

    # Legacy shared-password fallback.
    expected = _portal_password()
    if expected and username == _expected_username() and secrets.compare_digest(password, expected):
        request.session[SESSION_KEY] = True
        return RedirectResponse(url="/portal/", status_code=303)

    return RedirectResponse(url="/portal/login?invalid=1", status_code=303)


@router.get("/logout")
def portal_logout(request: Request) -> RedirectResponse:
    sid = request.cookies.get(SESSION_COOKIE)
    if sid:
        try:
            SessionLocal = get_session_factory()
            with SessionLocal() as db:
                revoke_session(db, sid)
        except Exception:
            pass
    request.session.clear()
    resp = RedirectResponse(url="/portal/login", status_code=303)
    resp.delete_cookie(SESSION_COOKIE, path="/")
    return resp


@router.get("/", response_class=HTMLResponse)
def portal_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "index.html",
        {"show_sign_out": True},
    )


@router.get("/portfolio", response_class=HTMLResponse)
def portal_portfolio(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "portfolio.html",
        {"show_sign_out": True},
    )


@router.get("/actions", response_class=HTMLResponse)
def portal_actions_inbox(request: Request) -> HTMLResponse:
    """Upcoming Actions inbox (M6).

    Page is fully static — JS at /static/actions_inbox.js handles all
    fetching, filter chips, and drawer wiring.
    """
    return templates.TemplateResponse(
        request,
        "actions_inbox.html",
        {"show_sign_out": True},
    )


def _settings_context(request: Request, *, saved: bool = False, error: str | None = None) -> dict:
    db_value = app_settings.get_setting(SETTING_KEY_AGG_ROW_CAP)
    env_value = os.environ.get("PORTFOLIO_AGG_ROW_CAP", "")
    effective = _aggregate_row_cap()
    return {
        "show_sign_out": True,
        "portfolio_cap_db": db_value or "",
        "portfolio_cap_env": env_value,
        "portfolio_cap_default": _DEFAULT_AGG_ROW_CAP,
        "portfolio_cap_effective": effective,
        "saved": saved,
        "error": error,
    }


@router.get("/settings", response_class=HTMLResponse)
def portal_settings(request: Request) -> HTMLResponse:
    saved = request.query_params.get("saved") == "1"
    error = request.query_params.get("error") or None
    return templates.TemplateResponse(
        request,
        "settings.html",
        _settings_context(request, saved=saved, error=error),
    )


@router.post("/settings/portfolio-cap")
def portal_settings_save_portfolio_cap(
    value: str = Form(""),
) -> RedirectResponse:
    raw = (value or "").strip()
    if not raw:
        try:
            app_settings.set_setting(SETTING_KEY_AGG_ROW_CAP, None)
        except Exception:
            return RedirectResponse(url="/portal/settings?error=db", status_code=303)
        return RedirectResponse(url="/portal/settings?saved=1", status_code=303)
    try:
        n = int(raw)
        if n < 0:
            raise ValueError
    except ValueError:
        return RedirectResponse(url="/portal/settings?error=invalid", status_code=303)
    try:
        app_settings.set_setting(SETTING_KEY_AGG_ROW_CAP, str(n))
    except Exception:
        return RedirectResponse(url="/portal/settings?error=db", status_code=303)
    return RedirectResponse(url="/portal/settings?saved=1", status_code=303)


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


@router.get("/report-all-applications.xlsx")
@router.get("/report-all-years.xlsx")
def download_report_all_applications(db: Session = Depends(get_db)) -> StreamingResponse:
    """Every application (any status); analytics columns populated when an analytics row exists."""
    from harness_analytics.excel_builder import build_excel_workbook_all_applications, workbook_to_bytesio

    wb = build_excel_workbook_all_applications(db)
    buf = workbook_to_bytesio(wb)
    data = buf.getvalue()
    headers = {
        "Content-Disposition": 'attachment; filename="harness_analytics_report_all_applications.xlsx"',
    }
    return StreamingResponse(
        iter([data]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.post("/recompute-all-analytics/start")
def portal_recompute_all_start(
    background_tasks: BackgroundTasks,
) -> JSONResponse:
    from harness_analytics import bulk_recompute as br

    job, hint = br.try_begin_bulk_recompute()
    payload = {"hint": hint, **br.job_to_json(job)}
    if hint == "already_active":
        return JSONResponse(payload, status_code=200)
    background_tasks.add_task(
        br.run_bulk_recompute_job,
        job.job_id,
        _portal_interview_window_days(),
    )
    return JSONResponse(payload, status_code=200)


@router.get("/recompute-all-analytics/status/{job_id}")
def portal_recompute_all_status(job_id: str) -> JSONResponse:
    from harness_analytics import bulk_recompute as br

    job = br.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Unknown job")
    return JSONResponse(br.job_to_json(job))


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


@router.post("/matter/{application_number:path}/recompute-analytics")
def portal_recompute_analytics(
    application_number: str,
    db: Session = Depends(get_db),
) -> RedirectResponse:
    app = _find_application(db, application_number)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    office_cfg = load_office_config()
    compute_analytics_for_application(
        db,
        app,
        interview_window_days=_portal_interview_window_days(),
        office_cfg=office_cfg,
    )
    db.commit()
    loc = f"/portal/matter/{quote(app.application_number, safe='')}?recomputed=1"
    return RedirectResponse(url=loc, status_code=303)


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
    recompute_url = f"/portal/matter/{quote(app.application_number, safe='')}/recompute-analytics"

    df_row = report_spreadsheet_row_for_application(db, app.application_number)
    spreadsheet_headers: list[str] = []
    spreadsheet_values: list[object] = []
    if not df_row.empty:
        spreadsheet_headers = [analytics_column_header(str(c)) for c in df_row.columns]
        spreadsheet_values = [_format_value(v) for v in df_row.iloc[0].tolist()]

    recomputed = request.query_params.get("recomputed") == "1"

    return templates.TemplateResponse(
        request,
        "matter.html",
        {
            "app": app,
            "has_xml": has_xml,
            "xml_url": xml_url,
            "recompute_url": recompute_url,
            "recomputed": recomputed,
            "spreadsheet_headers": spreadsheet_headers,
            "spreadsheet_values": spreadsheet_values,
            "app_fields": _application_field_pairs(app),
            "analytics_fields": _matter_analytics_field_pairs(app, aa) if aa else [],
            "events": events,
            "events_truncated": events_truncated,
            "documents": documents,
            "docs_truncated": docs_truncated,
            "show_sign_out": True,
        },
    )
