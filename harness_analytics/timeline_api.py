"""Timeline + Actions JSON endpoints (Milestone 4).

Mounted under ``/portal/api`` so the existing portal auth middleware guards
every route. All read endpoints are open to any authenticated user (any role);
mutating action endpoints require at least PARALEGAL.

Endpoints:

* ``GET  /portal/api/timeline/{application_number}``      — full matter timeline
* ``GET  /portal/api/timeline/deadlines/{deadline_id}``   — single deadline detail
* ``POST /portal/api/timeline/deadlines/{id}/actions``    — assign/complete/snooze/note
* ``GET  /portal/api/actions/inbox``                      — bucketed actions inbox

Shapes mirror PROSECUTION_TIMELINE_DESIGN.md §6 ("APIs"). Severity / status /
event-action vocabulary is intentionally kept small and stable so the UI can
render without server-driven CSS class names.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from harness_analytics.auth import CurrentUser, current_user, current_user_optional, require_role
from harness_analytics.db import get_db
from harness_analytics.models import (
    Application,
    ApplicationAnalytics,
    ComputedDeadline,
    DeadlineEvent,
    FileWrapperDocument,
    IfwRule,
    User,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/portal/api", tags=["timeline-api"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_app_lookup(raw: str) -> str:
    return "".join(raw.strip().split())


def _resolve_application(db: Session, key: str) -> Application:
    """Look up an application by its number; tolerate slashes/spaces/leading
    zeros so /portal/api/timeline/18/158,386 lands on 18158386."""
    norm = _normalize_app_lookup(key)
    if not norm:
        raise HTTPException(status_code=400, detail="Missing application number")
    app = db.scalar(select(Application).where(Application.application_number == norm))
    if app is None:
        digits = re.sub(r"\D", "", norm)
        if digits and digits != norm:
            app = db.scalar(
                select(Application).where(Application.application_number == digits)
            )
    if app is None:
        raise HTTPException(status_code=404, detail=f"Application {key!r} not found")
    return app


def _iso(d: Optional[date | datetime]) -> Optional[str]:
    if d is None:
        return None
    if isinstance(d, datetime):
        # SQLAlchemy gives us tz-aware datetimes for `timestamptz` columns.
        return d.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return d.isoformat()


def _rule_for(db: Session, rule_id: int) -> Optional[IfwRule]:
    return db.get(IfwRule, rule_id)


def _serialize_deadline(
    db: Session,
    cd: ComputedDeadline,
    *,
    rules_cache: Optional[dict[int, IfwRule]] = None,
    users_cache: Optional[dict[int, User]] = None,
    include_history: bool = False,
) -> dict[str, Any]:
    rules_cache = rules_cache if rules_cache is not None else {}
    users_cache = users_cache if users_cache is not None else {}

    rule = rules_cache.get(cd.rule_id)
    if rule is None:
        rule = _rule_for(db, cd.rule_id)
        if rule is not None:
            rules_cache[cd.rule_id] = rule

    assigned: Optional[dict[str, Any]] = None
    if cd.assigned_user_id is not None:
        u = users_cache.get(cd.assigned_user_id)
        if u is None:
            u = db.get(User, cd.assigned_user_id)
            if u is not None:
                users_cache[cd.assigned_user_id] = u
        if u is not None:
            assigned = {"id": u.id, "name": u.name or u.email, "email": u.email}

    payload: dict[str, Any] = {
        "id": cd.id,
        "application_id": cd.application_id,
        "rule_code": rule.code if rule else None,
        "rule_kind": rule.kind if rule else None,
        "description": rule.description if rule else None,
        "trigger_date": _iso(cd.trigger_date),
        "trigger_label": cd.trigger_source,
        "trigger_document_id": cd.trigger_document_id,
        "primary_date": _iso(cd.primary_date),
        "primary_label": cd.primary_label,
        "ssp_date": _iso(cd.ssp_date),
        "statutory_bar_date": _iso(cd.statutory_bar_date),
        "window_open_date": _iso(cd.window_open_date),
        "grace_end_date": _iso(cd.grace_end_date),
        "severity": cd.severity,
        "status": cd.status,
        "extendable": bool(rule and rule.kind == "standard_oa"),
        "assigned_user": assigned,
        "snoozed_until": _iso(cd.snoozed_until),
        "notes": cd.notes,
        "rows": list(cd.rows_json or []),
        "ids_phases": list(cd.ids_phases_json or []) if cd.ids_phases_json else None,
        "warnings": list(cd.warnings or []),
        "authority": rule.authority if rule else None,
        "user_note": rule.user_note if rule else None,
        "computed_at": _iso(cd.computed_at),
    }

    if include_history:
        payload["history"] = _serialize_history(db, cd.id, users_cache=users_cache)
    return payload


def _serialize_history(
    db: Session, deadline_id: int, *, users_cache: dict[int, User]
) -> list[dict[str, Any]]:
    rows = (
        db.query(DeadlineEvent)
        .filter(DeadlineEvent.deadline_id == deadline_id)
        .order_by(DeadlineEvent.occurred_at.desc(), DeadlineEvent.id.desc())
        .limit(200)
        .all()
    )
    out: list[dict[str, Any]] = []
    for ev in rows:
        actor: Optional[dict[str, Any]] = None
        if ev.user_id is not None:
            u = users_cache.get(ev.user_id) or db.get(User, ev.user_id)
            if u is not None:
                users_cache[ev.user_id] = u
                actor = {"id": u.id, "name": u.name or u.email}
        out.append(
            {
                "id": ev.id,
                "action": ev.action,
                "occurred_at": _iso(ev.occurred_at),
                "user": actor,
                "payload": ev.payload_json or {},
            }
        )
    return out


def _bucket_for(d: date, today: date) -> str:
    delta = (d - today).days
    if delta < 0:
        return "overdue"
    if delta <= 7:
        return "this_week"
    if delta <= 14:
        return "next_two_weeks"
    return "later"


_BUCKET_ORDER = (
    ("overdue", "Overdue"),
    ("this_week", "This week"),
    ("next_two_weeks", "Next 2 weeks"),
    ("later", "Days 15-30+"),
)


# ---------------------------------------------------------------------------
# GET /timeline/{application_number}
# ---------------------------------------------------------------------------


@router.get("/timeline/{application_number:path}")
def timeline_for_application(
    application_number: str,
    db: Session = Depends(get_db),
    _user: Optional[CurrentUser] = Depends(current_user_optional),
) -> JSONResponse:
    app = _resolve_application(db, application_number)

    deadlines = (
        db.query(ComputedDeadline)
        .filter(ComputedDeadline.application_id == app.id)
        .order_by(ComputedDeadline.primary_date.asc(), ComputedDeadline.id.asc())
        .all()
    )

    rules_cache: dict[int, IfwRule] = {}
    users_cache: dict[int, User] = {}

    response_windows: list[dict[str, Any]] = []
    informational: list[dict[str, Any]] = []
    for cd in deadlines:
        ser = _serialize_deadline(
            db, cd, rules_cache=rules_cache, users_cache=users_cache
        )
        kind = ser.get("rule_kind") or ""
        # response_windows carry a fee schedule the attorney must act on;
        # everything else is informational (milestones / soft windows).
        if kind in {"standard_oa", "hard_noa", "appeal_brief"}:
            response_windows.append(ser)
        else:
            informational.append(ser)

    # Build milestones from durable application facts + IFW history. We don't
    # need every IFW row — just the canonical events the attorney expects.
    milestones = _build_milestones(db, app)

    # Status pill from the most urgent OPEN response window.
    status_pill = _status_pill(deadlines)

    return JSONResponse(
        {
            "application_number": app.application_number,
            "title": app.invention_title,
            "filing_date": _iso(app.filing_date),
            "issue_date": _iso(app.issue_date),
            "patent_number": app.patent_number,
            "status_pill": status_pill,
            "milestones": milestones,
            "response_windows": response_windows,
            "informational": informational,
            "disclaimer": (
                "Derived from USPTO file history. Not a substitute for docketing."
            ),
            "as_of": _iso(datetime.now(timezone.utc)),
        }
    )


def _status_pill(deadlines: list[ComputedDeadline]) -> dict[str, Any]:
    open_rows = [d for d in deadlines if d.status == "OPEN"]
    if not open_rows:
        return {"label": "No open deadlines", "severity": "info"}
    open_rows.sort(key=lambda d: d.primary_date)
    head = open_rows[0]
    label = f"{head.primary_label} due {head.primary_date.isoformat()}"
    return {"label": label, "severity": head.severity or "info"}


def _build_milestones(db: Session, app: Application) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if app.filing_date:
        out.append(
            {
                "date": _iso(app.filing_date),
                "label": "Filed",
                "severity": "info",
                "source": "FILING_DATE",
            }
        )
    if app.earliest_priority_date and app.earliest_priority_date != app.filing_date:
        out.append(
            {
                "date": _iso(app.earliest_priority_date),
                "label": "Earliest priority",
                "severity": "info",
                "source": "PRIORITY",
            }
        )
    if app.issue_date:
        out.append(
            {
                "date": _iso(app.issue_date),
                "label": (
                    f"Patent {app.patent_number} issued"
                    if app.patent_number
                    else "Patent issued"
                ),
                "severity": "info",
                "source": "ISSUE_DATE",
            }
        )

    ifw_rows = (
        db.query(FileWrapperDocument)
        .filter(FileWrapperDocument.application_id == app.id)
        .order_by(FileWrapperDocument.mail_room_date.asc())
        .all()
    )
    for row in ifw_rows:
        if not row.mail_room_date:
            continue
        label = _milestone_label_for_code(row.document_code)
        if label is None:
            continue
        out.append(
            {
                "date": _iso(row.mail_room_date),
                "label": label,
                "severity": "info",
                "source": row.document_code,
            }
        )

    out.sort(key=lambda m: (m["date"] or "", m["source"]))
    return out


_MILESTONE_LABELS: dict[str, str] = {
    "CTNF": "Non-Final Office Action mailed",
    "CTFR": "Final Office Action mailed",
    "CTRS": "Restriction Requirement mailed",
    "NOA": "Notice of Allowance mailed",
    "ISSUE.NTF": "Issue Notification mailed",
    "ABN": "Abandonment notice",
    "EXIN": "Examiner interview held",
    "RCEX": "RCE filed",
    "A...": "Applicant response filed",
    "A.NE": "Amendment / Response after Final filed",
    "A.NA": "Amendment / Response after Allowance filed",
}


def _milestone_label_for_code(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    return _MILESTONE_LABELS.get(code.upper())


# ---------------------------------------------------------------------------
# GET /timeline/deadlines/{id}
# ---------------------------------------------------------------------------


@router.get("/timeline/deadlines/{deadline_id}")
def deadline_detail(
    deadline_id: int,
    db: Session = Depends(get_db),
    _user: Optional[CurrentUser] = Depends(current_user_optional),
) -> JSONResponse:
    cd = db.get(ComputedDeadline, deadline_id)
    if cd is None:
        raise HTTPException(status_code=404, detail="Deadline not found")
    app = db.get(Application, cd.application_id)
    payload = _serialize_deadline(db, cd, include_history=True)
    payload["application_number"] = app.application_number if app else None
    payload["application_title"] = app.invention_title if app else None
    return JSONResponse(payload)


# ---------------------------------------------------------------------------
# POST /timeline/deadlines/{id}/actions
# ---------------------------------------------------------------------------


_ALLOWED_ACTIONS = {"assign", "complete", "snooze", "unsnooze", "note", "reopen"}


@router.post("/timeline/deadlines/{deadline_id}/actions")
def deadline_action(
    deadline_id: int,
    body: dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("PARALEGAL")),
) -> JSONResponse:
    action = (body.get("action") or "").strip().lower()
    if action not in _ALLOWED_ACTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown action {action!r}; expected one of {sorted(_ALLOWED_ACTIONS)}",
        )
    cd = db.get(ComputedDeadline, deadline_id)
    if cd is None:
        raise HTTPException(status_code=404, detail="Deadline not found")

    payload = body.get("payload") or {}
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="payload must be a JSON object")

    audit_payload: dict[str, Any] = {"before": {}, "after": {}, "input": payload}

    if action == "assign":
        new_user_id = payload.get("user_id")
        if new_user_id is not None:
            new_user = db.get(User, int(new_user_id))
            if new_user is None:
                raise HTTPException(status_code=404, detail="Assignee user not found")
            audit_payload["before"]["assigned_user_id"] = cd.assigned_user_id
            cd.assigned_user_id = new_user.id
            audit_payload["after"]["assigned_user_id"] = new_user.id
        else:
            audit_payload["before"]["assigned_user_id"] = cd.assigned_user_id
            cd.assigned_user_id = None
            audit_payload["after"]["assigned_user_id"] = None

    elif action == "complete":
        if cd.status not in {"OPEN", "SUPERSEDED"}:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot complete deadline in status {cd.status!r}",
            )
        audit_payload["before"]["status"] = cd.status
        cd.status = "COMPLETED"
        cd.completed_at = datetime.now(timezone.utc)
        audit_payload["after"]["status"] = "COMPLETED"

    elif action == "snooze":
        until_raw = payload.get("until")
        if not until_raw:
            raise HTTPException(status_code=400, detail="snooze requires 'until' (ISO date)")
        try:
            until = date.fromisoformat(str(until_raw))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Bad until: {exc}") from exc
        audit_payload["before"]["snoozed_until"] = _iso(cd.snoozed_until)
        cd.snoozed_until = until
        audit_payload["after"]["snoozed_until"] = _iso(until)

    elif action == "unsnooze":
        audit_payload["before"]["snoozed_until"] = _iso(cd.snoozed_until)
        cd.snoozed_until = None
        audit_payload["after"]["snoozed_until"] = None

    elif action == "note":
        text_in = (payload.get("text") or "").strip()
        if not text_in:
            raise HTTPException(status_code=400, detail="note requires 'text'")
        audit_payload["before"]["notes"] = cd.notes
        cd.notes = (cd.notes + "\n\n" if cd.notes else "") + text_in
        audit_payload["after"]["notes_appended"] = text_in

    elif action == "reopen":
        if cd.status == "OPEN":
            raise HTTPException(status_code=409, detail="Deadline is already open")
        audit_payload["before"]["status"] = cd.status
        cd.status = "OPEN"
        cd.completed_at = None
        audit_payload["after"]["status"] = "OPEN"

    db.add(
        DeadlineEvent(
            deadline_id=cd.id,
            user_id=user.id,
            action=action.upper(),
            payload_json=audit_payload,
        )
    )
    db.commit()
    db.refresh(cd)
    return JSONResponse(_serialize_deadline(db, cd, include_history=True))


# ---------------------------------------------------------------------------
# GET /actions/inbox
# ---------------------------------------------------------------------------


_VALID_WINDOWS = {"7", "30", "90", "all"}
_VALID_SEVERITIES = {"danger", "warn", "info", "all"}
_VALID_STATUS = {"open", "overdue", "snoozed", "all"}
_VALID_ASSIGNEE = {"me", "all", "unassigned"}


@router.get("/actions/inbox")
def actions_inbox(
    window: str = Query("30"),
    assignee: str = Query("all"),
    severity: str = Query("all"),
    status: str = Query("open"),
    db: Session = Depends(get_db),
    user: Optional[CurrentUser] = Depends(current_user_optional),
) -> JSONResponse:
    if window not in _VALID_WINDOWS:
        raise HTTPException(status_code=400, detail=f"window must be one of {_VALID_WINDOWS}")
    if assignee not in _VALID_ASSIGNEE:
        raise HTTPException(status_code=400, detail=f"assignee must be one of {_VALID_ASSIGNEE}")
    if severity not in _VALID_SEVERITIES:
        raise HTTPException(status_code=400, detail=f"severity must be one of {_VALID_SEVERITIES}")
    if status not in _VALID_STATUS:
        raise HTTPException(status_code=400, detail=f"status must be one of {_VALID_STATUS}")

    today = date.today()
    tenant_id = user.tenant_id if user else "global"
    user_id = user.id if user else None
    q = db.query(ComputedDeadline).filter(
        ComputedDeadline.tenant_id == tenant_id,
    )
    if status == "open":
        q = q.filter(ComputedDeadline.status == "OPEN")
    elif status == "overdue":
        q = q.filter(
            ComputedDeadline.status == "OPEN",
            ComputedDeadline.primary_date < today,
        )
    elif status == "snoozed":
        q = q.filter(ComputedDeadline.snoozed_until >= today)

    if window != "all":
        cutoff = today + timedelta(days=int(window))
        q = q.filter(ComputedDeadline.primary_date <= cutoff)

    if severity != "all":
        q = q.filter(ComputedDeadline.severity == severity)

    if assignee == "me":
        if user_id is None:
            raise HTTPException(
                status_code=400,
                detail="assignee=me requires an authenticated user session",
            )
        q = q.filter(ComputedDeadline.assigned_user_id == user_id)
    elif assignee == "unassigned":
        q = q.filter(ComputedDeadline.assigned_user_id.is_(None))

    deadlines = q.order_by(ComputedDeadline.primary_date.asc()).limit(2000).all()

    rules_cache: dict[int, IfwRule] = {}
    users_cache: dict[int, User] = {}
    apps_cache: dict[int, Application] = {}
    buckets: dict[str, list[dict[str, Any]]] = {key: [] for key, _ in _BUCKET_ORDER}

    for cd in deadlines:
        bucket = _bucket_for(cd.primary_date, today)
        item = _serialize_deadline(
            db, cd, rules_cache=rules_cache, users_cache=users_cache
        )
        app = apps_cache.get(cd.application_id)
        if app is None:
            app = db.get(Application, cd.application_id)
            if app is not None:
                apps_cache[cd.application_id] = app
        if app is not None:
            item["application_number"] = app.application_number
            item["application_title"] = app.invention_title
        buckets[bucket].append(item)

    payload_buckets = []
    for key, label in _BUCKET_ORDER:
        items = buckets[key]
        payload_buckets.append(
            {"key": key, "label": label, "count": len(items), "items": items}
        )

    return JSONResponse(
        {
            "filters_applied": {
                "window": window,
                "assignee": assignee,
                "severity": severity,
                "status": status,
            },
            "buckets": payload_buckets,
            "total": sum(b["count"] for b in payload_buckets),
            "as_of": _iso(datetime.now(timezone.utc)),
        }
    )
