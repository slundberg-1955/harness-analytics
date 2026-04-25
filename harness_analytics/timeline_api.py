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

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
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
    IfwRuleVersion,
    SupersessionMap,
    User,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/portal/api", tags=["timeline-api"])

# Separate (unauthenticated) router for the per-user ICS feed. The portal
# middleware lets /portal/ics/* through; the route itself authenticates
# against users.ics_token.
ics_router = APIRouter(prefix="/portal/ics", tags=["timeline-ics"])


@ics_router.get("/{user_id}.ics")
def user_ics_feed(
    user_id: int,
    request: Request,
    token: str = Query(""),
    db: Session = Depends(get_db),
) -> Response:
    from harness_analytics.timeline.ics import (
        find_user_by_token,
        render_user_feed,
    )

    target = find_user_by_token(db, user_id, token)
    if target is None:
        # Don't leak whether the user exists.
        raise HTTPException(status_code=404, detail="Calendar not found")

    def label_for(rule_id: int) -> Optional[str]:
        rule = _rule_for(db, rule_id)
        return rule.description if rule else None

    base_url = str(request.base_url).rstrip("/")
    body = render_user_feed(
        db, target, base_url=base_url, rules_label_lookup=label_for
    )
    return Response(
        content=body,
        media_type="text/calendar; charset=utf-8",
        headers={
            "Content-Disposition": f'inline; filename="harness-{user_id}.ics"',
            "Cache-Control": "private, max-age=300",
        },
    )


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

    # M9: surface verification status. Done as a left-join-style lookup so
    # the API doesn't add a second query in the hot read path.
    verified_payload: Optional[dict[str, Any]] = None
    try:
        from harness_analytics.models import VerifiedDeadline

        vd = db.scalar(
            select(VerifiedDeadline).where(VerifiedDeadline.deadline_id == cd.id)
        )
        if vd is not None:
            actor = None
            if vd.verified_by_user_id is not None:
                u = users_cache.get(vd.verified_by_user_id) or db.get(
                    User, vd.verified_by_user_id
                )
                if u is not None:
                    users_cache[vd.verified_by_user_id] = u
                    actor = {"id": u.id, "name": u.name or u.email}
            verified_payload = {
                "verified_at": _iso(vd.verified_at),
                "verified_date": _iso(vd.verified_date),
                "verified_by": actor,
                "source": vd.source,
                "note": vd.note,
            }
    except Exception:  # noqa: BLE001
        # verified_deadlines may not exist yet on environments that haven't
        # applied the 0005 migration; we don't want to break the timeline UI.
        verified_payload = None

    # M0009: surface the close-audit triplet in one field when the deadline
    # is in a terminal state. The matter / inbox UI uses ``close_info`` to
    # render the "Closed by rule X" subtitle without a second query.
    close_info: Optional[dict[str, Any]] = None
    if cd.status in {"COMPLETED", "NAR"} and (
        cd.closed_disposition
        or cd.closed_by_rule_pattern
        or cd.closed_by_ifw_document_id is not None
    ):
        close_info = {
            "disposition": cd.closed_disposition,
            "matched_pattern": cd.closed_by_rule_pattern,
            "closed_by_ifw_document_id": cd.closed_by_ifw_document_id,
            "closed_at": _iso(cd.completed_at),
        }

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
        "verified": verified_payload,
        "close_info": close_info,
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
# GET /timeline/deadlines/{id}
# ---------------------------------------------------------------------------
#
# IMPORTANT: this route MUST be registered before the
# ``/timeline/{application_number:path}`` route below. The ``:path`` converter
# matches greedily (including slashes), so if the application-number route is
# registered first it shadows this one and every deadline detail call ends up
# hitting ``_resolve_application`` with ``"deadlines/<id>"``.


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
# POST /timeline/deadlines/{id}/actions
# ---------------------------------------------------------------------------


# M9: 'verify' / 'unverify' join the existing action verbs so they share the
# same audit trail, role gate, and shape on the wire.
# M0009: 'nar' / 'un-nar' add the manual NAR ("No Action Required")
# lifecycle alongside complete/reopen.
_ALLOWED_ACTIONS = {
    "assign", "complete", "snooze", "unsnooze", "note", "reopen",
    "verify", "unverify",
    "nar", "un-nar",
}


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
        cd.closed_disposition = "manual_complete"
        audit_payload["after"]["status"] = "COMPLETED"
        audit_payload["after"]["closed_disposition"] = "manual_complete"

    elif action == "nar":
        # Manual "No Action Required". Distinct from `complete` so dashboards
        # can split out items the attorney consciously stamped as not needing
        # work. A free-text reason is required so the audit row carries
        # context.
        if cd.status not in {"OPEN", "SUPERSEDED"}:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot NAR deadline in status {cd.status!r}",
            )
        reason = (payload.get("reason") or "").strip()
        if not reason:
            raise HTTPException(
                status_code=400, detail="nar requires non-empty 'reason'"
            )
        audit_payload["before"]["status"] = cd.status
        cd.status = "NAR"
        cd.completed_at = datetime.now(timezone.utc)
        cd.closed_disposition = "manual_nar"
        audit_payload["after"]["status"] = "NAR"
        audit_payload["after"]["closed_disposition"] = "manual_nar"
        audit_payload["after"]["reason"] = reason

    elif action == "un-nar":
        # Reverse a manual NAR back to OPEN. Distinct verb from REOPEN so the
        # history is clear about which terminal state we came from.
        if cd.status != "NAR":
            raise HTTPException(
                status_code=409,
                detail=f"Cannot un-NAR deadline in status {cd.status!r}",
            )
        audit_payload["before"]["status"] = cd.status
        cd.status = "OPEN"
        cd.completed_at = None
        cd.closed_by_ifw_document_id = None
        cd.closed_by_rule_pattern = None
        cd.closed_disposition = None
        audit_payload["after"]["status"] = "OPEN"

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
        # Clear close-audit columns so a reopened row doesn't carry stale
        # auto-close metadata from a prior recompute.
        cd.closed_by_ifw_document_id = None
        cd.closed_by_rule_pattern = None
        cd.closed_disposition = None
        audit_payload["after"]["status"] = "OPEN"

    elif action == "verify":
        # Attorney spot-check: stamp the deadline as verified by this user.
        # Requires ATTORNEY+ in spirit; we keep it at PARALEGAL+ on the role
        # gate but record who verified so dashboards can filter.
        from harness_analytics.models import VerifiedDeadline

        existing = db.scalar(
            select(VerifiedDeadline).where(VerifiedDeadline.deadline_id == cd.id)
        )
        note_in = (payload.get("note") or "").strip() or None
        verified_date = cd.primary_date
        if existing is not None:
            audit_payload["before"]["verified_by_user_id"] = existing.verified_by_user_id
            existing.verified_by_user_id = user.id
            existing.verified_date = verified_date
            existing.verified_at = datetime.now(timezone.utc)
            existing.note = note_in
        else:
            db.add(
                VerifiedDeadline(
                    deadline_id=cd.id,
                    verified_by_user_id=user.id,
                    verified_date=verified_date,
                    source="manual",
                    note=note_in,
                )
            )
        audit_payload["after"]["verified_by_user_id"] = user.id

    elif action == "unverify":
        from harness_analytics.models import VerifiedDeadline

        existing = db.scalar(
            select(VerifiedDeadline).where(VerifiedDeadline.deadline_id == cd.id)
        )
        if existing is None:
            raise HTTPException(status_code=409, detail="Deadline is not verified")
        audit_payload["before"]["verified_by_user_id"] = existing.verified_by_user_id
        db.delete(existing)
        audit_payload["after"]["verified_by_user_id"] = None

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
_VALID_STATUS = {"open", "overdue", "snoozed", "nar", "all"}
# M11: ``team`` shows every open deadline assigned to the caller's direct
# reports (users.manager_user_id == caller.id). Reserved for ATTORNEY+.
_VALID_ASSIGNEE = {"me", "all", "unassigned", "team"}
_TEAM_VIEW_MIN_ROLE = "ATTORNEY"


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
    elif status == "nar":
        # M0009: pull only NAR'd items (manual or auto). Default ``open``
        # remains NAR-excluding so the inbox doesn't flood with closed work.
        q = q.filter(ComputedDeadline.status == "NAR")

    if window != "all":
        cutoff = today + timedelta(days=int(window))
        q = q.filter(ComputedDeadline.primary_date <= cutoff)

    if severity != "all":
        q = q.filter(ComputedDeadline.severity == severity)
    else:
        # Default "All" view = actionable items only (warn + danger). Purely
        # informational rows (e.g. ids_phase phase tables anchored to the
        # filing date) would otherwise flood the Overdue bucket years after
        # the fact. Click the Info chip to opt back in.
        q = q.filter(ComputedDeadline.severity != "info")

    if assignee == "me":
        if user_id is None:
            raise HTTPException(
                status_code=400,
                detail="assignee=me requires an authenticated user session",
            )
        q = q.filter(ComputedDeadline.assigned_user_id == user_id)
    elif assignee == "unassigned":
        q = q.filter(ComputedDeadline.assigned_user_id.is_(None))
    elif assignee == "team":
        # Supervising attorneys can pull a roll-up of every deadline assigned
        # to a direct report. Plain VIEWER/PARALEGAL never gets the chip in
        # the UI; the API enforces the role gate as a backstop.
        from harness_analytics.auth import role_at_least

        if user_id is None or user is None:
            raise HTTPException(
                status_code=401,
                detail="assignee=team requires an authenticated user session",
            )
        if not role_at_least(user.role, _TEAM_VIEW_MIN_ROLE):
            raise HTTPException(
                status_code=403,
                detail=(
                    "assignee=team requires "
                    f"{_TEAM_VIEW_MIN_ROLE} or higher"
                ),
            )
        report_ids = [
            int(rid)
            for rid in db.scalars(
                select(User.id).where(User.manager_user_id == user_id)
            ).all()
        ]
        if not report_ids:
            # No reports → empty inbox rather than 404, so the UI can render
            # a friendly empty state instead of an error.
            q = q.filter(ComputedDeadline.assigned_user_id.is_(None)).filter(
                ComputedDeadline.id == -1
            )
        else:
            q = q.filter(ComputedDeadline.assigned_user_id.in_(report_ids))

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


# ---------------------------------------------------------------------------
# M8: Rules admin endpoints (ADMIN/OWNER)
#
# These power /portal/admin/rules. The list endpoint also surfaces the top
# unmapped IFW codes so the admin can decide whether to add a rule.
# Mutations enqueue a tenant-wide recompute via Arq when available; otherwise
# they recompute synchronously so dev/test setups without Redis still work.
# ---------------------------------------------------------------------------


_RULE_FIELDS_EDITABLE = {
    "description": str,
    "kind": str,
    "trigger_label": str,
    "user_note": str,
    "authority": str,
    "extendable": bool,
    "active": bool,
    "priority_tier": (str, type(None)),
    "ssp_months": (int, type(None)),
    "max_months": (int, type(None)),
    "due_months_from_grant": (int, type(None)),
    "grace_months_from_grant": (int, type(None)),
    "from_filing_months": (int, type(None)),
    "from_priority_months": (int, type(None)),
    "base_months_from_priority": (int, type(None)),
    "late_months_from_priority": (int, type(None)),
}

# M0009: ``variant_key`` is part of the unique key
# ``(tenant_id, code, variant_key)`` so it is **creation-only** — the admin
# update path silently ignores it. The two ``close_*_codes`` arrays are
# editable like ``aliases``: passed in as JSON arrays and persisted as text
# arrays. They live in their own list (not ``_RULE_FIELDS_EDITABLE``) so the
# array branch in ``admin_update_rule`` knows to coerce them as lists.
_RULE_FIELDS_ARRAY_EDITABLE = {
    "aliases",
    "warnings",
    "patent_type_applicability",
    "close_complete_codes",
    "close_nar_codes",
}


# M14: fields whose value is part of the rule "shape" — used to compute the
# tenant-vs-global diff. Excludes ``id``, ``tenant_id``, ``code``, ``aliases``,
# ``patent_type_applicability``, and ``updated_at`` because those don't
# meaningfully differ on a tenant override.
_RULE_DIFFABLE_FIELDS = (
    "description",
    "kind",
    "ssp_months",
    "max_months",
    "due_months_from_grant",
    "grace_months_from_grant",
    "from_filing_months",
    "from_priority_months",
    "base_months_from_priority",
    "late_months_from_priority",
    "extendable",
    "trigger_label",
    "user_note",
    "authority",
    "warnings",
    "priority_tier",
    "active",
    # M0009: variant_key + close arrays participate in the diff so an admin
    # can see at a glance when a tenant override has tuned its docket
    # cross-off rules differently from the global seed.
    "variant_key",
    "close_complete_codes",
    "close_nar_codes",
)


def _diff_fields(tenant_dict: dict[str, Any], global_dict: dict[str, Any]) -> list[str]:
    """Return the list of diffable fields whose tenant value != global value."""
    out: list[str] = []
    for f in _RULE_DIFFABLE_FIELDS:
        if tenant_dict.get(f) != global_dict.get(f):
            out.append(f)
    return out


def _attach_global_parent(
    db: Session, payload: dict[str, Any], row: IfwRule, tenant: str
) -> None:
    """Mutate ``payload`` in place to add ``global_parent`` + ``diff_fields``
    when a tenant-scoped row has a corresponding global by ``code``.

    No-op for rows that already are global or for tenants without a global
    counterpart (e.g. tenant invented its own code).
    """
    if row.tenant_id == "global" or tenant == "global":
        return
    parent = db.scalar(
        select(IfwRule).where(IfwRule.tenant_id == "global", IfwRule.code == row.code)
    )
    if parent is None:
        payload["global_parent"] = None
        payload["diff_fields"] = []
        return
    parent_dict = _rule_to_dict(parent)
    payload["global_parent"] = parent_dict
    payload["diff_fields"] = _diff_fields(payload, parent_dict)


def _rule_to_dict(row: IfwRule) -> dict[str, Any]:
    return {
        "id": row.id,
        "tenant_id": row.tenant_id,
        "code": row.code,
        "variant_key": getattr(row, "variant_key", "") or "",
        "close_complete_codes": list(
            getattr(row, "close_complete_codes", None) or []
        ),
        "close_nar_codes": list(getattr(row, "close_nar_codes", None) or []),
        "description": row.description,
        "kind": row.kind,
        "aliases": list(row.aliases or []),
        "ssp_months": row.ssp_months,
        "max_months": row.max_months,
        "due_months_from_grant": row.due_months_from_grant,
        "grace_months_from_grant": row.grace_months_from_grant,
        "from_filing_months": row.from_filing_months,
        "from_priority_months": row.from_priority_months,
        "base_months_from_priority": row.base_months_from_priority,
        "late_months_from_priority": row.late_months_from_priority,
        "extendable": bool(row.extendable),
        "trigger_label": row.trigger_label,
        "user_note": row.user_note or "",
        "authority": row.authority,
        "warnings": list(row.warnings or []),
        "priority_tier": row.priority_tier,
        "patent_type_applicability": list(row.patent_type_applicability or []),
        "active": bool(row.active),
        "updated_at": _iso(row.updated_at),
    }


@router.get("/admin/rules")
def admin_list_rules(
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    """List effective rules for the caller's tenant + top unmapped codes.

    Tenant overrides shadow globals; the response flags `is_override=True`
    on rows whose `tenant_id` matches the caller's tenant. The unmapped
    panel surfaces codes seen during materialization that have no rule —
    these are the most useful targets for new rules.
    """
    from harness_analytics.models import UnmappedIfwCode
    from harness_analytics.timeline.rules_repo import list_rules

    tenant = user.tenant_id or "global"
    rows = list_rules(db, tenant_id=tenant)

    rules_payload: list[dict[str, Any]] = []
    for r in rows:
        d = _rule_to_dict(r)
        d["is_override"] = r.tenant_id == tenant and tenant != "global"
        _attach_global_parent(db, d, r, tenant)
        rules_payload.append(d)

    unmapped_q = (
        db.query(UnmappedIfwCode)
        .filter(UnmappedIfwCode.tenant_id.in_([tenant, "global"]))
        .order_by(UnmappedIfwCode.count.desc(), UnmappedIfwCode.code.asc())
        .limit(50)
    )
    unmapped = [
        {
            "code": u.code,
            "count": int(u.count or 0),
            "first_seen": _iso(u.first_seen),
            "last_seen": _iso(u.last_seen),
        }
        for u in unmapped_q.all()
    ]

    return JSONResponse(
        {
            "tenant_id": tenant,
            "rules": rules_payload,
            "unmapped_codes": unmapped,
        }
    )


@router.get("/admin/rules/{rule_id}")
def admin_get_rule(
    rule_id: int,
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    row = db.get(IfwRule, rule_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Rule not found")
    tenant = user.tenant_id or "global"
    if row.tenant_id not in (tenant, "global"):
        raise HTTPException(status_code=403, detail="Rule belongs to another tenant")
    out = _rule_to_dict(row)
    out["is_override"] = row.tenant_id == tenant and tenant != "global"
    _attach_global_parent(db, out, row, tenant)
    return JSONResponse(out)


def _snapshot_rule_version(
    db: Session, target: IfwRule, edited_by_user_id: Optional[int]
) -> int:
    """Insert a new ``ifw_rule_versions`` row capturing ``target``'s current state.

    Should be called *before* applying any patch. Returns the version number
    that was just written. Versions auto-increment per rule_id starting at 1.
    """
    current_max = (
        db.scalar(
            select(func.coalesce(func.max(IfwRuleVersion.version), 0)).where(
                IfwRuleVersion.rule_id == target.id
            )
        )
        or 0
    )
    version = int(current_max) + 1
    snapshot = _rule_to_dict(target)
    # Drop volatile / non-restorable fields so a revert applies cleanly.
    for k in ("id", "tenant_id", "code", "updated_at"):
        snapshot.pop(k, None)
    db.add(
        IfwRuleVersion(
            rule_id=target.id,
            version=version,
            snapshot_json=snapshot,
            edited_by_user_id=edited_by_user_id,
        )
    )
    return version


def _coerce_field(name: str, value: Any) -> Any:
    """Coerce JSON input to the column type. Empty string → None for nullable
    numeric/text fields. Raises 400 on type mismatch."""
    types = _RULE_FIELDS_EDITABLE.get(name)
    if types is None:
        raise HTTPException(status_code=400, detail=f"Field {name!r} is not editable")
    if isinstance(types, tuple):
        # Nullable column.
        if value is None or value == "":
            return None
        primary = types[0]
        try:
            return primary(value) if primary is not str else str(value)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail=f"Field {name!r} expects {primary.__name__}"
            )
    if types is bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in {"1", "true", "yes", "on"}
        return bool(value)
    if types is int:
        try:
            return int(value)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail=f"Field {name!r} expects int")
    return str(value) if value is not None else ""


@router.put("/admin/rules/{rule_id}")
def admin_update_rule(
    rule_id: int,
    payload: dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    """Update editable fields on a rule. If the rule is a `global` row and the
    caller belongs to a non-global tenant, we *clone* it as a tenant override
    so changes don't leak across tenants. Either way we enqueue a tenant-wide
    recompute so the dashboard reflects the new rule within minutes.
    """
    from harness_analytics.timeline.materializer import recompute_for_tenant

    row = db.get(IfwRule, rule_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Rule not found")
    tenant = user.tenant_id or "global"
    if row.tenant_id not in (tenant, "global"):
        raise HTTPException(status_code=403, detail="Rule belongs to another tenant")

    # If admin is editing a global row from a non-global tenant, branch off
    # an override so the global stays canonical for everyone else.
    if row.tenant_id == "global" and tenant != "global":
        existing_override = db.scalar(
            select(IfwRule).where(IfwRule.tenant_id == tenant, IfwRule.code == row.code)
        )
        if existing_override is None:
            override = IfwRule(
                tenant_id=tenant,
                code=row.code,
                description=row.description,
                kind=row.kind,
                aliases=list(row.aliases or []),
                ssp_months=row.ssp_months,
                max_months=row.max_months,
                due_months_from_grant=row.due_months_from_grant,
                grace_months_from_grant=row.grace_months_from_grant,
                from_filing_months=row.from_filing_months,
                from_priority_months=row.from_priority_months,
                base_months_from_priority=row.base_months_from_priority,
                late_months_from_priority=row.late_months_from_priority,
                extendable=row.extendable,
                trigger_label=row.trigger_label,
                user_note=row.user_note or "",
                authority=row.authority,
                warnings=list(row.warnings or []),
                priority_tier=row.priority_tier,
                patent_type_applicability=list(row.patent_type_applicability or []),
                active=row.active,
            )
            db.add(override)
            db.flush()
            target = override
        else:
            target = existing_override
    else:
        target = row

    # M15: snapshot the pre-edit state for the audit trail. Best-effort so
    # a deployment without the ifw_rule_versions table doesn't break edits.
    try:
        _snapshot_rule_version(db, target, edited_by_user_id=user.id)
    except Exception:  # noqa: BLE001
        logger.exception("Could not snapshot ifw_rule_versions; continuing with edit")

    for k, v in payload.items():
        if k in _RULE_FIELDS_ARRAY_EDITABLE:
            if not isinstance(v, list):
                raise HTTPException(status_code=400, detail=f"{k} must be a list")
            setattr(target, k, [str(x) for x in v])
        elif k in _RULE_FIELDS_EDITABLE:
            setattr(target, k, _coerce_field(k, v))
        # `variant_key` is part of the unique key — it cannot be patched.
        # Silently ignore unknown keys (forward-compat).

    target.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(target)

    # Enqueue a tenant-wide recompute. Best-effort: if Redis isn't configured
    # we fall through to a synchronous call so dev/test still works.
    enqueue_status = "queued"
    try:
        import asyncio

        from harness_analytics.jobs.queue import enqueue

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # FastAPI is sync here; schedule on a fresh loop.
                raise RuntimeError("nested loop")
        except RuntimeError:
            asyncio.run(enqueue("timeline_recompute_all", tenant))
        else:
            asyncio.run(enqueue("timeline_recompute_all", tenant))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not enqueue tenant recompute, falling back: %s", exc)
        try:
            recompute_for_tenant(db, tenant)
            enqueue_status = "synchronous"
        except Exception:  # noqa: BLE001
            logger.exception("Synchronous recompute fallback also failed")
            enqueue_status = "deferred"

    out = _rule_to_dict(target)
    out["is_override"] = target.tenant_id == tenant and tenant != "global"
    _attach_global_parent(db, out, target, tenant)
    out["recompute"] = enqueue_status
    return JSONResponse(out)


# ---------------------------------------------------------------------------
# M15: ifw_rule_versions — per-rule edit history + revert
# ---------------------------------------------------------------------------


_VERSION_LIST_LIMIT = 50

# Mirror of _RULE_FIELDS_EDITABLE but expressed as a flat set so revert can
# walk the snapshot dict without re-deriving types from coerce.
_REVERTABLE_FIELDS = set(_RULE_FIELDS_EDITABLE.keys()) | _RULE_FIELDS_ARRAY_EDITABLE


def _version_to_dict(row: IfwRuleVersion) -> dict[str, Any]:
    snap = row.snapshot_json or {}
    return {
        "id": row.id,
        "rule_id": row.rule_id,
        "version": row.version,
        "edited_at": _iso(row.edited_at),
        "edited_by_user_id": row.edited_by_user_id,
        "snapshot": snap,
        # Cheap front-of-list summary so the UI can render "ssp_months: 3 → ?"
        # without computing a full diff client-side.
        "summary_fields": [k for k in _RULE_DIFFABLE_FIELDS if k in snap],
    }


@router.get("/admin/rules/{rule_id}/versions")
def admin_list_rule_versions(
    rule_id: int,
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    row = db.get(IfwRule, rule_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Rule not found")
    tenant = user.tenant_id or "global"
    if row.tenant_id not in (tenant, "global"):
        raise HTTPException(status_code=403, detail="Rule belongs to another tenant")
    versions = db.scalars(
        select(IfwRuleVersion)
        .where(IfwRuleVersion.rule_id == rule_id)
        .order_by(IfwRuleVersion.version.desc())
        .limit(_VERSION_LIST_LIMIT)
    ).all()
    return JSONResponse(
        {
            "rule_id": rule_id,
            "versions": [_version_to_dict(v) for v in versions],
        }
    )


@router.post("/admin/rules/{rule_id}/revert/{version}")
def admin_revert_rule_version(
    rule_id: int,
    version: int,
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    """Restore the editable fields of a rule from a saved snapshot.

    Reverting writes a new history row first (so the act of reverting is
    itself audited) and then enqueues the standard tenant recompute. The
    snapshot's ``id``/``tenant_id``/``code`` are intentionally never copied
    back, since those fields aren't editable.
    """
    from harness_analytics.timeline.materializer import recompute_for_tenant

    target = db.get(IfwRule, rule_id)
    if target is None:
        raise HTTPException(status_code=404, detail="Rule not found")
    tenant = user.tenant_id or "global"
    if target.tenant_id not in (tenant, "global"):
        raise HTTPException(status_code=403, detail="Rule belongs to another tenant")
    snapshot_row = db.scalar(
        select(IfwRuleVersion).where(
            IfwRuleVersion.rule_id == rule_id, IfwRuleVersion.version == version
        )
    )
    if snapshot_row is None:
        raise HTTPException(status_code=404, detail="Version not found")
    snapshot = snapshot_row.snapshot_json or {}

    try:
        _snapshot_rule_version(db, target, edited_by_user_id=user.id)
    except Exception:  # noqa: BLE001
        logger.exception("Could not snapshot pre-revert version; continuing")

    for k, v in snapshot.items():
        if k not in _REVERTABLE_FIELDS:
            continue
        if k in _RULE_FIELDS_ARRAY_EDITABLE:
            setattr(target, k, [str(x) for x in (v or [])])
        else:
            try:
                setattr(target, k, _coerce_field(k, v))
            except HTTPException:
                # An older snapshot might carry a value the current schema
                # can't accept; fall back to writing the raw value so we
                # don't drop history fields silently.
                setattr(target, k, v)

    target.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(target)

    enqueue_status = "queued"
    try:
        import asyncio

        from harness_analytics.jobs.queue import enqueue

        asyncio.run(enqueue("timeline_recompute_all", tenant))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not enqueue tenant recompute on revert: %s", exc)
        try:
            recompute_for_tenant(db, tenant)
            enqueue_status = "synchronous"
        except Exception:  # noqa: BLE001
            logger.exception("Synchronous recompute fallback failed on revert")
            enqueue_status = "deferred"

    out = _rule_to_dict(target)
    out["is_override"] = target.tenant_id == tenant and tenant != "global"
    _attach_global_parent(db, out, target, tenant)
    out["recompute"] = enqueue_status
    out["reverted_to_version"] = version
    return JSONResponse(out)


# ---------------------------------------------------------------------------
# M13: supersession-map admin
# ---------------------------------------------------------------------------


def _supersession_to_dict(row: "SupersessionMap", tenant: str) -> dict[str, Any]:
    return {
        "id": row.id,
        "prev_kind": row.prev_kind,
        "new_kind": row.new_kind,
        "tenant_id": row.tenant_id,
        "is_override": row.tenant_id == tenant and tenant != "global",
        "is_global": row.tenant_id == "global",
    }


@router.get("/admin/supersession")
def admin_list_supersession(
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    """Return the merged supersession-map view for the caller's tenant.

    Globals are returned alongside any tenant-specific overrides; the UI
    distinguishes them via ``is_global`` / ``is_override`` and only allows
    delete on tenant rows.
    """
    from harness_analytics.models import SupersessionMap

    tenant = user.tenant_id or "global"
    tenants = ["global"] if tenant == "global" else ["global", tenant]
    rows = db.scalars(
        select(SupersessionMap)
        .where(SupersessionMap.tenant_id.in_(tenants))
        .order_by(SupersessionMap.prev_kind.asc(), SupersessionMap.new_kind.asc())
    ).all()
    return JSONResponse(
        {
            "tenant_id": tenant,
            "pairs": [_supersession_to_dict(r, tenant) for r in rows],
        }
    )


@router.post("/admin/supersession")
def admin_create_supersession(
    payload: dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    """Create a tenant-scoped supersession pair.

    Globals are seeded by the rules-seed task and are intentionally not
    writable through this endpoint — admins clone them by re-creating the
    same pair under their tenant, which then takes precedence. Returns 409
    if the same pair already exists for this tenant.
    """
    from harness_analytics.models import SupersessionMap

    tenant = user.tenant_id or "global"
    if tenant == "global":
        raise HTTPException(
            status_code=403,
            detail="Global supersession pairs are managed via the seed file",
        )
    prev_kind = (payload.get("prev_kind") or "").strip()
    new_kind = (payload.get("new_kind") or "").strip()
    if not prev_kind or not new_kind:
        raise HTTPException(
            status_code=400, detail="prev_kind and new_kind are required"
        )
    existing = db.scalar(
        select(SupersessionMap).where(
            SupersessionMap.tenant_id == tenant,
            SupersessionMap.prev_kind == prev_kind,
            SupersessionMap.new_kind == new_kind,
        )
    )
    if existing is not None:
        raise HTTPException(
            status_code=409,
            detail="Supersession pair already exists for this tenant",
        )
    row = SupersessionMap(
        tenant_id=tenant, prev_kind=prev_kind, new_kind=new_kind
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return JSONResponse(_supersession_to_dict(row, tenant))


@router.delete("/admin/supersession/{pair_id}")
def admin_delete_supersession(
    pair_id: int,
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    from harness_analytics.models import SupersessionMap

    tenant = user.tenant_id or "global"
    row = db.get(SupersessionMap, pair_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Pair not found")
    if row.tenant_id == "global":
        raise HTTPException(
            status_code=403,
            detail="Global supersession pairs cannot be deleted via the API",
        )
    if row.tenant_id != tenant:
        raise HTTPException(status_code=403, detail="Pair belongs to another tenant")
    db.delete(row)
    db.commit()
    return JSONResponse({"deleted": pair_id})


# ---------------------------------------------------------------------------
# M9: ICS feed (per-user, token-authenticated) + token management
# ---------------------------------------------------------------------------


@router.get("/me/ics-token")
def my_ics_token(
    request: Request,
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(current_user),
) -> JSONResponse:
    """Return the current user's ICS feed URL, generating the token on
    first call. Idempotent."""
    from harness_analytics.timeline.ics import issue_or_reuse_token

    db_user = db.get(User, user.id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    token = issue_or_reuse_token(db, db_user)
    base = str(request.base_url).rstrip("/")
    feed_url = f"{base}/portal/ics/{db_user.id}.ics?token={token}"
    return JSONResponse({"user_id": db_user.id, "token": token, "url": feed_url})


@router.post("/me/ics-token/rotate")
def rotate_my_ics_token(
    request: Request,
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(current_user),
) -> JSONResponse:
    """Rotate the caller's ICS token. Old subscriptions immediately stop working."""
    from harness_analytics.timeline.ics import rotate_token

    db_user = db.get(User, user.id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    token = rotate_token(db, db_user)
    base = str(request.base_url).rstrip("/")
    feed_url = f"{base}/portal/ics/{db_user.id}.ics?token={token}"
    return JSONResponse({"user_id": db_user.id, "token": token, "url": feed_url})


# ---------------------------------------------------------------------------
# M11: who-am-I (used by the inbox UI to decide whether to show the Team
# chip) + ADMIN/OWNER user-management endpoints for setting manager_user_id
# ---------------------------------------------------------------------------


@router.get("/me")
def whoami(user: CurrentUser = Depends(current_user)) -> JSONResponse:
    """Return the caller's basic identity + role.

    Lightweight enough that the inbox UI can call it on every load without
    affecting page paint; the response intentionally avoids any DB lookups
    beyond the session check.
    """
    return JSONResponse(
        {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "role": user.role,
            "tenant_id": user.tenant_id,
        }
    )


def _user_summary(u: User) -> dict[str, Any]:
    return {
        "id": u.id,
        "email": u.email,
        "name": u.name,
        "role": u.role,
        "active": bool(u.active),
        "manager_user_id": u.manager_user_id,
    }


@router.get("/admin/users")
def admin_list_users(
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    """List users in the caller's tenant for the manager-assignment UI."""
    rows = db.scalars(
        select(User).where(User.tenant_id == user.tenant_id).order_by(User.email.asc())
    ).all()
    return JSONResponse(
        {
            "tenant_id": user.tenant_id,
            "users": [_user_summary(u) for u in rows],
        }
    )


# ---------------------------------------------------------------------------
# M12: saved views (per-user named filter snapshots)
# ---------------------------------------------------------------------------


_VALID_SAVED_VIEW_SURFACES = {"inbox", "portfolio"}
_SAVED_VIEW_NAME_MAX = 80


def _saved_view_to_dict(row: "SavedView") -> dict[str, Any]:
    return {
        "id": row.id,
        "surface": row.surface,
        "name": row.name,
        "params": row.params_json or {},
        "is_default": bool(row.is_default),
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
    }


@router.get("/me/views")
def list_saved_views(
    surface: str = Query("inbox"),
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(current_user),
) -> JSONResponse:
    from harness_analytics.models import SavedView

    if surface not in _VALID_SAVED_VIEW_SURFACES:
        raise HTTPException(
            status_code=400,
            detail=f"surface must be one of {sorted(_VALID_SAVED_VIEW_SURFACES)}",
        )
    rows = db.scalars(
        select(SavedView)
        .where(SavedView.user_id == user.id, SavedView.surface == surface)
        .order_by(SavedView.is_default.desc(), SavedView.name.asc())
    ).all()
    return JSONResponse(
        {
            "surface": surface,
            "views": [_saved_view_to_dict(r) for r in rows],
        }
    )


@router.post("/me/views")
def upsert_saved_view(
    payload: dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(current_user),
) -> JSONResponse:
    """Create or replace by ``(user_id, surface, name)``.

    Body: ``{"surface": "inbox", "name": "My week", "params": {...},
    "is_default": false}``. Names are unique per surface per user; saving
    with an existing name overwrites the params (intentional — keeps the
    UI affordance simple).
    """
    from harness_analytics.models import SavedView

    surface = (payload.get("surface") or "inbox").strip()
    if surface not in _VALID_SAVED_VIEW_SURFACES:
        raise HTTPException(
            status_code=400,
            detail=f"surface must be one of {sorted(_VALID_SAVED_VIEW_SURFACES)}",
        )
    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    if len(name) > _SAVED_VIEW_NAME_MAX:
        raise HTTPException(
            status_code=400,
            detail=f"name must be {_SAVED_VIEW_NAME_MAX} characters or less",
        )
    params = payload.get("params") or {}
    if not isinstance(params, dict):
        raise HTTPException(status_code=400, detail="params must be a JSON object")
    is_default = bool(payload.get("is_default") or False)

    existing = db.scalar(
        select(SavedView).where(
            SavedView.user_id == user.id,
            SavedView.surface == surface,
            SavedView.name == name,
        )
    )
    if existing is None:
        existing = SavedView(
            user_id=user.id,
            surface=surface,
            name=name,
            params_json=params,
            is_default=is_default,
        )
        db.add(existing)
    else:
        existing.params_json = params
        existing.is_default = is_default
    if is_default:
        # Clear default from sibling rows so only one stays sticky.
        db.flush()
        db.execute(
            select(SavedView).where(
                SavedView.user_id == user.id,
                SavedView.surface == surface,
                SavedView.id != existing.id,
                SavedView.is_default.is_(True),
            )
        )
        for sibling in db.scalars(
            select(SavedView).where(
                SavedView.user_id == user.id,
                SavedView.surface == surface,
                SavedView.id != existing.id,
                SavedView.is_default.is_(True),
            )
        ).all():
            sibling.is_default = False
    db.commit()
    db.refresh(existing)
    return JSONResponse(_saved_view_to_dict(existing))


@router.delete("/me/views/{view_id}")
def delete_saved_view(
    view_id: int,
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(current_user),
) -> JSONResponse:
    from harness_analytics.models import SavedView

    row = db.get(SavedView, view_id)
    if row is None or row.user_id != user.id:
        raise HTTPException(status_code=404, detail="View not found")
    db.delete(row)
    db.commit()
    return JSONResponse({"deleted": view_id})


@router.post("/me/views/{view_id}/default")
def set_default_saved_view(
    view_id: int,
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(current_user),
) -> JSONResponse:
    from harness_analytics.models import SavedView

    row = db.get(SavedView, view_id)
    if row is None or row.user_id != user.id:
        raise HTTPException(status_code=404, detail="View not found")
    siblings = db.scalars(
        select(SavedView).where(
            SavedView.user_id == user.id,
            SavedView.surface == row.surface,
            SavedView.is_default.is_(True),
            SavedView.id != row.id,
        )
    ).all()
    for s in siblings:
        s.is_default = False
    row.is_default = True
    db.commit()
    db.refresh(row)
    return JSONResponse(_saved_view_to_dict(row))


@router.put("/admin/users/{user_id}/manager")
def admin_set_user_manager(
    user_id: int,
    payload: dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    """Set or clear ``users.manager_user_id`` on a user in the same tenant.

    Body: ``{"manager_user_id": <int|null>}``. Defends against tenant leakage
    (both target and proposed manager must share the caller's tenant) and
    against trivial cycles (you cannot be your own manager).
    """
    target = db.get(User, user_id)
    if target is None or target.tenant_id != user.tenant_id:
        raise HTTPException(status_code=404, detail="User not found")
    raw = payload.get("manager_user_id")
    new_manager_id: Optional[int]
    if raw is None or raw == "":
        new_manager_id = None
    else:
        try:
            new_manager_id = int(raw)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail="manager_user_id must be an integer or null"
            )
        if new_manager_id == target.id:
            raise HTTPException(
                status_code=400, detail="A user cannot manage themselves"
            )
        manager = db.get(User, new_manager_id)
        if manager is None or manager.tenant_id != user.tenant_id:
            raise HTTPException(status_code=404, detail="Manager not found")
    target.manager_user_id = new_manager_id
    db.commit()
    db.refresh(target)
    return JSONResponse(_user_summary(target))


# ---------------------------------------------------------------------------
# M0009 follow-up: ADMIN-only endpoint that fires a tenant-wide timeline
# recompute on demand. Same machinery the lifespan uses for the optional
# ``BACKFILL_TIMELINE_ON_START`` boot job — just exposed via HTTP so the
# operator can kick a one-shot backfill without redeploying with a
# different env var. Idempotent via the existing lockfile, so concurrent
# clicks no-op rather than stacking.
# ---------------------------------------------------------------------------


@router.post("/admin/timeline/backfill")
def admin_trigger_timeline_backfill(
    user: CurrentUser = Depends(require_role("ADMIN")),
) -> JSONResponse:
    """Spawn the detached ``timeline-recompute`` subprocess for the caller's
    tenant. Returns immediately; tail progress at ``/health/backfill``.

    409 if a recompute is already in flight (lockfile pointed at a live
    PID). 200 otherwise with the spawned PID + start time.
    """
    from harness_analytics.server import _spawn_timeline_recompute

    spawned = _spawn_timeline_recompute(reason=f"admin-trigger:user={user.id}")
    if spawned is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "A recompute is already running. Tail /health/backfill for "
                "progress; retry once that run completes."
            ),
        )
    return JSONResponse(
        {
            "status": "spawned",
            "pid": spawned.get("pid"),
            "tenant": spawned.get("tenant"),
            "started_at": spawned.get("started_at"),
            "reason": spawned.get("reason"),
            "log": "/health/backfill",
        }
    )
