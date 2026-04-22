"""Per-user ICS feed generator (Milestone 9).

The user gets a stable URL ``/portal/ics/{user_id}.ics?token=<opaque>`` they
can subscribe from Outlook/Gmail/Apple Calendar. The token is stored on
``users.ics_token`` (rotatable from Settings); we don't sign with HMAC because
we want the user to be able to revoke a leaked URL by issuing a new token,
which is simpler than rotating a server-side HMAC key.

The generator does not attempt to be a complete RFC 5545 implementation —
it produces VEVENTs for every open computed deadline assigned (or visible) to
the user, with an all-day event on ``primary_date`` and a 24-hour VALARM.
"""

from __future__ import annotations

import secrets
from datetime import date, datetime, timezone
from typing import Iterable, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from harness_analytics.models import (
    Application,
    ComputedDeadline,
    User,
    VerifiedDeadline,
)


PRODID = "-//Harness Analytics//Prosecution Timeline//EN"


def issue_or_reuse_token(db: Session, user: User) -> str:
    """Return the user's ICS token, generating a fresh one on first use.

    Tokens are 32 url-safe bytes (256 bits of entropy). Calling this is
    idempotent — pass the same User and you get the same token back.
    """
    if user.ics_token:
        return user.ics_token
    token = secrets.token_urlsafe(32)
    user.ics_token = token
    db.add(user)
    db.commit()
    db.refresh(user)
    return user.ics_token


def rotate_token(db: Session, user: User) -> str:
    user.ics_token = secrets.token_urlsafe(32)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user.ics_token


def find_user_by_token(db: Session, user_id: int, token: str) -> Optional[User]:
    if not token:
        return None
    user = db.get(User, user_id)
    if user is None or not user.active or not user.ics_token:
        return None
    # Constant-time compare so a brute force can't see length differences.
    if not secrets.compare_digest(user.ics_token, token):
        return None
    return user


# ---------------------------------------------------------------------------
# ICS rendering
# ---------------------------------------------------------------------------


def _esc(text: str) -> str:
    """Escape a TEXT value per RFC 5545 §3.3.11."""
    return (
        (text or "")
        .replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\n", "\\n")
        .replace("\r", "")
    )


def _fold(line: str) -> str:
    """Fold a content line at 75 octets per RFC 5545 §3.1."""
    if len(line) <= 75:
        return line
    out = [line[:75]]
    rest = line[75:]
    while rest:
        out.append(" " + rest[:74])
        rest = rest[74:]
    return "\r\n".join(out)


def _utc_stamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _date_only(d: date) -> str:
    return d.strftime("%Y%m%d")


def _vevent_for(
    cd: ComputedDeadline,
    *,
    app: Optional[Application],
    rule_label: Optional[str],
    verified: bool,
    base_url: str,
    now: datetime,
) -> list[str]:
    summary_parts: list[str] = []
    if cd.severity == "danger":
        summary_parts.append("\U0001f534")  # red circle
    elif cd.severity == "warn":
        summary_parts.append("\u26a0\ufe0f")  # warning sign
    if app and app.application_number:
        summary_parts.append(f"[{app.application_number}]")
    summary_parts.append(rule_label or cd.primary_label or "Deadline")
    if verified:
        summary_parts.append("\u2713")
    summary = " ".join(summary_parts)

    description_lines: list[str] = []
    if app:
        if app.application_number:
            description_lines.append(f"Application: {app.application_number}")
        if app.invention_title:
            description_lines.append(f"Title: {app.invention_title}")
    if cd.notes:
        description_lines.append("Notes: " + cd.notes)
    if cd.statutory_bar_date and cd.statutory_bar_date != cd.primary_date:
        description_lines.append(
            f"Statutory bar: {cd.statutory_bar_date.isoformat()}"
        )
    if verified:
        description_lines.append("Verified by attorney.")
    description = "\\n".join(_esc(line) for line in description_lines)

    url = f"{base_url}/portal/matter/{app.application_number}#prosecution-timeline-card" if app and app.application_number else f"{base_url}/portal/actions"

    primary = cd.primary_date
    next_day = date.fromordinal(primary.toordinal() + 1)
    uid = f"deadline-{cd.id}@harness-analytics"

    lines = [
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{_utc_stamp(now)}",
        f"DTSTART;VALUE=DATE:{_date_only(primary)}",
        f"DTEND;VALUE=DATE:{_date_only(next_day)}",
        f"SUMMARY:{_esc(summary)}",
    ]
    if description:
        lines.append(f"DESCRIPTION:{description}")
    lines.append(f"URL:{_esc(url)}")
    lines.append(f"CATEGORIES:{_esc(cd.severity or 'info')}")
    lines.append(f"STATUS:{'CONFIRMED' if verified else 'TENTATIVE'}")
    lines.extend([
        "BEGIN:VALARM",
        "ACTION:DISPLAY",
        f"DESCRIPTION:{_esc(summary)}",
        "TRIGGER:-P1D",
        "END:VALARM",
        "END:VEVENT",
    ])
    return lines


def render_user_feed(
    db: Session,
    user: User,
    *,
    base_url: str,
    rules_label_lookup,
) -> str:
    """Render an ICS calendar containing every open deadline visible to ``user``.

    Visibility rule (kept conservative for the v1 feed): everything in the
    user's tenant that's still ``OPEN``. Filtering by assigned_user_id would
    miss attorneys who run docket reviews for their whole team, which is the
    expected use case for v1.
    """
    now = datetime.now(timezone.utc)
    tenant = user.tenant_id or "global"
    deadlines = (
        db.scalars(
            select(ComputedDeadline)
            .where(ComputedDeadline.tenant_id == tenant)
            .where(ComputedDeadline.status == "OPEN")
            .order_by(ComputedDeadline.primary_date.asc())
        )
        .all()
    )

    # Pre-load apps + verified flags.
    app_ids = {cd.application_id for cd in deadlines}
    apps = {a.id: a for a in db.scalars(
        select(Application).where(Application.id.in_(app_ids))
    ).all()} if app_ids else {}
    deadline_ids = [cd.id for cd in deadlines]
    verified_ids = set(
        db.scalars(
            select(VerifiedDeadline.deadline_id).where(
                VerifiedDeadline.deadline_id.in_(deadline_ids)
            )
        ).all()
    ) if deadline_ids else set()

    body: list[str] = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        f"PRODID:{PRODID}",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:Harness Deadlines ({_esc(user.email)})",
        "X-WR-TIMEZONE:UTC",
    ]
    for cd in deadlines:
        body.extend(
            _vevent_for(
                cd,
                app=apps.get(cd.application_id),
                rule_label=rules_label_lookup(cd.rule_id),
                verified=cd.id in verified_ids,
                base_url=base_url,
                now=now,
            )
        )
    body.append("END:VCALENDAR")
    return "\r\n".join(_fold(line) for line in body) + "\r\n"
