"""Real users + roles + DB-backed sessions for the portal.

Coexists with the legacy ``PORTAL_PASSWORD`` shared-secret path until at least
one ``OWNER`` user exists. After the first OWNER is created, the legacy path
remains available **only** as Basic-auth credentials, so any deployed scripts
keep working until they migrate.

Roles (precedence high to low): OWNER > ADMIN > ATTORNEY > PARALEGAL > VIEWER.

Sessions are random opaque tokens stored in ``user_sessions``. Cookie name is
``hp_session``; the cookie itself is ``HttpOnly`` and ``SameSite=Lax`` (and
``Secure`` on Railway). The starlette ``SessionMiddleware`` is no longer used
for auth state — it remains in the stack only to support legacy
``request.session`` flag reads during the rollout.
"""

from __future__ import annotations

import logging
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, Request
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.orm import Session

from harness_analytics.db import get_db
from harness_analytics.models import User, UserSession

logger = logging.getLogger(__name__)

# argon2 is the default and what we want; bcrypt is a fallback for environments
# without an argon2 system lib.
_pwd_context = CryptContext(schemes=["argon2", "bcrypt"], deprecated="auto")

ROLES = ("OWNER", "ADMIN", "ATTORNEY", "PARALEGAL", "VIEWER")
_ROLE_RANK: dict[str, int] = {r: i for i, r in enumerate(reversed(ROLES))}

SESSION_COOKIE = "hp_session"
SESSION_TTL = timedelta(days=14)


def hash_password(plain: str) -> str:
    return _pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return _pwd_context.verify(plain, hashed)
    except Exception:  # malformed hash, etc.
        return False


def role_at_least(actual: str, required: str) -> bool:
    return _ROLE_RANK.get(actual, -1) >= _ROLE_RANK.get(required, 99)


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CurrentUser:
    id: int
    email: str
    name: Optional[str]
    role: str
    tenant_id: str


def _new_session_id() -> str:
    return secrets.token_urlsafe(32)


def issue_session(
    db: Session,
    user: User,
    *,
    user_agent: Optional[str] = None,
    ip: Optional[str] = None,
) -> str:
    sid = _new_session_id()
    db.add(
        UserSession(
            id=sid,
            user_id=user.id,
            expires_at=datetime.now(timezone.utc) + SESSION_TTL,
            user_agent=(user_agent or "")[:512] or None,
            ip=(ip or "")[:64] or None,
        )
    )
    user.last_login_at = datetime.now(timezone.utc)
    db.commit()
    return sid


def revoke_session(db: Session, session_id: str) -> None:
    sess = db.get(UserSession, session_id)
    if sess is not None:
        db.delete(sess)
        db.commit()


def lookup_session(db: Session, session_id: str) -> Optional[CurrentUser]:
    sess = db.get(UserSession, session_id)
    if sess is None:
        return None
    if sess.expires_at < datetime.now(timezone.utc):
        return None
    user = db.get(User, sess.user_id)
    if user is None or not user.active:
        return None
    return CurrentUser(
        id=user.id,
        email=user.email,
        name=user.name,
        role=user.role,
        tenant_id=user.tenant_id,
    )


# ---------------------------------------------------------------------------
# CRUD helpers used by CLI bootstrap + admin UI
# ---------------------------------------------------------------------------


def find_user_by_email(db: Session, email: str) -> Optional[User]:
    return db.scalar(select(User).where(User.email == email.lower().strip()))


def authenticate(db: Session, email: str, password: str) -> Optional[User]:
    user = find_user_by_email(db, email)
    if user is None or not user.active:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


def create_user(
    db: Session,
    *,
    email: str,
    password: str,
    name: Optional[str] = None,
    role: str = "VIEWER",
    tenant_id: str = "global",
) -> User:
    role = role.upper()
    if role not in ROLES:
        raise ValueError(f"Unknown role {role!r}; expected one of {ROLES}")
    if find_user_by_email(db, email) is not None:
        raise ValueError(f"User {email!r} already exists")
    user = User(
        email=email.lower().strip(),
        name=name,
        password_hash=hash_password(password),
        role=role,
        tenant_id=tenant_id,
        active=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def has_any_owner(db: Session) -> bool:
    return db.scalar(select(User.id).where(User.role == "OWNER").limit(1)) is not None


def bootstrap_owner_from_env(db: Session) -> None:
    """If no users exist yet, create one OWNER from env so the portal isn't locked.

    Reads ``PORTAL_BOOTSTRAP_EMAIL``/``PORTAL_BOOTSTRAP_PASSWORD`` first, then
    falls back to ``PORTAL_USER`` + ``PORTAL_PASSWORD`` (the legacy shared
    creds) so existing deployments transition without operator action.
    """
    if has_any_owner(db):
        return
    email = (
        os.environ.get("PORTAL_BOOTSTRAP_EMAIL")
        or os.environ.get("PORTAL_USER")
    )
    password = (
        os.environ.get("PORTAL_BOOTSTRAP_PASSWORD")
        or os.environ.get("PORTAL_PASSWORD")
    )
    if not email or not password:
        return
    if "@" not in email:
        # PORTAL_USER was historically just "viewer"; synthesize an email.
        email = f"{email}@harness.local"
    try:
        create_user(
            db,
            email=email,
            password=password,
            name="Bootstrap owner",
            role="OWNER",
            tenant_id="global",
        )
        logger.info("Bootstrapped OWNER user %s from environment", email)
    except ValueError:
        # Race: another worker created the user; that's fine.
        pass


# ---------------------------------------------------------------------------
# FastAPI dependencies
# ---------------------------------------------------------------------------


def _read_session_id(request: Request) -> Optional[str]:
    return request.cookies.get(SESSION_COOKIE)


def current_user_optional(
    request: Request, db: Session = Depends(get_db)
) -> Optional[CurrentUser]:
    sid = _read_session_id(request)
    if not sid:
        return None
    return lookup_session(db, sid)


def current_user(
    request: Request, db: Session = Depends(get_db)
) -> CurrentUser:
    cu = current_user_optional(request, db)
    if cu is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return cu


def require_role(min_role: str):
    if min_role not in ROLES:
        raise ValueError(f"Unknown role {min_role!r}")

    def _dep(user: CurrentUser = Depends(current_user)) -> CurrentUser:
        if not role_at_least(user.role, min_role):
            raise HTTPException(
                status_code=403,
                detail=f"Requires role {min_role} or higher",
            )
        return user

    return _dep
