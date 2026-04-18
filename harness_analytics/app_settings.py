"""Process-wide app settings persisted to the `app_settings` key/value table.

Reads are cached in memory for ``_TTL_SECONDS`` so high-traffic endpoints (e.g.
``/portal/api/portfolio``) don't pay a DB round trip per request. Writes
invalidate the cache for the affected key.

Returns ``None`` when no value is set OR when the database is unreachable, so
callers can safely fall back to env vars / defaults without try/except.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from harness_analytics.db import get_engine

logger = logging.getLogger(__name__)

_TTL_SECONDS = 15.0
_lock = threading.Lock()
# key -> (cached_at_monotonic, value)
_cache: dict[str, tuple[float, Optional[str]]] = {}


def _cache_get(key: str) -> tuple[bool, Optional[str]]:
    now = time.monotonic()
    with _lock:
        entry = _cache.get(key)
        if entry and (now - entry[0]) < _TTL_SECONDS:
            return True, entry[1]
    return False, None


def _cache_put(key: str, value: Optional[str]) -> None:
    with _lock:
        _cache[key] = (time.monotonic(), value)


def invalidate(key: str | None = None) -> None:
    with _lock:
        if key is None:
            _cache.clear()
        else:
            _cache.pop(key, None)


def get_setting(key: str) -> Optional[str]:
    hit, value = _cache_get(key)
    if hit:
        return value
    try:
        engine = get_engine()
    except RuntimeError:
        # No DATABASE_URL configured; treat as "no setting".
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT value FROM app_settings WHERE key = :k"),
                {"k": key},
            ).first()
    except SQLAlchemyError:
        logger.exception("Failed to read app_setting %s", key)
        return None
    value = row[0] if row else None
    _cache_put(key, value)
    return value


def set_setting(key: str, value: Optional[str]) -> None:
    """Insert / update / delete (when ``value is None``) and invalidate cache."""
    engine = get_engine()
    with engine.begin() as conn:
        if value is None:
            conn.execute(
                text("DELETE FROM app_settings WHERE key = :k"), {"k": key}
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO app_settings (key, value, updated_at)
                    VALUES (:k, :v, now())
                    ON CONFLICT (key)
                    DO UPDATE SET value = EXCLUDED.value, updated_at = now()
                    """
                ),
                {"k": key, "v": value},
            )
    invalidate(key)
