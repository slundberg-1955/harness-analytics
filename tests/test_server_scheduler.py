"""Unit coverage for the in-process daily timeline-recompute scheduler.

The scheduler is intentionally tiny — an asyncio loop that wakes up at the
configured UTC hour and re-uses the same subprocess + lockfile machinery as
the one-shot backfill. Tests here pin down the only piece with non-trivial
logic: computing the next firing instant from "now".
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from harness_analytics import server


def _utc(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def test_next_recompute_at_today_when_target_is_in_the_future() -> None:
    now = _utc(2026, 4, 24, 3, 15)
    assert server._next_recompute_at(now, hour_utc=6) == _utc(2026, 4, 24, 6)


def test_next_recompute_at_tomorrow_when_target_already_passed() -> None:
    now = _utc(2026, 4, 24, 7, 0)
    assert server._next_recompute_at(now, hour_utc=6) == _utc(2026, 4, 25, 6)


def test_next_recompute_at_tomorrow_when_now_is_exactly_on_the_hour() -> None:
    """If "now" is exactly at the target instant we don't fire immediately —
    we schedule for the next day so a re-deploy at HH:00 doesn't double up
    with the run that just kicked off."""
    now = _utc(2026, 4, 24, 6, 0)
    assert server._next_recompute_at(now, hour_utc=6) == _utc(2026, 4, 25, 6)


@pytest.mark.parametrize("raw,expected", [
    ("", None),
    ("not-an-int", None),
    ("-1", None),
    ("24", None),
    ("0", 0),
    ("6", 6),
    ("23", 23),
    ("  9  ", 9),
])
def test_parse_recompute_hour(monkeypatch: pytest.MonkeyPatch, raw: str, expected: int | None) -> None:
    monkeypatch.setenv("TIMELINE_DAILY_RECOMPUTE_HOUR_UTC", raw)
    assert server._parse_recompute_hour() == expected


def test_parse_recompute_hour_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TIMELINE_DAILY_RECOMPUTE_HOUR_UTC", raising=False)
    assert server._parse_recompute_hour() is None
