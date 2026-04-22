"""Unit tests for the M9 ICS feed renderer.

We exercise the pure helpers (escape, fold, line generation) without a real
DB session — the SQLAlchemy interactions are covered by the wiring tests in
``tests/test_timeline_api.py``.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

from harness_analytics.timeline import ics


def test_esc_handles_special_chars() -> None:
    assert ics._esc("a, b; c\\d\ne") == "a\\, b\\; c\\\\d\\ne"
    assert ics._esc("") == ""
    assert ics._esc(None) == ""


def test_fold_long_lines_wraps_at_75() -> None:
    text = "X" * 200
    folded = ics._fold(text)
    parts = folded.split("\r\n")
    assert parts[0] == "X" * 75
    # Continuation lines start with a single space and are at most 75 octets
    # (1 leading space + 74 content chars).
    for cont in parts[1:]:
        assert cont.startswith(" ")
        assert len(cont) <= 75


def test_utc_stamp_formats_z() -> None:
    dt = datetime(2026, 4, 18, 14, 22, 30, tzinfo=timezone.utc)
    assert ics._utc_stamp(dt) == "20260418T142230Z"


def test_date_only_no_separators() -> None:
    assert ics._date_only(date(2026, 4, 18)) == "20260418"


def test_vevent_for_minimal_deadline_has_required_fields() -> None:
    cd = SimpleNamespace(
        id=42,
        primary_date=date(2026, 7, 1),
        primary_label="Response due",
        severity="warn",
        statutory_bar_date=date(2026, 9, 1),
        notes=None,
    )
    app = SimpleNamespace(application_number="18158386", invention_title="Foo")
    now = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
    lines = ics._vevent_for(
        cd,
        app=app,
        rule_label="Final OA",
        verified=True,
        base_url="https://harness.example",
        now=now,
    )
    body = "\n".join(lines)
    assert "BEGIN:VEVENT" in body
    assert "END:VEVENT" in body
    assert "UID:deadline-42@harness-analytics" in body
    assert "DTSTART;VALUE=DATE:20260701" in body
    assert "DTEND;VALUE=DATE:20260702" in body
    assert "STATUS:CONFIRMED" in body  # verified
    assert "TRIGGER:-P1D" in body
    assert "[18158386]" in body
    # Statutory bar > primary, so it shows up in description.
    assert "Statutory bar: 2026-09-01" in body


def test_vevent_status_tentative_when_not_verified() -> None:
    cd = SimpleNamespace(
        id=1,
        primary_date=date(2026, 7, 1),
        primary_label="X",
        severity="info",
        statutory_bar_date=None,
        notes=None,
    )
    lines = ics._vevent_for(
        cd, app=None, rule_label=None, verified=False,
        base_url="https://x", now=datetime.now(timezone.utc),
    )
    assert "STATUS:TENTATIVE" in "\n".join(lines)
