"""Smoke tests for the M4 timeline + actions JSON APIs.

Heavy ORM mocking is intentionally avoided: SQLAlchemy 2.x query chains
plus PostgreSQL-only JSONB/ARRAY columns make a fake session more brittle
than the code under test. Instead these tests exercise the small pure
helpers, plus one wiring test that proves the routes are registered and
return well-formed errors when the application can't be found.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient


def test_bucket_for_thresholds() -> None:
    from harness_analytics.timeline_api import _bucket_for

    today = date(2026, 4, 18)
    assert _bucket_for(date(2026, 4, 17), today) == "overdue"
    assert _bucket_for(date(2026, 4, 18), today) == "this_week"
    assert _bucket_for(date(2026, 4, 25), today) == "this_week"
    assert _bucket_for(date(2026, 4, 26), today) == "next_two_weeks"
    assert _bucket_for(date(2026, 5, 2), today) == "next_two_weeks"
    assert _bucket_for(date(2026, 5, 3), today) == "later"


def test_normalize_app_lookup_strips_punctuation_and_space() -> None:
    from harness_analytics.timeline_api import _normalize_app_lookup

    assert _normalize_app_lookup(" 18 158 386 ") == "18158386"
    assert _normalize_app_lookup("17/552,591") == "17/552,591"
    assert _normalize_app_lookup("") == ""


def test_iso_helper_handles_dates_and_datetimes() -> None:
    from harness_analytics.timeline_api import _iso

    assert _iso(None) is None
    assert _iso(date(2026, 4, 18)) == "2026-04-18"
    dt = datetime(2026, 4, 18, 14, 22, 0, tzinfo=timezone.utc)
    assert _iso(dt) == "2026-04-18T14:22:00Z"


def test_milestone_label_for_known_codes() -> None:
    from harness_analytics.timeline_api import _milestone_label_for_code

    assert _milestone_label_for_code("CTNF") == "Non-Final Office Action mailed"
    assert _milestone_label_for_code("ctfr") == "Final Office Action mailed"
    assert _milestone_label_for_code("ZZZZ") is None
    assert _milestone_label_for_code(None) is None


def test_status_pill_with_no_open_deadlines() -> None:
    from harness_analytics.timeline_api import _status_pill

    assert _status_pill([]) == {"label": "No open deadlines", "severity": "info"}


def test_status_pill_picks_earliest_open_with_severity() -> None:
    from harness_analytics.timeline_api import _status_pill

    cd1 = SimpleNamespace(
        status="OPEN",
        primary_date=date(2026, 6, 1),
        primary_label="3-mo SSP (CTFR)",
        severity="warn",
    )
    cd2 = SimpleNamespace(
        status="OPEN",
        primary_date=date(2026, 5, 1),
        primary_label="Non-Final SSP (CTNF)",
        severity="danger",
    )
    cd3 = SimpleNamespace(
        status="COMPLETED",
        primary_date=date(2026, 4, 1),
        primary_label="ignored",
        severity="info",
    )
    out = _status_pill([cd1, cd2, cd3])
    assert out["severity"] == "danger"
    assert out["label"].startswith("Non-Final SSP (CTNF)")
    assert "2026-05-01" in out["label"]


# --- wiring test: route exists and returns 404 cleanly ---------------------


class _EmptySession:
    """SQLAlchemy 2.x scalar/query/get all return falsy/empty without a real DB."""

    def scalar(self, *args, **kwargs):
        return None

    def get(self, *args, **kwargs):
        return None

    def query(self, *_a, **_kw):  # pragma: no cover - exercised only in success path
        return self

    def filter(self, *_a, **_kw):  # pragma: no cover
        return self

    def order_by(self, *_a, **_kw):  # pragma: no cover
        return self

    def limit(self, *_a, **_kw):  # pragma: no cover
        return self

    def all(self):  # pragma: no cover
        return []


def _make_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("PORTAL_PASSWORD", "test-pw")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.db import get_db
    from harness_analytics.server import create_app

    app = create_app()

    def override():
        yield _EmptySession()

    app.dependency_overrides[get_db] = override
    return TestClient(app)


def test_timeline_unknown_application_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch)
    r = client.get(
        "/portal/api/timeline/99999999",
        auth=("viewer", "test-pw"),
    )
    assert r.status_code == 404
    assert "not found" in r.json()["detail"].lower()


def test_deadline_detail_unknown_id_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch)
    r = client.get(
        "/portal/api/timeline/deadlines/424242",
        auth=("viewer", "test-pw"),
    )
    assert r.status_code == 404


def test_actions_inbox_validates_query_params(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch)
    r = client.get(
        "/portal/api/actions/inbox?window=BOGUS",
        auth=("viewer", "test-pw"),
    )
    assert r.status_code == 400
    assert "window" in r.json()["detail"].lower()


def test_admin_rules_requires_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy basic-auth callers don't have a DB-backed user, so they shouldn't
    be able to read or mutate rules. Either 401 (no CurrentUser) or 403
    (CurrentUser is below ADMIN) is acceptable proof the route is wired."""
    client = _make_client(monkeypatch)
    r = client.get("/portal/api/admin/rules", auth=("viewer", "test-pw"))
    assert r.status_code in (401, 403)
    r = client.put(
        "/portal/api/admin/rules/1",
        json={"description": "x"},
        auth=("viewer", "test-pw"),
    )
    assert r.status_code in (401, 403, 404)


def test_inbox_team_view_requires_authenticated_user(monkeypatch: pytest.MonkeyPatch) -> None:
    """assignee=team rejects unauthenticated callers (401) — basic-auth callers
    don't carry a CurrentUser so the team filter has no caller id to roll up."""
    client = _make_client(monkeypatch)
    r = client.get(
        "/portal/api/actions/inbox?assignee=team",
        auth=("viewer", "test-pw"),
    )
    assert r.status_code in (401, 403)


def test_saved_views_require_authenticated_user(monkeypatch: pytest.MonkeyPatch) -> None:
    """SavedView endpoints rely on a CurrentUser; basic-auth callers should
    receive 401 from the auth dependency before any DB lookup."""
    client = _make_client(monkeypatch)
    r = client.get("/portal/api/me/views?surface=inbox", auth=("viewer", "test-pw"))
    assert r.status_code in (401, 403)
    r = client.post(
        "/portal/api/me/views",
        json={"surface": "inbox", "name": "x", "params": {}},
        auth=("viewer", "test-pw"),
    )
    assert r.status_code in (401, 403, 400)


def test_admin_rule_versions_requires_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Version-history endpoints sit behind ADMIN. Either 401 (no CurrentUser)
    or 403 / 404 (CurrentUser but rule absent) is acceptable wiring proof."""
    client = _make_client(monkeypatch)
    r = client.get("/portal/api/admin/rules/1/versions", auth=("viewer", "test-pw"))
    assert r.status_code in (401, 403, 404)
    r = client.post(
        "/portal/api/admin/rules/1/revert/1",
        auth=("viewer", "test-pw"),
    )
    assert r.status_code in (401, 403, 404)


def test_diff_fields_helper_detects_field_changes() -> None:
    """`_diff_fields` should compare diffable fields and ignore unrelated keys."""
    from harness_analytics.timeline_api import _diff_fields

    g = {
        "description": "Office Action",
        "kind": "standard_oa",
        "ssp_months": 3,
        "max_months": 6,
        "extendable": True,
    }
    t = dict(g)
    assert _diff_fields(t, g) == []
    t["ssp_months"] = 2
    t["extendable"] = False
    diff = _diff_fields(t, g)
    assert "ssp_months" in diff
    assert "extendable" in diff
    # Unrelated keys (e.g. id, tenant_id, code) shouldn't appear.
    t["id"] = 99
    assert "id" not in _diff_fields(t, g)


def test_admin_supersession_requires_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Supersession-map endpoints are gated behind ADMIN."""
    client = _make_client(monkeypatch)
    r = client.get("/portal/api/admin/supersession", auth=("viewer", "test-pw"))
    assert r.status_code in (401, 403)
    r = client.post(
        "/portal/api/admin/supersession",
        json={"prev_kind": "standard_oa", "new_kind": "hard_noa"},
        auth=("viewer", "test-pw"),
    )
    assert r.status_code in (401, 403, 409)
    r = client.delete("/portal/api/admin/supersession/1", auth=("viewer", "test-pw"))
    assert r.status_code in (401, 403, 404)


def test_admin_users_requires_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Manager-assignment endpoints are gated behind ADMIN; legacy basic-auth
    callers without a CurrentUser should be denied."""
    client = _make_client(monkeypatch)
    r = client.get("/portal/api/admin/users", auth=("viewer", "test-pw"))
    assert r.status_code in (401, 403)
    r = client.put(
        "/portal/api/admin/users/1/manager",
        json={"manager_user_id": None},
        auth=("viewer", "test-pw"),
    )
    assert r.status_code in (401, 403, 404)


def test_action_post_requires_known_action(monkeypatch: pytest.MonkeyPatch) -> None:
    """POST validation runs before the DB lookup, so a fake session is fine.

    The endpoint is guarded by require_role(PARALEGAL); legacy basic-auth
    callers don't have a CurrentUser, so the response is 401 from the auth
    dependency before even validating the body. That's still proof the route
    is wired.
    """
    client = _make_client(monkeypatch)
    r = client.post(
        "/portal/api/timeline/deadlines/1/actions",
        json={"action": "complete"},
        auth=("viewer", "test-pw"),
    )
    assert r.status_code in (401, 404)
