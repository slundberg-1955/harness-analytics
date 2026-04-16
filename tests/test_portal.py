"""Portal auth: session login + HTTP Basic; no DB required for most cases."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from starlette.testclient import TestClient


def test_portal_requires_auth_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTAL_PASSWORD", "only-for-test")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.server import create_app

    client = TestClient(create_app())
    r = client.get("/portal/")
    assert r.status_code == 401
    assert "WWW-Authenticate" in r.headers


def test_portal_redirects_html_to_login(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTAL_PASSWORD", "only-for-test")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.server import create_app

    client = TestClient(create_app(), follow_redirects=False)
    r = client.get("/portal/", headers={"Accept": "text/html"})
    assert r.status_code == 303
    assert r.headers.get("location", "").rstrip("/").endswith("/portal/login")


def test_portal_503_when_password_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PORTAL_PASSWORD", raising=False)
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.server import create_app

    client = TestClient(create_app())
    r = client.get("/portal/", auth=("viewer", "x"))
    assert r.status_code == 503


def test_portal_rejects_wrong_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTAL_PASSWORD", "correct")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.server import create_app

    client = TestClient(create_app())
    r = client.get("/portal/", auth=("viewer", "wrong"))
    assert r.status_code == 401


def test_portal_ok_with_basic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTAL_PASSWORD", "correct")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.server import create_app

    client = TestClient(create_app())
    r = client.get("/portal/", auth=("viewer", "correct"))
    assert r.status_code == 200
    assert "Portal" in r.text


def test_portal_session_login_form(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTAL_PASSWORD", "correct")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.server import create_app

    client = TestClient(create_app(), follow_redirects=False)
    r = client.post(
        "/portal/login",
        data={"username": "viewer", "password": "correct"},
    )
    assert r.status_code == 303
    assert r.headers.get("location", "").endswith("/portal/")
    r2 = client.get("/portal/")
    assert r2.status_code == 200
    assert "Portal" in r2.text


def test_portal_login_shows_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTAL_PASSWORD", "correct")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.server import create_app

    client = TestClient(create_app())
    r = client.get("/portal/login?invalid=1")
    assert r.status_code == 200
    assert "Invalid" in r.text


def test_health_unauthenticated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PORTAL_PASSWORD", raising=False)
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.server import create_app

    client = TestClient(create_app())
    r = client.get("/health")
    assert r.status_code == 200
