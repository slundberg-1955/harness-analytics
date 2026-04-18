"""/portal/settings page + DB-backed app_settings precedence over env / default."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient


def _patch_inmemory_settings(monkeypatch: pytest.MonkeyPatch) -> dict[str, str | None]:
    """Replace app_settings.{get,set}_setting with an in-process dict for tests."""
    from harness_analytics import app_settings as mod

    store: dict[str, str | None] = {}

    def fake_get(key: str) -> str | None:
        return store.get(key)

    def fake_set(key: str, value: str | None) -> None:
        if value is None:
            store.pop(key, None)
        else:
            store[key] = value

    monkeypatch.setattr(mod, "get_setting", fake_get)
    monkeypatch.setattr(mod, "set_setting", fake_set)
    monkeypatch.setattr(mod, "_cache", {})
    return store


def _make_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("PORTAL_PASSWORD", "test-pw")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.server import create_app

    return TestClient(create_app())


def test_settings_page_renders_with_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_inmemory_settings(monkeypatch)
    monkeypatch.delenv("PORTFOLIO_AGG_ROW_CAP", raising=False)
    client = _make_client(monkeypatch)
    r = client.get("/portal/settings", auth=("viewer", "test-pw"))
    assert r.status_code == 200
    body = r.text
    assert "Aggregate row cap" in body
    assert "5,000" in body  # default formatted in effective row


def test_settings_post_persists_and_takes_effect(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _patch_inmemory_settings(monkeypatch)
    client = _make_client(monkeypatch)

    r = client.post(
        "/portal/settings/portfolio-cap",
        data={"value": "1234"},
        auth=("viewer", "test-pw"),
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert "saved=1" in r.headers["location"]
    assert store["portfolio.aggregateRowCap"] == "1234"

    from harness_analytics.portfolio_api import _aggregate_row_cap

    assert _aggregate_row_cap() == 1234


def test_settings_db_value_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _patch_inmemory_settings(monkeypatch)
    monkeypatch.setenv("PORTFOLIO_AGG_ROW_CAP", "9999")
    store["portfolio.aggregateRowCap"] = "42"

    from harness_analytics.portfolio_api import _aggregate_row_cap

    assert _aggregate_row_cap() == 42


def test_settings_post_blank_clears_override(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _patch_inmemory_settings(monkeypatch)
    store["portfolio.aggregateRowCap"] = "777"
    client = _make_client(monkeypatch)

    r = client.post(
        "/portal/settings/portfolio-cap",
        data={"value": "  "},
        auth=("viewer", "test-pw"),
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert "saved=1" in r.headers["location"]
    assert "portfolio.aggregateRowCap" not in store


def test_settings_post_invalid_value_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_inmemory_settings(monkeypatch)
    client = _make_client(monkeypatch)

    r = client.post(
        "/portal/settings/portfolio-cap",
        data={"value": "abc"},
        auth=("viewer", "test-pw"),
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert "error=invalid" in r.headers["location"]


def test_settings_db_zero_disables_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _patch_inmemory_settings(monkeypatch)
    store["portfolio.aggregateRowCap"] = "0"

    from harness_analytics.portfolio_api import _aggregate_row_cap

    assert _aggregate_row_cap() == 0
