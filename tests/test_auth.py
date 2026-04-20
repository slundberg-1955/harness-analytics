"""Unit tests for the new user/role/session auth layer (M0.2)."""
from __future__ import annotations

import pytest

from harness_analytics import auth


def test_password_round_trip() -> None:
    h = auth.hash_password("hunter2")
    assert h != "hunter2"
    assert auth.verify_password("hunter2", h) is True
    assert auth.verify_password("wrong", h) is False


def test_role_at_least_ordering() -> None:
    assert auth.role_at_least("OWNER", "VIEWER") is True
    assert auth.role_at_least("OWNER", "ADMIN") is True
    assert auth.role_at_least("ADMIN", "OWNER") is False
    assert auth.role_at_least("ATTORNEY", "PARALEGAL") is True
    assert auth.role_at_least("VIEWER", "PARALEGAL") is False


def test_role_at_least_rejects_unknown() -> None:
    # Unknown roles don't accidentally satisfy a check.
    assert auth.role_at_least("MARTIAN", "VIEWER") is False
