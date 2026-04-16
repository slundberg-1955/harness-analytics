#!/usr/bin/env python3
"""Resolve Railway Postgres DATABASE_PUBLIC_URL and run harness_analytics analytics."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _public_db_url() -> str:
    raw = subprocess.check_output(
        ["railway", "variable", "list", "-s", "Postgres", "--json"],
        cwd=REPO,
        text=True,
    )
    data = json.loads(raw)
    url = data.get("DATABASE_PUBLIC_URL") or data.get("DATABASE_URL")
    if not url:
        raise SystemExit("No DATABASE_PUBLIC_URL or DATABASE_URL on Postgres service")
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def main() -> None:
    url = _public_db_url()
    cmd = [
        sys.executable,
        "-m",
        "harness_analytics",
        "analytics",
        "--db-url",
        url,
        *sys.argv[1:],
    ]
    raise SystemExit(subprocess.call(cmd, cwd=REPO))


if __name__ == "__main__":
    main()
