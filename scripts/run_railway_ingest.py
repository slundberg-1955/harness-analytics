#!/usr/bin/env python3
"""Resolve Railway Postgres DATABASE_PUBLIC_URL and run harness_analytics ingest (no secrets printed)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
FOLDER = "/Users/stevelundberg/Development Ancillary Files/biblioxmls copy"


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
        "ingest",
        "--db-url",
        url,
        "--folder",
        FOLDER,
        "--commit-every",
        "100",
        "--skip-analytics",
        "--error-log",
        str(REPO / "ingest_railway_errors.jsonl"),
        *sys.argv[1:],
    ]
    raise SystemExit(subprocess.call(cmd, cwd=REPO))


if __name__ == "__main__":
    main()
