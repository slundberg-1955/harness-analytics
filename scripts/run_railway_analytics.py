#!/usr/bin/env python3
"""Run harness_analytics analytics against Railway Postgres.

By default this runs **inside** the Railway app container via ``railway ssh``, so
``DATABASE_URL`` is the internal URL (same region as Postgres). Use ``--local``
to run on this machine with the Postgres plugin's public URL instead.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DEFAULT_SERVICE = os.environ.get("RAILWAY_ANALYTICS_SERVICE", "harness-analytics")
CONTAINER_LOG = "/app/analytics_run.log"


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


def _parse_argv(argv: list[str]) -> tuple[bool, bool, list[str]]:
    """Return (local, detached, analytics_argv)."""
    local = False
    detached = True
    out: list[str] = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--local":
            local = True
            i += 1
            continue
        if a in ("--foreground", "-f"):
            detached = False
            i += 1
            continue
        out.append(a)
        i += 1
    return local, detached, out


def _run_local(analytics_argv: list[str]) -> int:
    url = _public_db_url()
    cmd = [
        sys.executable,
        "-m",
        "harness_analytics",
        "analytics",
        "--db-url",
        url,
        *analytics_argv,
    ]
    return subprocess.call(cmd, cwd=REPO)


def _run_railway(analytics_argv: list[str], *, detached: bool) -> int:
    inner = " ".join(shlex.quote(x) for x in analytics_argv)
    base = f"cd /app && export PYTHONUNBUFFERED=1 && python -m harness_analytics analytics {inner}"
    if detached:
        remote = (
            f"/usr/bin/nohup sh -c {shlex.quote(base)} "
            f"> {shlex.quote(CONTAINER_LOG)} 2>&1 < /dev/null & echo $!"
        )
        ssh = ["railway", "ssh", "-s", DEFAULT_SERVICE, "--", "sh", "-c", remote]
        print(
            f"Detached run on Railway service {DEFAULT_SERVICE!r}; "
            f"pid printed below. Log file in container: {CONTAINER_LOG}\n"
            f"  tail -f: railway ssh -s {DEFAULT_SERVICE} -- "
            f"sh -c {shlex.quote('tail -f ' + CONTAINER_LOG)}",
            file=sys.stderr,
        )
    else:
        ssh = ["railway", "ssh", "-s", DEFAULT_SERVICE, "--", "sh", "-c", base]
    return subprocess.call(ssh, cwd=REPO)


def main() -> None:
    if len(sys.argv) == 2 and sys.argv[1] in ("-h", "--help"):
        print(
            __doc__.strip()
            + "\n\nOptions (this wrapper):\n"
            "  --local        Run analytics on this machine using Postgres DATABASE_PUBLIC_URL.\n"
            "  -f, --foreground  Run over SSH in the foreground (no nohup / log file).\n"
            "\nAny other arguments are passed to: python -m harness_analytics analytics …\n"
            f"Service name: {DEFAULT_SERVICE!r} (override with RAILWAY_ANALYTICS_SERVICE).\n"
            f"Detached log path (in container): {CONTAINER_LOG}\n"
        )
        raise SystemExit(0)
    local, detached, analytics_argv = _parse_argv(sys.argv[1:])
    if local:
        raise SystemExit(_run_local(analytics_argv))
    raise SystemExit(_run_railway(analytics_argv, detached=detached))


if __name__ == "__main__":
    main()
