"""Minimal HTTP service for Railway (health + optional DB check + portal)."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text

from harness_analytics.auth import bootstrap_owner_from_env
from harness_analytics.db import get_database_url, get_session_factory
from harness_analytics.portal import install_portal_security, router as portal_router
from harness_analytics.portfolio_api import router as portfolio_api_router
from harness_analytics.schema_migrations import ensure_schema_migrations
from harness_analytics.timeline_api import (
    ics_router as timeline_ics_router,
    router as timeline_api_router,
)


def _normalize_db_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    ensure_schema_migrations()
    if get_database_url():
        try:
            SessionLocal = get_session_factory()
            with SessionLocal() as db:
                bootstrap_owner_from_env(db)
            # Seed global IFW rules (idempotent). Best-effort.
            try:
                from harness_analytics.timeline.rules_repo import seed_global_rules

                with SessionLocal() as db:
                    seed_global_rules(db, tenant_id="global")
            except Exception:  # noqa: BLE001
                pass
        except Exception:  # noqa: BLE001
            pass

        # Optional one-shot timeline backfill on container start. Set
        # ``BACKFILL_TIMELINE_ON_START=1`` in the environment to fire a
        # detached subprocess that recomputes deadlines for every
        # application in the global tenant. The subprocess writes a
        # ``/tmp/harness_timeline_backfill.lock`` file containing its PID
        # so that subsequent uvicorn workers (and re-deploys) skip
        # spawning a duplicate while one is already running.
        try:
            _maybe_spawn_timeline_backfill()
        except Exception:  # noqa: BLE001
            pass
    yield


_BACKFILL_LOCK_PATH = "/tmp/harness_timeline_backfill.lock"
_BACKFILL_LOG_PATH = "/tmp/harness_timeline_backfill.log"


def _maybe_spawn_timeline_backfill() -> None:
    flag = (os.environ.get("BACKFILL_TIMELINE_ON_START") or "").strip().lower()
    if flag not in {"1", "true", "yes", "on"}:
        return

    import subprocess
    import sys

    # Already-running guard: if the lockfile points at a live PID, do not
    # spawn another. PID liveness is checked via /proc on Linux; on
    # platforms without /proc we err on the side of not spawning.
    if Path(_BACKFILL_LOCK_PATH).exists():
        try:
            pid_text = Path(_BACKFILL_LOCK_PATH).read_text().strip().splitlines()[0]
            pid = int(pid_text)
            if Path(f"/proc/{pid}").exists():
                return
        except Exception:  # noqa: BLE001
            pass

    tenant_id = (os.environ.get("BACKFILL_TIMELINE_TENANT") or "global").strip() or "global"
    log = open(_BACKFILL_LOG_PATH, "ab", buffering=0)
    proc = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-m",
            "harness_analytics",
            "timeline-recompute",
            "--tenant-id",
            tenant_id,
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        Path(_BACKFILL_LOCK_PATH).write_text(
            f"{proc.pid}\ntenant={tenant_id}\nstarted_at={os.popen('date -u +%FT%TZ').read().strip()}\n"
        )
    except Exception:  # noqa: BLE001
        pass


def create_app() -> FastAPI:
    app = FastAPI(title="harness-analytics", version="0.1.0", lifespan=_lifespan)
    app.include_router(portal_router)
    app.include_router(portfolio_api_router)
    app.include_router(timeline_api_router)
    app.include_router(timeline_ics_router)

    static_dir = Path(__file__).resolve().parent / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    favicon_path = static_dir / "favicon.svg"

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Response:
        # Serve the SVG for clients that auto-request /favicon.ico instead of
        # honoring the <link rel="icon"> in the page head. Modern browsers
        # accept SVG faviocns regardless of the .ico extension.
        if favicon_path.is_file():
            return FileResponse(favicon_path, media_type="image/svg+xml")
        return Response(status_code=404)

    install_portal_security(app)

    @app.get("/")
    def root() -> dict[str, str]:
        return {
            "service": "harness-analytics",
            "health": "/health",
            "db_health": "/health/db",
            "portal": "/portal/",
        }

    @app.get("/health")
    def health() -> dict[str, str]:
        """Railway / load balancer liveness (always 200 if process is up)."""
        return {"status": "ok"}

    @app.get("/health/backfill")
    def health_backfill() -> JSONResponse:
        """Lightweight status for the optional ``BACKFILL_TIMELINE_ON_START``
        worker. Returns whether a recompute is in flight, plus a tail of its
        log so an operator can monitor progress without SSH.
        """
        out: dict[str, object] = {"running": False}
        try:
            if Path(_BACKFILL_LOCK_PATH).exists():
                lock_text = Path(_BACKFILL_LOCK_PATH).read_text()
                out["lock"] = lock_text
                try:
                    pid = int(lock_text.splitlines()[0])
                    out["pid"] = pid
                    out["running"] = Path(f"/proc/{pid}").exists()
                except Exception:  # noqa: BLE001
                    pass
            if Path(_BACKFILL_LOG_PATH).exists():
                with open(_BACKFILL_LOG_PATH, "rb") as fh:
                    fh.seek(0, 2)
                    end = fh.tell()
                    fh.seek(max(0, end - 4096))
                    tail = fh.read().decode("utf-8", errors="replace")
                out["log_tail"] = tail
                out["log_size"] = end
        except Exception as exc:  # noqa: BLE001
            out["error"] = str(exc)
        return JSONResponse(out)

    @app.get("/health/db")
    def health_db() -> JSONResponse:
        """Verify DATABASE_URL connectivity (optional diagnostic)."""
        raw = os.environ.get("DATABASE_URL")
        if not raw:
            return JSONResponse({"database": "not_configured"}, status_code=503)
        url = _normalize_db_url(raw)
        try:
            engine = create_engine(url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return JSONResponse({"database": "ok"})
        except Exception as exc:  # noqa: BLE001
            return JSONResponse({"database": "error", "detail": str(exc)}, status_code=503)

    return app


app = create_app()
