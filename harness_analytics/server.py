"""Minimal HTTP service for Railway (health + optional DB check + portal)."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text

from harness_analytics.portal import install_portal_security, router as portal_router
from harness_analytics.portfolio_api import router as portfolio_api_router
from harness_analytics.schema_migrations import ensure_schema_migrations


def _normalize_db_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    ensure_schema_migrations()
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="harness-analytics", version="0.1.0", lifespan=_lifespan)
    app.include_router(portal_router)
    app.include_router(portfolio_api_router)

    static_dir = Path(__file__).resolve().parent / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

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
