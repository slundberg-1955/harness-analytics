"""Minimal HTTP service for Railway (health + optional DB check + portal)."""

from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text

from harness_analytics._debug_agent_log import agent_log
from harness_analytics.portal import install_portal_security, router as portal_router


def _normalize_db_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def create_app() -> FastAPI:
    # #region agent log
    agent_log(
        "server.py:create_app",
        "enter",
        data={
            "port_env": os.environ.get("PORT"),
            "railway_env": os.environ.get("RAILWAY_ENVIRONMENT"),
        },
        hypothesis_id="H1_PORT",
    )
    # #endregion
    app = FastAPI(title="harness-analytics", version="0.1.0")
    # #region agent log
    agent_log("server.py:create_app", "after_FastAPI_init", hypothesis_id="H3_BOOT")
    # #endregion
    app.include_router(portal_router)
    # #region agent log
    agent_log("server.py:create_app", "after_include_portal_router", hypothesis_id="H3_BOOT")
    # #endregion
    install_portal_security(app)
    # #region agent log
    agent_log("server.py:create_app", "after_install_portal_security", hypothesis_id="H3_BOOT")
    # #endregion

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
        # #region agent log
        agent_log("server.py:health", "handler_invoked", hypothesis_id="H4_HEALTH")
        # #endregion
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

    # #region agent log
    agent_log("server.py:create_app", "before_return_app", hypothesis_id="H3_BOOT")
    # #endregion
    return app


# #region agent log
agent_log("server.py:module", "before_module_level_create_app", hypothesis_id="H3_BOOT")
# #endregion
app = create_app()
# #region agent log
agent_log("server.py:module", "after_module_level_create_app", data={"ok": True}, hypothesis_id="H3_BOOT")
# #endregion
