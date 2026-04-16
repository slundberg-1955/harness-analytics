"""Lightweight idempotent DDL for deployments without Alembic."""

from __future__ import annotations

import logging

from sqlalchemy import create_engine, inspect, text

from harness_analytics.db import get_database_url

logger = logging.getLogger(__name__)


def ensure_application_analytics_schema() -> None:
    """
    Add analytics columns introduced after initial deploy.

    SQLAlchemy create_all does not ALTER existing tables, so Railway/production
    databases need this once per new column.
    """
    url = get_database_url()
    if not url:
        return

    engine = create_engine(url, pool_pre_ping=True)
    try:
        insp = inspect(engine)
        if not insp.has_table("application_analytics"):
            return
        col_names = {c["name"] for c in insp.get_columns("application_analytics")}
        if "ifw_a_ne_count" in col_names:
            return
        with engine.begin() as conn:
            conn.execute(
                text(
                    "ALTER TABLE application_analytics "
                    "ADD COLUMN ifw_a_ne_count INTEGER NOT NULL DEFAULT 0"
                )
            )
        logger.info("Added column application_analytics.ifw_a_ne_count")
    finally:
        engine.dispose()
