"""Lightweight idempotent DDL for deployments without Alembic."""

from __future__ import annotations

import logging

from sqlalchemy import create_engine, inspect, text

from harness_analytics.db import get_database_url

logger = logging.getLogger(__name__)


def _drop_column_if_exists(engine, table: str, column: str) -> None:
    insp = inspect(engine)
    if not insp.has_table(table):
        return
    col_names = {c["name"] for c in insp.get_columns(table)}
    if column not in col_names:
        return
    stmt = text(f"ALTER TABLE {table} DROP COLUMN {column}")
    with engine.begin() as conn:
        conn.execute(stmt)
    logger.info("Dropped column %s.%s", table, column)


def _add_column_if_missing(engine, table: str, column: str, ddl_suffix: str) -> None:
    insp = inspect(engine)
    if not insp.has_table(table):
        return
    col_names = {c["name"] for c in insp.get_columns(table)}
    if column in col_names:
        return
    stmt = text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_suffix}")
    with engine.begin() as conn:
        conn.execute(stmt)
    logger.info("Added column %s.%s", table, column)


def ensure_schema_migrations() -> None:
    """
    Add columns introduced after initial deploy (create_all does not ALTER tables).

    Safe to run on every process startup.
    """
    url = get_database_url()
    if not url:
        return

    engine = create_engine(url, pool_pre_ping=True)
    try:
        _drop_column_if_exists(engine, "application_analytics", "billing_attorney_reg")
        _drop_column_if_exists(engine, "application_analytics", "billing_attorney_name")
        _add_column_if_missing(
            engine,
            "application_analytics",
            "ifw_a_ne_count",
            "INTEGER NOT NULL DEFAULT 0",
        )
        _add_column_if_missing(
            engine,
            "application_analytics",
            "ifw_ctrs_count",
            "INTEGER NOT NULL DEFAULT 0",
        )
        _add_column_if_missing(
            engine,
            "applications",
            "continuity_child_of_prior_us",
            "BOOLEAN NOT NULL DEFAULT false",
        )
        for col in (
            "oa_ext_1mo_count",
            "oa_ext_2mo_count",
            "oa_ext_3mo_count",
            "oa_ext_gt_90d_count",
            "ctrs_ext_1mo_count",
            "ctrs_ext_2mo_count",
            "ctrs_ext_3mo_count",
            "ctrs_ext_gt_90d_count",
        ):
            _add_column_if_missing(
                engine,
                "application_analytics",
                col,
                "INTEGER NOT NULL DEFAULT 0",
            )
    finally:
        engine.dispose()


# Backwards-compatible name for imports
ensure_application_analytics_schema = ensure_schema_migrations
