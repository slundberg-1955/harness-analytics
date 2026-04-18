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


# View exposing the spec-flattened `patent_applications` shape on top of
# `applications` + `application_analytics`. Read-only; populated by joins.
_PATENT_APPLICATIONS_VIEW_SQL = """
CREATE OR REPLACE VIEW patent_applications AS
SELECT
    a.application_number,
    a.invention_title,
    a.filing_date,
    a.issue_date,
    a.issue_year,
    CASE
        WHEN a.application_status_code ~ '^[0-9]+$'
        THEN a.application_status_code::int
        ELSE NULL
    END AS application_status_code,
    a.application_status_text,
    a.patent_number,
    a.customer_number,
    a.hdp_customer_number,
    a.group_art_unit,
    a.patent_class,
    TRIM(
        COALESCE(a.examiner_first_name, '') || ' ' ||
        COALESCE(a.examiner_last_name, '')
    ) AS examiner_name,
    a.examiner_first_name,
    a.examiner_last_name,
    a.assignee_name,
    a.continuity_child_of_prior_us AS is_continuation,
    COALESCE(aa.ifw_ctrs_count, 0)       AS has_restriction_ctrs_count,
    COALESCE(aa.ifw_a_ne_count, 0)       AS ifw_a_ne_count,
    COALESCE(aa.nonfinal_oa_count, 0)    AS nonfinal_oa_count,
    COALESCE(aa.final_oa_count, 0)       AS final_oa_count,
    COALESCE(aa.total_substantive_oas, 0) AS total_substantive_oas,
    aa.first_noa_date,
    COALESCE(aa.had_examiner_interview, FALSE) AS had_examiner_interview,
    COALESCE(aa.interview_count, 0)      AS interview_count,
    COALESCE(aa.interview_led_to_noa, FALSE) AS noa_within_90_days_of_interview,
    aa.days_interview_to_noa             AS days_last_interview_to_noa,
    COALESCE(aa.rce_count, 0)            AS rce_count,
    aa.days_filing_to_first_oa,
    aa.days_filing_to_noa,
    aa.days_filing_to_issue,
    COALESCE(aa.is_jac, FALSE)           AS is_jac,
    aa.office_name,
    a.imported_at,
    GREATEST(a.imported_at, aa.updated_at) AS updated_at
FROM applications a
LEFT JOIN application_analytics aa ON aa.application_id = a.id
"""


_PORTFOLIO_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_pa_status ON applications (application_status_code)",
    "CREATE INDEX IF NOT EXISTS idx_pa_issue_year ON applications (issue_year)",
    "CREATE INDEX IF NOT EXISTS idx_pa_art_unit ON applications (group_art_unit)",
    "CREATE INDEX IF NOT EXISTS idx_pa_examiner_last ON applications (examiner_last_name)",
    "CREATE INDEX IF NOT EXISTS idx_pa_assignee ON applications (assignee_name)",
    "CREATE INDEX IF NOT EXISTS idx_pa_filing_date ON applications (filing_date)",
]


_APP_SETTINGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def _ensure_app_settings_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_APP_SETTINGS_TABLE_SQL))


def _ensure_patent_applications_view(engine) -> None:
    insp = inspect(engine)
    if not insp.has_table("applications") or not insp.has_table("application_analytics"):
        return
    with engine.begin() as conn:
        conn.execute(text(_PATENT_APPLICATIONS_VIEW_SQL))
        for stmt in _PORTFOLIO_INDEX_SQL:
            conn.execute(text(stmt))
    logger.info("Ensured patent_applications view and portfolio indexes")


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
        for legacy in (
            "oa_ext_1mo_count",
            "oa_ext_2mo_count",
            "oa_ext_3mo_count",
            "oa_ext_gt_90d_count",
            "ctrs_ext_1mo_count",
            "ctrs_ext_2mo_count",
            "ctrs_ext_3mo_count",
            "ctrs_ext_gt_90d_count",
        ):
            _drop_column_if_exists(engine, "application_analytics", legacy)
        for col in (
            "ctnf_ext_1mo_count",
            "ctnf_ext_2mo_count",
            "ctnf_ext_3mo_count",
            "ctfr_ext_1mo_count",
            "ctfr_ext_2mo_count",
            "ctfr_ext_3mo_count",
            "ctrs_ext_1mo_count",
            "ctrs_ext_2mo_count",
            "ctrs_ext_3mo_count",
        ):
            _add_column_if_missing(
                engine,
                "application_analytics",
                col,
                "INTEGER NOT NULL DEFAULT 0",
            )
        _ensure_app_settings_table(engine)
        _ensure_patent_applications_view(engine)
    finally:
        engine.dispose()


# Backwards-compatible name for imports
ensure_application_analytics_schema = ensure_schema_migrations
