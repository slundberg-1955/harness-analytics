"""Lightweight idempotent DDL for deployments without Alembic."""

from __future__ import annotations

import logging
import threading

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
    a.applicant_name,
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
    "CREATE INDEX IF NOT EXISTS idx_pa_applicant ON applications (applicant_name)",
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


# Per-row PL/pgSQL backfill: parses xml_raw with xmlparse(content ...) and
# pulls //Applicants/Applicant[1]/LegalEntityName via xpath. Wrapped in a
# per-row EXCEPTION handler so a single malformed XML cannot abort the run.
# Gated by app_settings key so it only executes on the first deploy that
# introduces the applicant_name column.
_APPLICANT_BACKFILL_FLAG = "schema.applicantNameBackfilled"
_APPLICANT_BACKFILL_SQL = """
DO $$
DECLARE
  r RECORD;
  v_name TEXT;
  v_done INTEGER := 0;
BEGIN
  FOR r IN
    SELECT id, xml_raw
      FROM applications
     WHERE applicant_name IS NULL
       AND xml_raw IS NOT NULL
       AND xml_raw <> ''
       AND position('<Applicant' in xml_raw) > 0
  LOOP
    BEGIN
      SELECT NULLIF(BTRIM((
               xpath('//Applicants/Applicant[1]/LegalEntityName/text()',
                     xmlparse(content r.xml_raw))
             )[1]::text), '')
        INTO v_name;
      IF v_name IS NOT NULL THEN
        UPDATE applications SET applicant_name = v_name WHERE id = r.id;
        v_done := v_done + 1;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;
  RAISE NOTICE 'applicant_name backfill: % rows updated', v_done;
END $$;
"""


def _set_backfill_flag(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO app_settings (key, value, updated_at) "
                "VALUES (:k, '1', now()) "
                "ON CONFLICT (key) DO UPDATE SET value = '1', updated_at = now()"
            ),
            {"k": _APPLICANT_BACKFILL_FLAG},
        )


def _run_applicant_backfill_worker(database_url: str) -> None:
    """Long-running backfill executed in a daemon thread so it does not block boot."""
    engine = create_engine(database_url, pool_pre_ping=True)
    try:
        logger.info("Backfilling applicant_name from xml_raw (background)…")
        # autocommit so the single big DO block doesn't keep an open
        # transaction across the entire run.
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(_APPLICANT_BACKFILL_SQL))
        _set_backfill_flag(engine)
        logger.info("applicant_name backfill complete")
    except Exception:
        logger.exception("applicant_name backfill failed; will retry on next startup")
    finally:
        engine.dispose()


def _backfill_applicant_names_once(engine) -> None:
    """Schedule the one-time backfill in a daemon thread.

    The cheap gate-check (flag set? anything to backfill?) runs synchronously so
    we never spawn a thread on subsequent boots; the heavy work is dispatched
    to a background thread so the FastAPI lifespan returns immediately and
    Railway healthchecks don't time out.
    """
    insp = inspect(engine)
    if not insp.has_table("applications") or not insp.has_table("app_settings"):
        return
    cols = {c["name"] for c in insp.get_columns("applications")}
    if "applicant_name" not in cols or "xml_raw" not in cols:
        return
    with engine.connect() as conn:
        already = conn.execute(
            text("SELECT value FROM app_settings WHERE key = :k"),
            {"k": _APPLICANT_BACKFILL_FLAG},
        ).first()
        if already and (already[0] or "").strip() in ("1", "true", "yes"):
            return
        pending = conn.execute(
            text(
                "SELECT 1 FROM applications "
                "WHERE applicant_name IS NULL AND xml_raw IS NOT NULL "
                "AND xml_raw <> '' LIMIT 1"
            )
        ).first()
    if pending is None:
        # Nothing to do; record the flag so we never re-check.
        _set_backfill_flag(engine)
        return

    database_url = get_database_url()
    if not database_url:
        return
    t = threading.Thread(
        target=_run_applicant_backfill_worker,
        args=(database_url,),
        name="applicant-name-backfill",
        daemon=True,
    )
    t.start()
    logger.info("applicant_name backfill thread started (daemon)")


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
        _add_column_if_missing(
            engine,
            "applications",
            "applicant_name",
            "TEXT",
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
        _backfill_applicant_names_once(engine)
    finally:
        engine.dispose()


# Backwards-compatible name for imports
ensure_application_analytics_schema = ensure_schema_migrations
