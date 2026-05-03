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
    GREATEST(a.imported_at, aa.updated_at) AS updated_at,
    -- New columns must be appended at the end so that CREATE OR REPLACE VIEW
    -- doesn't try to rename existing positions (Postgres only allows adding
    -- columns at the tail of an existing view).
    a.applicant_name,
    a.has_child_continuation,
    a.earliest_priority_date,
    a.tenant_id,
    -- Timeline summary columns (nullable when computed_deadlines doesn't exist yet
    -- or no deadlines have been computed for this app). Computed via correlated
    -- subqueries so that filter + sort on these columns just works.
    (
        SELECT cd.primary_date
          FROM computed_deadlines cd
         WHERE cd.application_id = a.id
           AND cd.status = 'OPEN'
         ORDER BY cd.primary_date ASC
         LIMIT 1
    ) AS next_deadline_date,
    (
        SELECT cd.primary_label
          FROM computed_deadlines cd
         WHERE cd.application_id = a.id
           AND cd.status = 'OPEN'
         ORDER BY cd.primary_date ASC
         LIMIT 1
    ) AS next_deadline_label,
    (
        SELECT cd.severity
          FROM computed_deadlines cd
         WHERE cd.application_id = a.id
           AND cd.status = 'OPEN'
         ORDER BY cd.primary_date ASC
         LIMIT 1
    ) AS next_deadline_severity,
    COALESCE((
        SELECT count(*) FROM computed_deadlines cd
         WHERE cd.application_id = a.id
           AND cd.status = 'OPEN'
    ), 0) AS open_deadline_count,
    COALESCE((
        SELECT count(*) FROM computed_deadlines cd
         WHERE cd.application_id = a.id
           AND cd.status = 'OPEN'
           AND cd.primary_date < CURRENT_DATE
    ), 0) AS overdue_deadline_count,
    -- Surface the underlying applications.id so downstream APIs can join to
    -- file_wrapper_documents / prosecution_events without an extra round
    -- trip. Appended at the end so CREATE OR REPLACE VIEW is allowed on
    -- existing prod schema. Not exposed in _row_to_json (server-side only).
    a.id AS application_id,
    -- Allowance Analytics v2 fields. APPENDED AT THE TAIL — Postgres
    -- CREATE OR REPLACE VIEW only allows new columns at the end of an
    -- existing view's column list, so any later additions must keep
    -- following this comment, never get inserted in the middle.
    -- Three columns are direct on ``applications`` (backfilled from
    -- xml_raw); the next four are aliases / in-view derivations so
    -- callers can speak the spec vocabulary without storing denormalized
    -- data that could drift.
    a.abandonment_date,
    a.family_root_app_no,
    a.has_foreign_priority,
    -- Prefer the application-level NOA mailed date (XML-derived); fall
    -- back to the analytics-row first_noa_date (event-derived) when the
    -- XML didn't surface an explicit element. This makes the new column
    -- usable today while ``noa_mailed_date`` is still backfilling.
    COALESCE(a.noa_mailed_date, aa.first_noa_date) AS noa_mailed_date,
    COALESCE(aa.final_oa_count, 0) AS final_rejection_count,
    COALESCE(a.issue_date, a.noa_mailed_date, aa.first_noa_date, a.abandonment_date)
        AS disposal_date,
    CASE
        WHEN a.filing_date IS NOT NULL
         AND COALESCE(a.noa_mailed_date, aa.first_noa_date) IS NOT NULL
        THEN ROUND(
            (COALESCE(a.noa_mailed_date, aa.first_noa_date) - a.filing_date)
            / 30.44::numeric, 1)
        ELSE NULL
    END AS months_to_allowance,
    -- Data-quality flag (Allowance Analytics v2): TRUE when this row has a
    -- joined application_analytics row. compute_first_action_allowance uses
    -- it to exclude apps with no analytics-row from the FAA numerator,
    -- because rce_count / final_oa_count COALESCE to 0 in that case and
    -- would otherwise misclassify them as "first-action" allowances. Apps
    -- still count toward the FAA denominator (closed = patented + abandoned)
    -- because application_status_code lives on `applications` and is
    -- populated independent of the analytics job.
    (aa.application_id IS NOT NULL) AS has_analytics_row,
    -- Filings-by-Type chart bucket: provisional / regular / con / div / cip /
    -- design / other. Populated at ingest by parse_biblio_xml's classifier
    -- and backfilled from xml_raw + application_number prefix for legacy
    -- rows. Appended at the tail to satisfy CREATE OR REPLACE VIEW rules.
    a.application_type
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

_USERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'VIEWER',
    tenant_id TEXT NOT NULL DEFAULT 'global',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at TIMESTAMPTZ
)
"""

_USER_SESSIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS user_sessions (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    user_agent TEXT,
    ip TEXT
)
"""

_AUTH_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_users_tenant ON users (tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions (user_id)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_expires ON user_sessions (expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_applications_tenant ON applications (tenant_id)",
]

_IFW_RULES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ifw_rules (
    id SERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'global',
    code TEXT NOT NULL,
    aliases TEXT[],
    description TEXT NOT NULL,
    kind TEXT NOT NULL,
    ssp_months INT,
    max_months INT,
    due_months_from_grant INT,
    grace_months_from_grant INT,
    from_filing_months INT,
    from_priority_months INT,
    base_months_from_priority INT,
    late_months_from_priority INT,
    extendable BOOLEAN NOT NULL DEFAULT FALSE,
    trigger_label TEXT NOT NULL,
    user_note TEXT NOT NULL DEFAULT '',
    authority TEXT NOT NULL,
    warnings TEXT[],
    priority_tier TEXT,
    patent_type_applicability TEXT[] NOT NULL DEFAULT
        ARRAY['UTILITY','DESIGN','PLANT','REISSUE','REEXAM'],
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (tenant_id, code)
)
"""

_UNMAPPED_IFW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS unmapped_ifw_codes (
    id SERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'global',
    code TEXT NOT NULL,
    count INT NOT NULL DEFAULT 0,
    first_seen TIMESTAMPTZ DEFAULT now(),
    last_seen TIMESTAMPTZ DEFAULT now(),
    UNIQUE (tenant_id, code)
)
"""

_IFW_RULES_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_ifw_rules_code_active ON ifw_rules (code) WHERE active = TRUE",
]

_COMPUTED_DEADLINES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS computed_deadlines (
    id BIGSERIAL PRIMARY KEY,
    application_id INT NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    rule_id INT NOT NULL REFERENCES ifw_rules(id),
    trigger_event_id INT REFERENCES prosecution_events(id) ON DELETE SET NULL,
    trigger_document_id INT REFERENCES file_wrapper_documents(id) ON DELETE SET NULL,
    trigger_date DATE NOT NULL,
    trigger_source TEXT NOT NULL,
    ssp_date DATE,
    statutory_bar_date DATE,
    primary_date DATE NOT NULL,
    primary_label TEXT NOT NULL,
    rows_json JSONB NOT NULL,
    window_open_date DATE,
    grace_end_date DATE,
    ids_phases_json JSONB,
    status TEXT NOT NULL DEFAULT 'OPEN',
    completed_event_id INT REFERENCES prosecution_events(id) ON DELETE SET NULL,
    completed_at TIMESTAMPTZ,
    superseded_by BIGINT REFERENCES computed_deadlines(id) ON DELETE SET NULL,
    assigned_user_id INT REFERENCES users(id) ON DELETE SET NULL,
    snoozed_until DATE,
    notes TEXT,
    warnings TEXT[],
    severity TEXT,
    tenant_id TEXT NOT NULL DEFAULT 'global',
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_DEADLINE_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS deadline_events (
    id BIGSERIAL PRIMARY KEY,
    deadline_id BIGINT NOT NULL REFERENCES computed_deadlines(id) ON DELETE CASCADE,
    user_id INT REFERENCES users(id) ON DELETE SET NULL,
    action TEXT NOT NULL,
    payload_json JSONB,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

_SUPERSESSION_MAP_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS supersession_map (
    id SERIAL PRIMARY KEY,
    prev_kind TEXT NOT NULL,
    new_kind TEXT NOT NULL,
    tenant_id TEXT NOT NULL DEFAULT 'global',
    UNIQUE (tenant_id, prev_kind, new_kind)
)
"""

_DEADLINE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_cd_app ON computed_deadlines (application_id)",
    "CREATE INDEX IF NOT EXISTS idx_cd_primary_open ON computed_deadlines (primary_date) WHERE status = 'OPEN'",
    "CREATE INDEX IF NOT EXISTS idx_cd_assigned_open ON computed_deadlines (assigned_user_id, primary_date) WHERE status = 'OPEN'",
    "CREATE INDEX IF NOT EXISTS idx_cd_status ON computed_deadlines (status, primary_date)",
    "CREATE INDEX IF NOT EXISTS idx_cd_tenant_open ON computed_deadlines (tenant_id, primary_date) WHERE status = 'OPEN'",
    "CREATE INDEX IF NOT EXISTS idx_de_deadline ON deadline_events (deadline_id)",
]

# M9: per-deadline verification (attorney spot-check) + per-user ICS feed token.
_VERIFIED_DEADLINES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS verified_deadlines (
    id BIGSERIAL PRIMARY KEY,
    deadline_id BIGINT NOT NULL REFERENCES computed_deadlines(id) ON DELETE CASCADE,
    verified_by_user_id INT REFERENCES users(id) ON DELETE SET NULL,
    verified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    verified_date DATE NOT NULL,
    source TEXT NOT NULL DEFAULT 'manual',
    note TEXT,
    UNIQUE (deadline_id)
)
"""

_VERIFIED_DEADLINES_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_vd_user ON verified_deadlines (verified_by_user_id)",
]

_USERS_ICS_TOKEN_SQL = (
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS ics_token TEXT UNIQUE"
)

# M11: supervising-user pointer for the team-view inbox.
_USERS_MANAGER_SQL = (
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS manager_user_id INT "
    "REFERENCES users(id) ON DELETE SET NULL"
)
_USERS_MANAGER_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_users_manager ON users (manager_user_id)"
)

# M15: edit history for ifw_rules.
_IFW_RULE_VERSIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ifw_rule_versions (
    id BIGSERIAL PRIMARY KEY,
    rule_id INT NOT NULL REFERENCES ifw_rules(id) ON DELETE CASCADE,
    version INT NOT NULL,
    snapshot_json JSONB NOT NULL,
    edited_by_user_id INT REFERENCES users(id) ON DELETE SET NULL,
    edited_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (rule_id, version)
)
"""

_IFW_RULE_VERSIONS_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_ifw_rule_versions_rule "
    "ON ifw_rule_versions (rule_id, version DESC)"
)

# M12: per-user named filter snapshots (inbox today, portfolio later).
_SAVED_VIEWS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS saved_views (
    id BIGSERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    surface TEXT NOT NULL,
    name TEXT NOT NULL,
    params_json JSONB NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, surface, name)
)
"""

_SAVED_VIEWS_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_saved_views_user_surface "
    "ON saved_views (user_id, surface)"
)


def _ensure_app_settings_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_APP_SETTINGS_TABLE_SQL))


def _ensure_auth_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_USERS_TABLE_SQL))
        conn.execute(text(_USER_SESSIONS_TABLE_SQL))
        for stmt in _AUTH_INDEXES_SQL:
            conn.execute(text(stmt))


def _ensure_timeline_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_IFW_RULES_TABLE_SQL))
        conn.execute(text(_UNMAPPED_IFW_TABLE_SQL))
        conn.execute(text(_COMPUTED_DEADLINES_TABLE_SQL))
        conn.execute(text(_DEADLINE_EVENTS_TABLE_SQL))
        conn.execute(text(_SUPERSESSION_MAP_TABLE_SQL))
        conn.execute(text(_VERIFIED_DEADLINES_TABLE_SQL))
        # ics_token may not exist on older deployments — add idempotently.
        try:
            conn.execute(text(_USERS_ICS_TOKEN_SQL))
        except Exception:  # noqa: BLE001
            # `users` may not exist yet on a brand-new install; auth tables
            # are ensured separately and this column is added on next boot.
            pass
        # M11: supervising-user pointer.
        try:
            conn.execute(text(_USERS_MANAGER_SQL))
            conn.execute(text(_USERS_MANAGER_INDEX_SQL))
        except Exception:  # noqa: BLE001
            pass
        # M12: saved views (per-user filter snapshots).
        try:
            conn.execute(text(_SAVED_VIEWS_TABLE_SQL))
            conn.execute(text(_SAVED_VIEWS_INDEX_SQL))
        except Exception:  # noqa: BLE001
            pass
        # M15: edit history for ifw_rules.
        try:
            conn.execute(text(_IFW_RULE_VERSIONS_TABLE_SQL))
            conn.execute(text(_IFW_RULE_VERSIONS_INDEX_SQL))
        except Exception:  # noqa: BLE001
            pass
        for stmt in (
            _IFW_RULES_INDEXES_SQL
            + _DEADLINE_INDEXES_SQL
            + _VERIFIED_DEADLINES_INDEXES_SQL
        ):
            conn.execute(text(stmt))


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


# One-shot backfill for has_child_continuation: parses xml_raw with
# xmlparse(content ...) and inspects //ChildContinuityList/ChildContinuity for
# Continuation, Continuation-in-Part, or Divisional descriptions (CHM strict
# definition). Wrapped in a per-row EXCEPTION handler so a single malformed
# XML cannot abort the whole run. Gated by an app_settings flag so it only
# executes once per environment.
_CHILD_CONT_BACKFILL_FLAG = "schema.hasChildContinuationBackfilled"
_CHILD_CONT_BACKFILL_SQL = """
DO $$
DECLARE
  r RECORD;
  v_flag BOOLEAN;
  v_done INTEGER := 0;
BEGIN
  FOR r IN
    SELECT id, xml_raw
      FROM applications
     WHERE has_child_continuation IS NULL
       AND xml_raw IS NOT NULL
       AND xml_raw <> ''
  LOOP
    BEGIN
      SELECT EXISTS (
        SELECT 1 FROM unnest(xpath(
          '//ChildContinuityList/ChildContinuity[
             ContinuityDescription/text()="Continuation" or
             ContinuityDescription/text()="Continuation in Part" or
             ContinuityDescription/text()="Continuation-in-Part" or
             ContinuityDescription/text()="Continuation In Part" or
             ContinuityDescription/text()="Division" or
             ContinuityDescription/text()="Divisional"
           ]',
          xmlparse(content r.xml_raw)
        )) AS x
      )
      INTO v_flag;
      UPDATE applications SET has_child_continuation = COALESCE(v_flag, FALSE) WHERE id = r.id;
      v_done := v_done + 1;
    EXCEPTION WHEN OTHERS THEN
      -- Mark malformed rows as FALSE so we don't keep re-trying them on every boot.
      BEGIN
        UPDATE applications SET has_child_continuation = FALSE WHERE id = r.id;
      EXCEPTION WHEN OTHERS THEN
        NULL;
      END;
    END;
  END LOOP;
  RAISE NOTICE 'has_child_continuation backfill: % rows updated', v_done;
END $$;
"""


def _set_child_cont_backfill_flag(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO app_settings (key, value, updated_at) "
                "VALUES (:k, '1', now()) "
                "ON CONFLICT (key) DO UPDATE SET value = '1', updated_at = now()"
            ),
            {"k": _CHILD_CONT_BACKFILL_FLAG},
        )


def _run_child_cont_backfill_worker(database_url: str) -> None:
    """Long-running backfill executed in a daemon thread so it does not block boot."""
    engine = create_engine(database_url, pool_pre_ping=True)
    try:
        logger.info("Backfilling has_child_continuation from xml_raw (background)…")
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(_CHILD_CONT_BACKFILL_SQL))
        _set_child_cont_backfill_flag(engine)
        logger.info("has_child_continuation backfill complete")
    except Exception:
        logger.exception("has_child_continuation backfill failed; will retry on next startup")
    finally:
        engine.dispose()


def _backfill_has_child_continuation_once(engine) -> None:
    """Schedule the one-time has_child_continuation backfill in a daemon thread."""
    insp = inspect(engine)
    if not insp.has_table("applications") or not insp.has_table("app_settings"):
        return
    cols = {c["name"] for c in insp.get_columns("applications")}
    if "has_child_continuation" not in cols or "xml_raw" not in cols:
        return
    with engine.connect() as conn:
        already = conn.execute(
            text("SELECT value FROM app_settings WHERE key = :k"),
            {"k": _CHILD_CONT_BACKFILL_FLAG},
        ).first()
        if already and (already[0] or "").strip() in ("1", "true", "yes"):
            return
        pending = conn.execute(
            text(
                "SELECT 1 FROM applications "
                "WHERE has_child_continuation IS NULL AND xml_raw IS NOT NULL "
                "AND xml_raw <> '' LIMIT 1"
            )
        ).first()
    if pending is None:
        _set_child_cont_backfill_flag(engine)
        return

    database_url = get_database_url()
    if not database_url:
        return
    t = threading.Thread(
        target=_run_child_cont_backfill_worker,
        args=(database_url,),
        name="has-child-continuation-backfill",
        daemon=True,
    )
    t.start()
    logger.info("has_child_continuation backfill thread started (daemon)")


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


# Backfill earliest_priority_date from xml_raw using xpath. Picks the
# minimum FilingDate across DomesticPriorityList + ForeignPriorityList +
# DomesticBenefit. Same per-row EXCEPTION isolation as the other backfills
# so a single malformed XML row can't abort the whole pass.
_PRIORITY_BACKFILL_FLAG = "schema.earliestPriorityDateBackfilled"
_PRIORITY_BACKFILL_SQL = """
DO $$
DECLARE
  r RECORD;
  v_date DATE;
  v_done INTEGER := 0;
BEGIN
  FOR r IN
    SELECT id, xml_raw
      FROM applications
     WHERE earliest_priority_date IS NULL
       AND xml_raw IS NOT NULL
       AND xml_raw <> ''
  LOOP
    BEGIN
      -- Live USPTO bib XML names the foreign-priority date child
      -- ``<ForeignPriorityDate>`` (not ``<FilingDate>`` like the
      -- domestic blocks). We pull both spellings to stay tolerant of
      -- either schema.
      SELECT MIN(NULLIF(BTRIM(t::text), '')::date) INTO v_date
        FROM unnest(
          xpath('//DomesticPriorityList/DomesticPriority/FilingDate/text()', xmlparse(content r.xml_raw))
            || xpath('//ForeignPriority/ForeignPriorityDate/text()', xmlparse(content r.xml_raw))
            || xpath('//ForeignPriority/FilingDate/text()', xmlparse(content r.xml_raw))
            || xpath('//DomesticBenefit/FilingDate/text()', xmlparse(content r.xml_raw))
        ) AS t;
      IF v_date IS NOT NULL THEN
        UPDATE applications SET earliest_priority_date = v_date WHERE id = r.id;
        v_done := v_done + 1;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;
  RAISE NOTICE 'earliest_priority_date backfill: % rows updated', v_done;
END $$;
"""


def _set_priority_backfill_flag(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO app_settings (key, value, updated_at) "
                "VALUES (:k, '1', now()) "
                "ON CONFLICT (key) DO UPDATE SET value = '1', updated_at = now()"
            ),
            {"k": _PRIORITY_BACKFILL_FLAG},
        )


def _run_priority_backfill_worker(database_url: str) -> None:
    engine = create_engine(database_url, pool_pre_ping=True)
    try:
        logger.info("Backfilling earliest_priority_date from xml_raw (background)…")
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(_PRIORITY_BACKFILL_SQL))
        _set_priority_backfill_flag(engine)
        logger.info("earliest_priority_date backfill complete")
    except Exception:
        logger.exception("earliest_priority_date backfill failed; will retry on next startup")
    finally:
        engine.dispose()


def _backfill_earliest_priority_date_once(engine) -> None:
    insp = inspect(engine)
    if not insp.has_table("applications") or not insp.has_table("app_settings"):
        return
    cols = {c["name"] for c in insp.get_columns("applications")}
    if "earliest_priority_date" not in cols or "xml_raw" not in cols:
        return
    with engine.connect() as conn:
        already = conn.execute(
            text("SELECT value FROM app_settings WHERE key = :k"),
            {"k": _PRIORITY_BACKFILL_FLAG},
        ).first()
        if already and (already[0] or "").strip() in ("1", "true", "yes"):
            return
        pending = conn.execute(
            text(
                "SELECT 1 FROM applications "
                "WHERE earliest_priority_date IS NULL AND xml_raw IS NOT NULL "
                "AND xml_raw <> '' LIMIT 1"
            )
        ).first()
    if pending is None:
        _set_priority_backfill_flag(engine)
        return
    database_url = get_database_url()
    if not database_url:
        return
    t = threading.Thread(
        target=_run_priority_backfill_worker,
        args=(database_url,),
        name="earliest-priority-date-backfill",
        daemon=True,
    )
    t.start()
    logger.info("earliest_priority_date backfill thread started (daemon)")


# Allowance Analytics v2: backfill the four XML-derived columns
# (``abandonment_date``, ``noa_mailed_date``, ``family_root_app_no``,
# ``has_foreign_priority``) for already-ingested rows. Each row is wrapped in
# a per-row EXCEPTION so a single malformed XML can't abort the run, exactly
# the same shape as the ``has_child_continuation`` backfill above.
#
# A single DO block does all four updates so we walk ``xml_raw`` once. The
# ``ContinuityDescription`` filtering on the family-root xpath excludes PCT
# parents (Harness convention treats them as a separate priority axis).
_ALLOWANCE_BACKFILL_FLAG = "schema.allowanceMetricsBackfilled"
_ALLOWANCE_BACKFILL_SQL = """
DO $$
DECLARE
  r RECORD;
  v_doc xml;
  v_abandon DATE;
  v_noa DATE;
  v_family TEXT;
  v_has_fp BOOLEAN;
  v_done INTEGER := 0;
BEGIN
  FOR r IN
    SELECT id, application_number, xml_raw
      FROM applications
     WHERE (abandonment_date IS NULL
            OR noa_mailed_date IS NULL
            OR family_root_app_no IS NULL
            OR has_foreign_priority IS NULL)
       AND xml_raw IS NOT NULL
       AND xml_raw <> ''
  LOOP
    BEGIN
      v_doc := xmlparse(content r.xml_raw);

      -- abandonment_date: prefer explicit element; fall back to the earliest
      -- FileContentHistory whose description mentions abandonment.
      SELECT MIN(NULLIF(BTRIM(t::text), '')::date) INTO v_abandon
        FROM unnest(
          xpath('//AbandonmentDate/text()', v_doc)
          || xpath(
               '//FileContentHistories/FileContentHistory[
                  contains(translate(TransactionDescription/text(),
                    ''ABCDEFGHIJKLMNOPQRSTUVWXYZ'',
                    ''abcdefghijklmnopqrstuvwxyz''),
                    ''abandonment'')
                ]/TransactionDate/text()',
               v_doc)
        ) AS t;

      -- noa_mailed_date: prefer explicit element; fall back to earliest
      -- FCH entry whose description mentions Notice of Allowance.
      SELECT MIN(NULLIF(BTRIM(t::text), '')::date) INTO v_noa
        FROM unnest(
          xpath('//NoticeOfAllowanceMailedDate/text()', v_doc)
          || xpath(
               '//FileContentHistories/FileContentHistory[
                  contains(translate(TransactionDescription/text(),
                    ''ABCDEFGHIJKLMNOPQRSTUVWXYZ'',
                    ''abcdefghijklmnopqrstuvwxyz''),
                    ''notice of allowance'')
                ]/TransactionDate/text()',
               v_doc)
        ) AS t;

      -- family_root_app_no: pick the parent application number whose entry
      -- lists this row's application as the child, excluding PCT parents.
      -- Default to the row's own application number when nothing is listed.
      SELECT NULLIF(BTRIM((
               xpath(
                 '//Continuity/ParentContinuityList/ParentContinuity[
                    ChildApplicationNumber/text()=$child
                    and not(starts-with(ParentApplicationNumber/text(), ''PCT''))
                  ]/ParentApplicationNumber/text()',
                 v_doc,
                 ARRAY[ARRAY['child', r.application_number]]
               )
             )[1]::text), '')
        INTO v_family;
      IF v_family IS NULL THEN
        v_family := r.application_number;
      END IF;

      -- has_foreign_priority: any ForeignPriority entry, or any PCT parent.
      -- Live bib XML wraps entries in <ForeignPriorities> (plural). We
      -- match the leaf element directly to stay schema-tolerant.
      SELECT
        EXISTS (SELECT 1 FROM unnest(xpath('//ForeignPriority', v_doc)) x)
        OR EXISTS (
          SELECT 1 FROM unnest(
            xpath('//Continuity/ParentContinuityList/ParentContinuity/ParentApplicationNumber/text()', v_doc)
          ) AS y
          WHERE upper(y::text) LIKE 'PCT/%' OR upper(y::text) LIKE 'PCT %'
        )
      INTO v_has_fp;

      UPDATE applications SET
        abandonment_date     = COALESCE(abandonment_date, v_abandon),
        noa_mailed_date      = COALESCE(noa_mailed_date, v_noa),
        family_root_app_no   = COALESCE(family_root_app_no, v_family),
        has_foreign_priority = COALESCE(has_foreign_priority, COALESCE(v_has_fp, FALSE))
       WHERE id = r.id;

      v_done := v_done + 1;
    EXCEPTION WHEN OTHERS THEN
      -- Mark malformed rows as resolved-with-defaults so we don't retry
      -- them on every boot. has_foreign_priority defaults to FALSE; the
      -- date fields stay NULL (legitimately unknown).
      BEGIN
        UPDATE applications SET
          has_foreign_priority = COALESCE(has_foreign_priority, FALSE),
          family_root_app_no   = COALESCE(family_root_app_no, application_number)
         WHERE id = r.id;
      EXCEPTION WHEN OTHERS THEN
        NULL;
      END;
    END;
  END LOOP;
  RAISE NOTICE 'allowance_metrics backfill: % rows updated', v_done;
END $$;
"""


def _set_allowance_backfill_flag(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO app_settings (key, value, updated_at) "
                "VALUES (:k, '1', now()) "
                "ON CONFLICT (key) DO UPDATE SET value = '1', updated_at = now()"
            ),
            {"k": _ALLOWANCE_BACKFILL_FLAG},
        )


def _run_allowance_backfill_worker(database_url: str) -> None:
    engine = create_engine(database_url, pool_pre_ping=True)
    try:
        logger.info("Backfilling allowance_metrics from xml_raw (background)…")
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(_ALLOWANCE_BACKFILL_SQL))
        _set_allowance_backfill_flag(engine)
        logger.info("allowance_metrics backfill complete")
    except Exception:
        logger.exception("allowance_metrics backfill failed; will retry on next startup")
    finally:
        engine.dispose()


def _backfill_allowance_metrics_once(engine) -> None:
    """Schedule the one-time Allowance Analytics v2 backfill in a daemon thread."""
    insp = inspect(engine)
    if not insp.has_table("applications") or not insp.has_table("app_settings"):
        return
    cols = {c["name"] for c in insp.get_columns("applications")}
    required = {
        "abandonment_date",
        "noa_mailed_date",
        "family_root_app_no",
        "has_foreign_priority",
        "xml_raw",
    }
    if not required.issubset(cols):
        return
    with engine.connect() as conn:
        already = conn.execute(
            text("SELECT value FROM app_settings WHERE key = :k"),
            {"k": _ALLOWANCE_BACKFILL_FLAG},
        ).first()
        if already and (already[0] or "").strip() in ("1", "true", "yes"):
            return
        pending = conn.execute(
            text(
                "SELECT 1 FROM applications "
                "WHERE (abandonment_date IS NULL "
                "       OR noa_mailed_date IS NULL "
                "       OR family_root_app_no IS NULL "
                "       OR has_foreign_priority IS NULL) "
                "  AND xml_raw IS NOT NULL AND xml_raw <> '' LIMIT 1"
            )
        ).first()
    if pending is None:
        _set_allowance_backfill_flag(engine)
        return

    database_url = get_database_url()
    if not database_url:
        return
    t = threading.Thread(
        target=_run_allowance_backfill_worker,
        args=(database_url,),
        name="allowance-metrics-backfill",
        daemon=True,
    )
    t.start()
    logger.info("allowance_metrics backfill thread started (daemon)")


# Filings-by-Type backfill: bucket each row into one of
# ``provisional`` / ``regular`` / ``con`` / ``div`` / ``cip`` / ``design`` /
# ``other`` using application-number prefix (for provisional/design/reissue)
# plus an xpath probe of the row's own ``ParentContinuity`` entry's
# ``ContinuityDescription`` for CON/CIP/DIV. Mirrors the Python classifier
# in ``xml_parser.classify_application_type`` so SQL-backfilled rows match
# Python-ingested rows. Per-row EXCEPTION isolation keeps a single bad XML
# from aborting the run; the fallback writes ``regular`` for non-prefix
# rows when the XML can't be parsed (same conservative behavior as
# ``classify_application_type_from_xml``).
_APPLICATION_TYPE_BACKFILL_FLAG = "schema.applicationTypeBackfilled"
_APPLICATION_TYPE_BACKFILL_SQL = """
DO $$
DECLARE
  r RECORD;
  v_doc xml;
  v_kind TEXT;
  v_desc TEXT;
  v_done INTEGER := 0;
BEGIN
  FOR r IN
    SELECT id, application_number, xml_raw
      FROM applications
     WHERE application_type IS NULL
  LOOP
    BEGIN
      v_kind := NULL;

      -- App-number prefix wins (matches xml_parser._has_prefix order).
      IF r.application_number ~ '^6[0-3]/' OR r.application_number ~ '^6[0-3][0-9]' THEN
        v_kind := 'provisional';
      ELSIF r.application_number ~ '^29/' OR r.application_number ~ '^29[0-9]' THEN
        v_kind := 'design';
      ELSIF r.application_number ~ '^(35|90|95|96)/' OR r.application_number ~ '^(35|90|95|96)[0-9]' THEN
        v_kind := 'other';
      END IF;

      IF v_kind IS NULL AND r.xml_raw IS NOT NULL AND r.xml_raw <> '' THEN
        v_doc := xmlparse(content r.xml_raw);
        SELECT lower(BTRIM(t::text)) INTO v_desc
          FROM unnest(
            xpath(
              '//Continuity/ParentContinuityList/ParentContinuity[
                 ChildApplicationNumber/text()=$child
                 and not(starts-with(ParentApplicationNumber/text(), ''PCT''))
               ]/ContinuityDescription/text()',
              v_doc,
              ARRAY[ARRAY['child', r.application_number]]
            )
          ) AS t
         LIMIT 1;
        IF v_desc IN ('continuation in part', 'continuation-in-part', 'continuation in-part') THEN
          v_kind := 'cip';
        ELSIF v_desc IN ('division', 'divisional') THEN
          v_kind := 'div';
        ELSIF v_desc = 'continuation' THEN
          v_kind := 'con';
        END IF;
      END IF;

      IF v_kind IS NULL THEN
        v_kind := 'regular';
      END IF;

      UPDATE applications SET application_type = v_kind WHERE id = r.id;
      v_done := v_done + 1;
    EXCEPTION WHEN OTHERS THEN
      BEGIN
        UPDATE applications SET application_type = 'regular' WHERE id = r.id;
      EXCEPTION WHEN OTHERS THEN
        NULL;
      END;
    END;
  END LOOP;
  RAISE NOTICE 'application_type backfill: % rows updated', v_done;
END $$;
"""


def _set_application_type_backfill_flag(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO app_settings (key, value, updated_at) "
                "VALUES (:k, '1', now()) "
                "ON CONFLICT (key) DO UPDATE SET value = '1', updated_at = now()"
            ),
            {"k": _APPLICATION_TYPE_BACKFILL_FLAG},
        )


def _run_application_type_backfill_worker(database_url: str) -> None:
    engine = create_engine(database_url, pool_pre_ping=True)
    try:
        logger.info("Backfilling application_type from xml_raw + app-no prefix (background)…")
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(_APPLICATION_TYPE_BACKFILL_SQL))
        _set_application_type_backfill_flag(engine)
        logger.info("application_type backfill complete")
    except Exception:
        logger.exception("application_type backfill failed; will retry on next startup")
    finally:
        engine.dispose()


# Recompute has_foreign_priority + earliest_priority_date from xml_raw
# for *every* row, regardless of current value. Needed because earlier
# builds shipped a wrong xpath (looking for ``ForeignPriorityList`` as
# the wrapper, when the live USPTO bib XML actually wraps entries in
# ``ForeignPriorities``). Those bad reads wrote ``false`` (not NULL)
# into the column, so the standard "NULL-only" backfill above can never
# correct them. This recompute runs once per database — gated by
# :data:`_FOREIGN_PRIORITY_RECOMPUTE_FLAG` in ``app_settings`` — and
# overwrites the values directly from XML using the corrected xpath.
_FOREIGN_PRIORITY_RECOMPUTE_FLAG = "schema.foreignPriorityRecomputed"
_FOREIGN_PRIORITY_RECOMPUTE_SQL = """
DO $$
DECLARE
  r RECORD;
  v_doc xml;
  v_has_fp BOOLEAN;
  v_pri_date DATE;
  v_done INTEGER := 0;
BEGIN
  FOR r IN
    SELECT id, xml_raw
      FROM applications
     WHERE xml_raw IS NOT NULL AND xml_raw <> ''
  LOOP
    BEGIN
      v_doc := xmlparse(content r.xml_raw);

      SELECT
        EXISTS (SELECT 1 FROM unnest(xpath('//ForeignPriority', v_doc)) x)
        OR EXISTS (
          SELECT 1 FROM unnest(
            xpath('//Continuity/ParentContinuityList/ParentContinuity/ParentApplicationNumber/text()', v_doc)
          ) AS y
          WHERE upper(y::text) LIKE 'PCT/%' OR upper(y::text) LIKE 'PCT %'
        )
      INTO v_has_fp;

      SELECT MIN(NULLIF(BTRIM(t::text), '')::date) INTO v_pri_date
        FROM unnest(
          xpath('//DomesticPriorityList/DomesticPriority/FilingDate/text()', v_doc)
            || xpath('//ForeignPriority/ForeignPriorityDate/text()', v_doc)
            || xpath('//ForeignPriority/FilingDate/text()', v_doc)
            || xpath('//DomesticBenefit/FilingDate/text()', v_doc)
        ) AS t;

      UPDATE applications
         SET has_foreign_priority = v_has_fp,
             earliest_priority_date = COALESCE(v_pri_date, earliest_priority_date)
       WHERE id = r.id;
      v_done := v_done + 1;
    EXCEPTION WHEN OTHERS THEN
      NULL;
    END;
  END LOOP;
  RAISE NOTICE 'has_foreign_priority recompute: % rows updated', v_done;
END $$;
"""


def _set_foreign_priority_recompute_flag(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO app_settings (key, value, updated_at) "
                "VALUES (:k, '1', now()) "
                "ON CONFLICT (key) DO UPDATE SET value = '1', updated_at = now()"
            ),
            {"k": _FOREIGN_PRIORITY_RECOMPUTE_FLAG},
        )


def _run_foreign_priority_recompute_worker(database_url: str) -> None:
    engine = create_engine(database_url, pool_pre_ping=True)
    try:
        logger.info("Recomputing has_foreign_priority from xml_raw (background)…")
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(_FOREIGN_PRIORITY_RECOMPUTE_SQL))
        _set_foreign_priority_recompute_flag(engine)
        logger.info("has_foreign_priority recompute complete")
    except Exception:
        logger.exception(
            "has_foreign_priority recompute failed; will retry on next startup"
        )
    finally:
        engine.dispose()


def _recompute_has_foreign_priority_once(engine) -> None:
    """Schedule the one-time has_foreign_priority recompute in a daemon thread."""
    insp = inspect(engine)
    if not insp.has_table("applications") or not insp.has_table("app_settings"):
        return
    cols = {c["name"] for c in insp.get_columns("applications")}
    if "has_foreign_priority" not in cols or "xml_raw" not in cols:
        return
    with engine.connect() as conn:
        already = conn.execute(
            text("SELECT value FROM app_settings WHERE key = :k"),
            {"k": _FOREIGN_PRIORITY_RECOMPUTE_FLAG},
        ).first()
        if already and (already[0] or "").strip() in ("1", "true", "yes"):
            return

    database_url = get_database_url()
    if not database_url:
        return
    t = threading.Thread(
        target=_run_foreign_priority_recompute_worker,
        args=(database_url,),
        name="has-foreign-priority-recompute",
        daemon=True,
    )
    t.start()
    logger.info("has_foreign_priority recompute thread started (daemon)")


def _backfill_application_type_once(engine) -> None:
    insp = inspect(engine)
    if not insp.has_table("applications") or not insp.has_table("app_settings"):
        return
    cols = {c["name"] for c in insp.get_columns("applications")}
    if "application_type" not in cols:
        return
    with engine.connect() as conn:
        already = conn.execute(
            text("SELECT value FROM app_settings WHERE key = :k"),
            {"k": _APPLICATION_TYPE_BACKFILL_FLAG},
        ).first()
        if already and (already[0] or "").strip() in ("1", "true", "yes"):
            return
        pending = conn.execute(
            text("SELECT 1 FROM applications WHERE application_type IS NULL LIMIT 1")
        ).first()
    if pending is None:
        _set_application_type_backfill_flag(engine)
        return

    database_url = get_database_url()
    if not database_url:
        return
    t = threading.Thread(
        target=_run_application_type_backfill_worker,
        args=(database_url,),
        name="application-type-backfill",
        daemon=True,
    )
    t.start()
    logger.info("application_type backfill thread started (daemon)")


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
        _add_column_if_missing(
            engine,
            "applications",
            "has_child_continuation",
            "BOOLEAN",
        )
        _add_column_if_missing(
            engine,
            "applications",
            "earliest_priority_date",
            "DATE",
        )
        _add_column_if_missing(
            engine,
            "applications",
            "abandonment_date",
            "DATE",
        )
        _add_column_if_missing(
            engine,
            "applications",
            "noa_mailed_date",
            "DATE",
        )
        _add_column_if_missing(
            engine,
            "applications",
            "family_root_app_no",
            "TEXT",
        )
        _add_column_if_missing(
            engine,
            "applications",
            "has_foreign_priority",
            "BOOLEAN",
        )
        _add_column_if_missing(
            engine,
            "applications",
            "tenant_id",
            "TEXT NOT NULL DEFAULT 'global'",
        )
        _add_column_if_missing(
            engine,
            "applications",
            "application_type",
            "TEXT",
        )
        _ensure_auth_tables(engine)
        _ensure_timeline_tables(engine)
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
        _backfill_has_child_continuation_once(engine)
        _backfill_earliest_priority_date_once(engine)
        _backfill_allowance_metrics_once(engine)
        _backfill_application_type_once(engine)
        _recompute_has_foreign_priority_once(engine)
    finally:
        engine.dispose()


# Backwards-compatible name for imports
ensure_application_analytics_schema = ensure_schema_migrations
