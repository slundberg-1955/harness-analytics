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
    ), 0) AS overdue_deadline_count
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
    variant_key TEXT NOT NULL DEFAULT '',
    aliases TEXT[],
    close_complete_codes TEXT[] NOT NULL DEFAULT '{}',
    close_nar_codes TEXT[] NOT NULL DEFAULT '{}',
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
    UNIQUE (tenant_id, code, variant_key)
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
    closed_by_ifw_document_id BIGINT REFERENCES file_wrapper_documents(id) ON DELETE SET NULL,
    closed_by_rule_pattern TEXT,
    closed_disposition TEXT,
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

# 0009: docket cross-off / NAR feature.
#
# ``ifw_rules`` grows ``variant_key`` (so the same triggering code can drive
# multiple due-item variants) plus the two ``close_*_codes`` arrays consumed
# by the materializer's auto-close pass. ``computed_deadlines`` grows three
# audit columns and a ``(tenant_id, status, primary_date)`` index that backs
# the inbox filter without a full scan.
_DOCKET_CLOSE_IFW_RULES_COLS = (
    ("variant_key", "TEXT NOT NULL DEFAULT ''"),
    ("close_complete_codes", "TEXT[] NOT NULL DEFAULT '{}'"),
    ("close_nar_codes", "TEXT[] NOT NULL DEFAULT '{}'"),
)
_DOCKET_CLOSE_DEADLINE_COLS = (
    (
        "closed_by_ifw_document_id",
        "BIGINT REFERENCES file_wrapper_documents(id) ON DELETE SET NULL",
    ),
    ("closed_by_rule_pattern", "TEXT"),
    ("closed_disposition", "TEXT"),
)
_DEADLINES_STATUS_TENANT_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_deadlines_status_tenant "
    "ON computed_deadlines (tenant_id, status, primary_date)"
)
_IFW_RULES_VARIANT_UNIQUE_SQL = (
    "ALTER TABLE ifw_rules "
    "ADD CONSTRAINT uq_ifw_rules_tenant_code_variant "
    "UNIQUE (tenant_id, code, variant_key)"
)
_IFW_RULES_DROP_LEGACY_UNIQUE_SQL = (
    "ALTER TABLE ifw_rules DROP CONSTRAINT IF EXISTS uq_ifw_rules_tenant_code"
)


def _ensure_app_settings_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_APP_SETTINGS_TABLE_SQL))


def _ensure_docket_close_columns(engine) -> None:
    """Idempotently add the 0009 docket cross-off / NAR columns + index.

    Mirrors :file:`migrations/versions/0009_docket_cross_off.py` for
    deployments that boot without applying Alembic migrations (the dev
    container, the railway.toml init job, etc.). Safe to run on every
    startup — every step is gated by an information_schema check.

    Steps, in order:

    1. Append ``variant_key`` / ``close_complete_codes`` / ``close_nar_codes``
       to ``ifw_rules`` if missing.
    2. Swap the legacy ``uq_ifw_rules_tenant_code`` unique constraint for
       ``uq_ifw_rules_tenant_code_variant`` once ``variant_key`` exists.
    3. Append ``closed_by_ifw_document_id`` / ``closed_by_rule_pattern`` /
       ``closed_disposition`` to ``computed_deadlines`` if missing.
    4. Create the ``idx_deadlines_status_tenant`` composite index.
    """
    insp = inspect(engine)
    with engine.begin() as conn:
        if insp.has_table("ifw_rules"):
            existing = {c["name"] for c in insp.get_columns("ifw_rules")}
            for col, ddl in _DOCKET_CLOSE_IFW_RULES_COLS:
                if col not in existing:
                    conn.execute(
                        text(f"ALTER TABLE ifw_rules ADD COLUMN {col} {ddl}")
                    )
                    logger.info("Added column ifw_rules.%s", col)
            try:
                uniques = {
                    u["name"] for u in insp.get_unique_constraints("ifw_rules")
                }
            except Exception:  # noqa: BLE001
                uniques = set()
            if "uq_ifw_rules_tenant_code_variant" not in uniques:
                # Drop the legacy two-column key first (best-effort: the
                # constraint name might already be missing on hand-rolled
                # schemas).
                try:
                    conn.execute(text(_IFW_RULES_DROP_LEGACY_UNIQUE_SQL))
                except Exception:  # noqa: BLE001
                    pass
                try:
                    conn.execute(text(_IFW_RULES_VARIANT_UNIQUE_SQL))
                    logger.info(
                        "Created unique uq_ifw_rules_tenant_code_variant"
                    )
                except Exception:  # noqa: BLE001
                    # If the constraint already exists under a different name
                    # we don't want to abort startup.
                    logger.exception(
                        "Could not add uq_ifw_rules_tenant_code_variant"
                    )
        if insp.has_table("computed_deadlines"):
            existing = {c["name"] for c in insp.get_columns("computed_deadlines")}
            for col, ddl in _DOCKET_CLOSE_DEADLINE_COLS:
                if col not in existing:
                    conn.execute(
                        text(
                            f"ALTER TABLE computed_deadlines ADD COLUMN {col} {ddl}"
                        )
                    )
                    logger.info("Added column computed_deadlines.%s", col)
            conn.execute(text(_DEADLINES_STATUS_TENANT_INDEX_SQL))


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
      SELECT MIN(NULLIF(BTRIM(t::text), '')::date) INTO v_date
        FROM unnest(
          xpath('//DomesticPriorityList/DomesticPriority/FilingDate/text()', xmlparse(content r.xml_raw))
            || xpath('//ForeignPriorityList/ForeignPriority/FilingDate/text()', xmlparse(content r.xml_raw))
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
            "tenant_id",
            "TEXT NOT NULL DEFAULT 'global'",
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
        # 0009: docket cross-off / NAR. Run after _ensure_timeline_tables has
        # created ifw_rules + computed_deadlines, so the helper just needs to
        # add the new columns + swap the unique key.
        try:
            _ensure_docket_close_columns(engine)
        except Exception:  # noqa: BLE001
            logger.exception(
                "_ensure_docket_close_columns failed; continuing — "
                "Alembic 0009 will catch it up"
            )
        _ensure_patent_applications_view(engine)
        _backfill_applicant_names_once(engine)
        _backfill_has_child_continuation_once(engine)
        _backfill_earliest_priority_date_once(engine)
    finally:
        engine.dispose()


# Backwards-compatible name for imports
ensure_application_analytics_schema = ensure_schema_migrations
