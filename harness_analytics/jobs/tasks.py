"""Registered Arq tasks. Each task runs in the worker process.

Worker is started via ``python -m harness_analytics.jobs.worker``. Each task
opens its own SQLAlchemy session — never share a session across tasks.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def recompute_analytics_all(ctx: dict, *, interview_window_days: int = 90) -> dict:
    from harness_analytics.analytics import (
        compute_analytics_for_application,
        load_office_config,
    )
    from harness_analytics.db import get_session_factory
    from harness_analytics.models import Application

    SessionLocal = get_session_factory()
    office_cfg = load_office_config()
    done = 0
    with SessionLocal() as db:
        apps = db.query(Application).order_by(Application.id).all()
        total = len(apps)
        for app in apps:
            compute_analytics_for_application(
                db,
                app,
                interview_window_days=interview_window_days,
                office_cfg=office_cfg,
            )
            db.commit()
            done += 1
    logger.info("recompute_analytics_all done: %d/%d", done, total)
    return {"total": total, "done": done}


async def timeline_recompute_application(ctx: dict, application_id: int) -> dict:
    """Recompute timeline deadlines for a single application.

    Wired up in M3 once the materializer exists; for now the registration
    itself is the deliverable so portal code can enqueue without crashing.
    """
    try:
        from harness_analytics.timeline.materializer import (
            recompute_for_application,
        )
        from harness_analytics.db import get_session_factory

        SessionLocal = get_session_factory()
        with SessionLocal() as db:
            n = recompute_for_application(db, application_id)
        return {"application_id": application_id, "deadlines_written": n}
    except ImportError:
        logger.warning("timeline.materializer not yet available; skip app=%s", application_id)
        return {"application_id": application_id, "skipped": True}


async def timeline_recompute_all(ctx: dict, tenant_id: str = "global") -> dict:
    try:
        from harness_analytics.timeline.materializer import (
            recompute_for_tenant,
        )
        from harness_analytics.db import get_session_factory

        SessionLocal = get_session_factory()
        with SessionLocal() as db:
            n = recompute_for_tenant(db, tenant_id)
        return {"tenant_id": tenant_id, "applications_processed": n}
    except ImportError:
        logger.warning("timeline.materializer not yet available; skip tenant=%s", tenant_id)
        return {"tenant_id": tenant_id, "skipped": True}


async def seed_ifw_rules(ctx: dict, tenant_id: str = "global") -> dict:
    from harness_analytics.timeline.rules_repo import seed_global_rules
    from harness_analytics.db import get_session_factory

    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        n = seed_global_rules(db, tenant_id=tenant_id)
    return {"tenant_id": tenant_id, "rows_upserted": n}


REGISTERED_TASKS: list[Any] = [
    recompute_analytics_all,
    timeline_recompute_application,
    timeline_recompute_all,
    seed_ifw_rules,
]
