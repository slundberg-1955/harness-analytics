"""Arq worker entrypoint.

Run with::

    REDIS_URL=redis://... DATABASE_URL=... \\
        python -m harness_analytics.jobs.worker

This module also exposes ``WorkerSettings`` for ``arq`` to discover when run as
``arq harness_analytics.jobs.worker.WorkerSettings``.
"""

from __future__ import annotations

import logging
import sys

from harness_analytics.jobs import get_redis_url
from harness_analytics.jobs.tasks import REGISTERED_TASKS
from harness_analytics.schema_migrations import ensure_schema_migrations

logger = logging.getLogger(__name__)


def _redis_settings():
    from arq.connections import RedisSettings

    url = get_redis_url()
    if not url:
        raise RuntimeError(
            "REDIS_URL not set; the worker needs Redis to consume jobs."
        )
    return RedisSettings.from_dsn(url)


class WorkerSettings:
    """Discovered by the ``arq`` CLI."""

    functions = REGISTERED_TASKS
    max_jobs = 4
    job_timeout = 60 * 60  # 1 hour cap on long recomputes

    @staticmethod
    async def on_startup(ctx):  # noqa: D401 — arq hook
        ensure_schema_migrations()
        logger.info("Worker started; registered tasks: %s", [t.__name__ for t in REGISTERED_TASKS])

    @property
    def redis_settings(self):  # type: ignore[override]
        return _redis_settings()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        from arq.worker import run_worker
    except ImportError:
        print("arq is not installed. `pip install arq redis` (or use the [worker] extra).", file=sys.stderr)
        sys.exit(2)

    settings = WorkerSettings()
    settings.redis_settings  # fail fast if REDIS_URL missing
    run_worker(settings)


if __name__ == "__main__":
    main()
