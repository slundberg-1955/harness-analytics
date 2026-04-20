"""Arq + Redis job queue.

The queue is **optional**: if ``REDIS_URL`` is not set, callers should fall
back to the in-process threading path in :mod:`harness_analytics.bulk_recompute`
(or run the work synchronously). The Arq worker is meant to run as a second
service on Railway (``python -m harness_analytics.jobs.worker``).

Tasks shipped today:

* :func:`harness_analytics.jobs.tasks.recompute_analytics_all` — equivalent to
  the legacy bulk recompute, but durable across restarts.
* :func:`harness_analytics.jobs.tasks.timeline_recompute_application`
* :func:`harness_analytics.jobs.tasks.timeline_recompute_all`
* :func:`harness_analytics.jobs.tasks.seed_ifw_rules`

The latter three have working bodies wired up in milestones 2 and 3.
"""

from __future__ import annotations

import os


def get_redis_url() -> str | None:
    return os.environ.get("REDIS_URL") or os.environ.get("ARQ_REDIS_URL")


def is_queue_enabled() -> bool:
    return get_redis_url() is not None
