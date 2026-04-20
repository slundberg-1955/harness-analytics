"""Helpers to enqueue Arq tasks from the web process.

If Redis isn't configured these helpers raise ``RuntimeError`` so callers
can decide whether to run synchronously instead.
"""

from __future__ import annotations

from typing import Any

from harness_analytics.jobs import get_redis_url


async def enqueue(task_name: str, *args: Any, **kwargs: Any):
    """Enqueue a task by name. Returns the Arq Job object."""
    url = get_redis_url()
    if not url:
        raise RuntimeError("Queue is not enabled (REDIS_URL not set)")
    from arq import create_pool
    from arq.connections import RedisSettings

    redis = await create_pool(RedisSettings.from_dsn(url))
    try:
        return await redis.enqueue_job(task_name, *args, **kwargs)
    finally:
        await redis.close()
