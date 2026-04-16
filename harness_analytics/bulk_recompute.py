"""Background bulk analytics recompute for the portal (single active job per process)."""

from __future__ import annotations

import secrets
import threading
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy.orm import Session

from harness_analytics.analytics import compute_analytics_for_application, load_office_config
from harness_analytics.db import get_database_url, get_session_factory
from harness_analytics.models import Application

JobStatus = Literal["pending", "running", "completed", "failed"]


@dataclass
class BulkRecomputeJob:
    job_id: str
    status: JobStatus = "pending"
    total: int = 0
    done: int = 0
    error: str | None = None


_lock = threading.Lock()
_jobs: dict[str, BulkRecomputeJob] = {}
_active_job_id: str | None = None


def get_job(job_id: str) -> BulkRecomputeJob | None:
    with _lock:
        return _jobs.get(job_id)


def try_begin_bulk_recompute() -> tuple[BulkRecomputeJob | None, str]:
    """If no job is pending/running, register a new job and return (job, 'started').

    Otherwise return (existing_job, 'already_active').
    """
    global _active_job_id
    with _lock:
        if _active_job_id:
            existing = _jobs.get(_active_job_id)
            if existing and existing.status in ("pending", "running"):
                return existing, "already_active"
            _active_job_id = None
        job = BulkRecomputeJob(job_id=secrets.token_urlsafe(12))
        _jobs[job.job_id] = job
        _active_job_id = job.job_id
        return job, "started"


def _clear_active_if_matches(job_id: str) -> None:
    global _active_job_id
    with _lock:
        if _active_job_id == job_id:
            _active_job_id = None


def run_bulk_recompute_job(job_id: str, interview_window_days: int) -> None:
    """Runs in a FastAPI BackgroundTask: own DB session, one commit per application."""
    if not get_database_url():
        with _lock:
            j = _jobs.get(job_id)
            if j:
                j.status = "failed"
                j.error = "DATABASE_URL is not set"
        _clear_active_if_matches(job_id)
        return

    SessionLocal = get_session_factory()
    db: Session = SessionLocal()
    office_cfg: dict[str, Any] = load_office_config()
    try:
        apps = db.query(Application).order_by(Application.id).all()
        with _lock:
            j = _jobs.get(job_id)
            if j:
                j.total = len(apps)
                j.status = "running"
        for i, app in enumerate(apps, start=1):
            compute_analytics_for_application(
                db,
                app,
                interview_window_days=interview_window_days,
                office_cfg=office_cfg,
            )
            db.commit()
            with _lock:
                jj = _jobs.get(job_id)
                if jj:
                    jj.done = i
        with _lock:
            jf = _jobs.get(job_id)
            if jf:
                jf.status = "completed"
    except Exception as exc:  # noqa: BLE001 — surface any failure to the portal UI
        with _lock:
            jf = _jobs.get(job_id)
            if jf:
                jf.status = "failed"
                jf.error = repr(exc)[:2000]
    finally:
        db.close()
        _clear_active_if_matches(job_id)


def job_to_json(job: BulkRecomputeJob) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "status": job.status,
        "total": job.total,
        "done": job.done,
        "error": job.error,
    }
