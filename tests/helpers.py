"""Small helpers shared across tests."""

from __future__ import annotations

import redis

from pyflock.core import queue, repository
from pyflock.core.db import session_scope


def enqueue_job(
    r: redis.Redis, *, type: str, spec: dict, max_attempts: int = 3, timeout: int = 30
) -> str:
    """Create a job row and push it onto the queue, as the API would. Returns id."""
    with session_scope() as session:
        job = repository.create_job(
            session, type=type, spec=spec, max_attempts=max_attempts, timeout=timeout
        )
        job_id = job.id
    queue.enqueue(r, job_id)
    return job_id


def get_job(job_id: str) -> dict | None:
    """Return a job as a plain dict (safe to read after the session closes)."""
    with session_scope() as session:
        job = repository.get_job(session, job_id)
        return job.to_dict() if job is not None else None
