"""Postgres data-access layer for jobs and nodes.

Every function takes an explicit :class:`~sqlalchemy.orm.Session` so callers
control the transaction boundary (the API request, the worker's per-job unit of
work, or the reaper's sweep). Combine with :func:`pyflock.core.db.session_scope`.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from pyflock.core.enums import JobState, NodeState
from pyflock.core.models import Job, Node


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# Jobs
# --------------------------------------------------------------------------- #
def create_job(
    session: Session,
    *,
    type: str,
    spec: dict,
    max_attempts: int,
    timeout: int,
    priority: int = 0,
) -> Job:
    """Insert a new job in the ``QUEUED`` state and return it."""
    job = Job(
        type=type,
        spec=spec,
        state=JobState.QUEUED,
        max_attempts=max_attempts,
        timeout=timeout,
        priority=priority,
    )
    session.add(job)
    session.flush()  # populate job.id before the caller enqueues it
    return job


def get_job(session: Session, job_id: str) -> Job | None:
    """Fetch a single job by id, or ``None``."""
    return session.get(Job, job_id)


def list_jobs(
    session: Session, *, state: JobState | None = None, limit: int = 100
) -> list[Job]:
    """List jobs, newest first, optionally filtered by state."""
    stmt = select(Job).order_by(Job.created_at.desc()).limit(limit)
    if state is not None:
        stmt = stmt.where(Job.state == state)
    return list(session.scalars(stmt))


def mark_running(session: Session, job_id: str, node_id: str) -> Job | None:
    """Transition a job to ``RUNNING``, recording the node and counting the attempt."""
    job = session.get(Job, job_id)
    if job is None:
        return None
    job.state = JobState.RUNNING
    job.assigned_node = node_id
    job.attempts += 1
    job.started_at = _now()
    return job


def mark_succeeded(
    session: Session, job_id: str, *, exit_code: int, stdout: str, stderr: str
) -> Job | None:
    """Record a successful execution."""
    job = session.get(Job, job_id)
    if job is None:
        return None
    job.state = JobState.SUCCEEDED
    job.exit_code = exit_code
    job.stdout = stdout
    job.stderr = stderr
    job.error = None
    job.finished_at = _now()
    return job


def mark_failed(
    session: Session,
    job_id: str,
    *,
    state: JobState,
    exit_code: int | None = None,
    stdout: str = "",
    stderr: str = "",
    error: str | None = None,
) -> Job | None:
    """Record a failed execution; ``state`` is ``FAILED`` (retryable) or ``DEAD_LETTER``."""
    job = session.get(Job, job_id)
    if job is None:
        return None
    job.state = state
    job.exit_code = exit_code
    job.stdout = stdout
    job.stderr = stderr
    job.error = error
    job.finished_at = _now()
    return job


def set_state(session: Session, job_id: str, state: JobState) -> Job | None:
    """Force a job into ``state`` (e.g. requeue back to ``QUEUED``)."""
    job = session.get(Job, job_id)
    if job is None:
        return None
    job.state = state
    if state == JobState.QUEUED:
        job.assigned_node = None
    return job


def counts_by_state(session: Session) -> dict[str, int]:
    """Return a mapping of every JobState to how many jobs are in it."""
    rows = session.execute(select(Job.state, func.count()).group_by(Job.state)).all()
    counts = {s.value: 0 for s in JobState}
    for state, count in rows:
        counts[JobState(state).value] = int(count)
    return counts


# --------------------------------------------------------------------------- #
# Nodes
# --------------------------------------------------------------------------- #
def upsert_node(
    session: Session, *, node_id: str, hostname: str, concurrency: int
) -> Node:
    """Create or refresh a node's durable metadata row."""
    node = session.get(Node, node_id)
    if node is None:
        node = Node(id=node_id, hostname=hostname, concurrency=concurrency)
        session.add(node)
    else:
        node.hostname = hostname
        node.concurrency = concurrency
        node.state = NodeState.ALIVE
        node.last_heartbeat = _now()
    session.flush()
    return node


def touch_node(session: Session, node_id: str) -> Node | None:
    """Update a node's ``last_heartbeat`` and keep it marked ALIVE."""
    node = session.get(Node, node_id)
    if node is None:
        return None
    node.last_heartbeat = _now()
    node.state = NodeState.ALIVE
    return node


def mark_node_dead(session: Session, node_id: str) -> Node | None:
    """Mark a node DEAD (called by the reaper after reclaiming its jobs)."""
    node = session.get(Node, node_id)
    if node is None:
        return None
    node.state = NodeState.DEAD
    return node


def get_node(session: Session, node_id: str) -> Node | None:
    """Fetch a node by id."""
    return session.get(Node, node_id)


def list_nodes(session: Session) -> list[Node]:
    """All known nodes, most recently seen first."""
    stmt = select(Node).order_by(Node.last_heartbeat.desc())
    return list(session.scalars(stmt))
