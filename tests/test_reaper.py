"""Scheduler reaper: delayed promotion and dead-node reclamation."""

from __future__ import annotations

import sys

from pyflock.core import queue, registry, repository
from pyflock.core.db import session_scope
from pyflock.core.enums import JobState
from pyflock.scheduler.reaper import Reaper
from tests.helpers import enqueue_job, get_job

FAIL_CMD = [sys.executable, "-c", "import sys; sys.exit(1)"]


def _reaper(redis_client) -> Reaper:
    return Reaper(redis_client=redis_client)


def test_promote_delayed_requeues_and_marks_queued(redis_client):
    job_id = enqueue_job(redis_client, type="sleep", spec={"seconds": 0})
    # Simulate a scheduled retry that is already due.
    queue.claim(redis_client, "n1")
    queue.schedule_retry(redis_client, "n1", job_id, run_at=0.0)
    with session_scope() as session:
        repository.set_state(session, job_id, JobState.FAILED)

    promoted = _reaper(redis_client).promote_delayed()

    assert promoted == [job_id]
    assert queue.queue_depth(redis_client) == 1
    assert get_job(job_id)["state"] == "queued"


def _orphan_a_job(redis_client, node_id, *, max_attempts=3) -> str:
    """Register a node, claim a job onto it, mark it running, then kill the node."""
    registry.mark_alive(redis_client, node_id, ttl=10)
    with session_scope() as session:
        repository.upsert_node(session, node_id=node_id, hostname=node_id, concurrency=1)
    job_id = enqueue_job(redis_client, type="shell", spec={"command": ["true"]},
                         max_attempts=max_attempts)
    queue.claim(redis_client, node_id)
    with session_scope() as session:
        repository.mark_running(session, job_id, node_id)
    # Node dies: heartbeat expires.
    from pyflock.core import keys

    redis_client.delete(keys.heartbeat(node_id))
    return job_id


def test_reap_requeues_orphan_with_retries_left(redis_client):
    job_id = _orphan_a_job(redis_client, "dead-1", max_attempts=3)

    counters = _reaper(redis_client).reap_dead_nodes()

    assert counters == {"requeued": 1, "dead_lettered": 0, "nodes": 1}
    assert queue.queue_depth(redis_client) == 1
    assert queue.processing_ids(redis_client, "dead-1") == []
    job = get_job(job_id)
    assert job["state"] == "queued"
    assert job["attempts"] == 1  # unchanged by reclamation
    # Node is marked dead and forgotten.
    assert "dead-1" not in registry.known_ids(redis_client)
    with session_scope() as session:
        assert repository.get_node(session, "dead-1").state == "dead"


def test_reap_dead_letters_orphan_without_retries_left(redis_client):
    job_id = _orphan_a_job(redis_client, "dead-2", max_attempts=1)

    counters = _reaper(redis_client).reap_dead_nodes()

    assert counters == {"requeued": 0, "dead_lettered": 1, "nodes": 1}
    assert queue.dead_letter_ids(redis_client) == [job_id]
    assert get_job(job_id)["state"] == "dead_letter"


def test_reap_handles_dead_node_with_no_inflight_jobs(redis_client):
    registry.mark_alive(redis_client, "idle-dead", ttl=10)
    with session_scope() as session:
        repository.upsert_node(session, node_id="idle-dead", hostname="x", concurrency=1)
    from pyflock.core import keys

    redis_client.delete(keys.heartbeat("idle-dead"))

    counters = _reaper(redis_client).reap_dead_nodes()

    assert counters == {"requeued": 0, "dead_lettered": 0, "nodes": 1}
    assert "idle-dead" not in registry.known_ids(redis_client)
