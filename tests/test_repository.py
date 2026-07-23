"""Postgres/SQLite data-access layer."""

from __future__ import annotations

from pyflock.core import repository
from pyflock.core.db import session_scope
from pyflock.core.enums import JobState, NodeState


def test_create_and_get_job():
    with session_scope() as session:
        job = repository.create_job(
            session, type="shell", spec={"command": "echo hi"}, max_attempts=3, timeout=30
        )
        job_id = job.id
        assert job.state == JobState.QUEUED

    with session_scope() as session:
        fetched = repository.get_job(session, job_id)
        assert fetched is not None
        assert fetched.type == "shell"
        assert fetched.spec == {"command": "echo hi"}


def test_mark_running_increments_attempts():
    with session_scope() as session:
        job = repository.create_job(session, type="sleep", spec={}, max_attempts=3, timeout=30)
        job_id = job.id

    with session_scope() as session:
        repository.mark_running(session, job_id, "node-1")

    with session_scope() as session:
        job = repository.get_job(session, job_id)
        assert job.state == JobState.RUNNING
        assert job.attempts == 1
        assert job.assigned_node == "node-1"
        assert job.started_at is not None


def test_mark_succeeded_and_failed():
    with session_scope() as session:
        j1 = repository.create_job(session, type="sleep", spec={}, max_attempts=3, timeout=30)
        j2 = repository.create_job(session, type="sleep", spec={}, max_attempts=3, timeout=30)
        id1, id2 = j1.id, j2.id

    with session_scope() as session:
        repository.mark_succeeded(session, id1, exit_code=0, stdout="done", stderr="")
        repository.mark_failed(
            session, id2, state=JobState.DEAD_LETTER, exit_code=1, error="boom"
        )

    with session_scope() as session:
        assert repository.get_job(session, id1).state == JobState.SUCCEEDED
        assert repository.get_job(session, id1).stdout == "done"
        dead = repository.get_job(session, id2)
        assert dead.state == JobState.DEAD_LETTER
        assert dead.error == "boom"


def test_counts_by_state():
    with session_scope() as session:
        for _ in range(3):
            repository.create_job(session, type="sleep", spec={}, max_attempts=1, timeout=1)

    with session_scope() as session:
        counts = repository.counts_by_state(session)
        assert counts[JobState.QUEUED.value] == 3
        assert counts[JobState.SUCCEEDED.value] == 0


def test_set_state_requeue_clears_assignment():
    with session_scope() as session:
        job = repository.create_job(session, type="sleep", spec={}, max_attempts=3, timeout=30)
        job_id = job.id
        repository.mark_running(session, job_id, "node-x")

    with session_scope() as session:
        repository.set_state(session, job_id, JobState.QUEUED)

    with session_scope() as session:
        job = repository.get_job(session, job_id)
        assert job.state == JobState.QUEUED
        assert job.assigned_node is None


def test_node_upsert_touch_and_mark_dead():
    with session_scope() as session:
        node = repository.upsert_node(session, node_id="n1", hostname="host", concurrency=4)
        assert node.state == NodeState.ALIVE

    with session_scope() as session:
        repository.mark_node_dead(session, "n1")

    with session_scope() as session:
        assert repository.get_node(session, "n1").state == NodeState.DEAD
        assert len(repository.list_nodes(session)) == 1
