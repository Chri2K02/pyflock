"""Worker agent: registration and the execute/retry/dead-letter decisions."""

from __future__ import annotations

import sys

from pyflock.core import queue, registry
from pyflock.worker.agent import WorkerAgent
from tests.helpers import enqueue_job, get_job

FAIL_CMD = [sys.executable, "-c", "import sys; sys.exit(1)"]
OK_CMD = [sys.executable, "-c", "print('ok')"]


def _agent(redis_client, node_id="worker-1") -> WorkerAgent:
    return WorkerAgent(redis_client=redis_client, node_id=node_id)


def test_register_creates_node_and_heartbeat(redis_client):
    agent = _agent(redis_client)
    agent.register()

    assert registry.is_alive(redis_client, "worker-1")
    nodes = get_nodes()
    assert any(n["id"] == "worker-1" for n in nodes)


def test_run_once_executes_job_to_success(redis_client):
    agent = _agent(redis_client)
    job_id = enqueue_job(redis_client, type="shell", spec={"command": OK_CMD})

    processed = agent.run_once(block_timeout=0)

    assert processed == job_id
    job = get_job(job_id)
    assert job["state"] == "succeeded"
    assert "ok" in job["stdout"]
    # Job is acked off the processing list.
    assert queue.processing_ids(redis_client, "worker-1") == []


def test_failing_job_is_retried_when_budget_remains(redis_client):
    agent = _agent(redis_client)
    job_id = enqueue_job(redis_client, type="shell", spec={"command": FAIL_CMD}, max_attempts=2)

    agent.run_once(block_timeout=0)

    job = get_job(job_id)
    assert job["state"] == "failed"
    assert job["attempts"] == 1
    assert queue.delayed_count(redis_client) == 1
    assert queue.processing_ids(redis_client, "worker-1") == []


def test_failing_job_is_dead_lettered_when_budget_exhausted(redis_client):
    agent = _agent(redis_client)
    job_id = enqueue_job(redis_client, type="shell", spec={"command": FAIL_CMD}, max_attempts=1)

    agent.run_once(block_timeout=0)

    job = get_job(job_id)
    assert job["state"] == "dead_letter"
    assert queue.dead_letter_ids(redis_client) == [job_id]


def test_unknown_job_type_is_dead_lettered(redis_client):
    agent = _agent(redis_client)
    job_id = enqueue_job(redis_client, type="bogus", spec={})

    agent.run_once(block_timeout=0)

    job = get_job(job_id)
    assert job["state"] == "dead_letter"
    assert "no executor" in job["error"]


def test_run_once_returns_none_when_queue_empty(redis_client):
    agent = _agent(redis_client)
    assert agent.run_once(block_timeout=0) is None


def get_nodes():
    from pyflock.core import repository
    from pyflock.core.db import session_scope

    with session_scope() as session:
        return [n.to_dict() for n in repository.list_nodes(session)]
