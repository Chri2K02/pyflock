"""End-to-end: a worker dies mid-job and the work still completes elsewhere.

This is the headline reliability property of pyflock. It exercises the full
stack (queue + registry + repository + reaper + worker) with fakeredis and
SQLite, no real infrastructure required.
"""

from __future__ import annotations

import sys

import pytest

from pyflock.core import keys, queue, registry, repository
from pyflock.core.db import session_scope
from pyflock.scheduler.reaper import Reaper
from pyflock.worker.agent import WorkerAgent
from tests.helpers import enqueue_job, get_job

OK_CMD = [sys.executable, "-c", "print('completed by survivor')"]


@pytest.mark.integration
def test_orphaned_job_is_recovered_and_completed(redis_client):
    # --- Node A comes online and claims a job, then crashes mid-flight. ---
    node_a = WorkerAgent(redis_client=redis_client, node_id="node-A")
    node_a.register()

    job_id = enqueue_job(redis_client, type="shell", spec={"command": OK_CMD}, max_attempts=3)

    # Node A claims the job and marks it running...
    assert queue.claim(redis_client, "node-A") == job_id
    with session_scope() as session:
        repository.mark_running(session, job_id, "node-A")
    # ...then the process dies: its heartbeat expires and the job is orphaned.
    redis_client.delete(keys.heartbeat("node-A"))
    assert queue.processing_ids(redis_client, "node-A") == [job_id]
    assert get_job(job_id)["state"] == "running"

    # --- The reaper reclaims the orphaned job. ---
    counters = Reaper(redis_client=redis_client).run_once()
    assert counters["requeued"] == 1
    assert queue.queue_depth(redis_client) == 1
    assert get_job(job_id)["state"] == "queued"
    assert "node-A" not in registry.known_ids(redis_client)

    # --- Node B picks up the recovered job and finishes it. ---
    node_b = WorkerAgent(redis_client=redis_client, node_id="node-B")
    node_b.register()
    assert node_b.run_once(block_timeout=0) == job_id

    final = get_job(job_id)
    assert final["state"] == "succeeded"
    assert final["assigned_node"] == "node-B"
    assert final["attempts"] == 2  # one aborted attempt on A + one success on B
    assert "completed by survivor" in final["stdout"]
    assert queue.processing_ids(redis_client, "node-B") == []
