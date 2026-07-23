"""Reliable queue semantics."""

from __future__ import annotations

from pyflock.core import keys, queue


def test_enqueue_and_claim_is_fifo(redis_client):
    queue.enqueue(redis_client, "a")
    queue.enqueue(redis_client, "b")

    assert queue.claim(redis_client, "node-1") == "a"
    assert queue.claim(redis_client, "node-1") == "b"
    assert queue.claim(redis_client, "node-1") is None


def test_claim_moves_job_to_processing_list(redis_client):
    queue.enqueue(redis_client, "job-1")
    queue.claim(redis_client, "node-1")

    assert queue.processing_ids(redis_client, "node-1") == ["job-1"]
    assert queue.queue_depth(redis_client) == 0


def test_ack_removes_from_processing(redis_client):
    queue.enqueue(redis_client, "job-1")
    queue.claim(redis_client, "node-1")
    queue.ack(redis_client, "node-1", "job-1")

    assert queue.processing_ids(redis_client, "node-1") == []


def test_schedule_retry_moves_to_delayed(redis_client):
    queue.enqueue(redis_client, "job-1")
    queue.claim(redis_client, "node-1")
    queue.schedule_retry(redis_client, "node-1", "job-1", run_at=0.0)

    assert queue.processing_ids(redis_client, "node-1") == []
    assert queue.delayed_count(redis_client) == 1


def test_promote_due_moves_ready_jobs_back(redis_client):
    redis_client.zadd(keys.DELAYED, {"ready": 1.0, "later": 10_000_000_000.0})

    promoted = queue.promote_due(redis_client, now=100.0)

    assert promoted == ["ready"]
    assert queue.queue_depth(redis_client) == 1
    assert queue.delayed_count(redis_client) == 1  # "later" still waiting


def test_dead_letter(redis_client):
    queue.enqueue(redis_client, "job-1")
    queue.claim(redis_client, "node-1")
    queue.dead_letter(redis_client, "node-1", "job-1")

    assert queue.processing_ids(redis_client, "node-1") == []
    assert queue.dead_letter_ids(redis_client) == ["job-1"]


def test_requeue_processing_drains_back_to_queue(redis_client):
    for jid in ("j1", "j2", "j3"):
        queue.enqueue(redis_client, jid)
        queue.claim(redis_client, "dead-node")

    moved = queue.requeue_processing(redis_client, "dead-node")

    assert sorted(moved) == ["j1", "j2", "j3"]
    assert queue.processing_ids(redis_client, "dead-node") == []
    assert queue.queue_depth(redis_client) == 3
