"""Reliable job queue built on Redis primitives.

Design
------
* The main queue (:data:`keys.QUEUE`) is a Redis list of job ids. Producers
  ``LPUSH`` onto the head; workers pull from the tail, giving FIFO ordering.
* Pulling uses ``BRPOPLPUSH`` (blocking) to atomically move a job id from the
  main queue onto the worker's per-node *processing* list. If the worker dies
  mid-job, the id is still sitting on that processing list, so the reaper can
  move it back — no job is silently lost.
* Delayed retries live in a sorted set (:data:`keys.DELAYED`) scored by the unix
  timestamp at which they become eligible; the scheduler promotes due entries
  back onto the main queue.
* Jobs that exhaust their retry budget are parked on the dead-letter list.

All functions take an explicit ``redis.Redis`` client so they are trivial to
unit-test with fakeredis.
"""

from __future__ import annotations

import time

import redis

from pyflock.core import keys


def enqueue(r: redis.Redis, job_id: str) -> None:
    """Place a job id onto the main queue for immediate pickup."""
    r.lpush(keys.QUEUE, job_id)


def claim_blocking(r: redis.Redis, node_id: str, timeout: int = 5) -> str | None:
    """Block up to ``timeout`` seconds for a job, moving it to the node's list.

    Returns the claimed job id, or ``None`` if the timeout elapsed with no work.
    """
    return r.brpoplpush(keys.QUEUE, keys.processing(node_id), timeout=timeout)


def claim(r: redis.Redis, node_id: str) -> str | None:
    """Non-blocking claim, mainly for tests and draining. See :func:`claim_blocking`."""
    return r.rpoplpush(keys.QUEUE, keys.processing(node_id))


def ack(r: redis.Redis, node_id: str, job_id: str) -> None:
    """Remove a finished job from the node's processing list."""
    r.lrem(keys.processing(node_id), 1, job_id)


def schedule_retry(r: redis.Redis, node_id: str, job_id: str, run_at: float) -> None:
    """Ack the job and register it for a future retry at ``run_at`` (unix ts)."""
    ack(r, node_id, job_id)
    r.zadd(keys.DELAYED, {job_id: run_at})


def dead_letter(r: redis.Redis, node_id: str, job_id: str) -> None:
    """Ack the job and park it on the dead-letter list."""
    ack(r, node_id, job_id)
    r.lpush(keys.DEAD_LETTER, job_id)


def promote_due(r: redis.Redis, now: float | None = None) -> list[str]:
    """Move every delayed job whose time has come back onto the main queue.

    Returns the list of promoted job ids. Safe for a single scheduler; the
    ``zrem`` guard prevents a job being promoted twice.
    """
    now = time.time() if now is None else now
    due = r.zrangebyscore(keys.DELAYED, min="-inf", max=now)
    promoted: list[str] = []
    for job_id in due:
        if r.zrem(keys.DELAYED, job_id):
            r.lpush(keys.QUEUE, job_id)
            promoted.append(job_id)
    return promoted


def requeue_processing(r: redis.Redis, node_id: str) -> list[str]:
    """Drain a (presumed dead) node's processing list back onto the main queue.

    Returns the ids that were recovered.
    """
    moved: list[str] = []
    while True:
        job_id = r.rpoplpush(keys.processing(node_id), keys.QUEUE)
        if job_id is None:
            break
        moved.append(job_id)
    return moved


def queue_depth(r: redis.Redis) -> int:
    """Number of jobs currently waiting on the main queue."""
    return int(r.llen(keys.QUEUE))


def delayed_count(r: redis.Redis) -> int:
    """Number of jobs waiting for a future retry."""
    return int(r.zcard(keys.DELAYED))


def dead_letter_ids(r: redis.Redis) -> list[str]:
    """All job ids currently in the dead-letter list."""
    return list(r.lrange(keys.DEAD_LETTER, 0, -1))


def processing_ids(r: redis.Redis, node_id: str) -> list[str]:
    """Job ids currently checked out by ``node_id``."""
    return list(r.lrange(keys.processing(node_id), 0, -1))
