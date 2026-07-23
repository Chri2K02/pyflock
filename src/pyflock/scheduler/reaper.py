"""The reaper loop: the cluster's self-healing background process.

On every tick it does two things:

1. **Promote delayed retries.** Jobs that failed and were scheduled for a future
   retry are moved back onto the main queue once their backoff has elapsed.
2. **Reclaim orphaned work.** For every node whose heartbeat has expired, any
   jobs still checked out on that node's processing list are recovered — either
   requeued (if they still have retries left) or dead-lettered. The dead node is
   then marked dead and forgotten.

This is the mechanism that lets you ``docker kill`` a worker mid-job and watch
the work complete elsewhere.
"""

from __future__ import annotations

import logging
import signal
import threading

import redis

from pyflock.config import Settings, get_settings
from pyflock.core import keys, queue, registry, repository
from pyflock.core.db import session_scope
from pyflock.core.enums import JobState
from pyflock.core.redis_client import get_redis
from pyflock.core.retry import RetryPolicy

log = logging.getLogger("pyflock.scheduler")


class Reaper:
    """Promotes due retries and reclaims jobs from dead nodes."""

    def __init__(
        self,
        *,
        redis_client: redis.Redis | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.redis = redis_client or get_redis()
        self.policy = RetryPolicy(
            max_attempts=self.settings.max_attempts,
            base=self.settings.backoff_base,
            cap=self.settings.backoff_cap,
        )
        self._stop = threading.Event()

    def promote_delayed(self) -> list[str]:
        """Move due retries back onto the main queue and mark them QUEUED."""
        promoted = queue.promote_due(self.redis)
        if promoted:
            with session_scope() as session:
                for job_id in promoted:
                    repository.set_state(session, job_id, JobState.QUEUED)
            log.info("promoted %d delayed job(s) back to the queue", len(promoted))
        return promoted

    def reap_dead_nodes(self) -> dict[str, int]:
        """Reclaim orphaned jobs from every dead node.

        Returns counters ``{"requeued": n, "dead_lettered": m, "nodes": k}``.
        """
        requeued = 0
        dead_lettered = 0
        dead = registry.dead_ids(self.redis)

        for node_id in dead:
            orphans = queue.processing_ids(self.redis, node_id)
            for job_id in orphans:
                if self._reclaim_job(node_id, job_id):
                    requeued += 1
                else:
                    dead_lettered += 1
            with session_scope() as session:
                repository.mark_node_dead(session, node_id)
            registry.forget(self.redis, node_id)
            # Clean up the (now-empty) processing list key.
            self.redis.delete(keys.processing(node_id))
            if orphans:
                log.warning(
                    "node %s died; reclaimed %d orphaned job(s)", node_id, len(orphans)
                )
            else:
                log.info("node %s died; no in-flight jobs to reclaim", node_id)

        return {"requeued": requeued, "dead_lettered": dead_lettered, "nodes": len(dead)}

    def _reclaim_job(self, node_id: str, job_id: str) -> bool:
        """Requeue an orphaned job if it has retries left, else dead-letter it.

        Returns True if the job was requeued, False if dead-lettered.
        """
        with session_scope() as session:
            job = repository.get_job(session, job_id)
            attempts = job.attempts if job else 0
            max_attempts = job.max_attempts if job else self.settings.max_attempts
            retry = attempts < max_attempts
            repository.set_state(
                session, job_id, JobState.QUEUED if retry else JobState.DEAD_LETTER
            )

        if retry:
            queue.ack(self.redis, node_id, job_id)
            queue.enqueue(self.redis, job_id)
            return True
        queue.dead_letter(self.redis, node_id, job_id)
        return False

    def run_once(self) -> dict[str, int]:
        """One full sweep. Returns combined counters."""
        promoted = self.promote_delayed()
        counters = self.reap_dead_nodes()
        counters["promoted"] = len(promoted)
        return counters

    def run(self) -> None:
        """Loop the sweep on ``reaper_interval`` until signalled to stop."""
        self._install_signal_handlers()
        log.info("reaper online (interval=%.1fs)", self.settings.reaper_interval)
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                log.exception("reaper sweep failed")
            self._stop.wait(self.settings.reaper_interval)
        log.info("reaper stopped")

    def shutdown(self) -> None:
        self._stop.set()

    def _install_signal_handlers(self) -> None:
        def _handler(signum, _frame):
            log.info("received signal %s", signum)
            self.shutdown()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _handler)
            except ValueError:
                pass
