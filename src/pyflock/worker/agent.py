"""The worker agent: register, heartbeat, pull jobs, execute, report results.

Concurrency model
-----------------
The agent runs ``worker_concurrency`` identical puller threads. Each thread
independently blocks on the queue, so the process runs at most that many jobs at
once and idle capacity naturally grabs the next available job. A separate daemon
thread renews the heartbeat. A ``threading.Event`` coordinates graceful
shutdown: puller threads stop claiming new work and in-flight jobs are allowed to
finish before the process exits.
"""

from __future__ import annotations

import logging
import signal
import socket
import threading
import time

import redis

from pyflock.config import Settings, get_settings
from pyflock.core import queue, registry, repository
from pyflock.core.db import session_scope
from pyflock.core.enums import JobState
from pyflock.core.redis_client import get_redis
from pyflock.core.retry import RetryPolicy
from pyflock.worker.executor import ExecutionResult, UnknownJobType, execute

log = logging.getLogger("pyflock.worker")

# How long a puller blocks on the queue before looping to re-check for shutdown.
CLAIM_TIMEOUT = 2


class WorkerAgent:
    """A single worker process managing a pool of puller threads."""

    def __init__(
        self,
        *,
        redis_client: redis.Redis | None = None,
        settings: Settings | None = None,
        node_id: str | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.redis = redis_client or get_redis()
        self.node_id = node_id or socket.gethostname()
        self.hostname = socket.gethostname()
        self.policy = RetryPolicy(
            max_attempts=self.settings.max_attempts,
            base=self.settings.backoff_base,
            cap=self.settings.backoff_cap,
        )
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    # ------------------------------------------------------------------ #
    # Registration & heartbeat
    # ------------------------------------------------------------------ #
    def register(self) -> None:
        """Announce this node to the cluster (Redis liveness + Postgres metadata)."""
        registry.mark_alive(self.redis, self.node_id, ttl=self.settings.heartbeat_ttl)
        with session_scope() as session:
            repository.upsert_node(
                session,
                node_id=self.node_id,
                hostname=self.hostname,
                concurrency=self.settings.worker_concurrency,
            )
        log.info(
            "registered node %s (concurrency=%d)",
            self.node_id,
            self.settings.worker_concurrency,
        )

    def heartbeat_once(self) -> None:
        """Renew the heartbeat TTL and touch the durable node row."""
        registry.refresh(self.redis, self.node_id, ttl=self.settings.heartbeat_ttl)
        with session_scope() as session:
            repository.touch_node(session, self.node_id)

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.heartbeat_once()
            except Exception:  # keep the node alive even through transient errors
                log.exception("heartbeat failed")
            self._stop.wait(self.settings.heartbeat_interval)

    # ------------------------------------------------------------------ #
    # Job processing
    # ------------------------------------------------------------------ #
    def run_once(self, block_timeout: int = CLAIM_TIMEOUT) -> str | None:
        """Claim and fully process a single job.

        Returns the processed job id, or ``None`` if no job was available within
        ``block_timeout`` seconds. ``block_timeout=0`` claims without blocking
        (used by tests).
        """
        if block_timeout > 0:
            job_id = queue.claim_blocking(self.redis, self.node_id, timeout=block_timeout)
        else:
            job_id = queue.claim(self.redis, self.node_id)
        if job_id is None:
            return None
        self._handle(job_id)
        return job_id

    def _handle(self, job_id: str) -> None:
        """Execute a claimed job and record the outcome, retry, or dead-letter it."""
        # Move the job into RUNNING and capture what we need to execute it.
        with session_scope() as session:
            job = repository.get_job(session, job_id)
            if job is None:
                log.warning("claimed unknown job %s; dropping", job_id)
                queue.ack(self.redis, self.node_id, job_id)
                return
            repository.mark_running(session, job_id, self.node_id)
            job_type, spec, timeout = job.type, dict(job.spec), job.timeout

        # Execute outside the DB transaction — this can be slow.
        try:
            result = execute(job_type, spec, timeout=timeout)
        except UnknownJobType as exc:
            self._record_dead_letter(job_id, error=str(exc))
            return
        except Exception as exc:  # unexpected executor crash — treat as a failure
            log.exception("executor crashed for job %s", job_id)
            result = ExecutionResult(exit_code=1, error=f"executor crashed: {exc}")

        self._record_result(job_id, result)

    def _record_result(self, job_id: str, result: ExecutionResult) -> None:
        if result.ok:
            with session_scope() as session:
                repository.mark_succeeded(
                    session,
                    job_id,
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                )
            queue.ack(self.redis, self.node_id, job_id)
            log.info("job %s succeeded", job_id)
            return

        # Decide retry vs dead-letter using the job's own retry budget.
        with session_scope() as session:
            job = repository.get_job(session, job_id)
            attempts = job.attempts if job else self.settings.max_attempts
            max_attempts = job.max_attempts if job else self.settings.max_attempts
            retry = attempts < max_attempts
            repository.mark_failed(
                session,
                job_id,
                state=JobState.FAILED if retry else JobState.DEAD_LETTER,
                exit_code=result.exit_code,
                stdout=result.stdout,
                stderr=result.stderr,
                error=result.error,
            )

        if retry:
            run_at = time.time() + self.policy.backoff_seconds(attempts)
            queue.schedule_retry(self.redis, self.node_id, job_id, run_at)
            log.info("job %s failed (attempt %d); retrying at +%.1fs", job_id, attempts,
                     self.policy.backoff_seconds(attempts))
        else:
            queue.dead_letter(self.redis, self.node_id, job_id)
            log.warning("job %s exhausted retries; dead-lettered", job_id)

    def _record_dead_letter(self, job_id: str, *, error: str) -> None:
        with session_scope() as session:
            repository.mark_failed(
                session, job_id, state=JobState.DEAD_LETTER, exit_code=1, error=error
            )
        queue.dead_letter(self.redis, self.node_id, job_id)
        log.warning("job %s dead-lettered: %s", job_id, error)

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    def _puller_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once(block_timeout=CLAIM_TIMEOUT)
            except Exception:
                log.exception("puller loop error; backing off briefly")
                self._stop.wait(1)

    def run(self) -> None:
        """Register, spin up puller + heartbeat threads, and block until shutdown."""
        self._install_signal_handlers()
        self.register()

        hb = threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True)
        hb.start()
        self._threads.append(hb)

        for i in range(self.settings.worker_concurrency):
            t = threading.Thread(target=self._puller_loop, name=f"puller-{i}", daemon=True)
            t.start()
            self._threads.append(t)

        log.info("worker %s online with %d pullers", self.node_id, self.settings.worker_concurrency)
        # Main thread waits for the stop signal.
        while not self._stop.is_set():
            self._stop.wait(0.5)

        log.info("worker %s shutting down; waiting for in-flight jobs", self.node_id)
        for t in self._threads:
            t.join(timeout=self.settings.job_default_timeout)

    def shutdown(self) -> None:
        """Signal all loops to stop after finishing in-flight work."""
        self._stop.set()

    def _install_signal_handlers(self) -> None:
        def _handler(signum, _frame):
            log.info("received signal %s", signum)
            self.shutdown()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _handler)
            except ValueError:
                # Not on the main thread (e.g. under tests) — skip.
                pass
