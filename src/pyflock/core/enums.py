"""State enumerations for jobs and nodes."""

from __future__ import annotations

from enum import Enum


class JobState(str, Enum):
    """Lifecycle states of a job.

    ``PENDING``      just created, not yet on the queue.
    ``QUEUED``       waiting in the Redis queue for a worker to pull it.
    ``RUNNING``      claimed by a worker and executing.
    ``SUCCEEDED``    finished with exit code 0.
    ``FAILED``       finished unsuccessfully; may be retried.
    ``DEAD_LETTER``  exhausted its retry budget; parked for inspection.
    """

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"

    @property
    def is_terminal(self) -> bool:
        """True when the job will not transition again on its own."""
        return self in (JobState.SUCCEEDED, JobState.DEAD_LETTER)


class NodeState(str, Enum):
    """Liveness of a worker node."""

    ALIVE = "alive"
    DEAD = "dead"


class JobType(str, Enum):
    """Built-in executor types shipped with pyflock."""

    SHELL = "shell"
    SLEEP = "sleep"
    FETCH_URL = "fetch_url"
