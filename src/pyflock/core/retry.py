"""Retry policy: exponential backoff with a cap."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    """Compute retry decisions and backoff delays.

    ``max_attempts`` is the total number of executions allowed (the first try
    counts as attempt 1). ``base`` and ``cap`` shape the exponential backoff.
    """

    max_attempts: int = 3
    base: float = 2.0
    cap: float = 60.0

    def should_retry(self, attempts: int) -> bool:
        """Return True if a job that has run ``attempts`` times may run again."""
        return attempts < self.max_attempts

    def backoff_seconds(self, attempts: int) -> float:
        """Delay before the next attempt, given ``attempts`` completed so far.

        attempts=1 -> base**0, attempts=2 -> base**1, ... capped at ``cap``.
        """
        if attempts < 1:
            return 0.0
        delay = self.base ** (attempts - 1)
        return float(min(delay, self.cap))
