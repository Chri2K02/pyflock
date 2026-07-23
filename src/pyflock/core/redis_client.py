"""Redis connection helper.

Responses are decoded to ``str`` so callers work with plain strings rather than
bytes. A single connection pool is shared per process.
"""

from __future__ import annotations

import redis

from pyflock.config import get_settings

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    """Return the process-wide Redis client, creating it on first use."""
    global _client
    if _client is None:
        _client = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
    return _client


def set_redis(client: redis.Redis | None) -> None:
    """Override the cached client (used by tests to inject fakeredis)."""
    global _client
    _client = client
