"""Runtime configuration, loaded from environment variables (prefix ``PYFLOCK_``)."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central settings object shared by every pyflock process.

    Values are read from the environment (or a local ``.env`` file) using the
    ``PYFLOCK_`` prefix, e.g. ``PYFLOCK_REDIS_URL``.
    """

    model_config = SettingsConfigDict(
        env_prefix="PYFLOCK_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Connections
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql+psycopg://pyflock:pyflock@localhost:5432/pyflock"

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_url: str = "http://localhost:8000"

    # Worker
    worker_concurrency: int = 4
    job_default_timeout: int = 300

    # Heartbeats / liveness
    heartbeat_interval: float = 3.0
    heartbeat_ttl: int = 10

    # Scheduler / retries
    reaper_interval: float = 5.0
    max_attempts: int = 3
    backoff_base: float = 2.0
    backoff_cap: float = 60.0


@lru_cache
def get_settings() -> Settings:
    """Return a process-wide cached :class:`Settings` instance."""
    return Settings()
