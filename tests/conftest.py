"""Shared pytest fixtures.

Tests run against a throwaway SQLite database (one file per test) and an
in-process fakeredis instance, so the whole suite runs with zero infrastructure
(``pytest`` with no Docker required). The same code paths run against Postgres +
real Redis in production; only the connection objects differ.
"""

from __future__ import annotations

import fakeredis
import pytest

from pyflock.config import get_settings
from pyflock.core import db
from pyflock.core.redis_client import set_redis


@pytest.fixture(autouse=True)
def configured(tmp_path, monkeypatch):
    """Point pyflock at a temp SQLite DB and a fresh fakeredis for each test."""
    db_path = tmp_path / "pyflock.db"
    monkeypatch.setenv("PYFLOCK_DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("PYFLOCK_HEARTBEAT_TTL", "10")
    monkeypatch.setenv("PYFLOCK_MAX_ATTEMPTS", "3")

    get_settings.cache_clear()
    db.reset_engine()

    fake = fakeredis.FakeStrictRedis(decode_responses=True)
    set_redis(fake)

    db.create_all()
    yield

    set_redis(None)
    db.reset_engine()
    get_settings.cache_clear()


@pytest.fixture
def redis_client():
    """The shared fakeredis client the code under test will use."""
    from pyflock.core.redis_client import get_redis

    return get_redis()


@pytest.fixture
def api_client():
    """A FastAPI TestClient bound to the configured app."""
    from fastapi.testclient import TestClient

    from pyflock.api.app import create_app

    with TestClient(create_app()) as client:
        yield client
