"""SQLAlchemy engine/session management and the declarative base."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from pyflock.config import get_settings


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


_engine: Engine | None = None
_SessionFactory: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    """Return the process-wide SQLAlchemy engine, creating it on first use."""
    global _engine
    if _engine is None:
        url = get_settings().database_url
        # ``future`` engines are the default in SQLAlchemy 2.0; pool_pre_ping
        # keeps long-lived worker/scheduler connections healthy across restarts
        # of the database container.
        _engine = create_engine(url, pool_pre_ping=True, future=True)
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    """Return a cached session factory bound to the engine."""
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _SessionFactory


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide a transactional session scope.

    Commits on success, rolls back on exception, and always closes.
    """
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_all() -> None:
    """Create all tables. Idempotent; safe to call on every process start."""
    # Import models so they are registered on the metadata before create_all.
    from pyflock.core import models  # noqa: F401

    Base.metadata.create_all(get_engine())


def reset_engine() -> None:
    """Drop cached engine/session factory. Used by tests to swap databases."""
    global _engine, _SessionFactory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _SessionFactory = None
