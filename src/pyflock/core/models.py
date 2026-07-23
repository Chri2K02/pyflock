"""ORM models: :class:`Job` and :class:`Node`."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from pyflock.core.db import Base
from pyflock.core.enums import JobState, NodeState


def _uuid() -> str:
    return uuid.uuid4().hex


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Job(Base):
    """A unit of work dispatched to the cluster.

    ``spec`` carries the type-specific payload (e.g. the shell command, the URL
    to fetch, or the sleep duration). ``result`` captures stdout/stderr/exit code
    after execution.
    """

    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=_uuid)
    type: Mapped[str] = mapped_column(String(32), nullable=False)
    spec: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    state: Mapped[JobState] = mapped_column(
        String(16), nullable=False, default=JobState.PENDING
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    timeout: Mapped[int] = mapped_column(Integer, nullable=False, default=300)

    assigned_node: Mapped[str | None] = mapped_column(String(64), nullable=True)

    exit_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    stdout: Mapped[str | None] = mapped_column(Text, nullable=True)
    stderr: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    def to_dict(self) -> dict:
        """Serialize to a JSON-friendly dict for API responses."""
        return {
            "id": self.id,
            "type": self.type,
            "spec": self.spec,
            "state": JobState(self.state).value,
            "priority": self.priority,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "timeout": self.timeout,
            "assigned_node": self.assigned_node,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "error": self.error,
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
        }


class Node(Base):
    """A worker node's durable metadata.

    Liveness is authoritative in Redis (the heartbeat key's TTL); this row keeps
    the human-readable metadata plus the last time we observed the node.
    """

    __tablename__ = "nodes"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    concurrency: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    state: Mapped[NodeState] = mapped_column(String(16), nullable=False, default=NodeState.ALIVE)

    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    last_heartbeat: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )

    def to_dict(self, *, alive: bool | None = None) -> dict:
        """Serialize to a JSON-friendly dict.

        ``alive`` (if provided) overrides the stored ``state`` with the live
        Redis-derived liveness so the API always reports the truth.
        """
        state = (
            (NodeState.ALIVE if alive else NodeState.DEAD).value
            if alive is not None
            else NodeState(self.state).value
        )
        return {
            "id": self.id,
            "hostname": self.hostname,
            "concurrency": self.concurrency,
            "state": state,
            "registered_at": _iso(self.registered_at),
            "last_heartbeat": _iso(self.last_heartbeat),
        }


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None
