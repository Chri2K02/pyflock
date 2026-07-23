"""Pydantic request/response schemas for the control-plane API."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from pyflock.core.enums import JobType


class JobSubmit(BaseModel):
    """Payload for ``POST /jobs``."""

    type: str = Field(..., description="One of: shell, sleep, fetch_url")
    spec: dict = Field(default_factory=dict, description="Type-specific payload")
    max_attempts: int | None = Field(default=None, ge=1, le=20)
    timeout: int | None = Field(default=None, ge=1)
    priority: int = Field(default=0)

    @field_validator("type")
    @classmethod
    def _known_type(cls, value: str) -> str:
        valid = {t.value for t in JobType}
        if value not in valid:
            raise ValueError(f"unknown job type {value!r}; expected one of {sorted(valid)}")
        return value


class JobOut(BaseModel):
    """A job as returned by the API."""

    id: str
    type: str
    spec: dict
    state: str
    priority: int
    attempts: int
    max_attempts: int
    timeout: int
    assigned_node: str | None
    exit_code: int | None
    stdout: str | None
    stderr: str | None
    error: str | None
    created_at: str | None
    updated_at: str | None
    started_at: str | None
    finished_at: str | None


class NodeOut(BaseModel):
    """A worker node as returned by the API."""

    id: str
    hostname: str
    concurrency: int
    state: str
    registered_at: str | None
    last_heartbeat: str | None


class HealthOut(BaseModel):
    """Cluster health summary for ``GET /health``."""

    status: str
    jobs: dict[str, int]
    queue_depth: int
    delayed: int
    dead_letter: int
    nodes_total: int
    nodes_alive: int
