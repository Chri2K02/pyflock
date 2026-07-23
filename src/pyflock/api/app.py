"""FastAPI application factory and route definitions."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from pyflock import __version__
from pyflock.api.schemas import HealthOut, JobOut, JobSubmit, NodeOut
from pyflock.config import get_settings
from pyflock.core import queue, registry, repository
from pyflock.core.db import create_all, session_scope
from pyflock.core.enums import JobState
from pyflock.core.redis_client import get_redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure the schema exists before serving requests. Idempotent.
    create_all()
    yield


def create_app() -> FastAPI:
    """Build and return the FastAPI application."""
    app = FastAPI(
        title="pyflock control plane",
        version=__version__,
        summary="Submit jobs to and monitor a distributed worker cluster.",
        lifespan=lifespan,
    )

    @app.get("/health", response_model=HealthOut, tags=["cluster"])
    def health() -> HealthOut:
        r = get_redis()
        with session_scope() as session:
            counts = repository.counts_by_state(session)
            nodes = repository.list_nodes(session)
        alive = sum(1 for n in nodes if registry.is_alive(r, n.id))
        return HealthOut(
            status="ok",
            jobs=counts,
            queue_depth=queue.queue_depth(r),
            delayed=queue.delayed_count(r),
            dead_letter=len(queue.dead_letter_ids(r)),
            nodes_total=len(nodes),
            nodes_alive=alive,
        )

    @app.post("/jobs", response_model=JobOut, status_code=201, tags=["jobs"])
    def submit_job(payload: JobSubmit) -> JobOut:
        settings = get_settings()
        r = get_redis()
        with session_scope() as session:
            job = repository.create_job(
                session,
                type=payload.type,
                spec=payload.spec,
                max_attempts=payload.max_attempts or settings.max_attempts,
                timeout=payload.timeout or settings.job_default_timeout,
                priority=payload.priority,
            )
            data = job.to_dict()
        # Enqueue only after the row is durably committed.
        queue.enqueue(r, data["id"])
        return JobOut(**data)

    @app.get("/jobs", response_model=list[JobOut], tags=["jobs"])
    def list_jobs(
        state: JobState | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[JobOut]:
        with session_scope() as session:
            jobs = repository.list_jobs(session, state=state, limit=limit)
            return [JobOut(**j.to_dict()) for j in jobs]

    @app.get("/jobs/{job_id}", response_model=JobOut, tags=["jobs"])
    def get_job(job_id: str) -> JobOut:
        with session_scope() as session:
            job = repository.get_job(session, job_id)
            if job is None:
                raise HTTPException(status_code=404, detail=f"job {job_id} not found")
            return JobOut(**job.to_dict())

    @app.get("/nodes", response_model=list[NodeOut], tags=["cluster"])
    def list_nodes() -> list[NodeOut]:
        r = get_redis()
        with session_scope() as session:
            nodes = repository.list_nodes(session)
            return [NodeOut(**n.to_dict(alive=registry.is_alive(r, n.id))) for n in nodes]

    return app


app = create_app()
