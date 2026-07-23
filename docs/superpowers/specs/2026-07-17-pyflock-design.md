# pyflock — Design Spec

**Date:** 2026-07-17
**Status:** Approved (brainstorming complete)

## One-liner

A fault-tolerant distributed job scheduler in Python. A control plane dispatches
work to a horizontally-scalable pool of Docker worker nodes, with heartbeat-based
health monitoring, automatic failure recovery, retries with backoff, and a REST
API + CLI for control.

## Purpose & constraints

- **Purpose:** portfolio / résumé backend showcase. Must look impressive on GitHub
  and be trivial for a reviewer to clone and run.
- **Not** connected to any of the author's other projects. **No AI/LLM inside the
  system** — it is a pure backend distributed-systems piece.
- **Success criteria:** `git clone` + `docker compose up --scale worker=N` yields a
  live multi-node cluster that accepts jobs, runs them, survives a worker being
  killed mid-job (orphaned jobs are recovered), and is inspectable via REST + CLI.

## Locked decisions

| Decision | Choice |
|---|---|
| Main language | **Python 3.12** |
| Cluster model | Docker containers via docker-compose (`--scale worker=N`), not real cloud VMs |
| Monitoring surface | REST API + CLI only (no web dashboard, no Grafana) |
| Queue + node registry | Redis |
| Durable job/result state | Postgres (SQLAlchemy) |
| Dispatch model | Worker **pull** (`BLPOP`), control plane does not push |

## Architecture

```
             ┌────────────────────────────────────────┐
  CLI  ─────▶│           Control Plane (API)           │
  REST ─────▶│  FastAPI: submit jobs, query status,    │
             │  register nodes, expose cluster health  │
             └───────────────┬────────────────────────┘
                             │
              ┌──────────────┼───────────────┐
              ▼              ▼                ▼
          [ Redis ]     [ Postgres ]    [ Scheduler ]
        queue + node   durable job/     reaper loop:
        registry +     result records   detect dead nodes,
        heartbeats                      requeue orphaned jobs
              ▲
     ┌────────┼────────┬────────────┐
     ▼        ▼        ▼            ▼
 [worker-1][worker-2][worker-3]  ... (Docker containers, scale N)
  each: pulls jobs, runs them, heartbeats, reports results
```

**Why pull:** workers `BLPOP` jobs off a Redis queue rather than the control plane
pushing to specific nodes. Simpler, naturally load-balances (idle workers grab
work), and scales to N workers with zero config.

## Components

Each component has one clear purpose, communicates through a defined interface, and
is independently testable.

| Component | Responsibility | Depends on |
|---|---|---|
| **`api`** (FastAPI) | HTTP surface: `POST /jobs`, `GET /jobs/{id}`, `GET /nodes`, `GET /health`. Validates input, enqueues jobs, reads state. | Redis, Postgres |
| **`scheduler`** (reaper loop) | Background task: scans node heartbeats, marks dead nodes, requeues their in-flight jobs, applies retry/backoff. | Redis, Postgres |
| **`worker`** (agent) | Pulls jobs (`BLPOP`), executes them, sends heartbeats, writes results/logs back. Enforces per-worker concurrency limit. | Redis, Postgres |
| **`core`** (shared lib) | Job/Node models, state enums, Redis key helpers, Postgres data-access, retry policy. Pure logic; minimal I/O side effects. | — |
| **`cli`** (Typer) | `cluster submit`, `cluster status`, `cluster nodes`, `cluster logs <id>`. Thin client over the REST API. | api (HTTP) |

## Job model & lifecycle

A **job** is a command + args + optional timeout. Generic: any shell command. Ships
with a couple of built-in demo job types (e.g. `sleep/compute`, `fetch-url`) so the
system runs out of the box.

State machine:

```
PENDING → QUEUED → RUNNING → (SUCCEEDED | FAILED → retry | DEAD_LETTER)
```

## Fault tolerance

- **Heartbeats:** workers write `node:{id}:heartbeat` to Redis every few seconds
  with a TTL. Scheduler treats a missing key as a dead node.
- **Orphan recovery (headline feature):** a `RUNNING` job whose worker died is
  requeued. Demonstrated in the README by killing a container mid-job.
- **Retries:** exponential backoff, max attempts, then dead-letter queue.
- **Delivery guarantee:** at-least-once, documented honestly. Jobs should be written
  idempotently where practical.

## Tech stack

Python 3.12 · FastAPI · Redis · Postgres (SQLAlchemy) · Typer (CLI) ·
Docker + docker-compose · pytest (real Redis/Postgres via testcontainers or
compose) · Ruff + type hints.

## Explicitly out of scope (YAGNI)

- Web dashboard / Grafana / Prometheus.
- Real cloud VM provisioning (AWS/GCP/k8s).
- AuthN/AuthZ, multi-tenancy.
- Any AI/LLM component.
- Exactly-once delivery / distributed transactions.
