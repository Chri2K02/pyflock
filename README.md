# pyflock

[![CI](https://github.com/Chri2K02/pyflock/actions/workflows/ci.yml/badge.svg)](https://github.com/Chri2K02/pyflock/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A fault-tolerant distributed job scheduler in Python. A control plane dispatches
work to a horizontally-scalable pool of Docker worker nodes, with heartbeat-based
health monitoring, automatic recovery of jobs orphaned by dead workers, retries
with exponential backoff, and a REST API + CLI for control.

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
 [worker-1][worker-2][worker-3]  ...  (scale to N containers)
  each: pulls jobs, runs them, heartbeats, reports results
```

## Highlights

- **Worker pull model.** Workers atomically claim jobs from a Redis queue with
  `BRPOPLPUSH`, so idle capacity self-balances and you scale by adding containers —
  no central assignment logic to get wrong.
- **Reliable queue.** A claimed job is held on a per-node *processing* list until
  it's acknowledged, so a crash never silently drops work.
- **Self-healing.** A scheduler reaper watches heartbeats; when a node's heartbeat
  expires, its in-flight jobs are recovered and requeued (or dead-lettered if out
  of retries).
- **Retries with backoff.** Failed jobs are re-scheduled on an exponential backoff
  and dead-lettered once their per-job attempt budget is exhausted.
- **Fully containerized.** `docker compose up` gives you a live multi-node cluster.

## Quickstart

```bash
git clone <your-fork-url> pyflock && cd pyflock
docker compose up --build --scale worker=3
```

That starts Redis, Postgres, the API (on `http://localhost:8000`), the scheduler,
and three workers. Open the interactive API docs at `http://localhost:8000/docs`.

### Submit and inspect jobs with the CLI

Install the client locally (`pip install -e .`) or run it from any machine that can
reach the API:

```bash
export PYFLOCK_API_URL=http://localhost:8000

pyflock shell "echo hello from the cluster"
pyflock sleep 10
pyflock fetch https://example.com

pyflock health          # cluster summary
pyflock nodes           # worker liveness
pyflock jobs            # recent jobs
pyflock status <job-id> # full record for one job
pyflock logs <job-id>   # stdout / stderr
```

### Or use the REST API directly

```bash
curl -X POST localhost:8000/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"shell","spec":{"command":"echo hi"}}'

curl localhost:8000/health
```

## The headline demo: kill a worker mid-job

pyflock's defining property is that losing a worker doesn't lose work.

```bash
# 1. Submit a long job.
pyflock sleep 30

# 2. Find the worker running it and kill it hard.
docker compose ps
docker kill <the-worker-container-running-the-job>

# 3. Within one reaper interval, the job is requeued and another worker finishes it.
watch pyflock jobs
```

The scheduler notices the missing heartbeat, moves the orphaned job off the dead
node's processing list back onto the queue, and a surviving worker completes it.
This is covered end-to-end in `tests/test_integration.py`.

## Built-in job types

| type        | spec                          | what it does                              |
|-------------|-------------------------------|-------------------------------------------|
| `shell`     | `{"command": [...] \| "..."}` | runs a command (exec form or shell form)  |
| `sleep`     | `{"seconds": N}`              | sleeps N seconds (stand-in for compute)   |
| `fetch_url` | `{"url": "https://..."}`      | HTTP GET, reports status + body length    |

## API reference

| Method | Path          | Description                              |
|--------|---------------|------------------------------------------|
| POST   | `/jobs`       | Submit a job; returns the created record |
| GET    | `/jobs`       | List jobs (`?state=`, `?limit=`)         |
| GET    | `/jobs/{id}`  | Fetch one job                            |
| GET    | `/nodes`      | List worker nodes and liveness           |
| GET    | `/health`     | Cluster health + job counts              |

## Job lifecycle

```
PENDING → QUEUED → RUNNING → SUCCEEDED
                        │
                        ├── FAILED ──(retry with backoff)──▶ QUEUED
                        └── DEAD_LETTER  (budget exhausted)
```

Delivery is **at-least-once**: a job may run more than once if a worker dies after
executing but before acknowledging, so job payloads should be idempotent.

## Design notes

- **Redis** holds the transient coordination state: the FIFO job queue, per-node
  processing lists, a sorted set of delayed retries, the dead-letter list, and the
  node registry with TTL-based heartbeats.
- **Postgres** holds the durable record of every job and node — the source of
  truth for status, results, and history.
- **Liveness** is authoritative in Redis: a node is alive exactly as long as its
  heartbeat key (refreshed on an interval, with a TTL) exists.
- Each module has a single responsibility and takes its Redis/DB handles
  explicitly, which is what lets the whole test suite run against fakeredis +
  SQLite with no external services.

## Development

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest                 # full suite, no infrastructure required
ruff check .
```

The suite runs against an in-process fake Redis and a throwaway SQLite database, so
`pytest` is green on a clean checkout. The same code paths run against real Redis +
Postgres under docker-compose.

## Project layout

```
src/pyflock/
  config.py            # env-driven settings
  core/                # domain library
    models.py          # Job, Node ORM models
    enums.py           # job/node states
    keys.py            # Redis key definitions
    queue.py           # reliable queue (claim / ack / retry / dead-letter)
    registry.py        # node liveness via heartbeats
    retry.py           # exponential backoff policy
    repository.py      # Postgres data access
    db.py              # engine / session management
  api/                 # FastAPI control plane
  worker/              # worker agent + job executors
  scheduler/           # reaper loop
  cli/                 # Typer CLI client
tests/                 # unit + end-to-end tests
```

## License

MIT
