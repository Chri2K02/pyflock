"""Control-plane HTTP API."""

from __future__ import annotations

from pyflock.core import queue


def test_submit_job_creates_and_enqueues(api_client, redis_client):
    resp = api_client.post("/jobs", json={"type": "shell", "spec": {"command": "echo hi"}})
    assert resp.status_code == 201
    body = resp.json()
    assert body["type"] == "shell"
    assert body["state"] == "queued"
    # The id is now on the queue.
    assert queue.queue_depth(redis_client) == 1
    assert queue.claim(redis_client, "n") == body["id"]


def test_submit_rejects_unknown_type(api_client):
    resp = api_client.post("/jobs", json={"type": "nope", "spec": {}})
    assert resp.status_code == 422


def test_get_job_and_404(api_client):
    created = api_client.post("/jobs", json={"type": "sleep", "spec": {"seconds": 1}}).json()

    ok = api_client.get(f"/jobs/{created['id']}")
    assert ok.status_code == 200
    assert ok.json()["id"] == created["id"]

    missing = api_client.get("/jobs/does-not-exist")
    assert missing.status_code == 404


def test_list_jobs_and_filter(api_client):
    api_client.post("/jobs", json={"type": "sleep", "spec": {"seconds": 1}})
    api_client.post("/jobs", json={"type": "sleep", "spec": {"seconds": 2}})

    all_jobs = api_client.get("/jobs").json()
    assert len(all_jobs) == 2

    queued = api_client.get("/jobs", params={"state": "queued"}).json()
    assert len(queued) == 2

    running = api_client.get("/jobs", params={"state": "running"}).json()
    assert running == []


def test_health(api_client):
    api_client.post("/jobs", json={"type": "sleep", "spec": {"seconds": 1}})

    data = api_client.get("/health").json()
    assert data["status"] == "ok"
    assert data["queue_depth"] == 1
    assert data["jobs"]["queued"] == 1
    assert data["nodes_total"] == 0
    assert data["nodes_alive"] == 0


def test_nodes_empty_initially(api_client):
    assert api_client.get("/nodes").json() == []
