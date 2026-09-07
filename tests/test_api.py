from __future__ import annotations

from fastapi.testclient import TestClient

from event_worker.api import app, get_queue
from event_worker.clock import FrozenClock
from event_worker.queues.memory import InMemoryQueue


def test_health_and_enqueue_get_stats() -> None:
    queue = InMemoryQueue(clock=FrozenClock())
    app.dependency_overrides[get_queue] = lambda: queue
    client = TestClient(app)
    try:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"

        created = client.post("/v1/jobs", json={"kind": "echo", "payload": {"message": "api"}})
        assert created.status_code == 201
        job_id = created.json()["id"]

        fetched = client.get(f"/v1/jobs/{job_id}")
        assert fetched.status_code == 200
        assert fetched.json()["kind"] == "echo"

        missing = client.get("/v1/jobs/does-not-exist")
        assert missing.status_code == 404

        stats = client.get("/v1/queue/stats")
        assert stats.json()["ready"] == 1
        assert stats.json()["enqueued_total"] == 1
    finally:
        app.dependency_overrides.clear()


def test_dlq_list_and_requeue() -> None:
    queue = InMemoryQueue(clock=FrozenClock())
    app.dependency_overrides[get_queue] = lambda: queue
    client = TestClient(app)
    try:
        job = queue.enqueue("echo", {"message": "x"}, max_attempts=1)
        claimed = queue.claim("w1", lease_seconds=30)
        queue.fail(claimed.id, "dead", worker_id="w1", retryable=True)

        listed = client.get("/v1/queue/dlq")
        assert listed.status_code == 200
        assert listed.json()[0]["id"] == job.id

        revived = client.post(f"/v1/queue/dlq/{job.id}/requeue")
        assert revived.status_code == 200
        assert revived.json()["status"] == "queued"
    finally:
        app.dependency_overrides.clear()
