from __future__ import annotations

from event_worker.clock import FrozenClock
from event_worker.models import JobStatus


def test_enqueue_then_claim(queue, clock: FrozenClock) -> None:
    created = queue.enqueue("echo", {"message": "hi"})
    assert created.status == JobStatus.QUEUED
    claimed = queue.claim("w1", lease_seconds=30)
    assert claimed is not None
    assert claimed.id == created.id
    assert claimed.attempts == 1
    assert claimed.status == JobStatus.LEASED
    assert claimed.lease_owner == "w1"
    assert claimed.lease_expires_at is not None
    assert claimed.lease_expires_at > clock.now()


def test_claim_empty_queue_returns_none(queue) -> None:
    assert queue.claim("w1") is None


def test_leased_job_is_not_claimed_again(queue) -> None:
    queue.enqueue("echo", {"message": "one"})
    first = queue.claim("w1", lease_seconds=30)
    second = queue.claim("w2", lease_seconds=30)
    assert first is not None
    assert second is None


def test_two_ready_jobs_go_to_two_workers(queue) -> None:
    a = queue.enqueue("echo", {"message": "a"})
    b = queue.enqueue("echo", {"message": "b"})
    first = queue.claim("w1", lease_seconds=30)
    second = queue.claim("w2", lease_seconds=30)
    assert {first.id, second.id} == {a.id, b.id}
    assert first.lease_owner != second.lease_owner


def test_ack_completes_and_frees_lease(queue) -> None:
    job = queue.enqueue("echo", {"message": "ok"})
    claimed = queue.claim("w1", lease_seconds=30)
    acked = queue.ack(claimed.id, {"ok": True}, worker_id="w1")
    assert acked.status == JobStatus.COMPLETED
    assert acked.result == {"ok": True}
    loaded = queue.get(job.id)
    assert loaded is not None
    assert loaded.status == JobStatus.COMPLETED
    assert queue.claim("w2") is None
    assert queue.stats().completed == 1


def test_idempotent_enqueue(queue) -> None:
    first = queue.enqueue("echo", {"message": "a"}, idempotency_key="k1")
    second = queue.enqueue("echo", {"message": "b"}, idempotency_key="k1")
    assert first.id == second.id
    assert queue.stats().ready == 1
