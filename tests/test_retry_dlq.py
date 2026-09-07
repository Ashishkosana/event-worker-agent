from __future__ import annotations

from event_worker.backoff import retry_delay_seconds
from event_worker.clock import FrozenClock
from event_worker.config import Settings
from event_worker.handler import JobHandler
from event_worker.models import JobStatus
from event_worker.worker import Worker


def test_retryable_fail_delays_then_returns_to_ready(queue, clock: FrozenClock) -> None:
    job = queue.enqueue("echo", {"message": "x"}, max_attempts=3)
    claimed = queue.claim("w1", lease_seconds=30)
    failed = queue.fail(
        claimed.id,
        "boom",
        worker_id="w1",
        retryable=True,
        delay_seconds=10,
    )
    assert failed.status == JobStatus.DELAYED
    assert queue.claim("w2") is None
    clock.advance(10)
    assert queue.promote_delayed() == 1
    again = queue.claim("w2", lease_seconds=30)
    assert again is not None
    assert again.id == job.id
    assert again.attempts == 2


def test_max_attempts_moves_to_dlq(queue) -> None:
    job = queue.enqueue("echo", {"message": "x"}, max_attempts=2)
    for worker_id in ("w1", "w2"):
        claimed = queue.claim(worker_id, lease_seconds=30)
        assert claimed is not None
        updated = queue.fail(
            claimed.id,
            "still failing",
            worker_id=worker_id,
            retryable=True,
            delay_seconds=0,
        )
    assert updated.status == JobStatus.DEAD
    loaded = queue.get(job.id)
    assert loaded is not None
    assert loaded.status == JobStatus.DEAD
    assert loaded.attempts == 2
    dlq = queue.list_dlq()
    assert [j.id for j in dlq] == [job.id]
    assert queue.claim("w3") is None


def test_terminal_fail_skips_remaining_retries(queue) -> None:
    job = queue.enqueue("poison", {}, max_attempts=5)
    claimed = queue.claim("w1", lease_seconds=30)
    dead = queue.fail(
        claimed.id,
        "poison",
        worker_id="w1",
        retryable=False,
    )
    assert dead.status == JobStatus.DEAD
    assert dead.attempts == 1
    assert queue.get(job.id).status == JobStatus.DEAD


def test_requeue_from_dlq(queue) -> None:
    job = queue.enqueue("echo", {"message": "x"}, max_attempts=1)
    claimed = queue.claim("w1", lease_seconds=30)
    queue.fail(claimed.id, "dead", worker_id="w1", retryable=True)
    revived = queue.requeue_from_dlq(job.id)
    assert revived.status == JobStatus.QUEUED
    assert revived.attempts == 0
    again = queue.claim("w2", lease_seconds=30)
    assert again.id == job.id


def test_worker_transient_then_success(memory_queue, settings: Settings) -> None:
    settings.backoff_base_seconds = 0.0
    settings.lease_seconds = 30.0
    worker = Worker(memory_queue, settings, handler=JobHandler(), worker_id="w")
    job = memory_queue.enqueue(
        "transient",
        {"succeed_on_attempt": 2},
        max_attempts=3,
    )
    first = worker.run_once()
    assert first is not None
    mid = memory_queue.get(job.id)
    assert mid is not None
    assert mid.status == JobStatus.QUEUED
    assert mid.attempts == 1
    second = worker.run_once()
    assert second is not None
    done = memory_queue.get(job.id)
    assert done is not None
    assert done.status == JobStatus.COMPLETED
    assert done.attempts == 2


def test_worker_poison_goes_to_dlq(memory_queue, settings: Settings) -> None:
    worker = Worker(memory_queue, settings, handler=JobHandler(), worker_id="w")
    job = memory_queue.enqueue("poison", {}, max_attempts=3)
    worker.run_once()
    dead = memory_queue.get(job.id)
    assert dead is not None
    assert dead.status == JobStatus.DEAD
    assert dead.attempts == 1
    assert memory_queue.list_dlq()[0].id == job.id


def test_backoff_grows_and_caps() -> None:
    assert retry_delay_seconds(1, base=1.0, cap=10.0, jitter=0.0) == 1.0
    assert retry_delay_seconds(2, base=1.0, cap=10.0, jitter=0.0) == 2.0
    assert retry_delay_seconds(3, base=1.0, cap=10.0, jitter=0.0) == 4.0
    assert retry_delay_seconds(8, base=1.0, cap=10.0, jitter=0.0) == 10.0
