from __future__ import annotations

from datetime import timedelta

from event_worker.clock import FrozenClock
from event_worker.config import Settings
from event_worker.handler import JobHandler
from event_worker.models import JobStatus
from event_worker.worker import Worker


def _worker(queue, settings: Settings) -> Worker:
    return Worker(queue, settings, handler=JobHandler(), worker_id="w")


def test_worker_echo_claim_tools_ack(memory_queue, settings: Settings) -> None:
    worker = _worker(memory_queue, settings)
    job = memory_queue.enqueue("echo", {"message": "ping"})
    handled = worker.run_once()
    assert handled is not None
    done = memory_queue.get(job.id)
    assert done is not None
    assert done.status == JobStatus.COMPLETED
    assert done.attempts == 1
    assert done.result is not None
    assert done.result["echo"]["echo"] == "ping"
    assert [step["name"] for step in done.tool_trace] == ["echo", "record_result"]
    assert memory_queue.stats().acked_total == 1
    assert memory_queue.claim("other") is None


def test_worker_classify_ack_persists_intent_trace(memory_queue, settings: Settings) -> None:
    worker = _worker(memory_queue, settings)
    job = memory_queue.enqueue("classify", {"text": "please refund order 42"})
    worker.run_once()
    done = memory_queue.get(job.id)
    assert done is not None
    assert done.status == JobStatus.COMPLETED
    assert done.result["classify_intent"]["intent"] == "refund"
    assert [step["name"] for step in done.tool_trace] == [
        "normalize_event",
        "classify_intent",
        "record_result",
    ]


def test_worker_notify_retry_backoff_then_dlq(
    memory_queue,
    settings: Settings,
    clock: FrozenClock,
) -> None:
    settings.backoff_base_seconds = 4.0
    settings.backoff_cap_seconds = 60.0
    settings.backoff_jitter = 0.0
    worker = _worker(memory_queue, settings)
    job = memory_queue.enqueue(
        "notify",
        {"url": "https://hook.fail.invalid", "text": "hi"},
        max_attempts=3,
    )

    worker.run_once()
    first = memory_queue.get(job.id)
    assert first is not None
    assert first.status == JobStatus.DELAYED
    assert first.attempts == 1
    assert first.last_error is not None
    assert "downstream rejected" in first.last_error
    assert any(step["name"] == "call_downstream" and not step["ok"] for step in first.tool_trace)
    assert first.available_at == clock.now() + timedelta(seconds=4.0)
    assert worker.run_once() is None

    clock.advance(4.0)
    worker.run_once()
    second = memory_queue.get(job.id)
    assert second is not None
    assert second.status == JobStatus.DELAYED
    assert second.attempts == 2
    assert second.available_at == clock.now() + timedelta(seconds=8.0)

    clock.advance(8.0)
    worker.run_once()
    dead = memory_queue.get(job.id)
    assert dead is not None
    assert dead.status == JobStatus.DEAD
    assert dead.attempts == 3
    assert memory_queue.list_dlq()[0].id == job.id
    assert worker.run_once() is None


def test_worker_unknown_kind_goes_to_dlq(memory_queue, settings: Settings) -> None:
    worker = _worker(memory_queue, settings)
    job = memory_queue.enqueue("not-a-kind", {}, max_attempts=5)
    worker.run_once()
    dead = memory_queue.get(job.id)
    assert dead is not None
    assert dead.status == JobStatus.DEAD
    assert dead.attempts == 1
    assert "unknown job kind" in (dead.last_error or "")
    assert memory_queue.list_dlq()[0].id == job.id


def test_worker_ticket_refund_acks(memory_queue, settings: Settings) -> None:
    worker = _worker(memory_queue, settings)
    job = memory_queue.enqueue("ticket", {"text": "please refund order 7"})
    worker.run_once()
    done = memory_queue.get(job.id)
    assert done is not None
    assert done.status == JobStatus.COMPLETED
    assert [step["name"] for step in done.tool_trace] == [
        "normalize_event",
        "classify_intent",
        "call_downstream",
        "record_result",
    ]
