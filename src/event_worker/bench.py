from __future__ import annotations

import statistics
import time

from event_worker.config import Settings
from event_worker.handler import JobHandler
from event_worker.queues.memory import InMemoryQueue
from event_worker.worker import Worker


def _pct(samples: list[float], p: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    idx = min(len(ordered) - 1, max(0, int(round((p / 100) * (len(ordered) - 1)))))
    return ordered[idx]


def run_bench(n: int = 200) -> dict[str, object]:
    """Measure enqueue + claim/ack on the in-memory backend.

    These numbers are **local process timings**, not Kafka-at-scale
    throughput and not an SLO. They exist so the README can quote a
    labeled mock, not a marketing number.
    """
    queue = InMemoryQueue()
    settings = Settings(
        queue_backend="memory",
        lease_seconds=5.0,
        backoff_base_seconds=0.0,
        backoff_jitter=0.0,
        poll_interval=0.0,
    )
    worker = Worker(queue, settings, handler=JobHandler(), worker_id="bench")

    enqueue_ms: list[float] = []
    handle_ms: list[float] = []

    for i in range(n):
        t0 = time.perf_counter()
        queue.enqueue("echo", {"message": f"n={i}"})
        enqueue_ms.append((time.perf_counter() - t0) * 1000)

    t_loop = time.perf_counter()
    handled = 0
    for _ in range(n):
        t0 = time.perf_counter()
        job = worker.run_once()
        handle_ms.append((time.perf_counter() - t0) * 1000)
        if job is None:
            break
        handled += 1
    loop_ms = (time.perf_counter() - t_loop) * 1000
    stats = queue.stats()

    return {
        "label": "local in-memory mock timings — not a production SLO",
        "n_enqueued": n,
        "n_handled": handled,
        "completed": stats.completed,
        "enqueue_ms": {
            "p50": round(_pct(enqueue_ms, 50), 3),
            "p99": round(_pct(enqueue_ms, 99), 3),
            "mean": round(statistics.fmean(enqueue_ms), 3) if enqueue_ms else 0.0,
        },
        "claim_handle_ack_ms": {
            "p50": round(_pct(handle_ms, 50), 3),
            "p99": round(_pct(handle_ms, 99), 3),
            "mean": round(statistics.fmean(handle_ms), 3) if handle_ms else 0.0,
        },
        "full_loop_ms": round(loop_ms, 3),
    }
