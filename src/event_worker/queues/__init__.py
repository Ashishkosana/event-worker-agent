from __future__ import annotations

from event_worker.clock import Clock, SystemClock
from event_worker.config import Settings
from event_worker.queues.base import JobQueue
from event_worker.queues.memory import InMemoryQueue
from event_worker.queues.redis_queue import RedisQueue


def build_queue(settings: Settings, clock: Clock | None = None) -> JobQueue:
    clock = clock or SystemClock()
    backend = settings.queue_backend.lower()
    if backend == "memory":
        return InMemoryQueue(
            clock=clock,
            completed_keep=settings.completed_keep,
        )
    if backend == "redis":
        import redis

        client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
        return RedisQueue(
            client,
            clock=clock,
            prefix=settings.key_prefix,
            completed_keep=settings.completed_keep,
        )
    raise ValueError(f"unknown queue backend: {settings.queue_backend}")


__all__ = ["JobQueue", "InMemoryQueue", "RedisQueue", "build_queue"]
