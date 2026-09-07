from __future__ import annotations

import pytest
from fakeredis import FakeRedis

from event_worker.clock import FrozenClock
from event_worker.config import Settings
from event_worker.queues.memory import InMemoryQueue
from event_worker.queues.redis_queue import RedisQueue


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock()


@pytest.fixture(params=["memory", "fakeredis"])
def queue(request, clock: FrozenClock):
    if request.param == "memory":
        return InMemoryQueue(clock=clock)
    client = FakeRedis(decode_responses=True)
    return RedisQueue(client, clock=clock, prefix=f"ew-{request.param}")


@pytest.fixture
def memory_queue(clock: FrozenClock) -> InMemoryQueue:
    return InMemoryQueue(clock=clock)


@pytest.fixture
def settings() -> Settings:
    return Settings(
        queue_backend="memory",
        lease_seconds=30.0,
        max_attempts=3,
        backoff_base_seconds=1.0,
        backoff_cap_seconds=60.0,
        backoff_jitter=0.0,
        poll_interval=0.0,
    )
