from __future__ import annotations

from typing import Any, Protocol

from event_worker.models import Job, QueueStats


class JobQueue(Protocol):
    def enqueue(
        self,
        kind: str,
        payload: dict[str, Any] | None = None,
        *,
        max_attempts: int = 3,
        job_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> Job: ...

    def claim(self, worker_id: str, *, lease_seconds: float = 30.0) -> Job | None: ...

    def ack(
        self,
        job_id: str,
        result: dict[str, Any] | None,
        *,
        worker_id: str,
        tool_trace: list[dict[str, Any]] | None = None,
    ) -> Job: ...

    def fail(
        self,
        job_id: str,
        error: str,
        *,
        worker_id: str,
        retryable: bool,
        delay_seconds: float = 0.0,
        tool_trace: list[dict[str, Any]] | None = None,
    ) -> Job: ...

    def get(self, job_id: str) -> Job | None: ...

    def stats(self) -> QueueStats: ...

    def list_dlq(self, limit: int = 50) -> list[Job]: ...

    def requeue_from_dlq(self, job_id: str) -> Job: ...

    def reclaim_expired(self) -> int: ...

    def promote_delayed(self) -> int: ...
