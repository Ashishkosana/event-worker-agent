from __future__ import annotations

import heapq
import threading
import uuid
from collections import deque
from datetime import timedelta
from typing import Any

from event_worker.clock import Clock, SystemClock
from event_worker.errors import JobNotFoundError, JobStateError, StaleLeaseError
from event_worker.models import Job, JobStatus, QueueStats


class InMemoryQueue:
    """SQS-like in-memory queue: ready / leased / delayed / DLQ.

    Used by tests and the ``memory`` backend. Same claim/lease/retry
    contract as :class:`RedisQueue`.
    """

    def __init__(self, clock: Clock | None = None, completed_keep: int = 100) -> None:
        self._clock = clock or SystemClock()
        self._completed_keep = completed_keep
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}
        self._ready: deque[str] = deque()
        self._delayed: list[tuple[float, int, str]] = []
        self._seq = 0
        self._inflight: dict[str, str] = {}  # job_id -> worker_id
        self._dlq: deque[str] = deque()
        self._completed: deque[str] = deque()
        self._idemp: dict[str, str] = {}
        self._enqueued_total = 0
        self._claimed_total = 0
        self._acked_total = 0
        self._failed_total = 0
        self._reclaimed_total = 0

    def enqueue(
        self,
        kind: str,
        payload: dict[str, Any] | None = None,
        *,
        max_attempts: int = 3,
        job_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> Job:
        with self._lock:
            if idempotency_key and idempotency_key in self._idemp:
                return self._jobs[self._idemp[idempotency_key]].model_copy(deep=True)
            now = self._clock.now()
            job = Job(
                id=job_id or str(uuid.uuid4()),
                kind=kind,
                payload=payload or {},
                status=JobStatus.QUEUED,
                max_attempts=max_attempts,
                idempotency_key=idempotency_key,
                available_at=now,
                created_at=now,
                updated_at=now,
            )
            self._jobs[job.id] = job
            self._ready.append(job.id)
            if idempotency_key:
                self._idemp[idempotency_key] = job.id
            self._enqueued_total += 1
            return job.model_copy(deep=True)

    def claim(self, worker_id: str, *, lease_seconds: float = 30.0) -> Job | None:
        with self._lock:
            self._reclaim_expired_unlocked()
            self._promote_delayed_unlocked()
            if not self._ready:
                return None
            job_id = self._ready.popleft()
            job = self._jobs[job_id]
            now = self._clock.now()
            job.attempts += 1
            job.status = JobStatus.LEASED
            job.lease_owner = worker_id
            job.lease_expires_at = now + timedelta(seconds=lease_seconds)
            job.updated_at = now
            self._inflight[job_id] = worker_id
            self._claimed_total += 1
            return job.model_copy(deep=True)

    def ack(
        self,
        job_id: str,
        result: dict[str, Any] | None,
        *,
        worker_id: str,
        tool_trace: list[dict[str, Any]] | None = None,
    ) -> Job:
        with self._lock:
            job = self._require_lease(job_id, worker_id)
            now = self._clock.now()
            job.status = JobStatus.COMPLETED
            job.result = result
            if tool_trace is not None:
                job.tool_trace = tool_trace
            job.lease_owner = None
            job.lease_expires_at = None
            job.updated_at = now
            job.last_error = None
            self._inflight.pop(job_id, None)
            self._completed.append(job_id)
            while len(self._completed) > self._completed_keep:
                self._completed.popleft()
            self._acked_total += 1
            return job.model_copy(deep=True)

    def fail(
        self,
        job_id: str,
        error: str,
        *,
        worker_id: str,
        retryable: bool,
        delay_seconds: float = 0.0,
        tool_trace: list[dict[str, Any]] | None = None,
    ) -> Job:
        with self._lock:
            job = self._require_lease(job_id, worker_id)
            now = self._clock.now()
            job.last_error = error
            if tool_trace is not None:
                job.tool_trace = tool_trace
            job.lease_owner = None
            job.lease_expires_at = None
            job.updated_at = now
            self._inflight.pop(job_id, None)
            self._failed_total += 1
            if not retryable or job.attempts >= job.max_attempts:
                job.status = JobStatus.DEAD
                self._dlq.append(job_id)
            elif delay_seconds <= 0:
                job.status = JobStatus.QUEUED
                job.available_at = now
                self._ready.append(job_id)
            else:
                job.status = JobStatus.DELAYED
                job.available_at = now + timedelta(seconds=delay_seconds)
                self._seq += 1
                heapq.heappush(self._delayed, (job.available_at.timestamp(), self._seq, job_id))
            return job.model_copy(deep=True)

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            self._reclaim_expired_unlocked()
            self._promote_delayed_unlocked()
            job = self._jobs.get(job_id)
            return job.model_copy(deep=True) if job else None

    def stats(self) -> QueueStats:
        with self._lock:
            self._reclaim_expired_unlocked()
            self._promote_delayed_unlocked()
            return QueueStats(
                ready=len(self._ready),
                leased=len(self._inflight),
                delayed=len(self._delayed),
                completed=len(self._completed),
                dead=len(self._dlq),
                enqueued_total=self._enqueued_total,
                claimed_total=self._claimed_total,
                acked_total=self._acked_total,
                failed_total=self._failed_total,
                reclaimed_total=self._reclaimed_total,
            )

    def list_dlq(self, limit: int = 50) -> list[Job]:
        with self._lock:
            ids = list(self._dlq)[-limit:]
            return [self._jobs[i].model_copy(deep=True) for i in ids if i in self._jobs]

    def requeue_from_dlq(self, job_id: str) -> Job:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFoundError(job_id)
            if job.status != JobStatus.DEAD or job_id not in self._dlq:
                raise JobStateError(job_id, "job is not on the DLQ")
            self._dlq.remove(job_id)
            now = self._clock.now()
            job.status = JobStatus.QUEUED
            job.attempts = 0
            job.last_error = None
            job.available_at = now
            job.updated_at = now
            self._ready.append(job_id)
            return job.model_copy(deep=True)

    def reclaim_expired(self) -> int:
        with self._lock:
            return self._reclaim_expired_unlocked()

    def promote_delayed(self) -> int:
        with self._lock:
            return self._promote_delayed_unlocked()

    def _require_lease(self, job_id: str, worker_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise JobNotFoundError(job_id)
        owner = self._inflight.get(job_id)
        if owner is None:
            raise StaleLeaseError(job_id, "job is not leased")
        if owner != worker_id:
            raise StaleLeaseError(job_id, "not the lease owner")
        if job.lease_expires_at is not None and job.lease_expires_at <= self._clock.now():
            raise StaleLeaseError(job_id, "lease expired")
        return job

    def _reclaim_expired_unlocked(self) -> int:
        now = self._clock.now()
        expired = [
            job_id
            for job_id, owner in list(self._inflight.items())
            if (job := self._jobs.get(job_id)) is not None
            and job.lease_expires_at is not None
            and job.lease_expires_at <= now
        ]
        for job_id in expired:
            job = self._jobs[job_id]
            job.status = JobStatus.QUEUED
            job.lease_owner = None
            job.lease_expires_at = None
            job.updated_at = now
            self._inflight.pop(job_id, None)
            self._ready.append(job_id)
            self._reclaimed_total += 1
        return len(expired)

    def _promote_delayed_unlocked(self) -> int:
        now_ts = self._clock.now().timestamp()
        moved = 0
        while self._delayed and self._delayed[0][0] <= now_ts:
            _, _, job_id = heapq.heappop(self._delayed)
            job = self._jobs.get(job_id)
            if job is None or job.status != JobStatus.DELAYED:
                continue
            job.status = JobStatus.QUEUED
            job.updated_at = self._clock.now()
            self._ready.append(job_id)
            moved += 1
        return moved
