from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from redis import Redis

from event_worker.clock import Clock, SystemClock
from event_worker.errors import JobNotFoundError, JobStateError, StaleLeaseError
from event_worker.models import Job, JobStatus, QueueStats


class RedisQueue:
    """Redis backend with the same claim / lease / DLQ contract as memory."""

    def __init__(
        self,
        client: Redis,
        *,
        clock: Clock | None = None,
        prefix: str = "ew",
        completed_keep: int = 100,
    ) -> None:
        self._r = client
        self._clock = clock or SystemClock()
        self._prefix = prefix
        self._completed_keep = completed_keep

    def _k(self, name: str) -> str:
        return f"{self._prefix}:{name}"

    def _job_key(self, job_id: str) -> str:
        return f"{self._prefix}:job:{job_id}"

    def _idemp_key(self, key: str) -> str:
        return f"{self._prefix}:idemp:{key}"

    def _save(self, job: Job) -> None:
        self._r.set(self._job_key(job.id), job.model_dump_json())

    def _load(self, job_id: str) -> Job | None:
        raw = self._r.get(self._job_key(job_id))
        if raw is None:
            return None
        return Job.model_validate_json(raw)

    def enqueue(
        self,
        kind: str,
        payload: dict[str, Any] | None = None,
        *,
        max_attempts: int = 3,
        job_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> Job:
        if idempotency_key:
            existing_id = self._r.get(self._idemp_key(idempotency_key))
            if existing_id:
                job = self._load(existing_id)
                if job is not None:
                    return job
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
        self._save(job)
        if idempotency_key:
            self._r.set(self._idemp_key(idempotency_key), job.id)
        self._r.rpush(self._k("ready"), job.id)
        self._r.hincrby(self._k("stats"), "enqueued_total", 1)
        return job

    def claim(self, worker_id: str, *, lease_seconds: float = 30.0) -> Job | None:
        now = self._clock.now()
        lease_until = now + timedelta(seconds=lease_seconds)
        self.reclaim_expired()
        self.promote_delayed()
        job_id = self._r.lpop(self._k("ready"))
        if not job_id:
            return None
        self._r.zadd(self._k("inflight"), {job_id: lease_until.timestamp()})
        job = self._load(job_id)
        if job is None:
            self._r.zrem(self._k("inflight"), job_id)
            return None
        # Lua reclaim does not rewrite job JSON; heal leased→queued leftovers.
        if job.status == JobStatus.LEASED and job.lease_owner != worker_id:
            job.last_error = job.last_error or "reclaimed after lease expiry"
        job.attempts += 1
        job.status = JobStatus.LEASED
        job.lease_owner = worker_id
        job.lease_expires_at = lease_until
        job.updated_at = now
        self._save(job)
        self._r.hincrby(self._k("stats"), "claimed_total", 1)
        return job

    def ack(
        self,
        job_id: str,
        result: dict[str, Any] | None,
        *,
        worker_id: str,
        tool_trace: list[dict[str, Any]] | None = None,
    ) -> Job:
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
        self._save(job)
        self._r.zrem(self._k("inflight"), job_id)
        self._r.lpush(self._k("completed"), job_id)
        self._r.ltrim(self._k("completed"), 0, self._completed_keep - 1)
        self._r.hincrby(self._k("stats"), "acked_total", 1)
        return job

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
        job = self._require_lease(job_id, worker_id)
        now = self._clock.now()
        job.last_error = error
        if tool_trace is not None:
            job.tool_trace = tool_trace
        job.lease_owner = None
        job.lease_expires_at = None
        job.updated_at = now
        self._r.zrem(self._k("inflight"), job_id)
        self._r.hincrby(self._k("stats"), "failed_total", 1)
        if not retryable or job.attempts >= job.max_attempts:
            job.status = JobStatus.DEAD
            self._save(job)
            self._r.rpush(self._k("dlq"), job_id)
        elif delay_seconds <= 0:
            job.status = JobStatus.QUEUED
            job.available_at = now
            self._save(job)
            self._r.rpush(self._k("ready"), job_id)
        else:
            job.status = JobStatus.DELAYED
            job.available_at = now + timedelta(seconds=delay_seconds)
            self._save(job)
            self._r.zadd(self._k("delayed"), {job_id: job.available_at.timestamp()})
        return job

    def get(self, job_id: str) -> Job | None:
        self.reclaim_expired()
        self.promote_delayed()
        return self._load(job_id)

    def stats(self) -> QueueStats:
        self.reclaim_expired()
        self.promote_delayed()
        counters = self._r.hgetall(self._k("stats"))
        return QueueStats(
            ready=int(self._r.llen(self._k("ready")) or 0),
            leased=int(self._r.zcard(self._k("inflight")) or 0),
            delayed=int(self._r.zcard(self._k("delayed")) or 0),
            completed=int(self._r.llen(self._k("completed")) or 0),
            dead=int(self._r.llen(self._k("dlq")) or 0),
            enqueued_total=int(counters.get("enqueued_total", 0)),
            claimed_total=int(counters.get("claimed_total", 0)),
            acked_total=int(counters.get("acked_total", 0)),
            failed_total=int(counters.get("failed_total", 0)),
            reclaimed_total=int(counters.get("reclaimed_total", 0)),
        )

    def list_dlq(self, limit: int = 50) -> list[Job]:
        ids = self._r.lrange(self._k("dlq"), -limit, -1) or []
        jobs = []
        for job_id in ids:
            job = self._load(job_id)
            if job is not None:
                jobs.append(job)
        return jobs

    def requeue_from_dlq(self, job_id: str) -> Job:
        job = self._load(job_id)
        if job is None:
            raise JobNotFoundError(job_id)
        removed = self._r.lrem(self._k("dlq"), 1, job_id)
        if not removed or job.status != JobStatus.DEAD:
            raise JobStateError(job_id, "job is not on the DLQ")
        now = self._clock.now()
        job.status = JobStatus.QUEUED
        job.attempts = 0
        job.last_error = None
        job.available_at = now
        job.updated_at = now
        self._save(job)
        self._r.rpush(self._k("ready"), job_id)
        return job

    def reclaim_expired(self) -> int:
        now = self._clock.now()
        expired = self._r.zrangebyscore(self._k("inflight"), "-inf", now.timestamp())
        moved = 0
        for job_id in expired or []:
            if self._r.zrem(self._k("inflight"), job_id):
                job = self._load(job_id)
                if job is not None:
                    job.status = JobStatus.QUEUED
                    job.lease_owner = None
                    job.lease_expires_at = None
                    job.updated_at = now
                    self._save(job)
                self._r.rpush(self._k("ready"), job_id)
                self._r.hincrby(self._k("stats"), "reclaimed_total", 1)
                moved += 1
        return moved

    def promote_delayed(self) -> int:
        now = self._clock.now()
        due = self._r.zrangebyscore(self._k("delayed"), "-inf", now.timestamp())
        moved = 0
        for job_id in due or []:
            if self._r.zrem(self._k("delayed"), job_id):
                job = self._load(job_id)
                if job is not None:
                    job.status = JobStatus.QUEUED
                    job.updated_at = now
                    self._save(job)
                self._r.rpush(self._k("ready"), job_id)
                moved += 1
        return moved

    def _require_lease(self, job_id: str, worker_id: str) -> Job:
        job = self._load(job_id)
        if job is None:
            raise JobNotFoundError(job_id)
        score = self._r.zscore(self._k("inflight"), job_id)
        if score is None:
            raise StaleLeaseError(job_id, "job is not leased")
        if job.lease_owner != worker_id:
            raise StaleLeaseError(job_id, "not the lease owner")
        if job.lease_expires_at is not None and job.lease_expires_at <= self._clock.now():
            raise StaleLeaseError(job_id, "lease expired")
        return job
