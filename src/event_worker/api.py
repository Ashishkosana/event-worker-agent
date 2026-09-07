from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from event_worker import __version__
from event_worker.config import Settings, load_settings
from event_worker.errors import JobNotFoundError, JobStateError
from event_worker.models import Job, QueueStats
from event_worker.queues import build_queue
from event_worker.queues.base import JobQueue

app = FastAPI(
    title="event-worker-agent",
    version=__version__,
    description="Enqueue jobs for the lease-claiming agent worker.",
)


class EnqueueRequest(BaseModel):
    kind: str
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=3, ge=1, le=20)
    job_id: str | None = None
    idempotency_key: str | None = None


def get_settings() -> Settings:
    return load_settings()


_queue: JobQueue | None = None


def get_queue(settings: Settings = Depends(get_settings)) -> JobQueue:
    global _queue
    if _queue is None:
        _queue = build_queue(settings)
    return _queue


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.post("/v1/jobs", response_model=Job, status_code=201)
def enqueue_job(body: EnqueueRequest, queue: JobQueue = Depends(get_queue)) -> Job:
    return queue.enqueue(
        body.kind,
        body.payload,
        max_attempts=body.max_attempts,
        job_id=body.job_id,
        idempotency_key=body.idempotency_key,
    )


@app.get("/v1/jobs/{job_id}", response_model=Job)
def get_job(job_id: str, queue: JobQueue = Depends(get_queue)) -> Job:
    job = queue.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get("/v1/queue/stats", response_model=QueueStats)
def queue_stats(queue: JobQueue = Depends(get_queue)) -> QueueStats:
    return queue.stats()


@app.get("/v1/queue/dlq", response_model=list[Job])
def list_dlq(limit: int = 50, queue: JobQueue = Depends(get_queue)) -> list[Job]:
    return queue.list_dlq(limit=limit)


@app.post("/v1/queue/dlq/{job_id}/requeue", response_model=Job)
def requeue_dlq(job_id: str, queue: JobQueue = Depends(get_queue)) -> Job:
    try:
        return queue.requeue_from_dlq(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except JobStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
