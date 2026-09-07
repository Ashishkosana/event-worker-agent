from __future__ import annotations

import logging
import os
import socket
import time
import uuid

from event_worker.backoff import retry_delay_seconds
from event_worker.config import Settings
from event_worker.errors import StaleLeaseError
from event_worker.handler import JobHandler
from event_worker.models import Job, Outcome
from event_worker.queues.base import JobQueue

log = logging.getLogger("event_worker.worker")


def default_worker_id(configured: str = "") -> str:
    if configured:
        return configured
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


class Worker:
    def __init__(
        self,
        queue: JobQueue,
        settings: Settings,
        handler: JobHandler | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.queue = queue
        self.settings = settings
        self.handler = handler or JobHandler()
        self.worker_id = worker_id or default_worker_id(settings.worker_id)

    def run_once(self) -> Job | None:
        self.queue.reclaim_expired()
        self.queue.promote_delayed()
        job = self.queue.claim(self.worker_id, lease_seconds=self.settings.lease_seconds)
        if job is None:
            return None
        log.info("claimed %s kind=%s attempt=%s", job.id, job.kind, job.attempts)
        decision = self.handler.handle(job)
        try:
            if decision.outcome == Outcome.SUCCESS:
                self.queue.ack(
                    job.id,
                    decision.result,
                    worker_id=self.worker_id,
                    tool_trace=decision.tool_trace,
                )
                log.info("acked %s", job.id)
            elif decision.outcome == Outcome.RETRYABLE:
                delay = retry_delay_seconds(
                    job.attempts,
                    base=self.settings.backoff_base_seconds,
                    cap=self.settings.backoff_cap_seconds,
                    jitter=self.settings.backoff_jitter,
                )
                updated = self.queue.fail(
                    job.id,
                    decision.error or "retryable tool failure",
                    worker_id=self.worker_id,
                    retryable=True,
                    delay_seconds=delay,
                    tool_trace=decision.tool_trace,
                )
                log.warning(
                    "retry %s attempt=%s/%s delay=%.3fs error=%s",
                    job.id,
                    updated.attempts,
                    updated.max_attempts,
                    delay,
                    decision.error,
                )
            else:
                self.queue.fail(
                    job.id,
                    decision.error or "terminal tool failure",
                    worker_id=self.worker_id,
                    retryable=False,
                    tool_trace=decision.tool_trace,
                )
                log.error("dead-lettered %s error=%s", job.id, decision.error)
        except StaleLeaseError as exc:
            log.warning("lost lease on %s: %s", job.id, exc)
        return job

    def run_forever(self) -> None:
        log.info("worker %s polling", self.worker_id)
        while True:
            job = self.run_once()
            if job is None:
                time.sleep(self.settings.poll_interval)
