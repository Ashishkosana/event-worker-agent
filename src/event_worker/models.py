from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(StrEnum):
    QUEUED = "queued"
    LEASED = "leased"
    DELAYED = "delayed"
    COMPLETED = "completed"
    DEAD = "dead"


class Job(BaseModel):
    id: str
    kind: str
    payload: dict[str, Any] = Field(default_factory=dict)
    status: JobStatus = JobStatus.QUEUED
    attempts: int = 0
    max_attempts: int = 3
    idempotency_key: str | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    available_at: datetime
    created_at: datetime
    updated_at: datetime
    last_error: str | None = None
    result: dict[str, Any] | None = None
    tool_trace: list[dict[str, Any]] = Field(default_factory=list)


class QueueStats(BaseModel):
    ready: int = 0
    leased: int = 0
    delayed: int = 0
    completed: int = 0
    dead: int = 0
    enqueued_total: int = 0
    claimed_total: int = 0
    acked_total: int = 0
    failed_total: int = 0
    reclaimed_total: int = 0


class Outcome(StrEnum):
    SUCCESS = "success"
    RETRYABLE = "retryable"
    TERMINAL = "terminal"


class ToolCall(BaseModel):
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class ToolResult(BaseModel):
    name: str
    ok: bool
    output: dict[str, Any] = Field(default_factory=dict)
    retryable: bool = False
    error: str | None = None

    def as_trace(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "ok": self.ok,
            "output": self.output,
            "retryable": self.retryable,
            "error": self.error,
        }


class HandlerDecision(BaseModel):
    outcome: Outcome
    result: dict[str, Any] | None = None
    error: str | None = None
    tool_trace: list[dict[str, Any]] = Field(default_factory=list)
