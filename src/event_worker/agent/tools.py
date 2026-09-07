from __future__ import annotations

from collections.abc import Callable
from typing import Any

from event_worker.models import Job, ToolCall, ToolResult

ToolFn = Callable[[dict[str, Any], Job], ToolResult]


class MockToolRuntime:
    """In-process mock tools. No network. Deterministic for tests.

    This is the Milestone 1 tool plane. Swap individual handlers or the
    whole runtime when you wire real HTTP / LLM tools.
    """

    def __init__(self, extras: dict[str, ToolFn] | None = None) -> None:
        self._tools: dict[str, ToolFn] = {
            "echo": self._echo,
            "normalize_event": self._normalize_event,
            "classify_intent": self._classify_intent,
            "call_downstream": self._call_downstream,
            "record_result": self._record_result,
            "raise_poison": self._raise_poison,
            "flaky_downstream": self._flaky_downstream,
        }
        if extras:
            self._tools.update(extras)

    def names(self) -> list[str]:
        return sorted(self._tools)

    def invoke(self, call: ToolCall, job: Job) -> ToolResult:
        fn = self._tools.get(call.name)
        if fn is None:
            return ToolResult(
                name=call.name,
                ok=False,
                retryable=False,
                error=f"unknown tool: {call.name}",
            )
        try:
            return fn(call.arguments, job)
        except Exception as exc:  # noqa: BLE001 — tools must not kill the worker
            return ToolResult(
                name=call.name,
                ok=False,
                retryable=True,
                error=f"tool crashed: {exc}",
            )

    def _echo(self, arguments: dict[str, Any], job: Job) -> ToolResult:
        return ToolResult(
            name="echo",
            ok=True,
            output={"echo": arguments.get("message", job.payload)},
        )

    def _normalize_event(self, arguments: dict[str, Any], job: Job) -> ToolResult:
        payload = {**job.payload, **arguments}
        body = payload.get("text") or payload.get("body") or payload
        return ToolResult(
            name="normalize_event",
            ok=True,
            output={
                "event_id": payload.get("event_id") or job.id,
                "type": payload.get("type") or job.kind,
                "body": body,
            },
        )

    def _classify_intent(self, arguments: dict[str, Any], _job: Job) -> ToolResult:
        text = str(arguments.get("text") or arguments.get("body") or "").lower()
        if "refund" in text:
            intent = "refund"
        elif "signup" in text or "welcome" in text:
            intent = "signup"
        elif "alert" in text or "page" in text:
            intent = "alert"
        else:
            intent = "unknown"
        return ToolResult(
            name="classify_intent",
            ok=True,
            output={"intent": intent, "confidence": 0.72 if intent != "unknown" else 0.2},
        )

    def _call_downstream(self, arguments: dict[str, Any], job: Job) -> ToolResult:
        url = str(arguments.get("url") or job.payload.get("url") or "https://mock.local/hook")
        if url.endswith("fail.invalid") or job.payload.get("fail") is True:
            return ToolResult(
                name="call_downstream",
                ok=False,
                retryable=bool(job.payload.get("retryable", True)),
                error=f"downstream rejected {url}",
            )
        return ToolResult(
            name="call_downstream",
            ok=True,
            output={"status": 200, "url": url, "mocked": True},
        )

    def _record_result(self, arguments: dict[str, Any], job: Job) -> ToolResult:
        return ToolResult(
            name="record_result",
            ok=True,
            output={"recorded": True, "job_id": job.id, "fields": arguments},
        )

    def _raise_poison(self, _arguments: dict[str, Any], job: Job) -> ToolResult:
        return ToolResult(
            name="raise_poison",
            ok=False,
            retryable=False,
            error=f"poison payload for job {job.id}",
        )

    def _flaky_downstream(self, arguments: dict[str, Any], job: Job) -> ToolResult:
        succeed_on = int(
            arguments.get("succeed_on_attempt")
            or job.payload.get("succeed_on_attempt")
            or 10**9
        )
        if job.attempts >= succeed_on:
            return ToolResult(
                name="flaky_downstream",
                ok=True,
                output={"status": 200, "attempt": job.attempts, "mocked": True},
            )
        return ToolResult(
            name="flaky_downstream",
            ok=False,
            retryable=True,
            error=f"transient failure on attempt {job.attempts}",
        )
