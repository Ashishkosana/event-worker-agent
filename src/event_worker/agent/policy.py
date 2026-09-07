from __future__ import annotations

from typing import Any

from event_worker.models import HandlerDecision, Job, Outcome, ToolCall, ToolResult

# kind → ordered tool names. This is the deterministic planner table.
KIND_PLANS: dict[str, list[str]] = {
    "echo": ["echo", "record_result"],
    "normalize": ["normalize_event", "record_result"],
    "classify": ["normalize_event", "classify_intent", "record_result"],
    "notify": ["normalize_event", "call_downstream", "record_result"],
    "poison": ["raise_poison"],
    "transient": ["flaky_downstream", "record_result"],
}


class AgentPolicy:
    """Job-handler policy: which tools to call, and how to classify the result.

    This is the agent brain for Milestone 1. It is **not** an LLM.

    Why a deterministic policy first:
    - Retry / DLQ behavior must be testable without a model.
    - Tool traces stay replayable.
    - A later LLM planner can emit ``ToolCall`` lists; ``classify()``
      remains the authority on success vs retryable vs terminal.

    Interview version: "the worker claims a job, the policy selects tools,
    tools run, the policy decides ack / backoff / DLQ."
    """

    def __init__(self, plans: dict[str, list[str]] | None = None) -> None:
        self.plans = plans or KIND_PLANS

    def known_kinds(self) -> list[str]:
        return sorted(self.plans)

    def select_tools(self, job: Job) -> list[ToolCall]:
        steps = self.plans.get(job.kind)
        if not steps:
            return []
        calls: list[ToolCall] = []
        context: dict[str, Any] = {}
        for name in steps:
            calls.append(self._build_call(name, job, context))
        return calls

    def classify(self, job: Job, results: list[ToolResult]) -> HandlerDecision:
        trace = [r.as_trace() for r in results]
        if job.kind not in self.plans:
            return HandlerDecision(
                outcome=Outcome.TERMINAL,
                error=f"unknown job kind: {job.kind}",
                tool_trace=trace,
            )
        if not results:
            return HandlerDecision(
                outcome=Outcome.TERMINAL,
                error="policy produced no tool results",
                tool_trace=trace,
            )

        failed = next((r for r in results if not r.ok), None)
        if failed is None:
            merged: dict[str, Any] = {"kind": job.kind, "job_id": job.id}
            for result in results:
                merged[result.name] = result.output
            return HandlerDecision(outcome=Outcome.SUCCESS, result=merged, tool_trace=trace)

        if failed.retryable:
            return HandlerDecision(
                outcome=Outcome.RETRYABLE,
                error=failed.error or f"{failed.name} failed",
                tool_trace=trace,
            )
        return HandlerDecision(
            outcome=Outcome.TERMINAL,
            error=failed.error or f"{failed.name} failed terminally",
            tool_trace=trace,
        )

    def _build_call(self, name: str, job: Job, _context: dict[str, Any]) -> ToolCall:
        payload = job.payload
        if name == "echo":
            return ToolCall(name=name, arguments={"message": payload.get("message", payload)})
        if name == "normalize_event":
            return ToolCall(name=name, arguments=payload)
        if name == "classify_intent":
            text = payload.get("text") or payload.get("body") or ""
            return ToolCall(name=name, arguments={"text": text, "body": text})
        if name == "call_downstream":
            return ToolCall(
                name=name,
                arguments={"url": payload.get("url", "https://mock.local/hook")},
            )
        if name == "record_result":
            return ToolCall(name=name, arguments={"kind": job.kind})
        if name == "flaky_downstream":
            return ToolCall(
                name=name,
                arguments={"succeed_on_attempt": payload.get("succeed_on_attempt")},
            )
        return ToolCall(name=name, arguments=dict(payload))
