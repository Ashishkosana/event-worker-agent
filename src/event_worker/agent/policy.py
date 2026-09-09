from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from event_worker.models import HandlerDecision, Job, Outcome, ToolCall, ToolResult

# kind → ordered tool names. Deterministic planner table (no hosted LLM).
KIND_PLANS: dict[str, list[str]] = {
    "echo": ["echo", "record_result"],
    "normalize": ["normalize_event", "record_result"],
    "classify": ["normalize_event", "classify_intent", "record_result"],
    "notify": ["normalize_event", "call_downstream", "record_result"],
    "ticket": ["normalize_event", "classify_intent", "record_result"],
    "poison": ["raise_poison"],
    "transient": ["flaky_downstream", "record_result"],
}

# After classify_intent, ticket jobs call a mock downstream for these intents.
BRANCH_DOWNSTREAM_INTENTS = frozenset({"refund", "alert"})

# classify() never retries these, even if a tool sets retryable=True.
ALWAYS_TERMINAL_TOOLS = frozenset({"raise_poison"})

DEFAULT_MAX_STEPS = 16


class AgentPolicy:
    """Job-handler policy: which tools to call, and how to classify the result.

    This is the agent brain. It is **not** an LLM. CI stays offline.

    Why a deterministic policy first:
    - Retry / DLQ behavior must be testable without a model.
    - Tool traces stay replayable.
    - A later LLM planner can emit ``ToolCall`` lists; ``classify()``
      remains the authority on success vs retryable vs terminal.

    Interview version: "the worker claims a job, the policy selects tools,
    tools run, the policy decides ack / backoff / DLQ."
    """

    def __init__(
        self,
        plans: dict[str, list[str]] | None = None,
        max_steps: int = DEFAULT_MAX_STEPS,
    ) -> None:
        self.plans = plans or dict(KIND_PLANS)
        self.max_steps = max_steps

    def known_kinds(self) -> list[str]:
        return sorted(self.plans)

    def preview(self, kind: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Serialize the initial tool plan. Used by the CLI; no queue I/O."""
        job = _preview_job(kind, payload or {})
        calls = self.select_tools(job)
        return {
            "kind": kind,
            "planner": "AgentPolicy",
            "hosted_llm": False,
            "tools": [call.model_dump() for call in calls],
            "note": (
                "Initial plan from job.kind. ticket may insert call_downstream "
                "after classify_intent when the intent is refund or alert."
            ),
        }

    def select_tools(
        self,
        job: Job,
        results: list[ToolResult] | None = None,
    ) -> list[ToolCall]:
        """Return remaining tool calls, with arguments bound from prior results.

        Called with ``results=None`` (or ``[]``) for the initial plan. The
        handler invokes the first call, then asks again so kinds like
        ``ticket`` can branch after ``classify_intent``.
        """
        prior = list(results or [])
        remaining = self._remaining_names(job, prior)
        return [self._build_call(name, job, prior) for name in remaining]

    def classify(self, job: Job, results: list[ToolResult]) -> HandlerDecision:
        """Decide success / retryable / terminal. Sole retry/DLQ authority."""
        trace = [result.as_trace() for result in results]
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

        unknown = next((result for result in results if _unknown_tool_error(result)), None)
        if unknown is not None:
            return HandlerDecision(
                outcome=Outcome.TERMINAL,
                error=unknown.error or f"unknown tool: {unknown.name}",
                tool_trace=trace,
            )

        failed = next((result for result in results if not result.ok), None)
        if failed is None:
            merged: dict[str, Any] = {"kind": job.kind, "job_id": job.id}
            for result in results:
                merged[result.name] = result.output
            return HandlerDecision(outcome=Outcome.SUCCESS, result=merged, tool_trace=trace)

        if failed.name in ALWAYS_TERMINAL_TOOLS or not failed.retryable:
            return HandlerDecision(
                outcome=Outcome.TERMINAL,
                error=failed.error or f"{failed.name} failed terminally",
                tool_trace=trace,
            )
        return HandlerDecision(
            outcome=Outcome.RETRYABLE,
            error=failed.error or f"{failed.name} failed",
            tool_trace=trace,
        )

    def _remaining_names(self, job: Job, results: list[ToolResult]) -> list[str]:
        plan = self._full_plan(job, results)
        start = 0
        for result in results:
            if start < len(plan) and plan[start] == result.name:
                start += 1
                continue
            break
        return plan[start:]

    def _full_plan(self, job: Job, results: list[ToolResult]) -> list[str]:
        if job.kind not in self.plans:
            return []
        if job.kind == "ticket":
            plan = ["normalize_event", "classify_intent"]
            intent = _intent_from(results)
            if intent in BRANCH_DOWNSTREAM_INTENTS:
                plan.append("call_downstream")
            plan.append("record_result")
            return plan
        return list(self.plans[job.kind])

    def _build_call(self, name: str, job: Job, results: list[ToolResult]) -> ToolCall:
        payload = job.payload
        normalized = _output_named(results, "normalize_event")
        classified = _output_named(results, "classify_intent")

        if name == "echo":
            return ToolCall(name=name, arguments={"message": payload.get("message", payload)})
        if name == "normalize_event":
            return ToolCall(name=name, arguments=dict(payload))
        if name == "classify_intent":
            text = _text_for_classify(payload, normalized)
            return ToolCall(name=name, arguments={"text": text, "body": text})
        if name == "call_downstream":
            return ToolCall(name=name, arguments={"url": _downstream_url(payload, classified)})
        if name == "record_result":
            fields: dict[str, Any] = {"kind": job.kind}
            if classified.get("intent") is not None:
                fields["intent"] = classified["intent"]
            return ToolCall(name=name, arguments=fields)
        if name == "flaky_downstream":
            return ToolCall(
                name=name,
                arguments={"succeed_on_attempt": payload.get("succeed_on_attempt")},
            )
        return ToolCall(name=name, arguments=dict(payload))


def _preview_job(kind: str, payload: dict[str, Any]) -> Job:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Job(
        id="preview",
        kind=kind,
        payload=payload,
        available_at=now,
        created_at=now,
        updated_at=now,
    )


def _output_named(results: list[ToolResult], name: str) -> dict[str, Any]:
    for result in reversed(results):
        if result.name == name and result.ok:
            return result.output
    return {}


def _intent_from(results: list[ToolResult]) -> str | None:
    intent = _output_named(results, "classify_intent").get("intent")
    return str(intent) if intent is not None else None


def _text_for_classify(payload: dict[str, Any], normalized: dict[str, Any]) -> str:
    body = normalized.get("body")
    if isinstance(body, str) and body:
        return body
    text = payload.get("text") or payload.get("body") or ""
    return text if isinstance(text, str) else str(text)


def _downstream_url(payload: dict[str, Any], classified: dict[str, Any]) -> str:
    url = payload.get("url")
    if isinstance(url, str) and url:
        return url
    intent = classified.get("intent")
    if intent == "refund":
        return "https://mock.local/refund"
    if intent == "alert":
        return "https://mock.local/alert"
    return "https://mock.local/hook"


def _unknown_tool_error(result: ToolResult) -> bool:
    """Mock runtime (and a future planner) mark missing names this way."""
    return (result.error or "").startswith("unknown tool:")
