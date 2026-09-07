from __future__ import annotations

from event_worker.agent.policy import AgentPolicy
from event_worker.agent.tools import MockToolRuntime
from event_worker.models import HandlerDecision, Job, Outcome, ToolResult


class JobHandler:
    """Claimed-job → tool calls → ack/retry/DLQ decision."""

    def __init__(
        self,
        policy: AgentPolicy | None = None,
        runtime: MockToolRuntime | None = None,
    ) -> None:
        self.policy = policy or AgentPolicy()
        self.runtime = runtime or MockToolRuntime()

    def handle(self, job: Job) -> HandlerDecision:
        calls = self.policy.select_tools(job)
        if not calls:
            return self.policy.classify(job, [])

        results: list[ToolResult] = []
        for call in calls:
            result = self.runtime.invoke(call, job)
            results.append(result)
            if not result.ok:
                break
        return self.policy.classify(job, results)


def apply_decision_label(decision: HandlerDecision) -> str:
    if decision.outcome == Outcome.SUCCESS:
        return "acked"
    if decision.outcome == Outcome.RETRYABLE:
        return "retry"
    return "dlq"
