from __future__ import annotations

from event_worker.agent.policy import AgentPolicy
from event_worker.agent.tools import MockToolRuntime
from event_worker.models import HandlerDecision, Job, Outcome, ToolResult


class JobHandler:
    """Claimed-job → policy selects tools → invoke → policy classifies."""

    def __init__(
        self,
        policy: AgentPolicy | None = None,
        runtime: MockToolRuntime | None = None,
    ) -> None:
        self.policy = policy or AgentPolicy()
        self.runtime = runtime or MockToolRuntime()

    def handle(self, job: Job) -> HandlerDecision:
        results: list[ToolResult] = []
        for _ in range(self.policy.max_steps):
            calls = self.policy.select_tools(job, results)
            if not calls:
                return self.policy.classify(job, results)
            result = self.runtime.invoke(calls[0], job)
            results.append(result)
            if not result.ok:
                return self.policy.classify(job, results)
        leftover = self.policy.select_tools(job, results)
        if leftover:
            return HandlerDecision(
                outcome=Outcome.TERMINAL,
                error="policy exceeded max tool steps",
                tool_trace=[result.as_trace() for result in results],
            )
        return self.policy.classify(job, results)


def apply_decision_label(decision: HandlerDecision) -> str:
    if decision.outcome == Outcome.SUCCESS:
        return "acked"
    if decision.outcome == Outcome.RETRYABLE:
        return "retry"
    return "dlq"
