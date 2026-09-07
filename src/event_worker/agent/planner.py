from __future__ import annotations

from event_worker.models import Job, ToolCall


class LLMPlanner:
    """Stub for a model that emits ``ToolCall`` lists.

    Do **not** use this on the default worker path. ``AgentPolicy.select_tools``
    is the implemented planner. Keep ``AgentPolicy.classify`` as the
    retry / DLQ authority so a flaky model cannot loop a poison job forever.

    To implement later:
    1. Send ``job.kind`` + ``job.payload`` + available tool schemas to a model.
    2. Parse a strict JSON list of ``{name, arguments}``.
    3. Reject unknown tool names (terminal, do not retry).
    4. Return ``list[ToolCall]`` and let ``JobHandler`` run them.
    """

    def plan(self, job: Job) -> list[ToolCall]:
        raise NotImplementedError(
            "LLMPlanner is a stub. Implement plan() to call your model and "
            f"return ToolCall objects for job {job.id}. Keep "
            "AgentPolicy.classify() as the retry/DLQ authority."
        )
