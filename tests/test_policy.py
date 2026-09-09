from __future__ import annotations

from datetime import UTC, datetime

import pytest

from event_worker.agent.planner import LLMPlanner
from event_worker.agent.policy import AgentPolicy
from event_worker.agent.tools import MockToolRuntime
from event_worker.handler import JobHandler
from event_worker.models import Job, JobStatus, Outcome, ToolResult


def _job(kind: str, payload: dict | None = None, attempts: int = 1) -> Job:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return Job(
        id="job-1",
        kind=kind,
        payload=payload or {},
        status=JobStatus.LEASED,
        attempts=attempts,
        available_at=now,
        created_at=now,
        updated_at=now,
    )


def test_echo_success_records_trace() -> None:
    decision = JobHandler().handle(_job("echo", {"message": "ping"}))
    assert decision.outcome == Outcome.SUCCESS
    assert decision.result is not None
    assert decision.result["echo"]["echo"] == "ping"
    names = [step["name"] for step in decision.tool_trace]
    assert names == ["echo", "record_result"]


def test_classify_refund_intent() -> None:
    decision = JobHandler().handle(_job("classify", {"text": "please refund order 9"}))
    assert decision.outcome == Outcome.SUCCESS
    assert decision.result["classify_intent"]["intent"] == "refund"


def test_unknown_kind_is_terminal() -> None:
    decision = JobHandler().handle(_job("not-a-kind", {}))
    assert decision.outcome == Outcome.TERMINAL
    assert "unknown job kind" in (decision.error or "")


def test_transient_retryable_until_threshold() -> None:
    handler = JobHandler()
    first = handler.handle(_job("transient", {"succeed_on_attempt": 3}, attempts=1))
    assert first.outcome == Outcome.RETRYABLE
    later = handler.handle(_job("transient", {"succeed_on_attempt": 3}, attempts=3))
    assert later.outcome == Outcome.SUCCESS


def test_poison_is_terminal() -> None:
    decision = JobHandler().handle(_job("poison", {}))
    assert decision.outcome == Outcome.TERMINAL
    assert "poison" in (decision.error or "")


def test_notify_downstream_failure_retryable() -> None:
    decision = JobHandler().handle(
        _job("notify", {"url": "https://hook.fail.invalid", "text": "hi"})
    )
    assert decision.outcome == Outcome.RETRYABLE


def test_policy_known_kinds_match_plans() -> None:
    policy = AgentPolicy()
    assert "echo" in policy.known_kinds()
    assert "ticket" in policy.known_kinds()
    assert policy.select_tools(_job("echo"))[0].name == "echo"


def test_select_tools_binds_classify_from_normalize() -> None:
    policy = AgentPolicy()
    job = _job("classify", {"text": "please refund"})
    initial = policy.select_tools(job)
    assert [call.name for call in initial] == [
        "normalize_event",
        "classify_intent",
        "record_result",
    ]
    remaining = policy.select_tools(
        job,
        [
            ToolResult(
                name="normalize_event",
                ok=True,
                output={"body": "please refund order 9", "event_id": "job-1"},
            )
        ],
    )
    assert remaining[0].name == "classify_intent"
    assert remaining[0].arguments["text"] == "please refund order 9"


def test_classify_unknown_tool_is_terminal_even_if_retryable() -> None:
    policy = AgentPolicy()
    decision = policy.classify(
        _job("echo"),
        [
            ToolResult(
                name="invented_by_model",
                ok=False,
                retryable=True,
                error="unknown tool: invented_by_model",
            )
        ],
    )
    assert decision.outcome == Outcome.TERMINAL
    assert "unknown tool" in (decision.error or "")


def test_classify_poison_overrides_retryable_hint() -> None:
    policy = AgentPolicy()
    decision = policy.classify(
        _job("poison"),
        [ToolResult(name="raise_poison", ok=False, retryable=True, error="poison payload")],
    )
    assert decision.outcome == Outcome.TERMINAL


def test_ticket_refund_inserts_downstream() -> None:
    decision = JobHandler().handle(_job("ticket", {"text": "please refund order 9"}))
    assert decision.outcome == Outcome.SUCCESS
    names = [step["name"] for step in decision.tool_trace]
    assert names == [
        "normalize_event",
        "classify_intent",
        "call_downstream",
        "record_result",
    ]
    assert decision.result["call_downstream"]["url"] == "https://mock.local/refund"


def test_ticket_unknown_intent_skips_downstream() -> None:
    decision = JobHandler().handle(_job("ticket", {"text": "just saying hello"}))
    assert decision.outcome == Outcome.SUCCESS
    names = [step["name"] for step in decision.tool_trace]
    assert names == ["normalize_event", "classify_intent", "record_result"]


def test_max_steps_is_terminal() -> None:
    policy = AgentPolicy(plans={"loop": ["echo", "echo", "echo"]}, max_steps=1)
    decision = JobHandler(policy=policy).handle(_job("loop", {"message": "x"}))
    assert decision.outcome == Outcome.TERMINAL
    assert "max tool steps" in (decision.error or "")


def test_preview_is_offline_and_lists_tools() -> None:
    preview = AgentPolicy().preview("classify", {"text": "refund"})
    assert preview["hosted_llm"] is False
    assert preview["planner"] == "AgentPolicy"
    assert [step["name"] for step in preview["tools"]] == [
        "normalize_event",
        "classify_intent",
        "record_result",
    ]


def test_mock_runtime_lists_tools() -> None:
    assert "classify_intent" in MockToolRuntime().names()


def test_handler_does_not_use_llm_planner() -> None:
    handler = JobHandler()
    assert isinstance(handler.policy, AgentPolicy)
    assert not hasattr(handler, "planner")


def test_llm_planner_is_explicit_stub() -> None:
    with pytest.raises(NotImplementedError, match="stub"):
        LLMPlanner().plan(_job("echo"))
