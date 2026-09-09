from __future__ import annotations

from typer.testing import CliRunner

from event_worker.cli import app


def test_memory_demo_echo_completes(monkeypatch) -> None:
    monkeypatch.setenv("EWA_QUEUE_BACKEND", "memory")
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["demo", "--kind", "echo", "--payload", '{"message":"hi"}'],
    )
    assert result.exit_code == 0, result.output
    assert '"status": "completed"' in result.output
    assert "echo" in result.output
    assert "tools echo -> record_result" in result.output
    assert "status completed" in result.output


def test_memory_demo_poison_dead_letters(monkeypatch) -> None:
    monkeypatch.setenv("EWA_QUEUE_BACKEND", "memory")
    runner = CliRunner()
    result = runner.invoke(app, ["demo", "--kind", "poison", "--payload", "{}"])
    assert result.exit_code == 0, result.output
    assert '"status": "dead"' in result.output
    assert "tools raise_poison" in result.output


def test_plan_prints_deterministic_tools() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["plan", "--kind", "classify", "--payload", '{"text":"refund"}'])
    assert result.exit_code == 0, result.output
    assert '"planner": "AgentPolicy"' in result.output
    assert '"hosted_llm": false' in result.output
    assert "classify_intent" in result.output
