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


def test_memory_demo_poison_dead_letters(monkeypatch) -> None:
    monkeypatch.setenv("EWA_QUEUE_BACKEND", "memory")
    runner = CliRunner()
    result = runner.invoke(app, ["demo", "--kind", "poison", "--payload", "{}"])
    assert result.exit_code == 0, result.output
    assert '"status": "dead"' in result.output
