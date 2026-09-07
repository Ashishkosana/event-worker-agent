from __future__ import annotations

import json
import logging
from typing import Any

import typer
import uvicorn

from event_worker.bench import run_bench
from event_worker.config import load_settings
from event_worker.queues import build_queue
from event_worker.worker import Worker

app = typer.Typer(no_args_is_help=True, add_completion=False)
log_level_opt = typer.Option("INFO", "--log-level", help="Python log level")


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


@app.command()
def serve(
    host: str = "0.0.0.0",
    port: int = 8000,
    log_level: str = log_level_opt,
) -> None:
    """Run the FastAPI enqueue API."""
    _configure_logging(log_level)
    uvicorn.run("event_worker.api:app", host=host, port=port, log_level=log_level.lower())


@app.command()
def worker(
    once: bool = typer.Option(False, "--once", help="Claim and handle at most one job"),
    log_level: str = log_level_opt,
) -> None:
    """Run the claim loop."""
    _configure_logging(log_level)
    settings = load_settings()
    queue = build_queue(settings)
    w = Worker(queue, settings)
    if once:
        job = w.run_once()
        if job is None:
            typer.echo("queue empty")
        else:
            latest = queue.get(job.id)
            typer.echo(latest.model_dump_json(indent=2) if latest else job.id)
        return
    w.run_forever()


@app.command()
def enqueue(
    kind: str = typer.Option("echo", "--kind"),
    payload: str = typer.Option("{}", "--payload", help="JSON object"),
    max_attempts: int = typer.Option(3, "--max-attempts"),
    idempotency_key: str | None = typer.Option(None, "--idempotency-key"),
) -> None:
    """Put a job on the ready queue."""
    settings = load_settings()
    queue = build_queue(settings)
    body: dict[str, Any] = json.loads(payload)
    job = queue.enqueue(
        kind,
        body,
        max_attempts=max_attempts,
        idempotency_key=idempotency_key,
    )
    typer.echo(job.model_dump_json(indent=2))


@app.command("get")
def get_job(job_id: str) -> None:
    settings = load_settings()
    queue = build_queue(settings)
    job = queue.get(job_id)
    if job is None:
        raise typer.Exit(code=1)
    typer.echo(job.model_dump_json(indent=2))


@app.command()
def stats() -> None:
    settings = load_settings()
    queue = build_queue(settings)
    typer.echo(queue.stats().model_dump_json(indent=2))


@app.command()
def demo(
    kind: str = typer.Option("echo", "--kind"),
    payload: str = typer.Option("{}", "--payload", help="JSON object"),
    max_attempts: int = typer.Option(3, "--max-attempts"),
    log_level: str = log_level_opt,
) -> None:
    """Enqueue and handle one job in this process.

    Use this with ``EWA_QUEUE_BACKEND=memory`` — that backend is
    process-local and does not survive a second CLI invocation.
    """
    _configure_logging(log_level)
    settings = load_settings()
    queue = build_queue(settings)
    body: dict[str, Any] = json.loads(payload)
    job = queue.enqueue(kind, body, max_attempts=max_attempts)
    typer.echo(f"enqueued {job.id} kind={job.kind}")
    worker = Worker(queue, settings)
    handled = worker.run_once()
    if handled is None:
        typer.echo("worker saw an empty queue")
        raise typer.Exit(code=1)
    latest = queue.get(handled.id)
    typer.echo(latest.model_dump_json(indent=2) if latest else handled.id)


@app.command()
def bench(
    n: int = typer.Option(200, "--n", help="Jobs to enqueue/claim/ack"),
) -> None:
    """Local in-memory timings. Not a production SLO."""
    result = run_bench(n=n)
    typer.echo(json.dumps(result, indent=2))
