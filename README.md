# event-worker-agent

Event-driven **agent worker**: enqueue a job, a worker **claims it with a lease**, the handler **calls tools**, failures **retry with backoff**, poison work lands on a **DLQ**.

This is a portfolio Milestone 1. It is an honest "queues / event-driven / worker" JD signal. It is **not** Kafka-at-scale, not multi-region, and not a production message bus.

```
enqueue API / CLI
        │
        ▼
   Redis ready queue
        │
        ▼
 worker claims (visibility timeout / lease)
        │
        ▼
 AgentPolicy.select_tools → mock tools → AgentPolicy.classify
        │
        ├── success  → ack
        ├── retryable → delayed (backoff) → ready
        └── terminal or max attempts → DLQ
```

## What this is / is not

| This repo | Not this repo |
| --- | --- |
| SQS-like claim + visibility timeout | Kafka, Pulsar, SNS fan-out |
| One Redis list + sorted sets | Partitioned log, consumer groups |
| Deterministic agent policy + mock tools | Hosted LLM brain in CI |
| Local in-memory timings, labeled as mock | "1M msgs/sec" claims |
| At-least-once + lease fencing | Exactly-once side effects |

Spirit of tick / ledgerline: **claiming, leases, retries, DLQ**. The worker is an *agent* (job → tools → decision), not a generic cron.

## Architecture

```
┌─────────────┐   POST /v1/jobs    ┌─────────────┐
│  FastAPI    │ ─────────────────▶ │   Queue     │
│  or CLI     │                    │ ready       │
└─────────────┘                    │ delayed     │
                                   │ inflight    │
┌─────────────┐   claim / ack      │ dlq         │
│  Worker     │ ◀───────────────▶  └─────────────┘
│  run_once() │                         │
└──────┬──────┘                    Redis (compose)
       │                           or InMemory / fakeredis (tests)
       ▼
┌──────────────────────────────────────────────┐
│  JobHandler                                  │
│   AgentPolicy.select_tools()   YOU IMPLEMENT │
│   MockToolRuntime.invoke()                   │
│   AgentPolicy.classify()       retry vs DLQ  │
└──────────────────────────────────────────────┘
```

**Lease:** claim moves a job to `inflight` until `lease_expires_at`. Another worker cannot claim it. If the owner crashes, reclaim puts it back on `ready`. Ack/fail from the wrong worker or after expiry raises `StaleLeaseError`.

**Attempts:** increment on each successful claim. Retryable failure + `attempts < max_attempts` → delayed with exponential backoff. Otherwise → DLQ.

**Backends:** `InMemoryQueue` (tests / `EWA_QUEUE_BACKEND=memory`) and `RedisQueue` (Docker Compose). Same contract. Tests run both (fakeredis).

## Interview bridge

When a JD says *event-driven*, *SQS*, *workers*, *retries*, *DLQ*:

- **Visibility timeout / lease** — naive `LPOP` loses the job if the process dies. A lease keeps the payload until ack or expiry.
- **At-least-once** — a crash after the tool ran but before ack will reclaim and run again. Side effects must be idempotent (we store an optional `idempotency_key` on enqueue).
- **Poison messages** — a terminal policy outcome skips remaining retries so a bad payload cannot burn the loop.
- **Backoff** — `min(cap, base * 2**(attempts-1))` plus jitter. Stops a hot poison from retry-storming Redis.
- **DLQ** — inspect via `GET /v1/queue/dlq`, replay via `POST /v1/queue/dlq/{id}/requeue`.

What I would say I have **not** built: cross-AZ replication, exactly-once sinks, schema registry, or Kafka consumer lag dashboards.

## YOU IMPLEMENT — agent policy

The queue, worker loop, leases, backoff, and DLQ are done.

The **agent tool-calling brain** lives in `src/event_worker/agent/`:

| File | Role |
| --- | --- |
| [`policy.py`](src/event_worker/agent/policy.py) | **Implemented.** `select_tools(job)` builds an ordered `ToolCall` list from `kind`. `classify(job, results)` is the retry/DLQ authority (`success` / `retryable` / `terminal`). |
| [`tools.py`](src/event_worker/agent/tools.py) | **Implemented mock runtime.** `echo`, `normalize_event`, `classify_intent`, `call_downstream`, `record_result`, `flaky_downstream`, `raise_poison`. |
| [`planner.py`](src/event_worker/agent/planner.py) | **Stub.** `LLMPlanner.plan()` raises `NotImplementedError`. Swap this in later; keep `classify()` deterministic. |

Job kinds the policy already handles:

| `kind` | Tools | Typical outcome |
| --- | --- | --- |
| `echo` | echo → record_result | success |
| `normalize` | normalize_event → record_result | success |
| `classify` | normalize → classify_intent → record | success (keyword intent) |
| `notify` | normalize → call_downstream → record | retryable if URL ends with `fail.invalid` |
| `transient` | flaky_downstream → record | retry until `payload.succeed_on_attempt` |
| `poison` | raise_poison | terminal DLQ on first attempt |
| anything else | — | terminal (`unknown job kind`) |

Extension points (not required for Milestone 1):

1. Replace `MockToolRuntime` handlers with real HTTP / DB tools.
2. Implement `LLMPlanner.plan()` to emit `ToolCall[]`. Reject unknown names as **terminal**.
3. Keep `AgentPolicy.classify()` as the only path that can mark a job retryable.

## Milestone 1 checklist

- [x] Job model + enqueue API and CLI
- [x] Worker loop that claims with a visibility timeout / lease
- [x] Mock tool calls on the job payload
- [x] Failure → retry with backoff → DLQ after N attempts
- [x] Docker Compose: Redis + API + worker
- [x] pytest for claim / lease / retry / DLQ / policy
- [x] README: architecture, interview bridge, YOU IMPLEMENT

## Run

Python 3.12+.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
ruff check src tests
event-worker bench --n 200   # labeled local in-memory timings
```

In-memory worker (no Redis):

```bash
EWA_QUEUE_BACKEND=memory event-worker enqueue --kind echo --payload '{"message":"hi"}'
EWA_QUEUE_BACKEND=memory event-worker worker --once
```

### Docker Compose

```bash
docker compose up --build
curl -s localhost:8000/health
curl -s -X POST localhost:8000/v1/jobs \
  -H 'content-type: application/json' \
  -d '{"kind":"classify","payload":{"text":"please refund order 42"}}'
curl -s localhost:8000/v1/queue/stats
```

The worker container claims from the same Redis. Watch logs for `claimed` / `acked` / `retry` / `dead-lettered`.

Poison path (DLQ on first attempt):

```bash
curl -s -X POST localhost:8000/v1/jobs \
  -H 'content-type: application/json' \
  -d '{"kind":"poison","payload":{}}'
curl -s localhost:8000/v1/queue/dlq
```

### API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | liveness |
| `POST` | `/v1/jobs` | enqueue (`kind`, `payload`, `max_attempts`, optional `idempotency_key`) |
| `GET` | `/v1/jobs/{id}` | job document |
| `GET` | `/v1/queue/stats` | ready / leased / delayed / dead depths |
| `GET` | `/v1/queue/dlq` | dead letters |
| `POST` | `/v1/queue/dlq/{id}/requeue` | reset attempts, put back on ready |

CLI: `event-worker serve` · `event-worker worker` · `event-worker enqueue` · `event-worker stats`.

Env (see `.env.example`): `REDIS_URL`, `EWA_QUEUE_BACKEND`, `EWA_LEASE_SECONDS`, `EWA_BACKOFF_*`.

## Benchmarks (honest)

`event-worker bench` times enqueue + claim/handle/ack on **`InMemoryQueue` in this process**. CI runs `event-worker bench --n 50`.

Those figures are **local mock timings**, not a production SLO and not comparable to a hosted queue. If you quote them, keep the label.

## Layout

```
src/event_worker/
  models.py           Job, ToolCall, HandlerDecision
  queues/             InMemory + Redis (claim, lease, DLQ)
  backoff.py          exponential delay
  agent/policy.py     implemented tool-calling policy
  agent/tools.py      mock tools
  agent/planner.py    LLM stub
  handler.py          policy + tools
  worker.py           claim loop
  api.py              FastAPI
  cli.py              typer
tests/                claim, lease, retry/DLQ, policy, API
```

## License

MIT
