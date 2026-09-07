from __future__ import annotations

from event_worker.bench import run_bench


def test_local_bench_is_labeled_and_completes() -> None:
    result = run_bench(n=20)
    assert result["n_handled"] == 20
    assert result["completed"] == 20
    assert "local in-memory" in str(result["label"])
    assert result["enqueue_ms"]["p50"] >= 0
    assert result["claim_handle_ack_ms"]["p99"] >= 0
