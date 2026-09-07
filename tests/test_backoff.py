from __future__ import annotations

import random

from event_worker.backoff import retry_delay_seconds


def test_jitter_stays_within_band() -> None:
    rng = random.Random(0)
    delay = retry_delay_seconds(3, base=1.0, cap=60.0, jitter=0.1, rng=rng)
    assert 3.6 <= delay <= 4.4


def test_zero_jitter_is_exact() -> None:
    assert retry_delay_seconds(1, base=0.25, cap=1.0, jitter=0.0) == 0.25
