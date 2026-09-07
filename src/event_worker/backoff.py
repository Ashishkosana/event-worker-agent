from __future__ import annotations

import random


def retry_delay_seconds(
    attempts: int,
    *,
    base: float = 1.0,
    cap: float = 60.0,
    jitter: float = 0.1,
    rng: random.Random | None = None,
) -> float:
    """Exponential backoff for the *next* retry after ``attempts`` started.

    ``attempts`` is the number of claims already made (1 after the first
    failure). Delay is ``min(cap, base * 2**(attempts-1))`` with optional
    symmetric jitter. This is a local policy, not a production SLO.
    """
    if attempts < 1:
        attempts = 1
    raw = min(cap, base * (2 ** (attempts - 1)))
    if jitter <= 0:
        return raw
    spread = raw * jitter
    picker = rng.uniform if rng is not None else random.uniform
    return max(0.0, picker(raw - spread, raw + spread))
