from __future__ import annotations

import pytest

from event_worker.clock import FrozenClock
from event_worker.errors import StaleLeaseError
from event_worker.models import JobStatus


def test_expired_lease_is_reclaimable(queue, clock: FrozenClock) -> None:
    queue.enqueue("echo", {"message": "lease"})
    first = queue.claim("w1", lease_seconds=10)
    assert first is not None
    clock.advance(11)
    reclaimed = queue.reclaim_expired()
    assert reclaimed == 1
    second = queue.claim("w2", lease_seconds=10)
    assert second is not None
    assert second.id == first.id
    assert second.attempts == 2
    assert second.lease_owner == "w2"


def test_claim_auto_reclaims_expired_lease(queue, clock: FrozenClock) -> None:
    queue.enqueue("echo", {"message": "auto"})
    queue.claim("w1", lease_seconds=5)
    clock.advance(6)
    second = queue.claim("w2", lease_seconds=5)
    assert second is not None
    assert second.lease_owner == "w2"
    assert second.attempts == 2


def test_wrong_worker_cannot_ack(queue) -> None:
    queue.enqueue("echo", {"message": "fence"})
    claimed = queue.claim("w1", lease_seconds=30)
    with pytest.raises(StaleLeaseError):
        queue.ack(claimed.id, {"ok": True}, worker_id="w2")


def test_ack_after_expiry_is_stale(queue, clock: FrozenClock) -> None:
    queue.enqueue("echo", {"message": "late"})
    claimed = queue.claim("w1", lease_seconds=5)
    clock.advance(6)
    with pytest.raises(StaleLeaseError):
        queue.ack(claimed.id, {"ok": True}, worker_id="w1")
    other = queue.claim("w2", lease_seconds=5)
    assert other is not None
    assert other.status == JobStatus.LEASED
