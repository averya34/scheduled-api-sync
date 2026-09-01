"""Tests for synckit.ratelimit."""

from __future__ import annotations

import threading

import pytest
from conftest import FakeClock, FakeSleep

from synckit.ratelimit import RateLimitTimeout, TokenBucket


def make_bucket(rate=10.0, capacity=None, **kwargs):
    clock = FakeClock()
    sleeper = FakeSleep(clock)
    bucket = TokenBucket(rate, capacity, clock=clock, sleep=sleeper, **kwargs)
    return bucket, clock, sleeper


# -- construction ---------------------------------------------------------


def test_capacity_defaults_to_rate():
    bucket, _, _ = make_bucket(rate=7.0)
    assert bucket.capacity == 7.0


def test_bucket_starts_full():
    bucket, _, _ = make_bucket(rate=5.0, capacity=20.0)
    assert bucket.tokens == 20.0


def test_initial_tokens_can_be_set():
    bucket, _, _ = make_bucket(rate=5.0, capacity=20.0, initial_tokens=3.0)
    assert bucket.tokens == 3.0


def test_initial_tokens_are_clamped_to_capacity():
    bucket, _, _ = make_bucket(rate=5.0, capacity=10.0, initial_tokens=999.0)
    assert bucket.tokens == 10.0


def test_non_positive_rate_is_rejected():
    with pytest.raises(ValueError, match="rate"):
        TokenBucket(0)


def test_non_positive_capacity_is_rejected():
    with pytest.raises(ValueError, match="capacity"):
        TokenBucket(5, 0)


def test_negative_initial_tokens_is_rejected():
    with pytest.raises(ValueError, match="initial_tokens"):
        TokenBucket(5, initial_tokens=-1)


def test_repr_mentions_rate_and_capacity():
    bucket, _, _ = make_bucket(rate=3.0, capacity=6.0)
    text = repr(bucket)
    assert "rate=3.0" in text
    assert "capacity=6.0" in text


# -- burst, throttle, refill ---------------------------------------------


def test_burst_up_to_capacity_without_waiting():
    bucket, _, sleeper = make_bucket(rate=1.0, capacity=5.0)
    for _ in range(5):
        assert bucket.acquire() == 0.0
    assert sleeper.count == 0
    assert bucket.tokens == pytest.approx(0.0)


def test_throttles_once_the_burst_is_spent():
    bucket, _, sleeper = make_bucket(rate=2.0, capacity=2.0)
    bucket.acquire()
    bucket.acquire()
    waited = bucket.acquire()
    # One more token at 2/s means a half-second wait.
    assert waited == pytest.approx(0.5)
    assert sleeper.calls == [pytest.approx(0.5)]


def test_refill_accrues_over_time():
    bucket, clock, _ = make_bucket(rate=10.0, capacity=10.0, initial_tokens=0.0)
    clock.advance(0.5)
    assert bucket.tokens == pytest.approx(5.0)


def test_refill_is_capped_at_capacity():
    bucket, clock, _ = make_bucket(rate=10.0, capacity=10.0, initial_tokens=0.0)
    clock.advance(3600)
    assert bucket.tokens == 10.0


def test_full_burst_throttle_refill_cycle():
    bucket, clock, sleeper = make_bucket(rate=4.0, capacity=8.0)
    for _ in range(8):
        assert bucket.acquire() == 0.0
    assert sleeper.count == 0

    assert bucket.acquire() == pytest.approx(0.25)

    clock.advance(2.0)  # 8 tokens accrue, capped at capacity
    assert bucket.tokens == 8.0
    for _ in range(8):
        assert bucket.acquire() == 0.0


def test_steady_state_rate_is_respected_over_many_calls():
    bucket, clock, sleeper = make_bucket(rate=5.0, capacity=1.0, initial_tokens=1.0)
    for _ in range(11):
        bucket.acquire()
    # 1 free token, then 10 waits of 0.2s each.
    assert sleeper.total == pytest.approx(2.0)
    assert clock.now == pytest.approx(1002.0)


# -- try_acquire ----------------------------------------------------------


def test_try_acquire_succeeds_while_tokens_remain():
    bucket, _, _ = make_bucket(rate=1.0, capacity=2.0)
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is True


def test_try_acquire_fails_instead_of_blocking():
    bucket, _, sleeper = make_bucket(rate=1.0, capacity=1.0)
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False
    assert sleeper.count == 0


def test_try_acquire_succeeds_again_after_refill():
    bucket, clock, _ = make_bucket(rate=1.0, capacity=1.0)
    assert bucket.try_acquire() is True
    assert bucket.try_acquire() is False
    clock.advance(1.0)
    assert bucket.try_acquire() is True


def test_try_acquire_rejects_non_positive_token_counts():
    bucket, _, _ = make_bucket()
    with pytest.raises(ValueError, match="tokens"):
        bucket.try_acquire(0)


def test_requesting_more_than_capacity_raises():
    bucket, _, _ = make_bucket(rate=1.0, capacity=5.0)
    with pytest.raises(ValueError, match="capacity"):
        bucket.try_acquire(6)
    with pytest.raises(ValueError, match="capacity"):
        bucket.acquire(6)


# -- fractional and weighted costs ---------------------------------------


def test_fractional_token_costs():
    bucket, _, _ = make_bucket(rate=1.0, capacity=1.0)
    assert bucket.try_acquire(0.25) is True
    assert bucket.tokens == pytest.approx(0.75)


def test_weighted_acquire_waits_proportionally():
    bucket, _, sleeper = make_bucket(rate=2.0, capacity=4.0, initial_tokens=0.0)
    bucket.acquire(4)
    assert sleeper.calls == [pytest.approx(2.0)]


# -- timeouts -------------------------------------------------------------


def test_timeout_raises_when_the_wait_is_too_long():
    bucket, _, _ = make_bucket(rate=1.0, capacity=1.0, initial_tokens=0.0)
    with pytest.raises(RateLimitTimeout):
        bucket.acquire(timeout=0.5)


def test_timeout_not_triggered_when_the_wait_fits():
    bucket, _, sleeper = make_bucket(rate=1.0, capacity=1.0, initial_tokens=0.0)
    assert bucket.acquire(timeout=2.0) == pytest.approx(1.0)
    assert sleeper.count == 1


def test_timeout_message_mentions_the_budget():
    bucket, _, _ = make_bucket(rate=1.0, capacity=1.0, initial_tokens=0.0)
    with pytest.raises(RateLimitTimeout, match="budget"):
        bucket.acquire(timeout=0.1)


def test_timeout_does_not_consume_tokens():
    bucket, _, _ = make_bucket(rate=1.0, capacity=1.0, initial_tokens=0.5)
    with pytest.raises(RateLimitTimeout):
        bucket.acquire(timeout=0.1)
    assert bucket.tokens == pytest.approx(0.5)


# -- clock robustness -----------------------------------------------------


def test_backwards_clock_does_not_mint_tokens():
    clock = FakeClock()
    bucket = TokenBucket(10.0, 10.0, clock=clock, sleep=FakeSleep(), initial_tokens=0.0)
    clock.advance(-100)
    assert bucket.tokens == pytest.approx(0.0)


def test_stationary_clock_grants_nothing():
    bucket, _, _ = make_bucket(rate=10.0, capacity=10.0, initial_tokens=2.0)
    assert bucket.tokens == pytest.approx(2.0)
    assert bucket.tokens == pytest.approx(2.0)


# -- stats and reset ------------------------------------------------------


def test_stats_track_acquisitions_and_throttling():
    bucket, _, _ = make_bucket(rate=2.0, capacity=2.0)
    bucket.acquire()
    bucket.acquire()
    bucket.acquire()
    stats = bucket.stats()
    assert stats.acquired == 3
    assert stats.throttled == 1
    assert stats.total_wait == pytest.approx(0.5)
    assert stats.rate == 2.0
    assert stats.capacity == 2.0


def test_reset_refills_the_bucket():
    bucket, _, _ = make_bucket(rate=1.0, capacity=5.0)
    for _ in range(5):
        bucket.try_acquire()
    assert bucket.tokens == pytest.approx(0.0)
    bucket.reset()
    assert bucket.tokens == 5.0


def test_reset_to_an_explicit_level():
    bucket, _, _ = make_bucket(rate=1.0, capacity=5.0)
    bucket.reset(2.0)
    assert bucket.tokens == 2.0


def test_reset_preserves_counters():
    bucket, _, _ = make_bucket(rate=1.0, capacity=2.0)
    bucket.try_acquire()
    bucket.reset()
    assert bucket.stats().acquired == 1


# -- context manager ------------------------------------------------------


def test_context_manager_consumes_one_token():
    bucket, _, _ = make_bucket(rate=1.0, capacity=3.0)
    with bucket:
        pass
    assert bucket.tokens == pytest.approx(2.0)


def test_context_manager_does_not_refund_on_error():
    bucket, _, _ = make_bucket(rate=1.0, capacity=3.0)
    with pytest.raises(RuntimeError), bucket:
        raise RuntimeError("request failed")
    assert bucket.tokens == pytest.approx(2.0)


# -- concurrency ----------------------------------------------------------


def test_concurrent_acquire_never_oversubscribes():
    # A real (tiny) thread test: the clock never advances, so exactly
    # ``capacity`` acquisitions can succeed no matter how they interleave.
    clock = FakeClock()
    bucket = TokenBucket(1000.0, 50.0, clock=clock, sleep=lambda s: None, initial_tokens=50.0)
    successes: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        got = bucket.try_acquire()
        with lock:
            successes.append(got)

    threads = [threading.Thread(target=worker) for _ in range(200)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sum(successes) == 50
    assert bucket.tokens == pytest.approx(0.0)
