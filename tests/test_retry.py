"""Tests for synckit.retry."""

from __future__ import annotations

import pytest
from conftest import FakeSleep, FlakyCallable, SequenceRandom

from synckit.retry import (
    DEFAULT_RETRYABLE_STATUS,
    HttpError,
    RetryError,
    RetryPolicy,
    TransientError,
    compute_delay,
    retry,
    retry_call,
    retryable_statuses,
)

# -- policy validation ----------------------------------------------------


def test_default_policy_has_sensible_values():
    policy = RetryPolicy()
    assert policy.max_attempts >= 1
    assert policy.base_delay > 0
    assert policy.jitter is True
    assert policy.respect_retry_after is True


def test_max_attempts_below_one_is_rejected():
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=0)


def test_negative_base_delay_is_rejected():
    with pytest.raises(ValueError, match="base_delay"):
        RetryPolicy(base_delay=-1)


def test_negative_max_delay_is_rejected():
    with pytest.raises(ValueError, match="max_delay"):
        RetryPolicy(max_delay=-0.5)


def test_negative_max_retry_after_is_rejected():
    with pytest.raises(ValueError, match="max_retry_after"):
        RetryPolicy(max_retry_after=-1)


def test_retryable_status_is_normalised_to_frozenset():
    policy = RetryPolicy(retryable_status=[429, 503, 503])
    assert policy.retryable_status == frozenset({429, 503})


def test_default_retryable_status_set():
    assert DEFAULT_RETRYABLE_STATUS == frozenset({429, 500, 502, 503, 504})


def test_retryable_statuses_helper_widens_the_default():
    widened = retryable_statuses([430])
    assert 430 in widened
    assert DEFAULT_RETRYABLE_STATUS <= widened


# -- classification -------------------------------------------------------


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retryable_http_statuses(status):
    assert RetryPolicy().is_retryable(HttpError(status)) is True


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 418, 422])
def test_client_errors_are_not_retryable(status):
    assert RetryPolicy().is_retryable(HttpError(status)) is False


def test_status_check_wins_over_exception_class():
    # HttpError is on the exception allow-list here, but a 403 must still
    # not be retried.
    policy = RetryPolicy(retryable_exceptions=(HttpError,))
    assert policy.is_retryable(HttpError(403)) is False
    assert policy.is_retryable(HttpError(503)) is True


def test_transient_error_is_retryable_by_default():
    assert RetryPolicy().is_retryable(TransientError("blip")) is True


def test_oserror_is_retryable_by_default():
    assert RetryPolicy().is_retryable(OSError("connection reset")) is True


def test_value_error_is_not_retryable_by_default():
    assert RetryPolicy().is_retryable(ValueError("bad payload")) is False


def test_custom_retryable_exception_set():
    policy = RetryPolicy(retryable_exceptions=(KeyError,))
    assert policy.is_retryable(KeyError("k")) is True
    assert policy.is_retryable(TransientError("t")) is False


# -- delay computation ----------------------------------------------------


def test_backoff_doubles_per_attempt():
    policy = RetryPolicy(base_delay=1.0, max_delay=1000.0)
    assert [policy.backoff_for(n) for n in (1, 2, 3, 4)] == [1.0, 2.0, 4.0, 8.0]


def test_backoff_is_capped_at_max_delay():
    policy = RetryPolicy(base_delay=1.0, max_delay=5.0)
    assert policy.backoff_for(10) == 5.0


def test_backoff_rejects_attempt_zero():
    with pytest.raises(ValueError, match="attempt"):
        RetryPolicy().backoff_for(0)


def test_no_jitter_returns_the_raw_backoff():
    policy = RetryPolicy(base_delay=2.0, jitter=False)
    assert compute_delay(policy, 3) == 8.0


def test_full_jitter_uses_the_random_source():
    policy = RetryPolicy(base_delay=1.0, jitter=True, max_delay=100.0)
    assert compute_delay(policy, 4, rand=SequenceRandom([0.5])) == 4.0


@pytest.mark.parametrize("draw", [0.0, 0.01, 0.25, 0.5, 0.99, 1.0])
def test_jitter_is_bounded_by_zero_and_the_backoff(draw):
    policy = RetryPolicy(base_delay=1.0, max_delay=64.0)
    for attempt in range(1, 8):
        ceiling = policy.backoff_for(attempt)
        delay = compute_delay(policy, attempt, rand=SequenceRandom([draw]))
        assert 0.0 <= delay <= ceiling


def test_jitter_with_real_random_stays_in_range():
    policy = RetryPolicy(base_delay=0.5, max_delay=10.0)
    for _ in range(200):
        assert 0.0 <= compute_delay(policy, 5) <= policy.backoff_for(5)


def test_zero_base_delay_short_circuits_jitter():
    policy = RetryPolicy(base_delay=0.0)
    assert compute_delay(policy, 3, rand=SequenceRandom([0.9])) == 0.0


# -- Retry-After ----------------------------------------------------------


def test_retry_after_overrides_computed_backoff():
    policy = RetryPolicy(base_delay=1.0)
    assert compute_delay(policy, 1, retry_after=17.0, rand=SequenceRandom([0.1])) == 17.0


def test_retry_after_accepts_a_numeric_string():
    assert compute_delay(RetryPolicy(), 1, retry_after="12") == 12.0


def test_retry_after_is_capped_by_max_retry_after():
    policy = RetryPolicy(max_retry_after=30.0)
    assert compute_delay(policy, 1, retry_after=8000) == 30.0


def test_unparseable_retry_after_falls_back_to_backoff():
    policy = RetryPolicy(base_delay=2.0, jitter=False)
    assert compute_delay(policy, 1, retry_after="next tuesday") == 2.0


def test_negative_retry_after_is_ignored():
    policy = RetryPolicy(base_delay=2.0, jitter=False)
    assert compute_delay(policy, 1, retry_after=-5) == 2.0


def test_retry_after_ignored_when_disabled():
    policy = RetryPolicy(base_delay=2.0, jitter=False, respect_retry_after=False)
    assert compute_delay(policy, 1, retry_after=99) == 2.0


def test_retry_call_sleeps_for_the_retry_after_value():
    sleeper = FakeSleep()
    func = FlakyCallable(1, lambda: HttpError(429, retry_after=42.0))
    result = retry_call(
        func,
        policy=RetryPolicy(max_attempts=3, base_delay=1.0),
        sleep=sleeper,
    )
    assert result == "ok"
    assert sleeper.calls == [42.0]


# -- retry_call behaviour -------------------------------------------------


def test_success_on_first_attempt_never_sleeps():
    sleeper = FakeSleep()
    assert retry_call(lambda: 7, sleep=sleeper) == 7
    assert sleeper.count == 0


def test_arguments_are_forwarded():
    def add(a, b, c=0):
        return a + b + c

    assert retry_call(add, 1, 2, c=3) == 6


def test_retries_until_success():
    sleeper = FakeSleep()
    func = FlakyCallable(2, lambda: TransientError("blip"))
    assert retry_call(func, policy=RetryPolicy(max_attempts=5), sleep=sleeper) == "ok"
    assert func.calls == 3
    assert sleeper.count == 2


def test_exhaustion_raises_retry_error_with_attempt_count():
    sleeper = FakeSleep()
    func = FlakyCallable(99, lambda: TransientError("always"))
    with pytest.raises(RetryError) as info:
        retry_call(func, policy=RetryPolicy(max_attempts=4), sleep=sleeper)
    assert info.value.attempts == 4
    assert func.calls == 4
    # One fewer sleep than attempts: no point pausing after the last failure.
    assert sleeper.count == 3


def test_retry_error_preserves_the_last_exception():
    func = FlakyCallable(99, lambda: HttpError(503, "upstream down"))
    with pytest.raises(RetryError) as info:
        retry_call(func, policy=RetryPolicy(max_attempts=2), sleep=FakeSleep())
    assert isinstance(info.value.last_exception, HttpError)
    assert info.value.last_exception.status_code == 503
    assert isinstance(info.value.__cause__, HttpError)


def test_non_retryable_exception_passes_straight_through():
    sleeper = FakeSleep()
    func = FlakyCallable(99, lambda: ValueError("malformed payload"))
    with pytest.raises(ValueError, match="malformed payload"):
        retry_call(func, policy=RetryPolicy(max_attempts=5), sleep=sleeper)
    assert func.calls == 1
    assert sleeper.count == 0


def test_client_error_is_not_retried_by_retry_call():
    func = FlakyCallable(99, lambda: HttpError(401, "bad token"))
    with pytest.raises(HttpError):
        retry_call(func, policy=RetryPolicy(max_attempts=5), sleep=FakeSleep())
    assert func.calls == 1


def test_max_attempts_one_disables_retrying():
    func = FlakyCallable(99, lambda: TransientError("nope"))
    with pytest.raises(RetryError):
        retry_call(func, policy=RetryPolicy(max_attempts=1), sleep=FakeSleep())
    assert func.calls == 1


def test_sleep_durations_follow_the_jitter_source():
    sleeper = FakeSleep()
    func = FlakyCallable(3, lambda: TransientError("blip"))
    retry_call(
        func,
        policy=RetryPolicy(max_attempts=5, base_delay=1.0, max_delay=100.0),
        sleep=sleeper,
        rand=SequenceRandom([1.0]),
    )
    assert sleeper.calls == [1.0, 2.0, 4.0]


def test_on_retry_observer_receives_each_attempt():
    seen = []
    func = FlakyCallable(2, lambda: TransientError("blip"))
    retry_call(
        func,
        policy=RetryPolicy(max_attempts=4, base_delay=1.0, jitter=False),
        sleep=FakeSleep(),
        on_retry=lambda attempt, delay, exc: seen.append((attempt, delay, type(exc).__name__)),
    )
    assert seen == [(1, 1.0, "TransientError"), (2, 2.0, "TransientError")]


def test_on_retry_is_not_called_on_success():
    seen = []
    retry_call(lambda: 1, on_retry=lambda *a: seen.append(a), sleep=FakeSleep())
    assert seen == []


def test_zero_delay_skips_the_sleep_call_entirely():
    sleeper = FakeSleep()
    func = FlakyCallable(2, lambda: TransientError("blip"))
    retry_call(func, policy=RetryPolicy(max_attempts=4, base_delay=0.0), sleep=sleeper)
    assert sleeper.count == 0
    assert func.calls == 3


def test_keyboard_interrupt_is_not_swallowed():
    def boom():
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        retry_call(boom, sleep=FakeSleep())


# -- decorator form -------------------------------------------------------


def test_decorator_retries_and_preserves_metadata():
    sleeper = FakeSleep()

    @retry(RetryPolicy(max_attempts=3, base_delay=0.1), sleep=sleeper)
    def fetch(cursor):
        """Fetch a page."""
        fetch.calls += 1
        if fetch.calls < 3:
            raise HttpError(502)
        return f"page-{cursor}"

    fetch.calls = 0
    assert fetch("abc") == "page-abc"
    assert fetch.__name__ == "fetch"
    assert fetch.__doc__ == "Fetch a page."
    assert sleeper.count == 2


def test_decorator_exposes_its_policy():
    policy = RetryPolicy(max_attempts=9)

    @retry(policy)
    def noop():
        return None

    assert noop.retry_policy is policy


def test_decorator_without_a_policy_uses_the_default():
    @retry()
    def noop():
        return None

    assert noop.retry_policy.max_attempts == RetryPolicy().max_attempts


def test_decorator_propagates_non_retryable_errors():
    @retry(RetryPolicy(max_attempts=5), sleep=FakeSleep())
    def bad():
        raise HttpError(422, "unprocessable")

    with pytest.raises(HttpError) as info:
        bad()
    assert info.value.status_code == 422


# -- HttpError shape ------------------------------------------------------


def test_http_error_default_message():
    assert str(HttpError(503)) == "HTTP 503"


def test_http_error_custom_message_and_retry_after():
    err = HttpError(429, "slow down", retry_after=5)
    assert str(err) == "slow down"
    assert err.retry_after == 5
