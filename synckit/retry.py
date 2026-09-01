"""Retry logic with exponential backoff and full jitter.

Why this module exists
----------------------
Scheduled integration jobs fail for boring reasons: the upstream API is
rate limiting us, a load balancer returns a 502 during a rolling deploy,
a DNS blip drops one connection out of a thousand. None of those are bugs
in our code, and none of them should page anybody. They should be retried.

What must *not* be retried is just as important. A 401 means the token is
wrong, and hammering the endpoint five more times will not conjure a valid
credential -- it will just produce five more audit-log entries and, on some
providers, trip an automated lockout. A 422 means the payload is malformed;
the payload will still be malformed on attempt four. So the retry set is an
explicit allow-list, not "everything that raised".

Why full jitter instead of fixed or plain exponential backoff
-------------------------------------------------------------
When a shared upstream dependency recovers from an outage, every client that
was backing off wakes up at roughly the same moment and re-sends at once.
That synchronised surge -- the thundering herd -- can knock the dependency
straight back over, producing a recovery loop that looks like flapping.

Plain exponential backoff does not fix this, because every client computes
the *same* deterministic delay sequence (1s, 2s, 4s, 8s...). Adding a small
random nudge ("equal jitter") helps but still leaves the retries clustered
around the deterministic mean.

Full jitter -- sleeping a uniform random amount in ``[0, computed_delay]``
-- spreads the retry attempts evenly across the whole backoff window. It has
the same worst-case delay as plain exponential backoff, a lower mean delay,
and it decorrelates clients from one another. It is the approach AWS
documented after measuring the alternatives, and it is what we use here.

Testability
-----------
Both the sleep function and the random source are injectable. Tests pass a
recorder for ``sleep`` and a deterministic callable for ``rand``, so the
whole module runs in microseconds with zero wall-clock time and zero
flakiness. Production code simply omits both arguments and gets
``time.sleep`` and ``random.random``.
"""

from __future__ import annotations

import functools
import random
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

__all__ = [
    "DEFAULT_RETRYABLE_STATUS",
    "HttpError",
    "RetryError",
    "RetryPolicy",
    "TransientError",
    "compute_delay",
    "retry",
    "retry_call",
]

T = TypeVar("T")

#: HTTP status codes that are worth retrying.
#:
#: 429 is the only 4xx in the set: it is the server explicitly telling us to
#: slow down and come back, which is a retry instruction, not a client bug.
#: Every other 4xx describes a request that will fail identically forever.
#: The 5xx entries cover the transient server-side failures that show up
#: during deploys, failovers and capacity events.
DEFAULT_RETRYABLE_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})


class TransientError(Exception):
    """A failure the caller believes is worth retrying.

    Provided as a convenience so callers that are not speaking HTTP (a flaky
    file share, a database failover) have something meaningful to raise
    without inventing their own type.
    """


class HttpError(Exception):
    """An HTTP-shaped failure carrying a status code and optional Retry-After.

    ``synckit`` deliberately does not ship an HTTP client -- the whole point
    of the zero-dependency constraint is that you bring your own transport.
    This exception is the narrow contract between your transport and our
    retry logic: raise it (or anything exposing ``status_code`` and
    ``retry_after``) and the policy can make an informed decision.
    """

    def __init__(
        self,
        status_code: int,
        message: str | None = None,
        retry_after: float | None = None,
    ) -> None:
        self.status_code = int(status_code)
        self.retry_after = retry_after
        super().__init__(message or f"HTTP {status_code}")

    def __str__(self) -> str:  # pragma: no cover - trivial
        return super().__str__()


class RetryError(Exception):
    """Raised when every attempt has been used up.

    The final underlying exception is attached both as ``__cause__`` (so
    tracebacks read naturally) and as ``.last_exception`` (so callers can
    branch on it without walking the exception chain).
    """

    def __init__(self, attempts: int, last_exception: BaseException) -> None:
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(f"giving up after {attempts} attempt(s): {last_exception!r}")


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for how hard, and how politely, to retry.

    Attributes
    ----------
    max_attempts:
        Total attempts including the first one. ``max_attempts=1`` disables
        retrying entirely, which is a legitimate configuration for a
        non-idempotent write you would rather fail loudly.
    base_delay:
        The first backoff interval in seconds. Delay grows as
        ``base_delay * 2 ** (attempt - 1)``.
    max_delay:
        Ceiling applied before jitter. Without a ceiling, a long-running job
        with eight attempts and a one-second base would eventually sleep for
        over two minutes, which usually exceeds the job's own timeout.
    jitter:
        When true, the actual sleep is uniform in ``[0, delay]`` (full
        jitter). When false the delay is used verbatim -- useful in tests
        and in single-client batch jobs where herding cannot occur.
    retryable_exceptions:
        Exception types that trigger a retry. Defaults to
        :class:`TransientError` plus :class:`OSError`, which covers socket
        and DNS failures from every stdlib transport.
    retryable_status:
        HTTP status codes that trigger a retry.
    respect_retry_after:
        When true, a ``Retry-After`` value supplied by the server overrides
        our computed backoff. The server knows when its quota window resets
        and we do not, so ignoring that hint is both rude and slower.
    max_retry_after:
        Safety valve for the above. Some providers return a Retry-After of
        several hours during an incident; sleeping that long inside a
        scheduled job just burns runner minutes until the job is killed.
    """

    max_attempts: int = 4
    base_delay: float = 0.5
    max_delay: float = 30.0
    jitter: bool = True
    retryable_exceptions: tuple[type[BaseException], ...] = (TransientError, OSError)
    retryable_status: frozenset[int] = field(default=DEFAULT_RETRYABLE_STATUS)
    respect_retry_after: bool = True
    max_retry_after: float = 300.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0:
            raise ValueError("base_delay must be >= 0")
        if self.max_delay < 0:
            raise ValueError("max_delay must be >= 0")
        if self.max_retry_after < 0:
            raise ValueError("max_retry_after must be >= 0")
        # Normalise so callers can pass any iterable of ints and still get
        # the cheap, hashable, immutable membership test we rely on.
        object.__setattr__(self, "retryable_status", frozenset(self.retryable_status))
        object.__setattr__(self, "retryable_exceptions", tuple(self.retryable_exceptions))

    def is_retryable(self, exc: BaseException) -> bool:
        """Return whether ``exc`` should be retried under this policy.

        Status codes are checked first and are authoritative. This matters
        because :class:`HttpError` could be added to
        ``retryable_exceptions`` by a careless caller, and we still must not
        retry a 403 just because its exception class is on the list.
        """
        status = getattr(exc, "status_code", None)
        if isinstance(status, int):
            return status in self.retryable_status
        return isinstance(exc, self.retryable_exceptions)

    def backoff_for(self, attempt: int) -> float:
        """Un-jittered backoff for a 1-based attempt number."""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        # 2 ** (attempt - 1) grows fast; cap before it overflows into a
        # float the caller can never actually wait out.
        exponent = min(attempt - 1, 32)
        return min(self.base_delay * (2.0**exponent), self.max_delay)


def _coerce_retry_after(value: Any) -> float | None:
    """Best-effort parse of a ``Retry-After`` value into seconds.

    Only the delta-seconds form is supported. The HTTP-date form exists in
    the spec but is rare in JSON APIs, and parsing it correctly requires
    knowing the server's clock skew -- a source of bugs we would rather not
    own. Unparseable values fall back to our own computed backoff.
    """
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    if seconds < 0:
        return None
    return seconds


def compute_delay(
    policy: RetryPolicy,
    attempt: int,
    *,
    retry_after: Any = None,
    rand: Callable[[], float] | None = None,
) -> float:
    """Return how long to sleep before the next attempt.

    Precedence is deliberate: an explicit server hint beats our guess, and
    jitter is *not* applied on top of a server hint. If the server said
    "come back in 30 seconds", coming back in a random 0-30 seconds means
    coming back too early and eating another 429.
    """
    if policy.respect_retry_after:
        hinted = _coerce_retry_after(retry_after)
        if hinted is not None:
            return min(hinted, policy.max_retry_after)

    delay = policy.backoff_for(attempt)
    if not policy.jitter or delay == 0:
        return delay
    source = rand if rand is not None else random.random
    # Full jitter: uniform over the whole window, not a wobble around it.
    return delay * source()


def retry_call(
    func: Callable[..., T],
    *args: Any,
    policy: RetryPolicy | None = None,
    sleep: Callable[[float], None] | None = None,
    rand: Callable[[], float] | None = None,
    on_retry: Callable[[int, float, BaseException], None] | None = None,
    **kwargs: Any,
) -> T:
    """Call ``func`` under ``policy``, retrying retryable failures.

    Parameters
    ----------
    on_retry:
        Optional observer invoked as ``(attempt, delay, exception)`` before
        each sleep. This is the hook for structured logging and metrics; the
        retry module itself stays logging-free so it can be used from
        contexts where the logging config is not yet installed.

    Raises
    ------
    RetryError
        When all attempts are exhausted on a retryable failure.
    BaseException
        Non-retryable exceptions propagate unchanged and immediately. We
        re-raise the original rather than wrapping it, because the caller's
        ``except ValueError`` should still work.
    """
    policy = policy or RetryPolicy()
    sleeper = sleep if sleep is not None else time.sleep

    last_exc: BaseException | None = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001 - re-raised or wrapped below
            if not policy.is_retryable(exc):
                raise
            last_exc = exc
            if attempt >= policy.max_attempts:
                break
            delay = compute_delay(
                policy,
                attempt,
                retry_after=getattr(exc, "retry_after", None),
                rand=rand,
            )
            if on_retry is not None:
                on_retry(attempt, delay, exc)
            if delay > 0:
                sleeper(delay)

    assert last_exc is not None  # only reachable via the retryable break
    raise RetryError(policy.max_attempts, last_exc) from last_exc


def retry(
    policy: RetryPolicy | None = None,
    *,
    sleep: Callable[[float], None] | None = None,
    rand: Callable[[], float] | None = None,
    on_retry: Callable[[int, float, BaseException], None] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator form of :func:`retry_call`.

    Kept as a separate factory rather than a dual-purpose callable because
    decorators that also work as direct calls are a well-known source of
    "why is my function returning a decorator" bugs.

    Example
    -------
    >>> policy = RetryPolicy(max_attempts=3, base_delay=0.1)
    >>> @retry(policy)
    ... def fetch_page(cursor):  # doctest: +SKIP
    ...     return client.get("/contacts", params={"cursor": cursor})
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return retry_call(
                func,
                *args,
                policy=policy,
                sleep=sleep,
                rand=rand,
                on_retry=on_retry,
                **kwargs,
            )

        # Expose the policy so operators can introspect a decorated function
        # at runtime instead of reading the source to find out how many
        # attempts it makes.
        wrapper.retry_policy = policy or RetryPolicy()  # type: ignore[attr-defined]
        return wrapper

    return decorator


def retryable_statuses(extra: Iterable[int] | Sequence[int] = ()) -> frozenset[int]:
    """Return the default retryable status set widened by ``extra``.

    Some providers use non-standard codes for throttling (Shopify's 430,
    Cloudflare's 522). Rather than encourage people to redefine the whole
    set and accidentally drop 503, we give them an additive helper.
    """
    return DEFAULT_RETRYABLE_STATUS | frozenset(int(code) for code in extra)
