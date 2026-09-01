"""Thread-safe token bucket rate limiting.

Why a token bucket and not a fixed window
-----------------------------------------
The naive approach to "600 requests per minute" is a fixed window: count
requests, reset the counter at the top of each minute. It is easy to write
and it is wrong in a specific, expensive way.

Consider a limit of 600/minute. A client that sends 600 requests in the last
second of 12:00 and another 600 in the first second of 12:01 has satisfied
the fixed-window rule perfectly, while delivering 1200 requests in a two
second span -- a 2x burst straight through the boundary. Upstream sees a
spike it never agreed to absorb, and the usual outcome is a 429 storm or,
worse, a silent tail-latency collapse for every other consumer of that API.

A token bucket does not have boundaries to exploit. Tokens accrue
continuously at ``rate`` per second up to a ceiling of ``capacity``. A
request costs a token. Once the bucket is empty the caller waits for the
next token to accrue, so the long-run rate is bounded no matter how the
requests are aligned in time. The ``capacity`` parameter is what lets you
*deliberately* allow a burst -- for example, letting a nightly sync fire 50
requests immediately to warm up, then settle to 10/s -- instead of getting
bursts by accident.

Sliding-window log algorithms give similar smoothness but require storing a
timestamp per request, which is unbounded memory for a job that makes a
hundred thousand calls. The bucket is O(1) in both time and space.

Why the clock is injectable
---------------------------
The only interesting behaviour of a rate limiter is what it does over time.
Testing that against the wall clock means either sleeping for real (slow,
and flaky on a loaded CI runner) or asserting on tolerances (flaky in a
different way). With an injectable monotonic clock the tests advance time by
hand and assert exact token counts.

``time.monotonic`` is the default rather than ``time.time`` because the
wall clock can jump backwards -- NTP correction, a VM resuming from a
snapshot, a daylight-saving change on a badly configured host. A backwards
jump in a rate limiter keyed on wall time either grants infinite tokens or
stalls the job for hours. Monotonic time cannot go backwards by definition.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from types import TracebackType

__all__ = ["RateLimitTimeout", "TokenBucket", "TokenBucketStats"]


class RateLimitTimeout(Exception):
    """Raised when tokens could not be acquired within the allowed wait."""


@dataclass(frozen=True)
class TokenBucketStats:
    """Point-in-time snapshot of a bucket, for logging and assertions."""

    capacity: float
    rate: float
    tokens: float
    acquired: int
    throttled: int
    total_wait: float


class TokenBucket:
    """A thread-safe token bucket limiter.

    Parameters
    ----------
    rate:
        Steady-state tokens added per second. This is the number you copy
        out of the provider's published quota.
    capacity:
        Maximum tokens the bucket can hold, i.e. the largest burst allowed
        from a standing start. Defaults to ``rate`` (one second of burst),
        which is the conservative choice: it smooths traffic without letting
        an idle job dump an hour of accrued quota in one go.
    clock:
        Zero-argument callable returning monotonically increasing seconds.
    sleep:
        Callable used to wait for tokens. Injectable so tests never block.
        Note that the lock is *released* around the sleep -- holding a mutex
        while sleeping would serialise every worker behind the first one and
        turn a rate limiter into a queue.
    initial_tokens:
        Starting fill level. Defaults to full, so a freshly started job may
        burst up to ``capacity``. Set to ``0`` when several processes share
        one quota and you would rather nobody starts hot.
    """

    def __init__(
        self,
        rate: float,
        capacity: float | None = None,
        *,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
        initial_tokens: float | None = None,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if capacity is None:
            capacity = rate
        if capacity <= 0:
            raise ValueError("capacity must be > 0")

        self.rate = float(rate)
        self.capacity = float(capacity)
        self._clock = clock if clock is not None else time.monotonic
        self._sleep = sleep if sleep is not None else time.sleep

        if initial_tokens is None:
            initial_tokens = self.capacity
        if initial_tokens < 0:
            raise ValueError("initial_tokens must be >= 0")
        self._tokens = min(float(initial_tokens), self.capacity)

        self._updated_at = self._clock()
        # RLock rather than Lock so that a subclass overriding acquire() can
        # safely call into peek()/stats() without deadlocking itself.
        self._lock = threading.RLock()

        self._acquired = 0
        self._throttled = 0
        self._total_wait = 0.0

    # -- internals ---------------------------------------------------------

    def _refill_locked(self) -> None:
        """Accrue tokens for elapsed time. Caller must hold the lock."""
        now = self._clock()
        elapsed = now - self._updated_at
        if elapsed <= 0:
            # A non-advancing or (with a hand-rolled clock) backwards clock
            # must never mint tokens. Re-anchor and move on.
            self._updated_at = now
            return
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._updated_at = now

    def _wait_time_locked(self, tokens: float) -> float:
        """Seconds until ``tokens`` are available. Caller must hold the lock."""
        deficit = tokens - self._tokens
        if deficit <= 0:
            return 0.0
        return deficit / self.rate

    # -- public API --------------------------------------------------------

    @property
    def tokens(self) -> float:
        """Current token count, refilled to *now* before reading."""
        with self._lock:
            self._refill_locked()
            return self._tokens

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Take ``tokens`` if available right now; never blocks.

        This is the right call inside an event loop or any place where
        blocking is unacceptable -- the caller decides what to do with the
        rejection (drop, queue, shed load) instead of having a sleep imposed
        on it.
        """
        if tokens <= 0:
            raise ValueError("tokens must be > 0")
        if tokens > self.capacity:
            # Structurally impossible, not merely unavailable. Fail loudly at
            # the call site rather than blocking forever at runtime.
            raise ValueError(
                f"cannot acquire {tokens} tokens from a bucket of capacity {self.capacity}"
            )
        with self._lock:
            self._refill_locked()
            if self._tokens >= tokens:
                self._tokens -= tokens
                self._acquired += 1
                return True
            return False

    def acquire(self, tokens: float = 1.0, *, timeout: float | None = None) -> float:
        """Block until ``tokens`` are available; return the seconds waited.

        Raises
        ------
        RateLimitTimeout
            If ``timeout`` is set and the required wait exceeds it. A sync
            job with a hard runtime budget wants this: better to end the
            batch early and resume from the checkpoint next run than to be
            killed mid-write by the runner's own timeout.
        """
        if tokens <= 0:
            raise ValueError("tokens must be > 0")
        if tokens > self.capacity:
            raise ValueError(
                f"cannot acquire {tokens} tokens from a bucket of capacity {self.capacity}"
            )

        waited = 0.0
        while True:
            with self._lock:
                self._refill_locked()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    self._acquired += 1
                    self._total_wait += waited
                    return waited
                wait = self._wait_time_locked(tokens)
                self._throttled += 1

            if timeout is not None and waited + wait > timeout:
                raise RateLimitTimeout(
                    f"need {wait:.3f}s more for {tokens} token(s), "
                    f"which exceeds the {timeout:.3f}s budget"
                )
            # Sleep outside the lock: see the class docstring.
            self._sleep(wait)
            waited += wait

    def stats(self) -> TokenBucketStats:
        """Snapshot of configuration and counters, safe to log."""
        with self._lock:
            self._refill_locked()
            return TokenBucketStats(
                capacity=self.capacity,
                rate=self.rate,
                tokens=self._tokens,
                acquired=self._acquired,
                throttled=self._throttled,
                total_wait=self._total_wait,
            )

    def reset(self, tokens: float | None = None) -> None:
        """Refill to ``tokens`` (default: full) and re-anchor the clock.

        Useful when a provider tells you the quota window has reset -- for
        example after a successful response carrying fresh quota headers.
        Counters are intentionally preserved so run-level totals stay true.
        """
        with self._lock:
            self._tokens = self.capacity if tokens is None else min(float(tokens), self.capacity)
            self._updated_at = self._clock()

    # Context-manager sugar: ``with bucket: ...`` costs one token.
    def __enter__(self) -> TokenBucket:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Tokens are deliberately *not* returned on failure. A request that
        # errored still consumed upstream capacity, and in the 429 case it
        # consumed more than its share.
        return None

    def __repr__(self) -> str:
        return (
            f"TokenBucket(rate={self.rate!r}, capacity={self.capacity!r}, "
            f"tokens={self.tokens:.3f})"
        )
