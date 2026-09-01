"""Shared deterministic test doubles.

Every one of these exists so that no test needs to sleep, look at the wall
clock, or depend on a real random number. A test suite for retry and rate
limiting code that uses real time is a test suite that is slow locally and
intermittently red on a busy CI runner.
"""

from __future__ import annotations

from typing import Any

import pytest


class FakeClock:
    """A monotonic clock that only moves when a test tells it to."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = float(start)

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> float:
        self.now += seconds
        return self.now


class FakeSleep:
    """Records requested sleeps and, optionally, advances a FakeClock."""

    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        if self.clock is not None:
            self.clock.advance(seconds)

    @property
    def total(self) -> float:
        return sum(self.calls)

    @property
    def count(self) -> int:
        return len(self.calls)


class SequenceRandom:
    """Deterministic stand-in for ``random.random``.

    Cycles through the supplied values so a test can pin the exact jitter
    multiplier applied at each attempt.
    """

    def __init__(self, values: list[float]) -> None:
        if not values:
            raise ValueError("values must not be empty")
        self.values = values
        self.index = 0

    def __call__(self) -> float:
        value = self.values[self.index % len(self.values)]
        self.index += 1
        return value


class FlakyCallable:
    """Fails ``failures`` times with ``exc_factory()``, then returns ``result``."""

    def __init__(self, failures: int, exc_factory: Any, result: Any = "ok") -> None:
        self.failures = failures
        self.exc_factory = exc_factory
        self.result = result
        self.calls = 0
        self.args: list[tuple[Any, ...]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        self.args.append(args)
        if self.calls <= self.failures:
            raise self.exc_factory()
        return self.result


class RecordingSink:
    """Sink that stores every batch it is handed."""

    def __init__(self, fail_on_batch: int | None = None, exc: Any = None) -> None:
        self.batches: list[list[Any]] = []
        self.fail_on_batch = fail_on_batch
        self.exc = exc or RuntimeError("sink exploded")

    def __call__(self, batch: Any) -> int:
        self.batches.append(list(batch))
        if self.fail_on_batch is not None and len(self.batches) == self.fail_on_batch:
            raise self.exc
        return len(batch)

    @property
    def written(self) -> list[Any]:
        return [record for batch in self.batches for record in batch]


def paged_source(pages: list[list[Any]]) -> Any:
    """Build a source callable that walks ``pages`` using integer cursors."""
    from synckit.runner import Page

    calls: list[Any] = []

    def source(cursor: Any) -> Page:
        calls.append(cursor)
        index = 0 if cursor is None else int(cursor)
        records = pages[index] if index < len(pages) else []
        next_cursor = index + 1 if index + 1 < len(pages) else None
        return Page(records=records, next_cursor=next_cursor)

    source.calls = calls  # type: ignore[attr-defined]
    return source


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def sleeper(clock: FakeClock) -> FakeSleep:
    return FakeSleep(clock)
