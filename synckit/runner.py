"""The sync loop: read pages, transform records, write batches, checkpoint.

What this ties together
-----------------------
The other modules are independent tools. :class:`SyncRunner` is the opinion
about how they fit: pull a page from the source, run each record through a
transform, hand the surviving records to the sink, record a checkpoint, and
only then ask for the next page. Rate limiting wraps the source and sink
calls; retries wrap them too; every interesting event is logged as
structured JSON.

Why dry run is non-negotiable
-----------------------------
This framework exists to write into other people's production systems -- a
CRM that the sales team is looking at right now, an accounting ledger, a
staff directory that controls door access. The failure mode of a bad sync is
not a stack trace, it is four thousand contacts with their owner field
overwritten, discovered on Monday morning, with no undo button. Restoring a
CRM from backup means taking it offline.

So every change to a sync -- a new transform rule, a different field
mapping, a widened date filter -- gets run with ``dry_run=True`` first. The
requirement that makes dry run trustworthy is that it takes *exactly the
same code path*: same pagination, same transforms, same batching, same
counters, same logs. The only difference is that the sink is swapped for a
recorder and the checkpoint store for an in-memory one. A dry run
implemented as a separate branch is a dry run that tests different code from
the one that will actually execute, which is worse than none because it
manufactures confidence.

The counters returned from a dry run therefore answer the question people
actually care about before a deploy: *how many records would this touch?*
If the answer to "sync one day of updates" is 190,000, something in the
filter is wrong and you found out for free.

Why checkpoint after each batch rather than at the end
------------------------------------------------------
Checkpointing at the end of a run is checkpointing that only works when
nothing goes wrong, which is when you do not need it. We save after each
batch the sink confirmed, so an interruption costs at most one batch of
re-processing. That does require the sink to be idempotent for the batch
that was in flight when the process died -- upserting on a stable external
id is the usual way -- and that requirement is documented rather than
hidden, because there is no way to have exactly-once delivery across two
systems that do not share a transaction.

Failure policy
--------------
Two kinds of failure are distinguished deliberately:

* A *record* failure (the transform raised on one malformed row) is
  isolated. One contact with a null email should not abandon 400,000 good
  ones. It is counted, logged with the record's id, and the run continues --
  unless ``fail_fast`` is set.
* A *batch* failure (the sink rejected the write after retries) stops the
  run by default. The checkpoint is not advanced, so the next run retries
  from the last confirmed position. Continuing past a failed write would
  silently skip records and produce the worst outcome in the whole design
  space: a job that reports success while losing data.
"""

from __future__ import annotations

import logging as _stdlib_logging
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

from synckit.checkpoint import Checkpoint, InMemoryCheckpointStore
from synckit.ratelimit import TokenBucket
from synckit.retry import RetryPolicy, retry_call

__all__ = [
    "Page",
    "SkipRecord",
    "SyncError",
    "SyncResult",
    "SyncRunner",
]


class SkipRecord(Exception):
    """Raise from a transform to drop a record without counting it as failed.

    Filtering inside the transform is common ("ignore contacts with no
    email") and it is not an error. Returning ``None`` does the same thing;
    the exception exists for transforms that decide deep inside a helper
    call, where threading a sentinel back up is awkward.
    """


class SyncError(Exception):
    """Raised when the run aborts on an unrecoverable batch failure."""


@dataclass
class Page:
    """One page of records from the source, plus the cursor for the next.

    ``next_cursor is None`` terminates the run. That is the explicit signal
    rather than "an empty page", because plenty of APIs return an empty page
    in the middle of a keyset-paginated scan when a whole page of rows was
    filtered server-side, and stopping there would silently truncate.
    """

    records: Sequence[Any] = field(default_factory=list)
    next_cursor: Any = None

    def __len__(self) -> int:
        return len(self.records)


@dataclass
class SyncResult:
    """Summary of a completed (or aborted) run.

    Everything an operator needs in one object, so the job's final log line
    and its exit status come from the same source of truth.
    """

    job: str
    run_id: str
    dry_run: bool
    records_read: int = 0
    records_written: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    batches_written: int = 0
    pages_fetched: int = 0
    duration: float = 0.0
    cursor: Any = None
    started_at: float = 0.0
    completed: bool = False
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True when the run finished the stream with nothing failed.

        Used as the process exit condition. A run that completed but failed
        twelve records is *not* ok -- silent partial failure is the specific
        thing this framework exists to prevent.
        """
        return self.completed and self.records_failed == 0 and not self.errors

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["ok"] = self.ok
        return data

    def __str__(self) -> str:
        mode = "DRY RUN" if self.dry_run else "LIVE"
        return (
            f"[{mode}] {self.job}: read={self.records_read} written={self.records_written} "
            f"skipped={self.records_skipped} failed={self.records_failed} "
            f"batches={self.batches_written} in {self.duration:.2f}s"
        )


class SyncRunner:
    """Drive a paginated read/transform/write loop with checkpointing.

    Parameters
    ----------
    source:
        ``source(cursor) -> Page | (records, next_cursor) | Sequence``. Called
        with ``None`` on a cold start, otherwise with the stored cursor. A
        bare sequence is treated as a single terminal page, which keeps
        trivial jobs and tests from having to build a :class:`Page`.
    sink:
        ``sink(batch) -> int | None``. Receives the transformed records for
        one batch. The optional int return is the number actually written,
        which matters for upserts where the sink deduplicates; when it
        returns ``None`` we assume every record in the batch was written.
    transform:
        ``transform(record) -> record | None``. ``None`` (or
        :class:`SkipRecord`) skips. Defaults to identity.
    store:
        Checkpoint store. Defaults to an in-memory one, so a runner
        constructed without state still works -- it just cannot resume.
    dry_run:
        When true the real sink and store are never called. See the module
        docstring.
    batch_size:
        Records per sink call. Independent of the source's page size on
        purpose: the read side is tuned by the upstream API's page limits,
        the write side by the downstream API's bulk endpoint limits, and
        those two numbers are rarely the same.
    rate_limiter:
        Optional :class:`~synckit.ratelimit.TokenBucket` consulted before
        each source and sink call.
    retry_policy:
        Applied to source and sink calls. Transforms are *not* retried:
        they are pure local code, so a failure is a bug or bad data, and
        retrying it just fails three more times more slowly.
    max_records:
        Stop after this many records have been read. The safety rail for a
        first live run against an unfamiliar dataset.
    fail_fast:
        Abort on the first record-level failure instead of isolating it.
    clock:
        Injectable monotonic time source for the duration measurement.
    """

    def __init__(
        self,
        source: Callable[[Any], Any],
        sink: Callable[[Sequence[Any]], Any],
        *,
        transform: Callable[[Any], Any] | None = None,
        store: Any = None,
        job: str = "default",
        dry_run: bool = False,
        batch_size: int = 100,
        rate_limiter: TokenBucket | None = None,
        retry_policy: RetryPolicy | None = None,
        max_records: int | None = None,
        max_pages: int | None = None,
        fail_fast: bool = False,
        continue_on_batch_error: bool = False,
        logger: Any = None,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
        run_id: str | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if max_records is not None and max_records < 0:
            raise ValueError("max_records must be >= 0")

        self.source = source
        self.sink = sink
        self.transform = transform
        self.job = job
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.rate_limiter = rate_limiter
        self.retry_policy = retry_policy or RetryPolicy()
        self.max_records = max_records
        self.max_pages = max_pages
        self.fail_fast = fail_fast
        self.continue_on_batch_error = continue_on_batch_error
        self._clock = clock if clock is not None else time.monotonic
        self._sleep = sleep
        self.run_id = run_id or uuid.uuid4().hex[:12]

        self.log = logger if logger is not None else _stdlib_logging.getLogger("synckit")

        # A dry run gets a throwaway store so it can exercise the identical
        # "load then save after each batch" path without touching real state.
        self._real_store = store if store is not None else InMemoryCheckpointStore()
        self.store = InMemoryCheckpointStore() if dry_run else self._real_store

        #: Batches a dry run *would* have written, in order. This is the
        #: diffable artefact people actually review before going live.
        self.planned_writes: list[list[Any]] = []

    # -- helpers -----------------------------------------------------------

    def _throttle(self, tokens: float = 1.0) -> None:
        if self.rate_limiter is not None:
            self.rate_limiter.acquire(tokens)

    def _call_with_retry(self, func: Callable[..., Any], *args: Any) -> Any:
        return retry_call(
            func,
            *args,
            policy=self.retry_policy,
            sleep=self._sleep,
            on_retry=self._on_retry,
        )

    def _on_retry(self, attempt: int, delay: float, exc: BaseException) -> None:
        self.log.warning(
            "retrying after failure",
            extra={
                "job": self.job,
                "run_id": self.run_id,
                "attempt": attempt,
                "delay": round(delay, 3),
                "error": repr(exc),
            },
        )

    @staticmethod
    def _coerce_page(raw: Any) -> Page:
        """Normalise whatever the source returned into a :class:`Page`."""
        if isinstance(raw, Page):
            return raw
        if raw is None:
            return Page(records=[], next_cursor=None)
        if isinstance(raw, tuple) and len(raw) == 2:
            records, next_cursor = raw
            return Page(records=list(records), next_cursor=next_cursor)
        if isinstance(raw, dict):
            return Page(
                records=list(raw.get("records", [])),
                next_cursor=raw.get("next_cursor"),
            )
        if isinstance(raw, (list, tuple)):
            # A bare sequence is one final page: no cursor means stop.
            return Page(records=list(raw), next_cursor=None)
        raise TypeError(f"source returned an unsupported type: {type(raw).__name__}")

    def _apply_transform(self, record: Any) -> tuple[bool, Any]:
        """Return ``(keep, value)`` for one record."""
        if self.transform is None:
            return True, record
        try:
            result = self.transform(record)
        except SkipRecord:
            return False, None
        if result is None:
            return False, None
        return True, result

    def _record_id(self, record: Any) -> Any:
        """Best-effort identifier for logging a failure usefully.

        A failure log that says "a record failed" is nearly worthless during
        an incident; one that names the id lets you go look at the row.
        """
        if isinstance(record, dict):
            for key in ("id", "record_id", "external_id", "uuid", "pk"):
                if key in record:
                    return record[key]
            return None
        return getattr(record, "id", None)

    def _write_batch(self, batch: list[Any]) -> int:
        """Write one batch (or record the intent, in dry-run mode)."""
        if self.dry_run:
            self.planned_writes.append(list(batch))
            return len(batch)
        self._throttle()
        written = self._call_with_retry(self.sink, batch)
        if written is None:
            return len(batch)
        return int(written)

    # -- main loop ---------------------------------------------------------

    def iter_pages(self, cursor: Any = None) -> Iterator[Page]:
        """Yield pages from the source until the cursor runs out.

        Split out from :meth:`run` so it can be reused (and tested) on its
        own -- for example by a job that only wants to count what is
        upstream without touching a sink.
        """
        for _, page in self._iter_pages_with_cursor(cursor):
            yield page

    def _iter_pages_with_cursor(self, cursor: Any = None) -> Iterator[tuple[Any, Page]]:
        """Yield ``(cursor_used, page)`` pairs.

        The cursor that *fetched* a page is what we checkpoint to while that
        page is still being written, because resuming from it re-reads the
        page we were partway through rather than skipping its tail.
        """
        pages = 0
        while True:
            if self.max_pages is not None and pages >= self.max_pages:
                return
            self._throttle()
            page = self._coerce_page(self._call_with_retry(self.source, cursor))
            pages += 1
            yield cursor, page
            if page.next_cursor is None:
                return
            if page.next_cursor == cursor:
                # Defensive: a source that echoes the cursor back would spin
                # forever, hammering the API until the job timeout kills it.
                raise SyncError(
                    f"source returned an unchanged cursor ({cursor!r}); "
                    "this would loop forever"
                )
            cursor = page.next_cursor

    def run(self, *, start_cursor: Any = None, resume: bool = True) -> SyncResult:
        """Execute the sync and return a :class:`SyncResult`.

        Parameters
        ----------
        start_cursor:
            Explicit override, used for a targeted backfill.
        resume:
            When true (default) and no explicit cursor is given, the stored
            checkpoint is used. Pass ``False`` to force a full re-sync
            without having to delete state.
        """
        started_at = self._clock()
        checkpoint = self._real_store.load(self.job)
        if start_cursor is not None:
            cursor: Any = start_cursor
        elif resume:
            cursor = checkpoint.cursor
        else:
            cursor = None

        result = SyncResult(
            job=self.job,
            run_id=self.run_id,
            dry_run=self.dry_run,
            cursor=cursor,
            started_at=started_at,
        )

        self.log.info(
            "sync starting",
            extra={
                "job": self.job,
                "run_id": self.run_id,
                "dry_run": self.dry_run,
                "resume_cursor": cursor,
                "batch_size": self.batch_size,
            },
        )

        batch: list[Any] = []
        aborted = False
        last_page_cursor: Any = cursor

        try:
            for page_cursor, page in self._iter_pages_with_cursor(cursor):
                result.pages_fetched += 1
                # Position to resume from while this page is in flight.
                last_page_cursor = page_cursor
                for record in page.records:
                    if self.max_records is not None and result.records_read >= self.max_records:
                        break
                    result.records_read += 1
                    try:
                        keep, value = self._apply_transform(record)
                    except Exception as exc:  # transform bugs and bad data
                        result.records_failed += 1
                        message = f"transform failed for record {self._record_id(record)!r}: {exc}"
                        result.errors.append(message)
                        self.log.error(
                            "record transform failed",
                            extra={
                                "job": self.job,
                                "run_id": self.run_id,
                                "record_id": self._record_id(record),
                                "error": repr(exc),
                            },
                        )
                        if self.fail_fast:
                            raise SyncError(message) from exc
                        continue

                    if not keep:
                        result.records_skipped += 1
                        continue
                    batch.append(value)

                    if len(batch) >= self.batch_size:
                        # Checkpoint to the cursor that fetched *this* page,
                        # not the next one: the rest of this page is still
                        # unwritten, and resuming past it would lose records.
                        if not self._flush(batch, last_page_cursor, result, checkpoint):
                            aborted = True
                            break
                        batch = []

                if aborted:
                    break
                if self.max_records is not None and result.records_read >= self.max_records:
                    self.log.info(
                        "max_records reached",
                        extra={
                            "job": self.job,
                            "run_id": self.run_id,
                            "max_records": self.max_records,
                        },
                    )
                    break
                # Advance the checkpoint at page boundaries too, so a page
                # that produced no writes (everything filtered out) still
                # makes forward progress instead of being re-read forever.
                if not batch and page.next_cursor is not None:
                    result.cursor = page.next_cursor
                    self._save_checkpoint(checkpoint, page.next_cursor, result)

            if batch and not aborted:
                if not self._flush(batch, last_page_cursor, result, checkpoint):
                    aborted = True
                batch = []

            result.completed = not aborted
        finally:
            result.duration = self._clock() - started_at

        self.log.info(
            "sync finished",
            extra={"job": self.job, "run_id": self.run_id, **result.to_dict()},
        )
        return result

    # -- batch plumbing ----------------------------------------------------

    def _flush(
        self,
        batch: list[Any],
        cursor: Any,
        result: SyncResult,
        checkpoint: Checkpoint,
    ) -> bool:
        """Write one batch and checkpoint it. Returns False to abort the run."""
        try:
            written = self._write_batch(batch)
        except Exception as exc:  # includes RetryError once attempts are exhausted
            result.records_failed += len(batch)
            message = f"batch write failed ({len(batch)} records): {exc}"
            result.errors.append(message)
            self.log.error(
                "batch write failed",
                extra={
                    "job": self.job,
                    "run_id": self.run_id,
                    "batch_size": len(batch),
                    "error": repr(exc),
                },
            )
            if self.continue_on_batch_error:
                # Explicitly opted into by the caller. The checkpoint is
                # still not advanced past the failure, so nothing is lost
                # silently -- the run just keeps going to gather the full
                # picture of how bad the failure is.
                return True
            return False

        result.records_written += written
        result.batches_written += 1
        result.cursor = cursor
        self._save_checkpoint(checkpoint, result.cursor, result)
        self.log.info(
            "batch written",
            extra={
                "job": self.job,
                "run_id": self.run_id,
                "written": written,
                "cursor": result.cursor,
                "dry_run": self.dry_run,
            },
        )
        return True

    def _save_checkpoint(self, checkpoint: Checkpoint, cursor: Any, result: SyncResult) -> None:
        checkpoint.cursor = cursor
        checkpoint.records_processed = result.records_read
        checkpoint.job = self.job
        # Dry runs write to the throwaway store, so this line is safe in
        # both modes and there is no branch to get wrong.
        self.store.save(checkpoint)


def batched(items: Iterable[Any], size: int) -> Iterator[list[Any]]:
    """Yield ``items`` in lists of at most ``size``.

    ``itertools.batched`` exists from Python 3.12; this repository supports
    3.10, and a four-line helper is a better answer than a version check at
    every call site.
    """
    if size < 1:
        raise ValueError("size must be >= 1")
    chunk: list[Any] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
