"""synckit -- building blocks for scheduled API synchronisation jobs.

A small, dependency-free toolkit for the kind of job that runs on a cron
schedule, reads from one REST API, and writes into another: CRM to warehouse,
accounting system to reporting table, HR directory to access control.

The five pieces are independent -- use one or all of them:

===============  ==========================================================
``retry``        Exponential backoff with full jitter, an explicit retryable
                 status allow-list, and ``Retry-After`` support.
``ratelimit``    Thread-safe token bucket with an injectable clock.
``checkpoint``   Durable, atomically written cursor storage for resumability.
``logging``      JSON log formatting with recursive secret redaction.
``runner``       The read/transform/write loop, with a real dry-run mode.
===============  ==========================================================

Usage
-----
::

    from synckit import JsonCheckpointStore, Page, RetryPolicy, SyncRunner, TokenBucket

    def source(cursor):
        payload = crm.get("/contacts", params={"cursor": cursor, "limit": 200})
        return Page(records=payload["results"], next_cursor=payload.get("next"))

    def transform(record):
        if not record.get("email"):
            return None            # skipped, not failed
        return {"external_id": record["id"], "email": record["email"].lower()}

    def sink(batch):
        warehouse.upsert("contacts", batch)
        return len(batch)

    runner = SyncRunner(
        source,
        sink,
        transform=transform,
        store=JsonCheckpointStore(".state/contacts.json"),
        rate_limiter=TokenBucket(rate=10, capacity=20),
        retry_policy=RetryPolicy(max_attempts=5),
        job="crm-contacts",
        dry_run=True,              # prove it first, then flip to False
    )
    result = runner.run()
    print(result)

Every component that touches time or randomness accepts an injection point,
which is why the test suite runs in well under a second with no sleeping and
no flakiness.
"""

from __future__ import annotations

from synckit.checkpoint import (
    Checkpoint,
    CheckpointError,
    InMemoryCheckpointStore,
    JsonCheckpointStore,
    atomic_write_json,
)
from synckit.logging import (
    DEFAULT_SECRET_KEY_PATTERN,
    REDACTED,
    JsonFormatter,
    RedactionFilter,
    configure_logging,
    redact,
)
from synckit.ratelimit import RateLimitTimeout, TokenBucket, TokenBucketStats
from synckit.retry import (
    DEFAULT_RETRYABLE_STATUS,
    HttpError,
    RetryError,
    RetryPolicy,
    TransientError,
    compute_delay,
    retry,
    retry_call,
)
from synckit.runner import Page, SkipRecord, SyncError, SyncResult, SyncRunner, batched

__version__ = "0.3.0"

__all__ = [
    "DEFAULT_RETRYABLE_STATUS",
    "DEFAULT_SECRET_KEY_PATTERN",
    "REDACTED",
    "Checkpoint",
    "CheckpointError",
    "HttpError",
    "InMemoryCheckpointStore",
    "JsonCheckpointStore",
    "JsonFormatter",
    "Page",
    "RateLimitTimeout",
    "RedactionFilter",
    "RetryError",
    "RetryPolicy",
    "SkipRecord",
    "SyncError",
    "SyncResult",
    "SyncRunner",
    "TokenBucket",
    "TokenBucketStats",
    "TransientError",
    "__version__",
    "atomic_write_json",
    "batched",
    "compute_delay",
    "configure_logging",
    "redact",
    "retry",
    "retry_call",
]
