# scheduled-api-sync

[![CI](https://github.com/averya34/scheduled-api-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/averya34/scheduled-api-sync/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Dependencies: zero](https://img.shields.io/badge/dependencies-zero-brightgreen)](pyproject.toml)

**`synckit`** is a small, dependency-free Python toolkit for scheduled data
synchronisation jobs — the kind that run on a cron schedule, read from one
REST API, and write into another. CRM to warehouse. Accounting system to
reporting table. HR directory to access control.

I built this because I kept writing the same four hundred lines. My day job is
operations systems engineering, and a recurring shape of work is a nightly sync
between a CRM and a CMS database table. Each time I wrote one, I rebuilt the
same scaffolding — retry with backoff, a rate limiter to stay inside the
provider's quota, somewhere to keep the cursor, a way to prove the change was
safe before it touched live data. Each rebuild got one of those pieces subtly
wrong, and I only found out at 3 a.m. So I extracted the scaffolding, wrote the
tests I wished I'd had, and made every piece of it injectable so the tests are
fast and never flaky.

---

## The problem

A scheduled sync is deceptively easy to write and genuinely hard to write well.
The naive version works on the happy path and fails in ways that are expensive
and quiet.

**Silent partial failure.** A loop with a `try/except: continue` around it
processes 400,000 records, drops 900 of them, and exits zero. The workflow is
green. Nobody looks. Three weeks later someone asks why the pipeline report
undercounts, and the answer is buried in a log that has already been rotated
away. A job that fails loudly is fine; a job that succeeds incorrectly is not.

**Thundering herd retries.** When an upstream API recovers from an outage, every
client that was backing off deterministically wakes up at the same instant and
re-sends at once. That surge is frequently what knocks the API back over, so
recovery turns into flapping. Fixed backoff causes it. Plain exponential backoff
causes it too, because every client computes the same delay sequence.

**No resumability.** A forty-minute sync that dies at minute thirty-eight —
runner reclaimed, token expired, upstream 503 — starts again from record zero.
That is forty wasted minutes and forty minutes of quota, every time, and it
means the write path has to survive re-processing the entire dataset.

**Secrets in logs.** Somebody adds `log.debug("request: %s", request)` while
chasing a bug and the `Authorization` header goes into a CI log that, on a
public repository, is world-readable. GitHub masks values registered as
repository secrets, but not a token minted at runtime, not a session cookie,
and not the customer email addresses in the payload.

**No dry run.** A one-line change to a field mapping overwrites the `owner`
field on four thousand live CRM contacts. There is no undo. Restoring from
backup means taking the CRM offline while the sales team is using it.

`synckit` is five modules that each address one of those, plus a runner that
composes them.

---

## Install

```bash
pip install git+https://github.com/averya34/scheduled-api-sync.git
```

Or clone it and work on it:

```bash
git clone https://github.com/averya34/scheduled-api-sync.git
cd scheduled-api-sync
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check . && pytest -q
```

Python 3.10 or newer. No runtime dependencies — the whole thing is standard
library. `pytest` and `ruff` are the only dev dependencies.

---

## Usage

A real job: mirror CRM contacts into a reporting table, resumably, inside the
provider's quota, without ever logging a token.

```python
import os
from synckit import (
    JsonCheckpointStore, Page, RetryPolicy, SyncRunner, TokenBucket, configure_logging,
)

log = configure_logging("INFO", logger_name="synckit", static_fields={"service": "crm-contacts"})


def source(cursor):
    """Return one page of upstream records plus the cursor for the next."""
    payload = crm.get("/v3/contacts", params={"after": cursor, "limit": 200})
    return Page(records=payload["results"], next_cursor=payload.get("paging", {}).get("next"))


def transform(record):
    """Normalise a record, or return None to skip it (skipped != failed)."""
    email = (record.get("email") or "").strip().lower()
    if not email:
        return None
    return {"external_id": str(record["id"]), "email": email, "stage": record["stage"]}


def sink(batch):
    """Write a batch. Return how many were actually written, or None for all."""
    warehouse.upsert("contacts", batch, on_conflict="external_id")
    return len(batch)


runner = SyncRunner(
    source,
    sink,
    transform=transform,
    store=JsonCheckpointStore(".state/contacts.json"),
    job="crm-contacts",
    batch_size=100,
    rate_limiter=TokenBucket(rate=10, capacity=20),      # 10/s steady, burst of 20
    retry_policy=RetryPolicy(max_attempts=5, base_delay=0.5, max_delay=30.0),
    dry_run=os.environ.get("DRY_RUN") == "1",
    logger=log,
)

result = runner.run()
log.info("run summary", extra=result.to_dict())
raise SystemExit(0 if result.ok else 1)
```

A dry run prints exactly what a live run would do, and writes nothing:

```text
[DRY RUN] crm-contacts: read=4183 written=4106 skipped=77 failed=0 batches=42 in 6.31s
```

A complete runnable version is in [`examples/sync_contacts.py`](examples/sync_contacts.py),
and [`.github/workflows/example-sync.yml`](.github/workflows/example-sync.yml)
shows it wired to a cron schedule with a `dry_run` dispatch input.

---

## Modules

| Module | What it gives you | Key types |
| --- | --- | --- |
| `synckit.retry` | Exponential backoff with full jitter, an explicit retryable-status allow-list, `Retry-After` support, injectable sleep and RNG | `RetryPolicy`, `retry`, `retry_call`, `HttpError`, `RetryError` |
| `synckit.ratelimit` | Thread-safe token bucket with burst capacity, steady refill, optional timeout, injectable clock | `TokenBucket`, `TokenBucketStats`, `RateLimitTimeout` |
| `synckit.checkpoint` | Durable cursor storage, atomic writes, corrupt-file quarantine and recovery | `Checkpoint`, `JsonCheckpointStore`, `InMemoryCheckpointStore`, `atomic_write_json` |
| `synckit.logging` | JSON log formatting with recursive secret and email redaction at any depth | `JsonFormatter`, `RedactionFilter`, `configure_logging`, `redact` |
| `synckit.runner` | The read/transform/write loop with per-batch checkpointing and a real dry-run mode | `SyncRunner`, `SyncResult`, `Page`, `SkipRecord` |

### Retry policy at a glance

| Status | Retried | Reason |
| --- | --- | --- |
| 429 | Yes | The server is explicitly telling us to come back later |
| 500, 502, 503, 504 | Yes | Transient server-side failures: deploys, failovers, capacity |
| 400, 401, 403, 404, 409, 422 | No | The request will fail identically on attempt four |
| `OSError`, `TransientError` | Yes | Socket, DNS and connection-level blips |
| Everything else | No | A bug is not a network problem |

---

## Design decisions

**Zero runtime dependencies, deliberately.** This is not minimalism for its own
sake. A sync job runs with live credentials for someone else's production
system, often on infrastructure with restricted egress and an approval process
for new packages. Every dependency is code that executes with those credentials,
and dependency confusion is a well-documented attack path. The standard library
gives me JSON, atomic file operations, threading primitives, and a logging
framework — everything this problem actually needs. The cost is that I do not
ship an HTTP client, which turns out to be a feature: you keep the one you have
already vetted, and `synckit` composes with it instead of competing.

**Full jitter, not fixed or plain exponential backoff.** Plain exponential
backoff has every client computing the same delay sequence — 1s, 2s, 4s, 8s —
so a fleet that backed off together comes back together. Sleeping a uniform
random amount in `[0, computed_delay]` has the same worst case, a lower mean
delay, and it decorrelates clients from one another. `RetryPolicy` takes an
injectable random source, so the tests assert the exact delay at each attempt
instead of hoping a range check catches a regression. And when a server supplies
`Retry-After`, that value wins outright and jitter is *not* applied on top of it:
the server knows when its quota window resets, and coming back at a random point
inside its stated interval means coming back too early and eating another 429.

**A token bucket, not a fixed window.** A fixed-window counter for "600 requests
per minute" permits 600 requests in the last second of one minute and 600 more
in the first second of the next — a 2x burst straight through the boundary, and
upstream sees a spike it never agreed to absorb. A token bucket has no boundary
to exploit: tokens accrue continuously, so the long-run rate is bounded however
the requests are aligned in time. It also makes bursting an explicit choice
(`capacity`) rather than an accident. A sliding-window log gives similar
smoothness but needs a timestamp per request, which is unbounded memory for a
job making a hundred thousand calls; the bucket is O(1) in time and space. The
clock defaults to `time.monotonic` rather than `time.time`, because the wall
clock can jump backwards on NTP correction or a VM resume, and a rate limiter
keyed on wall time either grants infinite tokens or stalls for hours.

**Atomic checkpoint writes.** The obvious implementation — `open(path, "w")`
then `json.dump` — has a window where the file is truncated to zero bytes but
the new content is not yet flushed. Checkpoint writes happen constantly, which
is to say they happen at exactly the moments a job is most likely to be killed.
So: write to a temp file *in the same directory* (rename is only atomic within a
filesystem), `fsync` it so the bytes reach the device, then `os.replace`, which
is atomic on POSIX and Windows. The containing directory is fsynced too, because
POSIX otherwise permits the rename itself to be lost across a power failure.
Even with all that, corrupt files are handled rather than trusted: a bad file is
quarantined next to the original for inspection and treated as "no checkpoint",
because restarting from the beginning is a slow correct outcome while crash-
looping on startup is not.

**Redaction as a logging filter, not a convention.** "Don't log secrets" is a
rule enforced by humans under time pressure, which means it is not enforced. A
`logging.Filter` is applied to every record unconditionally, before any
formatter, so it still works for the plain-text handler somebody attaches while
debugging — which is precisely when a secret is most likely to be printed. It
walks dicts, lists, tuples and sets to arbitrary depth, because the interesting
secret is never at the top level; it is at
`record.extra.request.headers.Authorization`. A key match redacts the whole
subtree, since a dict under a key called `credentials` is not made safe by
scrubbing its leaves. Email addresses are redacted too: CRM records are full of
them, they are personal data under GDPR, and GitHub's secret masking does
nothing for them.

**Dry run that runs the same code.** This framework writes into systems where
mistakes are not recoverable in any practical sense. The only trustworthy dry
run is one that takes the identical code path — same pagination, same
transforms, same batching, same counters, same logs — with the sink swapped for
a recorder and the checkpoint store for an in-memory one. A dry run implemented
as a separate branch tests different code from the one that will execute, which
is worse than no dry run at all because it manufactures confidence. The counters
it returns answer the question people actually care about before a deploy: how
many records would this touch?

**Two distinct classes of failure.** A *record* failure (the transform raised on
one malformed row) is isolated, counted and logged with the record's id; one
contact with a null email should not abandon 400,000 good ones. A *batch*
failure (the sink rejected a write after retries) stops the run by default and
does **not** advance the checkpoint, so the next run retries from the last
confirmed position. Continuing past a failed write would skip records silently
and produce the worst outcome in the whole design space: a job that reports
success while losing data. `SyncResult.ok` is false if anything failed at all,
so the process exit code tells the truth.

**Checkpoint after each batch, not at the end.** Checkpointing at the end only
works when nothing goes wrong, which is when you do not need it. Saving after
each confirmed batch caps the cost of an interruption at one batch of
re-processing. That does require the sink to be idempotent — upserting on a
stable external id is the usual approach — and I state that requirement plainly
rather than hiding it, because there is no exactly-once delivery between two
systems that do not share a transaction.

**Everything time-related is injectable.** Every component that touches time or
randomness takes a `clock`, `sleep` or `rand` parameter with a sensible default.
This is what makes 265 tests run in well under a second with no sleeping, no
wall-clock reads and no tolerance-based assertions. Test suites for retry and
rate-limiting code are notorious for being slow locally and intermittently red
on a busy CI runner; that is a design problem in the code under test, not an
inherent property of the domain.

---

## Tests

**265 tests**, all passing, across the five modules. No network, no sleeping, no
randomness that is not seeded or stubbed. The suite runs in well under a second.

```bash
pytest -q      # 265 passed
ruff check .   # All checks passed!
```

| File | Focus |
| --- | --- |
| `tests/test_retry.py` | Backoff growth and capping, jitter bounds across the full `[0, 1)` draw range, `Retry-After` honoured and capped, exhaustion raising `RetryError` with the cause attached, non-retryable errors passing straight through untouched, 4xx never retried while 429 always is, decorator metadata preservation |
| `tests/test_ratelimit.py` | Burst to capacity then throttle then refill, steady-state rate over many calls, fractional and weighted costs, timeouts that do not consume tokens, backwards and stationary clocks, and a 200-thread test proving the bucket never oversubscribes |
| `tests/test_checkpoint.py` | Atomic write leaving no temp files and never truncating the original on failure, truncated/empty/array/non-dict corruption all recovering, quarantine with collision handling, strict mode, multi-job isolation in one file |
| `tests/test_logging.py` | Redaction at six levels of nesting, inside lists, inside lists of dicts, tuples and sets preserved, whole-subtree redaction, input never mutated, depth limit, bearer and inline `api_key=` patterns in free text, emails, and end-to-end assertions that a secret never reaches the stream |
| `tests/test_runner.py` | Dry run writing nothing while reporting identical counts to a live run, checkpoint advancing per batch and not advancing past a failed one, resume from a stored cursor, record failures isolated versus `fail_fast`, batch failure halting the run, retry exhaustion on the sink, `max_records`/`max_pages` limits, cursor-loop detection |

`conftest.py` holds the shared doubles: `FakeClock`, `FakeSleep`,
`SequenceRandom`, `RecordingSink` and a paged source builder.

---

## Licence

MIT — see [LICENSE](LICENSE). Copyright (c) 2026 Avery McQueen.

Security policy and threat model: [SECURITY.md](SECURITY.md).
