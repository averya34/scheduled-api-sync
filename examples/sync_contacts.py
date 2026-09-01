"""Example job: mirror CRM contacts into a reporting table.

Runnable as-is. With no ``API_BASE_URL`` configured it uses a small built-in
fake source so that ``python examples/sync_contacts.py --dry-run`` works on a
fresh clone with nothing set up -- which is what makes this example useful as
a smoke test in CI rather than just documentation.

Everything network-facing lives behind the ``source`` and ``sink`` callables.
Swap them for ``urllib.request`` calls (or your HTTP client of choice) and the
rest of the job is unchanged.
"""

from __future__ import annotations

import argparse
import os
import sys

from synckit import (
    JsonCheckpointStore,
    Page,
    RetryPolicy,
    SyncRunner,
    TokenBucket,
    configure_logging,
)

JOB_NAME = "crm-contacts"
STATE_PATH = os.environ.get("SYNC_STATE_PATH", ".state/contacts.json")

# Stand-in dataset so the example runs without credentials. A real job would
# read the token from the environment and never from a literal.
_FAKE_ROWS = [
    {"id": index, "email": f"person{index}@example.com", "owner": "sales", "stage": "lead"}
    for index in range(250)
]


def make_source(page_size: int = 100):
    """Return a ``source(cursor) -> Page`` callable over the fake dataset."""

    def source(cursor):
        offset = int(cursor or 0)
        rows = _FAKE_ROWS[offset : offset + page_size]
        next_offset = offset + page_size
        has_more = next_offset < len(_FAKE_ROWS)
        return Page(records=rows, next_cursor=next_offset if has_more else None)

    return source


def transform(record):
    """Normalise one CRM row, or return None to skip it.

    Skipping is a first-class outcome, not an error: a contact without an
    email cannot be matched downstream, and failing the whole run over it
    would mean one bad row blocks every good one.
    """
    email = (record.get("email") or "").strip().lower()
    if not email:
        return None
    return {
        "external_id": str(record["id"]),
        "email": email,
        "owner": record.get("owner") or "unassigned",
        "stage": record.get("stage") or "unknown",
    }


def make_sink(token: str | None):
    """Return a ``sink(batch) -> int`` callable.

    The token is captured here rather than read inside the loop so there is
    exactly one place where a credential enters the job.
    """

    def sink(batch):
        if not token:
            # No credential configured: behave as a no-op writer so the
            # example still exercises the full loop.
            return len(batch)
        # Real implementation would POST the batch to the warehouse API.
        return len(batch)

    return sink


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync CRM contacts into the reporting table.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="compute and report everything, write nothing",
    )
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--max-records", type=int, default=None)
    args = parser.parse_args(argv)

    # Static fields make it possible to find every line from one run later.
    log = configure_logging(
        "INFO",
        logger_name="synckit",
        static_fields={"service": JOB_NAME, "ci_run": os.environ.get("GITHUB_RUN_ID", "local")},
    )

    runner = SyncRunner(
        make_source(),
        make_sink(os.environ.get("API_TOKEN")),
        transform=transform,
        store=JsonCheckpointStore(STATE_PATH),
        job=JOB_NAME,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        max_records=args.max_records,
        # Published quota is 10 requests/second; the burst of 20 covers the
        # short spike at the start of the run without exceeding the average.
        rate_limiter=TokenBucket(rate=10, capacity=20),
        retry_policy=RetryPolicy(max_attempts=5, base_delay=0.5, max_delay=30.0),
        logger=log,
    )

    result = runner.run()
    log.info("run summary", extra=result.to_dict())
    print(result)

    # Non-zero exit on partial failure. A scheduled job that fails quietly is
    # worse than one that never ran, because nobody goes looking for it.
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
