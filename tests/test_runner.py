"""Tests for synckit.runner."""

from __future__ import annotations

import logging

import pytest
from conftest import FakeClock, FakeSleep, RecordingSink, paged_source

from synckit.checkpoint import Checkpoint, InMemoryCheckpointStore, JsonCheckpointStore
from synckit.ratelimit import TokenBucket
from synckit.retry import HttpError, RetryPolicy, TransientError
from synckit.runner import Page, SkipRecord, SyncError, SyncResult, SyncRunner, batched


def records(count, start=0):
    return [{"id": index, "value": index * 2} for index in range(start, start + count)]


def build(pages, **kwargs):
    """Construct a runner over ``pages`` with a recording sink."""
    sink = kwargs.pop("sink", None) or RecordingSink()
    kwargs.setdefault("clock", FakeClock())
    kwargs.setdefault("sleep", FakeSleep())
    kwargs.setdefault("logger", logging.getLogger("test.runner.silent"))
    runner = SyncRunner(paged_source(pages), sink, **kwargs)
    return runner, sink


@pytest.fixture(autouse=True)
def _silence_runner_logging():
    log = logging.getLogger("test.runner.silent")
    log.handlers = [logging.NullHandler()]
    log.propagate = False
    yield


# -- Page and helpers -----------------------------------------------------


def test_page_length():
    assert len(Page(records=[1, 2, 3])) == 3


def test_page_defaults_to_empty():
    page = Page()
    assert len(page) == 0
    assert page.next_cursor is None


def test_batched_splits_evenly():
    assert list(batched(range(6), 2)) == [[0, 1], [2, 3], [4, 5]]


def test_batched_keeps_the_remainder():
    assert list(batched(range(5), 2)) == [[0, 1], [2, 3], [4]]


def test_batched_of_nothing_is_nothing():
    assert list(batched([], 3)) == []


def test_batched_rejects_a_zero_size():
    with pytest.raises(ValueError, match="size"):
        list(batched([1], 0))


def test_coerce_page_from_a_tuple():
    page = SyncRunner._coerce_page((["a", "b"], "next"))
    assert page.records == ["a", "b"]
    assert page.next_cursor == "next"


def test_coerce_page_from_a_bare_list():
    page = SyncRunner._coerce_page(["a"])
    assert page.next_cursor is None


def test_coerce_page_from_a_dict():
    page = SyncRunner._coerce_page({"records": [1], "next_cursor": 2})
    assert page.records == [1]
    assert page.next_cursor == 2


def test_coerce_page_from_none():
    assert len(SyncRunner._coerce_page(None)) == 0


def test_coerce_page_rejects_junk():
    with pytest.raises(TypeError):
        SyncRunner._coerce_page(42)


# -- construction ---------------------------------------------------------


def test_batch_size_must_be_positive():
    with pytest.raises(ValueError, match="batch_size"):
        SyncRunner(lambda c: [], lambda b: None, batch_size=0)


def test_negative_max_records_is_rejected():
    with pytest.raises(ValueError, match="max_records"):
        SyncRunner(lambda c: [], lambda b: None, max_records=-1)


def test_run_id_is_generated_when_absent():
    runner, _ = build([[]])
    assert runner.run_id
    assert len(runner.run_id) == 12


def test_run_id_can_be_supplied():
    runner, _ = build([[]], run_id="fixed-id")
    assert runner.run().run_id == "fixed-id"


# -- the happy path -------------------------------------------------------


def test_single_page_is_read_transformed_and_written():
    runner, sink = build([records(3)], batch_size=10)
    result = runner.run()
    assert result.records_read == 3
    assert result.records_written == 3
    assert result.batches_written == 1
    assert sink.written == records(3)
    assert result.completed is True
    assert result.ok is True


def test_multiple_pages_are_all_consumed():
    pages = [records(5, 0), records(5, 5), records(5, 10)]
    runner, sink = build(pages, batch_size=100)
    result = runner.run()
    assert result.records_read == 15
    assert result.pages_fetched == 3
    assert len(sink.written) == 15


def test_batches_respect_batch_size():
    runner, sink = build([records(10)], batch_size=3)
    runner.run()
    assert [len(batch) for batch in sink.batches] == [3, 3, 3, 1]


def test_batch_size_is_independent_of_page_size():
    runner, sink = build([records(4, 0), records(4, 4)], batch_size=5)
    runner.run()
    assert [len(batch) for batch in sink.batches] == [5, 3]


def test_empty_source_produces_an_empty_result():
    runner, sink = build([[]])
    result = runner.run()
    assert result.records_read == 0
    assert result.batches_written == 0
    assert sink.batches == []
    assert result.completed is True


def test_transform_is_applied():
    runner, sink = build([records(3)], transform=lambda r: {"id": r["id"]}, batch_size=10)
    runner.run()
    assert sink.written == [{"id": 0}, {"id": 1}, {"id": 2}]


def test_transform_returning_none_skips_the_record():
    runner, sink = build(
        [records(4)],
        transform=lambda r: None if r["id"] % 2 else r,
        batch_size=10,
    )
    result = runner.run()
    assert result.records_skipped == 2
    assert result.records_written == 2


def test_skip_record_exception_skips_without_failing():
    def transform(record):
        if record["id"] == 1:
            raise SkipRecord
        return record

    runner, _ = build([records(3)], transform=transform, batch_size=10)
    result = runner.run()
    assert result.records_skipped == 1
    assert result.records_failed == 0


def test_sink_return_value_overrides_the_assumed_count():
    def deduplicating_sink(batch):
        return 1

    runner, _ = build([records(4)], sink=deduplicating_sink, batch_size=4)
    assert runner.run().records_written == 1


def test_sink_returning_none_counts_the_whole_batch():
    runner, _ = build([records(4)], sink=lambda batch: None, batch_size=4)
    assert runner.run().records_written == 4


def test_duration_uses_the_injected_clock():
    clock = FakeClock()

    def source(cursor):
        clock.advance(2.5)
        return Page(records=records(1), next_cursor=None)

    runner = SyncRunner(
        source, RecordingSink(), clock=clock, logger=logging.getLogger("test.runner.silent")
    )
    assert runner.run().duration == pytest.approx(2.5)


# -- dry run --------------------------------------------------------------


def test_dry_run_writes_nothing_to_the_sink():
    runner, sink = build([records(6)], dry_run=True, batch_size=2)
    result = runner.run()
    assert sink.batches == []
    assert result.dry_run is True


def test_dry_run_still_reports_full_counts():
    runner, _ = build([records(6)], dry_run=True, batch_size=2)
    result = runner.run()
    assert result.records_read == 6
    assert result.records_written == 6
    assert result.batches_written == 3


def test_dry_run_records_the_planned_writes():
    runner, _ = build([records(5)], dry_run=True, batch_size=2)
    runner.run()
    assert [len(batch) for batch in runner.planned_writes] == [2, 2, 1]
    assert runner.planned_writes[0] == records(2)


def test_dry_run_leaves_the_real_checkpoint_untouched(tmp_path):
    store = JsonCheckpointStore(tmp_path / "cp.json")
    store.save(Checkpoint(cursor=0, job="crm"))
    saves_before = store.save_count

    runner, _ = build([records(3, 0), records(3, 3)], dry_run=True, store=store, job="crm")
    runner.run()

    assert store.save_count == saves_before
    assert store.load("crm").cursor == 0


def test_dry_run_and_live_run_agree_on_counts():
    pages = [records(4, 0), records(4, 4), records(4, 8)]
    dry, _ = build(pages, dry_run=True, batch_size=5)
    live, _ = build(pages, dry_run=False, batch_size=5)
    dry_result = dry.run()
    live_result = live.run()
    assert (dry_result.records_read, dry_result.records_written, dry_result.batches_written) == (
        live_result.records_read,
        live_result.records_written,
        live_result.batches_written,
    )


def test_dry_run_applies_transforms_and_skips():
    runner, _ = build(
        [records(6)],
        dry_run=True,
        batch_size=10,
        transform=lambda r: None if r["id"] < 2 else r,
    )
    result = runner.run()
    assert result.records_skipped == 2
    assert result.records_written == 4


# -- checkpointing --------------------------------------------------------


def test_checkpoint_is_saved_after_each_batch():
    store = InMemoryCheckpointStore()
    runner, _ = build([records(9)], store=store, batch_size=3, job="crm")
    runner.run()
    assert store.save_count == 3


def test_checkpoint_advances_across_pages():
    store = InMemoryCheckpointStore()
    runner, _ = build([records(2, 0), records(2, 2), records(2, 4)], store=store, batch_size=2)
    runner.run()
    assert store.load("default").records_processed == 6


def test_run_resumes_from_the_stored_cursor():
    store = InMemoryCheckpointStore()
    store.save(Checkpoint(cursor=2, job="crm"))
    pages = [records(2, 0), records(2, 2), records(2, 4)]
    runner, sink = build(pages, store=store, job="crm", batch_size=10)
    result = runner.run()
    # Pages 0 and 1 are skipped entirely.
    assert result.pages_fetched == 1
    assert sink.written == records(2, 4)


def test_resume_can_be_disabled():
    store = InMemoryCheckpointStore()
    store.save(Checkpoint(cursor=2, job="crm"))
    pages = [records(2, 0), records(2, 2), records(2, 4)]
    runner, _ = build(pages, store=store, job="crm", batch_size=10)
    assert runner.run(resume=False).pages_fetched == 3


def test_explicit_start_cursor_overrides_the_checkpoint():
    store = InMemoryCheckpointStore()
    store.save(Checkpoint(cursor=0, job="crm"))
    pages = [records(1, 0), records(1, 1), records(1, 2)]
    runner, sink = build(pages, store=store, job="crm", batch_size=10)
    runner.run(start_cursor=2)
    assert sink.written == records(1, 2)


def test_checkpoint_persists_to_disk_between_runs(tmp_path):
    path = tmp_path / "cp.json"
    pages = [records(2, 0), records(2, 2)]

    first, _ = build(pages, store=JsonCheckpointStore(path), job="crm", batch_size=10)
    first.run()

    second, sink = build(pages, store=JsonCheckpointStore(path), job="crm", batch_size=10)
    second.run()
    # Everything was already consumed, so the second run re-reads only the
    # final page's cursor position and writes nothing new.
    assert len(sink.written) <= 2


def test_checkpoint_records_the_processed_total():
    store = InMemoryCheckpointStore()
    runner, _ = build([records(7)], store=store, batch_size=7, job="crm")
    runner.run()
    assert store.load("crm").records_processed == 7


def test_pages_that_filter_everything_still_advance_the_checkpoint():
    store = InMemoryCheckpointStore()
    pages = [records(2, 0), records(2, 2)]
    runner, _ = build(pages, store=store, transform=lambda r: None, batch_size=10, job="crm")
    result = runner.run()
    assert result.records_skipped == 4
    assert store.save_count > 0


# -- failure handling -----------------------------------------------------


def test_record_failure_is_isolated_by_default():
    def transform(record):
        if record["id"] == 2:
            raise ValueError("bad row")
        return record

    runner, sink = build([records(5)], transform=transform, batch_size=10)
    result = runner.run()
    assert result.records_failed == 1
    assert result.records_written == 4
    assert result.completed is True
    assert result.ok is False
    assert len(sink.written) == 4


def test_record_failure_message_includes_the_record_id():
    def transform(record):
        raise ValueError("bad row")

    runner, _ = build([records(1)], transform=transform, batch_size=10)
    result = runner.run()
    assert "0" in result.errors[0]


def test_fail_fast_aborts_on_the_first_record_failure():
    def transform(record):
        if record["id"] == 1:
            raise ValueError("bad row")
        return record

    runner, _ = build([records(5)], transform=transform, batch_size=10, fail_fast=True)
    with pytest.raises(SyncError):
        runner.run()


def test_batch_failure_stops_the_run():
    sink = RecordingSink(fail_on_batch=2, exc=ValueError("rejected"))
    runner, _ = build([records(9)], sink=sink, batch_size=3)
    result = runner.run()
    assert result.completed is False
    assert result.batches_written == 1
    assert result.records_failed == 3
    assert result.errors


def test_batch_failure_does_not_advance_the_checkpoint():
    store = InMemoryCheckpointStore()
    sink = RecordingSink(fail_on_batch=1, exc=ValueError("rejected"))
    runner, _ = build([records(4)], sink=sink, store=store, batch_size=2, job="crm")
    runner.run()
    assert store.load("crm").is_empty


def test_continue_on_batch_error_keeps_going():
    sink = RecordingSink(fail_on_batch=1, exc=ValueError("rejected"))
    runner, _ = build([records(6)], sink=sink, batch_size=2, continue_on_batch_error=True)
    result = runner.run()
    assert result.completed is True
    assert result.records_failed == 2
    assert result.batches_written == 2
    assert result.ok is False


def test_transient_sink_failures_are_retried():
    calls = {"n": 0}

    def flaky_sink(batch):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TransientError("connection reset")
        return len(batch)

    runner, _ = build(
        [records(3)],
        sink=flaky_sink,
        batch_size=3,
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.01),
    )
    result = runner.run()
    assert result.records_written == 3
    assert calls["n"] == 2


def test_retry_exhaustion_on_the_sink_fails_the_batch():
    def always_fails(batch):
        raise TransientError("upstream down")

    runner, _ = build(
        [records(2)],
        sink=always_fails,
        batch_size=2,
        retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0),
    )
    result = runner.run()
    assert result.completed is False
    assert result.records_failed == 2


def test_non_retryable_source_error_is_reported():
    def bad_source(cursor):
        raise HttpError(401, "bad token")

    runner = SyncRunner(
        bad_source,
        RecordingSink(),
        clock=FakeClock(),
        sleep=FakeSleep(),
        logger=logging.getLogger("test.runner.silent"),
    )
    with pytest.raises(HttpError):
        runner.run()


def test_a_source_that_never_advances_is_detected():
    def stuck_source(cursor):
        return Page(records=records(1), next_cursor="same")

    runner = SyncRunner(
        stuck_source,
        RecordingSink(),
        batch_size=100,
        clock=FakeClock(),
        sleep=FakeSleep(),
        logger=logging.getLogger("test.runner.silent"),
    )
    with pytest.raises(SyncError, match="unchanged cursor"):
        runner.run(start_cursor="same")


# -- limits ---------------------------------------------------------------


def test_max_records_caps_the_run():
    runner, sink = build([records(20)], max_records=7, batch_size=100)
    result = runner.run()
    assert result.records_read == 7
    assert len(sink.written) == 7


def test_max_pages_caps_the_run():
    pages = [records(2, index * 2) for index in range(5)]
    runner, _ = build(pages, max_pages=2, batch_size=100)
    assert runner.run().pages_fetched == 2


def test_max_records_of_zero_reads_nothing():
    runner, sink = build([records(5)], max_records=0, batch_size=10)
    result = runner.run()
    assert result.records_read == 0
    assert sink.batches == []


# -- rate limiting --------------------------------------------------------


def test_rate_limiter_is_consulted_for_reads_and_writes():
    clock = FakeClock()
    bucket = TokenBucket(1000.0, 100.0, clock=clock, sleep=lambda s: None)
    runner, _ = build([records(4)], batch_size=2, rate_limiter=bucket, clock=clock)
    runner.run()
    # One source call plus two sink calls.
    assert bucket.stats().acquired == 3


def test_dry_run_does_not_spend_write_tokens():
    clock = FakeClock()
    bucket = TokenBucket(1000.0, 100.0, clock=clock, sleep=lambda s: None)
    runner, _ = build([records(4)], batch_size=2, rate_limiter=bucket, dry_run=True, clock=clock)
    runner.run()
    assert bucket.stats().acquired == 1


# -- SyncResult -----------------------------------------------------------


def test_result_ok_requires_completion_and_no_failures():
    assert SyncResult(job="j", run_id="r", dry_run=False, completed=True).ok is True
    assert SyncResult(job="j", run_id="r", dry_run=False, completed=False).ok is False
    assert (
        SyncResult(job="j", run_id="r", dry_run=False, completed=True, records_failed=1).ok is False
    )


def test_result_to_dict_includes_ok():
    data = SyncResult(job="j", run_id="r", dry_run=True, completed=True).to_dict()
    assert data["ok"] is True
    assert data["dry_run"] is True


def test_result_str_marks_dry_runs():
    text = str(SyncResult(job="crm", run_id="r", dry_run=True))
    assert "DRY RUN" in text
    assert "crm" in text


def test_result_str_marks_live_runs():
    assert "LIVE" in str(SyncResult(job="crm", run_id="r", dry_run=False))


# -- iter_pages -----------------------------------------------------------


def test_iter_pages_walks_every_page():
    runner, _ = build([records(1, 0), records(1, 1), records(1, 2)])
    assert len(list(runner.iter_pages())) == 3


def test_iter_pages_honours_max_pages():
    runner, _ = build([records(1, index) for index in range(6)], max_pages=3)
    assert len(list(runner.iter_pages())) == 3
