"""Tests for synckit.checkpoint."""

from __future__ import annotations

import json
import os

import pytest
from conftest import FakeClock

from synckit.checkpoint import (
    Checkpoint,
    CheckpointError,
    InMemoryCheckpointStore,
    JsonCheckpointStore,
    atomic_write_json,
)


@pytest.fixture
def store_path(tmp_path):
    return tmp_path / "state" / "checkpoint.json"


# -- the dataclass --------------------------------------------------------


def test_new_checkpoint_is_empty():
    assert Checkpoint().is_empty is True


def test_checkpoint_with_a_cursor_is_not_empty():
    assert Checkpoint(cursor="abc").is_empty is False


def test_checkpoint_round_trips_through_a_dict():
    original = Checkpoint(cursor={"id": 5}, records_processed=12, job="crm", metadata={"a": 1})
    restored = Checkpoint.from_dict(original.to_dict())
    assert restored == original


def test_from_dict_ignores_unknown_keys():
    restored = Checkpoint.from_dict({"cursor": "x", "future_field": True})
    assert restored.cursor == "x"
    assert not hasattr(restored, "future_field")


def test_from_dict_tolerates_missing_fields():
    restored = Checkpoint.from_dict({})
    assert restored.cursor is None
    assert restored.records_processed == 0
    assert restored.job == "default"


def test_from_dict_coerces_null_counts():
    assert Checkpoint.from_dict({"records_processed": None}).records_processed == 0


# -- atomic_write_json ----------------------------------------------------


def test_atomic_write_creates_parent_directories(tmp_path):
    target = tmp_path / "a" / "b" / "c.json"
    atomic_write_json(target, {"hello": "world"})
    assert json.loads(target.read_text()) == {"hello": "world"}


def test_atomic_write_leaves_no_temp_files(tmp_path):
    target = tmp_path / "state.json"
    for index in range(5):
        atomic_write_json(target, {"n": index})
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]


def test_atomic_write_replaces_previous_content(tmp_path):
    target = tmp_path / "state.json"
    atomic_write_json(target, {"first": True})
    atomic_write_json(target, {"second": True})
    assert json.loads(target.read_text()) == {"second": True}


def test_atomic_write_cleans_up_when_serialisation_fails(tmp_path):
    target = tmp_path / "state.json"

    class Unserialisable:
        def __str__(self):
            raise RuntimeError("cannot stringify")

    with pytest.raises(RuntimeError):
        atomic_write_json(target, {"bad": Unserialisable()})
    assert list(tmp_path.iterdir()) == []


def test_atomic_write_never_truncates_the_original_on_failure(tmp_path):
    target = tmp_path / "state.json"
    atomic_write_json(target, {"good": 1})

    class Unserialisable:
        def __str__(self):
            raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        atomic_write_json(target, {"bad": Unserialisable()})
    # The whole point of write-temp-then-replace: the old file is intact.
    assert json.loads(target.read_text()) == {"good": 1}


def test_atomic_write_falls_back_to_str_for_exotic_types(tmp_path):
    import datetime

    target = tmp_path / "state.json"
    atomic_write_json(target, {"when": datetime.date(2026, 1, 2)})
    assert json.loads(target.read_text())["when"] == "2026-01-02"


# -- JsonCheckpointStore basics ------------------------------------------


def test_load_on_a_missing_file_returns_an_empty_checkpoint(store_path):
    checkpoint = JsonCheckpointStore(store_path).load("crm")
    assert checkpoint.is_empty
    assert checkpoint.job == "crm"


def test_save_then_load_round_trip(store_path):
    store = JsonCheckpointStore(store_path)
    store.save(Checkpoint(cursor="page-2", records_processed=200, job="crm"))
    loaded = store.load("crm")
    assert loaded.cursor == "page-2"
    assert loaded.records_processed == 200


def test_save_stamps_updated_at_from_the_injected_clock(store_path):
    clock = FakeClock(start=1750000000.0)
    store = JsonCheckpointStore(store_path, clock=clock)
    saved = store.save(Checkpoint(job="crm"))
    assert saved.updated_at == 1750000000.0
    assert store.load("crm").updated_at == 1750000000.0


def test_multiple_jobs_share_one_file(store_path):
    store = JsonCheckpointStore(store_path)
    store.save(Checkpoint(cursor="a", job="crm"))
    store.save(Checkpoint(cursor="b", job="ledger"))
    assert store.load("crm").cursor == "a"
    assert store.load("ledger").cursor == "b"
    assert store.jobs() == ["crm", "ledger"]


def test_saving_a_job_does_not_disturb_others(store_path):
    store = JsonCheckpointStore(store_path)
    store.save(Checkpoint(cursor="a", job="crm"))
    store.save(Checkpoint(cursor="b", job="ledger"))
    store.save(Checkpoint(cursor="a2", job="crm"))
    assert store.load("ledger").cursor == "b"


def test_clear_removes_only_the_named_job(store_path):
    store = JsonCheckpointStore(store_path)
    store.save(Checkpoint(cursor="a", job="crm"))
    store.save(Checkpoint(cursor="b", job="ledger"))
    store.clear("crm")
    assert store.load("crm").is_empty
    assert store.load("ledger").cursor == "b"


def test_clear_on_an_unknown_job_is_a_no_op(store_path):
    store = JsonCheckpointStore(store_path)
    store.clear("never-existed")
    assert store.save_count == 0


def test_cursor_can_be_a_composite_structure(store_path):
    store = JsonCheckpointStore(store_path)
    cursor = {"updated_at": "2026-02-01T00:00:00Z", "id": 4711}
    store.save(Checkpoint(cursor=cursor, job="crm"))
    assert store.load("crm").cursor == cursor


def test_job_key_wins_over_a_mismatched_body(store_path):
    store = JsonCheckpointStore(store_path)
    store.save(Checkpoint(cursor="a", job="crm"))
    raw = json.loads(store_path.read_text())
    raw["jobs"]["crm"]["job"] = "tampered"
    store_path.write_text(json.dumps(raw))
    assert store.load("crm").job == "crm"


def test_jobs_is_empty_for_a_missing_file(store_path):
    assert JsonCheckpointStore(store_path).jobs() == []


def test_written_file_is_valid_indented_json(store_path):
    store = JsonCheckpointStore(store_path)
    store.save(Checkpoint(cursor="a", job="crm"))
    text = store_path.read_text()
    assert text.endswith("\n")
    assert json.loads(text)["version"] == 1


# -- corruption handling --------------------------------------------------


def test_truncated_json_is_treated_as_no_checkpoint(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text('{"version": 1, "jobs": {"crm": {"cur')
    store = JsonCheckpointStore(store_path)
    assert store.load("crm").is_empty


def test_empty_file_is_treated_as_no_checkpoint(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text("")
    assert JsonCheckpointStore(store_path).load("crm").is_empty


def test_whitespace_only_file_is_treated_as_no_checkpoint(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text("   \n  ")
    assert JsonCheckpointStore(store_path).load("crm").is_empty


def test_json_array_at_top_level_is_treated_as_corrupt(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text("[1, 2, 3]")
    assert JsonCheckpointStore(store_path).load("crm").is_empty


def test_corrupt_file_is_quarantined_for_inspection(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text("{not json")
    store = JsonCheckpointStore(store_path, clock=FakeClock(start=1700000000.0))
    store.load("crm")
    quarantined = list(store_path.parent.glob("checkpoint.json.corrupt-*"))
    assert len(quarantined) == 1
    assert quarantined[0].read_text() == "{not json"


def test_quarantine_can_be_disabled(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text("{not json")
    store = JsonCheckpointStore(store_path, quarantine_corrupt=False)
    store.load("crm")
    assert list(store_path.parent.glob("*.corrupt-*")) == []
    assert store_path.exists()


def test_quarantine_avoids_name_collisions(store_path):
    store = JsonCheckpointStore(store_path, clock=FakeClock(start=1700000000.0))
    store_path.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(3):
        store_path.write_text("{still not json")
        store.load("crm")
    assert len(list(store_path.parent.glob("checkpoint.json.corrupt-*"))) == 3


def test_recovery_after_corruption_can_save_again(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text("garbage")
    store = JsonCheckpointStore(store_path)
    assert store.load("crm").is_empty
    store.save(Checkpoint(cursor="fresh", job="crm"))
    assert store.load("crm").cursor == "fresh"


def test_strict_mode_raises_on_corruption(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text("{oops")
    with pytest.raises(CheckpointError, match="corrupt"):
        JsonCheckpointStore(store_path, strict=True).load("crm")


def test_strict_mode_raises_on_an_empty_file(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text("")
    with pytest.raises(CheckpointError):
        JsonCheckpointStore(store_path, strict=True).load("crm")


def test_non_dict_job_entry_is_ignored(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text(json.dumps({"version": 1, "jobs": {"crm": "oops"}}))
    assert JsonCheckpointStore(store_path).load("crm").is_empty


def test_missing_jobs_key_is_tolerated(store_path):
    store_path.parent.mkdir(parents=True)
    store_path.write_text(json.dumps({"version": 1}))
    store = JsonCheckpointStore(store_path)
    assert store.load("crm").is_empty
    assert store.jobs() == []


# -- durability -----------------------------------------------------------


def test_repeated_saves_keep_the_file_parseable(store_path):
    store = JsonCheckpointStore(store_path)
    for index in range(25):
        store.save(Checkpoint(cursor=index, records_processed=index * 10, job="crm"))
        assert json.loads(store_path.read_text())["jobs"]["crm"]["cursor"] == index
    assert store.save_count == 25


def test_no_stray_temp_files_after_many_saves(store_path):
    store = JsonCheckpointStore(store_path)
    for index in range(10):
        store.save(Checkpoint(cursor=index, job="crm"))
    assert [p.name for p in store_path.parent.iterdir()] == ["checkpoint.json"]


def test_save_survives_a_pre_existing_directory(store_path):
    os.makedirs(store_path.parent, exist_ok=True)
    JsonCheckpointStore(store_path).save(Checkpoint(job="crm"))
    assert store_path.exists()


# -- InMemoryCheckpointStore ---------------------------------------------


def test_in_memory_store_round_trip():
    store = InMemoryCheckpointStore()
    store.save(Checkpoint(cursor="x", job="crm"))
    assert store.load("crm").cursor == "x"


def test_in_memory_store_returns_a_copy():
    store = InMemoryCheckpointStore()
    store.save(Checkpoint(cursor="x", job="crm"))
    loaded = store.load("crm")
    loaded.cursor = "mutated"
    assert store.load("crm").cursor == "x"


def test_in_memory_store_counts_saves():
    store = InMemoryCheckpointStore()
    for index in range(4):
        store.save(Checkpoint(cursor=index, job="crm"))
    assert store.save_count == 4


def test_in_memory_store_clear_and_jobs():
    store = InMemoryCheckpointStore(clock=FakeClock())
    store.save(Checkpoint(job="a"))
    store.save(Checkpoint(job="b"))
    assert store.jobs() == ["a", "b"]
    store.clear("a")
    assert store.jobs() == ["b"]


def test_in_memory_store_unknown_job_is_empty():
    assert InMemoryCheckpointStore().load("nope").is_empty
