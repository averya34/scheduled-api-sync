"""Durable checkpoint storage so an interrupted sync can resume.

The problem
-----------
A nightly sync that pulls 400,000 CRM contacts takes forty minutes. At
minute thirty-eight the GitHub Actions runner is reclaimed, or the upstream
API starts returning 503, or somebody cancels the workflow. Without a
checkpoint the next run starts from record zero: forty wasted minutes, forty
minutes of quota burned re-reading rows we already have, and a write path
that has to be idempotent enough to survive re-processing everything.

A checkpoint turns that into a resume. We persist the cursor -- whatever the
upstream calls it: an opaque page token, an `updated_after` timestamp, a
numeric offset -- after each batch that has been *fully written*. The next
run reads the cursor and continues.

Why the write has to be atomic
------------------------------
The obvious implementation, ``open(path, "w")`` then ``json.dump``, has a
window in which the file has been truncated to zero bytes but the new
content has not been flushed. If the process dies in that window -- and a
checkpoint write happens at exactly the moments a job is most likely to be
killed, i.e. constantly -- the file on disk is empty or half a JSON object.
The next run then finds a corrupt checkpoint, and depending on how carefully
you wrote the loader it either crashes on startup or silently restarts from
the beginning.

``os.replace`` is atomic on POSIX and on Windows: the destination path
always points either at the complete old file or at the complete new file,
never at anything in between. So we write to a temporary file in the *same
directory* (rename is only atomic within a filesystem, and ``/tmp`` is
frequently a different mount), ``fsync`` it so the bytes are actually on the
device rather than in the page cache, then ``os.replace`` it into position.

We also fsync the containing directory. Without that, POSIX permits the
rename itself to be lost across a power failure even though the file
contents survived, which would leave the old checkpoint in place. On
platforms where directory fsync is not supported the error is swallowed --
it is a durability optimisation, not a correctness requirement for the
in-process case.

Corrupt files are handled, not trusted
--------------------------------------
Even with atomic writes, a checkpoint file can be bad: a disk error, a
human editing it during an incident, a cache restored from a different
version of the job. :class:`JsonCheckpointStore` treats unreadable content
as "no checkpoint" rather than raising, optionally quarantining the bad file
next to the original so it can be inspected after the fact. Restarting from
the beginning is a slow, correct outcome; crash-looping on startup is not.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

__all__ = [
    "Checkpoint",
    "CheckpointError",
    "InMemoryCheckpointStore",
    "JsonCheckpointStore",
    "atomic_write_json",
]

SCHEMA_VERSION = 1


class CheckpointError(Exception):
    """Raised for unrecoverable checkpoint problems (e.g. strict-mode corruption)."""


@dataclass
class Checkpoint:
    """The resumable position of a sync job.

    Attributes
    ----------
    cursor:
        Opaque, provider-defined position marker. Kept as ``Any`` on purpose:
        a page token is a string, an offset is an int, and an incremental
        sync often needs a composite like ``{"updated_at": ..., "id": ...}``
        to break ties between records sharing a timestamp.
    records_processed:
        Running total across resumes, so the summary at the end of a
        multi-run backfill is meaningful.
    updated_at:
        Unix timestamp of the last save. The most common operational
        question about a sync is "is it stuck?", and this answers it without
        needing a metrics backend.
    job:
        Logical job name. One store file can hold several jobs, which keeps
        a repo full of small syncs from needing a state file each.
    metadata:
        Free-form extras (batch size in force, upstream schema version,
        last error). Anything here is written verbatim, so do not put
        credentials in it -- see ``synckit.logging`` for why.
    """

    cursor: Any = None
    records_processed: int = 0
    updated_at: float = 0.0
    job: str = "default"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Checkpoint:
        """Build a checkpoint from a dict, ignoring unknown keys.

        Forward compatibility matters here: an older deployment reading a
        file written by a newer one should degrade to "I understand the
        fields I know about", not explode.
        """
        return cls(
            cursor=data.get("cursor"),
            records_processed=int(data.get("records_processed", 0) or 0),
            updated_at=float(data.get("updated_at", 0.0) or 0.0),
            job=str(data.get("job", "default")),
            metadata=dict(data.get("metadata") or {}),
        )

    @property
    def is_empty(self) -> bool:
        """True when nothing has been recorded yet (a cold start)."""
        return self.cursor is None and self.records_processed == 0


def atomic_write_json(path: str | os.PathLike[str], payload: Any) -> None:
    """Serialise ``payload`` to ``path`` atomically.

    Exposed publicly because plenty of jobs need the same guarantee for
    their own small state files, and re-implementing it per project is how
    the truncation bug spreads.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Same directory as the target: os.replace is only atomic within a
    # single filesystem.
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
    except BaseException:
        # Never leave a stray temp file behind on failure; a directory
        # slowly filling with .tmp files is a real incident on small runners.
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise

    _fsync_dir(target.parent)


def _fsync_dir(directory: Path) -> None:
    """fsync a directory so the rename itself is durable.

    Unsupported on some platforms and filesystems; failure here costs
    durability across a power cut, not correctness, so it is not fatal.
    """
    try:
        dir_fd = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError:
        pass
    finally:
        os.close(dir_fd)


class InMemoryCheckpointStore:
    """Non-durable store used by tests and by ``dry_run`` executions.

    Having a real implementation of the interface that deliberately persists
    nothing means a dry run can exercise the identical code path as a live
    run without a conditional in the middle of :class:`~synckit.runner.SyncRunner`.
    """

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self._data: dict[str, Checkpoint] = {}
        self._clock = clock if clock is not None else time.time
        self.save_count = 0

    def load(self, job: str = "default") -> Checkpoint:
        existing = self._data.get(job)
        if existing is None:
            return Checkpoint(job=job)
        # Copy so a caller mutating the returned object cannot retroactively
        # change what a previous save "recorded".
        return Checkpoint.from_dict(existing.to_dict())

    def save(self, checkpoint: Checkpoint) -> Checkpoint:
        checkpoint.updated_at = self._clock()
        self._data[checkpoint.job] = Checkpoint.from_dict(checkpoint.to_dict())
        self.save_count += 1
        return checkpoint

    def clear(self, job: str = "default") -> None:
        self._data.pop(job, None)

    def jobs(self) -> list[str]:
        return sorted(self._data)


class JsonCheckpointStore:
    """JSON-file-backed checkpoint store with atomic writes.

    Parameters
    ----------
    path:
        File to store state in. Created on first save, including parent
        directories.
    clock:
        Injectable time source so tests can assert on ``updated_at``.
    strict:
        When true, a corrupt file raises :class:`CheckpointError` instead of
        being treated as empty. Off by default -- self-healing is the right
        default for an unattended job -- but worth enabling for a job where
        an accidental full re-sync would be more expensive than a failed run.
    quarantine_corrupt:
        When true (the default), a corrupt file is renamed to
        ``<name>.corrupt-<timestamp>`` before being replaced. Diagnosing "the
        checkpoint was bad" three days later is impossible if the evidence
        was silently overwritten.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        clock: Callable[[], float] | None = None,
        strict: bool = False,
        quarantine_corrupt: bool = True,
    ) -> None:
        self.path = Path(path)
        self._clock = clock if clock is not None else time.time
        self.strict = strict
        self.quarantine_corrupt = quarantine_corrupt
        self.save_count = 0

    # -- internals ---------------------------------------------------------

    def _read_all(self) -> dict[str, Any]:
        """Return the whole file as a dict, or ``{}`` if absent/unusable."""
        if not self.path.exists():
            return {}
        try:
            raw = self.path.read_text(encoding="utf-8")
        except OSError as exc:
            if self.strict:
                raise CheckpointError(f"cannot read checkpoint at {self.path}: {exc}") from exc
            return {}

        if not raw.strip():
            return self._handle_corrupt("file is empty")

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            return self._handle_corrupt(f"invalid JSON: {exc}")

        if not isinstance(data, dict):
            return self._handle_corrupt(f"expected an object, found {type(data).__name__}")
        return data

    def _handle_corrupt(self, reason: str) -> dict[str, Any]:
        if self.strict:
            raise CheckpointError(f"corrupt checkpoint at {self.path}: {reason}")
        if self.quarantine_corrupt:
            self._quarantine()
        return {}

    def _quarantine(self) -> Path | None:
        stamp = int(self._clock())
        destination = self.path.with_name(f"{self.path.name}.corrupt-{stamp}")
        # Collision is possible when two corrupt reads land in the same
        # second; suffix until we find a free name rather than clobbering.
        counter = 1
        while destination.exists():
            destination = self.path.with_name(f"{self.path.name}.corrupt-{stamp}-{counter}")
            counter += 1
        try:
            os.replace(self.path, destination)
        except OSError:
            return None
        return destination

    # -- public API --------------------------------------------------------

    def load(self, job: str = "default") -> Checkpoint:
        """Return the stored checkpoint for ``job``, or a fresh empty one."""
        data = self._read_all()
        entry = data.get("jobs", {}).get(job) if isinstance(data.get("jobs"), dict) else None
        if not isinstance(entry, dict):
            return Checkpoint(job=job)
        checkpoint = Checkpoint.from_dict(entry)
        # Trust the key we looked it up under over whatever the body claims;
        # a hand-edited file can easily disagree with itself.
        checkpoint.job = job
        return checkpoint

    def save(self, checkpoint: Checkpoint) -> Checkpoint:
        """Persist ``checkpoint`` atomically and stamp ``updated_at``."""
        checkpoint.updated_at = self._clock()
        data = self._read_all()
        jobs = data.get("jobs")
        if not isinstance(jobs, dict):
            jobs = {}
        jobs[checkpoint.job] = checkpoint.to_dict()
        payload = {
            "version": SCHEMA_VERSION,
            "jobs": jobs,
        }
        atomic_write_json(self.path, payload)
        self.save_count += 1
        return checkpoint

    def clear(self, job: str = "default") -> None:
        """Forget one job's checkpoint, forcing a full re-sync next run."""
        data = self._read_all()
        jobs = data.get("jobs")
        if not isinstance(jobs, dict) or job not in jobs:
            return
        del jobs[job]
        atomic_write_json(self.path, {"version": SCHEMA_VERSION, "jobs": jobs})
        self.save_count += 1

    def jobs(self) -> list[str]:
        """Names of every job present in the file."""
        data = self._read_all()
        jobs = data.get("jobs")
        return sorted(jobs) if isinstance(jobs, dict) else []
