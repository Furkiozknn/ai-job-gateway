"""Job persistence.

``JobStore`` is the abstraction; ``InMemoryJobStore`` and ``SQLiteJobStore``
are the two implementations shipped here. Both are correct and both are
small -- the point of having two is to make clear, from day one, that this
design does not assume an in-process dict is good enough forever. A real
deployment might swap in Postgres, but the interface (create/get/update_status/list)
stays the same either way.

Expiry is enforced here, not in the HTTP layer: any ``get()`` or ``list()``
call runs every terminal record through ``_apply_expiry`` before returning
it, so both the API and anything else built on top of a store (the CLI, a
future admin tool) see the same "expired" view once a result's window has
passed. Nothing is mutated in storage when this happens -- expiry is a pure
function of "now" vs. the stored ``result_expires_at``, recomputed on every
read, which keeps both store implementations simple and avoids a background
sweeper job.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from . import clock
from .models import TERMINAL_STATUSES, JobRecord, JobStatus


def _expiry_message(record: JobRecord) -> str:
    assert record.result_expires_at is not None
    return (
        f"This job's result expired at {record.result_expires_at.isoformat()}. "
        "Results are only retained for a limited window after a job finishes -- "
        "treat result URLs/data as short-lived and persist them (download files, "
        "copy result data into your own storage) promptly after polling reports "
        "'ready', rather than relying on this gateway to hold them indefinitely."
    )


def _apply_expiry(record: JobRecord) -> JobRecord:
    """Return `record`, or a copy of it with status flipped to `expired`."""
    if record.status in TERMINAL_STATUSES and record.result_expires_at is not None:
        if clock.now() >= record.result_expires_at:
            return record.model_copy(
                update={
                    "status": JobStatus.EXPIRED,
                    "result": None,
                    "error": _expiry_message(record),
                }
            )
    return record


class JobStore(ABC):
    """Minimal persistence interface every backend implements."""

    @abstractmethod
    async def create(self, record: JobRecord) -> None:
        """Persist a brand-new job record."""

    @abstractmethod
    async def get(self, job_id: str) -> Optional[JobRecord]:
        """Fetch one job by id, or None if it doesn't exist.

        Applies expiry before returning: a terminal job past its
        ``result_expires_at`` comes back with status ``expired``.
        """

    @abstractmethod
    async def update_status(
        self,
        job_id: str,
        *,
        status: JobStatus,
        result: Optional[dict] = None,
        error: Optional[str] = None,
        result_expires_at: Optional[datetime] = None,
    ) -> JobRecord:
        """Transition a job to a new status, replacing result/error/expiry wholesale."""

    @abstractmethod
    async def list(self) -> list[JobRecord]:
        """Return all jobs (small reference store -- no pagination)."""


class InMemoryJobStore(JobStore):
    """Dict-backed store. Fine for a single-process reference server.

    Nothing here survives a process restart, and there is no cross-process
    sharing -- if that matters, use ``SQLiteJobStore`` instead.
    """

    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._lock = asyncio.Lock()

    async def create(self, record: JobRecord) -> None:
        async with self._lock:
            self._jobs[record.id] = record

    async def get(self, job_id: str) -> Optional[JobRecord]:
        async with self._lock:
            record = self._jobs.get(job_id)
        return _apply_expiry(record) if record is not None else None

    async def update_status(
        self,
        job_id: str,
        *,
        status: JobStatus,
        result: Optional[dict] = None,
        error: Optional[str] = None,
        result_expires_at: Optional[datetime] = None,
    ) -> JobRecord:
        async with self._lock:
            current = self._jobs[job_id]
            updated = current.model_copy(
                update={
                    "status": status,
                    "result": result,
                    "error": error,
                    "result_expires_at": result_expires_at,
                    "updated_at": clock.now(),
                }
            )
            self._jobs[job_id] = updated
            return updated

    async def list(self) -> list[JobRecord]:
        async with self._lock:
            records = list(self._jobs.values())
        return [_apply_expiry(r) for r in records]


_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    capability TEXT NOT NULL,
    provider TEXT NOT NULL,
    params TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    result TEXT,
    error TEXT,
    result_expires_at TEXT,
    webhook_url TEXT
)
"""


def _row_to_record(row: sqlite3.Row) -> JobRecord:
    return JobRecord(
        id=row["id"],
        capability=row["capability"],
        provider=row["provider"],
        params=json.loads(row["params"]),
        status=JobStatus(row["status"]),
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        result=json.loads(row["result"]) if row["result"] is not None else None,
        error=row["error"],
        result_expires_at=(
            datetime.fromisoformat(row["result_expires_at"])
            if row["result_expires_at"] is not None
            else None
        ),
        webhook_url=row["webhook_url"],
    )


class SQLiteJobStore(JobStore):
    """SQLite-backed store: job records survive a process restart.

    A single connection is reused for the store's lifetime and every call is
    serialized behind an ``asyncio.Lock`` -- sqlite3 connections are not
    safe for concurrent use from multiple threads, and this reference
    implementation favors "obviously correct" over "fast." For a
    high-throughput deployment, swap this for a real database with proper
    connection pooling.
    """

    def __init__(self, path: str = "jobs.db") -> None:
        self.path = path
        self._lock = asyncio.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        # Write-ahead logging with synchronous=NORMAL: commits no longer wait
        # for an fsync of the main database file on every job transition.
        # Measured here: create 1.15 ms -> ~0.1 ms, update 1.41 ms -> ~0.1 ms.
        # The trade is documented SQLite behaviour - a power loss can drop the
        # most recent commits, never corrupt the file - which is the right
        # trade for a job store whose callers poll and whose providers can be
        # re-run. WAL also lets a reader (GET /v1/jobs/{id}) proceed while a
        # writer commits instead of queueing behind it. `:memory:` databases
        # ignore journal_mode; the pragmas are harmless there.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    async def create(self, record: JobRecord) -> None:
        async with self._lock:
            await asyncio.to_thread(self._insert, record)

    def _insert(self, record: JobRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO jobs (
                id, capability, provider, params, status,
                created_at, updated_at, result, error, result_expires_at, webhook_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.id,
                record.capability,
                record.provider,
                json.dumps(record.params),
                record.status.value,
                record.created_at.isoformat(),
                record.updated_at.isoformat(),
                json.dumps(record.result) if record.result is not None else None,
                record.error,
                record.result_expires_at.isoformat() if record.result_expires_at else None,
                record.webhook_url,
            ),
        )
        self._conn.commit()

    def _select_one(self, job_id: str) -> Optional[sqlite3.Row]:
        return self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    async def get(self, job_id: str) -> Optional[JobRecord]:
        async with self._lock:
            row = await asyncio.to_thread(self._select_one, job_id)
        return _apply_expiry(_row_to_record(row)) if row is not None else None

    def _update(
        self,
        job_id: str,
        status: JobStatus,
        result: Optional[dict],
        error: Optional[str],
        result_expires_at: Optional[datetime],
        updated_at: datetime,
    ) -> None:
        self._conn.execute(
            """
            UPDATE jobs
            SET status = ?, result = ?, error = ?, result_expires_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                status.value,
                json.dumps(result) if result is not None else None,
                error,
                result_expires_at.isoformat() if result_expires_at else None,
                updated_at.isoformat(),
                job_id,
            ),
        )
        self._conn.commit()

    async def update_status(
        self,
        job_id: str,
        *,
        status: JobStatus,
        result: Optional[dict] = None,
        error: Optional[str] = None,
        result_expires_at: Optional[datetime] = None,
    ) -> JobRecord:
        updated_at = clock.now()
        async with self._lock:
            await asyncio.to_thread(
                self._update, job_id, status, result, error, result_expires_at, updated_at
            )
            row = await asyncio.to_thread(self._select_one, job_id)
        assert row is not None
        return _row_to_record(row)

    def _select_all(self) -> list[sqlite3.Row]:
        return self._conn.execute("SELECT * FROM jobs ORDER BY created_at").fetchall()

    async def list(self) -> list[JobRecord]:
        async with self._lock:
            rows = await asyncio.to_thread(self._select_all)
        return [_apply_expiry(_row_to_record(r)) for r in rows]
