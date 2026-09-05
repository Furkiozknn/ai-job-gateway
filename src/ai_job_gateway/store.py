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

#: How many (idempotency_key -> job_id) pairs a store retains, oldest
#: evicted first. Bounded so a client minting a fresh key per call forever
#: can't grow storage without limit. Lives in the store (it used to be a
#: dict inside JobManager) because dedupe that evaporates on restart is
#: exactly when it's needed most: a deploy mid-request is the classic way
#: a client's response gets lost and its retry double-runs the provider.
MAX_IDEMPOTENCY_KEYS = 10_000


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

    async def list_page(
        self,
        *,
        status: Optional[JobStatus] = None,
        capability: Optional[str] = None,
        limit: int = 50,
    ) -> tuple[list[JobRecord], int]:
        """A newest-first page of jobs plus the total number matching.

        Filters compare against the *displayed* status -- the one a caller
        sees after expiry is applied -- so ``status=ready`` never returns a
        job whose result window has already passed, and ``status=expired``
        finds exactly those jobs. This default implementation scans
        ``list()``; ``SQLiteJobStore`` overrides it to push the filter,
        sort and limit into SQL so one dashboard poll doesn't pay for (or
        hold locks across) every row ever stored.
        """
        records = await self.list()
        if status is not None:
            records = [r for r in records if r.status == status]
        if capability is not None:
            records = [r for r in records if r.capability == capability]
        records.sort(key=lambda r: r.created_at, reverse=True)
        return records[:limit], len(records)

    async def stats(self) -> dict:
        """Counts by displayed status and by capability, plus the total.

        Returns ``{"total": int, "by_status": {...}, "by_capability": {...}}``.
        Like ``list_page``, the default scans ``list()`` and the SQLite
        store overrides it with one aggregate query.
        """
        records = await self.list()
        by_status: dict[str, int] = {}
        by_capability: dict[str, int] = {}
        for record in records:
            by_status[record.status.value] = by_status.get(record.status.value, 0) + 1
            by_capability[record.capability] = by_capability.get(record.capability, 0) + 1
        return {"total": len(records), "by_status": by_status, "by_capability": by_capability}

    @abstractmethod
    async def remember_idempotency_key(self, key: str, job_id: str) -> None:
        """Record that ``key`` led to ``job_id``, evicting the oldest entry
        beyond ``MAX_IDEMPOTENCY_KEYS``. Re-remembering a key overwrites."""

    @abstractmethod
    async def recall_idempotency_key(self, key: str) -> Optional[str]:
        """The job id ``key`` previously led to, or None."""

    @abstractmethod
    async def set_webhook_status(self, job_id: str, webhook_status: str) -> None:
        """Record webhook delivery outcome ("delivered"/"failed") without
        touching the job's own status/result/error."""


class InMemoryJobStore(JobStore):
    """Dict-backed store. Fine for a single-process reference server.

    Nothing here survives a process restart, and there is no cross-process
    sharing -- if that matters, use ``SQLiteJobStore`` instead.
    """

    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._idempotency_keys: dict[str, str] = {}
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

    async def remember_idempotency_key(self, key: str, job_id: str) -> None:
        async with self._lock:
            self._idempotency_keys.pop(key, None)  # re-insert moves it to newest
            if len(self._idempotency_keys) >= MAX_IDEMPOTENCY_KEYS:
                self._idempotency_keys.pop(next(iter(self._idempotency_keys)))
            self._idempotency_keys[key] = job_id

    async def recall_idempotency_key(self, key: str) -> Optional[str]:
        async with self._lock:
            return self._idempotency_keys.get(key)

    async def set_webhook_status(self, job_id: str, webhook_status: str) -> None:
        async with self._lock:
            current = self._jobs.get(job_id)
            if current is not None:
                self._jobs[job_id] = current.model_copy(
                    update={"webhook_status": webhook_status, "updated_at": clock.now()}
                )


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
    webhook_url TEXT,
    webhook_status TEXT
)
"""

_IDEMPOTENCY_SCHEMA = """
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    created_at TEXT NOT NULL
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
        webhook_status=row["webhook_status"],
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
        self._conn.execute(_IDEMPOTENCY_SCHEMA)
        # A database created before webhook_status existed lacks the column;
        # CREATE TABLE IF NOT EXISTS never adds one.
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(jobs)")}
        if "webhook_status" not in columns:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN webhook_status TEXT")
        # Indexes for the hot paths the audit measured: newest-first paging
        # (list_page), status-filtered listings, and the idempotency-key
        # eviction sort, which otherwise re-sorts the whole key table on
        # every remembered key once the cap is reached.
        self._conn.execute("CREATE INDEX IF NOT EXISTS ix_jobs_created_at ON jobs(created_at)")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_jobs_status_created_at ON jobs(status, created_at)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_idempotency_keys_created_at "
            "ON idempotency_keys(created_at)"
        )
        self._conn.commit()
        # From here on, transactions are explicit: every write runs inside
        # BEGIN IMMEDIATE (see _write_txn), which takes the write lock at
        # BEGIN time instead of at first write. Under a deferred
        # transaction, two processes sharing this file can both start
        # reading and then deadlock-abort ("database is locked" without
        # consuming busy_timeout) when both try to upgrade to a write;
        # IMMEDIATE makes the second writer queue on busy_timeout instead.
        self._conn.isolation_level = None

    def close(self) -> None:
        self._conn.close()

    def _write_txn(self, statements: list[tuple[str, tuple]]) -> None:
        """Run statements inside one BEGIN IMMEDIATE transaction."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            for sql, args in statements:
                self._conn.execute(sql, args)
        except BaseException:
            self._conn.rollback()
            raise
        self._conn.commit()

    async def create(self, record: JobRecord) -> None:
        async with self._lock:
            await asyncio.to_thread(self._insert, record)

    def _insert(self, record: JobRecord) -> None:
        self._write_txn(
            [
                (
                    """
                    INSERT INTO jobs (
                        id, capability, provider, params, status,
                        created_at, updated_at, result, error, result_expires_at,
                        webhook_url, webhook_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        record.webhook_status,
                    ),
                )
            ]
        )

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
        self._write_txn(
            [
                (
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
            ]
        )

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

    #: The status a caller actually observes: a terminal row whose result
    #: window has passed reads as 'expired' even though nothing rewrites the
    #: stored status (expiry is a pure function of now -- see module
    #: docstring). Any SQL that filters or groups by status must use this
    #: expression, not the raw column, or a ready-but-expired job would leak
    #: into a status=ready listing. The placeholder is "now" as an isoformat
    #: string; stored timestamps come from the same clock, so lexicographic
    #: comparison is chronological.
    _DISPLAY_STATUS_SQL = (
        "CASE WHEN status IN ('ready', 'error') "
        "AND result_expires_at IS NOT NULL AND result_expires_at <= ? "
        "THEN 'expired' ELSE status END"
    )

    def _select_page(
        self, status: Optional[str], capability: Optional[str], limit: int, now_iso: str
    ) -> tuple[list[sqlite3.Row], int]:
        where = f"WHERE (? IS NULL OR {self._DISPLAY_STATUS_SQL} = ?) AND (? IS NULL OR capability = ?)"
        filter_args = (status, now_iso, status, capability, capability)
        rows = self._conn.execute(
            f"SELECT * FROM jobs {where} ORDER BY created_at DESC, rowid DESC LIMIT ?",
            (*filter_args, limit),
        ).fetchall()
        total = self._conn.execute(
            f"SELECT COUNT(*) FROM jobs {where}", filter_args
        ).fetchone()[0]
        return rows, total

    async def list_page(
        self,
        *,
        status: Optional[JobStatus] = None,
        capability: Optional[str] = None,
        limit: int = 50,
    ) -> tuple[list[JobRecord], int]:
        now_iso = clock.now().isoformat()
        wanted = status.value if status is not None else None
        async with self._lock:
            rows, total = await asyncio.to_thread(
                self._select_page, wanted, capability, limit, now_iso
            )
        return [_apply_expiry(_row_to_record(r)) for r in rows], total

    def _select_stats(self, now_iso: str) -> list[sqlite3.Row]:
        return self._conn.execute(
            f"SELECT {self._DISPLAY_STATUS_SQL} AS status, capability, COUNT(*) AS n "
            "FROM jobs GROUP BY 1, 2",
            (now_iso,),
        ).fetchall()

    async def stats(self) -> dict:
        now_iso = clock.now().isoformat()
        async with self._lock:
            rows = await asyncio.to_thread(self._select_stats, now_iso)
        by_status: dict[str, int] = {}
        by_capability: dict[str, int] = {}
        total = 0
        for row in rows:
            by_status[row["status"]] = by_status.get(row["status"], 0) + row["n"]
            by_capability[row["capability"]] = by_capability.get(row["capability"], 0) + row["n"]
            total += row["n"]
        return {"total": total, "by_status": by_status, "by_capability": by_capability}

    def _remember_key(self, key: str, job_id: str) -> None:
        self._write_txn(
            [
                (
                    "INSERT OR REPLACE INTO idempotency_keys (key, job_id, created_at) VALUES (?, ?, ?)",
                    (key, job_id, clock.now().isoformat()),
                ),
                (
                    # Evict oldest beyond the cap; rowid breaks created_at
                    # ties so eviction order stays deterministic.
                    """
                    DELETE FROM idempotency_keys WHERE key IN (
                        SELECT key FROM idempotency_keys
                        ORDER BY created_at DESC, rowid DESC
                        LIMIT -1 OFFSET ?
                    )
                    """,
                    (MAX_IDEMPOTENCY_KEYS,),
                ),
            ]
        )

    async def remember_idempotency_key(self, key: str, job_id: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._remember_key, key, job_id)

    def _recall_key(self, key: str) -> Optional[str]:
        row = self._conn.execute(
            "SELECT job_id FROM idempotency_keys WHERE key = ?", (key,)
        ).fetchone()
        return row["job_id"] if row is not None else None

    async def recall_idempotency_key(self, key: str) -> Optional[str]:
        async with self._lock:
            return await asyncio.to_thread(self._recall_key, key)

    def _set_webhook_status(self, job_id: str, webhook_status: str, updated_at: datetime) -> None:
        self._write_txn(
            [
                (
                    "UPDATE jobs SET webhook_status = ?, updated_at = ? WHERE id = ?",
                    (webhook_status, updated_at.isoformat(), job_id),
                )
            ]
        )

    async def set_webhook_status(self, job_id: str, webhook_status: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._set_webhook_status, job_id, webhook_status, clock.now())
