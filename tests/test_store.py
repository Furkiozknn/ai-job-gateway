from __future__ import annotations

from datetime import timedelta

import pytest

from ai_job_gateway import clock
from ai_job_gateway.models import JobRecord, JobStatus
from ai_job_gateway.store import InMemoryJobStore, SQLiteJobStore


def _make_store(kind: str, tmp_path):
    if kind == "memory":
        return InMemoryJobStore()
    return SQLiteJobStore(str(tmp_path / "jobs.db"))


@pytest.fixture(params=["memory", "sqlite"])
def any_store(request, tmp_path):
    store = _make_store(request.param, tmp_path)
    yield store
    if hasattr(store, "close"):
        store.close()


def _new_record(**overrides) -> JobRecord:
    now = clock.now()
    defaults = dict(
        id="job-1",
        capability="mock-generate",
        provider="mock",
        params={"a": 1},
        status=JobStatus.PENDING,
        created_at=now,
        updated_at=now,
    )
    defaults.update(overrides)
    return JobRecord(**defaults)


@pytest.mark.asyncio
async def test_create_then_get_roundtrips(any_store):
    record = _new_record()
    await any_store.create(record)

    fetched = await any_store.get(record.id)
    assert fetched is not None
    assert fetched.id == record.id
    assert fetched.params == {"a": 1}
    assert fetched.status == JobStatus.PENDING


@pytest.mark.asyncio
async def test_get_unknown_id_returns_none(any_store):
    assert await any_store.get("does-not-exist") is None


@pytest.mark.asyncio
async def test_update_status_transitions_and_stamps_updated_at(any_store):
    record = _new_record()
    await any_store.create(record)

    updated = await any_store.update_status(
        record.id, status=JobStatus.READY, result={"ok": True}
    )
    assert updated.status == JobStatus.READY
    assert updated.result == {"ok": True}
    assert updated.updated_at >= record.updated_at

    fetched = await any_store.get(record.id)
    assert fetched.status == JobStatus.READY
    assert fetched.result == {"ok": True}


@pytest.mark.asyncio
async def test_list_returns_all_created_records(any_store):
    await any_store.create(_new_record(id="a"))
    await any_store.create(_new_record(id="b"))

    records = await any_store.list()
    assert {r.id for r in records} == {"a", "b"}


@pytest.mark.asyncio
async def test_terminal_job_past_ttl_reads_as_expired(any_store, monkeypatch):
    now = clock.now()
    record = _new_record(status=JobStatus.PENDING)
    await any_store.create(record)
    expires_at = now + timedelta(minutes=1)
    await any_store.update_status(
        record.id, status=JobStatus.READY, result={"x": 1}, result_expires_at=expires_at
    )

    monkeypatch.setattr(clock, "now", lambda: expires_at + timedelta(seconds=1))

    fetched = await any_store.get(record.id)
    assert fetched.status == JobStatus.EXPIRED
    assert fetched.result is None
    assert fetched.error is not None

    listed = await any_store.list()
    assert listed[0].status == JobStatus.EXPIRED


@pytest.mark.asyncio
async def test_terminal_job_before_ttl_is_not_expired(any_store, monkeypatch):
    now = clock.now()
    record = _new_record(status=JobStatus.PENDING)
    await any_store.create(record)
    expires_at = now + timedelta(minutes=30)
    await any_store.update_status(
        record.id, status=JobStatus.READY, result={"x": 1}, result_expires_at=expires_at
    )

    monkeypatch.setattr(clock, "now", lambda: expires_at - timedelta(seconds=1))

    fetched = await any_store.get(record.id)
    assert fetched.status == JobStatus.READY
    assert fetched.result == {"x": 1}


@pytest.mark.asyncio
async def test_pending_job_never_expires_even_far_in_the_future(any_store, monkeypatch):
    record = _new_record(status=JobStatus.PENDING)
    await any_store.create(record)

    monkeypatch.setattr(clock, "now", lambda: clock.now() + timedelta(days=365))
    fetched = await any_store.get(record.id)
    assert fetched.status == JobStatus.PENDING


@pytest.mark.asyncio
async def test_sqlite_store_survives_reopen(tmp_path):
    path = str(tmp_path / "jobs.db")
    store1 = SQLiteJobStore(path)
    await store1.create(_new_record(id="persisted"))
    store1.close()

    store2 = SQLiteJobStore(path)
    fetched = await store2.get("persisted")
    store2.close()

    assert fetched is not None
    assert fetched.id == "persisted"
