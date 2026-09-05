"""Regressions for the audit's scale/atomicity findings.

Four properties, each of which measurably failed (or was demonstrated
exploitable) before the fix they pin down:

- two concurrent submissions carrying the same idempotency key must
  resolve to one job and one provider run (the recall -> remember ->
  create sequence yields between store calls, so without per-key
  serialization both could pass recall before either remembered);
- errored jobs must expire like ready ones (they were the one kind of
  record that never even reached "expired", so stores grew forever);
- a provider that never returns must be failed by the job timeout
  instead of reading "processing" until the next restart sweep;
- ``max_concurrent_jobs`` must actually bound provider overlap;
- ``list_page``/``stats`` must filter on the *displayed* status -- a
  ready job past its result window is "expired" to callers, and pushing
  the filter into SQL must not change that.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from ai_job_gateway import clock
from ai_job_gateway.manager import JobManager
from ai_job_gateway.models import JobRecord, JobStatus
from ai_job_gateway.providers import MockProvider
from ai_job_gateway.store import InMemoryJobStore, SQLiteJobStore


async def _wait_until_terminal(store, job_id, timeout=2.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        record = await store.get(job_id)
        if record.status in (JobStatus.READY, JobStatus.ERROR):
            return record
        if asyncio.get_event_loop().time() >= deadline:
            raise AssertionError(
                f"job {job_id} never reached a terminal status (last: {record.status})"
            )
        await asyncio.sleep(0.01)


class CountingProvider(MockProvider):
    name = "counting"

    def __init__(self) -> None:
        super().__init__(delay_seconds=0.02)
        self.runs = 0

    async def run(self, job_id, params):
        self.runs += 1
        return await super().run(job_id, params)


@pytest.mark.asyncio
@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
async def test_concurrent_retries_with_one_key_run_the_provider_once(tmp_path, store_kind):
    """The audit double-ran a provider with two concurrent same-key submits
    against the SQLite store, in a single process -- every store call is an
    awaited to_thread, so both submissions interleaved between recall and
    remember. Per-key serialization must make this impossible for both
    stores."""
    store = (
        InMemoryJobStore()
        if store_kind == "memory"
        else SQLiteJobStore(str(tmp_path / "jobs.db"))
    )
    provider = CountingProvider()
    manager = JobManager(store, {"counting": provider})

    first, second = await asyncio.gather(
        manager.submit("counting", {"n": 1}, idempotency_key="same-key"),
        manager.submit("counting", {"n": 1}, idempotency_key="same-key"),
    )
    assert first.id == second.id

    await _wait_until_terminal(store, first.id)
    await asyncio.sleep(0.05)  # room for a (wrong) second task to surface
    assert provider.runs == 1
    if isinstance(store, SQLiteJobStore):
        store.close()


@pytest.mark.asyncio
async def test_error_jobs_expire_like_ready_ones(monkeypatch):
    """Before this, an error record carried no result_expires_at, so it was
    permanently un-expirable -- unbounded growth with nothing marking the
    backlog stale."""
    store = InMemoryJobStore()
    manager = JobManager(
        store,
        {"boom": MockProvider(delay_seconds=0.01, should_fail=True, failure_message="nope")},
        result_ttl=timedelta(minutes=5),
    )
    record = await manager.submit("boom", {})
    final = await _wait_until_terminal(store, record.id)
    assert final.status == JobStatus.ERROR
    assert final.result_expires_at is not None

    later = final.result_expires_at + timedelta(seconds=1)
    monkeypatch.setattr(clock, "now", lambda: later)
    assert (await store.get(record.id)).status == JobStatus.EXPIRED


@pytest.mark.asyncio
async def test_a_hung_provider_is_failed_by_the_job_timeout():
    class HangingProvider(MockProvider):
        name = "hang"

        async def run(self, job_id, params):
            await asyncio.sleep(3600)

    store = InMemoryJobStore()
    manager = JobManager(
        store, {"hang": HangingProvider()}, job_timeout=timedelta(seconds=0.05)
    )
    record = await manager.submit("hang", {})
    final = await _wait_until_terminal(store, record.id)
    assert final.status == JobStatus.ERROR
    assert "did not finish within" in final.error
    assert final.result_expires_at is not None


@pytest.mark.asyncio
async def test_max_concurrent_jobs_caps_provider_overlap():
    class OverlapProvider(MockProvider):
        name = "overlap"

        def __init__(self) -> None:
            super().__init__(delay_seconds=0)
            self.active = 0
            self.max_active = 0

        async def run(self, job_id, params):
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            await asyncio.sleep(0.03)
            self.active -= 1
            return {"ok": True}

    provider = OverlapProvider()
    store = InMemoryJobStore()
    manager = JobManager(store, {"overlap": provider}, max_concurrent_jobs=1)

    records = [await manager.submit("overlap", {"i": i}) for i in range(3)]
    for record in records:
        await _wait_until_terminal(store, record.id)
    assert provider.max_active == 1


def _record(i, status, *, expires=None, capability="cap-a"):
    now = clock.now()
    return JobRecord(
        id=f"j{i}",
        capability=capability,
        provider="p",
        params={},
        status=status,
        created_at=now + timedelta(seconds=i),
        updated_at=now,
        result_expires_at=expires,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
async def test_list_page_and_stats_use_the_displayed_status(tmp_path, store_kind):
    """A ready job past its result window is "expired" to every caller.
    Pushing filters into SQL must preserve that: status=ready must not leak
    it, status=expired must find it, and stats must count it as expired.
    Parametrized over both stores so the SQL path and the default scan stay
    behaviorally identical."""
    store = (
        InMemoryJobStore()
        if store_kind == "memory"
        else SQLiteJobStore(str(tmp_path / "jobs.db"))
    )
    now = clock.now()
    await store.create(_record(1, JobStatus.READY, expires=now - timedelta(seconds=1)))
    await store.create(_record(2, JobStatus.READY, expires=now + timedelta(hours=1)))
    await store.create(
        _record(3, JobStatus.ERROR, expires=now + timedelta(hours=1), capability="cap-b")
    )
    await store.create(_record(4, JobStatus.PENDING))

    page, total = await store.list_page(status=JobStatus.READY, limit=10)
    assert [r.id for r in page] == ["j2"] and total == 1

    page, total = await store.list_page(status=JobStatus.EXPIRED, limit=10)
    assert [r.id for r in page] == ["j1"] and total == 1

    page, total = await store.list_page(limit=2)
    assert [r.id for r in page] == ["j4", "j3"]  # newest first
    assert total == 4

    page, total = await store.list_page(capability="cap-b", limit=10)
    assert [r.id for r in page] == ["j3"] and total == 1

    counts = await store.stats()
    assert counts["total"] == 4
    assert counts["by_status"] == {"expired": 1, "ready": 1, "error": 1, "pending": 1}
    assert counts["by_capability"] == {"cap-a": 3, "cap-b": 1}
    if isinstance(store, SQLiteJobStore):
        store.close()
