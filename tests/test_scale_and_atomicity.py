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

import httpx
import pytest

from ai_job_gateway import clock
from ai_job_gateway.manager import DEFAULT_WEBHOOK_CONCURRENCY, JobManager
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


@pytest.mark.asyncio
async def test_recovery_fires_webhooks_in_waves_not_all_at_once():
    """The audit's webhook-amplification finding: a restart that recovers N
    unheard deliveries used to fire all N simultaneously.

    The per-attempt jitter already spread the herd's *retries*, but the
    first attempt of every recovered delivery went out at the same instant
    -- a thundering herd at a receiver that is very often recovering from
    the same outage, and N connections against one shared client pool.
    """
    store = InMemoryJobStore()
    unheard = 25
    for i in range(unheard):
        await store.create(
            JobRecord(
                id=f"unheard-{i}",
                capability="echo",
                provider="mock",
                params={},
                status=JobStatus.READY,
                created_at=clock.now(),
                updated_at=clock.now(),
                result={"ok": i},
                webhook_url=f"https://receiver.example/{i}",
            )
        )

    in_flight = 0
    peak = 0

    async def handler(request):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        try:
            # Long enough that every delivery admitted by the cap overlaps.
            await asyncio.sleep(0.05)
            return httpx.Response(200)
        finally:
            in_flight -= 1

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    cap = 4
    manager = JobManager(
        store,
        {"echo": MockProvider()},
        http_client=client,
        max_concurrent_webhooks=cap,
    )

    await manager.recover_interrupted_jobs()
    # recover_interrupted_jobs schedules the deliveries and returns; aclose()
    # closes the client without draining them, so wait for the outcomes.
    deadline = asyncio.get_event_loop().time() + 5.0
    while any(r.webhook_status is None for r in await store.list()):
        assert asyncio.get_event_loop().time() < deadline, "deliveries never finished"
        await asyncio.sleep(0.01)
    await manager.aclose()

    assert peak <= cap, f"{peak} webhooks were in flight at once, cap was {cap}"
    assert peak > 1, "the cap must still allow real concurrency, not serialize"
    delivered = [r for r in await store.list() if r.webhook_status == "delivered"]
    assert len(delivered) == unheard, "every recovered webhook must still be delivered"


@pytest.mark.asyncio
async def test_webhook_slot_is_not_held_across_the_backoff_sleep():
    """A retrying delivery must not occupy a slot while it sleeps.

    Holding the semaphore across the whole retry chain would let a handful
    of failing webhooks (up to ~65s of backoff) starve every other job's
    delivery. The slot is taken around the HTTP call only, so a delivery
    that is backing off leaves room for others to go out.
    """
    store = InMemoryJobStore()
    def _unheard(job_id, url):
        return JobRecord(
            id=job_id,
            capability="echo",
            provider="mock",
            params={},
            status=JobStatus.READY,
            created_at=clock.now(),
            updated_at=clock.now(),
            result={},
            webhook_url=url,
        )

    failing = _unheard("fails", "https://receiver.example/always-fails")
    healthy = _unheard("fine", "https://receiver.example/fine")
    await store.create(failing)
    await store.create(healthy)

    healthy_delivered = asyncio.Event()

    async def handler(request):
        if request.url.path == "/fine":
            healthy_delivered.set()
            return httpx.Response(200)
        return httpx.Response(500)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    # A cap of one: if the failing delivery held its slot through the
    # backoff, the healthy one could not get out until it exhausted.
    manager = JobManager(
        store,
        {"echo": MockProvider()},
        http_client=client,
        max_concurrent_webhooks=1,
    )
    await manager.recover_interrupted_jobs()
    await asyncio.wait_for(healthy_delivered.wait(), timeout=2.0)
    await manager.aclose()

    assert (await store.get(healthy.id)).webhook_status == "delivered"


@pytest.mark.asyncio
async def test_webhook_concurrency_is_capped_by_default():
    """The two tests above pass an explicit cap, so neither would notice the
    library default going back to unbounded - a mutation run proved exactly
    that. A deployment that constructs JobManager without naming the option
    is the common case, so pin it here.
    """
    manager = JobManager(InMemoryJobStore(), {"echo": MockProvider()})
    try:
        assert manager._webhook_slots is not None, (
            "JobManager must cap webhook concurrency by default"
        )
        assert DEFAULT_WEBHOOK_CONCURRENCY > 0
    finally:
        await manager.aclose()


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
@pytest.mark.asyncio
async def test_recovery_candidates_match_a_full_scan_exactly(store_kind, tmp_path):
    """The startup sweep no longer materialises every row ever stored, and
    the narrowed query must decide identically to the scan it replaced.

    The rows below cover each branch the sweep can take, including the two
    that make the predicate subtle: a ready job past its result window
    (displays 'expired', still has an unheard webhook, and is still re-fired
    today) and a terminal job whose delivery outcome was already recorded
    (skipped, and must not come back).
    """
    store = (
        InMemoryJobStore()
        if store_kind == "memory"
        else SQLiteJobStore(str(tmp_path / "recovery.db"))
    )
    now = clock.now()

    def rec(job_id, status, *, webhook=None, webhook_status=None, expires=None):
        return JobRecord(
            id=job_id, capability="cap", provider="p", params={}, status=status,
            created_at=now, updated_at=now, webhook_url=webhook,
            webhook_status=webhook_status, result_expires_at=expires,
        )

    rows = [
        rec("pending-no-hook", JobStatus.PENDING),
        rec("processing-with-hook", JobStatus.PROCESSING, webhook="https://e/1"),
        rec("ready-unheard", JobStatus.READY, webhook="https://e/2",
            expires=now + timedelta(hours=1)),
        rec("error-unheard", JobStatus.ERROR, webhook="https://e/3"),
        # Terminal, unheard, and already past its window: displays as
        # 'expired' yet must still be a candidate.
        rec("expired-unheard", JobStatus.READY, webhook="https://e/4",
            expires=now - timedelta(hours=1)),
        # Deliberately not candidates.
        rec("ready-delivered", JobStatus.READY, webhook="https://e/5",
            webhook_status="delivered"),
        rec("ready-failed", JobStatus.READY, webhook="https://e/6",
            webhook_status="failed"),
        rec("ready-no-hook", JobStatus.READY),
    ]
    for r in rows:
        await store.create(r)

    # What the old implementation looked at, filtered by the sweep's own rules.
    expected = {
        r.id
        for r in await store.list()
        if (r.status in (JobStatus.READY, JobStatus.ERROR, JobStatus.EXPIRED)
            and r.webhook_url and r.webhook_status is None)
        or r.status in (JobStatus.PENDING, JobStatus.PROCESSING)
    }
    got = {r.id for r in await store.list_recovery_candidates()}

    assert got == expected
    assert "expired-unheard" in got, "an expired-but-unheard webhook is still re-fired"
    assert "ready-delivered" not in got and "ready-no-hook" not in got


@pytest.mark.asyncio
async def test_aclose_lets_an_in_flight_webhook_finish():
    """Shutdown used to close the client out from under a delivery already
    on the wire, losing it invisibly: the job's own status is correct either
    way, so nothing recorded that the receiver never heard.
    """
    store = InMemoryJobStore()
    await store.create(
        JobRecord(
            id="mid-flight", capability="echo", provider="mock", params={},
            status=JobStatus.READY, created_at=clock.now(), updated_at=clock.now(),
            result={}, webhook_url="https://receiver.example/slow",
        )
    )

    async def handler(request):
        # Still in flight when aclose() is called below.
        await asyncio.sleep(0.2)
        return httpx.Response(200)

    manager = JobManager(
        store,
        {"echo": MockProvider()},
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    await manager.recover_interrupted_jobs()
    await asyncio.sleep(0)  # let the delivery task actually start
    await manager.aclose()

    assert (await store.get("mid-flight")).webhook_status == "delivered"


@pytest.mark.asyncio
async def test_aclose_does_not_hang_on_work_that_outlives_the_grace_period():
    """The drain is bounded on purpose: a webhook backing off can take ~65s
    and a provider run far longer, and shutdown must not wait for either.
    """
    store = InMemoryJobStore()
    await store.create(
        JobRecord(
            id="too-slow", capability="echo", provider="mock", params={},
            status=JobStatus.READY, created_at=clock.now(), updated_at=clock.now(),
            result={}, webhook_url="https://receiver.example/forever",
        )
    )

    async def handler(request):
        await asyncio.sleep(30)
        return httpx.Response(200)

    manager = JobManager(
        store,
        {"echo": MockProvider()},
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    await manager.recover_interrupted_jobs()
    await asyncio.sleep(0)
    started = asyncio.get_event_loop().time()
    await manager.aclose(drain_timeout=0.1)
    elapsed = asyncio.get_event_loop().time() - started

    assert elapsed < 5.0, f"aclose waited {elapsed:.1f}s on work it should abandon"
