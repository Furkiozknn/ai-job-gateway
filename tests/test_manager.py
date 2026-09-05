from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from ai_job_gateway import clock
from ai_job_gateway.manager import JobManager
from ai_job_gateway.models import JobStatus
from ai_job_gateway.providers import MockProvider
from ai_job_gateway.store import InMemoryJobStore


async def _wait_until_terminal(store, job_id, timeout=2.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        record = await store.get(job_id)
        if record.status in (JobStatus.READY, JobStatus.ERROR):
            return record
        if asyncio.get_event_loop().time() >= deadline:
            raise AssertionError(f"job {job_id} never reached a terminal status (last: {record.status})")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_submit_happy_path_reaches_ready(manager, store):
    record = await manager.submit("mock-generate", {"prompt": "a cat"})
    assert record.status == JobStatus.PENDING

    final = await _wait_until_terminal(store, record.id)
    assert final.status == JobStatus.READY
    assert final.result["params_received"] == {"prompt": "a cat"}
    assert final.result_expires_at is not None


@pytest.mark.asyncio
async def test_submit_error_path_reports_provider_message():
    registry = {"mock-generate": MockProvider(delay_seconds=0.01, should_fail=True, failure_message="nope")}
    store = InMemoryJobStore()
    manager = JobManager(store, registry)

    record = await manager.submit("mock-generate", {})
    final = await _wait_until_terminal(store, record.id)

    assert final.status == JobStatus.ERROR
    assert final.error == "nope"
    assert final.result is None


@pytest.mark.asyncio
async def test_unknown_capability_raises(manager):
    from ai_job_gateway.exceptions import UnknownCapabilityError

    with pytest.raises(UnknownCapabilityError):
        await manager.submit("does-not-exist", {})


@pytest.mark.asyncio
async def test_provider_exception_becomes_error_status(store):
    class ExplodingProvider(MockProvider):
        name = "boom"

        async def run(self, job_id, params):
            raise ValueError("kaboom")

    manager = JobManager(store, {"boom": ExplodingProvider()})
    record = await manager.submit("boom", {})
    final = await _wait_until_terminal(store, record.id)

    assert final.status == JobStatus.ERROR
    assert final.error == "kaboom"


@pytest.mark.asyncio
async def test_result_expires_after_ttl(monkeypatch, store):
    manager = JobManager(store, {"mock-generate": MockProvider(delay_seconds=0.01)}, result_ttl=timedelta(minutes=5))
    record = await manager.submit("mock-generate", {})
    final = await _wait_until_terminal(store, record.id)
    assert final.status == JobStatus.READY

    real_now = final.result_expires_at + timedelta(seconds=1)
    monkeypatch.setattr(clock, "now", lambda: real_now)

    expired = await store.get(record.id)
    assert expired.status == JobStatus.EXPIRED
    assert expired.result is None
    assert "expired" in expired.error.lower()


@pytest.mark.asyncio
async def test_webhook_delivered_on_completion(store):
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(await request.aread())
        return httpx.Response(200, json={"ok": True})

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager = JobManager(store, {"mock-generate": MockProvider(delay_seconds=0.01)}, http_client=http_client)

    record = await manager.submit("mock-generate", {"x": 1}, webhook_url="https://example.test/hook")
    await _wait_until_terminal(store, record.id)
    # webhook delivery is fired off after the record is already terminal; give
    # the background task a moment to actually run the (mocked, instant) POST.
    for _ in range(50):
        if calls:
            break
        await asyncio.sleep(0.01)

    assert len(calls) == 1
    import json

    payload = json.loads(calls[0])
    assert payload["status"] == "ready"
    assert payload["id"] == record.id


@pytest.mark.asyncio
async def test_webhook_never_raises_and_job_status_unaffected_on_delivery_failure(store, monkeypatch):
    import ai_job_gateway.manager as manager_module

    # Shrink the retry backoff so the (deliberately failing) delivery loop
    # exhausts quickly instead of taking many real seconds.
    monkeypatch.setattr(manager_module, "WEBHOOK_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(manager_module, "WEBHOOK_BACKOFF_CAP_SECONDS", 0.01)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager = JobManager(store, {"mock-generate": MockProvider(delay_seconds=0.01)}, http_client=http_client)

    record = await manager.submit("mock-generate", {}, webhook_url="https://example.test/hook")
    final = await _wait_until_terminal(store, record.id)
    assert final.status == JobStatus.READY  # webhook failing doesn't change this

    await asyncio.sleep(0.2)  # let the retry loop exhaust without raising


@pytest.mark.asyncio
async def test_store_failure_during_processing_transition_ends_job_as_error(store):
    """A store-layer failure (not a provider failure) while recording the
    ``processing`` transition must not leave the job stuck forever with an
    unretrieved task exception -- it should still land in ``error``."""

    class FlakyStore:
        """Wraps a real store; raises once on the first PROCESSING write."""

        def __init__(self, inner):
            self._inner = inner
            self._armed = True

        async def create(self, record):
            await self._inner.create(record)

        async def get(self, job_id):
            return await self._inner.get(job_id)

        async def update_status(self, job_id, **kwargs):
            if self._armed and kwargs.get("status") == JobStatus.PROCESSING:
                self._armed = False
                raise RuntimeError("simulated store outage")
            return await self._inner.update_status(job_id, **kwargs)

        async def list(self):
            return await self._inner.list()

        async def remember_idempotency_key(self, key, job_id):
            await self._inner.remember_idempotency_key(key, job_id)

        async def recall_idempotency_key(self, key):
            return await self._inner.recall_idempotency_key(key)

        async def set_webhook_status(self, job_id, webhook_status):
            await self._inner.set_webhook_status(job_id, webhook_status)

    flaky = FlakyStore(store)
    manager = JobManager(flaky, {"mock-generate": MockProvider(delay_seconds=0.01)})

    record = await manager.submit("mock-generate", {})
    final = await _wait_until_terminal(store, record.id)

    assert final.status == JobStatus.ERROR
    assert "internal error" in final.error.lower()


@pytest.mark.asyncio
async def test_webhook_delivery_includes_hmac_signature_when_secret_configured(store):
    import hashlib
    import hmac as hmac_module

    captured = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = await request.aread()
        captured["signature"] = request.headers.get("X-Gateway-Signature")
        return httpx.Response(200)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager = JobManager(
        store,
        {"mock-generate": MockProvider(delay_seconds=0.01)},
        http_client=http_client,
        webhook_signing_secret="s3cret",
    )

    record = await manager.submit("mock-generate", {}, webhook_url="https://example.test/hook")
    await _wait_until_terminal(store, record.id)
    for _ in range(50):
        if "signature" in captured:
            break
        await asyncio.sleep(0.01)

    expected = "sha256=" + hmac_module.new(
        b"s3cret", captured["body"], hashlib.sha256
    ).hexdigest()
    assert captured["signature"] == expected


@pytest.mark.asyncio
async def test_webhook_delivery_has_no_signature_header_without_secret(store):
    captured = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["signature"] = request.headers.get("X-Gateway-Signature")
        return httpx.Response(200)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager = JobManager(
        store, {"mock-generate": MockProvider(delay_seconds=0.01)}, http_client=http_client
    )

    record = await manager.submit("mock-generate", {}, webhook_url="https://example.test/hook")
    await _wait_until_terminal(store, record.id)
    for _ in range(50):
        if "signature" in captured:
            break
        await asyncio.sleep(0.01)

    assert captured["signature"] is None


@pytest.mark.asyncio
async def test_submit_with_same_idempotency_key_returns_original_record_and_runs_once(store):
    run_count = 0

    class CountingProvider(MockProvider):
        name = "counting"

        async def run(self, job_id, params):
            nonlocal run_count
            run_count += 1
            return await super().run(job_id, params)

    manager = JobManager(store, {"counting": CountingProvider(delay_seconds=0.01)})

    first = await manager.submit("counting", {"a": 1}, idempotency_key="key-1")
    second = await manager.submit("counting", {"a": 1}, idempotency_key="key-1")
    assert first.id == second.id

    await _wait_until_terminal(store, first.id)
    assert run_count == 1
    assert len(await store.list()) == 1


@pytest.mark.asyncio
async def test_submit_with_different_idempotency_keys_creates_distinct_jobs(manager):
    first = await manager.submit("echo", {"a": 1}, idempotency_key="key-a")
    second = await manager.submit("echo", {"a": 1}, idempotency_key="key-b")
    assert first.id != second.id


@pytest.mark.asyncio
async def test_submit_without_idempotency_key_never_dedupes(manager):
    first = await manager.submit("echo", {"a": 1})
    second = await manager.submit("echo", {"a": 1})
    assert first.id != second.id


@pytest.mark.asyncio
async def test_no_webhook_url_means_no_delivery_attempt(store):
    called = False

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager = JobManager(store, {"mock-generate": MockProvider(delay_seconds=0.01)}, http_client=http_client)

    record = await manager.submit("mock-generate", {})
    await _wait_until_terminal(store, record.id)
    await asyncio.sleep(0.05)

    assert called is False


async def test_webhook_deliveries_share_one_client_for_the_managers_lifetime(monkeypatch):
    """A client per delivery paid connection setup on every webhook and threw
    the pool away. One client per manager, closed by aclose()."""
    import httpx

    from ai_job_gateway import manager as manager_module
    from ai_job_gateway.manager import JobManager
    from ai_job_gateway.providers import EchoProvider
    from ai_job_gateway.store import InMemoryJobStore

    constructed = []
    received = []

    def handler(request):
        received.append(request.url.path)
        return httpx.Response(200)

    real_async_client = httpx.AsyncClient

    def counting_client(*args, **kwargs):
        constructed.append(1)
        return real_async_client(transport=httpx.MockTransport(handler))

    monkeypatch.setattr(manager_module.httpx, "AsyncClient", counting_client)

    store = InMemoryJobStore()
    manager = JobManager(store, {"echo": EchoProvider()})
    for i in range(3):
        record = await manager.submit("echo", {"i": i}, webhook_url=f"http://receiver.test/hook/{i}")
        for _ in range(100):
            current = await store.get(record.id)
            if current.status.value in ("ready", "error"):
                break
            await asyncio.sleep(0.01)
    for _ in range(100):
        if len(received) == 3:
            break
        await asyncio.sleep(0.01)

    assert received == ["/hook/0", "/hook/1", "/hook/2"]
    assert len(constructed) == 1
    await manager.aclose()
    assert manager._http_client is None


async def test_an_injected_client_is_never_closed_by_the_manager():
    import httpx

    from ai_job_gateway.manager import JobManager
    from ai_job_gateway.store import InMemoryJobStore

    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    manager = JobManager(InMemoryJobStore(), {}, http_client=client)
    await manager.aclose()
    assert not client.is_closed
    await client.aclose()


# --- durability: the three failure modes the ecosystem audit demonstrated ---


@pytest.mark.asyncio
async def test_idempotency_key_survives_a_restart_with_a_sqlite_store(tmp_path):
    """The audit's proven double-run: submit, restart the gateway (new store
    instance over the same file, new manager), retry the same key -- the
    provider must not run a second time."""
    from ai_job_gateway.store import SQLiteJobStore

    path = str(tmp_path / "jobs.db")
    run_count = 0

    class CountingProvider(MockProvider):
        name = "counting"

        async def run(self, job_id, params):
            nonlocal run_count
            run_count += 1
            return await super().run(job_id, params)

    store1 = SQLiteJobStore(path)
    manager1 = JobManager(store1, {"counting": CountingProvider(delay_seconds=0.01)})
    first = await manager1.submit("counting", {"a": 1}, idempotency_key="retry-across-restart")
    await _wait_until_terminal(store1, first.id)
    store1.close()

    store2 = SQLiteJobStore(path)
    manager2 = JobManager(store2, {"counting": CountingProvider(delay_seconds=0.01)})
    second = await manager2.submit("counting", {"a": 1}, idempotency_key="retry-across-restart")
    store2.close()

    assert second.id == first.id
    assert run_count == 1


@pytest.mark.asyncio
async def test_recover_interrupted_jobs_fails_stranded_jobs_honestly(store):
    """A job the previous process left processing has no task driving it;
    the startup sweep must land it in `error` with a resubmit message, and
    leave finished jobs alone."""
    manager = JobManager(store, {"mock-generate": MockProvider(delay_seconds=0.01)})

    done = await manager.submit("mock-generate", {})
    await _wait_until_terminal(store, done.id)

    from ai_job_gateway.models import JobRecord

    now = clock.now()
    stranded = JobRecord(
        id="stranded-1", capability="mock-generate", provider="mock",
        params={}, status=JobStatus.PROCESSING, created_at=now, updated_at=now,
    )
    await store.create(stranded)

    recovered = await manager.recover_interrupted_jobs()
    assert recovered == 1

    failed = await store.get("stranded-1")
    assert failed.status == JobStatus.ERROR
    assert "restart" in failed.error

    untouched = await store.get(done.id)
    assert untouched.status == JobStatus.READY


@pytest.mark.asyncio
async def test_webhook_outcome_recorded_as_delivered(store):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager = JobManager(store, {"mock-generate": MockProvider(delay_seconds=0.01)}, http_client=http_client)

    record = await manager.submit("mock-generate", {}, webhook_url="https://example.test/hook")
    await _wait_until_terminal(store, record.id)
    for _ in range(100):
        fetched = await store.get(record.id)
        if fetched.webhook_status is not None:
            break
        await asyncio.sleep(0.01)
    assert fetched.webhook_status == "delivered"


@pytest.mark.asyncio
async def test_webhook_exhaustion_dead_letters_the_job(store, monkeypatch):
    import ai_job_gateway.manager as manager_module

    monkeypatch.setattr(manager_module, "WEBHOOK_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(manager_module, "WEBHOOK_BACKOFF_CAP_SECONDS", 0.01)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager = JobManager(store, {"mock-generate": MockProvider(delay_seconds=0.01)}, http_client=http_client)

    record = await manager.submit("mock-generate", {}, webhook_url="https://example.test/hook")
    await _wait_until_terminal(store, record.id)
    for _ in range(100):
        fetched = await store.get(record.id)
        if fetched.webhook_status is not None:
            break
        await asyncio.sleep(0.01)
    assert fetched.webhook_status == "failed"
    assert fetched.status == JobStatus.READY  # delivery failing never touches the job itself


def test_webhook_backoff_is_capped_exponential_with_full_jitter():
    from ai_job_gateway.manager import (
        WEBHOOK_BACKOFF_BASE_SECONDS,
        WEBHOOK_BACKOFF_CAP_SECONDS,
        WEBHOOK_MAX_ATTEMPTS,
        _webhook_backoff_delays,
    )

    seen_bounds = []

    def fake_rng(low, high):
        seen_bounds.append((low, high))
        return high  # deterministic: always the upper bound

    delays = _webhook_backoff_delays(rng=fake_rng)
    assert len(delays) == WEBHOOK_MAX_ATTEMPTS - 1
    # Upper bounds double from the base and never exceed the cap.
    expected = [
        min(WEBHOOK_BACKOFF_CAP_SECONDS, WEBHOOK_BACKOFF_BASE_SECONDS * (2**n))
        for n in range(WEBHOOK_MAX_ATTEMPTS - 1)
    ]
    assert delays == expected
    assert all(low == 0.0 for low, _ in seen_bounds)  # full jitter: floor is zero
