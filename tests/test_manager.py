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
    # exhausts quickly instead of taking several real seconds.
    monkeypatch.setattr(manager_module, "WEBHOOK_RETRY_DELAYS", (0.01, 0.01))

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
