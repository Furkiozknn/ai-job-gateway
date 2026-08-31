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
