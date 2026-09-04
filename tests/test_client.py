from __future__ import annotations

from datetime import timedelta

import httpx
import pytest
from httpx import ASGITransport

from ai_job_gateway.client import JobGatewayClient
from ai_job_gateway.exceptions import JobExpiredError, JobFailedError, JobSubmissionError
from ai_job_gateway.manager import JobManager
from ai_job_gateway.providers import EchoProvider, MockProvider
from ai_job_gateway.server import create_app
from ai_job_gateway.store import InMemoryJobStore


@pytest.fixture
async def gateway_client():
    registry = {
        "echo": EchoProvider(),
        "always-fails": MockProvider(delay_seconds=0.01, should_fail=True, failure_message="nope"),
        "slow": MockProvider(delay_seconds=0.3),
    }
    manager = JobManager(InMemoryJobStore(), registry, result_ttl=timedelta(minutes=30))
    app = create_app(manager)
    http_client = httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    async with JobGatewayClient("http://test", http_client=http_client) as client:
        yield client


@pytest.mark.asyncio
async def test_submit_and_wait_happy_path(gateway_client):
    handle = await gateway_client.submit("echo", {"prompt": "hi"})
    result = await handle.wait(timeout=5)
    assert result == {"echoed": {"prompt": "hi"}}


@pytest.mark.asyncio
async def test_wait_raises_job_failed_error_on_error_status(gateway_client):
    handle = await gateway_client.submit("always-fails", {"a": 1})
    with pytest.raises(JobFailedError, match="nope"):
        await handle.wait(timeout=5)


@pytest.mark.asyncio
async def test_wait_times_out_before_slow_job_finishes(gateway_client):
    handle = await gateway_client.submit("slow", {"a": 1})
    with pytest.raises(TimeoutError):
        await handle.wait(timeout=0.05, poll_interval=0.02)


@pytest.mark.asyncio
async def test_submit_unknown_capability_raises_job_submission_error(gateway_client):
    with pytest.raises(JobSubmissionError) as exc_info:
        await gateway_client.submit("nope", {"a": 1})
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_submit_with_idempotency_key_returns_same_handle_on_retry(gateway_client):
    handle1 = await gateway_client.submit("echo", {"prompt": "hi"}, idempotency_key="retry-abc")
    handle2 = await gateway_client.submit("echo", {"prompt": "hi"}, idempotency_key="retry-abc")
    assert handle1.job_id == handle2.job_id


@pytest.mark.asyncio
async def test_poll_raises_job_expired_error_once_past_ttl(gateway_client, monkeypatch):
    from ai_job_gateway import clock

    handle = await gateway_client.submit("echo", {"a": 1})
    result = await handle.wait(timeout=5)
    assert result == {"echoed": {"a": 1}}

    from datetime import datetime

    record = await handle.poll()
    expires_at = datetime.fromisoformat(record["result_expires_at"])
    monkeypatch.setattr(clock, "now", lambda: expires_at + timedelta(seconds=1))

    with pytest.raises(JobExpiredError):
        await handle.poll()


async def test_api_key_rides_on_every_request():
    seen: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, request.headers.get("authorization")))
        if request.method == "POST":
            return httpx.Response(202, json={"id": "j1", "polling_url": "/v1/jobs/j1"})
        return httpx.Response(200, json={"id": "j1", "status": "ready", "result": {}})

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with JobGatewayClient("http://test", api_key="s3cret", http_client=http_client) as client:
        handle = await client.submit("echo", {"prompt": "hi"}, idempotency_key="k-1")
        await handle.poll()

    assert [auth for _, auth in seen] == ["Bearer s3cret", "Bearer s3cret"]


async def test_no_api_key_sends_no_authorization_header():
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return httpx.Response(202, json={"id": "j1", "polling_url": "/v1/jobs/j1"})

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    async with JobGatewayClient("http://test", http_client=http_client) as client:
        await client.submit("echo", {"prompt": "hi"})

    assert seen == [None]


async def test_client_with_key_talks_to_a_keyed_server_end_to_end():
    from ai_job_gateway.manager import JobManager
    from ai_job_gateway.providers import EchoProvider
    from ai_job_gateway.server import create_app
    from ai_job_gateway.store import InMemoryJobStore
    from httpx import ASGITransport

    app = create_app(JobManager(InMemoryJobStore(), {"echo": EchoProvider()}), api_key="s3cret")
    http_client = httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
    async with JobGatewayClient("http://test", api_key="s3cret", http_client=http_client) as client:
        handle = await client.submit("echo", {"prompt": "merhaba"})
        result = await handle.wait(timeout=5)
    assert result["echoed"]["prompt"] == "merhaba"
