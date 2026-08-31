from __future__ import annotations

import asyncio
from datetime import timedelta

import httpx
import pytest
from httpx import ASGITransport

from ai_job_gateway import clock
from ai_job_gateway.manager import JobManager
from ai_job_gateway.providers import EchoProvider, MockProvider
from ai_job_gateway.server import create_app
from ai_job_gateway.store import InMemoryJobStore


@pytest.fixture
def app_and_manager():
    store = InMemoryJobStore()
    registry = {
        "mock-generate": MockProvider(delay_seconds=0.01),
        "echo": EchoProvider(),
        "always-fails": MockProvider(delay_seconds=0.01, should_fail=True, failure_message="boom"),
    }
    manager = JobManager(store, registry, result_ttl=timedelta(minutes=30))
    app = create_app(manager)
    return app, manager


@pytest.fixture
async def client(app_and_manager):
    app, _ = app_and_manager
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def _poll_until_terminal(client: httpx.AsyncClient, polling_url: str, timeout: float = 2.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        resp = await client.get(polling_url)
        body = resp.json()
        if resp.status_code == 200 and body["status"] in ("ready", "error"):
            return resp
        if asyncio.get_event_loop().time() >= deadline:
            raise AssertionError(f"job never reached a terminal status: {body}")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_submit_returns_202_with_id_and_polling_url(client):
    resp = await client.post("/v1/echo", json={"hello": "world"})
    assert resp.status_code == 202
    body = resp.json()
    assert "id" in body
    assert body["polling_url"] == f"/v1/jobs/{body['id']}"


@pytest.mark.asyncio
async def test_submit_unknown_capability_is_404(client):
    resp = await client.post("/v1/nonexistent", json={"a": 1})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_submit_empty_body_is_422(client):
    resp = await client.post("/v1/echo", json={})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_submit_non_object_body_is_422(client):
    resp = await client.post("/v1/echo", json=[1, 2, 3])
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_full_submit_poll_ready_cycle(client):
    submit_resp = await client.post("/v1/mock-generate", json={"prompt": "hi"})
    polling_url = submit_resp.json()["polling_url"]

    final_resp = await _poll_until_terminal(client, polling_url)
    body = final_resp.json()
    assert body["status"] == "ready"
    assert body["result"]["params_received"] == {"prompt": "hi"}
    assert body["result_expires_at"] is not None


@pytest.mark.asyncio
async def test_submit_error_path_reports_error_over_http(client):
    submit_resp = await client.post("/v1/always-fails", json={"a": 1})
    polling_url = submit_resp.json()["polling_url"]

    final_resp = await _poll_until_terminal(client, polling_url)
    body = final_resp.json()
    assert body["status"] == "error"
    assert body["error"] == "boom"


@pytest.mark.asyncio
async def test_get_unknown_job_id_is_404(client):
    resp = await client.get("/v1/jobs/does-not-exist")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_expired_job_returns_410(client, monkeypatch):
    submit_resp = await client.post("/v1/mock-generate", json={"a": 1})
    polling_url = submit_resp.json()["polling_url"]
    final_resp = await _poll_until_terminal(client, polling_url)
    expires_at_str = final_resp.json()["result_expires_at"]

    from datetime import datetime

    expires_at = datetime.fromisoformat(expires_at_str)
    monkeypatch.setattr(clock, "now", lambda: expires_at + timedelta(seconds=1))

    resp = await client.get(polling_url)
    assert resp.status_code == 410


@pytest.mark.asyncio
async def test_capabilities_endpoint_lists_registry(client):
    resp = await client.get("/v1/capabilities")
    assert resp.status_code == 200
    body = resp.json()
    assert body["echo"] == "echo"
    assert body["mock-generate"] == "mock"


@pytest.mark.asyncio
async def test_submit_webhook_url_is_stripped_from_params(client, app_and_manager):
    _, manager = app_and_manager
    submit_resp = await client.post(
        "/v1/echo", json={"prompt": "hi", "webhook_url": "https://example.test/hook"}
    )
    job_id = submit_resp.json()["id"]
    record = await manager.store.get(job_id)
    assert record.webhook_url == "https://example.test/hook"
    assert "webhook_url" not in record.params
    assert record.params == {"prompt": "hi"}
