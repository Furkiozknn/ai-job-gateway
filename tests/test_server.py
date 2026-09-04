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


@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_submit_body_over_size_limit_is_413():
    from ai_job_gateway.manager import JobManager
    from ai_job_gateway.providers import EchoProvider
    from ai_job_gateway.server import create_app
    from ai_job_gateway.store import InMemoryJobStore

    manager = JobManager(InMemoryJobStore(), {"echo": EchoProvider()})
    app = create_app(manager, max_body_bytes=32)
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        resp = await c.post("/v1/echo", json={"prompt": "x" * 200})
    assert resp.status_code == 413


@pytest.mark.parametrize(
    "webhook_url",
    ["ftp://example.test/hook", "file:///etc/passwd", "not-a-url", "javascript:alert(1)"],
)
@pytest.mark.asyncio
async def test_submit_rejects_non_http_webhook_url(client, webhook_url):
    resp = await client.post("/v1/echo", json={"prompt": "hi", "webhook_url": webhook_url})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_submit_accepts_https_webhook_url(client):
    resp = await client.post(
        "/v1/echo", json={"prompt": "hi", "webhook_url": "https://example.test/hook"}
    )
    assert resp.status_code == 202


@pytest.mark.parametrize("capability", ["has space", "slash/here", "", "x" * 101, "emoji-😀"])
@pytest.mark.asyncio
async def test_submit_rejects_invalid_capability_name(client, capability):
    resp = await client.post(f"/v1/{capability}", json={"a": 1})
    assert resp.status_code in (404, 422)
    if capability and "/" not in capability:
        # a syntactically-invalid-but-nonempty single path segment should be
        # rejected as invalid input, not treated as merely "not registered"
        assert resp.status_code == 422


@pytest.mark.asyncio
async def test_submit_with_repeated_idempotency_key_does_not_duplicate_job(
    client, app_and_manager
):
    _, manager = app_and_manager
    headers = {"Idempotency-Key": "retry-123"}
    first = await client.post("/v1/echo", json={"prompt": "hi"}, headers=headers)
    second = await client.post("/v1/echo", json={"prompt": "hi"}, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    assert len(await manager.store.list()) == 1


@pytest.mark.asyncio
async def test_submit_without_idempotency_key_creates_distinct_jobs(client):
    first = await client.post("/v1/echo", json={"prompt": "hi"})
    second = await client.post("/v1/echo", json={"prompt": "hi"})
    assert first.json()["id"] != second.json()["id"]


# --- listing and stats -------------------------------------------------------

async def test_list_jobs_returns_newest_first_with_filters(client):
    a = (await client.post("/v1/echo", json={"n": 1})).json()
    await _poll_until_terminal(client, a["polling_url"])
    b = (await client.post("/v1/always-fails", json={"n": 2})).json()
    await _poll_until_terminal(client, b["polling_url"])
    c = (await client.post("/v1/echo", json={"n": 3})).json()
    await _poll_until_terminal(client, c["polling_url"])

    listing = (await client.get("/v1/jobs")).json()
    assert listing["count"] == 3 and listing["total_matching"] == 3
    assert [j["id"] for j in listing["jobs"]] == [c["id"], b["id"], a["id"]]

    errors = (await client.get("/v1/jobs", params={"status": "error"})).json()
    assert [j["id"] for j in errors["jobs"]] == [b["id"]]

    echoes = (await client.get("/v1/jobs", params={"capability": "echo", "limit": 1})).json()
    assert echoes["count"] == 1 and echoes["total_matching"] == 2
    assert echoes["jobs"][0]["id"] == c["id"]


async def test_list_jobs_rejects_an_unknown_status_and_a_hostile_capability(client):
    assert (await client.get("/v1/jobs", params={"status": "finished"})).status_code == 422
    assert (await client.get("/v1/jobs", params={"capability": "../x"})).status_code == 422
    assert (await client.get("/v1/jobs", params={"limit": 0})).status_code == 422


async def test_stats_counts_by_status_and_capability(client):
    for _ in range(2):
        r = (await client.post("/v1/echo", json={"x": 1})).json()
        await _poll_until_terminal(client, r["polling_url"])
    r = (await client.post("/v1/always-fails", json={"x": 1})).json()
    await _poll_until_terminal(client, r["polling_url"])

    stats = (await client.get("/v1/stats")).json()
    assert stats["total"] == 3
    assert stats["by_status"]["ready"] == 2 and stats["by_status"]["error"] == 1
    assert stats["by_capability"] == {"always-fails": 1, "echo": 2}
    assert stats["registered_capabilities"] == 3


async def test_shutdown_closes_the_webhook_client_the_manager_created(app_and_manager):
    """The lifespan hook is what closes the shared client; exercised through
    the ASGI lifespan protocol rather than by calling aclose() directly."""
    app, manager = app_and_manager
    await manager._webhook_client()
    assert manager._http_client is not None

    scope = {"type": "lifespan"}
    messages = iter([{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])
    sent = []

    async def receive():
        return next(messages)

    async def send(message):
        sent.append(message["type"])

    await app(scope, receive, send)
    assert "lifespan.shutdown.complete" in sent
    assert manager._http_client is None
