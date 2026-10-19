"""Job cancellation: JobManager.cancel(), POST /v1/jobs/{id}/cancel, the client."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import httpx
import pytest
from httpx import ASGITransport

from ai_job_gateway import JobGatewayClient, JobNotCancellableError, JobNotFoundError
from ai_job_gateway.manager import CANCELLED_ERROR, JobManager
from ai_job_gateway.models import JobStatus
from ai_job_gateway.providers import EchoProvider, Provider
from ai_job_gateway.server import create_app
from ai_job_gateway.store import InMemoryJobStore, SQLiteJobStore


class SlowProvider(Provider):
    """Runs until cancelled; records that it saw the cancellation."""

    name = "slow"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.calls = 0
        self.saw_cancel = False

    async def run(self, job_id: str, params: dict) -> dict:
        self.calls += 1
        self.started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            self.saw_cancel = True
            raise
        return {"done": True}


class StubbornProvider(Provider):
    """Swallows the cancellation and returns a result later anyway."""

    name = "stubborn"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.returned = asyncio.Event()

    async def run(self, job_id: str, params: dict) -> dict:
        self.started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            pass  # a badly behaved provider: keeps going
        await self.release.wait()
        self.returned.set()
        return {"late": True}


def _recording_client(seen: list[dict]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def _until(predicate, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.005)


async def test_cancel_a_running_job_stops_the_provider_and_records_it():
    slow = SlowProvider()
    manager = JobManager(InMemoryJobStore(), {"slow": slow})
    record = await manager.submit("slow", {"x": 1})
    await asyncio.wait_for(slow.started.wait(), 1)

    cancelled = await manager.cancel(record.id)

    assert cancelled.status == JobStatus.ERROR
    assert cancelled.error == CANCELLED_ERROR
    assert cancelled.result is None
    assert cancelled.result_expires_at is not None
    assert slow.saw_cancel
    stored = await manager.store.get(record.id)
    assert stored.status == JobStatus.ERROR and stored.error == CANCELLED_ERROR
    await manager.aclose(drain_timeout=1)


async def test_cancel_a_pending_job_means_its_provider_never_runs():
    slow = SlowProvider()
    manager = JobManager(InMemoryJobStore(), {"slow": slow}, max_concurrent_jobs=1)
    first = await manager.submit("slow", {"n": 1})
    await asyncio.wait_for(slow.started.wait(), 1)
    second = await manager.submit("slow", {"n": 2})
    await asyncio.sleep(0.02)
    assert (await manager.store.get(second.id)).status == JobStatus.PENDING

    cancelled = await manager.cancel(second.id)
    assert cancelled.error == CANCELLED_ERROR

    # Freeing the slot must not start the cancelled job.
    await manager.cancel(first.id)
    await asyncio.sleep(0.05)
    assert slow.calls == 1
    await manager.aclose(drain_timeout=1)


async def test_cancel_right_after_submit_before_the_task_ever_ran():
    slow = SlowProvider()
    manager = JobManager(InMemoryJobStore(), {"slow": slow})
    record = await manager.submit("slow", {"x": 1})
    # No await between submit and cancel that would let the task start.
    cancelled = await manager.cancel(record.id)
    assert cancelled.error == CANCELLED_ERROR
    await asyncio.sleep(0.05)
    assert slow.calls == 0
    assert (await manager.store.get(record.id)).error == CANCELLED_ERROR
    await manager.aclose(drain_timeout=1)


async def test_a_finished_job_cannot_be_cancelled_and_an_unknown_one_is_not_found():
    manager = JobManager(InMemoryJobStore(), {"echo": EchoProvider()})
    record = await manager.submit("echo", {"x": 1})
    await _until(lambda: record.id not in manager._active_jobs)
    with pytest.raises(JobNotCancellableError) as excinfo:
        await manager.cancel(record.id)
    assert excinfo.value.status == "ready"
    assert (await manager.store.get(record.id)).status == JobStatus.READY
    with pytest.raises(JobNotFoundError):
        await manager.cancel("no-such-job")


async def test_cancelling_twice_returns_the_same_cancelled_record():
    slow = SlowProvider()
    manager = JobManager(InMemoryJobStore(), {"slow": slow})
    record = await manager.submit("slow", {"x": 1})
    await asyncio.wait_for(slow.started.wait(), 1)

    one, two = await asyncio.gather(manager.cancel(record.id), manager.cancel(record.id))
    three = await manager.cancel(record.id)

    assert one.error == two.error == three.error == CANCELLED_ERROR
    assert one.id == two.id == three.id == record.id
    await manager.aclose(drain_timeout=1)


async def test_a_provider_that_ignores_cancellation_cannot_overwrite_it():
    stubborn = StubbornProvider()
    manager = JobManager(InMemoryJobStore(), {"stubborn": stubborn})
    record = await manager.submit("stubborn", {"x": 1})
    await asyncio.wait_for(stubborn.started.wait(), 1)

    cancelled = await manager.cancel(record.id, grace_seconds=0.05)
    assert cancelled.error == CANCELLED_ERROR

    stubborn.release.set()
    await asyncio.wait_for(stubborn.returned.wait(), 1)
    await _until(lambda: record.id not in manager._active_jobs)
    await asyncio.sleep(0.02)
    stored = await manager.store.get(record.id)
    assert stored.status == JobStatus.ERROR
    assert stored.error == CANCELLED_ERROR
    assert stored.result is None
    assert record.id not in manager._cancelling  # bookkeeping released
    await manager.aclose(drain_timeout=1)


async def test_cancellation_fires_the_webhook_once_with_the_cancelled_record():
    seen: list[dict] = []
    slow = SlowProvider()
    manager = JobManager(InMemoryJobStore(), {"slow": slow}, http_client=_recording_client(seen))
    record = await manager.submit("slow", {"x": 1}, webhook_url="https://hooks.example.test/x")
    await asyncio.wait_for(slow.started.wait(), 1)

    await manager.cancel(record.id)
    await _until(lambda: len(seen) == 1)
    await asyncio.sleep(0.05)

    assert len(seen) == 1
    assert seen[0]["id"] == record.id
    assert seen[0]["status"] == "error"
    assert seen[0]["error"] == CANCELLED_ERROR
    await _until(lambda: manager.store._jobs[record.id].webhook_status == "delivered")
    await manager.aclose(drain_timeout=1)


async def test_cancel_with_the_sqlite_store(tmp_path):
    store = SQLiteJobStore(str(tmp_path / "jobs.db"))
    slow = SlowProvider()
    manager = JobManager(store, {"slow": slow})
    try:
        record = await manager.submit("slow", {"x": 1})
        await asyncio.wait_for(slow.started.wait(), 1)
        cancelled = await manager.cancel(record.id)
        assert cancelled.error == CANCELLED_ERROR
        assert (await store.get(record.id)).error == CANCELLED_ERROR
        await manager.aclose(drain_timeout=1)
    finally:
        store.close()


async def test_job_timeout_still_fails_a_job_and_is_not_reported_as_cancelled():
    slow = SlowProvider()
    manager = JobManager(
        InMemoryJobStore(), {"slow": slow}, job_timeout=timedelta(seconds=0.05)
    )
    record = await manager.submit("slow", {"x": 1})
    await _until(lambda: record.id not in manager._active_jobs)
    stored = await manager.store.get(record.id)
    assert stored.status == JobStatus.ERROR
    assert "did not finish within" in stored.error
    assert stored.error != CANCELLED_ERROR


# --- HTTP and client --------------------------------------------------------


@pytest.fixture
def slow_app():
    slow = SlowProvider()
    manager = JobManager(InMemoryJobStore(), {"slow": slow, "echo": EchoProvider()})
    return create_app(manager), manager, slow


async def test_http_cancel_endpoint(slow_app):
    app, manager, slow = slow_app
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        submitted = (await c.post("/v1/slow", json={"prompt": "p"})).json()
        await asyncio.wait_for(slow.started.wait(), 1)

        resp = await c.post(f"/v1/jobs/{submitted['id']}/cancel")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "error"
        assert body["error"] == CANCELLED_ERROR

        polled = (await c.get(submitted["polling_url"])).json()
        assert polled["status"] == "error" and polled["error"] == CANCELLED_ERROR

        again = await c.post(f"/v1/jobs/{submitted['id']}/cancel")
        assert again.status_code == 200

        missing = await c.post("/v1/jobs/nope/cancel")
        assert missing.status_code == 404

        done = (await c.post("/v1/echo", json={"x": 1})).json()
        await _until(lambda: done["id"] not in manager._active_jobs)
        finished = await c.post(f"/v1/jobs/{done['id']}/cancel")
        assert finished.status_code == 409
        assert "already finished (ready)" in finished.json()["detail"]
    await manager.aclose(drain_timeout=1)


async def test_http_cancel_requires_the_api_key():
    manager = JobManager(InMemoryJobStore(), {"echo": EchoProvider()})
    app = create_app(manager, api_key="s3cret")
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        assert (await c.post("/v1/jobs/x/cancel")).status_code == 401
        authed = await c.post("/v1/jobs/x/cancel", headers={"Authorization": "Bearer s3cret"})
        assert authed.status_code == 404


async def test_cancel_is_in_the_openapi_schema(slow_app):
    app, _, _ = slow_app
    schema = app.openapi()
    operation = schema["paths"]["/v1/jobs/{job_id}/cancel"]["post"]
    assert {"200", "404", "409"} <= set(operation["responses"])


async def test_client_cancel(slow_app):
    app, manager, slow = slow_app
    http = httpx.AsyncClient(transport=ASGITransport(app=app))
    async with JobGatewayClient("http://test", http_client=http) as client:
        handle = await client.submit("slow", {"prompt": "p"})
        await asyncio.wait_for(slow.started.wait(), 1)
        record = await handle.cancel()
        assert record["error"] == CANCELLED_ERROR

        done = await client.submit("echo", {"x": 1})
        await done.wait(timeout=2, poll_interval=0.01)
        with pytest.raises(JobNotCancellableError) as excinfo:
            await client.cancel(done.job_id)
        assert "already finished" in str(excinfo.value)

        with pytest.raises(JobNotFoundError):
            await client.cancel("nope")
    await http.aclose()
    await manager.aclose(drain_timeout=1)
