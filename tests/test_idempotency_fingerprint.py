"""An Idempotency-Key reused with a *different* request must be refused.

Before the fingerprint, the key alone decided: the same key with other
params silently returned the original, unrelated job, so the caller got a
result for a request it never made. Stripe rejects that case and the IETF
Idempotency-Key draft recommends 422; these tests pin that behaviour for
both stores, across a restart, and for keys remembered before fingerprints
existed.
"""

from __future__ import annotations

import sqlite3

import httpx
import pytest

from ai_job_gateway.exceptions import IdempotencyKeyReusedError
from ai_job_gateway.manager import JobManager, request_fingerprint
from ai_job_gateway.providers import EchoProvider, MockProvider
from ai_job_gateway.server import create_app
from ai_job_gateway.store import InMemoryJobStore, JobStore, SQLiteJobStore


class CountingProvider(MockProvider):
    name = "counting"

    def __init__(self) -> None:
        super().__init__(delay_seconds=0.0)
        self.runs = 0

    async def run(self, job_id, params):
        self.runs += 1
        return await super().run(job_id, params)


@pytest.fixture(params=["memory", "sqlite"])
def any_store(request, tmp_path):
    if request.param == "memory":
        yield InMemoryJobStore()
    else:
        store = SQLiteJobStore(str(tmp_path / "jobs.db"))
        yield store
        store.close()


def test_fingerprint_ignores_key_order_and_covers_every_field():
    a = request_fingerprint("echo", {"a": 1, "b": [1, 2]}, None)
    assert a == request_fingerprint("echo", {"b": [1, 2], "a": 1}, None)
    assert a != request_fingerprint("echo", {"a": 2, "b": [1, 2]}, None)
    assert a != request_fingerprint("other", {"a": 1, "b": [1, 2]}, None)
    assert a != request_fingerprint("echo", {"a": 1, "b": [1, 2]}, "https://hook.test/x")
    assert len(a) == 64  # hex SHA-256


async def test_same_key_same_request_still_returns_the_original_job(any_store):
    provider = CountingProvider()
    manager = JobManager(any_store, {"counting": provider})
    first = await manager.submit("counting", {"a": 1, "b": 2}, idempotency_key="k")
    second = await manager.submit("counting", {"b": 2, "a": 1}, idempotency_key="k")
    assert second.id == first.id
    await manager.aclose()
    assert provider.runs == 1


@pytest.mark.parametrize(
    "capability, params, webhook_url",
    [
        ("counting", {"a": 2}, None),  # different params
        ("other", {"a": 1}, None),  # different capability
        ("counting", {"a": 1}, "https://hook.test/x"),  # different webhook_url
    ],
)
async def test_same_key_different_request_is_refused(any_store, capability, params, webhook_url):
    provider = CountingProvider()
    manager = JobManager(any_store, {"counting": provider, "other": provider})
    first = await manager.submit("counting", {"a": 1}, idempotency_key="k")

    with pytest.raises(IdempotencyKeyReusedError) as info:
        await manager.submit(capability, params, webhook_url=webhook_url, idempotency_key="k")

    assert info.value.job_id == first.id
    assert "'k'" in str(info.value)
    # Refusing must not create, run or re-point anything.
    assert [r.id for r in await any_store.list()] == [first.id]
    assert await any_store.recall_idempotency_key("k") == first.id
    await manager.aclose()
    assert provider.runs == 1


async def test_the_fingerprint_survives_a_restart(tmp_path):
    path = str(tmp_path / "jobs.db")
    store1 = SQLiteJobStore(path)
    manager1 = JobManager(store1, {"echo": EchoProvider()})
    first = await manager1.submit("echo", {"prompt": "a cat"}, idempotency_key="restart")
    await manager1.aclose()
    store1.close()

    store2 = SQLiteJobStore(path)
    manager2 = JobManager(store2, {"echo": EchoProvider()})
    try:
        assert (await manager2.submit("echo", {"prompt": "a cat"}, idempotency_key="restart")).id == first.id
        with pytest.raises(IdempotencyKeyReusedError):
            await manager2.submit("echo", {"prompt": "a dog"}, idempotency_key="restart")
    finally:
        await manager2.aclose()
        store2.close()


async def test_a_key_stored_before_fingerprints_existed_is_checked_against_its_record(tmp_path):
    """Upgrade path: an existing database has an idempotency_keys table
    without the fingerprint column. Opening it adds the column; the old key
    has no fingerprint, so the manager fingerprints the original record."""
    path = str(tmp_path / "jobs.db")
    store = SQLiteJobStore(path)
    manager = JobManager(store, {"echo": EchoProvider()})
    first = await manager.submit("echo", {"prompt": "a cat"}, idempotency_key="legacy")
    await manager.aclose()
    store.close()

    # Rebuild the table as the previous release created it.
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE old_keys (key TEXT PRIMARY KEY, job_id TEXT NOT NULL, created_at TEXT NOT NULL);
        INSERT INTO old_keys SELECT key, job_id, created_at FROM idempotency_keys;
        DROP TABLE idempotency_keys;
        ALTER TABLE old_keys RENAME TO idempotency_keys;
        """
    )
    conn.commit()
    conn.close()

    store = SQLiteJobStore(path)
    manager = JobManager(store, {"echo": EchoProvider()})
    try:
        assert await store.recall_idempotency_fingerprint("legacy") is None
        assert (await manager.submit("echo", {"prompt": "a cat"}, idempotency_key="legacy")).id == first.id
        with pytest.raises(IdempotencyKeyReusedError):
            await manager.submit("echo", {"prompt": "a dog"}, idempotency_key="legacy")
    finally:
        await manager.aclose()
        store.close()


async def test_a_custom_store_without_fingerprint_support_still_gets_the_check():
    """A JobStore written against the old interface inherits the no-op
    defaults; the record-based fallback still refuses a mismatched retry."""

    class LegacyStore(InMemoryJobStore):
        remember_idempotency_fingerprint = JobStore.remember_idempotency_fingerprint
        recall_idempotency_fingerprint = JobStore.recall_idempotency_fingerprint

    store = LegacyStore()
    manager = JobManager(store, {"echo": EchoProvider()})
    first = await manager.submit("echo", {"prompt": "a cat"}, idempotency_key="custom")
    assert await store.recall_idempotency_fingerprint("custom") is None
    assert (await manager.submit("echo", {"prompt": "a cat"}, idempotency_key="custom")).id == first.id
    with pytest.raises(IdempotencyKeyReusedError):
        await manager.submit("echo", {"prompt": "a dog"}, idempotency_key="custom")
    await manager.aclose()


async def test_an_evicted_key_takes_its_fingerprint_with_it(monkeypatch):
    from ai_job_gateway import store as store_module

    monkeypatch.setattr(store_module, "MAX_IDEMPOTENCY_KEYS", 2)
    store = InMemoryJobStore()
    for key in ("k1", "k2", "k3"):
        await store.remember_idempotency_key(key, f"job-{key}")
        await store.remember_idempotency_fingerprint(key, f"fp-{key}")
    assert await store.recall_idempotency_key("k1") is None
    assert await store.recall_idempotency_fingerprint("k1") is None
    assert await store.recall_idempotency_fingerprint("k3") == "fp-k3"
    # Re-pointing a key starts it without a fingerprint until one is stored.
    await store.remember_idempotency_key("k3", "job-new")
    assert await store.recall_idempotency_fingerprint("k3") is None


async def test_http_reuse_with_a_different_body_is_a_422_and_creates_nothing():
    manager = JobManager(InMemoryJobStore(), {"echo": EchoProvider()})
    app = create_app(manager)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gateway.test") as client:
        headers = {"Idempotency-Key": "order-4711"}
        first = await client.post("/v1/echo", json={"prompt": "a cat"}, headers=headers)
        assert first.status_code == 202

        retry = await client.post("/v1/echo", json={"prompt": "a cat"}, headers=headers)
        assert retry.status_code == 202
        assert retry.json()["id"] == first.json()["id"]

        reused = await client.post("/v1/echo", json={"prompt": "a dog"}, headers=headers)
        assert reused.status_code == 422
        assert reused.json() == {
            "detail": (
                "Idempotency-Key 'order-4711' was already used for a different request "
                "(capability, params or webhook_url); use a new key for a new request"
            )
        }
    await manager.aclose()
    assert len(await manager.store.list()) == 1
