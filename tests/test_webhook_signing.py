"""Timestamped webhook signatures: what a receiver can rely on."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from ai_job_gateway import clock
from ai_job_gateway import manager as manager_module
from ai_job_gateway.manager import JobManager
from ai_job_gateway.providers import EchoProvider
from ai_job_gateway.store import InMemoryJobStore
from ai_job_gateway.webhook_signing import (
    WEBHOOK_SIGNATURE_V2_HEADER,
    WEBHOOK_TIMESTAMP_HEADER,
    signature_headers,
    verify_webhook,
)

T0 = 1_758_800_000
BODY = b'{"id": "abc", "status": "ready"}'


def test_a_fresh_valid_signature_verifies():
    headers = signature_headers("s3cret", BODY, T0)
    assert verify_webhook("s3cret", BODY, headers, now=T0 + 10)


def test_header_lookup_is_case_insensitive():
    headers = {k.lower(): v for k, v in signature_headers("s3cret", BODY, T0).items()}
    assert verify_webhook("s3cret", BODY, headers, now=T0)


@pytest.mark.parametrize(
    "secret, body, now",
    [
        ("wrong", BODY, T0),  # forged
        ("s3cret", BODY + b" ", T0),  # tampered body
        ("s3cret", BODY, T0 + 301),  # replayed after the window
        ("s3cret", BODY, T0 - 301),  # from the future
    ],
)
def test_forged_tampered_or_stale_deliveries_fail(secret, body, now):
    headers = signature_headers("s3cret", BODY, T0)
    assert not verify_webhook(secret, body, headers, now=now)


def test_the_timestamp_is_bound_into_the_mac():
    """Swapping in a fresh timestamp header on a captured delivery is the
    replay attempt the V2 signature exists to stop."""
    headers = signature_headers("s3cret", BODY, T0)
    headers[WEBHOOK_TIMESTAMP_HEADER] = str(T0 + 3600)
    assert not verify_webhook("s3cret", BODY, headers, now=T0 + 3600)


def test_missing_or_garbage_headers_fail():
    assert not verify_webhook("s3cret", BODY, {}, now=T0)
    assert not verify_webhook(
        "s3cret", BODY, {WEBHOOK_TIMESTAMP_HEADER: "soon", WEBHOOK_SIGNATURE_V2_HEADER: "x"}, now=T0
    )


async def test_every_delivery_attempt_carries_a_fresh_timestamp(monkeypatch):
    """A retry a minute later must still verify, so each attempt is signed
    at send time rather than once up front."""
    monkeypatch.setattr(manager_module, "WEBHOOK_BACKOFF_CAP_SECONDS", 0.01)
    moments = iter(datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=m) for m in range(100))
    monkeypatch.setattr(clock, "now", lambda: next(moments))
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((dict(request.headers), request.content))
        return httpx.Response(500 if len(seen) < 3 else 200)

    store = InMemoryJobStore()
    manager = JobManager(
        store,
        {"echo": EchoProvider()},
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        webhook_signing_secret="s3cret",
    )
    record = await manager.submit("echo", {"x": 1}, webhook_url="https://example.test/hook")
    for _ in range(300):
        if (await store.get(record.id)).webhook_status == "delivered":
            break
        await asyncio.sleep(0.01)
    assert len(seen) == 3
    stamps = [int(h[WEBHOOK_TIMESTAMP_HEADER.lower()]) for h, _ in seen]
    assert stamps == sorted(stamps) and len(set(stamps)) == 3
    for headers, body in seen:
        ts = int(headers[WEBHOOK_TIMESTAMP_HEADER.lower()])
        assert verify_webhook("s3cret", body, headers, now=ts)
