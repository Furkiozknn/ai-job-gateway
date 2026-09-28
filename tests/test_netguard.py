"""The delivery-time SSRF guard, against real sockets on 127.0.0.1.

The submission-time check (test_server.TestWebhookHostRestriction) is not
enough on its own: DNS can answer differently when the webhook actually
fires, and the restart sweep re-fires URLs a previous process validated.
These tests drive the manager's default webhook client -- no injected
http_client, no MockTransport -- so what they pin is the real connect path.
"""

from __future__ import annotations

import asyncio
import ipaddress

import httpx
import pytest

from ai_job_gateway import manager as manager_module
from ai_job_gateway import netguard
from ai_job_gateway import server as server_mod
from ai_job_gateway.manager import JobManager
from ai_job_gateway.providers import EchoProvider
from ai_job_gateway.server import create_app
from ai_job_gateway.store import InMemoryJobStore


class _Receiver:
    """A minimal HTTP/1.1 server on 127.0.0.1 that records what it gets."""

    def __init__(self, status: int = 200, location: str | None = None) -> None:
        self.status = status
        self.location = location
        self.connections = 0
        self.requests: list[tuple[str, dict[str, str], bytes]] = []

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        head = await reader.readuntil(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        body = await reader.readexactly(int(headers.get("content-length", "0")))
        self.requests.append((lines[0], headers, body))
        extra = f"Location: {self.location}\r\n" if self.location else ""
        writer.write(
            f"HTTP/1.1 {self.status} X\r\nContent-Length: 0\r\n{extra}Connection: close\r\n\r\n".encode()
        )
        await writer.drain()
        writer.close()

    async def __aenter__(self) -> "_Receiver":
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc) -> None:
        self._server.close()
        await self._server.wait_closed()


def _resolve(monkeypatch, mapping: dict[str, list[str]]) -> list[str]:
    """Stub delivery-time DNS; returns the list of names looked up."""
    looked_up: list[str] = []

    def resolve(host: str):
        looked_up.append(host)
        try:
            return [ipaddress.ip_address(host)]
        except ValueError:
            return [ipaddress.ip_address(a) for a in mapping[host]]

    monkeypatch.setattr(netguard, "resolve_host", resolve)
    return looked_up


async def _webhook_status(store, job_id: str, timeout: float = 3.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        record = await store.get(job_id)
        if record.webhook_status is not None:
            return record.webhook_status
        await asyncio.sleep(0.01)
    raise AssertionError("webhook outcome was never recorded")


@pytest.mark.parametrize(
    "address, public",
    [
        ("93.184.216.34", True),
        ("2606:2800:220:1:248:1893:25c8:1946", True),
        ("::ffff:8.8.8.8", True),
        ("64:ff9b::808:808", True),
        ("127.0.0.1", False),
        ("169.254.169.254", False),
        ("10.0.0.1", False),
        ("100.64.0.1", False),  # carrier-grade NAT
        ("0.0.0.0", False),
        ("224.0.0.1", False),  # multicast: ipaddress says is_global
        ("::", False),
        ("::1", False),
        ("fe80::1", False),
        ("fd00::1", False),
        ("::ffff:127.0.0.1", False),
        ("::ffff:169.254.169.254", False),
        ("64:ff9b::7f00:1", False),  # NAT64 of 127.0.0.1: ipaddress says is_global
        ("::127.0.0.1", False),  # IPv4-compatible: ipaddress says is_global
        ("ff02::1", False),
    ],
)
def test_is_public_address(address, public):
    assert netguard.is_public_address(ipaddress.ip_address(address)) is public


async def test_delivery_to_a_private_address_is_refused_without_connecting(monkeypatch):
    """Private at delivery time -> no TCP connection, dead-lettered on the
    first attempt (a policy refusal is not retried)."""
    looked_up = _resolve(monkeypatch, {"hook.example": ["127.0.0.1"]})
    store = InMemoryJobStore()
    manager = JobManager(store, {"echo": EchoProvider()})
    async with _Receiver() as receiver:
        record = await manager.submit(
            "echo", {"x": 1}, webhook_url=f"http://hook.example:{receiver.port}/hook"
        )
        assert await _webhook_status(store, record.id) == "failed"
        assert receiver.connections == 0
    assert looked_up == ["hook.example"]  # one attempt, not five
    await manager.aclose()


async def test_dns_rebinding_between_submission_and_delivery_is_caught(monkeypatch):
    """The name answers public when the job is submitted and 127.0.0.1 when
    the webhook fires: the classic rebinding shape the old submission-only
    check let through."""
    monkeypatch.setattr(
        server_mod, "_resolve_host", lambda host: [ipaddress.ip_address("93.184.216.34")]
    )
    _resolve(monkeypatch, {"rebind.example": ["127.0.0.1"]})
    store = InMemoryJobStore()
    manager = JobManager(store, {"echo": EchoProvider()})
    app = create_app(manager)
    async with _Receiver() as receiver:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/v1/echo",
                json={"x": 1, "webhook_url": f"http://rebind.example:{receiver.port}/hook"},
            )
        assert resp.status_code == 202  # passed the submission-time check
        assert await _webhook_status(store, resp.json()["id"]) == "failed"
        assert receiver.connections == 0
    await manager.aclose()


async def test_connection_is_pinned_to_the_checked_address(monkeypatch):
    """The transport connects to the literal it checked and never asks DNS a
    second time (pinned.example resolves nowhere else: real DNS is disabled
    in tests, so a second lookup would fail). The Host header still names the
    original host, and an HTTPS_PROXY in the environment is not used."""
    monkeypatch.setattr(netguard, "is_public_address", lambda address: True)
    looked_up = _resolve(monkeypatch, {"pinned.example": ["127.0.0.1"]})
    monkeypatch.setenv("HTTP_PROXY", "http://10.255.255.1:3128")
    monkeypatch.setenv("HTTPS_PROXY", "http://10.255.255.1:3128")
    store = InMemoryJobStore()
    manager = JobManager(store, {"echo": EchoProvider()}, webhook_signing_secret="s")
    async with _Receiver() as receiver:
        record = await manager.submit(
            "echo", {"x": 1}, webhook_url=f"http://pinned.example:{receiver.port}/hook"
        )
        assert await _webhook_status(store, record.id) == "delivered"
    request_line, headers, body = receiver.requests[0]
    assert request_line == "POST /hook HTTP/1.1"
    assert headers["host"] == f"pinned.example:{receiver.port}"
    assert b'"x": 1' in body or b'"x":1' in body
    assert looked_up == ["pinned.example"]
    await manager.aclose()


async def test_redirects_are_not_followed(monkeypatch):
    """A public receiver answering 302 -> an internal URL must not bounce the
    POST inward: a 3xx is a failed attempt, nothing more."""
    monkeypatch.setattr(netguard, "is_public_address", lambda address: True)
    monkeypatch.setattr(manager_module, "WEBHOOK_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(manager_module, "WEBHOOK_BACKOFF_CAP_SECONDS", 0.01)
    _resolve(monkeypatch, {"public.example": ["127.0.0.1"]})
    store = InMemoryJobStore()
    manager = JobManager(store, {"echo": EchoProvider()})
    async with _Receiver() as internal:
        async with _Receiver(status=302, location=f"http://127.0.0.1:{internal.port}/admin") as public:
            record = await manager.submit(
                "echo", {"x": 1}, webhook_url=f"http://public.example:{public.port}/hook"
            )
            assert await _webhook_status(store, record.id) == "failed"
            assert len(public.requests) == 2
        assert internal.connections == 0
    await manager.aclose()


async def test_allow_private_webhooks_delivers_to_loopback(monkeypatch):
    """The local-dev opt-in reaches a receiver on 127.0.0.1 for real, and
    create_app's flag is enough to turn it on for the manager too."""
    store = InMemoryJobStore()
    manager = JobManager(store, {"echo": EchoProvider()})
    create_app(manager, allow_private_webhooks=True)
    assert manager.allow_private_webhooks is True
    async with _Receiver() as receiver:
        record = await manager.submit(
            "echo", {"x": 1}, webhook_url=f"http://127.0.0.1:{receiver.port}/hook"
        )
        assert await _webhook_status(store, record.id) == "delivered"
        assert len(receiver.requests) == 1
    await manager.aclose()
