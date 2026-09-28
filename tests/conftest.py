from __future__ import annotations

import ipaddress
import os
import socket

import httpcore
import pytest

from ai_job_gateway.manager import JobManager
from ai_job_gateway.providers import EchoProvider, MockProvider
from ai_job_gateway.store import InMemoryJobStore


def _is_loopback(host) -> bool:
    if isinstance(host, bytes):
        host = host.decode()
    if host in ("localhost", None):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


@pytest.fixture(autouse=True, scope="session")
def _no_real_network():
    """The suite never leaves the machine.

    A test that submits a job with a ``webhook_url`` and a manager without an
    injected ``http_client`` used to make a real outbound POST from the
    background task -- a DNS lookup and a TCP connect (through HTTPS_PROXY,
    where one is set), cut off when the test's event loop closed, which is
    what surfaced as "coroutine 'connect_tcp.<locals>.try_connect' was never
    awaited". Any DNS lookup or TCP connect to a non-loopback host now fails
    immediately; loopback stays allowed for tests that run their own local
    receiver.

    Session-scoped on purpose: a leaked background task typically runs
    *after* its test has finished, so a per-test guard would already have
    been removed. Every attempt is recorded with the test that was current
    when it happened and fails the session at the end.
    """
    attempts: list[str] = []
    real_getaddrinfo = socket.getaddrinfo
    real_connect_tcp = httpcore.AnyIOBackend.connect_tcp

    def record(what: str) -> None:
        attempts.append(f"{what} (during {os.environ.get('PYTEST_CURRENT_TEST', '?')})")

    def guarded_getaddrinfo(host, *args, **kwargs):
        if not _is_loopback(host):
            record(f"DNS lookup of {host!r}")
            raise socket.gaierror(socket.EAI_NONAME, "real DNS is disabled in tests")
        return real_getaddrinfo(host, *args, **kwargs)

    async def guarded_connect_tcp(self, host, port, *args, **kwargs):
        if not _is_loopback(host):
            record(f"TCP connect to {host}:{port}")
            raise httpcore.ConnectError("real network is disabled in tests")
        return await real_connect_tcp(self, host, port, *args, **kwargs)

    # A proxy on loopback (common in CI sandboxes and dev machines) would
    # otherwise launder every leaked request into an allowed loopback connect.
    saved_env = {k: os.environ.pop(k) for k in list(os.environ) if k.lower().endswith("_proxy")}
    socket.getaddrinfo = guarded_getaddrinfo
    httpcore.AnyIOBackend.connect_tcp = guarded_connect_tcp
    try:
        yield attempts
    finally:
        socket.getaddrinfo = real_getaddrinfo
        httpcore.AnyIOBackend.connect_tcp = real_connect_tcp
        os.environ.update(saved_env)
    assert not attempts, "tests tried to use the real network:\n" + "\n".join(attempts)


@pytest.fixture
def store():
    return InMemoryJobStore()


@pytest.fixture
def registry():
    return {
        "mock-generate": MockProvider(delay_seconds=0.01),
        "echo": EchoProvider(),
    }


@pytest.fixture
def manager(store, registry):
    return JobManager(store, registry)
