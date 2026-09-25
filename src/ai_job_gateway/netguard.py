"""Outbound-address policy for webhook delivery (the SSRF guard).

Two checks share one definition of "public":

* at submission, ``server`` resolves the ``webhook_url`` host and refuses the
  job with a 422 if any answer is non-public -- fast, legible feedback;
* at delivery, the webhook client's transport resolves the host *again*,
  refuses to connect if any answer is non-public, and then connects to the
  exact address it just checked. That second check is what closes DNS
  rebinding (a name that answers public at submission and 127.0.0.1 half an
  hour later) and covers jobs re-fired by the restart sweep, whose URLs were
  validated by a previous process under possibly different settings.

The TLS handshake still verifies the certificate against the URL's hostname:
only the TCP connect is pinned to the checked address, so pinning does not
weaken HTTPS. Redirects are never followed (httpx's default, relied on here):
a 3xx is a failed attempt, so a receiver cannot bounce the POST inward.
"""

from __future__ import annotations

import ipaddress
import socket
import ssl
import typing
from typing import Optional

import anyio
import httpcore
import httpx

IPAddress = typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

#: NAT64 prefixes (RFC 6052 well-known, RFC 8215 local-use). Python reports
#: 64:ff9b::7f00:1 as global, but on a NAT64 network it *is* 127.0.0.1.
_NAT64_NETWORKS = (
    ipaddress.ip_network("64:ff9b::/96"),
    ipaddress.ip_network("64:ff9b:1::/48"),
)


def resolve_host(host: str) -> list[IPAddress]:
    """Every address ``host`` resolves to (a literal IP resolves to itself)."""
    try:
        return [ipaddress.ip_address(host)]
    except ValueError:
        pass
    infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    return [ipaddress.ip_address(info[4][0]) for info in infos]


def is_public_address(address: IPAddress) -> bool:
    """True only for an address a webhook may be delivered to.

    ``ipaddress.is_global`` alone is not enough: it calls multicast
    (224.0.0.0/4) global, and it judges an IPv6 address that embeds an IPv4
    one by the IPv6 wrapper rather than by where the packet actually goes.
    """
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped is not None:  # ::ffff:127.0.0.1
            return is_public_address(address.ipv4_mapped)
        for net in _NAT64_NETWORKS:
            if address in net:  # 64:ff9b::7f00:1 -> 127.0.0.1
                return is_public_address(ipaddress.IPv4Address(int(address) & 0xFFFFFFFF))
        if int(address) >> 32 == 0:  # deprecated IPv4-compatible ::a.b.c.d
            return False
    return address.is_global and not address.is_multicast


class WebhookDestinationBlocked(Exception):
    """Raised instead of connecting when a webhook host resolves to a
    non-public address at delivery time. Deliberately not an httpx/httpcore
    error: it is a permanent refusal, not a transient network failure, so
    the delivery loop stops at the first one instead of retrying."""


class _GuardedBackend(httpcore.AsyncNetworkBackend):
    """Resolve, check every answer, then connect to a checked address."""

    def __init__(self, inner: Optional[httpcore.AsyncNetworkBackend] = None) -> None:
        self._inner = inner or httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: Optional[float] = None,
        local_address: Optional[str] = None,
        socket_options: Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> httpcore.AsyncNetworkStream:
        try:
            with anyio.fail_after(timeout):
                addresses = await anyio.to_thread.run_sync(resolve_host, host)
        except TimeoutError as exc:
            raise httpcore.ConnectTimeout(f"resolving {host!r} timed out") from exc
        except (OSError, UnicodeError) as exc:
            raise httpcore.ConnectError(f"could not resolve {host!r}: {exc}") from exc
        if not addresses:
            raise httpcore.ConnectError(f"{host!r} resolved to no addresses")
        blocked = [str(a) for a in addresses if not is_public_address(a)]
        if blocked:
            raise WebhookDestinationBlocked(
                f"webhook host {host!r} resolves to non-public address(es) "
                f"{', '.join(blocked)} at delivery time; refusing to connect"
            )
        last_exc: Optional[Exception] = None
        for address in addresses:
            try:
                # The literal checked above -- no second lookup that could
                # answer differently.
                return await self._inner.connect_tcp(
                    str(address),
                    port,
                    timeout=timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
            except (httpcore.ConnectError, httpcore.ConnectTimeout) as exc:
                last_exc = exc
        assert last_exc is not None
        raise last_exc

    async def connect_unix_socket(self, *args, **kwargs) -> httpcore.AsyncNetworkStream:
        raise WebhookDestinationBlocked("unix-socket webhook destinations are not allowed")

    async def sleep(self, seconds: float) -> None:
        await self._inner.sleep(seconds)


class GuardedWebhookTransport(httpx.AsyncHTTPTransport):
    """An httpx transport whose every TCP connect goes through the policy.

    No proxy support, on purpose: through a proxy the TCP connect goes to the
    proxy, so a connect-time check would inspect the wrong address. The
    client built around this transport sets ``trust_env=False`` so
    ``HTTPS_PROXY`` in the environment cannot silently bypass the guard; a
    deployment that must egress through a proxy injects its own
    ``http_client`` and owns egress policy at that proxy.
    """

    def __init__(self, *, verify: typing.Union[ssl.SSLContext, bool] = True) -> None:
        super().__init__(verify=verify, trust_env=False)
        # httpx does not expose httpcore's network_backend, so the pool is
        # rebuilt with it. The guard's behavior is pinned by tests that
        # drive real sockets, so an httpx change that bypasses this fails CI.
        self._pool = httpcore.AsyncConnectionPool(
            ssl_context=httpx.create_ssl_context(verify=verify, trust_env=False),
            max_connections=100,  # httpx's own defaults
            max_keepalive_connections=20,
            keepalive_expiry=5.0,
            network_backend=_GuardedBackend(),
        )


def guarded_webhook_client() -> httpx.AsyncClient:
    """The client JobManager delivers webhooks through by default."""
    return httpx.AsyncClient(transport=GuardedWebhookTransport(), trust_env=False)
