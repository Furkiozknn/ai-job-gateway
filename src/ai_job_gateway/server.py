"""The HTTP API: submit/poll/webhook, generically for any registered capability.

::

    POST /v1/{capability}   -> 202 {"id": ..., "polling_url": "/v1/jobs/{id}"}
    GET  /v1/jobs/{id}      -> the job record (200 while pending/processing/ready/error, 410 once expired, 404 if unknown)
    GET  /v1/capabilities   -> {capability: provider_name, ...}
    GET  /health            -> {"status": "ok"} -- liveness probe, no dependency on the store

This is the same shape independently converged on by fal.ai/BFL/RunPod's
worker-comfyui -- submit returns immediately, the caller polls (or gets a
webhook), and a `capability` is opaque to the transport: whatever
`JobManager`'s registry maps it to is what runs.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import asyncio
import hmac
import ipaddress
import json
import logging
import re
import socket
from typing import Optional, Any
from urllib.parse import urlsplit

from fastapi import Query, FastAPI, HTTPException, Request

from .exceptions import UnknownCapabilityError
from .manager import JobManager
from .models import JobStatus

#: Default cap on a submission's raw request body. FastAPI/Starlette place no
#: limit on body size by default -- `await request.json()` happily buffers an
#: arbitrarily large body into memory first. A generative-model `params` dict
#: (prompts, small config values) has no legitimate reason to be large; this
#: exists to stop one oversized POST from being a cheap memory-exhaustion
#: vector, not to accommodate real payloads. Override via `create_app(...,
#: max_body_bytes=...)` if a given deployment's params are genuinely bigger.
DEFAULT_MAX_BODY_BYTES = 1_000_000  # 1 MB

#: Capability names are a path segment, not free text -- constraining the
#: charset keeps error messages, logs, and the capabilities listing sane, and
#: rejects the class of "surprising path segment" inputs (leading dots,
#: whitespace, unicode lookalikes) before they ever reach the registry
#: lookup. This does not change behavior for any legitimately-named
#: capability: every capability shipped or documented by this repo already
#: matches it.
_CAPABILITY_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,100}$")

#: Schemes a webhook_url is allowed to use. Submitting a job asks this server
#: to make an outbound HTTP request to a caller-supplied URL later, on its
#: own schedule -- the textbook SSRF shape. Restricting the scheme to
#: http(s) is the cheap, safe-by-construction part of that hardening: it
#: costs no legitimate caller anything (a webhook receiver is always
#: http(s)) while rejecting schemes like `file://` or `gopher://` that exist
#: only to make an HTTP client do something other than an HTTP request.
#: Blocking specific *hosts* is the other half. A submitter who can name any
#: URL can point this server's outbound POST -- carrying the full job record
#: -- at 169.254.169.254 (cloud metadata), 127.0.0.1:<internal port>, or an
#: RFC-1918 neighbour. So by default the hostname is resolved at submission
#: time and the URL is rejected if any resolved address is loopback,
#: private, link-local, reserved, multicast or unspecified. The documented
#: local-dev workflow (webhook-sink on 127.0.0.1) opts back in with
#: `create_app(..., allow_private_webhooks=True)` / `serve
#: --allow-private-webhooks`. Known residue, on purpose: resolution happens
#: once at submission, so a DNS name that flips to a private address between
#: validation and delivery (rebinding) is not caught -- closing that needs
#: resolution pinning inside the HTTP client, which is beyond what a
#: reference implementation should hand-roll. The check fails closed: a
#: hostname that does not resolve is a 422, because "could not check" must
#: not become "allowed".
_ALLOWED_WEBHOOK_SCHEMES = {"http", "https"}


def _resolve_host(host: str) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    """Every address ``host`` resolves to. Module-level so tests stub DNS out."""
    try:
        return [ipaddress.ip_address(host)]
    except ValueError:
        pass
    infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    return [ipaddress.ip_address(info[4][0]) for info in infos]


async def _read_capped_body(request: Request, max_bytes: int) -> bytes:
    """Read the request body, raising 413 if it exceeds ``max_bytes``.

    Checks ``Content-Length`` first as a fast rejection when present, but
    does not rely on it alone -- a chunked request has no Content-Length, so
    the actual bytes are also counted while streaming in.
    """
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > max_bytes:
                raise HTTPException(413, f"request body exceeds {max_bytes} byte limit")
        except ValueError:
            pass  # malformed header; fall through to the streaming check below

    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(413, f"request body exceeds {max_bytes} byte limit")
        chunks.append(chunk)
    return b"".join(chunks)


async def _validate_webhook_url(webhook_url: Any, *, allow_private: bool) -> None:
    if webhook_url is None:
        return
    if not isinstance(webhook_url, str):
        raise HTTPException(422, "webhook_url must be a string if provided")
    parts = urlsplit(webhook_url)
    if parts.scheme.lower() not in _ALLOWED_WEBHOOK_SCHEMES or not parts.netloc:
        raise HTTPException(
            422,
            "webhook_url must be an absolute http:// or https:// URL",
        )
    if allow_private:
        return
    host = parts.hostname or ""
    try:
        addresses = await asyncio.get_running_loop().run_in_executor(
            None, _resolve_host, host
        )
    except (socket.gaierror, UnicodeError, OSError):
        raise HTTPException(
            422,
            f"webhook_url host {host!r} did not resolve; a webhook target must be reachable",
        ) from None
    for address in addresses:
        if not address.is_global:
            raise HTTPException(
                422,
                "webhook_url resolves to a private, loopback or otherwise "
                "non-public address, which this server will not call. Run with "
                "--allow-private-webhooks to permit this in local development.",
            )


def create_app(
    manager: JobManager,
    *,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    allow_private_webhooks: bool = False,
    api_key: Optional[str] = None,
) -> FastAPI:
    """Build a FastAPI app wired to the given JobManager.

    A factory rather than a module-level singleton so tests (and anything
    embedding this server) can spin up multiple independent apps, each with
    its own store/registry, in the same process.

    ``api_key`` set makes every ``/v1/*`` route require ``Authorization:
    Bearer <key>`` (compared constant-time); ``/health`` stays open, it is a
    liveness probe. Unset keeps the historical open behavior, which is
    acceptable only on the default 127.0.0.1 bind.
    """

    def _require_api_key(request: Request) -> None:
        if api_key is None:
            return
        supplied = request.headers.get("authorization", "")
        expected = f"Bearer {api_key}"
        if not hmac.compare_digest(supplied.encode(), expected.encode()):
            raise HTTPException(401, "missing or invalid API key")
    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        # Startup recovery sweep: a job still pending/processing in a
        # persistent store has no living task driving it (tasks died with
        # the previous process) and would read "processing" forever. A
        # fresh in-memory store makes this a no-op.
        recovered = await manager.recover_interrupted_jobs()
        if recovered:
            logging.getLogger(__name__).warning(
                "marked %d job(s) left pending/processing by a previous run as error",
                recovered,
            )
        try:
            yield
        finally:
            # Close the webhook client the manager created (an injected one
            # is left to its owner). Runs on server shutdown.
            await manager.aclose()

    app = FastAPI(
        lifespan=_lifespan,
        title="ai-job-gateway",
        description=(
            "Provider-agnostic reference implementation of the submit/poll/webhook "
            "async job contract for generative AI models."
        ),
    )
    app.state.manager = manager

    @app.post("/v1/{capability}", status_code=202)
    async def submit(capability: str, request: Request) -> dict[str, str]:
        _require_api_key(request)
        if not _CAPABILITY_NAME_RE.match(capability):
            raise HTTPException(
                422,
                "capability must be 1-100 characters of letters, digits, '_' or '-'",
            )

        raw_body = await _read_capped_body(request, max_body_bytes)
        try:
            body = json.loads(raw_body) if raw_body else None
        except json.JSONDecodeError:
            raise HTTPException(422, "request body must be valid JSON")
        if not isinstance(body, dict) or not body:
            raise HTTPException(422, "request body must be a non-empty JSON object")

        params: dict[str, Any] = dict(body)
        webhook_url = params.pop("webhook_url", None)
        await _validate_webhook_url(webhook_url, allow_private=allow_private_webhooks)

        idempotency_key = request.headers.get("idempotency-key") or None

        try:
            record = await manager.submit(
                capability, params, webhook_url=webhook_url, idempotency_key=idempotency_key
            )
        except UnknownCapabilityError as exc:
            raise HTTPException(404, str(exc))

        return {"id": record.id, "polling_url": f"/v1/jobs/{record.id}"}

    @app.get("/v1/jobs/{job_id}")
    async def get_job(job_id: str, request: Request) -> dict[str, Any]:
        _require_api_key(request)
        record = await manager.store.get(job_id)
        if record is None:
            raise HTTPException(404, f"no job with id {job_id!r}")
        if record.status == JobStatus.EXPIRED:
            raise HTTPException(410, record.error or "this job's result has expired")
        return record.model_dump(mode="json")

    @app.get("/v1/jobs")
    async def list_jobs(
        request: Request,
        status: Optional[str] = None,
        capability: Optional[str] = None,
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict[str, Any]:
        """Recent jobs, newest first, optionally filtered by status and/or
        capability. Expired jobs are listed with status "expired" (their
        result is gone; the record is not).

        Filtering, sorting and the limit live in ``JobStore.list_page`` --
        pushed into SQL by the SQLite store -- so one dashboard polling this
        endpoint no longer scans (or holds the store lock across) every row
        ever created.
        """
        _require_api_key(request)
        wanted_status: Optional[JobStatus] = None
        if status is not None:
            try:
                wanted_status = JobStatus(status)
            except ValueError:
                raise HTTPException(
                    422, f"status must be one of {[s.value for s in JobStatus]}, got {status!r}"
                )
        if capability is not None and not _CAPABILITY_NAME_RE.match(capability):
            raise HTTPException(422, "capability filter contains characters no capability can have")

        page, total_matching = await manager.store.list_page(
            status=wanted_status, capability=capability, limit=limit
        )
        return {
            "jobs": [r.model_dump(mode="json") for r in page],
            "count": len(page),
            "total_matching": total_matching,
        }

    @app.get("/v1/stats")
    async def stats(request: Request) -> dict[str, Any]:
        _require_api_key(request)
        """Counts by status and by capability - what an operator glances at
        to see whether the queue is draining or a provider is failing.
        Computed by ``JobStore.stats`` (one aggregate query in the SQLite
        store) rather than by materializing every record here."""
        counts = await manager.store.stats()
        by_status = {s.value: 0 for s in JobStatus}
        by_status.update(counts["by_status"])
        return {
            "total": counts["total"],
            "by_status": by_status,
            "by_capability": dict(sorted(counts["by_capability"].items())),
            "registered_capabilities": len(manager.registry),
        }

    @app.get("/v1/capabilities")
    async def list_capabilities(request: Request) -> dict[str, str]:
        _require_api_key(request)
        return {capability: provider.name for capability, provider in manager.registry.items()}

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Liveness probe. Deliberately checks nothing but that the process
        is up and serving requests -- it does not touch the store, so it
        stays meaningful even for a JobStore backend that's degraded."""
        return {"status": "ok"}

    return app
