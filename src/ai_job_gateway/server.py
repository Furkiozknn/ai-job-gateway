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

import json
import re
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request

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
#: Blocking specific *hosts* (loopback, link-local/cloud-metadata, private
#: ranges) is deliberately NOT done here -- see the security notes in the
#: README: this reference server's own documented local-dev workflow (and
#: its sibling webhook-sink project) is a webhook_url pointing at
#: 127.0.0.1/localhost, so host-level SSRF blocking belongs at the network
#: layer of a real deployment, not hardcoded into a reference gateway that
#: would then be unable to demo its own webhook feature.
_ALLOWED_WEBHOOK_SCHEMES = {"http", "https"}


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


def _validate_webhook_url(webhook_url: Any) -> None:
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


def create_app(manager: JobManager, *, max_body_bytes: int = DEFAULT_MAX_BODY_BYTES) -> FastAPI:
    """Build a FastAPI app wired to the given JobManager.

    A factory rather than a module-level singleton so tests (and anything
    embedding this server) can spin up multiple independent apps, each with
    its own store/registry, in the same process.
    """
    app = FastAPI(
        title="ai-job-gateway",
        description=(
            "Provider-agnostic reference implementation of the submit/poll/webhook "
            "async job contract for generative AI models."
        ),
    )
    app.state.manager = manager

    @app.post("/v1/{capability}", status_code=202)
    async def submit(capability: str, request: Request) -> dict[str, str]:
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
        _validate_webhook_url(webhook_url)

        idempotency_key = request.headers.get("idempotency-key") or None

        try:
            record = await manager.submit(
                capability, params, webhook_url=webhook_url, idempotency_key=idempotency_key
            )
        except UnknownCapabilityError as exc:
            raise HTTPException(404, str(exc))

        return {"id": record.id, "polling_url": f"/v1/jobs/{record.id}"}

    @app.get("/v1/jobs/{job_id}")
    async def get_job(job_id: str) -> dict[str, Any]:
        record = await manager.store.get(job_id)
        if record is None:
            raise HTTPException(404, f"no job with id {job_id!r}")
        if record.status == JobStatus.EXPIRED:
            raise HTTPException(410, record.error or "this job's result has expired")
        return record.model_dump(mode="json")

    @app.get("/v1/capabilities")
    async def list_capabilities() -> dict[str, str]:
        return {capability: provider.name for capability, provider in manager.registry.items()}

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Liveness probe. Deliberately checks nothing but that the process
        is up and serving requests -- it does not touch the store, so it
        stays meaningful even for a JobStore backend that's degraded."""
        return {"status": "ok"}

    return app
