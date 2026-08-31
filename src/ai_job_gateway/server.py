"""The HTTP API: submit/poll/webhook, generically for any registered capability.

::

    POST /v1/{capability}   -> 202 {"id": ..., "polling_url": "/v1/jobs/{id}"}
    GET  /v1/jobs/{id}      -> the job record (200 while pending/processing/ready/error, 410 once expired, 404 if unknown)
    GET  /v1/capabilities   -> {capability: provider_name, ...}

This is the same shape independently converged on by fal.ai/BFL/RunPod's
worker-comfyui -- submit returns immediately, the caller polls (or gets a
webhook), and a `capability` is opaque to the transport: whatever
`JobManager`'s registry maps it to is what runs.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Request

from .exceptions import UnknownCapabilityError
from .manager import JobManager
from .models import JobStatus


def create_app(manager: JobManager) -> FastAPI:
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
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            raise HTTPException(422, "request body must be valid JSON")
        if not isinstance(body, dict) or not body:
            raise HTTPException(422, "request body must be a non-empty JSON object")

        params: dict[str, Any] = dict(body)
        webhook_url = params.pop("webhook_url", None)
        if webhook_url is not None and not isinstance(webhook_url, str):
            raise HTTPException(422, "webhook_url must be a string if provided")

        try:
            record = await manager.submit(capability, params, webhook_url=webhook_url)
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

    return app
