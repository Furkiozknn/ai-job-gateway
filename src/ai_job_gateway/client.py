"""A small ergonomic client wrapping the submit -> poll loop.

::

    async with JobGatewayClient("http://localhost:8000") as client:
        handle = await client.submit("mock-generate", {"prompt": "a cat"})
        result = await handle.wait(timeout=30)

For the webhook path: pass ``webhook_url=...`` to ``submit()`` and run your
own HTTP endpoint to receive the finished job record -- this client doesn't
implement a receiver (that's inherently the caller's own server), only the
submission side of wiring one up.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

import httpx

from .exceptions import (
    JobExpiredError,
    JobFailedError,
    JobNotFoundError,
    JobSubmissionError,
)


class JobHandle:
    """A handle to one submitted job. Returned by ``JobGatewayClient.submit()``."""

    def __init__(self, client: "JobGatewayClient", job_id: str, polling_url: str) -> None:
        self._client = client
        self.job_id = job_id
        self.polling_url = polling_url

    async def poll(self) -> dict[str, Any]:
        """One GET against the polling URL. Returns the raw job record dict."""
        response = await self._client._http.get(self._client._base_url + self.polling_url)
        if response.status_code == 404:
            raise JobNotFoundError(self.job_id)
        if response.status_code == 410:
            detail = response.json().get("detail", "result expired")
            raise JobExpiredError(detail)
        response.raise_for_status()
        return response.json()

    async def wait(self, *, timeout: float = 60.0, poll_interval: float = 0.5) -> dict[str, Any]:
        """Poll until the job is ready, then return its result.

        Raises ``JobFailedError`` if the job ends in ``error`` status,
        ``JobExpiredError`` if its result expires before this call sees
        ``ready`` (e.g. a very long ``timeout`` outliving a short TTL), and
        ``TimeoutError`` if ``timeout`` elapses first.
        """
        deadline = time.monotonic() + timeout
        while True:
            record = await self.poll()
            status = record["status"]
            if status == "ready":
                return record["result"]
            if status == "error":
                raise JobFailedError(record.get("error") or "job failed with no error message")
            if status == "expired":
                raise JobExpiredError(record.get("error") or "result expired")
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"job {self.job_id} did not finish within {timeout}s "
                    f"(last observed status: {status!r})"
                )
            await asyncio.sleep(poll_interval)


class JobGatewayClient:
    """Talks to a running ai-job-gateway server."""

    def __init__(self, base_url: str, *, http_client: Optional[httpx.AsyncClient] = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = http_client or httpx.AsyncClient()
        self._owns_http = http_client is None

    async def submit(
        self,
        capability: str,
        params: dict[str, Any],
        *,
        webhook_url: Optional[str] = None,
    ) -> JobHandle:
        body = dict(params)
        if webhook_url:
            body["webhook_url"] = webhook_url
        response = await self._http.post(f"{self._base_url}/v1/{capability}", json=body)
        if response.status_code >= 400:
            raise JobSubmissionError(response.status_code, response.text)
        data = response.json()
        return JobHandle(self, data["id"], data["polling_url"])

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> "JobGatewayClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()
