"""JobManager: the orchestration seam between a JobStore and a capability ->
Provider registry.

Accepting a submission and running it are deliberately decoupled: ``submit()``
creates the job record and returns immediately (mirroring the BFL/RunPod-style
``POST -> {id, polling_url}`` contract), while the actual provider call runs as
a background ``asyncio`` task. This is an in-process task queue -- fine for a
single reference server, and explicitly not what a production deployment
would use at real traffic: swap this for a real queue (Redis/RQ, Celery, or
handing jobs to serverless GPU workers a la RunPod) once a single process's
concurrency isn't enough. The public shape (``submit`` returns a job id
immediately, the job transitions through the same statuses) doesn't have to
change when that swap happens.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import timedelta
from typing import Optional

import httpx

from . import clock
from .exceptions import UnknownCapabilityError
from .models import JobRecord, JobStatus
from .providers import Provider
from .store import JobStore

logger = logging.getLogger(__name__)

#: How long a ready/error result stays fetchable before GET starts returning
#: `expired`. Mirrors the BFL API's 10-minute window (this default is more
#: generous since there's no storage-cost pressure in a reference server);
#: override per-JobManager for a real deployment's own cost/UX tradeoff.
DEFAULT_RESULT_TTL = timedelta(minutes=30)

WEBHOOK_TIMEOUT_SECONDS = 10.0
#: Delivery attempts beyond the first, with the delay (seconds) before each.
WEBHOOK_RETRY_DELAYS = (1.0, 2.0, 4.0)


class JobManager:
    """Ties a store and a provider registry together and runs submitted jobs.

    ``registry`` maps a capability name (the ``{capability}`` in
    ``POST /v1/{capability}``) to the ``Provider`` that serves it. One
    provider instance can be registered under multiple capability names if
    that's ever useful; nothing here assumes a 1:1 mapping.
    """

    def __init__(
        self,
        store: JobStore,
        registry: dict[str, Provider],
        *,
        result_ttl: timedelta = DEFAULT_RESULT_TTL,
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.store = store
        self.registry = registry
        self.result_ttl = result_ttl
        self._http_client = http_client
        self._owns_http_client = http_client is None
        # Keeps a strong reference to in-flight background tasks -- asyncio
        # only weakly holds tasks created with create_task, so without this a
        # task can be garbage-collected mid-run.
        self._background_tasks: set[asyncio.Task] = set()

    async def submit(
        self,
        capability: str,
        params: dict,
        *,
        webhook_url: Optional[str] = None,
    ) -> JobRecord:
        """Create a job and schedule it to run in the background.

        Raises ``UnknownCapabilityError`` if ``capability`` isn't registered.
        Returns the freshly-created record (status ``pending``) immediately --
        the caller does not wait for the provider to finish.
        """
        provider = self.registry.get(capability)
        if provider is None:
            raise UnknownCapabilityError(capability)

        now = clock.now()
        record = JobRecord(
            id=uuid.uuid4().hex,
            capability=capability,
            provider=provider.name,
            params=params,
            status=JobStatus.PENDING,
            created_at=now,
            updated_at=now,
            webhook_url=webhook_url,
        )
        await self.store.create(record)

        task = asyncio.create_task(self._run(record.id, provider, params))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return record

    async def _run(self, job_id: str, provider: Provider, params: dict) -> None:
        await self.store.update_status(job_id, status=JobStatus.PROCESSING)
        try:
            result = await provider.run(job_id, params)
        except Exception as exc:  # noqa: BLE001 - deliberately broad: any
            # provider failure becomes a reported error, never an unhandled
            # exception that would silently kill the background task.
            record = await self.store.update_status(
                job_id, status=JobStatus.ERROR, error=str(exc)
            )
        else:
            expires_at = clock.now() + self.result_ttl
            record = await self.store.update_status(
                job_id,
                status=JobStatus.READY,
                result=result,
                result_expires_at=expires_at,
            )
        await self._deliver_webhook(record)

    async def _deliver_webhook(self, record: JobRecord) -> None:
        """POST the finished record to record.webhook_url, if set.

        The job's own status is already correctly ready/error in the store
        before this is even called -- webhook delivery failing (including
        after all retries) never changes that. This only ever logs.
        """
        if not record.webhook_url:
            return

        client = self._http_client or httpx.AsyncClient()
        payload = record.model_dump(mode="json")
        last_exc: Optional[Exception] = None
        try:
            for attempt, delay in enumerate((0.0, *WEBHOOK_RETRY_DELAYS), start=1):
                if delay:
                    await asyncio.sleep(delay)
                try:
                    response = await client.post(
                        record.webhook_url,
                        json=payload,
                        timeout=WEBHOOK_TIMEOUT_SECONDS,
                    )
                    response.raise_for_status()
                    return
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    logger.debug(
                        "webhook delivery attempt %d to %s failed: %s",
                        attempt,
                        record.webhook_url,
                        exc,
                    )
            logger.warning(
                "webhook delivery to %s gave up after %d attempts for job %s: %s",
                record.webhook_url,
                len(WEBHOOK_RETRY_DELAYS) + 1,
                record.id,
                last_exc,
            )
        finally:
            if self._owns_http_client:
                await client.aclose()
