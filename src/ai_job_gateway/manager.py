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
import hashlib
import hmac
import json
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

#: How many recent (idempotency_key -> job_id) pairs to remember. Bounded so
#: a client that mints a fresh key per call forever can't grow this without
#: limit -- the oldest entries are evicted first once the cap is hit. This is
#: a single-process, in-memory best-effort dedupe window (like
#: `InMemoryJobStore`, it does not survive a restart and is not shared across
#: processes); it exists to absorb the common case a caller's own retry
#: logic creates -- a submission whose response was lost to a network blip,
#: retried with the same key -- not to provide exactly-once semantics across
#: a distributed deployment.
MAX_IDEMPOTENCY_KEYS = 10_000

#: HTTP header a webhook delivery carries the payload's HMAC-SHA256 signature
#: in, when JobManager was constructed with a `webhook_signing_secret`. A
#: receiver recomputes the same HMAC over the raw request body and compares
#: it (constant-time) to this header's value to confirm the request really
#: came from this gateway and the body wasn't tampered with in transit --
#: the same shape Stripe/GitHub-style webhook signing uses.
WEBHOOK_SIGNATURE_HEADER = "X-Gateway-Signature"

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
        webhook_signing_secret: Optional[str] = None,
    ) -> None:
        self.store = store
        self.registry = registry
        self.result_ttl = result_ttl
        self._http_client = http_client
        self._owns_http_client = http_client is None
        self._webhook_signing_secret = webhook_signing_secret
        # Keeps a strong reference to in-flight background tasks -- asyncio
        # only weakly holds tasks created with create_task, so without this a
        # task can be garbage-collected mid-run.
        self._background_tasks: set[asyncio.Task] = set()
        # Best-effort submission dedupe -- see MAX_IDEMPOTENCY_KEYS.
        self._idempotency_keys: dict[str, str] = {}

    async def submit(
        self,
        capability: str,
        params: dict,
        *,
        webhook_url: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> JobRecord:
        """Create a job and schedule it to run in the background.

        Raises ``UnknownCapabilityError`` if ``capability`` isn't registered.
        Returns the freshly-created record (status ``pending``) immediately --
        the caller does not wait for the provider to finish.

        If ``idempotency_key`` was used in a previous call and that job's
        record still exists, the *original* record is returned and no new
        job is created or scheduled -- this lets a caller safely retry a
        submission (e.g. after a timed-out or dropped response) without
        double-running the provider. A key is only ever consumed once it's
        led to a real job; a call that raises ``UnknownCapabilityError``
        leaves the key free to try again.
        """
        provider = self.registry.get(capability)
        if provider is None:
            raise UnknownCapabilityError(capability)

        if idempotency_key is not None:
            existing_id = self._idempotency_keys.get(idempotency_key)
            if existing_id is not None:
                existing = await self.store.get(existing_id)
                if existing is not None:
                    return existing
                # The original record is gone (e.g. evicted from an
                # in-memory store some other way) -- fall through and treat
                # this as a fresh submission rather than erroring.

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

        if idempotency_key is not None:
            if len(self._idempotency_keys) >= MAX_IDEMPOTENCY_KEYS:
                self._idempotency_keys.pop(next(iter(self._idempotency_keys)))
            self._idempotency_keys[idempotency_key] = record.id

        task = asyncio.create_task(self._run(record.id, provider, params))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return record

    async def _run(self, job_id: str, provider: Provider, params: dict) -> None:
        try:
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
        except Exception:  # noqa: BLE001 - deliberately broad: a failure in
            # the *store* itself (e.g. the update_status(PROCESSING) call
            # above, or the update_status(ERROR) call in the inner except
            # branch) is not covered by the inner try/except, which only
            # guards provider.run(). Without this outer guard, a store-layer
            # failure would propagate out of this asyncio.create_task with
            # nothing to retrieve it (asyncio just logs "Task exception was
            # never retrieved" to stderr), leaving the job stuck at its last
            # status forever -- no error recorded, no webhook ever attempted,
            # and nothing observable through the API. Catch it, log it, and
            # make a best-effort attempt to still record the job as failed.
            logger.exception(
                "job %s: unrecoverable failure while running/recording status", job_id
            )
            try:
                record = await self.store.update_status(
                    job_id, status=JobStatus.ERROR, error="internal error while running job"
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "job %s: also failed to record error status after the failure above; "
                    "giving up (job will remain stuck at its last recorded status)",
                    job_id,
                )
                return
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
        # Serialized once, up front, so the bytes that get signed are
        # byte-for-byte identical to the bytes that get sent -- posting via
        # httpx's `json=` re-serializes on every attempt, which could in
        # principle drift from whatever was signed.
        body = json.dumps(record.model_dump(mode="json")).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self._webhook_signing_secret:
            signature = hmac.new(
                self._webhook_signing_secret.encode("utf-8"), body, hashlib.sha256
            ).hexdigest()
            headers[WEBHOOK_SIGNATURE_HEADER] = f"sha256={signature}"

        last_exc: Optional[Exception] = None
        try:
            for attempt, delay in enumerate((0.0, *WEBHOOK_RETRY_DELAYS), start=1):
                if delay:
                    await asyncio.sleep(delay)
                try:
                    response = await client.post(
                        record.webhook_url,
                        content=body,
                        headers=headers,
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
