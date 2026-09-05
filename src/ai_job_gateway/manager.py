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
import random
import uuid
from datetime import timedelta
from typing import Optional

import httpx

from . import clock
from .exceptions import UnknownCapabilityError
from .models import TERMINAL_STATUSES, JobRecord, JobStatus
from .providers import Provider
from .store import JobStore

logger = logging.getLogger(__name__)

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
#: Webhook delivery: total attempts, and the capped-exponential full-jitter
#: backoff between them. The delay before retry n is uniform in
#: [0, min(cap, base * 2**(n-1))] -- full jitter, per the standard AWS
#: analysis: when a receiver hiccups, many jobs finish and retry at once,
#: and fixed delays re-synchronize that herd onto the recovering receiver
#: at exactly the same instants.
WEBHOOK_MAX_ATTEMPTS = 5
WEBHOOK_BACKOFF_BASE_SECONDS = 1.0
WEBHOOK_BACKOFF_CAP_SECONDS = 30.0


def _webhook_backoff_delays(rng=random.uniform) -> list[float]:
    """The randomized delays before each retry (the first attempt is
    immediate and not represented here). Module-level and rng-injectable so
    the shape is testable without sleeping."""
    return [
        rng(0.0, min(WEBHOOK_BACKOFF_CAP_SECONDS, WEBHOOK_BACKOFF_BASE_SECONDS * (2**n)))
        for n in range(WEBHOOK_MAX_ATTEMPTS - 1)
    ]


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

    async def _webhook_client(self) -> httpx.AsyncClient:
        """The HTTP client webhooks go out through, created once.

        Building a client per delivery - the previous behaviour when none was
        injected - paid the connection setup (TCP, and TLS for an https
        receiver) on every webhook and threw the pool away afterwards. One
        client per manager keeps connections alive across deliveries; the
        retry loop's three attempts to the same receiver reuse one socket.
        """
        if self._http_client is None:
            self._http_client = httpx.AsyncClient()
        return self._http_client

    async def aclose(self) -> None:
        """Close the webhook client this manager created. A client that was
        injected belongs to whoever injected it and is left alone. The app
        factory registers this as a shutdown handler."""
        if self._owns_http_client and self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

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
            existing_id = await self.store.recall_idempotency_key(idempotency_key)
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
        if idempotency_key is not None:
            # Persisted in the store (not a manager-local dict, as it used
            # to be): dedupe that evaporates on restart fails exactly when
            # it's needed most -- a deploy mid-request is the classic way a
            # response gets lost and the client's retry double-runs the
            # provider. Demonstrated live in the ecosystem audit.
            #
            # Remembered BEFORE the record is created, deliberately: a crash
            # between the two calls then leaves a key pointing at a job that
            # doesn't exist, which the recall path above already treats as a
            # fresh submission (and re-remembers) -- a self-healing no-op.
            # The other order left the opposite window: record created, key
            # not yet remembered, so the client's retry double-ran the
            # provider -- the exact failure this feature exists to prevent.
            await self.store.remember_idempotency_key(idempotency_key, record.id)
        await self.store.create(record)

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

        client = await self._webhook_client()
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
        delays = (0.0, *_webhook_backoff_delays())
        for attempt, delay in enumerate(delays, start=1):
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
                await self._record_webhook_outcome(record.id, "delivered")
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
            len(delays),
            record.id,
            last_exc,
        )
        # The dead letter: without this, an exhausted delivery was a log
        # line and nothing else -- no way to see through the API which jobs
        # never reached their receiver.
        await self._record_webhook_outcome(record.id, "failed")

    async def _record_webhook_outcome(self, job_id: str, outcome: str) -> None:
        try:
            await self.store.set_webhook_status(job_id, outcome)
        except Exception:  # noqa: BLE001 - bookkeeping about delivery must
            # never take down the delivery task itself.
            logger.exception("job %s: failed to record webhook outcome %r", job_id, outcome)

    async def recover_interrupted_jobs(self) -> int:
        """Fail interrupted jobs honestly at startup; returns how many.

        Background tasks die with the process, so a job found ``pending`` or
        ``processing`` when the server starts has, provably, nothing driving
        it anymore -- left alone it would read "processing" forever, which
        the audit demonstrated against a restarted SQLite-backed gateway.
        Marking it ``error`` with an explicit resubmit message is the only
        truthful recovery short of re-running the provider, which this
        deliberately does not do: whether a half-finished generation's side
        effects (billing, uploads) are safe to repeat is the caller's call
        to make, and they signal it by resubmitting with a fresh
        idempotency key (the original key keeps returning the errored
        record, which is how the caller learns what happened). The webhook,
        if any, fires for the error like any other terminal transition, so
        receivers hear about it too.

        Also re-fires webhooks for jobs that finished but whose delivery
        outcome was never recorded (terminal status, webhook_url set,
        webhook_status still None) - the previous process died mid-delivery
        and the receiver may never have heard. At-least-once, deliberately;
        these do not count toward the returned number.

        Single-process assumption, stated plainly: this sweep treats every
        pending/processing row as ownerless. Run two gateway processes over
        one SQLite file and the second one's startup will fail the first
        one's live jobs. Multi-process deployments need a store with real
        ownership (see README limitations).
        """
        recovered = 0
        for record in await self.store.list():
            if (
                record.status in TERMINAL_STATUSES
                and record.webhook_url
                and record.webhook_status is None
            ):
                # A terminal job with no recorded delivery outcome means the
                # previous process died between finishing the job and
                # finishing (or recording) its webhook -- the receiver may
                # never have heard. Re-fire it: at-least-once, on purpose.
                # If the crash landed after a 2xx but before the outcome was
                # recorded, the receiver sees a duplicate, which is the
                # standard webhook contract (receivers dedupe on job id).
                task = asyncio.create_task(self._deliver_webhook(record))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
                continue
            if record.status not in (JobStatus.PENDING, JobStatus.PROCESSING):
                continue
            updated = await self.store.update_status(
                record.id,
                status=JobStatus.ERROR,
                error=(
                    "interrupted by a gateway restart before completing; "
                    "submit the job again to re-run it"
                ),
            )
            recovered += 1
            if updated.webhook_url:
                task = asyncio.create_task(self._deliver_webhook(updated))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
        return recovered
