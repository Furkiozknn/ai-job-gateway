"""Shared submit/poll response-interpretation logic for ai-job-gateway
-compatible HTTP clients.

Vendored, not pip-installed -- each consuming repo copies this file verbatim
and writes its own thin sync/async wrapper around it, using whichever HTTP
client (httpx.Client, httpx.AsyncClient, ...) that repo already depends on.
This module does no I/O of its own, imports nothing beyond the standard
library, and raises no repo-specific exception types -- it only turns a
response's (status_code, parsed JSON body, raw text) into either a plain
value or a small, generic error a wrapper can catch and re-raise as its own
domain exception.

Deliberately narrow scope: this covers exactly the logic that was found,
independently, reimplemented incorrectly in more than one repo (the same
~15-20 lines, each time) -- URL building, submission-rejection detection,
the 410-Gone-on-expiry special case, and ready/error/expired body-status
mapping. It does NOT cover repo-specific behavior that differs on purpose
(e.g. ai-job-gateway's client raising a distinct JobNotFoundError on a 404
poll -- keep handling that locally in each wrapper, before calling into
this module).

It also owns the *policy* half of retrying a poll after a transient failure
(which status codes count as transient, and the bounded backoff schedule),
so every wrapper retries the same way. The retry loop itself -- catching
the HTTP client's own network-error type and sleeping -- stays in each
wrapper, because that is I/O.

Canonical copy: ai-job-gateway/src/ai_job_gateway/gateway_poll.py
Copies, byte-identical, checked by each repo's arac/vendor-dogrula.py
(there is no import relationship; re-copy after changing the canonical file):
  - ai-workflow-engine/src/ai_workflow_engine/gateway_poll.py
  - model-comparison-harness/src/model_comparison_harness/gateway_poll.py
  - prompt-template-manager/src/prompt_template_manager/gateway_poll.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


class GatewayHTTPError(Exception):
    """The gateway rejected a request (HTTP status >= 400) outside the
    documented 410-on-expiry special case. Callers should catch this and
    re-raise it as their own domain-specific exception type."""

    def __init__(self, status_code: int, body_text: str) -> None:
        self.status_code = status_code
        self.body_text = body_text
        super().__init__(f"gateway returned {status_code}: {body_text}")


class GatewayContractError(ValueError):
    """The gateway answered with a success status, but the body does not
    have the shape the contract promises (not a JSON object, or a required
    field missing).

    Distinct from GatewayHTTPError on purpose: nothing was rejected, the
    server is just not speaking the contract. ``field`` names the missing
    field, or is None when the body is not an object at all.
    This replaces an ``assert`` (stripped under ``python -O``) and a bare
    ``KeyError``, neither of which said what was wrong.
    """

    def __init__(self, status_code: int, body_text: str, field: Optional[str], reason: str) -> None:
        self.status_code = status_code
        self.body_text = body_text
        self.field = field
        self.reason = reason
        super().__init__(f"gateway returned {status_code} but {reason}: {body_text}")


#: Poll responses worth asking again: the gateway, or a proxy or load
#: balancer in front of it, is briefly unavailable. A GET of the polling URL
#: has no side effects, so repeating it is always safe. 500 is deliberately
#: not here -- it is the gateway reporting its own bug, not a blip.
TRANSIENT_POLL_STATUS_CODES = frozenset({502, 503, 504})

#: How many times one poll is retried after a transient failure, and the
#: capped exponential schedule between attempts (0.5 s, 1 s, 2 s by
#: default: under 4 s in total, so a real outage still surfaces quickly).
POLL_RETRY_ATTEMPTS = 3
POLL_RETRY_BASE_SECONDS = 0.5
POLL_RETRY_CAP_SECONDS = 4.0


def is_transient_poll_status(status_code: int) -> bool:
    """True if a poll response with this status should be retried."""
    return status_code in TRANSIENT_POLL_STATUS_CODES


def poll_retry_delays(
    attempts: int = POLL_RETRY_ATTEMPTS,
    *,
    base: float = POLL_RETRY_BASE_SECONDS,
    cap: float = POLL_RETRY_CAP_SECONDS,
) -> list[float]:
    """The waits before each retry of one poll; ``len()`` is the retry budget.

    A wrapper retries a poll GET that raised its HTTP client's network error,
    or returned a status for which ``is_transient_poll_status`` is True, at
    most ``len(delays)`` times, sleeping ``delays[n]`` before retry n, and
    then lets the last failure through unchanged. Bounded on purpose: one
    network blip should not fail a whole run, and a dead gateway should not
    hang it either.
    """
    return [min(cap, base * (2**n)) for n in range(max(0, attempts))]


@dataclass
class PollOutcome:
    """What a completed (< 400, non-410) poll response's JSON body means."""

    terminal: bool
    ready: bool
    status: Optional[str]
    result: Optional[Any] = None
    error_message: Optional[str] = None


def submit_url(base_url: str, capability: str) -> str:
    """The URL to POST a new job to."""
    return f"{base_url.rstrip('/')}/v1/{capability}"


def resolve_polling_url(base_url: str, polling_url_path: str) -> str:
    """Turn the polling_url path returned by a submission response into a
    full URL against the same base."""
    return base_url.rstrip("/") + polling_url_path


def parse_submission(status_code: int, body_json: Optional[dict], body_text: str) -> tuple[str, str]:
    """Interpret a submission (POST /v1/{capability}) response.

    Returns (job_id, polling_url_path) on success. Raises GatewayHTTPError
    for any status_code >= 400 -- submission rejection has no special case
    analogous to poll's 410, every repo already treats it uniformly.
    Raises GatewayContractError for a success status whose body is not a
    JSON object or lacks ``id`` or ``polling_url``. What a valid value looks
    like (e.g. a same-origin path) is each wrapper's call, as before.
    """
    if status_code >= 400:
        raise GatewayHTTPError(status_code, body_text)
    if not isinstance(body_json, dict):
        raise GatewayContractError(
            status_code, body_text, None, "the submission response is not a JSON object"
        )
    for field in ("id", "polling_url"):
        if field not in body_json:
            raise GatewayContractError(
                status_code, body_text, field, f"the submission response is missing {field!r}"
            )
    return body_json["id"], body_json["polling_url"]


def is_expired_poll_response(status_code: int) -> bool:
    """True if this poll response is the documented 410-Gone-on-expiry case.

    This is the exact check that must run *before* any generic
    status_code >= 400 handling (raise_for_status() or equivalent) -- the
    gateway's contract is HTTP 410, not a 200 body with status="expired",
    once a terminal job's result has passed its TTL (see ADR-005). Getting
    this ordering backwards is precisely the bug this module exists to stop
    recurring (see ADR-008).
    """
    return status_code == 410


def expired_detail(body_json: Optional[dict]) -> str:
    """Extract the human-readable detail from a 410 poll response's body."""
    return (body_json or {}).get("detail") or "job result has expired"


def classify_poll_body(body_json: dict) -> PollOutcome:
    """Interpret a poll response's JSON body once status_code has already
    been confirmed to be a non-error, non-410 response (i.e. the body's own
    "status" field is the source of truth).

    Maps "ready" -> terminal+ready with result, "error"/"expired" ->
    terminal, not ready, with an error_message, anything else (pending,
    processing, ...) -> not terminal, keep polling.
    """
    status = body_json.get("status")
    if status == "ready":
        return PollOutcome(terminal=True, ready=True, status=status, result=body_json.get("result"))
    if status in ("error", "expired"):
        message = body_json.get("error") or f"job ended with status {status!r}"
        return PollOutcome(terminal=True, ready=False, status=status, error_message=message)
    return PollOutcome(terminal=False, ready=False, status=status)
