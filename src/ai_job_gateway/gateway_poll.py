"""Shared submit/poll response-interpretation logic for ai-job-gateway
-compatible HTTP clients.

Vendored, not pip-installed (see ADR-006 and ADR-008 in this repo's
research/lab/DECISIONS.md): every consuming repo copies this file verbatim
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

Canonical source: Furkiozknn/Furkiozknn, research/lab/shared/gateway_poll.py
Known copies (keep in sync by hand -- there is no import relationship):
  - ai-job-gateway/src/ai_job_gateway/client.py
  - prompt-template-manager/src/prompt_template_manager/gateway_client.py
  - model-comparison-harness/src/model_comparison_harness/backends.py
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
    """
    if status_code >= 400:
        raise GatewayHTTPError(status_code, body_text)
    assert body_json is not None, "a successful submission response must have a JSON body"
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
