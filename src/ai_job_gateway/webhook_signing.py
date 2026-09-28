"""Webhook signatures: how the gateway signs a delivery, and how a receiver
checks one.

Every signed delivery carries three headers::

    X-Gateway-Timestamp:    1758800000                 (unix seconds, per attempt)
    X-Gateway-Signature-V2: sha256=<hex>               HMAC-SHA256(secret, b"<timestamp>." + body)
    X-Gateway-Signature:    sha256=<hex>               HMAC-SHA256(secret, body)   (legacy)

Verify the V2 one. It binds the timestamp into the MAC, so a receiver that
also rejects timestamps outside a small window (``verify_webhook`` defaults
to five minutes) cannot be fed a captured delivery again later. The legacy
body-only signature stays for receivers written against the first version;
it proves origin and integrity but not freshness, so a recorded request
verifies forever.

The timestamp is taken per attempt, so a retry 60 seconds later is still
fresh. Receivers should still dedupe on the job ``id`` in the body: delivery
is at-least-once (see the README's restart-recovery section).
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Mapping, Optional

from . import clock

WEBHOOK_SIGNATURE_HEADER = "X-Gateway-Signature"
WEBHOOK_SIGNATURE_V2_HEADER = "X-Gateway-Signature-V2"
WEBHOOK_TIMESTAMP_HEADER = "X-Gateway-Timestamp"

DEFAULT_TOLERANCE_SECONDS = 300


def _mac(secret: str, message: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def signature_headers(secret: str, body: bytes, timestamp: int) -> dict[str, str]:
    """The headers one delivery attempt carries."""
    return {
        WEBHOOK_TIMESTAMP_HEADER: str(timestamp),
        WEBHOOK_SIGNATURE_V2_HEADER: "sha256=" + _mac(secret, f"{timestamp}.".encode() + body),
        WEBHOOK_SIGNATURE_HEADER: "sha256=" + _mac(secret, body),
    }


def verify_webhook(
    secret: str,
    body: bytes,
    headers: Mapping[str, str],
    *,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
    now: Optional[int] = None,
) -> bool:
    """Receiver side: True iff ``body`` carries a valid, fresh V2 signature.

    ``body`` must be the raw request bytes, not re-serialized JSON.
    ``headers`` may be any mapping; lookup is case-insensitive. The MAC is
    compared in constant time.
    """
    lowered = {k.lower(): v for k, v in headers.items()}
    raw_ts = lowered.get(WEBHOOK_TIMESTAMP_HEADER.lower(), "")
    supplied = lowered.get(WEBHOOK_SIGNATURE_V2_HEADER.lower(), "")
    try:
        timestamp = int(raw_ts)
    except ValueError:
        return False
    current = int(clock.now().timestamp()) if now is None else now
    if abs(current - timestamp) > tolerance_seconds:
        return False
    expected = "sha256=" + _mac(secret, f"{timestamp}.".encode() + body)
    return hmac.compare_digest(supplied.encode(), expected.encode())
