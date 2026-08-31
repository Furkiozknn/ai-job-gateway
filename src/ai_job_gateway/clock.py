"""The gateway's one time source.

Every part of the codebase that needs "now" imports this module and calls
``clock.now()`` -- never ``datetime.now()`` directly, and never
``from ai_job_gateway.clock import now`` (that would bind the function at
import time, before a test gets a chance to monkeypatch it).

Centralizing this lets tests exercise expiry behavior deterministically by
monkeypatching ``ai_job_gateway.clock.now`` instead of sleeping in real time
for however many minutes the expiry window is configured for.
"""

from __future__ import annotations

from datetime import datetime, timezone


def now() -> datetime:
    """Return the current time, timezone-aware, in UTC."""
    return datetime.now(timezone.utc)
