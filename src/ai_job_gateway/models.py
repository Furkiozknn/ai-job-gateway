"""The one data shape everything in this repo passes around: a job record."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel


class JobStatus(str, Enum):
    """The states a job can be in.

    ``expired`` is not something a provider or the job manager ever sets
    directly -- it is computed at read time (see ``store._apply_expiry``)
    whenever a terminal job's ``result_expires_at`` has passed. It is a real
    status a caller can observe, just not one anything writes to storage.
    """

    PENDING = "pending"
    PROCESSING = "processing"
    READY = "ready"
    ERROR = "error"
    EXPIRED = "expired"


TERMINAL_STATUSES = (JobStatus.READY, JobStatus.ERROR)


class JobRecord(BaseModel):
    """The full record for one job, as returned by the store and the API."""

    id: str
    capability: str
    provider: str
    params: dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    created_at: datetime
    updated_at: datetime
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    result_expires_at: Optional[datetime] = None
    webhook_url: Optional[str] = None
