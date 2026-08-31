"""ai-job-gateway: a provider-agnostic, self-hostable reference implementation
of the submit/poll/webhook async job contract used by fal.ai, BFL's own
hosted API, and RunPod's worker-comfyui.

Public surface::

    from ai_job_gateway import (
        JobGatewayClient,   # client: submit() -> JobHandle, handle.wait()
        JobManager,         # server-side orchestration: store + provider registry
        JobRecord, JobStatus,
        Provider, EchoProvider, MockProvider, ProviderError,
        JobStore, InMemoryJobStore, SQLiteJobStore,
        create_app,         # FastAPI app factory
    )
"""

from __future__ import annotations

from .client import JobGatewayClient, JobHandle
from .exceptions import (
    JobExpiredError,
    JobFailedError,
    JobNotFoundError,
    JobSubmissionError,
    UnknownCapabilityError,
)
from .manager import JobManager
from .models import JobRecord, JobStatus
from .providers import EchoProvider, MockProvider, Provider, ProviderError
from .server import create_app
from .store import InMemoryJobStore, JobStore, SQLiteJobStore

__all__ = [
    "JobGatewayClient",
    "JobHandle",
    "JobExpiredError",
    "JobFailedError",
    "JobNotFoundError",
    "JobSubmissionError",
    "UnknownCapabilityError",
    "JobManager",
    "JobRecord",
    "JobStatus",
    "EchoProvider",
    "MockProvider",
    "Provider",
    "ProviderError",
    "create_app",
    "InMemoryJobStore",
    "JobStore",
    "SQLiteJobStore",
]

__version__ = "0.1.0"
