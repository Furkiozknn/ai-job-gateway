"""Exceptions shared by the job manager and the client library."""

from __future__ import annotations


class UnknownCapabilityError(Exception):
    """Raised by JobManager.submit() when the capability isn't registered."""

    def __init__(self, capability: str) -> None:
        self.capability = capability
        super().__init__(f"unknown capability: {capability!r}")


class JobNotFoundError(Exception):
    """Raised by the client when a job id doesn't exist on the server."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"no job with id {job_id!r}")


class JobFailedError(Exception):
    """Raised by JobHandle.wait() when the job finished with status=error."""


class JobExpiredError(Exception):
    """Raised by JobHandle.wait() when the job's result expired before it was fetched."""


class JobSubmissionError(Exception):
    """Raised by the client when POST /v1/{capability} itself is rejected."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"submission rejected ({status_code}): {message}")
