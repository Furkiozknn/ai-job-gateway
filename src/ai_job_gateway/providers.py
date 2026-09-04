"""The Provider interface, plus the two demo implementations.

A provider is the pluggable seam of this whole project: everything else
(the job manager, the HTTP API, the client) is generic over "some async
callable that turns params into a result dict." This module holds the
interface and two providers that need no network, no API key and no GPU, so
the rest of the system is testable on its own. The real ones live next door:
`providers_pollinations` (hosted, keyless image generation) and
`providers_local` (local media operations via mini-creative-toolkit).
"""

from __future__ import annotations

import asyncio
import random
from abc import ABC, abstractmethod
from typing import Any


class ProviderError(Exception):
    """Optional, explicit way for a provider to signal job failure.

    You don't have to raise this specific type -- ``run()`` can raise
    anything and the job manager will catch it, store ``str(exc)`` as the
    job's error message, and move the job to ``error`` status. This class
    just exists for providers that want to be unambiguous about "this is a
    provider-side failure" versus, say, a bug.
    """


class Provider(ABC):
    """Implement this to add a new backend.

    One method: given a job id and the params dict the caller submitted,
    either return a JSON-serializable result dict, or raise. That's the
    entire contract -- no base-class state, no lifecycle hooks, nothing to
    configure beyond what your own ``__init__`` needs.

    Example -- wrapping a hypothetical image model's HTTP API::

        class MyImageProvider(Provider):
            name = "my-image-model"

            def __init__(self, api_key: str, http_client: httpx.AsyncClient):
                self.api_key = api_key
                self.http_client = http_client

            async def run(self, job_id: str, params: dict) -> dict:
                response = await self.http_client.post(
                    "https://api.example.com/generate",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    json=params,
                )
                response.raise_for_status()
                return response.json()

    Register it with ``JobManager(store, {"my-capability": MyImageProvider(...)})``
    and it's live under ``POST /v1/my-capability``.
    """

    #: Short name recorded on job records and shown by GET /v1/capabilities.
    #: Override on subclasses (or set as a class attribute like the examples
    #: below); defaults to the class name if left unset.
    name: str = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.name:
            cls.name = cls.__name__

    @abstractmethod
    async def run(self, job_id: str, params: dict) -> dict:
        """Do the work. Return a JSON-serializable result, or raise."""
        raise NotImplementedError


class EchoProvider(Provider):
    """The "hello world" provider: returns the params it was given.

    No delay, no configuration, no external call. Useful as a copy-paste
    starting point when writing a new provider, and as a trivial capability
    to point smoke tests and curl examples at.
    """

    name = "echo"

    async def run(self, job_id: str, params: dict) -> dict:
        return {"echoed": params}


class MockProvider(Provider):
    """A deterministic fake provider for tests, demos, and local dev.

    Configure it up front to simulate a fixed or randomized processing delay
    (``delay_seconds`` as a float, or a ``(min, max)`` tuple for jitter), and
    to always succeed or always fail. ``set_should_fail`` lets a running
    instance be flipped between the two mid-test, which is handy for
    exercising both the happy path and the error path against the same
    registered capability.
    """

    name = "mock"

    def __init__(
        self,
        *,
        delay_seconds: float | tuple[float, float] = 0.05,
        should_fail: bool = False,
        failure_message: str = "mock provider was configured to fail",
    ) -> None:
        self.delay_seconds = delay_seconds
        self.should_fail = should_fail
        self.failure_message = failure_message

    def set_should_fail(self, value: bool, message: str | None = None) -> None:
        self.should_fail = value
        if message is not None:
            self.failure_message = message

    async def run(self, job_id: str, params: dict) -> dict:
        delay = self.delay_seconds
        if isinstance(delay, tuple):
            delay = random.uniform(*delay)
        if delay:
            await asyncio.sleep(delay)
        if self.should_fail:
            raise ProviderError(self.failure_message)
        return {
            "provider": self.name,
            "job_id": job_id,
            "params_received": params,
            "output": f"mock-result-for-{job_id}",
        }


def default_registry() -> dict[str, Provider]:
    """The registry the CLI's ``serve`` command runs.

    Two demo providers, one real hosted one, and - when the ``media`` extra
    is installed - the local media operations. ``GET /v1/capabilities`` shows
    exactly which of these are live in a given process.
    """
    from .providers_local import local_media_registry, toolkit_available
    from .providers_pollinations import PollinationsImageProvider

    registry: dict[str, Provider] = {
        "mock-generate": MockProvider(delay_seconds=(0.5, 2.0)),
        "echo": EchoProvider(),
        "generate-image": PollinationsImageProvider(),
    }
    if toolkit_available():
        registry.update(local_media_registry())
    return registry
