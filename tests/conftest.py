from __future__ import annotations

import pytest

from ai_job_gateway.manager import JobManager
from ai_job_gateway.providers import EchoProvider, MockProvider
from ai_job_gateway.store import InMemoryJobStore


@pytest.fixture
def store():
    return InMemoryJobStore()


@pytest.fixture
def registry():
    return {
        "mock-generate": MockProvider(delay_seconds=0.01),
        "echo": EchoProvider(),
    }


@pytest.fixture
def manager(store, registry):
    return JobManager(store, registry)
