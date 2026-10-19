"""JobHandle.poll() retries a transient failure a bounded number of times,
and JobGatewayClient.submit() reports a malformed 2xx instead of crashing."""

from __future__ import annotations

import httpx
import pytest

from ai_job_gateway import client as client_module
from ai_job_gateway.client import JobGatewayClient
from ai_job_gateway.exceptions import JobSubmissionError


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch):
    slept: list[float] = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(client_module.asyncio, "sleep", fake_sleep)
    return slept


def _client(responses):
    """A client whose GETs answer from ``responses`` in order (an exception
    instance is raised instead of answered); POST always accepts."""
    calls = {"get": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"id": "j1", "polling_url": "/v1/jobs/j1"})
        item = responses[calls["get"]]
        calls["get"] += 1
        if isinstance(item, Exception):
            raise item
        return item

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return JobGatewayClient("http://gateway.test", http_client=http), calls


READY = httpx.Response(200, json={"id": "j1", "status": "ready", "result": {"ok": True}})


async def test_a_network_blip_and_a_503_are_retried_then_the_poll_succeeds(_no_backoff_sleep):
    client, calls = _client([httpx.ConnectError("boom"), httpx.Response(503), READY])
    async with client:
        handle = await client.submit("echo", {"a": 1})
        record = await handle.poll()
    assert record["status"] == "ready"
    assert calls["get"] == 3
    assert _no_backoff_sleep == client_module.poll_retry_delays()[:2]


async def test_retries_are_bounded_and_the_last_network_error_is_raised():
    attempts = len(client_module.poll_retry_delays()) + 1
    client, calls = _client([httpx.ConnectError("down")] * attempts)
    async with client:
        handle = await client.submit("echo", {"a": 1})
        with pytest.raises(httpx.ConnectError):
            await handle.poll()
    assert calls["get"] == attempts


async def test_a_persistent_503_surfaces_as_before_after_the_retries():
    attempts = len(client_module.poll_retry_delays()) + 1
    client, calls = _client([httpx.Response(503)] * attempts)
    async with client:
        handle = await client.submit("echo", {"a": 1})
        with pytest.raises(httpx.HTTPStatusError):
            await handle.poll()
    assert calls["get"] == attempts


@pytest.mark.parametrize("status", [500, 404, 410])
async def test_non_transient_statuses_are_not_retried(status):
    client, calls = _client([httpx.Response(status, json={"detail": "x"})] * 4)
    async with client:
        handle = await client.submit("echo", {"a": 1})
        with pytest.raises(Exception):
            await handle.poll()
    assert calls["get"] == 1


@pytest.mark.parametrize(
    "response, expected",
    [
        (httpx.Response(202, json={"polling_url": "/v1/jobs/j1"}), "missing 'id'"),
        (httpx.Response(202, json=["not", "an", "object"]), "not a JSON object"),
        (httpx.Response(202, text="<html>proxy page</html>"), "not a JSON object"),
    ],
)
async def test_a_malformed_successful_submission_is_a_submission_error(response, expected):
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response))
    async with JobGatewayClient("http://gateway.test", http_client=http) as client:
        with pytest.raises(JobSubmissionError, match=expected) as info:
            await client.submit("echo", {"a": 1})
    assert info.value.status_code == 202
