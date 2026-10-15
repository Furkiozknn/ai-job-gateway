"""GET /metrics and the GatewayMetrics instrumentation behind it."""

from __future__ import annotations

import asyncio
import re
from datetime import timedelta

import httpx
import pytest
from httpx import ASGITransport

from ai_job_gateway import GatewayMetrics
from ai_job_gateway.manager import JobManager
from ai_job_gateway.metrics import CONTENT_TYPE, Counter, Histogram
from ai_job_gateway.providers import EchoProvider, MockProvider, Provider
from ai_job_gateway.server import create_app
from ai_job_gateway.store import InMemoryJobStore

# One sample line of the text format: name, optional {labels}, value.
_SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})? (-?[0-9.e+-]+|\+Inf|-Inf|NaN)$')


def _samples(text: str) -> dict[str, float]:
    """Parse exposition text into {'name{labels}': value}, checking every
    line is a comment or a well-formed sample along the way."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("# HELP ") or line.startswith("# TYPE "):
            continue
        match = _SAMPLE.match(line)
        assert match, f"not a valid sample line: {line!r}"
        name, labels, value = match.groups()
        out[name + (labels or "")] = float(value)
    return out


class Gate(Provider):
    """Blocks until released, so a run can be observed while in flight."""

    name = "gate"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def run(self, job_id: str, params: dict) -> dict:
        self.started.set()
        await self.release.wait()
        return {"ok": True}


class Sleeper(Provider):
    name = "sleeper"

    async def run(self, job_id: str, params: dict) -> dict:
        await asyncio.sleep(30)
        return {}


async def _settle(manager: JobManager) -> None:
    for _ in range(200):
        if not any(not t.done() for t in manager._background_tasks):
            return
        await asyncio.sleep(0.005)
    raise AssertionError("background work did not finish")


@pytest.fixture
def app_and_manager():
    registry = {
        "echo": EchoProvider(),
        "always-fails": MockProvider(delay_seconds=0, should_fail=True, failure_message="boom"),
        "gate": Gate(),
    }
    manager = JobManager(InMemoryJobStore(), registry)
    return create_app(manager), manager


async def test_metrics_endpoint_reports_outcomes_and_store_state(app_and_manager):
    app, manager = app_and_manager
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/v1/echo", json={"x": 1})
        await c.post("/v1/echo", json={"x": 2})
        await c.post("/v1/always-fails", json={"x": 3})
        await _settle(manager)

        resp = await c.get("/metrics")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == CONTENT_TYPE
        samples = _samples(resp.text)

    assert samples['ajg_jobs_created_total{capability="echo"}'] == 2
    assert samples['ajg_jobs_created_total{capability="always-fails"}'] == 1
    assert samples['ajg_provider_run_seconds_count{capability="echo",outcome="ready"}'] == 2
    assert samples['ajg_provider_run_seconds_count{capability="always-fails",outcome="error"}'] == 1
    assert samples['ajg_job_queue_wait_seconds_count{capability="echo"}'] == 2
    assert samples['ajg_jobs{status="ready"}'] == 2
    assert samples['ajg_jobs{status="error"}'] == 1
    assert samples['ajg_jobs{status="pending"}'] == 0
    assert samples["ajg_capabilities_registered"] == 3
    # Every finished run is in the +Inf bucket, and buckets are cumulative.
    assert samples['ajg_provider_run_seconds_bucket{capability="echo",outcome="ready",le="+Inf"}'] == 2
    assert (
        samples['ajg_provider_run_seconds_bucket{capability="echo",outcome="ready",le="0.1"}']
        <= samples['ajg_provider_run_seconds_bucket{capability="echo",outcome="ready",le="600"}']
    )


async def test_in_flight_gauge_rises_and_falls(app_and_manager):
    app, manager = app_and_manager
    gate = manager.registry["gate"]
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.post("/v1/gate", json={"x": 1})
        await asyncio.wait_for(gate.started.wait(), 1)
        during = _samples((await c.get("/metrics")).text)
        gate.release.set()
        await _settle(manager)
        after = _samples((await c.get("/metrics")).text)

    assert during['ajg_provider_runs_in_flight{capability="gate"}'] == 1
    assert during['ajg_jobs{status="processing"}'] == 1
    assert after['ajg_provider_runs_in_flight{capability="gate"}'] == 0
    assert after['ajg_provider_run_seconds_count{capability="gate",outcome="ready"}'] == 1


async def test_an_idempotent_replay_is_not_counted_as_a_new_job(app_and_manager):
    app, manager = app_and_manager
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        for _ in range(3):
            await c.post("/v1/echo", json={"x": 1}, headers={"Idempotency-Key": "k-1"})
        await _settle(manager)
        samples = _samples((await c.get("/metrics")).text)
    assert samples['ajg_jobs_created_total{capability="echo"}'] == 1


async def test_a_timed_out_run_is_counted_as_cancelled():
    manager = JobManager(
        InMemoryJobStore(), {"sleeper": Sleeper()}, job_timeout=timedelta(seconds=0.02)
    )
    await manager.submit("sleeper", {"x": 1})
    await _settle(manager)
    samples = _samples(manager.metrics.render())
    assert samples['ajg_provider_run_seconds_count{capability="sleeper",outcome="cancelled"}'] == 1
    assert samples['ajg_provider_runs_in_flight{capability="sleeper"}'] == 0


async def test_queue_wait_measures_time_spent_pending_behind_the_slot_cap():
    gate = Gate()
    manager = JobManager(InMemoryJobStore(), {"gate": gate}, max_concurrent_jobs=1)
    await manager.submit("gate", {"n": 1})
    await asyncio.wait_for(gate.started.wait(), 1)
    await manager.submit("gate", {"n": 2})
    await asyncio.sleep(0.15)  # the second job waits this long for the slot
    gate.release.set()
    await _settle(manager)
    hist = manager.metrics.queue_wait.series[("gate",)]
    counts, (total_sum, total_count) = hist
    assert total_count == 2
    assert total_sum >= 0.1


async def test_webhook_outcomes_are_counted():
    ok = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(204)))
    manager = JobManager(InMemoryJobStore(), {"echo": EchoProvider()}, http_client=ok)
    await manager.submit("echo", {"x": 1}, webhook_url="https://hooks.example.test/a")
    await _settle(manager)
    samples = _samples(manager.metrics.render())
    assert samples['ajg_webhook_deliveries_total{outcome="delivered"}'] == 1
    await ok.aclose()


async def test_metrics_require_the_api_key_when_one_is_set():
    manager = JobManager(InMemoryJobStore(), {"echo": EchoProvider()})
    app = create_app(manager, api_key="k")
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        assert (await c.get("/metrics")).status_code == 401
        ok = await c.get("/metrics", headers={"Authorization": "Bearer k"})
        assert ok.status_code == 200
        assert (await c.get("/health")).status_code == 200  # still open


async def test_metrics_is_documented_in_openapi(app_and_manager):
    app, _ = app_and_manager
    operation = app.openapi()["paths"]["/metrics"]["get"]
    assert CONTENT_TYPE in operation["responses"]["200"]["content"]


def test_label_values_are_escaped_and_label_names_are_enforced():
    counter = Counter("t_total", "help", ("capability",))
    counter.inc(capability='we"ird\\name\nx')
    (line,) = [l for l in counter.render() if not l.startswith("#")]
    assert line == 't_total{capability="we\\"ird\\\\name\\nx"} 1'
    with pytest.raises(ValueError):
        counter.inc(wrong="x")


def test_histogram_buckets_are_cumulative_and_values_above_the_top_land_in_inf():
    hist = Histogram("h_seconds", "help", (), buckets=(1, 5))
    for value in (0.5, 2, 2, 9):
        hist.observe(value)
    samples = _samples("\n".join(hist.render()))
    assert samples['h_seconds_bucket{le="1"}'] == 1
    assert samples['h_seconds_bucket{le="5"}'] == 3
    assert samples['h_seconds_bucket{le="+Inf"}'] == 4
    assert samples["h_seconds_count"] == 4
    assert samples["h_seconds_sum"] == 13.5


def test_a_custom_metrics_object_can_be_injected():
    metrics = GatewayMetrics(buckets=(1,))
    manager = JobManager(InMemoryJobStore(), {"echo": EchoProvider()}, metrics=metrics)
    assert manager.metrics is metrics
