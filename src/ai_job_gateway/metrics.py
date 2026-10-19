"""Prometheus metrics for the gateway, without a prometheus_client dependency.

``GET /metrics`` serves these in the Prometheus text exposition format
(version 0.0.4), which Prometheus, VictoriaMetrics, Grafana Agent/Alloy and
the OpenTelemetry Collector's ``prometheus`` receiver all scrape. The
format is small and stable, so it is rendered here by hand rather than by
adding a fifth runtime dependency to a reference server.

What is measured, and where:

- ``ajg_jobs_created_total{capability}``: one per job actually created. An
  idempotent replay that returns an existing job does not count.
- ``ajg_job_queue_wait_seconds{capability}``: submission to provider start,
  i.e. the time a job sat ``pending`` behind ``--max-concurrent-jobs``.
- ``ajg_provider_run_seconds{capability,outcome}``: how long
  ``Provider.run()`` took. ``outcome`` is ``ready`` (returned), ``error``
  (raised) or ``cancelled`` (its task was cancelled, which is also what
  ``--job-timeout`` does to a run that overstays).
- ``ajg_provider_runs_in_flight{capability}``: provider calls running now.
- ``ajg_webhook_deliveries_total{outcome}``: ``delivered`` or ``failed``
  (every attempt exhausted, the dead letter).
- ``ajg_jobs{status}`` and ``ajg_capabilities_registered``: read from the
  store and the registry at scrape time, so they are right after a restart
  too, where the counters above start again from zero (Prometheus handles a
  counter reset; ``rate()`` and ``increase()`` already account for it).

Labels are only ever capability names (bounded by the registry), statuses
and outcomes, so the series count stays small whatever callers send.
"""

from __future__ import annotations

import math
import time
from typing import Iterable, Optional

from .providers import Provider

#: Prometheus text exposition format, version 0.0.4.
CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

#: Seconds. Generative jobs run from well under a second (echo, a resize) to
#: ten minutes (the default --job-timeout), so the buckets span that range.
DEFAULT_BUCKETS: tuple[float, ...] = (0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(names: Iterable[str], values: Iterable[str], extra: str = "") -> str:
    parts = [f'{n}="{_escape(v)}"' for n, v in zip(names, values)]
    if extra:
        parts.append(extra)
    return "{" + ",".join(parts) + "}" if parts else ""


def _number(value: float) -> str:
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if float(value).is_integer():
        return str(int(value))
    return repr(float(value))


class _Metric:
    kind = ""

    def __init__(self, name: str, help_text: str, labelnames: tuple[str, ...] = ()) -> None:
        self.name = name
        self.help = help_text
        self.labelnames = labelnames

    def _key(self, labels: dict[str, str]) -> tuple[str, ...]:
        if set(labels) != set(self.labelnames):
            raise ValueError(f"{self.name} takes labels {self.labelnames}, got {tuple(labels)}")
        return tuple(str(labels[n]) for n in self.labelnames)

    def _header(self) -> list[str]:
        return [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} {self.kind}"]


class Counter(_Metric):
    kind = "counter"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.values: dict[tuple[str, ...], float] = {}

    def inc(self, amount: float = 1.0, **labels: str) -> None:
        key = self._key(labels)
        self.values[key] = self.values.get(key, 0.0) + amount

    def render(self) -> list[str]:
        lines = self._header()
        for key, value in sorted(self.values.items()):
            lines.append(f"{self.name}{_labels(self.labelnames, key)} {_number(value)}")
        return lines


class Gauge(Counter):
    kind = "gauge"

    def dec(self, amount: float = 1.0, **labels: str) -> None:
        self.inc(-amount, **labels)


class Histogram(_Metric):
    kind = "histogram"

    def __init__(
        self,
        name: str,
        help_text: str,
        labelnames: tuple[str, ...] = (),
        buckets: tuple[float, ...] = DEFAULT_BUCKETS,
    ) -> None:
        super().__init__(name, help_text, labelnames)
        self.buckets = tuple(sorted(buckets))
        # Per label set: one count per bucket (non-cumulative), sum, count.
        self.series: dict[tuple[str, ...], list] = {}

    def observe(self, value: float, **labels: str) -> None:
        key = self._key(labels)
        counts, total = self.series.setdefault(key, [[0] * len(self.buckets), [0.0, 0]])
        for i, bound in enumerate(self.buckets):
            if value <= bound:
                counts[i] += 1
                break
        total[0] += value
        total[1] += 1

    def render(self) -> list[str]:
        lines = self._header()
        for key, (counts, (total_sum, total_count)) in sorted(self.series.items()):
            cumulative = 0
            for bound, count in zip(self.buckets, counts):
                cumulative += count
                le = f'le="{_number(bound)}"'
                lines.append(
                    f"{self.name}_bucket{_labels(self.labelnames, key, le)} {cumulative}"
                )
            inf = 'le="+Inf"'
            lines.append(f"{self.name}_bucket{_labels(self.labelnames, key, inf)} {total_count}")
            lines.append(f"{self.name}_sum{_labels(self.labelnames, key)} {_number(total_sum)}")
            lines.append(f"{self.name}_count{_labels(self.labelnames, key)} {total_count}")
        return lines


class InstrumentedProvider(Provider):
    """Wraps one submission's provider to time its run and count outcomes.

    Built per job by ``JobManager.submit``, so the moment it is created is
    the submission time and the queue wait needs no extra bookkeeping. It
    reports the wrapped provider's ``name``, so job records and
    ``GET /v1/capabilities`` are unchanged.
    """

    def __init__(self, inner: Provider, capability: str, metrics: "GatewayMetrics") -> None:
        self.inner = inner
        self.name = inner.name
        self.capability = capability
        self.metrics = metrics
        self.submitted_at = time.monotonic()

    async def run(self, job_id: str, params: dict) -> dict:
        started = time.monotonic()
        m = self.metrics
        m.queue_wait.observe(started - self.submitted_at, capability=self.capability)
        m.in_flight.inc(capability=self.capability)
        outcome = "cancelled"  # BaseException other than Exception: CancelledError
        try:
            result = await self.inner.run(job_id, params)
            outcome = "ready"
            return result
        except Exception:
            outcome = "error"
            raise
        finally:
            m.in_flight.dec(capability=self.capability)
            m.provider_runs.observe(
                time.monotonic() - started, capability=self.capability, outcome=outcome
            )


class GatewayMetrics:
    """The gateway's metric set. One per ``JobManager``."""

    def __init__(self, buckets: tuple[float, ...] = DEFAULT_BUCKETS) -> None:
        self.jobs_created = Counter(
            "ajg_jobs_created_total",
            "Jobs created (idempotent replays of an existing job are not counted).",
            ("capability",),
        )
        self.queue_wait = Histogram(
            "ajg_job_queue_wait_seconds",
            "Seconds from submission until the provider started (time spent pending).",
            ("capability",),
            buckets,
        )
        self.provider_runs = Histogram(
            "ajg_provider_run_seconds",
            "Seconds Provider.run() took, by outcome (ready, error, cancelled).",
            ("capability", "outcome"),
            buckets,
        )
        self.in_flight = Gauge(
            "ajg_provider_runs_in_flight",
            "Provider calls running right now.",
            ("capability",),
        )
        self.webhooks = Counter(
            "ajg_webhook_deliveries_total",
            "Finished webhook deliveries by outcome (delivered, failed after every retry).",
            ("outcome",),
        )

    def instrument(self, capability: str, provider: Provider) -> Provider:
        return InstrumentedProvider(provider, capability, self)

    def render(
        self,
        *,
        jobs_by_status: Optional[dict[str, int]] = None,
        registered_capabilities: Optional[int] = None,
    ) -> str:
        """The exposition text. The two keyword arguments are point-in-time
        values the caller reads from the store and the registry."""
        lines: list[str] = []
        for metric in (
            self.jobs_created,
            self.queue_wait,
            self.provider_runs,
            self.in_flight,
            self.webhooks,
        ):
            lines.extend(metric.render())
        if jobs_by_status is not None:
            jobs = Gauge("ajg_jobs", "Jobs in the store by displayed status.", ("status",))
            for status, count in jobs_by_status.items():
                jobs.inc(count, status=status)
            lines.extend(jobs.render())
        if registered_capabilities is not None:
            lines.append("# HELP ajg_capabilities_registered Capabilities this server serves.")
            lines.append("# TYPE ajg_capabilities_registered gauge")
            lines.append(f"ajg_capabilities_registered {registered_capabilities}")
        return "\n".join(lines) + "\n"
