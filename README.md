# ai-job-gateway

A provider-agnostic, self-hostable reference implementation of the async job contract independently converged on by [fal.ai](https://fal.ai), [Black Forest Labs' own hosted API](https://bfl.ai), and RunPod's [`worker-comfyui`](https://github.com/runpod-workers/worker-comfyui):

```
POST /v1/{capability}   ->  202 { "id": "...", "polling_url": "/v1/jobs/{id}" }
GET  {polling_url}      ->  { "status": "pending"|"processing"|"ready"|"error"|"expired", "result": {...}?, "error": "..."? }
```

Submit a job, get an id back immediately, poll (or get a webhook) until it's done. That's the whole public contract, for *any* generative model — image, video, lip-sync, whatever a `Provider` wraps.

This isn't a copy of fal.ai's or RunPod's code — it's an original implementation of the same well-known, provider-independently-discovered API shape, built as a genuinely reusable open-source building block for a small ecosystem of focused AI-creative-platform repos. Elsewhere in that ecosystem, a future image-gen wrapper, video-gen wrapper, or lip-sync wrapper registers itself here as a `Provider` under a capability name, and every one of them gets the same submit/poll/webhook contract, job persistence, and expiry semantics for free.

## Why this contract

BFL's own hosted API and RunPod's `worker-comfyui` reach the same shape independently:

- **Submit returns immediately.** The caller never blocks on GPU time; `POST` hands back a job id and a URL to poll.
- **Result URLs are short-lived.** BFL expires image URLs after 10 minutes — a deliberate cost/storage-lifecycle decision that forces the caller to persist results promptly instead of treating the gateway as permanent storage. This repo makes that a first-class, `GET`-visible `expired` status rather than a silent 404.
- **Webhooks are optional, polling is the fallback.** Some callers want push, most are fine polling every second or two. Both should work off the same job record.

## Architecture

```
┌─────────────┐   submit/poll    ┌──────────────┐   run(job_id, params)   ┌───────────┐
│ Your caller │ ───────────────► │  HTTP API     │ ──────────────────────► │ Provider  │
│ (or the     │ ◄─────────────── │  (FastAPI)    │                         │ (your     │
│  client lib)│   job record     │               │ ◄────────────────────── │  backend) │
└─────────────┘                  └──────┬────────┘   result dict / raise  └───────────┘
                                         │
                                         ▼
                                  ┌──────────────┐
                                  │  JobStore     │  in-memory or SQLite,
                                  │  (+ expiry)   │  same interface either way
                                  └──────────────┘
```

- **`Provider`** — the pluggable seam. One async method: `run(job_id, params) -> dict`, or raise. Everything else in this repo is generic over "some provider."
- **`JobManager`** — accepts a submission, creates the job record, runs the provider as a background `asyncio` task, and delivers a webhook (with retries) on completion. This is an in-process task queue — a real deployment at scale swaps this for a real queue (Redis/RQ, Celery, or dispatching to serverless GPU workers like RunPod) without the public contract changing.
- **`JobStore`** — `create` / `get` / `update_status` / `list`, with two implementations (`InMemoryJobStore`, `SQLiteJobStore`) behind the same interface, so "in-memory is good enough" is never an unexamined assumption. Expiry (`result_expires_at` vs. "now") is computed at read time in the store, not mutated into storage — both implementations stay simple and there's no background sweeper.
- **`create_app(manager)`** — a FastAPI app factory implementing the HTTP contract generically for whatever capabilities are registered.
- **`JobGatewayClient` / `JobHandle`** — an ergonomic Python client wrapping the submit → poll loop (`handle.wait(timeout=...)`).

## Writing a new Provider

```python
from ai_job_gateway import Provider

class MyImageProvider(Provider):
    name = "my-image-model"

    def __init__(self, api_key: str, http_client):
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
```

Register it: `JobManager(store, {"my-capability": MyImageProvider(...)})` — it's live under `POST /v1/my-capability`. No base-class state, no lifecycle hooks required beyond `run()`.

Two providers ship out of the box, needing no network, API key, or GPU, so the whole system is testable and demoable on its own:

- **`EchoProvider`** (`echo`) — returns the params it was given. The "hello world" capability.
- **`MockProvider`** (`mock-generate` in the default registry) — simulates configurable delay and can be flipped to always fail, for exercising both the happy path and the error path.

## Running the reference server

```bash
uv sync
uv run ai-job-gateway serve
# or, with SQLite persistence instead of in-memory:
uv run ai-job-gateway serve --db jobs.db
```

Then, from another terminal:

```bash
curl -s -X POST http://127.0.0.1:8000/v1/mock-generate -d '{"prompt": "a cat riding a bike"}' | tee /tmp/job.json
# {"id": "…", "polling_url": "/v1/jobs/…"}

curl -s http://127.0.0.1:8000/v1/jobs/$(python3 -c "import json;print(json.load(open('/tmp/job.json'))['id'])")
# {"id": "…", "status": "processing", …}   -- poll again in a second or two
# {"id": "…", "status": "ready", "result": {…}, "result_expires_at": "…"}

curl -s http://127.0.0.1:8000/v1/capabilities
# {"mock-generate": "mock", "echo": "echo"}
```

Or use the bundled CLI convenience for the same round-trip:

```bash
uv run ai-job-gateway submit mock-generate '{"prompt": "a cat riding a bike"}'
```

### Webhooks

Pass `webhook_url` in the submission body (alongside your real params — it's popped out server-side before your provider ever sees it):

```bash
curl -X POST http://127.0.0.1:8000/v1/mock-generate \
  -d '{"prompt": "a cat", "webhook_url": "https://your-server.example/hooks/job-done"}'
```

Your endpoint receives a `POST` with the full job record as JSON once the job reaches `ready` or `error`. Delivery retries up to 3 times with backoff on failure; if every attempt fails, it's logged and dropped — the job's own status in the store is already correct regardless of whether the webhook ever arrives, so a caller relying on polling as a fallback is never left with stale state.

## Using the Python client

```python
import asyncio
from ai_job_gateway import JobGatewayClient

async def main():
    async with JobGatewayClient("http://127.0.0.1:8000") as client:
        handle = await client.submit("mock-generate", {"prompt": "a cat riding a bike"})
        result = await handle.wait(timeout=30)
        print(result)

asyncio.run(main())
```

`handle.wait()` raises `JobFailedError` on an `error` status, `JobExpiredError` if the result expires before you fetch it, and plain `TimeoutError` if your `timeout` elapses first.

## Development

```bash
uv sync --group dev
uv run pytest
```

The suite is fully async (`pytest-asyncio`), exercises the manager/store/server/client layers independently and together (server tests drive the FastAPI app in-process via `httpx.ASGITransport` — no real sockets), and verifies expiry/webhook-retry behavior by monkeypatching `ai_job_gateway.clock.now` and webhook delivery via `httpx.MockTransport`, never by sleeping for real minutes.

## Roadmap — what a production deployment would add

This is a reference implementation; it's honest about what it isn't:

- **Real job queue.** The in-process `asyncio.create_task` model is fine for one server process. Real traffic needs a real queue (Redis/RQ, Celery, or dispatching to serverless GPU workers) so job execution survives a server restart and scales past one machine.
- **Auth & rate limiting.** There is none. A public deployment needs API keys and per-key quotas before this is safe to expose.
- **Real providers.** `EchoProvider`/`MockProvider` prove the contract; a real deployment implements `Provider` for actual backends (FLUX, Wan2.2, MuseTalk, whatever).
- **Multi-worker/multi-process coordination.** `InMemoryJobStore` doesn't share state across processes; `SQLiteJobStore` survives a restart but isn't built for concurrent multi-process writers. A real deployment at that scale wants Postgres (or similar) behind the same `JobStore` interface.
- **Adaptive batching / multi-stage pipelines.** Out of scope here — see this project's sibling research notes on generative-AI infrastructure patterns for where that fits in a larger system.

## License

MIT — see [LICENSE](LICENSE).
