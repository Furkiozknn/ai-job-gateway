# ai-job-gateway

**Submit a generative-AI job, get an id back instantly, poll or get webhooked when it's done — a small, hardened, provider-agnostic reference server for the async job contract every serious inference API ends up with.**

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

## Providers that ship

The default registry (`ai-job-gateway serve`, `GET /v1/capabilities`) is no longer only demos. Every capability states where it runs and what it needs:

| Capability | Provider | Runs | Needs | Notes |
| --- | --- | --- | --- | --- |
| `echo` | `EchoProvider` | local | nothing | Returns its params. The "hello world". |
| `mock-generate` | `MockProvider` | local | nothing | Configurable delay, can be flipped to fail. For tests and demos. |
| `generate-image` | `PollinationsImageProvider` | **hosted** | network | Real text-to-image via Pollinations.ai. **No API key**, but the prompt leaves the machine. |
| `media-resize`, `media-convert`, `media-strip-metadata`, `media-watermark`, `media-remove-background`, `media-upscale`, `media-optimize`, `media-inspect`, `media-thumbnail`, `media-extract-audio` | `LocalMediaProvider` | local | the `media` extra | Real local media work via [mini-creative-toolkit](https://github.com/Furkiozknn/mini-creative-toolkit). Registered only when it is installed. |

```bash
uv sync --extra media      # opt in to the local media capabilities
uv run ai-job-gateway serve
curl -s http://127.0.0.1:8000/v1/capabilities
# {"mock-generate": "mock", "echo": "echo", "generate-image": "pollinations",
#  "media-resize": "local-media:resize", "media-upscale": "local-media:upscale", ...}
```

A real two-step run, no key anywhere:

```bash
uv run ai-job-gateway submit generate-image '{"prompt": "a lighthouse at dusk", "width": 768, "height": 512}'
# {"output_path": "output/<job-id>.jpg", "format": "jpg", "execution": "hosted", "service": "Pollinations.ai", ...}
uv run ai-job-gateway submit media-upscale '{"image_path": "output/<job-id>.jpg", "scale": 2}'
# {"output_path": "...", "selected_method": "fsrcnn", "selection_reason": "...", "execution": "local", ...}
```

This is exactly the chain [ai-workflow-engine](https://github.com/Furkiozknn/ai-workflow-engine) runs as a YAML pipeline (`generate → upscale`), so that pipeline now does real work end to end.

### What the real providers guarantee

**`generate-image`** is the only capability that leaves the machine, and it says so in every result (`"execution": "hosted"`, a `disclosure` field). The response is never trusted: status, content type, a streaming byte budget (`max_download_bytes`, default 64 MB) and the image's magic bytes are all checked before anything is written, so an HTML error page served with HTTP 200 is refused rather than saved as a `.jpg`. Output lands in `GATEWAY_OUTPUT_DIR` (default `./output`), named by job id after sanitising. The prompt is never logged. If the service ever starts requiring a key, that returns a specific error and nothing else in the gateway is affected.

**`media-*`** run entirely locally, in a worker thread so the event loop keeps serving polls. Params are the toolkit tool's own keyword arguments (`image_path`, `width`, `goal`, ...); anything the tool does not declare is refused, and `config` is reserved. Path validation, size limits and the `MCT_ALLOWED_ROOTS` restriction are the toolkit's and apply to the gateway process's environment — set `MCT_ALLOWED_ROOTS` if the gateway is reachable by callers you do not fully trust, because a local job reads and writes files as the user running the server. The gateway and the toolkit share a filesystem; that is the deployment model, not an oversight.

The two demo providers remain:

- **`EchoProvider`** (`echo`) — returns the params it was given. The "hello world" capability.
- **`MockProvider`** (`mock-generate`) — simulates configurable delay and can be flipped to always fail, for exercising both the happy path and the error path.

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
# {"mock-generate": "mock", "echo": "echo", "generate-image": "pollinations", ...}
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

Your endpoint receives a `POST` with the full job record as JSON once the job reaches `ready` or `error`. Delivery makes up to 5 attempts with capped exponential backoff and full jitter (delay before retry *n* is uniform in `[0, min(30s, 1s·2ⁿ⁻¹)]` — randomized so a herd of jobs finishing during a receiver outage doesn't re-synchronize its retries onto the recovering receiver). The outcome is recorded on the job record as `webhook_status`: `"delivered"` after a 2xx, `"failed"` once every attempt is exhausted — the dead-letter signal, queryable via `GET /v1/jobs/{id}`. The job's own status in the store is already correct regardless of whether the webhook ever arrives, so a caller relying on polling as a fallback is never left with stale state.

`webhook_url` must be an absolute `http://` or `https://` URL — anything else (`file://`, `javascript:`, a bare hostname) is rejected with `422` at submission time, before it's ever stored or dialed. Point it at any local HTTP listener that logs incoming requests to inspect deliveries and exercise the retry behavior against a real server instead of a mock transport.

**Verifying deliveries are genuine.** Construct the `JobManager` with `webhook_signing_secret="..."` and every webhook POST carries an `X-Gateway-Signature: sha256=<hex>` header — the HMAC-SHA256 of the exact request body, keyed by that secret. Your receiver recomputes the same HMAC over the raw body and compares it (constant-time) to the header to confirm the request really came from this gateway and wasn't forged or altered in transit — the same shape Stripe- and GitHub-style webhook signing uses. Signing is opt-in and off by default; nothing about the documented payload shape changes when it's off.

### Idempotent submission

Pass an `Idempotency-Key` header on `POST /v1/{capability}` to make retrying a submission safe. If a request with the same key was already accepted, the *original* job's `{id, polling_url}` is returned and no second job is created or run — useful when your own retry logic (or a flaky network) might resend a submission whose response never arrived:

```bash
curl -X POST http://127.0.0.1:8000/v1/mock-generate \
  -H 'Idempotency-Key: order-4711-attempt-1' \
  -d '{"prompt": "a cat"}'
```

The dedupe window is bounded to the 10,000 most recent keys and lives in the job store: with `SQLiteJobStore` it **survives a restart** — a deploy mid-request is exactly when a lost response gets retried, so restart-volatile dedupe would fail at the moment it matters (with `InMemoryJobStore` it is as volatile as the jobs themselves). Omit the header and every submission is independent, exactly as before.

### Restart recovery

Background tasks die with the process, so a job still `pending`/`processing` in a persistent store at startup has nothing driving it anymore. On startup the server sweeps these and marks each `error` with an explicit "interrupted by a gateway restart; submit the job again" message (and fires its webhook, if any) instead of letting it read `processing` forever. Providers are deliberately **not** re-run automatically — whether a half-finished generation's side effects are safe to repeat is the caller's decision, made by resubmitting with a fresh idempotency key. The sweep also re-fires webhooks for jobs that finished but whose delivery outcome was never recorded (`webhook_status` still null) — the previous process died mid-delivery and the receiver may never have heard. Delivery is therefore **at-least-once**: a crash landing after the receiver's 2xx but before the outcome was recorded produces a duplicate on the next startup, so receivers should dedupe on the job `id`. And because the sweep treats every pending/processing row as ownerless, it assumes a **single gateway process** — starting a second process over the same SQLite file would fail the first one's live jobs (one more reason multi-process wants Postgres, per Limitations).

### Operability

`GET /v1/jobs?status=ready&capability=generate-image&limit=50` lists recent jobs newest first, filtered by either or both, and `GET /v1/stats` returns counts by status and by capability — the two things an operator glances at to see whether the queue is draining or one provider is failing. Both read the store's unpaginated `list()` and filter in memory, which is the honest shape for a reference store.

`GET /health` returns `{"status": "ok"}` and touches nothing but the running process — a liveness probe for a load balancer or orchestrator, independent of whatever `JobStore` backend is degraded or not.

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

## Security notes

This is a reference implementation exposed to whatever calls it, so it's worth being explicit about what's hardened and what deliberately isn't:

- **Request body size is capped** (1 MB by default, `create_app(manager, max_body_bytes=...)` to change it). Without this, `POST /v1/{capability}` would buffer an arbitrarily large body into memory before validating anything — a cheap denial-of-service vector. The body is now read as a capped stream, not via a single unbounded `request.json()`.
- **`webhook_url` is validated by scheme *and* by destination.** Submitting a job asks this server to make an outbound HTTP request to a caller-supplied URL later, on its own schedule — the textbook SSRF shape. Only `http://`/`https://` URLs with a host are accepted (`file://`, `gopher://` → `422`), and by default the hostname is resolved at submission time and rejected if any resolved address is loopback, private, link-local (that includes `169.254.169.254`, the cloud-metadata classic), reserved or multicast. The check fails closed: an unresolvable host is a `422`, because "could not check" must not become "allowed". The documented local-dev workflow — pointing `webhook_url` at a `webhook-sink` on 127.0.0.1 — opts back in with `serve --allow-private-webhooks` (or `create_app(..., allow_private_webhooks=True)`).
- **Known residue, on purpose:** the destination check runs once, at submission. A DNS name that answers public at validation and private at delivery (rebinding) is not caught — closing that requires resolution pinning inside the HTTP client, which a reference implementation should not hand-roll. A deployment that can't accept that residue should also enforce egress policy at its network boundary.
- **Optional API key.** Set `AJG_API_KEY` in the server's environment (it is deliberately not a CLI flag — argv leaks into process listings) and every `/v1/*` route requires `Authorization: Bearer <key>`, compared constant-time; `/health` stays open as a liveness probe. The bundled client speaks it too: `JobGatewayClient(url, api_key=...)` sends the header on every request, and `ai-job-gateway submit` reads the same `AJG_API_KEY` from its environment. Unset keeps the historical open behavior, which is acceptable only on the default `127.0.0.1` bind — without a key, anyone who can reach the port can submit compute and read every job's params and results.
- **Capability names are constrained** to 1–100 characters of `[A-Za-z0-9_-]`, rejected with `422` otherwise, so a malformed path segment fails fast and legibly rather than becoming an opaque "unknown capability" or an odd log line.
- **Webhook deliveries can be signed** (HMAC-SHA256, opt-in via `webhook_signing_secret`) so a receiver can verify a delivery genuinely came from this gateway and wasn't forged or tampered with — see [Webhooks](#webhooks) above.
- **Still absent:** authentication and rate limiting. Anyone who can reach this server can submit jobs and read any job's result by id (job ids are unguessable UUIDs, but there's no ownership check). A public deployment needs API keys and per-key quotas before this is safe to expose — see the roadmap below.

## Roadmap — what a production deployment would add

This is a reference implementation; it's honest about what it isn't:

- **Real job queue.** The in-process `asyncio.create_task` model is fine for one server process. Real traffic needs a real queue (Redis/RQ, Celery, or dispatching to serverless GPU workers) so job execution survives a server restart and scales past one machine.
- **Auth & rate limiting.** There is none. A public deployment needs API keys and per-key quotas before this is safe to expose.
- **Real providers.** `EchoProvider`/`MockProvider` prove the contract; a real deployment implements `Provider` for actual backends (FLUX, Wan2.2, MuseTalk, whatever).
- **Multi-worker/multi-process coordination.** `InMemoryJobStore` doesn't share state across processes; `SQLiteJobStore` survives a restart and takes its write locks eagerly (`BEGIN IMMEDIATE`, so a second process queues on `busy_timeout` instead of deadlock-aborting), but SQLite is still a single-writer database. A real deployment at that scale wants Postgres (or similar) behind the same `JobStore` interface.
- **DNS-rebinding-proof webhook delivery.** See Security notes above — submission-time resolution is checked; pinning resolution at delivery time is left to deployments that need it.
- **Adaptive batching / multi-stage pipelines.** Out of scope here — see this project's sibling research notes on generative-AI infrastructure patterns for where that fits in a larger system.

## License

MIT — see [LICENSE](LICENSE).
