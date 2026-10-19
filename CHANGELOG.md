# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- `gateway_poll.parse_submission` no longer uses `assert` (skipped under
  `python -O`) or a bare `KeyError` to validate a successful submission
  response. It raises the new `GatewayContractError` (a `ValueError`),
  naming the missing field. `JobGatewayClient.submit()` turns it, and a
  non-JSON 2xx body, into `JobSubmissionError`.

### Added
- Bounded poll retry: `JobHandle.poll()` retries a network error or a
  502/503/504 up to three times with backoff (0.5 s, 1 s, 2 s). The policy
  (`is_transient_poll_status`, `poll_retry_delays`) is in `gateway_poll.py`,
  so the vendored copies share it.
- Job cancellation: `POST /v1/jobs/{id}/cancel` stops a `pending` or
  `processing` job (`200` with the record, `409` once the job has finished,
  `404` for an unknown id). A pending job never starts; a running provider's
  task is cancelled. The job reads `error` with the fixed message
  `CANCELLED_ERROR`, so the public status set is unchanged, and its webhook
  fires once. A provider that ignores the cancellation is recorded as
  cancelled after `CANCEL_GRACE_SECONDS` (5 s) and its late result is
  discarded. Also `JobManager.cancel()`, `JobGatewayClient.cancel()`,
  `JobHandle.cancel()`, `JobGatewayClient.handle(job_id)`, the
  `JobNotCancellableError` exception and an `ai-job-gateway cancel` command.
- `GET /metrics` in the Prometheus text format, with no new dependency.
  It covers jobs created per capability, queue wait and provider run time
  histograms (the run time split by `ready` / `error` / `cancelled`),
  provider calls in flight, webhook outcomes, and jobs by status read from
  the store. It needs the bearer key when `AJG_API_KEY` is set. In Python
  it is `GatewayMetrics` and `JobManager(metrics=...)`.

### Changed
- An `Idempotency-Key` reused with a different request (capability, params
  or `webhook_url`) is now rejected with `422` instead of silently returning
  the original, unrelated job. A SHA-256 fingerprint of the canonical request
  JSON is stored with the key (`SQLiteJobStore` adds a `fingerprint` column
  to an existing database on open). Keys remembered before this change are
  compared against their original job record. `JobManager.submit()` raises
  the new `IdempotencyKeyReusedError`; `JobStore` gains two non-abstract
  methods, `remember_idempotency_fingerprint` and
  `recall_idempotency_fingerprint`, so custom stores keep working unchanged.
- The provider call runs as its own asyncio task inside the job's task, so a
  cancellation stops exactly the provider and never a store write in flight.

## [0.1.0] - 2026-09-25

First tagged release. Everything below is in it.

### Contract
- `POST /v1/{capability}` returns `202 {id, polling_url}` at once; the
  provider runs in the background. `GET /v1/jobs/{id}` reports `pending`,
  `processing`, `ready`, `error`, and `410` once the result window has passed
  (`expired`, computed at read time, never a silent 404).
- `GET /v1/jobs` (filters, newest first), `GET /v1/stats`,
  `GET /v1/capabilities`, `GET /health`.
- Optional webhook on completion: 5 attempts, capped exponential backoff
  with full jitter, the outcome recorded as `webhook_status`
  (`delivered` / `failed`).
- `Idempotency-Key` on submit, stored in the job store, so with
  `SQLiteJobStore` it survives a restart. Concurrent submits carrying the
  same key produce one job.
- Restart sweep: jobs left `pending`/`processing` by a dead process are
  marked `error`, and webhooks whose outcome was never recorded are sent
  again (at-least-once delivery).

### Providers
- `echo`, `mock-generate` (local, no key), `generate-image` (hosted,
  Pollinations.ai, no key), and `media-*` local capabilities through the
  optional `media` extra (mini-creative-toolkit).

### Security
- Request bodies capped (1 MB default), capability names constrained,
  `Idempotency-Key` capped at 255 characters.
- Optional shared API key (`AJG_API_KEY`), compared in constant time.
  `serve` warns when it binds a non-loopback address without one.
- `webhook_url` must be http(s) and resolve only to public addresses,
  checked at submission, then checked again at connect time with the
  connection pinned to the checked address. This blocks DNS rebinding and
  also covers the restart sweep's re-sends. Multicast, IPv4-mapped IPv6
  and NAT64 forms of private addresses are refused. Redirects are not
  followed.
- Webhook signing (`AJG_WEBHOOK_SECRET`): `X-Gateway-Signature-V2` binds
  `X-Gateway-Timestamp` into the HMAC so receivers can reject replays, and
  `verify_webhook()` does the receiver-side check. The body-only
  `X-Gateway-Signature` is still sent for existing receivers.

### Tooling
- `ai-job-gateway serve` / `submit` CLI, the `JobGatewayClient` Python
  client, and the vendored `gateway_poll.py` contract helper.
- CI runs the test suite on Python 3.11, 3.12 and 3.13, runs it both with and
  without the `media` extra, and builds and checks the package. The suite
  refuses any real network access.

[0.1.0]: https://github.com/Furkiozknn/ai-job-gateway/releases/tag/v0.1.0
