# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

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
