"""Command-line entry point.

``ai-job-gateway serve`` runs the reference server with the demo registry
(``mock-generate``, ``echo``). ``ai-job-gateway submit`` is a thin convenience
for a quick manual check against a running server -- not the main deliverable,
just a fast way to poke at things without writing a Python script.
"""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import os
import sys


def _is_loopback_bind(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _serve(args: argparse.Namespace) -> None:
    import uvicorn

    from .manager import JobManager
    from .providers import default_registry
    from .server import create_app
    from .store import InMemoryJobStore, SQLiteJobStore

    from datetime import timedelta

    api_key = os.environ.get("AJG_API_KEY") or None
    if api_key is None and not _is_loopback_bind(args.host):
        print(
            f"warning: serving on {args.host} with no AJG_API_KEY set -- anyone who can "
            "reach this port can submit jobs and read every job's params and results. "
            "Set AJG_API_KEY or put the gateway behind your own auth.",
            file=sys.stderr,
        )

    store = SQLiteJobStore(args.db) if args.db else InMemoryJobStore()
    manager = JobManager(
        store,
        default_registry(),
        # Secrets come from the environment, never argv: process listings
        # and shell history are not places for a credential.
        webhook_signing_secret=os.environ.get("AJG_WEBHOOK_SECRET") or None,
        allow_private_webhooks=args.allow_private_webhooks,
        job_timeout=timedelta(seconds=args.job_timeout) if args.job_timeout > 0 else None,
        max_concurrent_jobs=args.max_concurrent_jobs if args.max_concurrent_jobs > 0 else None,
        max_concurrent_webhooks=(
            args.max_concurrent_webhooks if args.max_concurrent_webhooks > 0 else None
        ),
    )
    app = create_app(
        manager,
        allow_private_webhooks=args.allow_private_webhooks,
        api_key=api_key,
    )
    uvicorn.run(app, host=args.host, port=args.port)


def _submit(args: argparse.Namespace) -> None:
    import httpx

    from .client import JobGatewayClient
    from .exceptions import JobExpiredError, JobFailedError, JobSubmissionError

    async def run() -> None:
        try:
            params = json.loads(args.params)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"params must be valid JSON: {exc}")
        if not isinstance(params, dict):
            raise SystemExit("params must be a JSON object, e.g. '{\"prompt\": \"hi\"}'")

        async with JobGatewayClient(
            args.url, api_key=os.environ.get("AJG_API_KEY") or None
        ) as client:
            handle = await client.submit(args.capability, params)
            print(f"submitted job {handle.job_id} -> polling {handle.polling_url}")
            result = await handle.wait(timeout=args.timeout)
            print(json.dumps(result, indent=2))

    # One readable line per failure and exit 1, never a traceback: the usual
    # first mistakes are "no server running" and a mistyped capability.
    try:
        asyncio.run(run())
    except httpx.TransportError as exc:
        raise SystemExit(
            f"error: cannot reach a gateway at {args.url} ({type(exc).__name__}). "
            "Start one in another terminal with: ai-job-gateway serve"
        )
    except JobSubmissionError as exc:
        hint = ""
        if exc.status_code == 404:
            hint = f"\nlist the capabilities this server has: curl {args.url}/v1/capabilities"
        elif exc.status_code == 401:
            hint = "\nthe server wants a key: set AJG_API_KEY in this shell"
        try:  # the server answers {"detail": "..."}; show just the sentence
            detail = json.loads(exc.message)["detail"]
        except (ValueError, KeyError, TypeError):
            detail = exc.message
        raise SystemExit(f"error: the gateway rejected the job (HTTP {exc.status_code}): {detail}{hint}")
    except (JobFailedError, JobExpiredError) as exc:
        raise SystemExit(f"error: {exc}")
    except TimeoutError as exc:
        raise SystemExit(f"error: {exc}. The job is still running on the server; raise --timeout to wait longer.")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ai-job-gateway",
        description=(
            "Submit a job, get an id back at once, poll or get a webhook when it is done: "
            "a small self-hosted server (and a client) for the async job contract."
        ),
        epilog=(
            "quick start (two terminals):\n"
            "  ai-job-gateway serve\n"
            "  ai-job-gateway submit echo '{\"prompt\": \"hello\"}'\n"
            "\n"
            "environment: AJG_API_KEY (require / send a bearer key), "
            "AJG_WEBHOOK_SECRET (sign webhook deliveries).\n"
            "'serve --help' and 'submit --help' list every option."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser(
        "serve",
        help="run the reference HTTP server",
        description="Run the HTTP server (interactive docs at /docs). Capabilities: echo and "
        "mock-generate run locally with no key; generate-image is hosted and sends the prompt "
        "off-machine; media-* appear when the 'media' extra is installed.",
        epilog="example: ai-job-gateway serve --db jobs.db   (keep jobs across restarts)",
    )
    serve_parser.add_argument("--host", default="127.0.0.1", help="address to bind (default: 127.0.0.1)")
    serve_parser.add_argument("--port", type=int, default=8000, help="port to listen on (default: 8000)")
    serve_parser.add_argument(
        "--allow-private-webhooks",
        action="store_true",
        help=(
            "permit webhook_url targets that resolve to loopback/private "
            "addresses (the local webhook-sink workflow); off by default "
            "because the server POSTs the full job record there"
        ),
    )
    serve_parser.add_argument(
        "--db",
        default=None,
        help="path to a SQLite file for job persistence (default: in-memory, lost on restart)",
    )
    serve_parser.add_argument(
        "--job-timeout",
        type=float,
        default=600.0,
        help=(
            "seconds one provider run may take before the job is failed with an "
            "honest error instead of sitting 'processing' forever (0 disables; "
            "default: 600)"
        ),
    )
    serve_parser.add_argument(
        "--max-concurrent-jobs",
        type=int,
        default=100,
        help=(
            "how many provider runs may execute at once; jobs beyond the cap "
            "queue as 'pending' (0 removes the cap; default: 100)"
        ),
    )
    serve_parser.add_argument(
        "--max-concurrent-webhooks",
        type=int,
        default=10,
        help=(
            "how many webhook POSTs may be in flight at once across all jobs; "
            "a restart that recovers many unheard deliveries fires them in "
            "waves of this size instead of all at once (0 removes the cap; "
            "default: 10)"
        ),
    )
    serve_parser.set_defaults(func=_serve)

    submit_parser = subparsers.add_parser(
        "submit",
        help="submit a job to a running server and wait for the result",
        description="Submit one job to a running server, poll until it finishes and print "
        "the result as JSON. Exit 0 on success, 1 on any failure.",
        epilog="example: ai-job-gateway submit echo '{\"prompt\": \"hello\"}'",
    )
    submit_parser.add_argument(
        "capability", help="what to run, e.g. echo or mock-generate (see GET /v1/capabilities)"
    )
    submit_parser.add_argument("params", help='JSON object of params, e.g. \'{"prompt": "hi"}\'')
    submit_parser.add_argument(
        "--url", default="http://127.0.0.1:8000", help="gateway address (default: %(default)s)"
    )
    submit_parser.add_argument(
        "--timeout", type=float, default=60.0, help="seconds to wait for the result (default: %(default)s)"
    )
    submit_parser.set_defaults(func=_submit)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
