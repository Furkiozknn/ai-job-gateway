"""Command-line entry point.

``ai-job-gateway serve`` runs the reference server with the demo registry
(``mock-generate``, ``echo``). ``ai-job-gateway submit`` is a thin convenience
for a quick manual check against a running server -- not the main deliverable,
just a fast way to poke at things without writing a Python script.
"""

from __future__ import annotations

import argparse
import asyncio
import json


def _serve(args: argparse.Namespace) -> None:
    import uvicorn

    from .manager import JobManager
    from .providers import default_registry
    from .server import create_app
    from .store import InMemoryJobStore, SQLiteJobStore

    store = SQLiteJobStore(args.db) if args.db else InMemoryJobStore()
    manager = JobManager(store, default_registry())
    app = create_app(manager)
    uvicorn.run(app, host=args.host, port=args.port)


def _submit(args: argparse.Namespace) -> None:
    from .client import JobGatewayClient

    async def run() -> None:
        try:
            params = json.loads(args.params)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"params must be valid JSON: {exc}")
        if not isinstance(params, dict):
            raise SystemExit("params must be a JSON object, e.g. '{\"prompt\": \"hi\"}'")

        async with JobGatewayClient(args.url) as client:
            handle = await client.submit(args.capability, params)
            print(f"submitted job {handle.job_id} -> polling {handle.polling_url}")
            result = await handle.wait(timeout=args.timeout)
            print(json.dumps(result, indent=2))

    asyncio.run(run())


def main() -> None:
    parser = argparse.ArgumentParser(prog="ai-job-gateway")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="run the reference HTTP server")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)
    serve_parser.add_argument(
        "--db",
        default=None,
        help="path to a SQLite file for job persistence (default: in-memory, lost on restart)",
    )
    serve_parser.set_defaults(func=_serve)

    submit_parser = subparsers.add_parser(
        "submit", help="submit a job to a running server and wait for the result"
    )
    submit_parser.add_argument("capability")
    submit_parser.add_argument("params", help='JSON object of params, e.g. \'{"prompt": "hi"}\'')
    submit_parser.add_argument("--url", default="http://127.0.0.1:8000")
    submit_parser.add_argument("--timeout", type=float, default=60.0)
    submit_parser.set_defaults(func=_submit)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
