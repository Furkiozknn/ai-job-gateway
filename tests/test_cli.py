"""`ai-job-gateway serve` wiring: what the environment turns on."""

from __future__ import annotations

import sys

import pytest

from ai_job_gateway import cli


@pytest.fixture
def served(monkeypatch):
    captured = {}

    def fake_run(app, host, port):
        captured.update(app=app, host=host, port=port)

    import uvicorn

    monkeypatch.setattr(uvicorn, "run", fake_run)
    monkeypatch.delenv("AJG_API_KEY", raising=False)
    monkeypatch.delenv("AJG_WEBHOOK_SECRET", raising=False)

    def serve(*argv):
        monkeypatch.setattr(sys, "argv", ["ai-job-gateway", "serve", *argv])
        cli.main()
        return captured["app"].state.manager

    return serve


def test_webhook_signing_secret_comes_from_the_environment(served, monkeypatch):
    monkeypatch.setenv("AJG_WEBHOOK_SECRET", "s3cret")
    manager = served()
    assert manager._webhook_signing_secret == "s3cret"


def test_signing_is_off_without_the_variable(served):
    assert served()._webhook_signing_secret is None


def test_allow_private_webhooks_reaches_the_manager(served):
    assert served().allow_private_webhooks is False
    assert served("--allow-private-webhooks").allow_private_webhooks is True


def test_public_bind_without_a_key_warns(served, capsys):
    served("--host", "0.0.0.0")
    assert "no AJG_API_KEY" in capsys.readouterr().err


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "::1"])
def test_loopback_bind_does_not_warn(served, capsys, host):
    served("--host", host)
    assert capsys.readouterr().err == ""


def test_public_bind_with_a_key_does_not_warn(served, monkeypatch, capsys):
    monkeypatch.setenv("AJG_API_KEY", "k")
    served("--host", "0.0.0.0")
    assert capsys.readouterr().err == ""


# --- `submit`: failures are one readable line and exit 1, never a traceback ---


def _run_submit(monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["ai-job-gateway", "submit", *argv])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    return str(exc.value.code)


def test_submit_without_a_server_says_how_to_start_one(monkeypatch):
    import socket

    with socket.socket() as s:  # a loopback port nothing listens on
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    msg = _run_submit(monkeypatch, "echo", "{}", "--url", f"http://127.0.0.1:{port}")
    assert "cannot reach a gateway" in msg and "ai-job-gateway serve" in msg


def test_submit_unknown_capability_points_at_the_capability_list(monkeypatch):
    from ai_job_gateway import JobGatewayClient, JobSubmissionError

    async def reject(self, capability, params, **kw):
        raise JobSubmissionError(404, '{"detail":"unknown capability: \'nope\'"}')

    monkeypatch.setattr(JobGatewayClient, "submit", reject)
    msg = _run_submit(monkeypatch, "nope", '{"a": 1}')
    assert "HTTP 404" in msg and "/v1/capabilities" in msg
    assert "unknown capability: 'nope'" in msg and '{"detail"' not in msg


def test_submit_timeout_is_a_message_not_a_traceback(monkeypatch):
    from ai_job_gateway import JobGatewayClient

    async def slow(self, capability, params, **kw):
        raise TimeoutError("job x did not finish within 1s")

    monkeypatch.setattr(JobGatewayClient, "submit", slow)
    assert "raise --timeout" in _run_submit(monkeypatch, "echo", '{"a": 1}')


def test_help_shows_a_quick_start(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["ai-job-gateway", "--help"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "quick start" in out and "AJG_API_KEY" in out
