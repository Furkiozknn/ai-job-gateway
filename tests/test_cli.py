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
