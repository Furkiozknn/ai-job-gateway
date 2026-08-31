from __future__ import annotations

import pytest

from ai_job_gateway.providers import EchoProvider, MockProvider, Provider, ProviderError


@pytest.mark.asyncio
async def test_echo_provider_returns_params_unchanged():
    provider = EchoProvider()
    result = await provider.run("job-1", {"a": 1, "b": "two"})
    assert result == {"echoed": {"a": 1, "b": "two"}}


@pytest.mark.asyncio
async def test_mock_provider_succeeds_by_default():
    provider = MockProvider(delay_seconds=0)
    result = await provider.run("job-1", {"prompt": "hi"})
    assert result["job_id"] == "job-1"
    assert result["params_received"] == {"prompt": "hi"}


@pytest.mark.asyncio
async def test_mock_provider_raises_provider_error_when_configured_to_fail():
    provider = MockProvider(delay_seconds=0, should_fail=True, failure_message="on purpose")
    with pytest.raises(ProviderError, match="on purpose"):
        await provider.run("job-1", {})


@pytest.mark.asyncio
async def test_mock_provider_set_should_fail_flips_behavior_mid_life():
    provider = MockProvider(delay_seconds=0)
    ok = await provider.run("job-1", {})
    assert "output" in ok

    provider.set_should_fail(True, "now it fails")
    with pytest.raises(ProviderError, match="now it fails"):
        await provider.run("job-2", {})

    provider.set_should_fail(False)
    ok_again = await provider.run("job-3", {})
    assert "output" in ok_again


def test_provider_subclass_defaults_name_to_class_name():
    class MyCustomProvider(Provider):
        async def run(self, job_id, params):
            return {}

    assert MyCustomProvider.name == "MyCustomProvider"


def test_provider_subclass_can_override_name():
    class WithExplicitName(Provider):
        name = "custom-name"

        async def run(self, job_id, params):
            return {}

    assert WithExplicitName.name == "custom-name"


def test_provider_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        Provider()  # abstract - no run() implementation
