"""The vendored gateway_poll module: submission parsing and poll-retry policy.

The same tests live in every repo that carries a copy of gateway_poll.py,
so a copy that drifts fails here as well as in the vendor check.
"""

from __future__ import annotations

import pytest

from ai_job_gateway.gateway_poll import (
    POLL_RETRY_ATTEMPTS,
    GatewayContractError,
    GatewayHTTPError,
    is_transient_poll_status,
    parse_submission,
    poll_retry_delays,
)


def test_parse_submission_returns_id_and_polling_url():
    body = {"id": "j1", "polling_url": "/v1/jobs/j1"}
    assert parse_submission(202, body, "") == ("j1", "/v1/jobs/j1")


def test_parse_submission_raises_http_error_for_a_rejection():
    with pytest.raises(GatewayHTTPError) as info:
        parse_submission(422, None, '{"detail": "bad"}')
    assert info.value.status_code == 422
    assert info.value.body_text == '{"detail": "bad"}'


@pytest.mark.parametrize("body", [None, [], "text", 3])
def test_a_success_without_a_json_object_is_a_contract_error_not_an_assert(body):
    # This used to be `assert body_json is not None`: an AssertionError with
    # no detail, and no check at all under `python -O`.
    with pytest.raises(GatewayContractError) as info:
        parse_submission(202, body, "raw body")
    assert info.value.field is None
    assert info.value.status_code == 202
    assert "not a JSON object" in str(info.value)
    assert "raw body" in str(info.value)


@pytest.mark.parametrize("missing", ["id", "polling_url"])
def test_a_missing_field_is_named_instead_of_a_bare_key_error(missing):
    body = {"id": "j1", "polling_url": "/v1/jobs/j1"}
    del body[missing]
    with pytest.raises(GatewayContractError) as info:
        parse_submission(202, body, "{}")
    assert info.value.field == missing
    assert f"missing {missing!r}" in str(info.value)


def test_contract_error_is_a_value_error_and_not_a_rejection():
    # Wrappers that map GatewayHTTPError to "submission rejected" must not
    # report a malformed 2xx that way.
    assert issubclass(GatewayContractError, ValueError)
    assert not issubclass(GatewayContractError, GatewayHTTPError)


@pytest.mark.parametrize("status", [502, 503, 504])
def test_gateway_and_proxy_unavailability_is_transient(status):
    assert is_transient_poll_status(status)


@pytest.mark.parametrize("status", [200, 202, 400, 404, 410, 422, 500])
def test_everything_else_is_not_retried(status):
    assert not is_transient_poll_status(status)


def test_retry_delays_are_bounded_and_grow():
    delays = poll_retry_delays()
    assert len(delays) == POLL_RETRY_ATTEMPTS
    assert delays == sorted(delays)
    assert delays[0] > 0
    assert sum(delays) < 10


def test_retry_delays_are_capped_and_can_be_disabled():
    assert poll_retry_delays(6, base=1.0, cap=3.0) == [1.0, 2.0, 3.0, 3.0, 3.0, 3.0]
    assert poll_retry_delays(0) == []
