"""Transport failures must not masquerade as unsupported runtime capabilities."""

from unittest.mock import Mock

import httpx
import pytest

from adx_environment import ConnectionConfig
from adx_environment import _transport
from adx_environment._transport import (
    EnvironmentClient,
    EnvironmentError,
    EnvironmentHTTPError,
)
from adx_environment.commands import Commands, UnsupportedFeature


@pytest.mark.parametrize(
    "error",
    [
        EnvironmentHTTPError(
            504,
            {},
            "node proxy CONNECT timed out",
            request_id="invoke-timeout",
            code="OUTCOME_UNKNOWN",
            retry="same_operation",
            outcome="unknown",
        ),
        httpx.ConnectTimeout("capability transport timeout"),
        EnvironmentError("environment OOM", code="ENVIRONMENT_OOM", outcome="terminal"),
        EnvironmentHTTPError(403, {}, "permission denied", code="PERMISSION_DENIED"),
    ],
)
def test_capability_transport_error_keeps_original_type_and_metadata(error):
    client = Mock(spec=EnvironmentClient)
    client.invoke.side_effect = error
    commands = Commands(client, "environment-a")
    with pytest.raises(type(error)) as raised:
        commands._require_recovery_capability()
    assert raised.value is error
    assert not commands._capabilities_checked
    client.invoke.assert_called_once_with("environment-a", "process.capabilities", {})


def test_capability_missing_in_confirmed_response_remains_unsupported():
    client = Mock(spec=EnvironmentClient)
    client.invoke.return_value = {"protocol_version": 1, "capabilities": []}
    with pytest.raises(UnsupportedFeature):
        Commands(client, "environment-a")._require_recovery_capability()


@pytest.fixture(name="make_client")
def client_factory():
    clients = []

    def factory(handler):
        client = EnvironmentClient(
            connection=ConnectionConfig(server_address="frontend:8889", token="test-token")
        )
        clients.append(client)
        client._http.close()
        client._http = httpx.Client(transport=httpx.MockTransport(handler))
        return client

    yield factory
    for client in clients:
        client.close()


def test_retryable_http_wrapper_keeps_first_error():
    first = httpx.ReadTimeout("first failed attempt")
    original = EnvironmentHTTPError(503, {}, "later failure", first_error=first)
    wrapped = _transport._RetryableHTTPStatus(original)
    assert wrapped.first_error is first


def service_error(status, message, request, *, terminal=False):
    return httpx.Response(
        status,
        json={
            "code": status,
            "message": message,
            "error": {
                "code": "CONFLICT" if terminal else "UNAVAILABLE",
                "retry": "never" if terminal else "same_operation",
                "outcome": "terminal" if terminal else "unknown",
                "requestId": request.headers["x-request-id"],
                "environmentId": "environment-a",
            },
        },
    )


def test_create_terminal_retry_preserves_first_failure_and_final_contract(monkeypatch, make_client):
    monkeypatch.setattr(_transport.time, "sleep", lambda _: None)
    seen = []

    def handler(request):
        seen.append(request.headers["x-request-id"])
        if len(seen) == 1:
            return service_error(503, "runtime inventory deadline exceeded", request)
        return service_error(
            409, "environment create has a terminal result", request, terminal=True
        )

    client = make_client(handler)
    with pytest.raises(EnvironmentHTTPError) as raised:
        client.create_info({"name": "environment-a", "createTimeoutSeconds": 600})
    error = raised.value
    assert error.status_code == 409
    assert error.code == "CONFLICT"
    assert error.retry == "never" and error.outcome == "terminal"
    assert error.first_error.status_code == 503
    assert "runtime inventory deadline exceeded" in str(error.first_error)
    assert error.first_error.request_id == error.request_id == seen[0]
    assert len(seen) == 2 and len(set(seen)) == 1


def test_create_retry_exhaustion_keeps_first_response(monkeypatch, make_client):
    monkeypatch.setattr(_transport.time, "sleep", lambda _: None)
    seen = []

    def handler(request):
        seen.append(request.headers["x-request-id"])
        return service_error(
            503, "first failure" if len(seen) == 1 else "last failure", request
        )

    with pytest.raises(EnvironmentHTTPError) as raised:
        make_client(handler).create_info({"name": "environment-a"})
    assert len(seen) == 3 and len(set(seen)) == 1
    assert "last failure" in str(raised.value)
    assert "first failure" in str(raised.value.first_error)
    assert raised.value.first_error.outcome == "unknown"


def test_create_transport_then_terminal_keeps_transport_cause(monkeypatch, make_client):
    monkeypatch.setattr(_transport.time, "sleep", lambda _: None)
    first = httpx.ReadError("SSE connection lost")
    attempts = []

    def handler(request):
        attempts.append(request)
        if len(attempts) == 1:
            raise first
        return service_error(403, "permission denied", request, terminal=True)

    with pytest.raises(EnvironmentHTTPError) as raised:
        make_client(handler).create_info({"name": "environment-a"})
    assert raised.value.status_code == 403
    assert raised.value.first_error is first
    assert len(attempts) == 2


def test_create_initial_terminal_failure_has_no_previous_failure(make_client):
    with pytest.raises(EnvironmentHTTPError) as raised:
        make_client(
            lambda r: service_error(409, "terminal", r, terminal=True)
        ).create_info({"name": "environment-a"})
    assert raised.value.first_error is None


@pytest.mark.parametrize("terminal", [False, True])
def test_capability_invoke_retries_keep_first_gateway_error(monkeypatch, terminal, make_client):
    monkeypatch.setattr(_transport.time, "sleep", lambda _: None)
    attempts = []

    def handler(request):
        if request.url.path == "/api/instances":
            return httpx.Response(
                200, json={"id": "environment-a", "status": "running"}
            )
        attempts.append(request)
        status = 403 if terminal and len(attempts) > 1 else 504
        return httpx.Response(
            status,
            text="first CONNECT timeout" if len(attempts) == 1 else "last failure",
        )

    client = make_client(handler)
    with pytest.raises(EnvironmentError) as raised:
        Commands(client, "environment-a")._require_recovery_capability()
    error = raised.value
    assert not isinstance(error, UnsupportedFeature)
    assert error.first_error.status_code == 504
    assert "first CONNECT timeout" in str(error.first_error)
    request_ids = [r.headers["x-adx-request-id"] for r in attempts]
    assert len(set(request_ids)) == 1
    assert error.first_error.request_id == error.request_id
    if terminal:
        assert error.status_code == 403 and len(attempts) == 2
    else:
        assert error.outcome == "unknown" and error.retry == "same_operation"
        assert len(attempts) == 3
