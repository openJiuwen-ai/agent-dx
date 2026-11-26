"""The direct invoke outcome accounts for every attempt of the operation."""

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

from adx_environment import _transport
from adx_environment._transport import EnvironmentClient, EnvironmentError as SDKEnvironmentError, EnvironmentHTTPError
from adx_environment.commands import CommandHandle
from adx_environment.types import ConnectionConfig


@pytest.fixture(name="setup_client")
def _setup_client(monkeypatch):
    def setup(handler, attempts):
        http_client = httpx.Client(transport=httpx.MockTransport(handler))
        monkeypatch.setattr(_transport, "acquire_shared_http_client", lambda *_: http_client)
        return EnvironmentClient(connection=ConnectionConfig(
            "gateway.invalid:443", "fixture-token", request_max_attempts=attempts,
        ))

    return setup


def connect_timeout(request_id, *, marker=True, response_id=None, status_code=504, **fields):
    headers = {"X-Request-Id": response_id if response_id is not None else request_id}
    if marker:
        headers["X-ADX-Error-Code"] = "RELAY_CONNECT_TIMEOUT"
    return httpx.Response(status_code, headers=headers, json={"error": {
        "code": "RELAY_CONNECT_TIMEOUT",
        "message": "Relay timed out connecting to the environment",
        "retry": "same_operation",
        "outcome": "not_started",
        "requestForwarded": False,
        "requestId": request_id,
        **fields,
    }})


@pytest.mark.parametrize(("faults", "confirmed"), [
    (("confirmed",), True),
    (("confirmed", "confirmed"), True),
    (("connect", "confirmed"), True),
    (("confirmed", "connect"), True),
    (("lost",), False),
    (("lost", "confirmed"), False),
    (("confirmed", "lost"), False),
    (("legacy",), False),
    (("legacy", "confirmed"), False),
    (("route_miss", "confirmed"), False),
    (("confirmed", "route_miss"), False),
    (("unmarked",), False),
    (("forged_never",), False),
    (("wrong_status",), False),
    (("wrong_response_id",), False),
    (("wrong_body_id",), False),
    (("integer_flag",), False),
])
def test_direct_connect_outcome(setup_client, faults, confirmed):
    attempts = []
    lost = httpx.ReadError("connection closed after forwarding")

    def handler(request):
        if request.url.path == "/api/instances":
            return httpx.Response(200, json=[{"id": "owned", "status": "running"}])
        assert request.url.path == "/direct/owned/invoke"
        operation = json.loads(request.read())
        request_id = request.headers["X-ADX-Request-ID"]
        assert operation["requestId"] == request_id
        assert operation["action"] == "process.start"
        fault = faults[len(attempts)]
        attempts.append(request_id)
        if fault == "connect":
            raise httpx.ConnectError("could not connect to ingress", request=request)
        if fault == "lost":
            raise lost
        if fault == "legacy":
            return httpx.Response(504, text="node proxy rejected CONNECT: 504 Gateway Timeout")
        if fault == "route_miss":
            return httpx.Response(404, text="route not found")
        if fault == "unmarked":
            return connect_timeout(request_id, marker=False)
        if fault == "forged_never":
            return connect_timeout(request_id, marker=False, retry="never")
        if fault == "wrong_status":
            return connect_timeout(request_id, status_code=400)
        if fault == "wrong_response_id":
            return connect_timeout(request_id, response_id="other-request")
        if fault == "wrong_body_id":
            return connect_timeout("other-request", response_id=request_id)
        if fault == "integer_flag":
            return connect_timeout(request_id, requestForwarded=0)
        return connect_timeout(request_id)

    client = setup_client(handler, len(faults))
    try:
        with pytest.raises(SDKEnvironmentError) as raised:
            client.invoke("owned", "process.start", {"command_id": "existing-operation"})
    finally:
        client.close()

    error = raised.value
    assert len(attempts) == len(faults)
    assert set(attempts) == {error.request_id} == {error.operation_id}
    assert error.environment_id == "owned"
    assert error.outcome == ("not_started" if confirmed else "unknown")
    if confirmed:
        assert isinstance(error, EnvironmentHTTPError)
        assert error.status_code == 504
        assert error.code == "RELAY_CONNECT_TIMEOUT"
        assert error.retry == "same_operation"
        assert error.request_forwarded is False
    else:
        assert error.code == "OUTCOME_UNKNOWN"
        assert error.request_forwarded is None
    if faults[0] == "lost":
        assert error.first_error is lost
    if faults == ("lost", "confirmed"):
        assert isinstance(error.__cause__, EnvironmentHTTPError)
        assert error.__cause__.request_forwarded is False


def test_retries_share_the_original_processing_and_transport_deadlines(monkeypatch, setup_client):
    now = 100.0
    budgets = []
    ids = []

    def advance(seconds):
        nonlocal now
        now += seconds

    monkeypatch.setattr(_transport, "time", SimpleNamespace(monotonic=lambda: now, sleep=advance))

    def handler(request):
        if request.method == "GET":
            raise AssertionError("a timed-out Relay cannot resolve an earlier ambiguous attempt")
        request_id = request.headers["X-ADX-Request-ID"]
        ids.append(request_id)
        budgets.append((
            int(request.headers["X-ADX-Processing-Timeout-Ms"]),
            request.extensions["timeout"]["read"],
        ))
        advance(0.4)
        if len(budgets) == 1:
            raise httpx.ReadError("lost response", request=request)
        return connect_timeout(request_id)

    client = setup_client(handler, 3)
    try:
        with pytest.raises(SDKEnvironmentError) as raised:
            client.invoke("owned", "process.get", {"command_id": "active"}, timeout=1)
    finally:
        client.close()
    assert raised.value.outcome == "unknown"
    assert set(ids) == {raised.value.operation_id}
    assert budgets == [(1000, 31.0), (500, 30.5)]


@pytest.mark.parametrize("use_async", [False, True])
def test_initial_command_wait_exposes_status_read_outcome(setup_client, use_async):
    actions = []

    def handler(request):
        args = json.loads(request.read())
        actions.append(args["action"])
        assert args["args"] == {"command_id": "existing-command"}
        return connect_timeout(args["requestId"])

    client = setup_client(handler, 1)
    handle = CommandHandle("existing-command", client, "owned")
    try:
        with pytest.raises(EnvironmentHTTPError) as raised:
            if use_async:
                asyncio.run(handle.wait_async(timeout=1))
            else:
                handle.wait(timeout=1)
    finally:
        client.close()
    assert raised.value.code == "RELAY_CONNECT_TIMEOUT"
    assert raised.value.outcome == "not_started"
    assert raised.value.request_forwarded is False
    assert actions == ["process.get"]
