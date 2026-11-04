"""Failed data routes must not become unsupported control-plane operations."""

import json
from unittest.mock import Mock

import httpx
import pytest

from adx_environment import _transport
from adx_environment._transport import (
    EnvironmentClient,
    EnvironmentError as SDKEnvironmentError,
    EnvironmentHTTPError,
)


def client_for(monkeypatch, handler):
    monkeypatch.setattr(_transport.time, "sleep", lambda _: None)
    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(_transport, "acquire_shared_http_client", Mock(return_value=http_client))
    return EnvironmentClient(server="route.invalid:443", token="fixture-token")


@pytest.mark.parametrize("fault", ["missing-route", "connect"])
@pytest.mark.parametrize(
    ("failure", "expected_code"),
    [
        ({"reason": "OomKilled", "exit_code": 137}, "ENVIRONMENT_OOM"),
        ({"reason": "Exited", "exit_code": 128}, "ENVIRONMENT_FAILED"),
    ],
)
def test_failed_environment_retains_cause_without_control_replay(
    monkeypatch, fault, failure, expected_code
):
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path == "/api/instances":
            return httpx.Response(
                200, json=[{"id": "owned", "status": "failed", "failure": failure}]
            )
        if request.url.path == "/direct/owned/invoke":
            if fault == "connect":
                raise httpx.ConnectError("fixture unavailable", request=request)
            return httpx.Response(404, text="environment route not found")
        raise AssertionError("A failed Environment must never use control-plane invoke")

    client = client_for(monkeypatch, handler)
    try:
        with pytest.raises(SDKEnvironmentError) as raised:
            client.invoke("owned", "file.make_dir", {"path": "/tmp/evidence"})
        error = raised.value
        assert error.code == expected_code
        assert error.failure == failure
        assert error.retry == "never"
        assert error.environment_id == "owned"
        assert error.first_error is not None
        direct = [r for r in calls if r.method == "POST"]
        assert direct
        assert {json.loads(r.content)["requestId"] for r in direct} == {
            error.request_id
        }
        assert {r.headers["X-ADX-Request-ID"] for r in direct} == {error.request_id}
        assert all(request.url.path == "/direct/owned/invoke" for request in direct)
    finally:
        client.close()


@pytest.mark.parametrize("catalog", ["running", "missing", "outage", "failed"])
@pytest.mark.parametrize("reported_outcome", ["not_started", "terminal"])
def test_terminal_route_after_unknown_attempt_preserves_logical_outcome(
    monkeypatch, catalog, reported_outcome
):
    calls = []
    first = httpx.ReadTimeout("reply lost after submission")

    def handler(request):
        calls.append(request)
        if request.url.path == "/api/instances":
            if catalog == "outage":
                return httpx.Response(503, text="catalog unavailable")
            if catalog == "missing":
                return httpx.Response(200, json=[])
            record = {"id": "owned", "status": catalog}
            if catalog == "failed":
                record["failure"] = {"reason": "OomKilled", "exit_code": 137}
            return httpx.Response(200, json=[record])
        assert request.url.path == "/direct/owned/invoke"
        if len(calls) == 1:
            raise first
        return httpx.Response(409, json={"error": {
            "code": "ENV_UNAVAILABLE", "retry": "never", "outcome": reported_outcome,
            "message": "route unavailable", "requestId": request.headers["X-ADX-Request-ID"],
        }})

    client = client_for(monkeypatch, handler)
    try:
        with pytest.raises(SDKEnvironmentError) as raised:
            client.invoke("owned", "process.start", {"cmd": "fixture"})
        error = raised.value
        assert error.first_error is first
        assert error.retry == "never"
        assert error.outcome == ("terminal" if catalog == "failed" else "unknown")
        assert error.code == ("ENVIRONMENT_OOM" if catalog == "failed" else "ENV_UNAVAILABLE")
        if catalog != "failed":
            assert isinstance(error, EnvironmentHTTPError)
            assert error.status_code == 409
            assert error.payload["error"]["outcome"] == reported_outcome
        direct = [request for request in calls if request.method == "POST"]
        assert len(direct) == 2
        assert {json.loads(request.content)["requestId"] for request in direct} == {error.request_id}
        assert [request.url.path for request in calls] == [
            "/direct/owned/invoke", "/direct/owned/invoke", "/api/instances"
        ]
    finally:
        client.close()


def test_operation_not_found_after_retry_keeps_first_failure(monkeypatch):
    calls = []
    first = httpx.ReadTimeout("reply lost")

    def handler(request):
        calls.append(request)
        assert request.url.path == "/direct/owned/invoke"
        if len(calls) == 1:
            raise first
        return httpx.Response(404, json={"error_code": "COMMAND_NOT_FOUND", "message": "gone"})

    client = client_for(monkeypatch, handler)
    try:
        with pytest.raises(EnvironmentHTTPError) as raised:
            client.invoke("owned", "process.get", {"command_id": "missing"})
        assert raised.value.status_code == 404
        assert raised.value.first_error is first
        assert len(calls) == 2
        assert {json.loads(request.content)["requestId"] for request in calls} == {raised.value.request_id}
    finally:
        client.close()


def test_transient_missing_route_does_not_disable_later_invokes(monkeypatch):
    healthy = False
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/api/instances":
            return httpx.Response(200, json=[{"id": "owned", "status": "running"}])
        if request.url.path == "/direct/owned/invoke":
            return (
                httpx.Response(200, json={"created": True})
                if healthy
                else httpx.Response(404)
            )
        raise AssertionError("Implicit control-plane fallback is unsupported")

    client = client_for(monkeypatch, handler)
    try:
        with pytest.raises(SDKEnvironmentError) as raised:
            client.invoke("owned", "file.make_dir", {"path": "/tmp/evidence"})
        assert raised.value.code == "ENVIRONMENT_UNAVAILABLE"
        assert raised.value.outcome == "not_started"
        assert raised.value.retry == "same_operation"
        healthy = True
        assert client.invoke("owned", "file.make_dir", {"path": "/tmp/evidence"}) == {
            "created": True
        }
        assert calls[-1] == "/direct/owned/invoke"
    finally:
        client.close()


def test_catalog_outage_preserves_unknown_execution_and_first_failure(monkeypatch):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/api/instances":
            return httpx.Response(503, text="catalog unavailable")
        if request.url.path == "/direct/owned/invoke":
            raise httpx.ReadTimeout("reply lost after submission", request=request)
        raise AssertionError("Unknown execution must not be replayed")

    client = client_for(monkeypatch, handler)
    try:
        with pytest.raises(SDKEnvironmentError) as raised:
            client.invoke("owned", "process.start", {"cmd": "irreversible-tool"})
        assert raised.value.code == "OUTCOME_UNKNOWN"
        assert raised.value.outcome == "unknown"
        assert isinstance(raised.value.first_error, httpx.ReadTimeout)
        assert all(p in {"/direct/owned/invoke", "/api/instances"} for p in calls)
    finally:
        client.close()


def test_operation_not_found_is_not_an_environment_route_failure(monkeypatch):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(
            404, json={"error_code": "COMMAND_NOT_FOUND", "message": "gone"}
        )

    client = client_for(monkeypatch, handler)
    try:
        with pytest.raises(EnvironmentHTTPError) as raised:
            client.invoke("owned", "process.get", {"command_id": "missing"})
        assert raised.value.status_code == 404
        assert calls == ["/direct/owned/invoke"]
    finally:
        client.close()


@pytest.mark.parametrize("catalog", ["failed-oom", "failed-exit", "running", "outage"])
def test_resuming_then_missing_route_consults_latest_environment_state(monkeypatch, catalog):
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path == "/api/instances":
            if catalog == "outage":
                return httpx.Response(503, text="catalog unavailable")
            record = {"id": "owned", "status": "running"}
            if catalog.startswith("failed-"):
                record.update(status="failed", failure={
                    "reason": "OomKilled" if catalog == "failed-oom" else "Exited", "exit_code": 137,
                })
            return httpx.Response(200, json=[record])
        assert request.url.path == "/direct/owned/invoke"
        if len(calls) == 1:
            return httpx.Response(503, headers={"X-ADX-Error-Code": "ENV_RESUMING"}, json={"error": {
                "code": "ENV_RESUMING", "retry": "same_operation", "outcome": "not_started",
                "requestForwarded": False, "requestId": request.headers["X-ADX-Request-ID"],
            }})
        return httpx.Response(404, text="environment route not found")

    client = client_for(monkeypatch, handler)
    try:
        with pytest.raises(SDKEnvironmentError) as raised:
            client.invoke("owned", "process.start", {"command_id": "command-a"})
        error = raised.value
        if catalog.startswith("failed-"):
            assert error.code == ("ENVIRONMENT_OOM" if catalog == "failed-oom" else "ENVIRONMENT_FAILED")
            assert error.outcome == "terminal" and error.retry == "never"
            assert error.failure["reason"] == ("OomKilled" if catalog == "failed-oom" else "Exited")
        else:
            assert error.code == "ENVIRONMENT_UNAVAILABLE"
            assert error.outcome == "not_started" and error.retry == "same_operation"
        assert error.first_error.code == "ENV_RESUMING"
        assert calls[-1].url.path == "/api/instances"
        direct = [request for request in calls if request.method == "POST"]
        assert len(direct) == 3
        assert {json.loads(request.content)["requestId"] for request in direct} == {error.request_id}
    finally:
        client.close()


@pytest.mark.parametrize("status_code", [409, 502])
@pytest.mark.parametrize("outcome", ["terminal", "not_started"])
@pytest.mark.parametrize("resuming", [False, True])
@pytest.mark.parametrize(
    "reason,code", [("OomKilled", "ENVIRONMENT_OOM"), ("Exited", "ENVIRONMENT_FAILED")]
)
def test_terminal_gateway_unavailable_keeps_committed_backend_cause(
    monkeypatch, status_code, outcome, resuming, reason, code
):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/api/instances":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": "owned",
                        "status": "failed",
                        "failure": {"reason": reason, "exit_code": 137},
                    }
                ],
            )
        if request.url.path == "/direct/owned/invoke":
            if resuming and calls.count(request.url.path) == 1:
                return httpx.Response(
                    503,
                    headers={"X-ADX-Error-Code": "ENV_RESUMING"},
                    json={
                        "error": {
                            "code": "ENV_RESUMING",
                            "retry": "same_operation",
                            "outcome": "not_started",
                            "requestForwarded": False,
                            "requestId": request.headers["X-ADX-Request-ID"],
                        }
                    },
                )
            return httpx.Response(
                status_code,
                json={
                    "error": {
                        "code": "ENV_UNAVAILABLE",
                        "retry": "never",
                        "outcome": outcome,
                        "requestId": request.headers["X-ADX-Request-ID"],
                    }
                },
            )
        raise AssertionError("no control replay")

    client = client_for(monkeypatch, handler)
    try:
        with pytest.raises(SDKEnvironmentError) as raised:
            client.invoke("owned", "process.get", {"command_id": "owned-command"})
        error = raised.value
        assert error.code == code
        assert error.failure["reason"] == reason
        assert error.first_error.code == (
            "ENV_RESUMING" if resuming else "ENV_UNAVAILABLE"
        )
        assert calls == ["/direct/owned/invoke"] * (2 if resuming else 1) + [
            "/api/instances"
        ]
    finally:
        client.close()


@pytest.mark.parametrize("catalog", ["running", "missing", "outage"])
def test_terminal_gateway_rejection_without_backend_failure_stays_authoritative(
    monkeypatch, catalog
):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/api/instances":
            if catalog == "missing":
                return httpx.Response(200, json=[])
            if catalog == "outage":
                return httpx.Response(503, text="catalog down")
            return httpx.Response(200, json=[{"id": "owned", "status": "running"}])
        return httpx.Response(
            409,
            json={
                "error": {
                    "code": "ENV_UNAVAILABLE",
                    "retry": "never",
                    "outcome": "terminal",
                    "message": "checkpoint expired",
                }
            },
        )

    client = client_for(monkeypatch, handler)
    try:
        with pytest.raises(EnvironmentHTTPError) as raised:
            client.invoke("owned", "process.get", {"command_id": "owned-command"})
        assert raised.value.code == "ENV_UNAVAILABLE"
        assert raised.value.retry == "never"
        assert raised.value.outcome == "terminal"
        assert "checkpoint expired" in str(raised.value)
        assert calls.count("/direct/owned/invoke") == 1
    finally:
        client.close()
