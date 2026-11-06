"""A legacy torn result is reread by stable ID, never resubmitted or guessed."""
import asyncio
import json
from unittest.mock import Mock

import httpx
import pytest
from adx_environment import EnvironmentError as SDKEnvironmentError
from adx_environment._transport import EnvironmentClient, EnvironmentHTTPError
from adx_environment.commands import CommandHandle, CommandNotFound, Commands
from adx_environment.types import CommandStatus, ConnectionConfig


class CommandClient:
    """Exercise both transport contracts without editing client internals."""

    def __init__(self, websocket=False):
        self.connection = object() if websocket else None
        self.invoke = Mock()

    @property
    def _connection(self):
        return self.connection


class CommandResponse(dict):
    """Transport request identity is metadata outside the response mapping."""

    def __init__(self, value):
        super().__init__(value)
        self.request_id = "req-42"


@pytest.mark.parametrize("status,code", [("SUCCEEDED", 0), ("FAILED", 17)])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("websocket", [False, True])
def test_torn_terminal_observation_is_reread_without_command_replay(status, code, asynchronous, websocket):
    client = CommandClient(websocket)
    client.invoke.side_effect = [
        {"command_id": "cmd-42", "pid": 42, "status": status, "exit_code": None},
        {"command_id": "cmd-42", "pid": 42, "status": status, "exit_code": code, "stdout": "retained"},
    ]
    handle = CommandHandle("cmd-42", client, "environment")
    result = asyncio.run(handle.wait_async(2)) if asynchronous else handle.wait(2)
    assert result.status == CommandStatus(status)
    assert result.exit_code == code
    assert result.stdout == "retained"
    assert client.invoke.call_count == 2
    assert all(call.args == ("environment", "process.get", {"command_id": "cmd-42"})
               for call in client.invoke.call_args_list)


def test_repeated_inconsistent_result_is_an_explicit_unknown_protocol_error():
    client = CommandClient()
    client.invoke.return_value = {"status": "SUCCEEDED", "exit_code": None}
    with pytest.raises(SDKEnvironmentError) as raised:
        CommandHandle("cmd-42", client, "environment").wait(2)
    assert raised.value.code == "COMMAND_RESULT_INCONSISTENT"
    assert raised.value.outcome == "unknown"
    assert raised.value.retry == "never"
    assert raised.value.environment_id == "environment"
    assert raised.value.operation_id == "cmd-42"
    assert client.invoke.call_count == 2


@pytest.mark.parametrize("snapshot", [
    {"status": "SUCCEEDED", "exit_code": 0},
    {"status": "FAILED", "exit_code": 17},
    {"status": "TIMED_OUT", "exit_code": None},
    {"status": "KILLED", "exit_code": None, "exit_signal": 9},
    {"status": "FAILED", "exit_code": None, "error_code": "SPAWN_FAILED"},
    {"status": "FAILED", "exit_code": None, "exit_signal": 9},
])
def test_valid_terminal_and_nullable_failure_results_are_not_repaired(snapshot):
    client = CommandClient()
    client.invoke.return_value = snapshot
    result = CommandHandle("cmd-42", client, "environment").wait(2)
    assert result.exit_code == snapshot["exit_code"]
    client.invoke.assert_called_once()


def test_authoritative_read_failure_is_preserved():
    client = CommandClient()
    error = EnvironmentHTTPError(403, {}, "forbidden", request_id="req-42")
    client.invoke.side_effect = [{"status": "SUCCEEDED", "exit_code": None}, error]
    with pytest.raises(EnvironmentHTTPError) as raised:
        CommandHandle("cmd-42", client, "environment").wait(2)
    assert raised.value is error
    assert client.invoke.call_count == 2


def test_command_expiry_during_repair_preserves_the_stable_identity():
    client = CommandClient()
    client.invoke.side_effect = [
        {"status": "SUCCEEDED", "exit_code": None},
        EnvironmentHTTPError(404, {"error": "not found"}, "not found", request_id="req-42"),
    ]
    with pytest.raises(CommandNotFound) as raised:
        CommandHandle("cmd-42", client, "environment").wait(2)
    assert raised.value.command_id == "cmd-42"
    assert raised.value.request_id == "req-42"


@pytest.mark.parametrize("status,code", [("SUCCEEDED", 0), ("FAILED", 17)])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_http_wait_validates_its_terminal_result_by_same_id(status, code, asynchronous):
    client = CommandClient()
    client.invoke.side_effect = [
        {"command_id": "cmd-42", "status": "RUNNING", "exit_code": None},
        {"command_id": "cmd-42", "status": status, "exit_code": None},
        {"command_id": "cmd-42", "status": status, "exit_code": code, "stdout": "retained"},
    ]
    handle = CommandHandle("cmd-42", client, "environment")
    result = asyncio.run(handle.wait_async(2)) if asynchronous else handle.wait(2)
    assert result.status == CommandStatus(status)
    assert result.exit_code == code
    assert result.stdout == "retained"
    assert [call.args[1] for call in client.invoke.call_args_list] == ["process.get", "process.wait", "process.get"]
    assert all(call.args[2]["command_id"] == "cmd-42" for call in client.invoke.call_args_list)


def test_http_wait_rejects_a_repeatedly_inconsistent_terminal_result():
    client = CommandClient()
    client.invoke.side_effect = [
        {"status": "RUNNING", "exit_code": None},
        {"status": "SUCCEEDED", "exit_code": None},
        CommandResponse({"status": "SUCCEEDED", "exit_code": None}),
    ]
    with pytest.raises(SDKEnvironmentError) as raised:
        CommandHandle("cmd-42", client, "environment").wait(2)
    assert raised.value.code == "COMMAND_RESULT_INCONSISTENT"
    assert raised.value.operation_id == "cmd-42"
    assert raised.value.environment_id == "environment"
    assert raised.value.request_id == "req-42"
    assert client.invoke.call_count == 3


@pytest.mark.parametrize("status", [None, "FAILED"])
@pytest.mark.parametrize("operation", ["poll", "wait", "wait_async", "http_wait", "reread", "list", "exec"])
def test_unavailable_observation_never_becomes_a_command_result(status, operation):
    client = CommandClient()
    payload = CommandResponse({
        "command_id": "cmd-42", "operation_id": "cmd-42", "pid": 42,
        "error_code": "COMMAND_STATE_UNAVAILABLE", "error": "command exit state is poisoned",
        "retry": "same_operation", "outcome": "unknown", "exit_code": None,
    })
    if status is not None:
        payload["status"] = status
    client.invoke.return_value = CommandResponse({"processes": [dict(payload)]}) if operation == "list" else payload
    if operation == "http_wait":
        client.invoke.side_effect = [{"status": "RUNNING"}, payload]
    if operation == "reread":
        client.invoke.side_effect = [{"status": "SUCCEEDED", "exit_code": None}, payload]
    handle = CommandHandle("cmd-42", client, "environment")
    with pytest.raises(SDKEnvironmentError) as raised:
        if operation == "wait_async":
            asyncio.run(handle.wait_async(2))
        elif operation in ("http_wait", "reread"):
            handle.wait(2)
        elif operation == "list":
            Commands(client, "environment").list()
        elif operation == "exec":
            Commands(client, "environment").run("true", timeout=2)
        else:
            getattr(handle, operation)()
    error = raised.value
    assert error.code == "COMMAND_STATE_UNAVAILABLE"
    assert error.outcome == "unknown"
    assert error.retry == "same_operation"
    assert error.operation_id == "cmd-42"
    assert error.environment_id == "environment"
    assert error.request_id == "req-42"
    assert str(error) == "command exit state is poisoned"
    assert all(call.args[1] != "process.start" for call in client.invoke.call_args_list)


def test_real_http_command_read_retains_flat_observation_error(monkeypatch):
    requests = []
    payload = {
        "command_id": "cmd-42", "status": "FAILED", "exit_code": None,
        "error_code": "COMMAND_STATE_UNAVAILABLE", "error": "command exit state is poisoned",
        "retry": "same_operation", "outcome": "unknown",
    }

    def handler(request):
        assert request.method == "POST"
        assert request.url.path == "/direct/environment/invoke"
        requests.append(json.loads(request.content))
        return httpx.Response(400, json=payload, headers={"X-Request-Id": "req-http"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as transport:
        monkeypatch.setattr("adx_environment._transport.acquire_shared_http_client", lambda *args: transport)
        client = EnvironmentClient(connection=ConnectionConfig(server_address="api.test:443", token="test"))
        try:
            with pytest.raises(EnvironmentHTTPError) as raised:
                CommandHandle("cmd-42", client, "environment").poll()
            error = raised.value
            assert error.code == "COMMAND_STATE_UNAVAILABLE"
            assert error.retry == "same_operation"
            assert error.outcome == "unknown"
            assert error.environment_id == "environment"
            assert error.operation_id == "cmd-42"
            assert error.request_id == "req-http"
            assert error.status_code == 400
            assert error.payload == payload
            assert len(requests) == 1
            assert requests[0]["action"] == "process.get"
            assert requests[0]["args"] == {"command_id": "cmd-42"}
        finally:
            client.close()


@pytest.mark.parametrize("operation", ["poll", "wait", "http_wait", "reread", "list", "exec"])
def test_http_observation_error_preserves_status_payload_and_request_identity(operation):
    client = CommandClient()
    payload = {
        "command_id": "cmd-42", "operation_id": "cmd-42", "status": "FAILED",
        "error_code": "COMMAND_STATE_UNAVAILABLE", "error": "command exit state is poisoned",
        "retry": "same_operation", "outcome": "unknown", "exit_code": None,
    }
    error = EnvironmentHTTPError(400, payload, "HTTP 400 command state is unavailable", request_id="req-42")
    client.invoke.side_effect = error
    if operation == "http_wait":
        client.invoke.side_effect = [{"status": "RUNNING"}, error]
    if operation == "reread":
        client.invoke.side_effect = [{"status": "SUCCEEDED", "exit_code": None}, error]
    handle = CommandHandle("cmd-42", client, "environment")
    with pytest.raises(EnvironmentHTTPError) as raised:
        if operation in ("http_wait", "reread"):
            handle.wait(2)
        elif operation == "list":
            Commands(client, "environment").list()
        elif operation == "exec":
            Commands(client, "environment").run("true", timeout=2)
        else:
            getattr(handle, operation)()
    assert raised.value is error
    assert error.status_code == 400
    assert error.payload is payload
    assert error.code == "COMMAND_STATE_UNAVAILABLE"
    assert error.retry == "same_operation"
    assert error.outcome == "unknown"
    assert error.operation_id == "cmd-42"
    assert error.environment_id == "environment"
    assert error.request_id == "req-42"
