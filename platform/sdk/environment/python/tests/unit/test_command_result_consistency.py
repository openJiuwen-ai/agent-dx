"""A legacy torn result is reread by stable ID, never resubmitted or guessed."""
import asyncio
from unittest.mock import Mock

import pytest
from adx_environment._transport import EnvironmentClient, EnvironmentError, EnvironmentHTTPError
from adx_environment.commands import CommandHandle, CommandNotFound
from adx_environment.types import CommandStatus


@pytest.mark.parametrize("status,code", [("SUCCEEDED", 0), ("FAILED", 17)])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("websocket", [False, True])
def test_torn_terminal_observation_is_reread_without_command_replay(status, code, asynchronous, websocket):
    client = Mock(spec=EnvironmentClient)
    client._connection = object() if websocket else None
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
    client = Mock(spec=EnvironmentClient)
    client.invoke.return_value = {"status": "SUCCEEDED", "exit_code": None}
    with pytest.raises(EnvironmentError) as raised:
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
    client = Mock(spec=EnvironmentClient)
    client.invoke.return_value = snapshot
    result = CommandHandle("cmd-42", client, "environment").wait(2)
    assert result.exit_code == snapshot["exit_code"]
    client.invoke.assert_called_once()


def test_authoritative_read_failure_is_preserved():
    client = Mock(spec=EnvironmentClient)
    error = EnvironmentHTTPError(403, {}, "forbidden", request_id="req-42")
    client.invoke.side_effect = [{"status": "SUCCEEDED", "exit_code": None}, error]
    with pytest.raises(EnvironmentHTTPError) as raised:
        CommandHandle("cmd-42", client, "environment").wait(2)
    assert raised.value is error
    assert client.invoke.call_count == 2


def test_command_expiry_during_repair_preserves_the_stable_identity():
    client = Mock(spec=EnvironmentClient)
    client.invoke.side_effect = [
        {"status": "SUCCEEDED", "exit_code": None},
        EnvironmentHTTPError(404, {"error": "not found"}, "not found", request_id="req-42"),
    ]
    with pytest.raises(CommandNotFound) as raised:
        CommandHandle("cmd-42", client, "environment").wait(2)
    assert raised.value.command_id == "cmd-42"
    assert raised.value.request_id == "req-42"
