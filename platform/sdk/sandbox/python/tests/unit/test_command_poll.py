"""Command wait and kill results preserve recoverable runtime semantics."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from adx_sandbox import _http_pool, commands
from adx_sandbox import _command_watch
from adx_sandbox._http_pool import SandboxClientClosedError
from adx_sandbox._transport import SandboxClient, SandboxError, SandboxHTTPError
from adx_sandbox.commands import CommandHandle, CommandWaitTimeout, Commands
from adx_sandbox.types import CommandResult, CommandStatus


@pytest.fixture
def clock(monkeypatch):
    state = SimpleNamespace(now=0.0, sleeps=[])

    def sleep(delay):
        state.sleeps.append(delay)
        state.now += delay

    monkeypatch.setattr(
        commands, "time", SimpleNamespace(monotonic=lambda: state.now, sleep=sleep)
    )
    return state


@pytest.mark.parametrize("options,expected_timeout", [
    ({}, None), ({"timeout": None}, None), ({"timeout": 60}, 60),
    ({"timeout": 2}, 2), ({"timeout": 0}, 0),
])
def test_background_execution_deadline_is_explicit(options, expected_timeout):
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = [
        {"protocol_version": 1, "capabilities": [
            "stable-command-id", "recoverable-command-result", "multiplexed-command-watch",
        ]},
        {"pid": 42},
    ]
    result = Commands(client, "sandbox").run(
        "sleep 120", background=True, command_id="cmd-deadline", **options,
    )
    assert result.command_id == "cmd-deadline"
    call = client.invoke.call_args
    assert call.args[:2] == ("sandbox", "process.start")
    request = call.args[2]
    assert request["command_id"] == "cmd-deadline"
    if expected_timeout is None:
        assert "timeout" not in request
    else:
        assert request["timeout"] == expected_timeout


@pytest.mark.parametrize("options,expected_timeout", [({}, 60), ({"timeout": 90}, 90)])
def test_foreground_default_deadline_is_preserved(monkeypatch, options, expected_timeout):
    collection = Commands(Mock(spec=SandboxClient), "sandbox")
    poll = Mock(return_value=CommandResult("done", "", 0))
    monkeypatch.setattr(collection, "_run_with_poll", poll)
    collection.run("sleep 120", **options)
    poll.assert_called_once_with("sleep 120", None, None, expected_timeout)


@pytest.fixture
def running(monkeypatch):
    handle = Mock(spec=CommandHandle)
    handle.id = "cmd-42"
    collection = Commands(Mock(spec=SandboxClient), "sandbox")
    monkeypatch.setattr(collection, "run", Mock(return_value=handle))
    return collection, handle


def test_long_command_client_closed_exits_without_retry_or_kill(clock, running, caplog):
    collection, handle = running
    error = SandboxClientClosedError("client closed")
    handle.wait.side_effect = error
    with pytest.raises(SandboxClientClosedError) as raised:
        collection._run_with_poll("sleep 60", None, None, 60)
    assert raised.value is error
    assert handle.wait.call_count == 1
    handle.kill.assert_not_called()
    assert clock.sleeps == []
    assert "command wait failed" not in caplog.text


@pytest.mark.parametrize("error", [
    httpx.ReadTimeout("timeout"), httpx.ConnectError("reset"),
    SandboxError("gateway unavailable", retry="after_backoff"),
])
def test_wait_error_retries_then_returns_result(clock, running, error):
    collection, handle = running
    expected = CommandResult("done", "", 0)
    handle.wait.side_effect = [error, expected]
    assert collection._run_with_poll("sleep 60", None, None, 60) is expected
    assert handle.wait.call_count == 2
    assert clock.sleeps == [1]
    handle.kill.assert_not_called()


@pytest.mark.parametrize("error", [
    SandboxError("sandbox exited", code="SANDBOX_EXITED", retry="never", request_id="req-1"),
    SandboxError("sandbox exited", code="SANDBOX_EXITED", retry="after_backoff", request_id="req-3"),
    SandboxHTTPError(409, {}, "scheduling failed", code="SCHEDULE_FAILED", retry="never"),
    SandboxHTTPError(403, {}, "forbidden", request_id="req-2"),
    RuntimeError("unexpected response"),
    ValueError("bad data"),
])
def test_terminal_wait_error_is_not_retried_or_replaced(clock, running, error):
    collection, handle = running
    handle.wait.side_effect = error
    with pytest.raises(type(error)) as raised:
        collection._run_with_poll("sleep 60", None, None, 60)
    assert raised.value is error
    handle.wait.assert_called_once()
    handle.kill.assert_not_called()
    assert clock.sleeps == []


def test_persistent_failure_is_paced_within_deadline(clock, running):
    collection, handle = running

    def wait(_timeout):
        clock.now += 0.1
        raise httpx.ReadTimeout("request failed")

    handle.wait.side_effect = wait
    result = collection._run_with_poll("sleep 60", None, None, 3)
    assert result.status == CommandStatus.TIMED_OUT
    assert clock.now == pytest.approx(3)
    assert clock.sleeps == pytest.approx([1, 1, 0.7])
    assert handle.wait.call_count == 3
    handle.kill.assert_called_once_with()


def test_failure_after_deadline_does_not_delay(clock, running):
    collection, handle = running

    def wait(_timeout):
        clock.now += 3
        raise httpx.ReadTimeout("request failed")

    handle.wait.side_effect = wait
    result = collection._run_with_poll("sleep 60", None, None, 3)
    assert result.status == CommandStatus.TIMED_OUT
    assert clock.sleeps == []
    assert handle.wait.call_count == 1
    handle.kill.assert_called_once_with()


def test_wait_timeout_continues_waiting(clock, running):
    collection, handle = running
    expected = CommandResult("done", "", 0)
    handle.wait.side_effect = [
        CommandResult("", "", None, status=CommandStatus.RUNNING, error_code="WAIT_TIMEOUT"),
        expected,
    ]
    assert collection._run_with_poll("sleep 60", None, None, 60) is expected
    assert clock.sleeps == []
    handle.kill.assert_not_called()


def test_http_wait_timeout_returns_running_result():
    client = Mock(spec=SandboxClient)
    client._connection = None
    client.invoke.side_effect = [
        {"command_id": "cmd-42", "status": "running"},
        SandboxHTTPError(
            400,
            {"status": "running", "error_code": "WAIT_TIMEOUT", "error": "still running"},
            "still running",
        ),
    ]

    result = CommandHandle("cmd-42", client, "sandbox").wait(timeout=2)

    assert result.status == CommandStatus.RUNNING
    assert result.error_code == "WAIT_TIMEOUT"
    assert result.error_message == "still running"
    assert result.exit_code is None


def test_http_wait_timeout_body_is_normalized_when_transport_returns_it():
    client = Mock(spec=SandboxClient)
    client._connection = None
    client.invoke.side_effect = [
        {"command_id": "cmd-42", "status": "running"},
        {"status": "running", "error_code": "WAIT_TIMEOUT", "error": "still running"},
    ]
    result = CommandHandle("cmd-42", client, "sandbox").wait(timeout=2)
    assert result.status == CommandStatus.RUNNING
    assert result.error_code == "WAIT_TIMEOUT"
    assert result.error_message == "still running"


def test_watched_wait_timeout_returns_running_result(monkeypatch):
    client = Mock(spec=SandboxClient)
    client._connection = object()
    client.invoke.return_value = {"command_id": "cmd-42", "status": "running"}
    manager = Mock()
    manager.wait.side_effect = CommandWaitTimeout("sandbox", "cmd-42", 2)
    monkeypatch.setattr(_command_watch, "manager_for", lambda _connection: manager)

    result = CommandHandle("cmd-42", client, "sandbox").wait(timeout=2)

    assert result.status == CommandStatus.RUNNING
    assert result.error_code == "WAIT_TIMEOUT"
    manager.wait.assert_called_once_with("sandbox", "cmd-42", 2)


def test_async_watched_wait_timeout_returns_running_result(monkeypatch):
    client = Mock(spec=SandboxClient)
    client._connection = object()
    client.invoke.return_value = {"command_id": "cmd-42", "status": "running"}
    manager = Mock()
    manager.wait_async = AsyncMock(side_effect=CommandWaitTimeout("sandbox", "cmd-42", 2))
    monkeypatch.setattr(_command_watch, "manager_for", lambda _connection: manager)

    result = asyncio.run(CommandHandle("cmd-42", client, "sandbox").wait_async(timeout=2))

    assert result.status == CommandStatus.RUNNING
    assert result.error_code == "WAIT_TIMEOUT"
    manager.wait_async.assert_awaited_once_with("sandbox", "cmd-42", 2)


@pytest.mark.parametrize("entry_point", ["handle", "collection"])
@pytest.mark.parametrize("status,code", [(404, "COMMAND_NOT_FOUND"), (400, "COMMAND_NOT_RUNNING")])
def test_kill_missing_or_finished_command_returns_false(entry_point, status, code):
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = SandboxHTTPError(status, {"error_code": code}, code)
    if entry_point == "handle":
        killed = CommandHandle("cmd-42", client, "sandbox").kill()
    else:
        killed = Commands(client, "sandbox").kill("cmd-42")
    assert killed is False


def test_kill_unexpected_runtime_failure_still_raises():
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = SandboxHTTPError(
        400, {"error_code": "SIGNAL_FAILED"}, "signal failed"
    )
    with pytest.raises(SandboxHTTPError):
        CommandHandle("cmd-42", client, "sandbox").kill()


def test_kill_noop_body_returns_false_but_other_error_body_raises():
    client = Mock(spec=SandboxClient)
    client.invoke.return_value = {
        "killed": False, "error_code": "COMMAND_NOT_RUNNING", "error": "already finished"
    }
    handle = CommandHandle("cmd-42", client, "sandbox")
    assert handle.kill() is False

    client.invoke.return_value = {
        "killed": False, "error_code": "SIGNAL_FAILED", "error": "signal failed"
    }
    with pytest.raises(SandboxError, match="signal failed"):
        handle.kill()


def test_local_deadline_after_noop_kill_returns_authoritative_result(clock, running):
    collection, handle = running
    terminal = CommandResult("done", "", 0, status=CommandStatus.SUCCEEDED)

    def wait(timeout):
        if timeout:
            clock.now += 3
            return CommandResult("", "", None, status=CommandStatus.RUNNING, error_code="WAIT_TIMEOUT")
        return terminal

    handle.wait.side_effect = wait
    handle.kill.return_value = False
    assert collection._run_with_poll("sleep 3", None, None, 3) is terminal
    handle.kill.assert_called_once_with()


@pytest.mark.parametrize("terminal", [
    CommandResult("partial", "deadline exceeded", None, status=CommandStatus.TIMED_OUT),
    CommandResult("finished", "", 0, status=CommandStatus.SUCCEEDED),
])
def test_local_deadline_racing_remote_completion_returns_terminal_result(clock, running, terminal):
    collection, handle = running
    waits = []

    def wait(timeout):
        waits.append(timeout)
        if len(waits) == 1:
            clock.now += 3
            raise TimeoutError("notification wait expired")
        assert timeout == 0
        return terminal

    handle.wait.side_effect = wait
    handle.kill.side_effect = SandboxHTTPError(
        400, {"error_code": "COMMAND_NOT_RUNNING", "killed": False}, "already terminal",
    )
    assert collection._run_with_poll("sleep 3", None, None, 3) is terminal
    handle.kill.assert_called_once_with()
    assert len(waits) == 2


@pytest.mark.parametrize("code,payload", [(401, {}), (400, {"error_code": "INVALID_COMMAND_ID"})])
def test_local_deadline_preserves_other_kill_errors(clock, running, code, payload):
    collection, handle = running
    error = SandboxHTTPError(code, payload, "kill rejected")
    handle.kill.side_effect = error

    def wait(_timeout):
        clock.now += 3
        raise TimeoutError("notification wait expired")

    handle.wait.side_effect = wait
    with pytest.raises(SandboxHTTPError) as raised:
        collection._run_with_poll("sleep 3", None, None, 3)
    assert raised.value is error
    assert handle.wait.call_count == 1


@pytest.mark.parametrize("direct", [False, True])
def test_close_during_poll_stops_wait_thread_and_preserves_other_lease(
    monkeypatch, direct
):
    entered = threading.Event()
    release = threading.Event()
    errors = []

    def handle(request):
        entered.set()
        assert release.wait(timeout=5)
        payload = {"status": "running"}
        if request.url.path.startswith("/api/"):
            payload = {"code": 200, "data": payload}
        return httpx.Response(200, json=payload)

    registry = _http_pool._SharedHTTPClientRegistry()
    monkeypatch.setattr(_http_pool, "_SHARED_HTTP_CLIENT_REGISTRY", registry)
    monkeypatch.setattr(
        _http_pool, "_new_http_client",
        lambda _verify: httpx.Client(transport=httpx.MockTransport(handle)),
    )
    client = SandboxClient(server="poll.example", token="first")
    other = SandboxClient(server="poll.example", token="second")
    client._direct_enabled = direct
    client._connection = None
    invoke = Mock(wraps=client.invoke)
    monkeypatch.setattr(client, "invoke", invoke)

    def wait():
        try:
            CommandHandle("cmd-42", client, "sandbox", 42).wait(timeout=3)
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=wait, daemon=True)
    worker.start()
    try:
        assert entered.wait(timeout=5)
        client.close()
        release.set()
        worker.join(timeout=5)
        assert not worker.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], SandboxClientClosedError)
        assert isinstance(errors[0], RuntimeError)
        assert "SandboxClient.close()" in str(errors[0])
        assert [call.args[1] for call in invoke.call_args_list] == ["process.get", "process.wait"]
        assert other.invoke("other", "process.poll", {"pid": 43}) == {"status": "running"}
    finally:
        release.set()
        worker.join(timeout=5)
        client.close()
        other.close()
        registry.close_all()
