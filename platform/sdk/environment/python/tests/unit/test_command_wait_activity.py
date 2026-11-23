"""Explicit command waits refresh activity without extending command execution."""

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from adx_environment import ConnectionConfig, _command_watch, _transport, commands
from adx_environment._transport import EnvironmentError as SDKEnvironmentError
from adx_environment.commands import CommandHandle, Commands, CommandWaitTimeout
from adx_environment.types import CommandStatus


class IdleCommandService:
    """Model an idle window separately from a command's execution deadline."""

    def __init__(self, finishes_at=130):
        self.now = 0.0
        self.last_request = 0.0
        self.finishes_at = finishes_at
        self.actions = []
        self.waits = []
        self.invoke = self.request
        self.connection = object()

    @property
    def _connection(self):
        return self.connection

    def request(self, environment, action, args, **kwargs):
        self.actions.append((environment, action, args, kwargs))
        self.last_request = self.now
        if action == "process.capabilities":
            return {"protocol_version": 1, "capabilities": [
                "stable-command-id", "recoverable-command-result", "multiplexed-command-watch",
            ]}
        if action == "process.start":
            return {"pid": 42}
        assert action == "process.get"
        if self.now >= self.finishes_at:
            return {"command_id": "cmd-active", "status": "SUCCEEDED", "exit_code": 0, "stdout": "done"}
        return {"command_id": "cmd-active", "status": "RUNNING", "exit_code": None}

    def wait(self, environment, command, timeout):
        self.waits.append(timeout)
        duration = min(timeout if timeout is not None else 1000, self.finishes_at - self.now)
        if self.now + duration - self.last_request >= 60:
            raise RuntimeError("environment deleted while caller was waiting")
        self.now += duration
        if self.now < self.finishes_at:
            raise CommandWaitTimeout(environment, command, timeout)

    async def wait_async(self, environment, command, timeout):
        self.wait(environment, command, timeout)


@pytest.fixture(name="service")
def command_service(monkeypatch):
    backend = IdleCommandService()

    def advance(seconds):
        backend.now += seconds

    monkeypatch.setattr(commands, "time", SimpleNamespace(monotonic=lambda: backend.now, sleep=advance))
    monkeypatch.setattr(_command_watch, "manager_for", lambda _connection: backend)
    return backend


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("timeout", [None, 180])
def test_explicit_wait_outlives_idle_window_and_stops_activity_after_completion(service, asynchronous, timeout):
    handle = CommandHandle("cmd-active", service, "environment")
    result = asyncio.run(handle.wait_async(timeout)) if asynchronous else handle.wait(timeout)
    assert result.status == CommandStatus.SUCCEEDED
    assert result.stdout == "done"
    assert service.now == 130
    assert len(service.actions) > 2
    assert all(action == "process.get" for _, action, _, _ in service.actions)
    assert [options["_deadline"] for _, _, _, options in service.actions[1:]] == list(range(20, 141, 10))
    requests = len(service.actions)
    service.now += 61
    assert service.now - service.last_request > 60
    assert len(service.actions) == requests


@pytest.mark.parametrize("asynchronous", [False, True])
def test_wait_deadline_is_total_and_does_not_kill_or_resubmit(service, asynchronous):
    handle = CommandHandle("cmd-active", service, "environment")
    result = asyncio.run(handle.wait_async(25)) if asynchronous else handle.wait(25)
    assert result.status == CommandStatus.RUNNING
    assert result.error_code == "WAIT_TIMEOUT"
    assert result.exit_code is None
    assert service.now == 25
    assert all(action == "process.get" for _, action, _, _ in service.actions)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_initial_registry_read_shares_the_wait_deadline(service, asynchronous):
    original = service.invoke

    def invoke(environment, action, args, **kwargs):
        assert kwargs.get("_deadline") == 8
        service.now += 6
        return original(environment, action, args, **kwargs)

    service.invoke = invoke
    handle = CommandHandle("cmd-active", service, "environment")
    result = asyncio.run(handle.wait_async(8)) if asynchronous else handle.wait(8)

    assert result.error_code == "WAIT_TIMEOUT"
    assert service.now == 8
    assert service.waits == [2]


def test_registry_read_retries_share_the_wait_deadline(service, monkeypatch):
    requests = []

    def advance(seconds):
        service.now += seconds

    def handler(request):
        requests.append(request)
        advance(0.5 if len(requests) == 1 else 2 - service.now)
        raise httpx.ReadTimeout("response delayed", request=request)

    monkeypatch.setattr(_transport, "time", SimpleNamespace(monotonic=lambda: service.now, sleep=advance))
    monkeypatch.setattr(_transport, "_DIRECT_RETRY_BACKOFF_SECONDS", 0.125)
    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        monkeypatch.setattr(_transport, "acquire_shared_http_client", lambda *_: http)
        client = _transport.EnvironmentClient(connection=ConnectionConfig(server_address="ingress", token="test-token"))
        with pytest.raises(SDKEnvironmentError) as raised:
            CommandHandle("cmd-active", client, "environment").wait(2)

    assert raised.value.code == "OUTCOME_UNKNOWN"
    assert [request.url.path for request in requests] == ["/direct/environment/invoke"] * 2
    assert requests[0].headers["X-ADX-Request-ID"] == requests[1].headers["X-ADX-Request-ID"]
    assert [r.headers["X-ADX-Processing-Timeout-Ms"] for r in requests] == ["2000", "1375"]
    assert [r.extensions["timeout"]["read"] for r in requests] == [2.0, 1.375]
    assert not service.waits


@pytest.mark.parametrize("asynchronous", [False, True])
def test_expired_watch_notification_does_not_start_another_read(service, asynchronous):
    service.finishes_at = 8
    handle = CommandHandle("cmd-active", service, "environment")

    result = asyncio.run(handle.wait_async(8)) if asynchronous else handle.wait(8)

    assert result.error_code == "WAIT_TIMEOUT"
    assert len(service.actions) == 1


@pytest.mark.parametrize("asynchronous", [False, True])
def test_wait_zero_checks_status_once_without_watching(service, asynchronous):
    service.finishes_at = 0
    handle = CommandHandle("cmd-active", service, "environment")
    result = asyncio.run(handle.wait_async(0)) if asynchronous else handle.wait(0)

    assert result.status == CommandStatus.SUCCEEDED
    assert result.exit_code == 0
    assert [action for _, action, _, _ in service.actions] == ["process.get"]
    assert not service.waits


def test_background_submission_without_wait_does_not_start_activity_polling(service):
    handle = Commands(service, "environment").run("sleep 130", background=True, command_id="cmd-active")
    assert handle.id == "cmd-active"
    assert [action for _, action, _, _ in service.actions] == ["process.capabilities", "process.start"]
    assert not service.waits
    service.now = 61
    assert service.now - service.last_request > 60


def test_async_wait_cancellation_stops_local_wait_without_killing_command(monkeypatch):
    async def scenario():
        client = IdleCommandService()
        entered = asyncio.Event()

        async def wait_async(_environment, _command, _timeout):
            entered.set()
            await asyncio.Event().wait()

        manager = SimpleNamespace(wait_async=wait_async)
        monkeypatch.setattr(_command_watch, "manager_for", lambda _connection: manager)
        pending = asyncio.create_task(CommandHandle("cmd-active", client, "environment").wait_async())
        await entered.wait()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        requests = len(client.actions)
        await asyncio.sleep(0)
        assert len(client.actions) == requests
        assert all(action == "process.get" for _, action, _, _ in client.actions)

    asyncio.run(scenario())


@pytest.mark.parametrize("asynchronous", [False, True])
def test_wait_notification_failure_preserves_original_error(service, monkeypatch, asynchronous):
    error = RuntimeError("checkpoint reconnect budget exhausted")
    manager = SimpleNamespace(wait=Mock(side_effect=error))

    async def wait_async(*_args):
        raise error

    manager.wait_async = wait_async
    monkeypatch.setattr(_command_watch, "manager_for", lambda _connection: manager)
    handle = CommandHandle("cmd-active", service, "environment")
    with pytest.raises(RuntimeError) as raised:
        if asynchronous:
            asyncio.run(handle.wait_async(180))
        else:
            handle.wait(180)
    assert raised.value is error
    assert len(service.actions) == 1


@pytest.mark.parametrize("asynchronous", [False, True])
def test_temporary_checkpoint_read_failure_is_retried_without_replaying_command(service, monkeypatch, asynchronous):
    original = service.invoke
    unavailable = True

    def invoke(environment, action, args, **kwargs):
        nonlocal unavailable
        if service.now >= 20 and unavailable:
            unavailable = False
            raise SDKEnvironmentError("checkpoint temporarily unavailable", retry="same_operation")
        return original(environment, action, args, **kwargs)

    service.invoke = invoke
    monkeypatch.setattr(commands, "_POLL_RETRY_DELAY", 0)
    handle = CommandHandle("cmd-active", service, "environment")
    result = asyncio.run(handle.wait_async(180)) if asynchronous else handle.wait(180)
    assert result.exit_code == 0
    assert not unavailable
    assert all(action == "process.get" for _, action, _, _ in service.actions)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_retryable_read_outage_is_bounded_and_keeps_error_identity(service, monkeypatch, asynchronous):
    original = service.invoke
    error = SDKEnvironmentError("checkpoint did not return", retry="after_backoff", request_id="read-failed")

    def invoke(environment, action, args, **kwargs):
        if service.now >= 10:
            service.now += 1
            raise error
        return original(environment, action, args, **kwargs)

    service.invoke = invoke
    monkeypatch.setattr(commands, "_POLL_RETRY_DELAY", 0)
    handle = CommandHandle("cmd-active", service, "environment")
    with pytest.raises(SDKEnvironmentError) as raised:
        if asynchronous:
            asyncio.run(handle.wait_async(180))
        else:
            handle.wait(180)
    assert raised.value is error
    assert service.now <= 41
    assert all(action == "process.get" for _, action, _, _ in service.actions)
