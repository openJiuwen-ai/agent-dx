import asyncio
import ssl
from unittest.mock import patch

from adx_environment._command_watch import _CommandWaitManager
from adx_environment.types import ConnectionConfig


class CommandWatchHarness(_CommandWaitManager):
    """Expose deterministic transport controls only to the white-box tests."""

    async def run_watch(self):
        await self._run()

    def subscriptions(self):
        return self._desired()

    def notify_terminal(self, key):
        self._notify(key)


def _manager() -> CommandWatchHarness:
    return CommandWatchHarness(
        ConnectionConfig(
            server_address="ingress.example:443",
            token="token",
            use_tls=True,
        )
    )


def test_async_waiters_share_manager_without_one_blocking_thread_per_waiter():
    async def scenario():
        manager = _manager()
        key = ("environment-1", "command-1")
        with patch.object(manager, "_ensure_thread_locked"):
            wait = asyncio.create_task(manager.wait_async(*key, timeout=1))
            await asyncio.sleep(0)
            assert manager.subscriptions() == {key}

            manager.notify_terminal(key)
            await wait
            assert manager.subscriptions() == set()

    asyncio.run(scenario())


def test_async_wait_cancellation_only_removes_local_subscription():
    async def scenario():
        manager = _manager()
        key = ("environment-1", "command-1")
        with patch.object(manager, "_ensure_thread_locked"):
            wait = asyncio.create_task(manager.wait_async(*key, timeout=None))
            await asyncio.sleep(0)
            wait.cancel()
            try:
                await wait
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError("cancelled wait must propagate cancellation")
            assert manager.subscriptions() == set()

    asyncio.run(scenario())


def test_command_watch_preserves_legacy_frontend_auth():
    async def scenario():
        manager = _manager()
        captured = {}

        def connect(_uri, **kwargs):
            captured.update(kwargs)
            raise asyncio.CancelledError

        with (
            patch.object(manager, "_desired", return_value={("environment", "command")}),
            patch("websockets.asyncio.client.connect", side_effect=connect),
        ):
            try:
                await manager.run_watch()
            except asyncio.CancelledError:
                pass
        assert captured["additional_headers"] == {"Authorization": "Bearer token", "X-Auth": "token"}

    asyncio.run(scenario())


def test_command_watch_tls_context_preserves_verification_setting():
    async def scenario(verify_tls):
        manager = CommandWatchHarness(
            ConnectionConfig(server_address="ingress.example:443", token="token", use_tls=True, verify_tls=verify_tls)
        )
        captured = {}

        def connect(_uri, **kwargs):
            captured.update(kwargs)
            raise asyncio.CancelledError

        with (
            patch.object(manager, "_desired", return_value={("environment", "command")}),
            patch("websockets.asyncio.client.connect", side_effect=connect),
        ):
            try:
                await manager.run_watch()
            except asyncio.CancelledError:
                pass
        context = captured["ssl"]
        assert context.check_hostname == verify_tls
        assert context.verify_mode == (ssl.CERT_REQUIRED if verify_tls else ssl.CERT_NONE)
        if verify_tls:
            assert context.get_ca_certs()

    for verify_tls in (False, True):
        asyncio.run(scenario(verify_tls))


def test_successful_frontend_handshake_does_not_reset_downstream_outage_budget():
    """A live ingress can repeatedly lose Execd during checkpoint or outage."""
    from contextlib import asynccontextmanager
    from unittest.mock import Mock

    async def scenario():
        manager = _manager()
        notified = Mock()
        attempts = 0
        clock = 1.0

        class Socket:
            async def send(self, _message):
                pass

            async def recv(self):
                raise ConnectionError("downstream temporarily unavailable")

        @asynccontextmanager
        async def connect(_uri, **_kwargs):
            nonlocal attempts
            attempts += 1
            if attempts > 5:
                raise asyncio.CancelledError
            yield Socket()

        async def backoff(_seconds):
            nonlocal clock
            clock += 16

        with (
            patch.object(manager, "_desired", return_value={("environment", "command")}),
            patch.object(manager, "_notify", notified),
            patch("websockets.asyncio.client.connect", side_effect=connect),
            patch("adx_environment._command_watch.time.monotonic", side_effect=lambda: clock),
            patch("adx_environment._command_watch.asyncio.sleep", side_effect=backoff),
        ):
            try:
                await manager.run_watch()
            except asyncio.CancelledError:
                pass
        assert attempts == 3
        notified.assert_called_once()
        assert notified.call_args.args[0] == ("environment", "command")
        assert "command watch unavailable" in notified.call_args.args[1]

    asyncio.run(scenario())


def test_new_subscription_wakes_idle_receiver_without_server_message():
    """A second command must not wait for a healthy, silent socket to time out."""
    import json
    from contextlib import asynccontextmanager

    async def scenario():
        manager = _manager()
        first_sent = asyncio.Event()
        second_sent = asyncio.Event()
        receiving = asyncio.Event()
        incoming = asyncio.Queue()
        incoming.put_nowait(json.dumps({"op": "ready", "protocolVersion": 1,
                                      "capabilities": ["multiplexed-command-watch"]}))

        class Socket:
            async def send(self, message):
                frame = json.loads(message)
                if frame["op"] != "subscribe":
                    return
                for command in frame["commands"]:
                    if command["commandId"] == "first":
                        first_sent.set()
                    if command["commandId"] == "second":
                        second_sent.set()

            async def recv(self):
                receiving.set()
                return await incoming.get()

        @asynccontextmanager
        async def connect(*_args, **_kwargs):
            yield Socket()

        tasks = []
        with patch.object(manager, "_ensure_thread_locked"), patch(
            "websockets.asyncio.client.connect", side_effect=connect
        ):
            tasks.append(asyncio.create_task(manager.wait_async("env", "first", 5)))
            await asyncio.sleep(0)
            tasks.append(asyncio.create_task(manager.run_watch()))
            try:
                await asyncio.wait_for(first_sent.wait(), 1)
                await asyncio.wait_for(receiving.wait(), 1)
                await asyncio.sleep(0)
                tasks.append(asyncio.create_task(manager.wait_async("env", "second", 5)))
                await asyncio.wait_for(second_sent.wait(), 0.15)
                assert manager.subscriptions() == {("env", "first"), ("env", "second")}
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())


def test_command_watch_reconnect_resubscribes_without_submitting_commands():
    import json
    from contextlib import asynccontextmanager

    async def scenario():
        manager = _manager()
        attempts = 0
        subscriptions = []
        incoming = asyncio.Queue()

        class Socket:
            def __init__(self, attempt):
                self.attempt = attempt
                self.received = False

            async def send(self, message):
                frame = json.loads(message)
                assert frame["op"] in ("subscribe", "unsubscribe")
                if frame["op"] == "subscribe":
                    subscriptions.append(frame["commands"])
                    if self.attempt == 2:
                        incoming.put_nowait(json.dumps({"op": "state", "environmentId": "env",
                                                      "commandId": "stable", "status": "SUCCEEDED"}))

            async def recv(self):
                if not self.received:
                    self.received = True
                    return json.dumps({"op": "ready", "protocolVersion": 1,
                                       "capabilities": ["multiplexed-command-watch"]})
                if self.attempt == 1:
                    raise ConnectionError("connection lost after subscription")
                return await incoming.get()

        @asynccontextmanager
        async def connect(*_args, **_kwargs):
            nonlocal attempts
            attempts += 1
            yield Socket(attempts)

        with patch.object(manager, "_ensure_thread_locked"), patch(
            "websockets.asyncio.client.connect", side_effect=connect
        ):
            waiter = asyncio.create_task(manager.wait_async("env", "stable", 3))
            await asyncio.sleep(0)
            runner = asyncio.create_task(manager.run_watch())
            try:
                await waiter
                assert attempts == 2
                assert subscriptions == [[{"environmentId": "env", "commandId": "stable"}]] * 2
            finally:
                runner.cancel()
                await asyncio.gather(runner, return_exceptions=True)

    asyncio.run(scenario())


def test_cancelling_last_waiter_unsubscribes_without_waiting_for_a_message():
    import json
    from contextlib import asynccontextmanager

    async def scenario():
        manager = _manager()
        subscribed, unsubscribed = asyncio.Event(), asyncio.Event()
        incoming = asyncio.Queue()
        incoming.put_nowait(json.dumps({"op": "ready", "protocolVersion": 1,
                                      "capabilities": ["multiplexed-command-watch"]}))

        class Socket:
            async def send(self, message):
                op = json.loads(message)["op"]
                (subscribed if op == "subscribe" else unsubscribed).set()

            async def recv(self):
                return await incoming.get()

        @asynccontextmanager
        async def connect(*_args, **_kwargs):
            yield Socket()

        with patch.object(manager, "_ensure_thread_locked"), patch(
            "websockets.asyncio.client.connect", side_effect=connect
        ):
            waiter = asyncio.create_task(manager.wait_async("env", "cancelled", None))
            await asyncio.sleep(0)
            runner = asyncio.create_task(manager.run_watch())
            try:
                await asyncio.wait_for(subscribed.wait(), 1)
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
                await asyncio.wait_for(unsubscribed.wait(), 0.15)
                assert manager.subscriptions() == set()
            finally:
                runner.cancel()
                await asyncio.gather(runner, return_exceptions=True)

    asyncio.run(scenario())


def test_concurrent_waiters_share_one_transport_and_observe_missing_state():
    import json
    from contextlib import asynccontextmanager

    async def scenario():
        manager = _manager()
        incoming = asyncio.Queue()
        incoming.put_nowait(json.dumps({"op": "ready", "protocolVersion": 1,
                                      "capabilities": ["multiplexed-command-watch"]}))
        connections = 0

        class Socket:
            async def send(self, message):
                frame = json.loads(message)
                if frame["op"] == "subscribe":
                    for i, command in enumerate(frame["commands"]):
                        incoming.put_nowait(json.dumps({"op": "state", **command,
                                                      "status": "EXPIRED" if i == 0 else "SUCCEEDED"}))

            async def recv(self):
                return await incoming.get()

        @asynccontextmanager
        async def connect(*_args, **_kwargs):
            nonlocal connections
            connections += 1
            yield Socket()

        with patch.object(manager, "_ensure_thread_locked"), patch(
            "websockets.asyncio.client.connect", side_effect=connect
        ):
            waits = [asyncio.create_task(manager.wait_async("env", str(i), 2)) for i in range(20)]
            await asyncio.sleep(0)
            runner = asyncio.create_task(manager.run_watch())
            try:
                await asyncio.gather(*waits)
                assert connections == 1
            finally:
                runner.cancel()
                await asyncio.gather(runner, return_exceptions=True)

    asyncio.run(scenario())
