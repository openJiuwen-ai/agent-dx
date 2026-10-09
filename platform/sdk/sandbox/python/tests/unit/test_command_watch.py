import asyncio
import ssl
from unittest.mock import patch

from adx_sandbox._command_watch import _CommandWaitManager
from adx_sandbox.types import ConnectionConfig


def _manager() -> _CommandWaitManager:
    return _CommandWaitManager(
        ConnectionConfig(
            server_address="ingress.example:443",
            token="token",
            use_tls=True,
        )
    )


def test_async_waiters_share_manager_without_one_blocking_thread_per_waiter():
    async def scenario():
        manager = _manager()
        key = ("sandbox-1", "command-1")
        with patch.object(manager, "_ensure_thread_locked"):
            wait = asyncio.create_task(manager.wait_async(*key, timeout=1))
            await asyncio.sleep(0)
            assert manager._desired() == {key}

            manager._notify(key)
            await wait
            assert manager._desired() == set()

    asyncio.run(scenario())


def test_async_wait_cancellation_only_removes_local_subscription():
    async def scenario():
        manager = _manager()
        key = ("sandbox-1", "command-1")
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
            assert manager._desired() == set()

    asyncio.run(scenario())


def test_command_watch_preserves_legacy_frontend_auth():
    async def scenario():
        manager = _manager()
        captured = {}

        def connect(_uri, **kwargs):
            captured.update(kwargs)
            raise asyncio.CancelledError

        with (
            patch.object(manager, "_desired", return_value={("sandbox", "command")}),
            patch("websockets.asyncio.client.connect", side_effect=connect),
        ):
            try:
                await manager._run()
            except asyncio.CancelledError:
                pass
        assert captured["additional_headers"] == {"Authorization": "Bearer token", "X-Auth": "token"}

    asyncio.run(scenario())


def test_command_watch_tls_context_preserves_verification_setting():
    async def scenario(verify_tls):
        manager = _CommandWaitManager(
            ConnectionConfig(server_address="ingress.example:443", token="token", use_tls=True, verify_tls=verify_tls)
        )
        captured = {}

        def connect(_uri, **kwargs):
            captured.update(kwargs)
            raise asyncio.CancelledError

        with (
            patch.object(manager, "_desired", return_value={("sandbox", "command")}),
            patch("websockets.asyncio.client.connect", side_effect=connect),
        ):
            try:
                await manager._run()
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
            patch.object(manager, "_desired", return_value={("sandbox", "command")}),
            patch.object(manager, "_notify", notified),
            patch("websockets.asyncio.client.connect", side_effect=connect),
            patch("adx_sandbox._command_watch.time.monotonic", side_effect=lambda: clock),
            patch("adx_sandbox._command_watch.asyncio.sleep", side_effect=backoff),
        ):
            try:
                await manager._run()
            except asyncio.CancelledError:
                pass
        assert attempts == 3
        notified.assert_called_once()
        assert notified.call_args.args[0] == ("sandbox", "command")
        assert "command watch unavailable" in notified.call_args.args[1]

    asyncio.run(scenario())
