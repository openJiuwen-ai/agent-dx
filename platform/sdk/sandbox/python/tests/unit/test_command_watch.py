import asyncio
from unittest.mock import patch

from adx_sandbox._command_watch import _CommandWaitManager
from adx_sandbox.types import ConnectionConfig


def _manager() -> _CommandWaitManager:
    return _CommandWaitManager(
        ConnectionConfig(
            server_address="edge.example:443",
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
        assert captured["additional_headers"] == {
            "Authorization": "Bearer token", "X-Auth": "token"
        }

    asyncio.run(scenario())
