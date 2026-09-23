"""One hidden multiplexed command-watch WebSocket per connection context."""

import asyncio
import json
import queue
import ssl
import threading
import time
import weakref
from collections import defaultdict
from typing import Dict, Optional, Set, Tuple

from .types import ConnectionConfig
from ._command_metrics import increment

_TERMINAL = frozenset(("SUCCEEDED", "FAILED", "TIMED_OUT", "KILLED"))
_MANAGERS: "weakref.WeakValueDictionary[ConnectionConfig, _CommandWaitManager]" = (
    weakref.WeakValueDictionary()
)
_MANAGERS_LOCK = threading.Lock()


def _complete_async_waiter(future: asyncio.Future, error: Optional[str]) -> None:
    if not future.done():
        future.set_result(error)


def manager_for(connection: ConnectionConfig) -> "_CommandWaitManager":
    with _MANAGERS_LOCK:
        manager = _MANAGERS.get(connection)
        if manager is None:
            manager = _CommandWaitManager(connection)
            _MANAGERS[connection] = manager
        return manager


class _CommandWaitManager:
    def __init__(self, connection: ConnectionConfig):
        self._connection = connection
        self._lock = threading.Lock()
        self._waiters: Dict[Tuple[str, str], Set[threading.Event]] = defaultdict(set)
        self._async_waiters: Dict[
            Tuple[str, str], Set[Tuple[asyncio.AbstractEventLoop, asyncio.Future]]
        ] = defaultdict(set)
        self._errors: Dict[Tuple[str, str], str] = {}
        self._changed: "queue.SimpleQueue[None]" = queue.SimpleQueue()
        self._thread: Optional[threading.Thread] = None

    def wait(self, sandbox_id: str, command_id: str, timeout: Optional[float]) -> None:
        key = (sandbox_id, command_id)
        event = threading.Event()
        with self._lock:
            self._waiters[key].add(event)
            self._errors.pop(key, None)
            self._ensure_thread_locked()
        self._changed.put(None)
        try:
            if not event.wait(timeout):
                from .commands import CommandWaitTimeout

                raise CommandWaitTimeout(sandbox_id, command_id, timeout)
            with self._lock:
                error = self._errors.get(key)
            if error:
                from .commands import CommandUnavailable

                raise CommandUnavailable(
                    error, sandbox_id=sandbox_id, command_id=command_id
                )
        finally:
            with self._lock:
                waiters = self._waiters.get(key)
                if waiters is not None:
                    waiters.discard(event)
                    if not waiters:
                        self._waiters.pop(key, None)
                        self._errors.pop(key, None)
            self._changed.put(None)

    async def wait_async(
        self, sandbox_id: str, command_id: str, timeout: Optional[float]
    ) -> None:
        key = (sandbox_id, command_id)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        async_waiter = (loop, future)
        with self._lock:
            self._async_waiters[key].add(async_waiter)
            self._errors.pop(key, None)
            self._ensure_thread_locked()
        self._changed.put(None)
        try:
            try:
                error = await asyncio.wait_for(future, timeout)
            except asyncio.TimeoutError as timeout_error:
                from .commands import CommandWaitTimeout

                raise CommandWaitTimeout(
                    sandbox_id, command_id, timeout
                ) from timeout_error
            if error:
                from .commands import CommandUnavailable

                raise CommandUnavailable(
                    error, sandbox_id=sandbox_id, command_id=command_id
                )
        finally:
            with self._lock:
                waiters = self._async_waiters.get(key)
                if waiters is not None:
                    waiters.discard(async_waiter)
                    if not waiters:
                        self._async_waiters.pop(key, None)
                        if key not in self._waiters:
                            self._errors.pop(key, None)
            self._changed.put(None)

    def _ensure_thread_locked(self) -> None:
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._thread_main,
                name="adx-command-watch",
                daemon=True,
            )
            self._thread.start()

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run())
        finally:
            with self._lock:
                self._thread = None
                # A waiter can arrive while the previous loop is between its
                # last desired-set check and this cleanup. Hand it to a fresh
                # transport thread instead of leaving it asleep indefinitely.
                if self._waiters or self._async_waiters:
                    self._ensure_thread_locked()

    def _desired(self) -> Set[Tuple[str, str]]:
        with self._lock:
            return set(self._waiters) | set(self._async_waiters)

    def _notify(self, key: Tuple[str, str], error: Optional[str] = None) -> None:
        with self._lock:
            if error:
                self._errors[key] = error
            for event in self._waiters.get(key, ()):
                event.set()
            for loop, future in self._async_waiters.get(key, ()):
                try:
                    loop.call_soon_threadsafe(_complete_async_waiter, future, error)
                except RuntimeError:
                    # The caller can close its event loop while a terminal
                    # notification is concurrently in flight. Its finally
                    # path removes the waiter; the shared transport stays up.
                    pass

    async def _run(self) -> None:
        import websockets.asyncio.client as ws_client

        reconnect_budget = 30.0
        empty_since: Optional[float] = None
        unavailable_since: Optional[float] = None
        while True:
            desired = self._desired()
            if not desired:
                empty_since = empty_since or time.monotonic()
                if time.monotonic() - empty_since >= 5:
                    return
                await asyncio.sleep(0.1)
                continue
            empty_since = None
            scheme = "wss" if self._connection.use_tls else "ws"
            uri = f"{scheme}://{self._connection.server_address}/api/sandbox/v1/commands/watch"
            ssl_context = None
            if scheme == "wss":
                ssl_context = (
                    ssl.create_default_context()
                    if self._connection.verify_tls
                    else ssl._create_unverified_context()  # noqa: SLF001
                )
            try:
                token = self._connection.resolved_token()
                async with ws_client.connect(
                    uri,
                    additional_headers={"Authorization": f"Bearer {token}", "X-Auth": token},
                    ssl=ssl_context,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=1 << 20,
                ) as websocket:
                    unavailable_since = None
                    sent: Set[Tuple[str, str]] = set()
                    ready = False
                    while self._desired():
                        desired = self._desired()
                        added = desired - sent
                        removed = sent - desired
                        if added:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "protocolVersion": 1,
                                        "op": "subscribe",
                                        "commands": [
                                            {"sandboxId": sid, "commandId": cid}
                                            for sid, cid in sorted(added)
                                        ],
                                    }
                                )
                            )
                        if removed:
                            await websocket.send(
                                json.dumps(
                                    {
                                        "op": "unsubscribe",
                                        "commands": [
                                            {"sandboxId": sid, "commandId": cid}
                                            for sid, cid in sorted(removed)
                                        ],
                                    }
                                )
                            )
                        sent = desired
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), timeout=0.25)
                        except asyncio.TimeoutError:
                            continue
                        state = json.loads(raw)
                        if state.get("op") == "ready":
                            if state.get("protocolVersion") != 1 or "multiplexed-command-watch" not in state.get("capabilities", ()):
                                raise RuntimeError("server does not support command watch protocol v1")
                            ready = True
                            continue
                        if not ready:
                            raise RuntimeError("ingress sent command state before protocol negotiation")
                        key = (str(state.get("sandboxId", "")), str(state.get("commandId", "")))
                        if state.get("status") == "REJECTED":
                            self._notify(key, str(state.get("error", "command watch rejected")))
                        elif str(state.get("status", "")) in _TERMINAL:
                            self._notify(key)
            except Exception as error:
                increment("command_wait_reconnect_total")
                unavailable_since = unavailable_since or time.monotonic()
                if time.monotonic() - unavailable_since >= reconnect_budget:
                    for key in self._desired():
                        self._notify(key, f"command watch unavailable: {error}")
                    return
                await asyncio.sleep(0.2)
