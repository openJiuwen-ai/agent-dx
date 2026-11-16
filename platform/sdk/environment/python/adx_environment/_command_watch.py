"""One hidden multiplexed command-watch WebSocket per connection context."""

import asyncio
import json
import ssl
import threading
import time
import weakref
from collections import defaultdict
from typing import Dict, Optional, Set, Tuple

from ._command_metrics import increment
from .types import ConnectionConfig

_TERMINAL = frozenset(("SUCCEEDED", "FAILED", "TIMED_OUT", "KILLED"))
_WAKE_STATES = _TERMINAL | {"NOT_FOUND", "EXPIRED"}
_MANAGERS: "weakref.WeakValueDictionary[ConnectionConfig, _CommandWaitManager]" = weakref.WeakValueDictionary()
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
        self._async_waiters: Dict[Tuple[str, str], Set[Tuple[asyncio.AbstractEventLoop, asyncio.Future]]] = defaultdict(
            set
        )
        self._errors: Dict[Tuple[str, str], str] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._changed: Optional[asyncio.Event] = None
        self._thread: Optional[threading.Thread] = None
        self._runtime_observed = False

    def wait(self, environment_id: str, command_id: str, timeout: Optional[float]) -> None:
        key = (environment_id, command_id)
        event = threading.Event()
        with self._lock:
            self._waiters[key].add(event)
            self._errors.pop(key, None)
            self._ensure_thread_locked()
        self._signal_change()
        try:
            if not event.wait(timeout):
                from .commands import CommandWaitTimeout

                raise CommandWaitTimeout(environment_id, command_id, timeout)
            with self._lock:
                error = self._errors.get(key)
            if error:
                from .commands import CommandUnavailable

                raise CommandUnavailable(error, environment_id=environment_id, command_id=command_id)
        finally:
            with self._lock:
                waiters = self._waiters.get(key)
                if waiters is not None:
                    waiters.discard(event)
                    if not waiters:
                        self._waiters.pop(key, None)
                        self._errors.pop(key, None)
            self._signal_change()

    async def wait_async(self, environment_id: str, command_id: str, timeout: Optional[float]) -> None:
        key = (environment_id, command_id)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        async_waiter = (loop, future)
        with self._lock:
            self._async_waiters[key].add(async_waiter)
            self._errors.pop(key, None)
            self._ensure_thread_locked()
        self._signal_change()
        try:
            try:
                error = await asyncio.wait_for(future, timeout)
            except asyncio.TimeoutError as timeout_error:
                from .commands import CommandWaitTimeout

                raise CommandWaitTimeout(environment_id, command_id, timeout) from timeout_error
            if error:
                from .commands import CommandUnavailable

                raise CommandUnavailable(error, environment_id=environment_id, command_id=command_id)
        finally:
            with self._lock:
                waiters = self._async_waiters.get(key)
                if waiters is not None:
                    waiters.discard(async_waiter)
                    if not waiters:
                        self._async_waiters.pop(key, None)
                        if key not in self._waiters:
                            self._errors.pop(key, None)
            self._signal_change()

    def _signal_change(self) -> None:
        with self._lock:
            loop, changed = self._loop, self._changed
        if loop is not None and changed is not None:
            try:
                loop.call_soon_threadsafe(changed.set)
            except RuntimeError:
                # The worker's final desired-set check starts a replacement
                # thread when a waiter arrives during loop shutdown.
                pass

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
        loop = asyncio.get_running_loop()
        changed = asyncio.Event()
        with self._lock:
            self._loop, self._changed = loop, changed
        try:
            await self._run_loop(changed)
        finally:
            with self._lock:
                if self._loop is loop:
                    self._loop, self._changed = None, None

    def _mark_runtime_observed(self) -> None:
        self._runtime_observed = True

    async def _run_loop(self, changed: asyncio.Event) -> None:
        import websockets.asyncio.client as ws_client

        reconnect_budget = 30.0
        unavailable_since: Optional[float] = None
        while True:
            # Clear before reading the desired set so a concurrent change is
            # either included in that snapshot or wakes the subsequent wait.
            changed.clear()
            if not self._desired():
                try:
                    await asyncio.wait_for(changed.wait(), timeout=5)
                except asyncio.TimeoutError:
                    return
                continue
            scheme = "wss" if self._connection.use_tls else "ws"
            uri = f"{scheme}://{self._connection.server_address}/api/environment/v1/commands/watch"
            ssl_context = None
            if scheme == "wss":
                if self._connection.verify_tls:
                    ssl_context = ssl.create_default_context()
                else:
                    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
            try:
                self._runtime_observed = False
                token = self._connection.resolved_token()
                async with ws_client.connect(
                    uri,
                    additional_headers={"Authorization": f"Bearer {token}", "X-Auth": token},
                    ssl=ssl_context,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=1 << 20,
                ) as websocket:
                    await self._watch_socket(websocket, changed)
                    return
            except Exception as error:
                increment("command_wait_reconnect_total")
                if self._runtime_observed:
                    unavailable_since = None
                unavailable_since = unavailable_since or time.monotonic()
                if time.monotonic() - unavailable_since >= reconnect_budget:
                    for key in self._desired():
                        self._notify(key, f"command watch unavailable: {error}")
                    return
                await asyncio.sleep(0.2)

    async def _watch_socket(self, websocket, changed: asyncio.Event) -> None:
        sent: Set[Tuple[str, str]] = set()
        ready = False
        idle_since: Optional[float] = None
        receiver = asyncio.create_task(websocket.recv())
        try:
            while True:
                changed.clear()
                desired = self._desired()
                added, removed = desired - sent, sent - desired
                for op, commands in (("subscribe", added), ("unsubscribe", removed)):
                    if commands:
                        frame = {
                            "op": op,
                            "commands": [
                                {"environmentId": sid, "commandId": cid} for sid, cid in sorted(commands)
                            ],
                        }
                        if op == "subscribe":
                            frame["protocolVersion"] = 1
                        await websocket.send(json.dumps(frame))
                sent = desired
                if desired:
                    idle_since = None
                    idle_timeout = None
                else:
                    if idle_since is None:
                        idle_since = time.monotonic()
                    idle_timeout = max(0.0, 5 - (time.monotonic() - idle_since))
                update = asyncio.create_task(changed.wait())
                try:
                    done, _ = await asyncio.wait(
                        (receiver, update), timeout=idle_timeout, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    update.cancel()
                    await asyncio.gather(update, return_exceptions=True)
                if not done:
                    return
                if receiver not in done:
                    continue
                state = json.loads(receiver.result())
                receiver = asyncio.create_task(websocket.recv())
                if state.get("op") == "ready":
                    if state.get("protocolVersion") != 1 or "multiplexed-command-watch" not in state.get(
                        "capabilities", ()
                    ):
                        raise RuntimeError("server does not support command watch protocol v1")
                    ready = True
                    continue
                if not ready:
                    raise RuntimeError("ingress sent command state before protocol negotiation")
                key = (str(state.get("environmentId", "")), str(state.get("commandId", "")))
                if state.get("status") == "REJECTED":
                    self._notify(key, str(state.get("error", "command watch rejected")))
                elif str(state.get("status", "")) in _WAKE_STATES:
                    self._mark_runtime_observed()
                    self._notify(key)
                elif str(state.get("status", "")) in ("PENDING", "RUNNING"):
                    self._mark_runtime_observed()
        finally:
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)
