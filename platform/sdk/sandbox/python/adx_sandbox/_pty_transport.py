# Copyright (c) 2026 Ant Group Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Thread-backed WebSocket transport for the synchronous PTY API."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable, Sequence
from concurrent.futures import Future
from typing import Any
from urllib.parse import quote, urlencode

_PROTOCOL = "sandbox.pty.v1"
_PROTOCOL_VERSION = 1
_WRITE_TIMEOUT = 10.0


class _PtyTransportError(RuntimeError):
    """Internal transport failure converted to PtyError by the public API."""


def _build_pty_uri(
    *,
    server: str,
    use_tls: bool,
    instance_id: str,
    command: Sequence[str],
    rows: int,
    cols: int,
) -> str:
    parameters: list[tuple[str, str]] = [
        ("tty", "true"),
        ("rows", str(rows)),
        ("cols", str(cols)),
        ("protocol", _PROTOCOL),
    ]
    parameters.extend(("command", argument) for argument in command)
    scheme = "wss" if use_tls else "ws"
    authority = server.removeprefix("https://").removeprefix("http://").rstrip("/")
    instance = quote(instance_id, safe="")
    return f"{scheme}://{authority}/direct/{instance}/pty?{urlencode(parameters)}"


class _PtyConnection:
    def __init__(
        self,
        uri: str,
        *,
        token: str,
        ssl_context: Any,
        rows: int,
        cols: int,
        on_data: Callable[[bytes], None] | None,
        on_done: Callable[[], None],
    ) -> None:
        self._uri = uri
        self._token = token
        self._ssl_context = ssl_context
        self._rows = rows
        self._cols = cols
        self._on_data = on_data
        self._on_done = on_done
        self._ready = threading.Event()
        self._done = threading.Event()
        self._state_lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._thread_main,
            name="adx-sandbox-pty",
            daemon=True,
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._run_task: asyncio.Task[None] | None = None
        self._websocket: Any = None
        self._session_id: str | None = None
        self._exit_code: int | None = None
        self._error: BaseException | None = None
        self._closed_by_client = False

    @property
    def session_id(self) -> str:
        if self._session_id is None:
            raise _PtyTransportError("PTY session has not started")
        return self._session_id

    @property
    def exit_code(self) -> int | None:
        return self._exit_code

    @property
    def done(self) -> bool:
        return self._done.is_set()

    def start(self, timeout: float) -> None:
        self._thread.start()
        if not self._ready.wait(timeout):
            self.close()
            raise TimeoutError(f"PTY did not start within {timeout:g} seconds")
        self._raise_error()
        if self._session_id is None:
            raise _PtyTransportError("PTY connection closed before it started")

    def send_stdin(self, data: bytes) -> None:
        self._send(data)

    def close_stdin(self) -> None:
        self._send("STDIN_EOF")

    def resize(self, *, rows: int, cols: int) -> None:
        self._send(f"RESIZE:{cols}:{rows}")

    def wait(self, timeout: float | None) -> int:
        if not self._done.wait(timeout):
            raise TimeoutError(f"PTY did not exit within {timeout:g} seconds")
        self._raise_error()
        if self._exit_code is None:
            if self._closed_by_client:
                raise _PtyTransportError("PTY session was closed by the client")
            raise _PtyTransportError("PTY connection closed without an exit status")
        return self._exit_code

    def close(self) -> None:
        with self._state_lock:
            if self._closed_by_client:
                return
            self._closed_by_client = True
            loop = self._loop
            run_task = self._run_task
            websocket = self._websocket
        if loop is not None and websocket is not None and not self._done.is_set():
            future = asyncio.run_coroutine_threadsafe(websocket.close(), loop)
            try:
                future.result(timeout=_WRITE_TIMEOUT)
            except Exception:
                pass
        elif loop is not None and run_task is not None and not run_task.done():
            loop.call_soon_threadsafe(run_task.cancel)
        if self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(timeout=_WRITE_TIMEOUT)

    def _send(self, data: bytes | str) -> None:
        if self._done.is_set():
            self._raise_error()
            raise _PtyTransportError("PTY session is no longer running")
        if threading.current_thread() is self._thread:
            raise _PtyTransportError(
                "PTY methods cannot be called from the on_data callback"
            )
        with self._state_lock:
            loop = self._loop
            websocket = self._websocket
        if loop is None or websocket is None:
            raise _PtyTransportError("PTY transport is not connected")
        future: Future[Any] = asyncio.run_coroutine_threadsafe(
            websocket.send(data), loop
        )
        try:
            future.result(timeout=_WRITE_TIMEOUT)
        except Exception as exc:
            raise _PtyTransportError(f"failed to write to PTY: {exc}") from exc

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run())
        except BaseException as exc:
            self._set_error(exc)
        finally:
            self._ready.set()
            self._done.set()
            try:
                self._on_done()
            except Exception:
                pass

    async def _run(self) -> None:
        import websockets.asyncio.client as ws_client

        loop = asyncio.get_running_loop()
        with self._state_lock:
            self._loop = loop
            self._run_task = asyncio.current_task()
        try:
            async with ws_client.connect(
                self._uri,
                additional_headers={
                    "Authorization": f"Bearer {self._token}",
                    "X-Auth": self._token,
                },
                ssl=self._ssl_context,
                ping_interval=20,
                ping_timeout=10,
            ) as websocket:
                with self._state_lock:
                    self._websocket = websocket
                await websocket.send(f"RESIZE:{self._cols}:{self._rows}")
                async for message in websocket:
                    if isinstance(message, bytes):
                        if self._on_data is not None:
                            try:
                                self._on_data(message)
                            except BaseException as exc:
                                self._set_error(
                                    _PtyTransportError(
                                        f"PTY on_data callback failed: {exc}"
                                    )
                                )
                                await websocket.close()
                                return
                        continue
                    self._handle_control(message)
                    if self._done.is_set():
                        await websocket.close()
                        await websocket.wait_closed()
                        return
        except BaseException as exc:
            if not self._closed_by_client and not self._done.is_set():
                self._set_error(
                    _PtyTransportError(f"PTY WebSocket connection failed: {exc}")
                )
        finally:
            with self._state_lock:
                self._websocket = None
                self._loop = None
                self._run_task = None
        if not self._closed_by_client and not self._done.is_set():
            self._set_error(
                _PtyTransportError("PTY connection closed before terminal status")
            )

    def _handle_control(self, message: str) -> None:
        try:
            event = json.loads(message)
        except json.JSONDecodeError as exc:
            self._set_error(_PtyTransportError(f"invalid PTY control frame: {exc}"))
            return
        if not isinstance(event, dict) or event.get("version") != _PROTOCOL_VERSION:
            self._set_error(_PtyTransportError("unsupported PTY control frame"))
            return
        event_type = event.get("type")
        session_id = event.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            self._set_error(_PtyTransportError("PTY control frame has no session_id"))
            return
        if event_type == "started":
            self._session_id = session_id
            self._ready.set()
        elif event_type == "exited":
            exit_code = event.get("exit_code")
            if isinstance(exit_code, bool) or not isinstance(exit_code, int):
                self._set_error(_PtyTransportError("PTY exit frame has no exit_code"))
                return
            self._session_id = session_id
            self._exit_code = exit_code
            self._ready.set()
            self._done.set()
        elif event_type == "error":
            detail = event.get("message")
            if not isinstance(detail, str):
                detail = "unknown error"
            self._session_id = session_id
            self._set_error(_PtyTransportError(f"remote PTY error: {detail}"))
        else:
            self._set_error(
                _PtyTransportError(f"unknown PTY control event {event_type!r}")
            )

    def _set_error(self, error: BaseException) -> None:
        with self._state_lock:
            if self._error is None:
                self._error = error
        self._ready.set()
        self._done.set()

    def _raise_error(self) -> None:
        with self._state_lock:
            error = self._error
        if error is not None:
            raise error
