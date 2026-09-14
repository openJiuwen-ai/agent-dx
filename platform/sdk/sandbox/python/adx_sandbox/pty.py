# Copyright (c) 2026 Ant Group Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
"""Interactive pseudo-terminal support for adx sandboxes."""

from __future__ import annotations

import os
import shlex
import ssl
import threading
from collections.abc import Callable, Sequence

from ._pty_transport import (
    _build_pty_uri,
    _PtyConnection,
    _PtyTransportError,
)
from .types import ConnectionConfig


class PtyError(RuntimeError):
    """Raised when a PTY cannot start or terminates at the transport layer."""


def _validate_size(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")


def _normalize_command(command: str | Sequence[str]) -> list[str]:
    if isinstance(command, str):
        try:
            arguments = shlex.split(command)
        except ValueError as exc:
            raise ValueError(f"invalid PTY command: {exc}") from exc
    elif isinstance(command, Sequence) and not isinstance(command, (bytes, bytearray)):
        arguments = list(command)
    else:
        raise TypeError("command must be a string or a sequence of strings")
    if not arguments or not all(
        isinstance(argument, str) and argument for argument in arguments
    ):
        raise ValueError("command must contain at least one non-empty argument")
    return arguments


def _use_tls(connection: ConnectionConfig | None = None) -> bool:
    if connection is not None:
        return connection.use_tls
    if os.environ.get("ADX_SERVER_ADDRESS", "").strip():
        raw = os.environ.get("ADX_TLS", "1")
        return raw.strip().lower() not in ("0", "false", "no")
    gateway = os.environ.get("ADX_GATEWAY_ADDRESS", "").strip()
    if gateway:
        raw = os.environ.get("ADX_GATEWAY_TLS", "0")
    else:
        raw = os.environ.get("ADX_TLS", "1")
    return raw.strip().lower() not in ("0", "false", "no")


def _pty_server() -> str:
    return (
        os.environ.get("ADX_SERVER_ADDRESS", "").strip()
        or os.environ.get("ADX_GATEWAY_ADDRESS", "").strip()
    )


def _ssl_context(use_tls: bool, verify_tls: bool = False) -> ssl.SSLContext | None:
    if not use_tls:
        return None
    if verify_tls:
        context = ssl.create_default_context()
    else:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


class PtySession:
    """A connection-scoped interactive process running in a sandbox."""

    def __init__(
        self,
        connection: _PtyConnection,
        *,
        remove: Callable[[PtySession], None],
    ) -> None:
        self._connection = connection
        self._remove = remove

    @property
    def session_id(self) -> str:
        try:
            return self._connection.session_id
        except _PtyTransportError as exc:
            raise PtyError(str(exc)) from exc

    @property
    def exit_code(self) -> int | None:
        return self._connection.exit_code

    @property
    def done(self) -> bool:
        return self._connection.done

    def send_stdin(self, data: bytes) -> None:
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")
        try:
            self._connection.send_stdin(data)
        except _PtyTransportError as exc:
            raise PtyError(str(exc)) from exc

    def close_stdin(self) -> None:
        try:
            self._connection.close_stdin()
        except _PtyTransportError as exc:
            raise PtyError(str(exc)) from exc

    def resize(self, *, rows: int, cols: int) -> None:
        _validate_size("rows", rows)
        _validate_size("cols", cols)
        try:
            self._connection.resize(rows=rows, cols=cols)
        except _PtyTransportError as exc:
            raise PtyError(str(exc)) from exc

    def wait(self, timeout: float | None = None) -> int:
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        try:
            return self._connection.wait(timeout)
        except _PtyTransportError as exc:
            raise PtyError(str(exc)) from exc

    def close(self) -> None:
        self._connection.close()
        self._remove(self)

    def __enter__(self) -> PtySession:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class Pty:
    """Factory for interactive PTY sessions in one sandbox."""

    def __init__(
        self,
        instance_id: str,
        *,
        connection: ConnectionConfig | None = None,
    ) -> None:
        if connection is not None and not isinstance(connection, ConnectionConfig):
            raise TypeError("connection must be a ConnectionConfig or None")
        self._instance_id = instance_id
        self._connection_config = connection
        self._sessions: set[PtySession] = set()
        self._lock = threading.Lock()

    def create(
        self,
        command: str | Sequence[str] = "/bin/bash",
        *,
        rows: int = 24,
        cols: int = 80,
        on_data: Callable[[bytes], None] | None = None,
        timeout: float = 60,
    ) -> PtySession:
        arguments = _normalize_command(command)
        _validate_size("rows", rows)
        _validate_size("cols", cols)
        if on_data is not None and not callable(on_data):
            raise TypeError("on_data must be callable")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise TypeError("timeout must be a number")
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")

        if self._connection_config is not None:
            token = self._connection_config.token
            server = self._connection_config.server_address
        else:
            token = os.environ.get("ADX_TOKEN", "").strip()
            if not token:
                raise RuntimeError("ADX_TOKEN is not set")
            server = _pty_server()
            if not server:
                raise RuntimeError(
                    "ADX_SERVER_ADDRESS or ADX_GATEWAY_ADDRESS is not set"
                )
        use_tls = _use_tls(self._connection_config)
        uri = _build_pty_uri(
            server=server,
            use_tls=use_tls,
            instance_id=self._instance_id,
            token=token,
            command=arguments,
            rows=rows,
            cols=cols,
        )

        session_ref: list[PtySession] = []

        def on_done() -> None:
            if session_ref:
                self._remove(session_ref[0])

        transport = _PtyConnection(
            uri,
            ssl_context=_ssl_context(
                use_tls,
                self._connection_config.verify_tls if self._connection_config else False,
            ),
            rows=rows,
            cols=cols,
            on_data=on_data,
            on_done=on_done,
        )
        session = PtySession(transport, remove=self._remove)
        session_ref.append(session)
        with self._lock:
            self._sessions.add(session)
        try:
            transport.start(float(timeout))
        except (TimeoutError, _PtyTransportError) as exc:
            transport.close()
            self._remove(session)
            if isinstance(exc, TimeoutError):
                raise
            raise PtyError(str(exc)) from exc
        return session

    def _remove(self, session: PtySession) -> None:
        with self._lock:
            self._sessions.discard(session)

    def _close(self) -> None:
        with self._lock:
            sessions = list(self._sessions)
        for session in sessions:
            session.close()
