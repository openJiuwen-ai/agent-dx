#!/usr/bin/env python3
# coding=UTF-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Sandbox filesystem copy operations.

All actual data transfer is delegated to :mod:`yr.cli.exec` so the transport
layer is not duplicated:

* :func:`yr.cli.exec.copy_to_remote` / :func:`~yr.cli.exec.copy_to_remote_streaming`
* :func:`yr.cli.exec.copy_from_remote` / :func:`~yr.cli.exec.copy_from_remote_streaming`
* :func:`yr.cli.exec.choose_cp_mode` – auto mode selection
"""

import asyncio
import os
import fnmatch
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from yr.agentexecutor.sandbox.sandbox import Sandbox


class CpDirection(Enum):
    """Transfer direction for sandbox copy operations.

    ``UPLOAD``   – copy from local filesystem **into** the sandbox (default).
    ``DOWNLOAD`` – copy from the sandbox **out** to the local filesystem.
    """

    UPLOAD = "upload"
    DOWNLOAD = "download"


# ── Filesystem I/O implementations ──
# These functions contain the actual file operation logic and are called
# by SandboxInstance inside the sandbox. They are placed here (not in
# sandbox.py) so that all filesystem logic is co-located.

def _build_fs_item(entry_path: str) -> Dict:
    """Build a filesystem item dict matching jiuwenbox API format.

    Fields: name, path, size, is_directory, modified_time, type
    """
    entry_name = os.path.basename(entry_path)
    is_dir = os.path.isdir(entry_path)
    try:
        size = os.path.getsize(entry_path) if not is_dir else 0
    except OSError:
        size = 0
    try:
        mtime = datetime.fromtimestamp(
            os.path.getmtime(entry_path)
        ).strftime("%Y-%m-%dT%H:%M:%S.%f")
    except OSError:
        mtime = None
    if is_dir:
        item_type = None
    else:
        _, ext = os.path.splitext(entry_name)
        item_type = ext if ext else None
    return {
        "name": entry_name,
        "path": entry_path,
        "size": size,
        "is_directory": is_dir,
        "modified_time": mtime,
        "type": item_type,
    }


def _read_file_impl(path: str, mode: str = "rb"):
    """Read a file using native Python I/O."""
    with open(path, mode) as f:
        return f.read()


def _write_file_impl(path: str, data, mode: str = "wb") -> None:
    """Write data to a file using native Python I/O.

    Parent directories are created automatically.
    """
    parent_dir = os.path.dirname(path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    with open(path, mode) as f:
        f.write(data)


_MAX_LIST_ENTRIES = 10000
_MAX_LIST_DEPTH = 20


def _list_files_impl(
    path: str,
    recursive: bool = False,
    max_depth: Optional[int] = None,
    include_files: bool = True,
    include_dirs: bool = True,
) -> List[Dict]:
    """List files and directories using native os.listdir.

    Raises FileNotFoundError when the root path does not exist.

    Safety limits:
    - _MAX_LIST_ENTRIES: caps total entries to prevent OOM when scanning
      directories with a very large number of files.
    - _MAX_LIST_DEPTH: caps recursion depth to prevent stack overflow on
      pathological directory structures.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"path not found: {path}")

    result: List[Dict] = []

    effective_max_depth = max_depth if (max_depth is not None and max_depth > 0) else _MAX_LIST_DEPTH

    def _scan(dir_path: str, current_depth: int) -> None:
        if len(result) >= _MAX_LIST_ENTRIES:
            return
        if current_depth > effective_max_depth:
            return
        try:
            entries = os.listdir(dir_path)
        except OSError:
            return
        for entry in entries:
            if len(result) >= _MAX_LIST_ENTRIES:
                return
            entry_path = os.path.join(dir_path, entry)
            is_dir = os.path.isdir(entry_path)
            if is_dir:
                if include_dirs:
                    result.append(_build_fs_item(entry_path))
                if recursive:
                    _scan(entry_path, current_depth + 1)
            else:
                if include_files:
                    result.append(_build_fs_item(entry_path))

    _scan(path, 0)
    return result


def _search_files_impl(
    path: str,
    pattern: str,
    exclude_patterns: Optional[List[str]] = None,
) -> List[Dict]:
    """Search files by glob pattern using fnmatch."""
    excludes = exclude_patterns or []
    result: List[Dict] = []

    def _is_excluded(entry_name: str) -> bool:
        return any(fnmatch.fnmatch(entry_name, pat) for pat in excludes)

    def _search(dir_path: str) -> None:
        try:
            entries = os.listdir(dir_path)
        except OSError:
            return
        for entry in entries:
            entry_path = os.path.join(dir_path, entry)
            if os.path.isdir(entry_path):
                _search(entry_path)
            else:
                if fnmatch.fnmatch(entry, pattern) and not _is_excluded(entry):
                    result.append(_build_fs_item(entry_path))

    _search(path)
    return result


def _get_gateway_host() -> str:
    """Return the faasfrontend host:port for exec WebSocket connections.

    Resolution order:
    1. ``YR_GATEWAY_ADDRESS`` environment variable
    2. ``YR_SERVER_ADDRESS`` environment variable
    3. :class:`~yr.config_manager.ConfigManager` ``server_address``
    """
    from yr.config_manager import ConfigManager

    host = os.environ.get("YR_GATEWAY_ADDRESS", "").strip()
    if host:
        return host
    addr = os.environ.get("YR_SERVER_ADDRESS", "").strip()
    if addr:
        return addr
    return ConfigManager().server_address.strip()


class SandboxFilesystem:
    """Namespace for sandbox filesystem copy operations.

    Access via :attr:`~yr.sandbox.sandbox.Sandbox.filesystem`.

    Transport is fully delegated to :mod:`yr.cli.exec` — no duplicate
    implementation of tar/gzip streaming logic.

    Example::

        sb = yr.sandbox.create()

        # Upload a local file into the sandbox
        sb.filesystem.copy_from_local("/local/data.csv", "/sandbox/data.csv")

        # Download a file from the sandbox to a local path
        sb.filesystem.copy_to_local("/sandbox/output.txt", "/local/output.txt")
    """

    def __init__(self, sandbox: "Sandbox") -> None:
        self._sandbox = sandbox

    @staticmethod
    def _env_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
        """Parse a tri-state boolean env var (true/false/unset)."""
        raw = os.environ.get(name)
        if raw is None:
            return default
        v = raw.strip().lower()
        if v in ("1", "true", "yes", "on"):
            return True
        if v in ("0", "false", "no", "off"):
            return False
        return default

    def copy_from_local(
        self,
        local_path: str,
        remote_path: str,
        streaming: Optional[bool] = None,
        trace_id: Optional[str] = None,
    ) -> None:
        """Copy a local file or directory **into** the sandbox.

        Args:
            local_path:  Absolute or relative path on the **local** machine.
            remote_path: Absolute path inside the **sandbox**.
            streaming:   ``None`` = auto, ``True`` = gzip streaming,
                         ``False`` = non-streaming tar.
            trace_id:    Trace id for link tracing, sent as the ``trace`` query
                         parameter on the exec websocket. None (default) omits it.

        Raises:
            FileNotFoundError: *local_path* does not exist.
            RuntimeError: Server address is not configured.
        """
        self._cp(local_path, remote_path, CpDirection.UPLOAD, streaming, trace_id=trace_id)

    def copy_to_local(
        self,
        remote_path: str,
        local_path: str,
        streaming: Optional[bool] = None,
        trace_id: Optional[str] = None,
    ) -> None:
        """Copy a file or directory **from** the sandbox to the local machine.

        Args:
            remote_path: Absolute path inside the **sandbox**.
            local_path:  Absolute or relative path on the **local** machine.
            streaming:   ``None`` = auto, ``True`` = gzip streaming,
                         ``False`` = non-streaming tar.
            trace_id:    Trace id for link tracing, sent as the ``trace`` query
                         parameter on the exec websocket. None (default) omits it.

        Raises:
            RuntimeError: Server address is not configured.
        """
        self._cp(remote_path, local_path, CpDirection.DOWNLOAD, streaming, trace_id=trace_id)

    def _resolve_transport_kwargs(self, host: str, port: str) -> dict:
        """Auto-resolve transport kwargs (ssl/token/cert/verify) from yr config.

        Mirrors the logic used by ``yrcli exec`` / ``yrcli cp`` so that
        :meth:`copy_from_local` / :meth:`copy_to_local` honour the same
        configuration already passed to :func:`yr.init`.

        Resolution rules:

        * **use_ssl** — ``True`` if any of: port is ``443``,
          ``ConfigManager.enable_tls`` is set, client cert+key both configured,
          CA file configured, or env ``YR_USE_SSL`` is truthy.
        * **cert_file / key_file / ca_file** — taken from
          ``ConfigManager.certificate_file_path`` / ``private_key_path`` /
          ``verify_file_path`` (empty string → ``None``).
        * **verify_server** — ``True`` by default; ``False`` if env
          ``YR_INSECURE`` is truthy or ``YR_VERIFY_SERVER`` is falsey.
        * **token** — ``ConfigManager.auth_token`` or env ``YR_AUTH_TOKEN``.
        """
        from yr.config_manager import ConfigManager

        cfg = ConfigManager()

        def _nonempty(s):
            s = (s or "").strip()
            return s or None

        cert_file = _nonempty(getattr(cfg, "certificate_file_path", ""))
        key_file = _nonempty(getattr(cfg, "private_key_path", ""))
        ca_file = _nonempty(getattr(cfg, "verify_file_path", ""))
        token = _nonempty(getattr(cfg, "auth_token", "")) or _nonempty(
            os.environ.get("YR_AUTH_TOKEN")
        )

        use_ssl = bool(
            port == "443"
            or getattr(cfg, "enable_tls", False)
            or (cert_file and key_file)
            or ca_file
        )
        env_ssl = self._env_bool("YR_USE_SSL")
        if env_ssl is not None:
            use_ssl = env_ssl

        insecure = self._env_bool("YR_INSECURE", False)
        verify_server = self._env_bool("YR_VERIFY_SERVER", True)
        if insecure:
            verify_server = False

        return {
            "use_ssl": use_ssl,
            "cert_file": cert_file,
            "key_file": key_file,
            "ca_file": ca_file,
            "verify_server": verify_server,
            "token": token,
        }

    def _get_connection(self):
        """Resolve ``(host, port, instance_id, transport_kwargs)`` for exec WS.

        The exec WebSocket is served by faasfrontend, so the address is
        resolved via :func:`_get_gateway_host`, not the bus address used by
        :func:`yr.init`. Transport kwargs (ssl/token/cert/verify) are derived
        from :class:`~yr.config_manager.ConfigManager` and environment
        variables — see :meth:`_resolve_transport_kwargs`.

        Raises:
            RuntimeError: If no server address is configured.
        """
        import yr

        gateway_addr = _get_gateway_host()
        if not gateway_addr:
            raise RuntimeError(
                "Server address is not configured. "
                "Set YR_SERVER_ADDRESS (faasfrontend address) or call yr.init()."
            )
        host, port = gateway_addr.rsplit(":", 1)
        instance = getattr(self._sandbox, "_instance")
        instance_id = yr.get(instance.get_name.invoke())
        transport_kwargs = self._resolve_transport_kwargs(host, port)
        return host, port, instance_id, transport_kwargs

    def _cp(
        self,
        src: str,
        dst: str,
        direction: CpDirection = CpDirection.UPLOAD,
        streaming: Optional[bool] = None,
        trace_id: Optional[str] = None,
    ) -> None:
        """Core copy implementation; delegates to :mod:`yr.cli.exec` transport.

        Args:
            src:       Source path.
            dst:       Destination path.
            direction: ``UPLOAD`` → src is local, dst is sandbox path.
                       ``DOWNLOAD`` → src is sandbox path, dst is local.
            streaming: ``None`` = auto-select, ``True`` = gzip streaming,
                       ``False`` = non-streaming tar.
            trace_id:  Trace id for link tracing, appended as the ``trace``
                       query parameter on the exec websocket. None (default)
                       omits it.

        Raises:
            FileNotFoundError: Local source not found (upload only).
            RuntimeError: Server address not configured.
        """
        from yr.cli.exec import (
            ExecConnection,
            CopyRequest,
            copy_to_remote,
            copy_from_remote,
            copy_to_remote_streaming,
            copy_from_remote_streaming,
            choose_cp_mode,
        )

        upload = direction == CpDirection.UPLOAD
        local_path = src if upload else dst
        remote_path = dst if upload else src

        if upload and not os.path.exists(local_path):
            raise FileNotFoundError(f"Local source path not found: {local_path}")

        host, port, instance_id, transport_kwargs = self._get_connection()
        connection = ExecConnection(host=host, port=port, **transport_kwargs)

        if streaming is None:
            streaming = choose_cp_mode(local_path, remote_path, upload=upload)

        if upload:
            fn = copy_to_remote_streaming if streaming else copy_to_remote
            asyncio.run(
                fn(
                    connection,
                    CopyRequest(
                        instance=instance_id,
                        local_path=local_path,
                        remote_path=remote_path,
                        trace_id=trace_id,
                    ),
                )
            )
        else:
            fn = copy_from_remote_streaming if streaming else copy_from_remote
            asyncio.run(
                fn(
                    connection,
                    CopyRequest(
                        instance=instance_id,
                        local_path=local_path,
                        remote_path=remote_path,
                        trace_id=trace_id,
                    ),
                )
            )
