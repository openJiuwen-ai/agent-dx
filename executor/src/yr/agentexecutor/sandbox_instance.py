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

"""Process-local SandboxInstance used by the AgentExecutor Sandbox API.

This class intentionally does not use ``yr`` or ``@yr.instance``.  The Agent
process and the Executor already share one function-instance container, so the
Executor owns one local SandboxInstance and exposes its operations over the
loopback-only HTTP API.
"""

from __future__ import annotations

import fnmatch
import os
import signal
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

DEFAULT_MAX_LIST_ENTRIES = 10000
DEFAULT_MAX_LIST_DEPTH = 20
DEFAULT_EXECUTION_TIMEOUT_SECONDS = 300.0
PROCESS_KILL_WAIT_SECONDS = 1.0


class SandboxInstance:
    """Provides SandboxInstance-compatible operations in the current container."""

    def __init__(
        self,
        working_dir: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        *,
        max_list_entries: int = DEFAULT_MAX_LIST_ENTRIES,
        max_list_depth: int = DEFAULT_MAX_LIST_DEPTH,
        execution_timeout_seconds: float = DEFAULT_EXECUTION_TIMEOUT_SECONDS,
    ) -> None:
        if working_dir is None:
            self._temp_dir = tempfile.TemporaryDirectory(prefix="yr_sandbox_")
            self.working_dir = self._temp_dir.name
        else:
            self._temp_dir = None
            self.working_dir = working_dir
        self.env = dict(os.environ if env is None else env)
        self.max_list_entries = max_list_entries
        self.max_list_depth = max_list_depth
        if execution_timeout_seconds <= 0:
            raise ValueError("execution_timeout_seconds must be greater than zero")
        self.execution_timeout_seconds = execution_timeout_seconds
        self._initialized = True

    def __del__(self) -> None:
        try:
            self.cleanup()
        except Exception:  # noqa: BLE001 - destructors must never escape
            pass

    def execute(
        self,
        command: Union[str, Sequence[str]],
        working_dir: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Execute one command with the same result shape as SandboxInstance."""
        if not self._initialized:
            raise RuntimeError("Sandbox is not initialized")
        if isinstance(command, str):
            cmd_args = ["/bin/sh", "-c", command]
        elif isinstance(command, (list, tuple)):
            if not command:
                return self._command_error("Error: cmd list cannot be empty")
            if not all(isinstance(argument, str) for argument in command):
                return self._command_error("Error: All elements in command list must be strings")
            cmd_args = list(command)
        else:
            return self._command_error(
                "Error: cmd must be a string or a list of strings, "
                f"got {type(command).__name__}"
            )

        try:
            process = subprocess.Popen(
                args=cmd_args,
                shell=False,
                cwd=self.working_dir if working_dir is None else working_dir,
                env=self.env if env is None else dict(env),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
        except Exception as exc:  # noqa: BLE001 - compatibility returns command errors
            return self._command_error(str(exc))

        effective_timeout = self.execution_timeout_seconds if timeout is None else timeout
        try:
            stdout, stderr = process.communicate(timeout=effective_timeout)
            return {
                "returncode": process.returncode,
                "stdout": stdout,
                "stderr": stderr,
            }
        except subprocess.TimeoutExpired as exc:
            self._kill_process_group(process)
            stdout = self._drain_after_kill(process, self._timeout_text(exc.stdout))
            return {
                "returncode": -1,
                "stdout": stdout,
                "stderr": f"Command timed out after {effective_timeout:g} seconds",
            }
        except Exception as exc:  # noqa: BLE001 - compatibility returns command errors
            self._kill_process_group(process)
            self._drain_after_kill(process)
            return self._command_error(str(exc))

    @staticmethod
    def read_file(path: str, mode: str = "rb") -> Union[str, bytes]:
        if mode not in {"r", "rb"}:
            raise ValueError("read mode must be 'r' or 'rb'")
        with open(path, mode) as source:
            return source.read()

    @staticmethod
    def write_file(path: str, data: Union[str, bytes], mode: str = "wb") -> None:
        if mode not in {"w", "wb", "a", "ab"}:
            raise ValueError("write mode must be one of 'w', 'wb', 'a', or 'ab'")
        binary = "b" in mode
        if binary and not isinstance(data, bytes):
            raise TypeError("binary write mode requires bytes data")
        if not binary and not isinstance(data, str):
            raise TypeError("text write mode requires string data")
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open(mode) as destination:
            destination.write(data)

    def list_files(
        self,
        path: str,
        recursive: bool = False,
        max_depth: Optional[int] = None,
        include_files: bool = True,
        include_dirs: bool = True,
    ) -> List[Dict[str, Any]]:
        root = Path(path)
        if not root.exists():
            raise FileNotFoundError(f"path not found: {path}")
        if not root.is_dir():
            return [self._build_item(root)] if include_files else []

        effective_depth = (
            max_depth
            if max_depth is not None and max_depth > 0
            else self.max_list_depth
        )
        result: List[Dict[str, Any]] = []

        def scan(directory: Path, current_depth: int) -> None:
            if len(result) >= self.max_list_entries or current_depth > effective_depth:
                return
            try:
                entries = list(directory.iterdir())
            except OSError:
                return
            for entry in entries:
                if len(result) >= self.max_list_entries:
                    return
                try:
                    is_directory = entry.is_dir()
                except OSError:
                    continue
                if is_directory:
                    if include_dirs:
                        result.append(self._build_item(entry))
                    if recursive:
                        scan(entry, current_depth + 1)
                elif include_files:
                    result.append(self._build_item(entry))

        scan(root, 0)
        return result

    def search_files(
        self,
        path: str,
        pattern: str,
        exclude_patterns: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        root = Path(path)
        if not root.exists():
            raise FileNotFoundError(f"path not found: {path}")
        if not root.is_dir():
            raise NotADirectoryError(f"path is not a directory: {path}")
        excludes = list(exclude_patterns or [])
        result: List[Dict[str, Any]] = []

        def scan(directory: Path, depth: int) -> None:
            if len(result) >= self.max_list_entries or depth > self.max_list_depth:
                return
            try:
                entries = list(directory.iterdir())
            except OSError:
                return
            for entry in entries:
                if len(result) >= self.max_list_entries:
                    return
                try:
                    if entry.is_dir():
                        scan(entry, depth + 1)
                    elif fnmatch.fnmatch(entry.name, pattern) and not any(
                        fnmatch.fnmatch(entry.name, excluded) for excluded in excludes
                    ):
                        result.append(self._build_item(entry))
                except OSError:
                    continue

        scan(root, 0)
        return result

    def cleanup(self) -> None:
        if not self._initialized:
            return
        self._initialized = False
        if self._temp_dir is not None:
            self._temp_dir.cleanup()
            self._temp_dir = None

    @staticmethod
    def _build_item(path: Path) -> Dict[str, Any]:
        is_directory = path.is_dir()
        try:
            size = 0 if is_directory else path.stat().st_size
        except OSError:
            size = 0
        try:
            modified_time = datetime.fromtimestamp(path.stat().st_mtime).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )
        except OSError:
            modified_time = None
        item_type = None if is_directory else (path.suffix or None)
        return {
            "name": path.name,
            "path": str(path),
            "size": size,
            "is_directory": is_directory,
            "modified_time": modified_time,
            "type": item_type,
        }

    @staticmethod
    def _command_error(message: str) -> Dict[str, Any]:
        return {"returncode": -1, "stdout": "", "stderr": message}

    @staticmethod
    def _kill_process_group(process: subprocess.Popen) -> None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        except OSError:
            try:
                process.kill()
            except OSError:
                pass

    @classmethod
    def _drain_after_kill(cls, process: subprocess.Popen, fallback: str = "") -> str:
        try:
            stdout, _ = process.communicate(timeout=PROCESS_KILL_WAIT_SECONDS)
            return stdout
        except subprocess.TimeoutExpired as exc:
            stdout = cls._timeout_text(exc.stdout) or fallback
            for pipe in (process.stdout, process.stderr):
                if pipe is not None:
                    try:
                        pipe.close()
                    except OSError:
                        pass
            try:
                process.wait(timeout=PROCESS_KILL_WAIT_SECONDS)
            except (OSError, subprocess.TimeoutExpired):
                pass
            return stdout
        except Exception:  # noqa: BLE001 - preserve the original execution result
            return fallback

    @staticmethod
    def _timeout_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)
