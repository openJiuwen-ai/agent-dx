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

"""Streaming filesystem operations used by the executor HTTP server."""

from __future__ import annotations

import os
import re
import stat as stat_module
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Optional

COPY_BUFFER_SIZE = 1024 * 1024
DEFAULT_MAX_FILE_SIZE = 512 * 1024 * 1024
# Keep these safeguards aligned with yuanrong/api/python/yr/sandbox/filesystem.py.
DEFAULT_MAX_LIST_ENTRIES = 10000
DEFAULT_MAX_LIST_DEPTH = 20
DEFAULT_LIST_TIMEOUT_SECONDS = 30
_PERMISSION_PATTERN = re.compile(r"^[0-7]{3,4}$")


class FileListTimeoutError(TimeoutError):
    """Raised when a recursive directory scan exceeds its time budget."""


def validate_path(path: str) -> Path:
    if not isinstance(path, str) or not path.strip():
        raise ValueError("path is required")
    if "\x00" in path:
        raise ValueError("path contains a NUL byte")
    return Path(path)


def validate_permissions(mode: str) -> Optional[int]:
    if not mode:
        return None
    if not isinstance(mode, str) or not _PERMISSION_PATTERN.fullmatch(mode):
        raise ValueError(
            f"invalid mode '{mode}', expected 3-4 digit octal (e.g. '600', '755')"
        )
    return int(mode, 8)


class FileHandler:
    """Provides atomic upload, ranged download metadata, and directory listing."""

    def __init__(
        self,
        max_file_size: int = DEFAULT_MAX_FILE_SIZE,
        max_list_entries: int = DEFAULT_MAX_LIST_ENTRIES,
        max_list_depth: int = DEFAULT_MAX_LIST_DEPTH,
        list_timeout_seconds: float = DEFAULT_LIST_TIMEOUT_SECONDS,
    ) -> None:
        self.max_file_size = max_file_size
        self.max_list_entries = max_list_entries
        self.max_list_depth = max_list_depth
        self.list_timeout_seconds = list_timeout_seconds

    def upload(self, path: str, source: BinaryIO, mode: str = "") -> dict:
        target = validate_path(path)
        permissions = validate_permissions(mode)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".upload", dir=target.parent)
        size = 0
        try:
            with os.fdopen(fd, "wb") as destination:
                while True:
                    chunk = source.read(COPY_BUFFER_SIZE)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > self.max_file_size:
                        raise ValueError(f"upload size exceeds max {self.max_file_size}")
                    destination.write(chunk)
                destination.flush()
                os.fsync(destination.fileno())
            if permissions is not None:
                os.chmod(temporary_name, permissions)
            os.replace(temporary_name, target)
        except BaseException:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise
        return {"success": True, "path": str(target), "size": size}

    def open_download(self, path: str, start: int = 0, end: Optional[int] = None):
        target = validate_path(path)
        stream = target.open("rb")
        try:
            file_stat = os.fstat(stream.fileno())
            if not stat_module.S_ISREG(file_stat.st_mode):
                raise ValueError("path must identify a regular file")
            total_size = file_stat.st_size
            effective_end, length = self.resolve_download_range(total_size, start, end)
            stream.seek(start)
        except BaseException:
            stream.close()
            raise
        return stream, total_size, effective_end, length

    @staticmethod
    def resolve_download_range(
        total_size: int, start: int = 0, end: Optional[int] = None
    ) -> tuple[int, int]:
        if start < 0 or (start >= total_size and total_size != 0):
            raise ValueError("requested range is not satisfiable")
        effective_end = total_size - 1 if end is None else min(end, total_size - 1)
        if effective_end < start and total_size != 0:
            raise ValueError("requested range is not satisfiable")
        length = 0 if total_size == 0 else effective_end - start + 1
        return effective_end, length

    def list(self, path: str, recursive: bool = False, max_depth: int = 0) -> dict:
        root = validate_path(path)
        if not root.exists():
            raise FileNotFoundError(str(root))
        if not root.is_dir():
            return {"items": [self._item(root)]}

        items = []
        effective_max_depth = max_depth if max_depth > 0 else self.max_list_depth
        deadline = time.monotonic() + self.list_timeout_seconds
        pending = [(root, 0)]
        while pending and len(items) < self.max_list_entries:
            if time.monotonic() >= deadline:
                raise FileListTimeoutError(
                    f"file list exceeded {self.list_timeout_seconds:g} seconds"
                )
            directory, parent_depth = pending.pop()
            for item in directory.iterdir():
                if time.monotonic() >= deadline:
                    raise FileListTimeoutError(
                        f"file list exceeded {self.list_timeout_seconds:g} seconds"
                    )
                depth = parent_depth + 1
                try:
                    item_data = self._item(item)
                except OSError:
                    # An entry may disappear between iterdir() and stat().
                    # Skip that entry without failing the whole directory list.
                    continue
                items.append(item_data)
                if len(items) >= self.max_list_entries:
                    break
                if recursive and item_data["is_directory"] and depth < effective_max_depth:
                    pending.append((item, depth))
            if not recursive:
                break
        items.sort(key=lambda value: value["path"])
        return {"items": items}

    @staticmethod
    def copy_range(source: BinaryIO, destination: BinaryIO, length: int) -> None:
        remaining = length
        while remaining > 0:
            chunk = source.read(min(COPY_BUFFER_SIZE, remaining))
            if not chunk:
                break
            destination.write(chunk)
            remaining -= len(chunk)

    @staticmethod
    def _item(path: Path) -> dict:
        try:
            file_stat = path.stat()
        except FileNotFoundError:
            # stat() follows symlinks. Fall back to lstat() so a dangling
            # symlink is represented as a non-directory entry.
            file_stat = path.lstat()
        is_directory = stat_module.S_ISDIR(file_stat.st_mode)
        return {
            "name": path.name,
            "path": str(path),
            "size": file_stat.st_size,
            "is_directory": is_directory,
            "modified_time": datetime.fromtimestamp(file_stat.st_mtime, timezone.utc).isoformat(),
            "type": "directory" if is_directory else "file",
        }
