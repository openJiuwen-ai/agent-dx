"""Filesystem helpers for sandbox v1.

Small file operations use Frontend invoke actions. Binary file and directory
copy paths prefer the ``/direct`` route so large payloads avoid JSON
envelopes. The EXECD direct route is a published sandbox target and remains
available under a block-network policy; invoke chunks remain the bounded
fallback for transport failures.
"""

import os
import shlex
import tarfile
import tempfile
import threading
import uuid
from typing import Iterator, List, Literal, Union, overload

from ._transport import SandboxClient
from .types import EntryInfo

_RPC_FILE_CHUNK_SIZE = 1024 * 1024


def _safe_extract_tar(tar: tarfile.TarFile, dest: str) -> None:
    """Extract a tar archive under ``dest``, rejecting members whose resolved
    path escapes ``dest`` (path-traversal guard; avoids unsafe extractall)."""
    dest_real = os.path.realpath(dest)
    for member in tar.getmembers():
        target = os.path.realpath(os.path.join(dest, member.name))
        if target != dest_real and not target.startswith(dest_real + os.sep):
            raise RuntimeError(
                f"unsafe tar member escapes destination: {member.name!r}"
            )
        tar.extract(member, dest)


def _tar_directory_chunks(
    local_path: str, chunk_size: int = 1024 * 1024
) -> Iterator[bytes]:
    """Stream a directory as tar bytes without materializing a temporary archive."""
    read_fd, write_fd = os.pipe()
    errors = []

    def produce_tar() -> None:
        try:
            with os.fdopen(write_fd, "wb") as writer:
                with tarfile.open(fileobj=writer, mode="w|") as tar:
                    for name in sorted(os.listdir(local_path)):
                        tar.add(os.path.join(local_path, name), arcname=name)
        except BaseException as exc:  # propagate producer failures to consumer
            errors.append(exc)

    producer = threading.Thread(
        target=produce_tar, name="adx-copy-from-local-tar", daemon=True
    )
    producer.start()

    try:
        with os.fdopen(read_fd, "rb") as reader:
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    finally:
        producer.join()

    if errors:
        raise errors[0]


def _check(result: dict, msg: str) -> dict:
    if result.get("error"):
        raise RuntimeError(f"{msg}: {result['error']}")
    return result


class Filesystem:
    """Client-side wrapper for filesystem operations on the remote sandbox."""

    def __init__(self, client: SandboxClient, sandbox_id: str):
        self._client = client
        self._sid = sandbox_id

    def _invoke(self, action: str, **args) -> dict:
        return self._client.invoke(self._sid, action, args)

    def _write_rpc_chunks(self, path: str, chunks: Iterator[bytes]) -> EntryInfo:
        offset = 0
        wrote_chunk = False
        for source_chunk in chunks:
            if not isinstance(source_chunk, bytes):
                raise TypeError("file chunks must be bytes")
            for start in range(0, len(source_chunk), _RPC_FILE_CHUNK_SIZE):
                chunk = source_chunk[start : start + _RPC_FILE_CHUNK_SIZE]
                result = _check(
                    self._invoke(
                        "file.write_chunk",
                        path=path,
                        offset=offset,
                        data=chunk.hex(),
                    ),
                    f"Failed to write {path}",
                )
                if int(result.get("bytes_written", -1)) != len(chunk):
                    raise RuntimeError(f"Short RuntimeRPC write for {path}")
                offset += len(chunk)
                wrote_chunk = True
        if not wrote_chunk:
            _check(
                self._invoke("file.write_chunk", path=path, offset=0, data=""),
                f"Failed to write {path}",
            )
        return self.get_info(path)

    def _read_rpc_chunks(self, path: str) -> Iterator[bytes]:
        offset = 0
        while True:
            result = _check(
                self._invoke(
                    "file.read_chunk",
                    path=path,
                    offset=offset,
                    limit=_RPC_FILE_CHUNK_SIZE,
                ),
                f"Failed to read {path}",
            )
            try:
                chunk = bytes.fromhex(str(result.get("data", "")))
            except ValueError as exc:
                raise RuntimeError(
                    f"Invalid RuntimeRPC file data for {path}"
                ) from exc
            if int(result.get("bytes_read", -1)) != len(chunk):
                raise RuntimeError(f"Invalid RuntimeRPC byte count for {path}")
            if chunk:
                yield chunk
                offset += len(chunk)
            if result.get("eof"):
                return
            if not chunk:
                raise RuntimeError(f"RuntimeRPC read made no progress for {path}")

    def _run_control_command(self, command: str, operation: str) -> None:
        result = self._invoke(
            "process.exec",
            cmd=command,
            envs=None,
            cwd=None,
            timeout=300,
        )
        if int(result.get("exit_code", -1)) != 0:
            detail = result.get("stderr") or result.get("error") or "unknown error"
            raise RuntimeError(f"{operation}: {detail}")

    def _remove_control_temp(self, path: str) -> None:
        try:
            self._invoke("file.remove", path=path)
        except Exception:
            pass

    @overload
    def read(self, path: str, format: Literal["text"] = "text") -> str: ...

    @overload
    def read(self, path: str, format: Literal["bytes"]) -> bytes: ...

    def read(self, path: str, format: str = "text") -> Union[str, bytes]:
        if format not in ("text", "bytes"):
            raise ValueError("format must be 'text' or 'bytes'")
        if self._client.direct_enabled:
            data = self._client.download_bytes_direct(
                self._sid, path, timeout_or_default()
            )
        else:
            data = b"".join(self._read_rpc_chunks(path))
        if format == "bytes":
            return data
        return data.decode("utf-8", errors="replace")

    def write(self, path: str, data: Union[str, bytes]) -> EntryInfo:
        payload = data if isinstance(data, bytes) else data.encode("utf-8")
        if not self._client.direct_enabled:
            return self._write_rpc_chunks(path, iter((payload,)))
        result = self._client.upload_bytes_direct(
            self._sid, payload, path, timeout_or_default()
        )
        return self._entry_from_upload(_check(result, f"Failed to write {path}"))

    def list(self, path: str, depth: int = 1) -> List[EntryInfo]:
        result = _check(
            self._invoke("file.list", path=path, depth=depth),
            f"Failed to list {path}",
        )
        return [
            EntryInfo(
                name=e["name"],
                path=e["path"],
                type=e["type"],
                size=e["size"],
                permissions=e["permissions"],
                modified_time=e["modified_time"],
            )
            for e in result["entries"]
        ]

    def exists(self, path: str) -> bool:
        return self._invoke("file.exists", path=path)["exists"]

    def remove(self, path: str) -> None:
        _check(self._invoke("file.remove", path=path), f"Failed to remove {path}")

    def rename(self, old_path: str, new_path: str) -> EntryInfo:
        result = _check(
            self._invoke("file.rename", old_path=old_path, new_path=new_path),
            f"Failed to rename {old_path} -> {new_path}",
        )
        return self._entry(result)

    def make_dir(self, path: str) -> bool:
        result = _check(
            self._invoke("file.make_dir", path=path), f"Failed to make dir {path}"
        )
        return result["created"]

    def get_info(self, path: str) -> EntryInfo:
        # EXECD's normalize_sandbox_action accepts file.stat / file.info /
        # fs.get_info — NOT file.get_info. Use file.stat. (Verified via E2E.)
        result = _check(
            self._invoke("file.stat", path=path),
            f"Failed to get info for {path}",
        )
        return self._entry(result)

    # ── bulk copy via direct HTTP or invoke chunks ────────────────

    def copy_from_local(self, local_path: str, remote_path: str) -> None:
        """Copy a local file or directory **into** the sandbox.

        Files use the ``/direct`` binary upload path. Directories are
        packed as a tar archive and uploaded over the same HTTP binary path.
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local source path not found: {local_path}")
        if not self._client.direct_enabled:
            if os.path.isdir(local_path):
                temp_path = f"/tmp/.adx-upload-{uuid.uuid4().hex}.tar"
                try:
                    self._write_rpc_chunks(
                        temp_path, _tar_directory_chunks(local_path)
                    )
                    command = (
                        f"mkdir -p -- {shlex.quote(remote_path)} && "
                        f"tar -xf {shlex.quote(temp_path)} "
                        f"-C {shlex.quote(remote_path)}"
                    )
                    self._run_control_command(
                        command,
                        f"Failed to copy {local_path} to {remote_path}",
                    )
                finally:
                    self._remove_control_temp(temp_path)
                return

            def local_chunks() -> Iterator[bytes]:
                with open(local_path, "rb") as source:
                    while True:
                        chunk = source.read(_RPC_FILE_CHUNK_SIZE)
                        if not chunk:
                            return
                        yield chunk

            self._write_rpc_chunks(remote_path, local_chunks())
            return
        if os.path.isdir(local_path):
            result = self._client.upload_stream_direct(
                self._sid,
                _tar_directory_chunks(local_path),
                remote_path,
                timeout_or_default(),
                upload_type="tar",
            )
            _check(result, f"Failed to copy {local_path} to {remote_path}")
            return
        result = self._client.upload_file_direct(
            self._sid, local_path, remote_path, timeout_or_default()
        )
        _check(result, f"Failed to copy {local_path} to {remote_path}")

    def copy_to_local(self, remote_path: str, local_path: str) -> None:
        """Copy a file or directory **from** the sandbox to the local machine.

        A remote directory arrives as a single tar archive that is extracted
        under ``local_path``.
        """
        if self.get_info(remote_path).type == "dir":
            os.makedirs(local_path, exist_ok=True)
            fd, tar_path = tempfile.mkstemp(suffix=".tar")
            os.close(fd)
            try:
                if self._client.direct_enabled:
                    self._client.download_file_direct(
                        self._sid,
                        remote_path,
                        tar_path,
                        timeout_or_default(),
                        download_type="tar",
                    )
                else:
                    remote_temp = f"/tmp/.adx-download-{uuid.uuid4().hex}.tar"
                    try:
                        command = (
                            f"tar -cf {shlex.quote(remote_temp)} "
                            f"-C {shlex.quote(remote_path)} ."
                        )
                        self._run_control_command(
                            command,
                            f"Failed to archive {remote_path}",
                        )
                        with open(tar_path, "wb") as target:
                            for chunk in self._read_rpc_chunks(remote_temp):
                                target.write(chunk)
                    finally:
                        self._remove_control_temp(remote_temp)
                with tarfile.open(tar_path, "r") as tar:
                    _safe_extract_tar(tar, local_path)
            finally:
                os.unlink(tar_path)
            return
        if not self._client.direct_enabled:
            parent = os.path.dirname(os.path.abspath(local_path))
            if parent:
                os.makedirs(parent, exist_ok=True)
            part_path = f"{local_path}.part"
            try:
                with open(part_path, "wb") as target:
                    for chunk in self._read_rpc_chunks(remote_path):
                        target.write(chunk)
                os.replace(part_path, local_path)
            finally:
                if os.path.exists(part_path):
                    os.unlink(part_path)
            return

        self._client.download_file_direct(
            self._sid, remote_path, local_path, timeout_or_default()
        )

    @staticmethod
    def _entry(result: dict) -> EntryInfo:
        return EntryInfo(
            name=result["name"],
            path=result["path"],
            type=result["type"],
            size=result["size"],
            permissions=result["permissions"],
            modified_time=result["modified_time"],
        )

    @staticmethod
    def _entry_from_upload(result: dict) -> EntryInfo:
        return EntryInfo(
            name=result.get("name", os.path.basename(result.get("path", ""))),
            path=result["path"],
            type=result.get("type", "file"),
            size=result["size"],
            permissions=result.get("permissions", ""),
            modified_time=result.get("modified_time", 0.0),
        )


def timeout_or_default() -> float:
    return float(os.environ.get("ADX_DIRECT_UPLOAD_TIMEOUT", "300"))
