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

"""Internal HTTP server exposed through the platform TCP tunnel.

The sandbox API (routes, validation, size limits, error mapping) lives in
``sandbox_http.SandboxRequestMixin``; this module keeps the transport layer
(files + exec handlers, JSON writing, body readers, concurrency limits)
shared by both slices.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlsplit

from .command_handler import CommandHandler
from .file_handler import DEFAULT_MAX_FILE_SIZE, FileHandler, FileListTimeoutError
from .sandbox_http import (
    DEFAULT_MAX_SANDBOX_REQUEST_SIZE,
    DEFAULT_MAX_SANDBOX_RESPONSE_SIZE,
    SANDBOX_PREFIX,
    SandboxRequestMixin,
    SandboxRequestTooLargeError,
    SandboxResponseTooLargeError,
)
from .sandbox_manager import SandboxManager

_LOG = logging.getLogger(__name__)
DEFAULT_MAX_CONCURRENT_REQUESTS = 64
TRACE_HEADER = "X-Trace-ID"
INSTANCE_ID_ENV = "INSTANCE_ID"


class _ExecutorThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(
        self,
        server_address,
        request_handler_class,
        *,
        max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS,
    ) -> None:
        if max_concurrent_requests <= 0:
            raise ValueError("max_concurrent_requests must be greater than zero")
        self._request_slots = threading.BoundedSemaphore(max_concurrent_requests)
        super().__init__(server_address, request_handler_class)

    def process_request(self, request, client_address) -> None:
        if not self._request_slots.acquire(blocking=False):
            self._reject_overloaded(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._request_slots.release()
            raise

    def process_request_thread(self, request, client_address) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()

    def _reject_overloaded(self, request) -> None:
        body = b'{"message":"executor is busy"}'
        response = (
            b"HTTP/1.1 503 Service Unavailable\r\n"
            b"Content-Type: application/json\r\n"
            + f"Content-Length: {len(body)}\r\n".encode("ascii")
            + b"Connection: close\r\n\r\n"
            + body
        )
        try:
            request.sendall(response)
        except OSError:
            pass
        finally:
            self.shutdown_request(request)


def parse_range(value: str, total_size: int) -> tuple[int, Optional[int]]:
    if not value:
        return 0, None
    if not value.startswith("bytes=") or "," in value:
        raise ValueError("unsupported range request")
    start_text, separator, end_text = value[6:].partition("-")
    if not separator or not start_text.isdigit() or (end_text and not end_text.isdigit()):
        raise ValueError("unsupported range request")
    start = int(start_text)
    end = int(end_text) if end_text else None
    if total_size == 0 or start >= total_size or (end is not None and end < start):
        raise ValueError("requested range is not satisfiable")
    return start, end


class ExecutorHTTPServer:
    """Owns a background ThreadingHTTPServer."""

    def __init__(
        self,
        host: str,
        port: int,
        max_file_size: int = DEFAULT_MAX_FILE_SIZE,
        sandbox_manager: Optional[SandboxManager] = None,
        *,
        max_sandbox_request_size: int = DEFAULT_MAX_SANDBOX_REQUEST_SIZE,
        max_sandbox_response_size: int = DEFAULT_MAX_SANDBOX_RESPONSE_SIZE,
        max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS,
    ) -> None:
        if max_sandbox_request_size <= 0:
            raise ValueError("max_sandbox_request_size must be greater than zero")
        if max_sandbox_response_size <= 0:
            raise ValueError("max_sandbox_response_size must be greater than zero")
        file_handler = FileHandler(max_file_size=max_file_size)
        command_handler_instance = CommandHandler()
        if sandbox_manager is None:
            raise ValueError("a SandboxManager must be provided by the runtime")
        manager_instance = sandbox_manager
        sandbox_request_limit = max_sandbox_request_size
        sandbox_response_limit = max_sandbox_response_size

        class RequestHandler(_ExecutorRequestHandler):
            files = file_handler
            command_handler = command_handler_instance
            sandbox_manager = manager_instance
            max_sandbox_request_size = sandbox_request_limit
            max_sandbox_response_size = sandbox_response_limit

        self._sandbox_manager = sandbox_manager
        try:
            self._server = _ExecutorThreadingHTTPServer(
                (host, port),
                RequestHandler,
                max_concurrent_requests=max_concurrent_requests,
            )
        except BaseException:
            raise
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="yuanrong-agentexecutor-http",
            daemon=True,
        )

    @property
    def address(self):
        return self._server.server_address

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not threading.current_thread():
            self._thread.join(timeout=5)


class _ExecutorRequestHandler(SandboxRequestMixin, BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    files = FileHandler()
    command_handler: CommandHandler

    def _handle_get(self) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path
        if path == "/healthz":
            self._write_json(HTTPStatus.OK, {"status": "ready"})
            return
        if parsed.path == "/v1/files/download":
            with self._agent_trace("download", parsed.path):
                self._download(parse_qs(parsed.query))
            return
        if parsed.path == "/v1/files/list":
            with self._agent_trace("filelist", parsed.path):
                self._list(parse_qs(parsed.query))
            return
        if path == SANDBOX_PREFIX or path.startswith(f"{SANDBOX_PREFIX}/"):
            self._sandbox_request("GET", path, parse_qs(parsed.query))
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"message": "endpoint not found"})

    def _handle_post(self) -> None:
        path = urlsplit(self.path).path
        if path == "/v1/files/upload":
            with self._agent_trace("upload", path):
                self._upload()
            return
        if path == "/v1/files/mkdir":
            with self._agent_trace("mkdir", path):
                self._mkdir()
            return
        if path == "/v1/exec":
            with self._agent_trace("exec", path):
                self._exec_command()
            return
        # sandbox POST: /v1/sandbox/sandboxes (create) or /v1/sandbox/sandboxes/{id}/execute
        if path == SANDBOX_PREFIX or path.startswith(f"{SANDBOX_PREFIX}/"):
            with self._agent_trace(path.rsplit("/", 1)[-1], path):
                self._sandbox_request("POST", path, {})
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"message": "endpoint not found"})

    def _handle_put(self) -> None:
        path = urlsplit(self.path).path
        if path == "/v1/files/upload":
            self._upload()
            return
        # sandbox PUT: /v1/sandbox/sandboxes/{id}/files/write
        if path.startswith(f"{SANDBOX_PREFIX}/"):
            self._sandbox_request("PUT", path, parse_qs(urlsplit(self.path).query))
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"message": "endpoint not found"})

    def _handle_delete(self) -> None:
        path = urlsplit(self.path).path
        if path.startswith(f"{SANDBOX_PREFIX}/"):
            self._sandbox_request("DELETE", path, {})
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"message": "endpoint not found"})

    @contextmanager
    def _agent_trace(self, op: str, path: str):
        """Emit one [agent.<op>.enter]/[agent.<op>.exit] pair keyed by trace_id/instance_id."""
        trace_id = self.headers.get(TRACE_HEADER, "")
        instance_id = os.getenv(INSTANCE_ID_ENV, "")
        line = f"[agent.{op}.enter] agentexecutor path={path}"
        if trace_id:
            line += f" trace_id={trace_id}"
        if instance_id:
            line += f" instance_id={instance_id}"
        _LOG.info(line)
        started = time.monotonic()
        try:
            yield
        finally:
            status = getattr(self, "_agent_response_status", None)
            result = f"HTTP:{int(status)}" if status is not None else "ERR"
            exit_line = (
                f"[agent.{op}.exit] agentexecutor result={result} "
                f"cost_ms={int((time.monotonic() - started) * 1000)}"
            )
            if trace_id:
                exit_line += f" trace_id={trace_id}"
            if instance_id:
                exit_line += f" instance_id={instance_id}"
            _LOG.info(exit_line)

    def _upload(self) -> None:
        query = parse_qs(urlsplit(self.path).query)
        path = self._query_value(query, "path")
        mode = self._query_value(query, "mode")
        content_length = self.headers.get("Content-Length")
        try:
            if content_length is not None:
                parsed_length = int(content_length)
                if parsed_length < 0:
                    raise ValueError("Content-Length must be non-negative")
                if parsed_length > self.files.max_file_size:
                    self._write_json(
                        HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                        {"message": "upload is too large"},
                    )
                    return
            source = self._request_body(content_length)
            result = self.files.upload(path, source, mode=mode)
            self._write_json(HTTPStatus.OK, result)
        except ValueError as exc:
            status = HTTPStatus.REQUEST_ENTITY_TOO_LARGE if "exceeds max" in str(exc) else HTTPStatus.BAD_REQUEST
            self._write_json(status, {"message": str(exc)})
        except OSError as exc:
            _LOG.exception("file upload failed")
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"message": str(exc)})

    def _request_body(self, content_length: Optional[str]):
        transfer_encoding = self.headers.get("Transfer-Encoding", "").lower()
        if "chunked" in transfer_encoding:
            return _ChunkedReader(self.rfile)
        if content_length is None:
            raise ValueError("Content-Length or chunked transfer encoding is required")
        return _LengthReader(self.rfile, int(content_length))

    def _mkdir(self) -> None:
        query = parse_qs(urlsplit(self.path).query)
        path = self._query_value(query, "path")
        mode = self._query_value(query, "mode")
        try:
            recursive = self._query_bool(query, "recursive", False)
            result = self.files.mkdir(path, mode=mode, recursive=recursive)
            self._write_json(HTTPStatus.OK, result)
        except (ValueError, FileNotFoundError) as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"message": str(exc)})
        except OSError as exc:
            _LOG.exception("file mkdir failed")
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"message": str(exc)})

    def _exec_command(self) -> None:
        try:
            payload = self._read_json_body()
            if "command" not in payload:
                raise ValueError("command is required")
            command = payload["command"]
            working_dir = payload.get("working_dir")
            env = payload.get("env")
            timeout = payload.get("timeout")
            if working_dir is not None and not isinstance(working_dir, str):
                raise TypeError("working_dir must be a string")
            if env is not None:
                if not isinstance(env, dict) or not all(
                    isinstance(key, str) and isinstance(value, str)
                    for key, value in env.items()
                ):
                    raise TypeError("env must be an object containing string values")
            if timeout is not None:
                if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                    raise TypeError("timeout must be a number")
                if timeout <= 0:
                    raise ValueError("timeout must be greater than zero")
            result = self.command_handler.execute(command, working_dir=working_dir, env=env, timeout=timeout)
            self._write_json(HTTPStatus.OK, result, max_size=self.max_sandbox_response_size)
        except (SandboxRequestTooLargeError, SandboxResponseTooLargeError) as exc:
            self._write_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"message": str(exc)})
        except (ValueError, TypeError) as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"message": str(exc)})
        except Exception as exc:  # noqa: BLE001 - keep the HTTP connection well-formed
            _LOG.exception("exec command failed")
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"message": str(exc)})

    def _download(self, query: dict[str, list[str]]) -> None:
        path = self._query_value(query, "path")
        try:
            source, total_size, effective_end, length = self.files.open_download(path)
        except FileNotFoundError:
            self._write_json(HTTPStatus.NOT_FOUND, {"message": "path not found"})
            return
        except ValueError as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"message": str(exc)})
            return
        except OSError as exc:
            _LOG.exception("file download failed")
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"message": str(exc)})
            return

        try:
            start, end = parse_range(self.headers.get("Range", ""), total_size)
            effective_end, length = self.files.resolve_download_range(total_size, start, end)
            source.seek(start)
        except ValueError as exc:
            source.close()
            self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
            self.send_header("Content-Range", f"bytes */{total_size}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            # Result is already reported by the [agent.download.exit] HTTP:416 line.
            _LOG.debug("rejected file range: %s", exc)
            return

        status = HTTPStatus.PARTIAL_CONTENT if self.headers.get("Range") else HTTPStatus.OK
        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        if status == HTTPStatus.PARTIAL_CONTENT:
            self.send_header("Content-Range", f"bytes {start}-{effective_end}/{total_size}")
        self.end_headers()
        try:
            with source:
                self.files.copy_range(source, self.wfile, length)
        except (BrokenPipeError, ConnectionResetError):
            _LOG.debug("file download client disconnected")

    def _list(self, query: dict[str, list[str]]) -> None:
        path = self._query_value(query, "path")
        try:
            recursive = self._query_bool(query, "recursive", False)
            max_depth = int(self._query_value(query, "max_depth", "0"))
            if max_depth < 0:
                raise ValueError("max_depth must be non-negative")
            result = self.files.list(path, recursive=recursive, max_depth=max_depth)
            self._write_json(HTTPStatus.OK, result)
        except FileNotFoundError:
            self._write_json(HTTPStatus.NOT_FOUND, {"message": "path not found"})
        except FileListTimeoutError as exc:
            self._write_json(HTTPStatus.GATEWAY_TIMEOUT, {"message": str(exc)})
        except ValueError as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"message": str(exc)})

    @staticmethod
    def _query_value(query: dict[str, list[str]], name: str, default: str = "") -> str:
        values = query.get(name)
        return values[0] if values else default

    @staticmethod
    def _query_bool(query: dict[str, list[str]], name: str, default: bool) -> bool:
        """Strictly parse a boolean query parameter; anything else is a 400.

        与 JSON body 端点的 _optional_bool 同一哲学：类型错误显式拒绝,
        而非静默降级为 False(那会让 recursive="aaa" 意外走非递归分支)。
        """
        values = query.get(name)
        if not values:
            return default
        value = values[0].lower()
        if value == "true":
            return True
        if value == "false":
            return False
        raise ValueError(f"{name} must be a boolean ('true' or 'false')")

    def send_response(self, code, message=None):
        """Record the response status so _agent_trace can report it on exit."""
        self._agent_response_status = int(code)
        super().send_response(code, message)

    def _write_json(
        self, status: HTTPStatus, value: dict, *,
        max_size: Optional[int] = None,
        extra_headers: Optional[dict[str, str]] = None,
    ) -> None:
        data = json.dumps(value, separators=(",", ":")).encode("utf-8")
        if max_size is not None and len(data) > max_size:
            raise SandboxResponseTooLargeError(
                f"response body exceeds max {max_size}"
            )
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for key, val in (extra_headers or {}).items():
            self.send_header(key, val)
        self.end_headers()
        if self.command != "HEAD":
            # RFC 9110 9.3.2: HEAD responses carry headers only.
            self.wfile.write(data)

    def log_message(self, format_string: str, *args) -> None:
        # Access-log line duplicates the [agent.<op>.enter/exit] pair; keep at DEBUG.
        _LOG.debug("executor http: " + format_string, *args)

    def _route_or_404(self) -> None:
        """Unified entry for methods without a dedicated do_* handler
        (PATCH/HEAD/OPTIONS/TRACE/CONNECT). Not a method whitelist: route by
        sandbox path first and let the dispatch tail decide; new methods only
        add a branch in _dispatch_sandbox, this layer stays untouched."""
        parsed = urlsplit(self.path)
        path = parsed.path
        if path == SANDBOX_PREFIX or path.startswith(f"{SANDBOX_PREFIX}/"):
            # This method never consumes a request body; drop keep-alive so a
            # pipelined next request cannot read the previous body's leftovers.
            self.close_connection = True
            self._sandbox_request(self.command, path, parse_qs(parsed.query))
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"message": "endpoint not found"})

    do_GET = _handle_get
    do_POST = _handle_post
    do_PUT = _handle_put
    do_DELETE = _handle_delete
    do_PATCH = do_HEAD = do_OPTIONS = do_TRACE = do_CONNECT = _route_or_404


class _LengthReader:
    def __init__(self, source, length: int) -> None:
        self._source = source
        self._remaining = length

    def read(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        requested = self._remaining if size < 0 else min(size, self._remaining)
        data = self._source.read(requested)
        self._remaining -= len(data)
        return data


class _ChunkedReader:
    """Minimal RFC 9112 chunk decoder for a request body forwarded by net/http."""

    def __init__(self, source) -> None:
        self._source = source
        self._chunk_remaining = 0
        self._finished = False

    def read(self, size: int = -1) -> bytes:
        if self._finished:
            return b""
        output = bytearray()
        requested = None if size < 0 else size
        while requested is None or len(output) < requested:
            if self._chunk_remaining == 0:
                self._start_chunk()
                if self._finished:
                    break
            amount = self._chunk_remaining
            if requested is not None:
                amount = min(amount, requested - len(output))
            data = self._source.read(amount)
            if not data:
                raise ConnectionError("unexpected EOF in chunked request body")
            output.extend(data)
            self._chunk_remaining -= len(data)
            if self._chunk_remaining == 0 and self._source.read(2) != b"\r\n":
                raise ValueError("invalid chunk terminator")
        return bytes(output)

    def _start_chunk(self) -> None:
        line = self._source.readline(4096)
        if not line.endswith(b"\r\n"):
            raise ValueError("invalid chunk header")
        size_text = line[:-2].split(b";", 1)[0]
        try:
            self._chunk_remaining = int(size_text, 16)
        except ValueError as exc:
            raise ValueError("invalid chunk size") from exc
        if self._chunk_remaining != 0:
            return
        while True:
            trailer = self._source.readline(4096)
            if trailer in (b"\r\n", b""):
                break
        self._finished = True
