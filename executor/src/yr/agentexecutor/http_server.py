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

"""Internal HTTP server exposed through the platform TCP tunnel."""

from __future__ import annotations

import base64
import ipaddress
import json
import logging
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import parse_qs, urlsplit

from .file_handler import DEFAULT_MAX_FILE_SIZE, FileHandler, FileListTimeoutError
from .sandbox_instance import SandboxInstance

_LOG = logging.getLogger(__name__)
DEFAULT_MAX_CONCURRENT_REQUESTS = 64
DEFAULT_MAX_SANDBOX_REQUEST_SIZE = 512 * 1024 * 1024
DEFAULT_MAX_SANDBOX_RESPONSE_SIZE = 512 * 1024 * 1024
_SANDBOX_ENDPOINTS = {
    "/v1/sandbox/execute",
    "/v1/sandbox/read_file",
    "/v1/sandbox/write_file",
    "/v1/sandbox/list_files",
    "/v1/sandbox/search_files",
}


class SandboxRequestTooLargeError(ValueError):
    """Raised when a Sandbox API JSON request exceeds its configured limit."""


class SandboxResponseTooLargeError(ValueError):
    """Raised when a Sandbox API JSON response exceeds its configured limit."""


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
        sandbox: Optional[SandboxInstance] = None,
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
        sandbox_instance = sandbox if sandbox is not None else SandboxInstance()
        sandbox_request_limit = max_sandbox_request_size
        sandbox_response_limit = max_sandbox_response_size

        class RequestHandler(_ExecutorRequestHandler):
            files = file_handler
            sandbox = sandbox_instance
            max_sandbox_request_size = sandbox_request_limit
            max_sandbox_response_size = sandbox_response_limit

        self._sandbox = sandbox_instance
        self._owns_sandbox = sandbox is None
        try:
            self._server = _ExecutorThreadingHTTPServer(
                (host, port),
                RequestHandler,
                max_concurrent_requests=max_concurrent_requests,
            )
        except BaseException:
            if self._owns_sandbox:
                self._sandbox.cleanup()
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
        if self._owns_sandbox:
            self._sandbox.cleanup()


class _ExecutorRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    files = FileHandler()
    sandbox: SandboxInstance
    max_sandbox_request_size = DEFAULT_MAX_SANDBOX_REQUEST_SIZE
    max_sandbox_response_size = DEFAULT_MAX_SANDBOX_RESPONSE_SIZE

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlsplit(self.path)
        if parsed.path == "/healthz":
            self._write_json(HTTPStatus.OK, {"status": "ready"})
            return
        if parsed.path == "/v1/files/download":
            self._download(parse_qs(parsed.query))
            return
        if parsed.path == "/v1/files/list":
            self._list(parse_qs(parsed.query))
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"message": "endpoint not found"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if path == "/v1/files/upload":
            self._upload()
            return
        if path in _SANDBOX_ENDPOINTS:
            self._sandbox_request(path)
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"message": "endpoint not found"})

    def do_PUT(self) -> None:  # noqa: N802
        self._upload_request()

    def _upload_request(self) -> None:
        if urlsplit(self.path).path != "/v1/files/upload":
            self._write_json(HTTPStatus.NOT_FOUND, {"message": "endpoint not found"})
            return
        self._upload()

    def _sandbox_request(self, path: str) -> None:
        if not self._client_is_loopback():
            self._write_json(HTTPStatus.FORBIDDEN, {"message": "sandbox API is loopback-only"})
            return
        try:
            payload = self._read_json_body()
            result = self._dispatch_sandbox(path, payload)
            self._write_json(
                HTTPStatus.OK, result, max_size=self.max_sandbox_response_size
            )
        except (SandboxRequestTooLargeError, SandboxResponseTooLargeError) as exc:
            self._write_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"message": str(exc)})
        except FileNotFoundError as exc:
            self._write_json(HTTPStatus.NOT_FOUND, {"message": str(exc)})
        except NotADirectoryError as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"message": str(exc)})
        except (ValueError, TypeError) as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"message": str(exc)})
        except OSError as exc:
            _LOG.exception("sandbox operation failed")
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"message": str(exc)})
        except Exception as exc:  # noqa: BLE001 - keep the HTTP connection well-formed
            _LOG.exception("unexpected sandbox operation failure")
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"message": str(exc)})

    def _dispatch_sandbox(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if path == "/v1/sandbox/execute":
            if "command" not in payload:
                raise ValueError("command is required")
            working_dir = self._aliased_value(payload, "working_dir", "cwd")
            env = self._aliased_value(payload, "env", "environment")
            timeout = self._aliased_value(payload, "timeout", "timeout_seconds")
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
            return self.sandbox.execute(
                payload["command"], working_dir=working_dir, env=env, timeout=timeout
            )

        if path == "/v1/sandbox/read_file":
            file_path = self._required_string(payload, "path")
            mode = payload.get("mode", "rb")
            if not isinstance(mode, str):
                raise TypeError("mode must be a string")
            content = self.sandbox.read_file(file_path, mode)
            if isinstance(content, bytes):
                return {
                    "path": file_path,
                    "mode": mode,
                    "content": base64.b64encode(content).decode("ascii"),
                    "content_encoding": "base64",
                }
            return {
                "path": file_path,
                "mode": mode,
                "content": content,
                "content_encoding": "text",
            }

        if path == "/v1/sandbox/write_file":
            file_path = self._required_string(payload, "path")
            mode = payload.get("mode", "wb")
            if not isinstance(mode, str):
                raise TypeError("mode must be a string")
            content = self._aliased_value(payload, "content", "data", required=True)
            encoding = payload.get("content_encoding", "base64" if "b" in mode else "text")
            if not isinstance(content, str):
                raise TypeError("content must be a string")
            if "b" in mode:
                if encoding != "base64":
                    raise ValueError("binary write mode requires content_encoding 'base64'")
                data: Any = base64.b64decode(content, validate=True)
            else:
                if encoding != "text":
                    raise ValueError("text write mode requires content_encoding 'text'")
                data = content
            self.sandbox.write_file(file_path, data, mode)
            return {"success": True, "path": file_path}

        if path == "/v1/sandbox/list_files":
            file_path = self._required_string(payload, "path")
            recursive = self._optional_bool(payload, "recursive", False)
            include_files = self._optional_bool(payload, "include_files", True)
            include_dirs = self._optional_bool(payload, "include_dirs", True)
            max_depth = payload.get("max_depth")
            if max_depth is not None:
                if isinstance(max_depth, bool) or not isinstance(max_depth, int):
                    raise TypeError("max_depth must be an integer")
                if max_depth < 0:
                    raise ValueError("max_depth must be non-negative")
            items = self.sandbox.list_files(
                file_path,
                recursive=recursive,
                max_depth=max_depth,
                include_files=include_files,
                include_dirs=include_dirs,
            )
            return {"items": items}

        if path == "/v1/sandbox/search_files":
            file_path = self._required_string(payload, "path")
            pattern = self._required_string(payload, "pattern")
            excludes = payload.get("exclude_patterns")
            if excludes is not None and (
                not isinstance(excludes, list)
                or not all(isinstance(item, str) for item in excludes)
            ):
                raise TypeError("exclude_patterns must be an array of strings")
            return {
                "items": self.sandbox.search_files(
                    file_path, pattern, exclude_patterns=excludes
                )
            }

        raise ValueError("unsupported sandbox operation")

    def _read_json_body(self) -> dict[str, Any]:
        content_type = self.headers.get("Content-Type", "").partition(";")[0].strip().lower()
        if content_type != "application/json":
            raise ValueError("Content-Type must be application/json")
        content_length = self.headers.get("Content-Length")
        if content_length is not None:
            try:
                parsed_length = int(content_length)
            except ValueError as exc:
                raise ValueError("invalid Content-Length") from exc
            if parsed_length < 0:
                raise ValueError("Content-Length must be non-negative")
            if parsed_length > self.max_sandbox_request_size:
                self.close_connection = True
                raise SandboxRequestTooLargeError(
                    f"request body exceeds max {self.max_sandbox_request_size}"
                )
        source = self._request_body(content_length)
        body = bytearray()
        while True:
            chunk = source.read(min(1024 * 1024, self.max_sandbox_request_size + 1 - len(body)))
            if not chunk:
                break
            body.extend(chunk)
            if len(body) > self.max_sandbox_request_size:
                self.close_connection = True
                raise SandboxRequestTooLargeError(
                    f"request body exceeds max {self.max_sandbox_request_size}"
                )
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("request body must be valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return payload

    def _client_is_loopback(self) -> bool:
        try:
            address = ipaddress.ip_address(self.client_address[0])
        except ValueError:
            return False
        if address.is_loopback:
            return True
        return bool(
            address.version == 6
            and address.ipv4_mapped
            and address.ipv4_mapped.is_loopback
        )

    @staticmethod
    def _required_string(payload: dict[str, Any], name: str) -> str:
        value = payload.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} is required")
        return value

    @staticmethod
    def _optional_bool(payload: dict[str, Any], name: str, default: bool) -> bool:
        value = payload.get(name, default)
        if not isinstance(value, bool):
            raise TypeError(f"{name} must be a boolean")
        return value

    @staticmethod
    def _aliased_value(
        payload: dict[str, Any], primary: str, alias: str, *, required: bool = False
    ) -> Any:
        if primary in payload and alias in payload:
            raise ValueError(f"use either {primary} or {alias}, not both")
        if primary in payload:
            return payload[primary]
        if alias in payload:
            return payload[alias]
        if required:
            raise ValueError(f"{primary} is required")
        return None

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
            _LOG.info("rejected file range: %s", exc)
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
            _LOG.info("file download client disconnected")

    def _list(self, query: dict[str, list[str]]) -> None:
        path = self._query_value(query, "path")
        recursive = self._query_value(query, "recursive", "false").lower() == "true"
        try:
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

    def _write_json(
        self, status: HTTPStatus, value: dict, *, max_size: Optional[int] = None
    ) -> None:
        data = json.dumps(value, separators=(",", ":")).encode("utf-8")
        if max_size is not None and len(data) > max_size:
            raise SandboxResponseTooLargeError(
                f"response body exceeds max {max_size}"
            )
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format_string: str, *args) -> None:
        _LOG.info("executor http: " + format_string, *args)


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
