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

import json
import logging
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
from typing import Optional

from .file_handler import DEFAULT_MAX_FILE_SIZE, FileHandler, FileListTimeoutError

_LOG = logging.getLogger(__name__)


class _ExecutorThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False


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

    def __init__(self, host: str, port: int, max_file_size: int = DEFAULT_MAX_FILE_SIZE) -> None:
        file_handler = FileHandler(max_file_size=max_file_size)

        class RequestHandler(_ExecutorRequestHandler):
            files = file_handler

        self._server = _ExecutorThreadingHTTPServer((host, port), RequestHandler)
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


class _ExecutorRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    files = FileHandler()

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
        self._upload_request()

    def do_PUT(self) -> None:  # noqa: N802
        self._upload_request()

    def _upload_request(self) -> None:
        if urlsplit(self.path).path != "/v1/files/upload":
            self._write_json(HTTPStatus.NOT_FOUND, {"message": "endpoint not found"})
            return
        self._upload()

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

    def _write_json(self, status: HTTPStatus, value: dict) -> None:
        data = json.dumps(value, separators=(",", ":")).encode("utf-8")
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
