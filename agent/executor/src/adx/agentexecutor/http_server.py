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
import os
import threading
import time
from contextlib import contextmanager
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import parse_qs, urlsplit

from .file_handler import DEFAULT_MAX_FILE_SIZE, FileHandler, FileListTimeoutError
from .sandbox.sandbox import SandboxCreateOptions
from .sandbox_manager import SandboxManager

_LOG = logging.getLogger(__name__)
DEFAULT_MAX_CONCURRENT_REQUESTS = 64
DEFAULT_MAX_SANDBOX_REQUEST_SIZE = 512 * 1024 * 1024
DEFAULT_MAX_SANDBOX_RESPONSE_SIZE = 512 * 1024 * 1024
_SANDBOX_PREFIX = "/v1/sandbox/sandboxes"
TRACE_HEADER = "X-Trace-ID"
INSTANCE_ID_ENV = "INSTANCE_ID"


class SandboxRequestTooLargeError(ValueError):
    """Raised when a Sandbox API JSON request exceeds its configured limit."""


class SandboxResponseTooLargeError(ValueError):
    """Raised when a Sandbox API JSON response exceeds its configured limit."""


class SandboxMethodNotAllowedError(ValueError):
    """Method+URL combination has no registered sandbox route (HTTP 405).

    Inherits ValueError so an unmapped except chain degrades to 400, not 500.
    """

    def __init__(self, method: str, path: str):
        super().__init__(f"method {method} not allowed for sandbox resource")
        self.method = method
        self.path = path


# Sandbox 路由表:(method, path 形状, route key)。"bare" 指裸 create URL,
# "{id}" 为实例段占位。dispatch 的分支匹配与 405 响应的 Allow 头都由这张表
# 驱动——新增请求方式时在表里加一行 + dispatch 加对应 route 分支即可,
# Allow 头自动跟随,不存在第二处方法枚举。
_SANDBOX_ROUTES: tuple[tuple[str, str, str], ...] = (
    ("POST", "bare", "create"),
    ("DELETE", "{id}", "delete"),
    ("POST", "{id}/execute", "execute"),
    ("GET", "{id}/files/read", "files_read"),
    ("PUT", "{id}/files/write", "files_write"),
    ("GET", "{id}/files/list", "files_list"),
    ("GET", "{id}/files/search", "files_search"),
)


def _shape_matches(shape: str, path: str) -> bool:
    """Whether ``path`` matches a route shape ("bare", "{id}", "{id}/execute", ...)."""
    if shape == "bare":
        return path == _SANDBOX_PREFIX
    parts = path[len(_SANDBOX_PREFIX) + 1:].split("/", 2)
    if not parts or not parts[0]:
        return False
    sub = parts[1] if len(parts) > 1 else ""
    action = parts[2] if len(parts) > 2 else ""
    segments = shape.split("/")[1:]  # 去掉 "{id}"
    segments += [""] * (2 - len(segments))
    return sub == segments[0] and action == segments[1]


def _match_sandbox_route(method: str, path: str) -> Optional[str]:
    """Pure method+path lookup in _SANDBOX_ROUTES; None = combination unregistered.

    Callers still resolve instance existence before treating None as 405,
    so unknown-id requests keep returning 404 (404 优先于 405).
    """
    for route_method, shape, key in _SANDBOX_ROUTES:
        if route_method == method and _shape_matches(shape, path):
            return key
    return None


def _allowed_sandbox_methods(path: str) -> str:
    """Allow-header value for a sandbox path: the methods registered on it."""
    return ", ".join(sorted(
        route_method
        for route_method, shape, _ in _SANDBOX_ROUTES
        if _shape_matches(shape, path)
    ))


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
        if sandbox_manager is None:
            raise ValueError("a SandboxManager must be provided by the runtime")
        manager_instance = sandbox_manager
        sandbox_request_limit = max_sandbox_request_size
        sandbox_response_limit = max_sandbox_response_size

        class RequestHandler(_ExecutorRequestHandler):
            files = file_handler
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
            name="adx-agentexecutor-http",
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
    sandbox_manager: SandboxManager
    max_sandbox_request_size = DEFAULT_MAX_SANDBOX_REQUEST_SIZE
    max_sandbox_response_size = DEFAULT_MAX_SANDBOX_RESPONSE_SIZE

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
        if path == _SANDBOX_PREFIX or path.startswith(f"{_SANDBOX_PREFIX}/"):
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
        # sandbox POST: /v1/sandbox/sandboxes (create) or /v1/sandbox/sandboxes/{id}/execute
        if path == _SANDBOX_PREFIX or path.startswith(f"{_SANDBOX_PREFIX}/"):
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
        if path.startswith(f"{_SANDBOX_PREFIX}/"):
            self._sandbox_request("PUT", path, parse_qs(urlsplit(self.path).query))
            return
        self._write_json(HTTPStatus.NOT_FOUND, {"message": "endpoint not found"})

    def _handle_delete(self) -> None:
        path = urlsplit(self.path).path
        if path.startswith(f"{_SANDBOX_PREFIX}/"):
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

    def _sandbox_request(self, method: str, path: str, query: dict[str, list[str]]) -> None:
        if not self._client_is_loopback():
            self._write_json(HTTPStatus.FORBIDDEN, {"message": "sandbox API is loopback-only"})
            return
        try:
            payload: dict[str, Any] = self._read_json_body() if method == "POST" else {}
            result = self._dispatch_sandbox(method, path, query, payload)
            self._write_json(
                HTTPStatus.OK, result, max_size=self.max_sandbox_response_size
            )
        except (SandboxRequestTooLargeError, SandboxResponseTooLargeError) as exc:
            self._write_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"message": str(exc)})
        except FileNotFoundError as exc:
            self._write_json(HTTPStatus.NOT_FOUND, {"message": str(exc)})
        except NotADirectoryError as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"message": str(exc)})
        except SandboxMethodNotAllowedError as exc:
            self._write_json(
                HTTPStatus.METHOD_NOT_ALLOWED,
                {"message": str(exc)},
                extra_headers={"Allow": _allowed_sandbox_methods(exc.path)},
            )
        except (ValueError, TypeError) as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"message": str(exc)})
        except RuntimeError as exc:
            _LOG.exception("sandbox operation failed")
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"message": str(exc)})
        except OSError as exc:
            _LOG.exception("sandbox operation failed")
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"message": str(exc)})
        except Exception as exc:  # noqa: BLE001 - keep the HTTP connection well-formed
            _LOG.exception("unexpected sandbox operation failure")
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"message": str(exc)})

    def _sandbox_route_error(self, method: str, path: str) -> Exception:
        """Map an unmatched method+path to 404 or 405 by what actually exists.

        判定顺序:实例存在性(404) → URL 形状(404) → 方法注册(405)。
        - bare URL 无 id 段,无实例可查,形状即一切:method 未注册 → 405;
        - 形状本身不在路由表(url 拼错/资源不存在) → 404;
        - 形状已注册但该 method 没注册,且实例存在 → 405;实例不存在 → 404。
        """
        if path == _SANDBOX_PREFIX:
            return SandboxMethodNotAllowedError(method, path)
        rest = path[len(_SANDBOX_PREFIX) + 1:]
        parts = rest.split("/", 2)
        if not parts or not parts[0]:
            return SandboxMethodNotAllowedError(method, path)
        shape_registered = any(
            _shape_matches(shape, path) for _, shape, _ in _SANDBOX_ROUTES
        )
        if not shape_registered:
            return FileNotFoundError(f"sandbox resource {path} not found")
        instance_id = parts[0]
        if self.sandbox_manager.get(instance_id) is None:
            return FileNotFoundError(f"sandbox {instance_id} not found")
        return SandboxMethodNotAllowedError(method, path)

    def _dispatch_sandbox(
        self, method: str, path: str, query: dict[str, list[str]], payload: dict[str, Any]
    ) -> dict[str, Any]:
        # 先按路由表做纯 method+path 匹配;组合未注册时区分 404(资源/形状不存在)
        # 与 405(资源存在但方法未注册),后续分支只认 route key。
        route = _match_sandbox_route(method, path)
        if route is None:
            raise self._sandbox_route_error(method, path)

        if route == "create":
            options = self._build_sandbox_options(payload)
            try:
                instance_id = self.sandbox_manager.create(options)
            except Exception as exc:
                if isinstance(exc, RuntimeError) and str(exc).startswith("create sandbox failed"):
                    raise
                raise RuntimeError(f"create sandbox failed: {exc}") from exc
            return {
                "instance_id": instance_id,
            }

        rest = path[len(_SANDBOX_PREFIX) + 1:]  # 去掉 "sandboxes/"
        parts = rest.split("/", 2)
        instance_id = parts[0]
        sub = parts[1] if len(parts) > 1 else ""
        action = parts[2] if len(parts) > 2 else ""

        if route == "delete":
            success = self.sandbox_manager.delete(instance_id)
            if not success:
                return {"success": False, "message": "sandbox not found"}
            return {"success": True}

        sandbox = self.sandbox_manager.get(instance_id)
        if sandbox is None:
            raise FileNotFoundError(f"sandbox {instance_id} not found")

        if route == "execute":
            if "command" not in payload:
                raise ValueError("command is required")
            working_dir = payload.get("working_dir")
            env = payload.get("env")
            timeout = payload.get("timeout")
            trace_id = payload.get("trace_id")
            if trace_id is not None and not isinstance(trace_id, str):
                raise TypeError("trace_id must be a string")
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
            try:
                return sandbox.exec(
                    payload["command"], working_dir=working_dir, env=env, timeout=timeout, trace_id=trace_id,
                )
            except Exception as exc:
                raise RuntimeError(f"execute failed: {exc}") from exc

        if route == "files_read":
            file_path = self._query_value(query, "path")
            if not file_path:
                raise ValueError("path is required")
            mode = self._query_value(query, "mode", "rb")
            if not isinstance(mode, str):
                raise TypeError("mode must be a string")
            trace_id = self._query_value(query, "trace_id", "")
            try:
                content = sandbox.read_file(file_path, mode, trace_id=trace_id)
            except Exception as exc:
                raise RuntimeError(f"read file failed: {exc}") from exc
            if isinstance(content, (bytes, bytearray, memoryview)):
                binary = bytes(content)
                return {
                    "path": file_path,
                    "mode": mode,
                    "content": base64.b64encode(binary).decode("ascii"),
                    "content_encoding": "base64",
                }
            return {
                "path": file_path,
                "mode": mode,
                "content": content,
                "content_encoding": "text",
            }

        if route == "files_write":
            file_path = self._query_value(query, "path")
            if not file_path:
                raise ValueError("path is required")
            mode = self._query_value(query, "mode", "wb")
            if not isinstance(mode, str):
                raise TypeError("mode must be a string")
            content = self._read_raw_body().decode("utf-8")
            if not content:
                raise ValueError("content is required")
            if "b" in mode:
                try:
                    data: Any = base64.b64decode(content, validate=True)
                except Exception as exc:
                    raise ValueError(f"invalid base64 content: {exc}") from exc
            else:
                data = content
            trace_id = self._query_value(query, "trace_id", "")
            try:
                sandbox.write_file(file_path, data, mode, trace_id=trace_id)
            except Exception as exc:
                raise RuntimeError(f"write file failed: {exc}") from exc
            return {"success": True, "path": file_path}

        if route == "files_list":
            file_path = self._query_value(query, "path")
            if not file_path:
                raise ValueError("path is required")
            recursive = self._query_value(query, "recursive", "false").lower() == "true"
            include_files = self._query_value(query, "include_files", "true").lower() == "true"
            include_dirs = self._query_value(query, "include_dirs", "true").lower() == "true"
            max_depth_str = self._query_value(query, "max_depth", "")
            max_depth: Optional[int] = None
            if max_depth_str:
                try:
                    max_depth = int(max_depth_str)
                except ValueError as exc:
                    raise ValueError("max_depth must be an integer") from exc
                if max_depth < 0:
                    raise ValueError("max_depth must be non-negative")
            trace_id = self._query_value(query, "trace_id", "")
            try:
                return sandbox.list_files(
                    file_path,
                    recursive=recursive,
                    max_depth=max_depth,
                    include_files=include_files,
                    include_dirs=include_dirs,
                    trace_id=trace_id,
                )
            except Exception as exc:
                raise RuntimeError(f"list files failed: {exc}") from exc

        if route == "files_search":
            file_path = self._query_value(query, "path")
            if not file_path:
                raise ValueError("path is required")
            pattern = self._query_value(query, "pattern")
            if not pattern:
                raise ValueError("pattern is required")
            excludes_str = self._query_value(query, "exclude_patterns", "")
            excludes: Optional[list[str]] = None
            if excludes_str:
                excludes = [s.strip() for s in excludes_str.split(",") if s.strip()]
            trace_id = self._query_value(query, "trace_id", "")
            try:
                return sandbox.search_files(
                    file_path, pattern, exclude_patterns=excludes,
                    trace_id=trace_id,
                )
            except Exception as exc:
                raise RuntimeError(f"search files failed: {exc}") from exc

        # route 匹配保证了此处不可达;保留 raise 以满足类型检查与防御。
        raise SandboxMethodNotAllowedError(method, path)

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

    def _read_raw_body(self) -> bytes:
        """Read the request body as raw bytes (for PUT write, not JSON)."""
        content_length = self.headers.get("Content-Length")
        if content_length is None:
            raise ValueError("Content-Length is required")
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
        return bytes(body)

    @staticmethod
    def _build_sandbox_options(payload: dict[str, Any]) -> SandboxCreateOptions:
        """Build SandboxCreateOptions from the create endpoint JSON payload.

        Fields map 1:1 to SandboxCreateOptions (sandbox.py). All optional.
        """
        ports_raw = payload.get("ports")
        if ports_raw is not None:
            if not isinstance(ports_raw, list) or not all(
                isinstance(p, str) for p in ports_raw
            ):
                raise TypeError("ports must be an array of strings")
            ports = ports_raw
        else:
            ports = None
        env_raw = payload.get("env")
        if env_raw is not None:
            if not isinstance(env_raw, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in env_raw.items()
            ):
                raise TypeError("env must be an object containing string values")
            env = env_raw
        else:
            env = None
        cpu = payload.get("cpu")
        if cpu is not None and (isinstance(cpu, bool) or not isinstance(cpu, int)):
            raise TypeError("cpu must be an integer")
        memory = payload.get("memory")
        if memory is not None and (isinstance(memory, bool) or not isinstance(memory, int)):
            raise TypeError("memory must be an integer")
        idle_timeout = payload.get("idle_timeout", 300)
        if isinstance(idle_timeout, bool) or not isinstance(idle_timeout, int):
            raise TypeError("idle_timeout must be an integer")
        if idle_timeout <= 0:
            raise ValueError("idle_timeout must be greater than zero")
        trace_id = payload.get("trace_id", "")
        if not isinstance(trace_id, str):
            raise TypeError("trace_id must be a string")
        return SandboxCreateOptions(
            image=payload.get("image"),
            cpu=cpu,
            memory=memory,
            sandbox_type=payload.get("sandbox_type", ""),
            ports=ports,
            upstream=payload.get("upstream"),
            working_dir=payload.get("working_dir"),
            env=env,
            idle_timeout=idle_timeout,
            trace_id=trace_id,
            user=payload.get("user"),
        )

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
        recursive = self._query_value(query, "recursive", "false").lower() == "true"
        try:
            result = self.files.mkdir(path, mode=mode, recursive=recursive)
            self._write_json(HTTPStatus.OK, result)
        except (ValueError, FileNotFoundError) as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"message": str(exc)})
        except OSError as exc:
            _LOG.exception("file mkdir failed")
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
        if path == _SANDBOX_PREFIX or path.startswith(f"{_SANDBOX_PREFIX}/"):
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
