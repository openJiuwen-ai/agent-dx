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

"""Sandbox API slice of the executor HTTP server (routes, validation, dispatch).

Extracted from ``http_server.py`` so the file/exec APIs and the sandbox API can
evolve separately. This module owns every sandbox concern:

- route table + method/path matching (405 ``Allow`` header included);
- loopback-only access control;
- request/response size limits (independent of the frontend file limit) and
  the size-limited body readers enforcing them — ``/v1/exec`` deliberately
  borrows the same limits, so it calls ``_read_json_body`` off this mixin;
- payload/query validation and the mapping of Python exceptions to HTTP codes.

The mixin relies on the host handler for the transport-level pieces that the
file API shares (``_write_json``, ``_query_value``, ``_query_bool``,
``_request_body``) — it is duck-typed on purpose, there is no
``SandboxRequestMixin`` without a ``BaseHTTPRequestHandler`` subclass carrying
it.
"""

from __future__ import annotations

import base64
import ipaddress
import json
import logging
from http import HTTPStatus
from typing import Any, Optional

from .sandbox.sandbox import SandboxCreateOptions
from .sandbox_manager import SandboxManager

_LOG = logging.getLogger(__name__)
DEFAULT_MAX_SANDBOX_REQUEST_SIZE = 512 * 1024 * 1024
DEFAULT_MAX_SANDBOX_RESPONSE_SIZE = 512 * 1024 * 1024
SANDBOX_PREFIX = "/v1/sandbox/sandboxes"


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
SANDBOX_ROUTES: tuple[tuple[str, str, str], ...] = (
    ("POST", "bare", "create"),
    ("DELETE", "{id}", "delete"),
    ("POST", "{id}/execute", "execute"),
    ("GET", "{id}/files/read", "files_read"),
    ("PUT", "{id}/files/write", "files_write"),
    ("GET", "{id}/files/list", "files_list"),
    ("GET", "{id}/files/search", "files_search"),
)


def shape_matches(shape: str, path: str) -> bool:
    """Whether ``path`` matches a route shape ("bare", "{id}", "{id}/execute", ...)."""
    if shape == "bare":
        return path == SANDBOX_PREFIX
    parts = path[len(SANDBOX_PREFIX) + 1:].split("/", 2)
    if not parts or not parts[0]:
        return False
    sub = parts[1] if len(parts) > 1 else ""
    action = parts[2] if len(parts) > 2 else ""
    segments = shape.split("/")[1:]  # 去掉 "{id}"
    segments += [""] * (2 - len(segments))
    return sub == segments[0] and action == segments[1]


def match_sandbox_route(method: str, path: str) -> Optional[str]:
    """Pure method+path lookup in SANDBOX_ROUTES; None = combination unregistered.

    Callers still resolve instance existence before treating None as 405,
    so unknown-id requests keep returning 404 (404 优先于 405).
    """
    for route_method, shape, key in SANDBOX_ROUTES:
        if route_method == method and shape_matches(shape, path):
            return key
    return None


def allowed_sandbox_methods(path: str) -> str:
    """Allow-header value for a sandbox path: the methods registered on it."""
    return ", ".join(sorted(
        route_method
        for route_method, shape, _ in SANDBOX_ROUTES
        if shape_matches(shape, path)
    ))


class SandboxRequestMixin:
    """Sandbox request handling, mixed into the executor request handler.

    Host contract (provided by ``_ExecutorRequestHandler`` in ``http_server``):
    ``sandbox_manager``, ``max_sandbox_request_size``,
    ``max_sandbox_response_size`` as class attributes, plus the shared
    ``_write_json`` / ``_query_value`` / ``_query_bool`` / ``_request_body``
    helpers and the ``rfile`` / ``headers`` / ``client_address`` /
    ``close_connection`` of ``BaseHTTPRequestHandler``.
    """

    sandbox_manager: SandboxManager
    max_sandbox_request_size = DEFAULT_MAX_SANDBOX_REQUEST_SIZE
    max_sandbox_response_size = DEFAULT_MAX_SANDBOX_RESPONSE_SIZE

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
                extra_headers={"Allow": allowed_sandbox_methods(exc.path)},
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
        if path == SANDBOX_PREFIX:
            return SandboxMethodNotAllowedError(method, path)
        rest = path[len(SANDBOX_PREFIX) + 1:]
        parts = rest.split("/", 2)
        if not parts or not parts[0]:
            return SandboxMethodNotAllowedError(method, path)
        shape_registered = any(
            shape_matches(route_shape, path) for _, route_shape, _ in SANDBOX_ROUTES
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
        route = match_sandbox_route(method, path)
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

        rest = path[len(SANDBOX_PREFIX) + 1:]  # 去掉 "sandboxes/"
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
            recursive = self._query_bool(query, "recursive", False)
            include_files = self._query_bool(query, "include_files", True)
            include_dirs = self._query_bool(query, "include_dirs", True)
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
