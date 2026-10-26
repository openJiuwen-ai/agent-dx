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

- yaml endpoint routing via ``sandbox_api_kind`` (405 ``Allow`` header included);
- loopback-only access control;
- request/response size limits (independent of the frontend file limit) and
  the size-limited body readers enforcing them — ``/v1/exec`` deliberately
  borrows the same limits, so it calls ``_read_json_body`` off this mixin;
- payload validation and the mapping of Python exceptions to HTTP codes.

Two yaml-contract API families share this mixin, both loopback-only (the
legacy ``/v1/sandbox/sandboxes`` prefix is not carried over to this branch):

- management plane ``/api/environment/v1/environments`` — Envelope + base64
  success bodies, ErrorEnvelope failures (environment.yaml contract);
- data plane ``POST /direct/{environmentId}/invoke`` — single action-dispatch
  endpoint, raw action JSON, action-level failures ride HTTP 200
  (data-plane.yaml contract; args semantics mirror the execd runtime).

The mixin relies on the host handler for the transport-level pieces that the
file API shares (``_write_json``, ``_request_body``) — it is duck-typed on
purpose, there is no ``SandboxRequestMixin`` without a
``BaseHTTPRequestHandler`` subclass carrying it.
"""

from __future__ import annotations

import base64
import fnmatch
import ipaddress
import json
import logging
import os
import uuid
from datetime import datetime
from http import HTTPStatus
from typing import Any, Optional

from .sandbox.sandbox import Sandbox, SandboxCreateOptions
from .sandbox_manager import SandboxManager

_LOG = logging.getLogger(__name__)
DEFAULT_MAX_SANDBOX_REQUEST_SIZE = 512 * 1024 * 1024
DEFAULT_MAX_SANDBOX_RESPONSE_SIZE = 512 * 1024 * 1024

# ── yaml 契约端点(管理面 environment.yaml + 数据面 data-plane.yaml)──────────
# 管理面:create/delete;成功响应 Envelope(data=base64 JSON),错误 ErrorEnvelope。
MGMT_ENV_PREFIX = "/api/environment/v1/environments"
# 数据面:单端点 action 分发,参数在 body args,action 级失败走 HTTP 200。
INVOKE_PREFIX = "/direct"

# 本期支持的 invoke action:契约原有 27 个枚举中的 5 个 + 本方案新增 file.search。
# process.run 与 process.exec 为同义别名(契约归一化目标 cmd_run);
# file.read_chunk/file.write_chunk 属契约有、本服务不实现(一律 400)。
INVOKE_ACTIONS: frozenset[str] = frozenset({
    "process.exec", "process.run",
    "file.read",
    "file.write",
    "file.list",
    "file.search",
})

# 管理面 ErrorEnvelope 的 符号码/重试/结果语义(设计方案符号码映射表)。
_MGMT_ERROR_SYMBOLS: dict[HTTPStatus, tuple[str, str, str]] = {
    HTTPStatus.BAD_REQUEST: ("invalid", "never", "not_started"),
    HTTPStatus.NOT_FOUND: ("not_found", "never", "terminal"),
    HTTPStatus.METHOD_NOT_ALLOWED: ("method_not_allowed", "never", "not_started"),
    HTTPStatus.REQUEST_ENTITY_TOO_LARGE: ("too_large", "never", "not_started"),
    HTTPStatus.INTERNAL_SERVER_ERROR: ("internal", "after_backoff", "unknown"),
}

# L-2:原 master 私有顶层 body 字段,按契约收口——upstream/working_dir/user
# 3 个业务字段改经 extra_config 携带、sandbox_type 由顶层 runtime 字段承载、
# trace_id 改走 X-Trace-ID 头;顶层出现即 400 并提示正确位置。
_MOVED_BODY_FIELDS = ("sandbox_type", "upstream", "working_dir", "user", "trace_id")


def sandbox_api_kind(path: str) -> Optional[str]:
    """Which sandbox API family a path belongs to: mgmt / invoke / None.

    http_server 各 do_* 的分发统一问这一个函数,再交给
    ``_sandbox_api_request``;新增前缀只改这里,不碰 http_server。
    旧前缀 ``/v1/sandbox/sandboxes`` 不在本分支注册(返回 None),
    由 http_server 统一按端点未找到返回 404。
    """
    if path == MGMT_ENV_PREFIX or path.startswith(f"{MGMT_ENV_PREFIX}/"):
        return "mgmt"
    if path == INVOKE_PREFIX or path.startswith(f"{INVOKE_PREFIX}/"):
        return "invoke"
    return None


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


class SandboxSearchLimitError(Exception):
    """file.search 匹配结果超出 max_results(HTTP 429 + 失败形态体)。"""


class SandboxRequestMixin:
    """Sandbox request handling, mixed into the executor request handler.

    Host contract (provided by ``_ExecutorRequestHandler`` in ``http_server``):
    ``sandbox_manager``, ``max_sandbox_request_size``,
    ``max_sandbox_response_size`` as class attributes, plus the shared
    ``_write_json`` / ``_request_body`` helpers and the ``rfile`` /
    ``headers`` / ``client_address`` / ``close_connection`` of
    ``BaseHTTPRequestHandler``.
    """

    sandbox_manager: SandboxManager
    max_sandbox_request_size = DEFAULT_MAX_SANDBOX_REQUEST_SIZE
    max_sandbox_response_size = DEFAULT_MAX_SANDBOX_RESPONSE_SIZE

    # ── 统一入口(http_server 各 do_* 经 sandbox_api_kind 路由到此)──────

    def _sandbox_api_request(self, method: str, path: str, query: dict[str, list[str]]) -> None:
        """Route a request to its sandbox API family; both families are loopback-only."""
        if not self._client_is_loopback():
            self._write_json(HTTPStatus.FORBIDDEN, {"message": "sandbox API is loopback-only"})
            return
        if sandbox_api_kind(path) == "invoke":
            self._invoke_request(method, path, query)
        else:
            self._mgmt_request(method, path, query)

    # ── 管理面:/api/environment/v1/environments(Envelope 成功/ErrorEnvelope)──

    def _mgmt_request(self, method: str, path: str, query: dict[str, list[str]]) -> None:
        request_id = self.headers.get("X-Request-Id") or str(uuid.uuid4())
        try:
            result = self._dispatch_mgmt(method, path, request_id)
            self._write_json(
                HTTPStatus.OK,
                self._mgmt_envelope(result),
                max_size=self.max_sandbox_response_size,
                extra_headers={"X-Request-Id": request_id},
            )
        except (SandboxRequestTooLargeError, SandboxResponseTooLargeError) as exc:
            self._write_mgmt_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, str(exc), request_id)
        except FileNotFoundError as exc:
            self._write_mgmt_error(HTTPStatus.NOT_FOUND, str(exc), request_id)
        except SandboxMethodNotAllowedError as exc:
            # 405 分支不读请求体(如带 body 的 PUT),断开 keep-alive 防串包
            # (同 _invoke_request/_route_or_404 策略)。
            self.close_connection = True
            self._write_mgmt_error(
                HTTPStatus.METHOD_NOT_ALLOWED, str(exc), request_id,
                extra_headers={"Allow": self._mgmt_allowed_methods(exc.path)},
            )
        except (ValueError, TypeError) as exc:
            self._write_mgmt_error(HTTPStatus.BAD_REQUEST, str(exc), request_id)
        except Exception as exc:  # noqa: BLE001 - keep the HTTP connection well-formed
            _LOG.exception("management sandbox operation failed")
            self._write_mgmt_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc), request_id)

    def _dispatch_mgmt(self, method: str, path: str, request_id: str) -> dict[str, Any]:
        if path == MGMT_ENV_PREFIX:
            if method != "POST":
                raise SandboxMethodNotAllowedError(method, path)
            payload = self._read_json_body()
            # L-2:snapshotId 分流——从快照恢复,其余 create 参数按契约忽略。
            snapshot_id = payload.get("snapshotId")
            if snapshot_id is not None:
                if not isinstance(snapshot_id, str) or not snapshot_id:
                    raise ValueError("snapshotId must be a non-empty string")
                try:
                    sandbox = Sandbox(
                        checkpoint_id=snapshot_id,
                        trace_id=self.headers.get("X-Trace-ID", ""),
                    )
                    instance_id = sandbox.get_instance_id()
                except Exception as exc:
                    if isinstance(exc, RuntimeError) and str(exc).startswith("restore sandbox failed"):
                        raise
                    raise RuntimeError(f"restore sandbox failed: {exc}") from exc
                self.sandbox_manager.register(instance_id, sandbox)
                return self._create_result(instance_id, request_id)
            options = self._build_sandbox_options(
                payload, self.headers.get("X-Trace-ID", "")
            )
            try:
                instance_id = self.sandbox_manager.create(options)
            except Exception as exc:
                if isinstance(exc, RuntimeError) and str(exc).startswith("create sandbox failed"):
                    raise
                raise RuntimeError(f"create sandbox failed: {exc}") from exc
            return self._create_result(instance_id, request_id)
        rest = path[len(MGMT_ENV_PREFIX) + 1:]
        parts = rest.split("/")
        if len(parts) == 1 and parts[0]:
            sandbox_id = parts[0]
            if method != "DELETE":
                # 与历史语义同序:实例不存在优先 404,存在才 405。
                if self.sandbox_manager.get(sandbox_id) is None:
                    raise FileNotFoundError(f"sandbox {sandbox_id} not found")
                raise SandboxMethodNotAllowedError(method, path)
            if not self.sandbox_manager.delete(sandbox_id):
                raise FileNotFoundError(f"sandbox {sandbox_id} not found")
            return {"success": True}
        raise FileNotFoundError(f"sandbox resource {path} not found")

    @staticmethod
    def _create_result(instance_id: str, request_id: str) -> dict[str, Any]:
        # 契约 CreateEnvironmentResult:{environmentId, status, requestId}。
        return {
            "environmentId": instance_id,
            "status": "running",
            "requestId": request_id,
        }

    @staticmethod
    def _mgmt_envelope(payload: dict[str, Any]) -> dict[str, Any]:
        """契约 Envelope:data = base64 编码的 JSON 业务结果(environment.yaml)。"""
        encoded = base64.b64encode(
            json.dumps(payload, separators=(",", ":")).encode("utf-8")
        ).decode("ascii")
        return {"code": 200, "message": "ok", "data": encoded}

    def _write_mgmt_error(
        self, status: HTTPStatus, message: str, request_id: str,
        extra_headers: Optional[dict[str, str]] = None,
    ) -> None:
        symbol, retry, outcome = _MGMT_ERROR_SYMBOLS.get(
            status, ("internal", "after_backoff", "unknown")
        )
        # ErrorEnvelope 响应同样回显 X-Request-Id 头(契约 Error response 定义),
        # 与成功路径一致;调用方传入的头(如 405 的 Allow)合并保留。
        headers = {"X-Request-Id": request_id}
        if extra_headers:
            headers.update(extra_headers)
        self._write_json(
            status,
            {
                "code": int(status),
                "message": message,
                "data": None,
                "error": {
                    "code": symbol,
                    "retry": retry,
                    "outcome": outcome,
                    "requestId": request_id,
                },
            },
            extra_headers=headers,
        )

    @staticmethod
    def _mgmt_allowed_methods(path: str) -> str:
        if path == MGMT_ENV_PREFIX:
            return "POST"
        return "DELETE"

    # ── 数据面:/direct/{environmentId}/invoke(raw JSON;action 级失败走 200)─

    def _invoke_request(self, method: str, path: str, query: dict[str, list[str]]) -> None:
        rest = path[len(INVOKE_PREFIX) + 1:] if len(path) > len(INVOKE_PREFIX) else ""
        parts = rest.split("/") if rest else []
        if len(parts) != 2 or not parts[0] or parts[1] != "invoke":
            self._write_json(
                HTTPStatus.NOT_FOUND, {"error": f"sandbox resource {path} not found"}
            )
            return
        if method != "POST":
            # 请求可能携带未读 body,断开 keep-alive 以免串包(同 _route_or_404 策略)。
            self.close_connection = True
            self._write_json(
                HTTPStatus.METHOD_NOT_ALLOWED, {"error": "method not allowed"},
                extra_headers={"Allow": "POST"},
            )
            return
        instance_id = parts[0]
        try:
            payload = self._read_json_body()
            # L-2:trace 走 X-Trace-ID 头,body 顶层旧位置拒绝。
            if "trace_id" in payload:
                raise ValueError("trace_id moved to X-Trace-ID header")
            # requestId 优先级:头 > body > 服务端生成(设计方案"认证与请求标识")。
            request_id = (
                self.headers.get("X-ADX-Request-ID")
                or payload.get("requestId")
                or str(uuid.uuid4())
            )
            if not isinstance(request_id, str) or not request_id:
                raise TypeError("requestId must be a non-empty string")
            action = payload.get("action")
            if not isinstance(action, str) or not action:
                raise ValueError("action is required")
            args = payload.get("args")
            if args is None:
                args = {}
            if not isinstance(args, dict):
                raise TypeError("args must be an object")
            trace_id = self.headers.get("X-Trace-ID", "")
            if action not in INVOKE_ACTIONS:
                raise ValueError(f"unsupported action: {action}")
            sandbox = self.sandbox_manager.get(instance_id)
            if sandbox is None:
                raise FileNotFoundError(f"sandbox {instance_id} not found")
            _LOG.info(
                "[invoke] instance=%s action=%s request_id=%s trace_id=%s",
                instance_id, action, request_id, trace_id,
            )
            result = self._dispatch_invoke(sandbox, action, args, trace_id)
            self._write_json(
                HTTPStatus.OK, result, max_size=self.max_sandbox_response_size
            )
        except (SandboxRequestTooLargeError, SandboxResponseTooLargeError) as exc:
            self._write_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": str(exc)})
        except SandboxSearchLimitError as exc:
            # 契约 file.search:数量超限返回 429,错误体为失败形态(无部分结果)。
            self._write_json(
                HTTPStatus.TOO_MANY_REQUESTS,
                {"entries": [], "error_code": "RESOURCE_EXHAUSTED", "error": str(exc)},
            )
        except FileNotFoundError as exc:
            self._write_json(HTTPStatus.NOT_FOUND, {"error": str(exc)})
        except (ValueError, TypeError) as exc:
            self._write_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except Exception as exc:  # noqa: BLE001 - keep the HTTP connection well-formed
            _LOG.exception("invoke failed")
            self._write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

    def _dispatch_invoke(
        self, sandbox: Any, action: str, args: dict[str, Any], trace_id: str
    ) -> dict[str, Any]:
        if action in ("process.exec", "process.run"):
            return self._invoke_exec(sandbox, args, trace_id)
        if action == "file.read":
            return self._invoke_file_read(sandbox, args, trace_id)
        if action == "file.write":
            return self._invoke_file_write(sandbox, args, trace_id)
        if action == "file.list":
            return self._invoke_file_list(sandbox, args, trace_id)
        if action == "file.search":
            return self._invoke_file_search(sandbox, args, trace_id)
        raise ValueError(f"unsupported action: {action}")  # 白名单已挡,防御保留

    @staticmethod
    def _invoke_int_arg(
        args: dict[str, Any], name: str, default: int, lo: int, hi: int
    ) -> int:
        """int 型 args 参数:非整数拒绝(400),越界按 execd 语义静默 clamp。"""
        value = args.get(name, default)
        if value is None:
            value = default
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an integer")
        return max(lo, min(hi, value))

    @staticmethod
    def _invoke_path_arg(args: dict[str, Any]) -> str:
        path = args.get("path")
        if not isinstance(path, str) or not path:
            raise ValueError("path is required")
        return path

    @staticmethod
    def _invoke_exec(sandbox: Any, args: dict[str, Any], trace_id: str) -> dict[str, Any]:
        command = args.get("cmd") if args.get("cmd") is not None else args.get("command")
        if command is None:
            raise ValueError("cmd (or command) is required")
        if isinstance(command, str):
            # 字符串经沙箱内 /bin/sh -c 执行,须非空。
            if not command:
                raise ValueError("cmd (or command) is required")
        elif not isinstance(command, list):
            # 数组不经 shell 直接执行(契约外扩展);空数组与元素合法性由
            # 运行期判定,错误随 action 级失败走 200。
            raise TypeError("cmd must be a string or an array of strings")
        cwd = args.get("cwd") if args.get("cwd") is not None else args.get("working_dir")
        if cwd is not None and not isinstance(cwd, str):
            raise TypeError("cwd must be a string")
        envs = args.get("envs") if args.get("envs") is not None else args.get("env")
        if envs is not None:
            if not isinstance(envs, dict) or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in envs.items()
            ):
                raise TypeError("envs must be an object containing string values")
        timeout = args.get("timeout")
        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise TypeError("timeout must be a number")
            if timeout < 0:
                raise ValueError("timeout must be non-negative")
        try:
            # 底层返回 {returncode, stdout, stderr};契约字段为 exit_code(适配转换)。
            result = sandbox.exec(
                command, working_dir=cwd, env=envs, timeout=timeout, trace_id=trace_id,
            )
        except Exception as exc:
            return {"stdout": "", "stderr": f"execute failed: {exc}", "exit_code": -1}
        if not isinstance(result, dict):
            return {"stdout": "", "stderr": "execute returned no result", "exit_code": -1}
        return {
            "stdout": result.get("stdout", ""),
            "stderr": result.get("stderr", ""),
            "exit_code": result.get("returncode", -1),
        }

    def _invoke_file_read(self, sandbox: Any, args: dict[str, Any], trace_id: str) -> dict[str, Any]:
        path = self._invoke_path_arg(args)
        binary = self._optional_bool(args, "binary", False)
        mode = "rb" if binary else "r"
        try:
            content = sandbox.read_file(path, mode, trace_id=trace_id)
        except Exception as exc:
            return {"data": None, "error": f"read file failed: {exc}"}
        if binary:
            raw = content if isinstance(content, (bytes, bytearray, memoryview)) \
                else str(content).encode("utf-8")
            # 契约二进制编码为 hex(非 base64),对齐 execd fs_read。
            return {"data": bytes(raw).hex(), "error": None}
        if isinstance(content, str):
            return {"data": content, "error": None}
        return {"data": bytes(content).decode("utf-8", errors="replace"), "error": None}

    def _invoke_file_write(self, sandbox: Any, args: dict[str, Any], trace_id: str) -> dict[str, Any]:
        path = self._invoke_path_arg(args)
        data = args.get("data")
        if not isinstance(data, str):
            raise ValueError("data is required")
        binary = self._optional_bool(args, "binary", False)
        # append 为契约外扩展:与 binary 组合映射底层 a/ab(默认 w/wb)。
        append = self._optional_bool(args, "append", False)
        mode = ("ab" if binary else "a") if append else ("wb" if binary else "w")
        if binary:
            try:
                payload: Any = bytes.fromhex(data)
            except ValueError as exc:
                raise ValueError(f"invalid hex data: {exc}") from exc
            size = len(payload)
        else:
            payload = data
            size = len(data.encode("utf-8"))
        try:
            sandbox.write_file(path, payload, mode, trace_id=trace_id)
        except Exception as exc:
            return {
                "name": os.path.basename(path), "path": path, "type": "file",
                "size": 0, "error": f"write file failed: {exc}",
            }
        return {
            "name": os.path.basename(path), "path": path, "type": "file",
            "size": size, "error": None,
        }

    def _invoke_file_list(self, sandbox: Any, args: dict[str, Any], trace_id: str) -> dict[str, Any]:
        path = self._invoke_path_arg(args)
        depth = self._invoke_int_arg(args, "depth", 1, 1, 64)
        # 契约 depth = 返回条目的最大相对深度;底层 _scan 自根(0 层)计数、
        # current_depth > max_depth 才停止——传 max_depth = depth - 1 才能将
        # 返回收敛到相对深度 <= depth(与 file.search 的深度剪枝语义一致)。
        try:
            result = sandbox.list_files(
                path,
                recursive=depth > 1,
                max_depth=depth - 1,
                include_files=True,
                include_dirs=True,
                trace_id=trace_id,
            )
        except Exception as exc:
            return {"entries": [], "error": f"list files failed: {exc}"}
        items = result.get("items", []) if isinstance(result, dict) else []
        return {"entries": [self._to_contract_entry(item) for item in items], "error": None}

    def _invoke_file_search(self, sandbox: Any, args: dict[str, Any], trace_id: str) -> dict[str, Any]:
        path = self._invoke_path_arg(args)
        # 契约 FileSearchArgs:pattern 可选默认 '*'(文件名或完整路径 glob,
        # 大小写敏感,'*' 跨分隔符);exclude 只过滤不剪枝;max_results 超限 429。
        pattern = args.get("pattern")
        if pattern is None:
            pattern = "*"
        if not isinstance(pattern, str):
            raise TypeError("pattern must be a string")
        excludes = args.get("exclude_patterns")
        if excludes is None:
            excludes = []
        if not isinstance(excludes, list) or not all(
            isinstance(item, str) for item in excludes
        ):
            raise TypeError("exclude_patterns must be an array of strings")
        depth = self._invoke_int_arg(args, "depth", 64, 1, 64)
        max_results = self._invoke_int_arg(args, "max_results", 1000, 1, 10000)
        # max_entries/timeout_ms 经校验接受:py 底层递归无扫描计数与超时机制,
        # 不触发对应的 429/408(与 permissions 恒 null 同属两实现已知差异)。
        self._invoke_int_arg(args, "max_entries", 100000, 1, 1000000)
        self._invoke_int_arg(args, "timeout_ms", 10000, 1, 30000)
        try:
            # 底层按 depth 剪枝取文件全集并统计扫描条目数(scanned:遍历遇到
            # 的文件与目录总数,不含根);pattern 文件名/完整路径双形态与
            # exclude 过滤在适配层完成,保证大小写敏感语义跨平台一致
            # (fnmatchcase)。
            result = sandbox.search_files(
                path, "*", exclude_patterns=None, depth=depth, trace_id=trace_id
            )
        except Exception as exc:
            # 契约失败分支 additionalProperties: false(仅 entries/error_code/
            # error),不带 scanned_entries。
            return {
                "entries": [],
                "error_code": "FILESYSTEM_ERROR",
                "error": f"search files failed: {exc}",
            }
        items = result.get("items", []) if isinstance(result, dict) else []
        scanned = result.get("scanned") if isinstance(result, dict) else None
        if not isinstance(scanned, int):
            scanned = len(items)  # 部署偏斜回退:以返回条目数近似
        matched = []
        for item in items:
            item_path = str(item.get("path", ""))
            # 匹配用路径统一为 posix 分隔:契约 glob 语义按运行时(Linux)
            # 路径定义,规范化消除宿主平台分隔符差异;响应内路径保持底层原样。
            match_path = item_path.replace("\\", "/")
            name = str(item.get("name", ""))
            if not (fnmatch.fnmatchcase(name, pattern)
                    or fnmatch.fnmatchcase(match_path, pattern)):
                continue
            if any(fnmatch.fnmatchcase(name, pat) or fnmatch.fnmatchcase(match_path, pat)
                   for pat in excludes):
                continue
            matched.append(self._to_contract_entry(item))
        # 契约语义:结果按 path 排序;失败不返回部分结果(超限整体 429)。
        matched.sort(key=lambda entry: entry["path"])
        if len(matched) > max_results:
            raise SandboxSearchLimitError(
                "file search max_results exceeded; narrow the search"
            )
        return {"entries": matched, "scanned_entries": scanned, "error": None}

    @staticmethod
    def _to_contract_entry(item: dict[str, Any]) -> dict[str, Any]:
        """底层条目 → 契约条目:type 改类别(file/dir)、modified_time 转 unix 秒。

        底层 modified_time 是本地时区格式化字符串,按本机时区解释;
        permissions 底层无数据,置 None(与 execd 条目结构的已知差异)。
        """
        is_dir = bool(item.get("is_directory"))
        modified_time = item.get("modified_time")
        epoch: Optional[float] = None
        if isinstance(modified_time, str) and modified_time:
            try:
                epoch = datetime.strptime(
                    modified_time, "%Y-%m-%dT%H:%M:%S.%f"
                ).timestamp()
            except ValueError:
                epoch = None
        return {
            "name": item.get("name", ""),
            "path": item.get("path", ""),
            "type": "dir" if is_dir else "file",
            "size": item.get("size", 0),
            "permissions": None,
            "modified_time": epoch,
        }

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

    @staticmethod
    def _build_sandbox_options(payload: dict[str, Any], trace_id: str = "") -> SandboxCreateOptions:
        """Build SandboxCreateOptions from the contract-shaped create payload.

        L-2 终态:读取 13 个契约字段(name/image/runtime/rootfs/cpu/memory/env/
        ports/idleTimeoutSeconds/mounts/tunnel/extra_config;snapshotId 由
        _dispatch_mgmt 提前分流),私有字段位置非法即 400(sandbox_type→runtime、
        upstream/working_dir/user→extra_config、trace_id→X-Trace-ID 头)。
        """
        for key in _MOVED_BODY_FIELDS:
            if key in payload:
                if key == "trace_id":
                    target = "X-Trace-ID header"
                elif key == "sandbox_type":
                    target = "runtime"
                else:
                    target = "extra_config"
                raise ValueError(f"{key} moved to {target}")

        name = payload.get("name")
        if name is not None and not isinstance(name, str):
            raise TypeError("name must be a string")
        image = payload.get("image")
        if image is not None and not isinstance(image, str):
            raise TypeError("image must be a string")

        # env / ports / cpu / memory / idle_timeout 校验沿用原实现。
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
        # 契约字段为 idleTimeoutSeconds(默认 0=关闭空闲回收);
        # idle_timeout 为旧内部名兼容别名,主名优先。
        idle_timeout = payload.get("idleTimeoutSeconds")
        if idle_timeout is None:
            idle_timeout = payload.get("idle_timeout", 0)
        if isinstance(idle_timeout, bool) or not isinstance(idle_timeout, int):
            raise TypeError("idleTimeoutSeconds must be an integer")
        if idle_timeout < 0:
            raise ValueError("idleTimeoutSeconds must be non-negative")

        # runtime:执行器类型,直通底层 sandbox_type 字段(执行器选择);
        # extra_config:三键提取映射底层顶层字段,剩余键整体透传,
        # sandbox_type 不在其识别键内(出现即 400 提示改用 runtime)。
        runtime = payload.get("runtime", "")
        if not isinstance(runtime, str):
            raise TypeError("runtime must be a string")
        extra_raw = payload.get("extra_config")
        if extra_raw is not None and not isinstance(extra_raw, dict):
            raise TypeError("extra_config must be an object")
        extra = dict(extra_raw) if extra_raw else {}
        if "sandbox_type" in extra:
            raise ValueError("sandbox_type moved to runtime")
        upstream = extra.pop("upstream", None)
        if upstream is not None and not isinstance(upstream, str):
            raise TypeError("extra_config.upstream must be a string")
        working_dir = extra.pop("working_dir", None)
        if working_dir is not None and not isinstance(working_dir, str):
            raise TypeError("extra_config.working_dir must be a string")
        user = extra.pop("user", None)
        if user is not None and not isinstance(user, str):
            raise TypeError("extra_config.user must be a string")

        # tunnel:{enabled, proxyPort}。enabled=true 须搭配 upstream;
        # enabled=false 显式关断 upstream 隐含的隧道;缺省由 upstream 自然驱动。
        tunnel = payload.get("tunnel")
        proxy_port = 8766
        if tunnel is not None:
            if not isinstance(tunnel, dict):
                raise TypeError("tunnel must be an object")
            enabled = tunnel.get("enabled")
            if enabled is not None and not isinstance(enabled, bool):
                raise TypeError("tunnel.enabled must be a boolean")
            proxy_port_raw = tunnel.get("proxyPort")
            if proxy_port_raw is not None:
                if isinstance(proxy_port_raw, bool) or not isinstance(proxy_port_raw, int):
                    raise TypeError("tunnel.proxyPort must be an integer")
                if not 1 <= proxy_port_raw <= 65535:
                    raise ValueError("tunnel.proxyPort must be within [1, 65535]")
                proxy_port = proxy_port_raw
            if enabled is True and upstream is None:
                raise ValueError("tunnel.enabled=true requires extra_config.upstream")
            if enabled is False:
                upstream = None

        # rootfs / mounts 翻译:字符串直传;对象由适配层组装(子键保留 + mounts 并入);
        # 仅 image 时维持底层既有组装路径;mounts 需以 rootfs 或 image 为基础。
        rootfs_raw = payload.get("rootfs")
        mounts_raw = payload.get("mounts")
        if mounts_raw is not None and not (
            isinstance(mounts_raw, list) and all(isinstance(m, dict) for m in mounts_raw)
        ):
            raise TypeError("mounts must be an array of objects")
        rootfs_value: Optional[str] = None
        if rootfs_raw is not None and image is not None:
            raise ValueError("rootfs and image are mutually exclusive")
        if isinstance(rootfs_raw, str):
            if mounts_raw is not None:
                raise ValueError("mounts require an object rootfs or image")
            rootfs_value = rootfs_raw
        elif isinstance(rootfs_raw, dict):
            rootfs_config: dict[str, Any] = dict(rootfs_raw)
            if mounts_raw is not None:
                rootfs_config["mounts"] = mounts_raw
            rootfs_value = json.dumps(rootfs_config)
        elif rootfs_raw is not None:
            raise TypeError("rootfs must be a string or an object")
        elif mounts_raw is not None:
            if image is None:
                raise ValueError("mounts require rootfs or image")
            rootfs_value = json.dumps(
                {"type": "image", "imageurl": image, "mounts": mounts_raw}
            )
            image = None  # 已并入 rootfs JSON,不再走底层 image 组装路径

        return SandboxCreateOptions(
            name=name,
            rootfs=rootfs_value,
            image=image,
            cpu=cpu,
            memory=memory,
            env=env,
            ports=ports,
            idle_timeout=idle_timeout,
            working_dir=working_dir,
            sandbox_type=runtime,
            upstream=upstream,
            user=user,
            proxy_port=proxy_port,
            extra_config=extra if extra else None,
            trace_id=trace_id,
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
    def _optional_bool(payload: dict[str, Any], name: str, default: bool) -> bool:
        value = payload.get(name, default)
        if not isinstance(value, bool):
            raise TypeError(f"{name} must be a boolean")
        return value
