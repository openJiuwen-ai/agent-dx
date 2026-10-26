#!/usr/bin/env python3
# coding=UTF-8

"""Yaml-contract tests for the executor sandbox HTTP slice.

The executor serves three endpoints under the platform contracts
(``platform/api/openapi/sandbox.yaml`` + ``data-plane.yaml``); this module
locks down the adapter behavior of ``sandbox_http.SandboxRequestMixin``:

- management plane ``POST/DELETE /api/environment/v1/environments`` — Envelope
  (base64 ``data``) successes, ErrorEnvelope failures with the symbolic-code
  table, X-Request-Id generation/echo;
- data plane ``POST /direct/{environmentId}/invoke`` — the 6 supported actions,
  arg aliases, hex round-trips, entry conversion, search depth filtering,
  action-level failures riding HTTP 200;
- L-2 create-body wiring — the top-level runtime field, extra_config private
  keys, name/rootfs/mounts translation, snapshotId restore branch, tunnel
  validation, moved-field 400s;
- the legacy ``/v1/sandbox/sandboxes`` prefix stays unregistered (404).

The SandboxManager is real; sandboxes are recording stubs backed by the host
filesystem, so the suite needs no yr runtime (conftest stubs the SDK imports).
Stub item paths are emitted posix-style so the adapter's depth filter behaves
as it does against the real (Linux) sandbox regardless of the host platform.
"""

import base64
import json
import logging
import os
import socket
import time
import urllib.error
import urllib.request
import uuid

import pytest

from yr.agentexecutor import sandbox_http
from yr.agentexecutor.http_server import ExecutorHTTPServer
from yr.agentexecutor.sandbox.filesystem import (
    _list_files_impl,
    _search_files_impl,
)
from yr.agentexecutor.sandbox.sandbox import SandboxCreateOptions
from yr.agentexecutor.sandbox_manager import SandboxManager

MGMT = "/api/environment/v1/environments"


class _StubSandbox:
    """Sandbox stand-in: records every RPC and answers from the host filesystem.

    ``exec`` replies with canned results (a real subprocess would make the
    suite platform-dependent); the file methods do real I/O under tmp_path so
    hex round-trips, chunk slicing and sparse fill exercise actual bytes.
    ``fail`` maps a method name to an exception raised on the next call,
    emulating RPC failures for the action-level-failure (HTTP 200) paths.
    Item shapes mirror the native implementation
    (``sandbox.filesystem._build_fs_item``).
    """

    def __init__(self):
        self.calls = []
        self.terminated = 0
        self.exec_reply = {"returncode": 0, "stdout": "", "stderr": ""}
        self.fail = {}

    def exec(self, command, *, working_dir=None, env=None, timeout=None,
             trace_id=None):
        self.calls.append({
            "method": "exec", "command": command, "working_dir": working_dir,
            "env": env, "timeout": timeout, "trace_id": trace_id,
        })
        if "exec" in self.fail:
            raise self.fail["exec"]
        return dict(self.exec_reply)

    def read_file(self, path, mode="rb", trace_id=None):
        self.calls.append({"method": "read_file", "path": path, "mode": mode,
                           "trace_id": trace_id})
        if "read_file" in self.fail:
            raise self.fail["read_file"]
        # 文本模式固定 UTF-8:对齐真实 Linux 沙箱的默认编码,测试跨平台确定。
        encoding = None if "b" in mode else "utf-8"
        with open(path, mode, encoding=encoding) as handle:
            return handle.read()

    def write_file(self, path, data, mode="wb", trace_id=None):
        self.calls.append({"method": "write_file", "path": path, "mode": mode,
                           "trace_id": trace_id})
        if "write_file" in self.fail:
            raise self.fail["write_file"]
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        encoding = None if "b" in mode else "utf-8"
        with open(path, mode, encoding=encoding) as handle:
            handle.write(data)

    def list_files(self, path, *, recursive=False, max_depth=None,
                   include_files=True, include_dirs=True, trace_id=None):
        self.calls.append({
            "method": "list_files", "path": path, "recursive": recursive,
            "max_depth": max_depth, "include_files": include_files,
            "include_dirs": include_dirs, "trace_id": trace_id,
        })
        if "list_files" in self.fail:
            raise self.fail["list_files"]
        # 委托真实底层实现:遍历语义(max_depth 计数口径、条目上限)与线上一致,
        # 适配层的深度换算由真实行为校验。
        return {"items": _list_files_impl(
            path, recursive=recursive, max_depth=max_depth,
            include_files=include_files, include_dirs=include_dirs)}

    def search_files(self, path, pattern, *, exclude_patterns=None,
                     depth=None, trace_id=None):
        self.calls.append({
            "method": "search_files", "path": path, "pattern": pattern,
            "exclude_patterns": exclude_patterns, "depth": depth,
            "trace_id": trace_id,
        })
        if "search_files" in self.fail:
            raise self.fail["search_files"]
        return _search_files_impl(
            path, pattern, exclude_patterns=exclude_patterns, depth=depth)

    def terminate(self, trace_id=None):
        self.calls.append({"method": "terminate", "trace_id": trace_id})
        self.terminated += 1


class _RecordingManager(SandboxManager):
    """Manager whose ``create`` records options instead of doing yr RPC.

    Tests capture the SandboxCreateOptions the adapter built and get back a
    canned instance id registered against a stub sandbox, so create → invoke
    → delete roundtrips work without a runtime.
    """

    def __init__(self, instance_id="sbx-created-1", create_error=None):
        super().__init__()
        self.stub = _StubSandbox()
        self.instance_id = instance_id
        self.create_error = create_error
        self.created = []
        self.registered = []

    def create(self, options):
        self.created.append(options)
        if self.create_error is not None:
            raise self.create_error
        self.register(self.instance_id, self.stub)
        return self.instance_id

    def register(self, instance_id, sandbox):
        self.registered.append((instance_id, sandbox))
        super().register(instance_id, sandbox)


def _start(manager, **overrides):
    kwargs = {"host": "127.0.0.1", "port": 0, "sandbox_manager": manager}
    kwargs.update(overrides)
    server = ExecutorHTTPServer(**kwargs)
    server.start()
    return server


@pytest.fixture
def sandbox_server():
    """Started server with a recording stub sandbox keyed "local"."""
    manager = SandboxManager()
    stub = _StubSandbox()
    manager.register("local", stub)
    server = _start(manager)
    try:
        yield server, stub
    finally:
        server.stop()


@pytest.fixture
def create_server():
    """Started server whose manager records created options (no RPC)."""
    manager = _RecordingManager()
    server = _start(manager)
    try:
        yield server, manager
    finally:
        server.stop()


def _request(server, path, *, method="POST", body=None, headers=None):
    """Send a request; return (status, parsed JSON body, response headers).

    HTTPError responses are surfaced like successes so tests can assert on
    the error envelopes directly. A HEAD response parses to ``None``.
    """
    host, port = server.address
    data = body if isinstance(body, bytes) else (
        json.dumps(body).encode("utf-8") if body is not None else None)
    request_headers = {"Content-Type": "application/json"} if data is not None else {}
    request_headers.update(headers or {})
    request = urllib.request.Request(
        f"http://{host}:{port}{path}", data=data, method=method,
        headers=request_headers)
    try:
        with urllib.request.urlopen(request) as response:
            status, raw, response_headers = (
                response.status, response.read(), dict(response.headers))
    except urllib.error.HTTPError as caught:
        status, raw, response_headers = (
            caught.code, caught.read(), dict(caught.headers))
    return status, (json.loads(raw) if raw else None), response_headers


def _invoke(server, instance_id, action, args=None, **kwargs):
    """POST /direct/{environmentId}/invoke with a well-formed action payload."""
    payload = {"action": action, "args": args or {}}
    return _request(server, f"/direct/{instance_id}/invoke",
                    body=payload, **kwargs)


def _decode_data(envelope):
    """Management-plane Envelope → the JSON business payload in ``data``."""
    return json.loads(base64.b64decode(envelope["data"]).decode("utf-8"))


def _assert_error_envelope(status, body, http_code, symbol, retry, outcome):
    assert status == http_code
    assert body["code"] == http_code
    assert body["data"] is None
    error = body["error"]
    assert error["code"] == symbol
    assert error["retry"] == retry
    assert error["outcome"] == outcome
    assert error["requestId"]


def _await_log(caplog, needle, timeout=5.0):
    """The exit log line lands after the response is sent; poll briefly."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and needle not in caplog.text:
        time.sleep(0.05)
    assert needle in caplog.text


# ── 管理面 create:Envelope、字段校验、L-2 接通 ────────────────────────────


def test_create_invoke_delete_roundtrip(create_server):
    server, manager = create_server
    status, body, headers = _request(
        server, MGMT, body={"image": "ubuntu:22.04"},
        headers={"X-Request-Id": "req-create-013"})
    assert status == 200
    assert headers["X-Request-Id"] == "req-create-013"
    assert (body["code"], body["message"]) == (200, "ok")
    assert _decode_data(body) == {"environmentId": "sbx-created-1",
                                  "status": "running",
                                  "requestId": "req-create-013"}
    assert isinstance(manager.created[0], SandboxCreateOptions)
    assert manager.created[0].image == "ubuntu:22.04"
    # 契约 idleTimeoutSeconds 默认 0(关闭空闲回收)。
    assert manager.created[0].idle_timeout == 0

    # 数据面在同一实例上可用;delete 走管理面 Envelope。
    status, body, _ = _invoke(server, "sbx-created-1", "process.exec",
                              {"cmd": "echo hi"})
    assert (status, body["stdout"], body["exit_code"]) == (200, "", 0)
    assert manager.stub.calls[-1]["command"] == "echo hi"

    status, body, _ = _request(server, f"{MGMT}/sbx-created-1", method="DELETE")
    assert status == 200
    assert _decode_data(body) == {"success": True}
    assert manager.stub.terminated == 1


def test_create_generates_and_echoes_request_id(create_server):
    server, _manager = create_server
    status, body, headers = _request(server, MGMT, body={})
    assert status == 200
    echoed = headers["X-Request-Id"]
    uuid.UUID(echoed)  # 缺省 request id 必须是可解析的 UUID
    assert _decode_data(body)["requestId"] == echoed


@pytest.mark.parametrize("override, message", [
    ({"name": 5}, "name must be a string"),
    ({"image": 5}, "image must be a string"),
    ({"cpu": "2000"}, "cpu must be an integer"),
    ({"cpu": True}, "cpu must be an integer"),
    ({"memory": 1.5}, "memory must be an integer"),
    ({"env": ["K"]}, "env must be an object"),
    ({"env": {"K": 1}}, "env must be an object containing string values"),
    ({"ports": "8080"}, "ports must be an array of strings"),
    ({"ports": [8080]}, "ports must be an array of strings"),
    ({"idleTimeoutSeconds": "600"}, "idleTimeoutSeconds must be an integer"),
    ({"idleTimeoutSeconds": -1}, "idleTimeoutSeconds must be non-negative"),
    # idle_timeout 为旧内部名兼容别名,同样参与校验。
    ({"idle_timeout": -1}, "idleTimeoutSeconds must be non-negative"),
    ({"rootfs": "raw", "image": "ubuntu:22.04"}, "mutually exclusive"),
    ({"rootfs": 5}, "rootfs must be a string or an object"),
    ({"mounts": "x"}, "mounts must be an array of objects"),
    ({"mounts": [{"host_dir": "/h"}]}, "mounts require rootfs or image"),
    # 字符串 rootfs 原样直传,mounts 无处并入——拒绝而非静默丢弃。
    ({"rootfs": "raw", "mounts": [{"host_dir": "/h"}]},
     "mounts require an object rootfs or image"),
    ({"extra_config": "x"}, "extra_config must be an object"),
    ({"extra_config": {"upstream": 5}}, "extra_config.upstream must be a string"),
    ({"extra_config": {"sandbox_type": "docker"}}, "sandbox_type moved to runtime"),
    ({"runtime": 5}, "runtime must be a string"),
    ({"tunnel": "x"}, "tunnel must be an object"),
    ({"tunnel": {"enabled": "x"}}, "tunnel.enabled must be a boolean"),
    ({"tunnel": {"proxyPort": "x"}}, "tunnel.proxyPort must be an integer"),
    ({"tunnel": {"proxyPort": 70000}}, "within [1, 65535]"),
    ({"tunnel": {"enabled": True}}, "requires extra_config.upstream"),
])
def test_create_rejects_invalid_contract_fields(create_server, override, message):
    server, _manager = create_server
    # 基座保持空 payload:mounts/rootfs 的互斥与依赖校验须在无 image 干扰下触发。
    payload = dict(override)
    status, body, _headers = _request(
        server, MGMT, body=payload, headers={"X-Request-Id": "req-400"})
    _assert_error_envelope(status, body, 400, "invalid", "never", "not_started")
    assert message in body["message"]
    assert body["error"]["requestId"] == "req-400"


def test_create_rejects_non_json_bodies(create_server):
    server, _manager = create_server
    status, body, _ = _request(server, MGMT, body=b"not-json")
    _assert_error_envelope(status, body, 400, "invalid", "never", "not_started")
    assert "valid JSON" in body["message"]

    status, body, _ = _request(server, MGMT, body=b"{}",
                               headers={"Content-Type": "text/plain"})
    assert (status, body["message"]) == (
        400, "Content-Type must be application/json")

    status, body, _ = _request(server, MGMT, body=b"[1, 2]")
    assert (status, body["message"]) == (
        400, "request body must be a JSON object")


@pytest.mark.parametrize("field", ["sandbox_type", "upstream", "working_dir",
                                   "user", "trace_id"])
def test_create_rejects_moved_top_level_fields(create_server, field):
    server, _manager = create_server
    status, body, _ = _request(server, MGMT, body={field: "x"})
    assert status == 400
    if field == "trace_id":
        target = "X-Trace-ID header"
    elif field == "sandbox_type":
        target = "runtime"
    else:
        target = "extra_config"
    assert body["message"] == f"{field} moved to {target}"


def test_create_maps_runtime_and_extra_config_private_keys(create_server):
    server, manager = create_server
    status, body, _ = _request(
        server, MGMT,
        body={
            "name": "sbx-name",
            "image": "ubuntu:22.04",
            "runtime": "docker",
            "cpu": 2000,
            "memory": 4096,
            "env": {"K": "V"},
            "ports": ["8080"],
            "idleTimeoutSeconds": 0,
            "extra_config": {
                "upstream": "upstream.example.com:8000",
                "working_dir": "/ws",
                "user": "1000:1000",
                "custom.key": "kept",
            },
        })
    assert status == 200
    options = manager.created[0]
    assert options.name == "sbx-name"
    assert options.image == "ubuntu:22.04"
    assert options.cpu == 2000
    assert options.memory == 4096
    assert options.env == {"K": "V"}
    assert options.ports == ["8080"]
    assert options.idle_timeout == 0
    # runtime 直通底层执行器选择;extra_config 三键提取映射底层字段,
    # 剩余键整体透传。
    assert options.sandbox_type == "docker"
    assert options.upstream == "upstream.example.com:8000"
    assert options.working_dir == "/ws"
    assert options.user == "1000:1000"
    assert options.extra_config == {"custom.key": "kept"}


def test_create_translates_rootfs_and_mounts(create_server):
    server, manager = create_server
    # 字符串 rootfs 直传底层。
    _request(server, MGMT, body={"rootfs": '{"type":"overlay"}'})
    assert manager.created[0].rootfs == '{"type":"overlay"}'
    assert manager.created[0].image is None

    # 对象 rootfs 由适配层组装为 JSON:子键保留、mounts 原样并入。
    _request(server, MGMT, body={
        "rootfs": {"type": "overlay", "imageurl": "http://images/rc.img"},
        "mounts": [{"host_dir": "/h", "sandbox_dir": "/s"}],
    })
    assert json.loads(manager.created[1].rootfs) == {
        "type": "overlay",
        "imageurl": "http://images/rc.img",
        "mounts": [{"host_dir": "/h", "sandbox_dir": "/s"}],
    }

    # 仅 image + mounts:image 折入 rootfs JSON,不再走底层 image 组装。
    _request(server, MGMT, body={
        "image": "ubuntu:22.04", "mounts": [{"host_dir": "/h"}]})
    assert json.loads(manager.created[2].rootfs) == {
        "type": "image", "imageurl": "ubuntu:22.04",
        "mounts": [{"host_dir": "/h"}]}
    assert manager.created[2].image is None

    # 仅 image:维持底层既有组装路径。
    _request(server, MGMT, body={"image": "ubuntu:22.04"})
    assert manager.created[3].image == "ubuntu:22.04"
    assert manager.created[3].rootfs is None


def test_create_tunnel_wiring(create_server):
    server, manager = create_server
    # enabled=true + upstream:proxyPort 接通,upstream 保留。
    _request(server, MGMT, body={
        "tunnel": {"enabled": True, "proxyPort": 9000},
        "extra_config": {"upstream": "up.example.com:8000"}})
    assert manager.created[0].proxy_port == 9000
    assert manager.created[0].upstream == "up.example.com:8000"

    # enabled=false:显式关断 upstream 隐含的隧道。
    _request(server, MGMT, body={
        "tunnel": {"enabled": False},
        "extra_config": {"upstream": "up.example.com:8000"}})
    assert manager.created[1].upstream is None
    assert manager.created[1].proxy_port == 8766

    # 缺省 tunnel:由 upstream 自然驱动。
    _request(server, MGMT,
             body={"extra_config": {"upstream": "up.example.com:8000"}})
    assert manager.created[2].upstream == "up.example.com:8000"


def test_named_create_pins_namespace_for_invoke_addressing(monkeypatch):
    # 命名实例的后续 invoke 一律按 "{ns}-{name}" 寻址(libruntime 由
    # functionMeta.ns 组合目标 id);create 侧不钉 namespace 时实例按裸名
    # 注册、invoke 却路由到带默认命名空间前缀的 id → instance not found。
    # 回归:命名创建必须显式携带 namespace,匿名创建不碰它。
    from yr.agentexecutor.sandbox import sandbox as sandbox_module

    captured = {}

    class _Method:
        def __init__(self, value):
            self._value = value

        def invoke(self, *args, **kwargs):
            return self._value

    class _FakeInstance:
        def __init__(self, instance_id):
            # 真实 SDK 里 get_name 是 InstanceProxy 上的 MethodProxy
            # 属性(带 .invoke()),不是普通方法。
            self.get_name = _Method(instance_id)

    class _Creator:
        @staticmethod
        def invoke(*args, **kwargs):
            return _FakeInstance("named-sbx-1")

    def _capture_options(cls, opt):
        captured["opt"] = opt
        return _Creator()

    # 裸 SandboxInstance(测试桩的 @yr.instance 为恒等装饰器)没有装饰器
    # 注入的 options 类方法,直接以 raising=False 打上。
    monkeypatch.setattr(sandbox_module.SandboxInstance, "options",
                        classmethod(_capture_options), raising=False)

    sandbox_module.Sandbox(name="yaml-test-sbx")
    opt = captured["opt"]
    assert opt.name == "yaml-test-sbx"
    assert isinstance(opt.namespace, str) and opt.namespace

    # 匿名创建:namespace 保持缺省,不引入命名寻址。
    sandbox_module.Sandbox()
    assert captured["opt"].namespace is None


def test_create_snapshotid_restores_via_checkpoint(create_server, monkeypatch):
    server, manager = create_server
    restored = []

    class _FakeRestoredSandbox:
        def __init__(self, checkpoint_id=None, trace_id=""):
            restored.append((checkpoint_id, trace_id))

        @staticmethod
        def get_instance_id():
            return "restored-1"

    monkeypatch.setattr(sandbox_http, "Sandbox", _FakeRestoredSandbox)
    status, body, _ = _request(
        server, MGMT,
        body={"snapshotId": "snap-013", "image": "ubuntu:22.04"},
        headers={"X-Trace-ID": "t-snap"},
    )
    assert status == 200
    # 分流到 Sandbox(checkpoint_id=...) + register,不走 create,其余参数忽略。
    assert restored == [("snap-013", "t-snap")]
    assert manager.created == []
    assert manager.registered[0][0] == "restored-1"
    assert _decode_data(body)["environmentId"] == "restored-1"


def test_create_internal_error_maps_to_500():
    manager = _RecordingManager(
        create_error=RuntimeError("create sandbox failed: backend down"))
    server = _start(manager)
    try:
        status, body, _ = _request(server, MGMT, body={})
        _assert_error_envelope(status, body, 500, "internal", "after_backoff",
                               "unknown")
        assert "backend down" in body["message"]
    finally:
        server.stop()


def test_create_reads_trace_header_for_options_and_agent_log(create_server,
                                                             caplog):
    caplog.set_level(logging.INFO)
    server, manager = create_server
    status, body, _ = _request(server, MGMT, body={"name": "n"},
                               headers={"X-Trace-ID": "t-create-9"})
    assert status == 200
    assert manager.created[0].trace_id == "t-create-9"
    # X-Trace-ID 复用于 agent 进出日志(POST 沙箱路由的 op 取路径最后一段)。
    assert "[agent.environments.enter] agentexecutor" in caplog.text
    assert "trace_id=t-create-9" in caplog.text
    _await_log(caplog, "[agent.environments.exit] agentexecutor result=HTTP:200")


def test_create_413_error_envelope():
    manager = _RecordingManager()
    server = _start(manager, max_sandbox_request_size=8)
    try:
        status, body, _ = _request(server, MGMT, body={"image": "ubuntu:22.04"})
        _assert_error_envelope(status, body, 413, "too_large", "never",
                               "not_started")
    finally:
        server.stop()


# ── 管理面 delete 与方法/资源判定 ────────────────────────────────────────


def test_delete_success_envelope_and_unknown_404(sandbox_server):
    server, stub = sandbox_server
    status, body, headers = _request(
        server, f"{MGMT}/local", method="DELETE",
        headers={"X-Request-Id": "req-del-007"})
    assert status == 200
    assert headers["X-Request-Id"] == "req-del-007"
    assert (body["code"], body["message"]) == (200, "ok")
    assert _decode_data(body) == {"success": True}
    assert stub.terminated == 1

    # 再删:not-found 从旧版的 200+success:false 改为 404 + ErrorEnvelope;
    # 错误响应同样回显 X-Request-Id 头(契约 Error response 定义)。
    status, body, headers = _request(
        server, f"{MGMT}/local", method="DELETE",
        headers={"X-Request-Id": "req-del-008"})
    _assert_error_envelope(status, body, 404, "not_found", "never", "terminal")
    assert body["message"] == "sandbox local not found"
    assert body["error"]["requestId"] == "req-del-008"
    assert headers["X-Request-Id"] == "req-del-008"
    assert stub.terminated == 1


def test_mgmt_method_not_allowed_and_not_found(sandbox_server):
    server, _stub = sandbox_server
    # 集合端点只注册 POST。
    for method in ("GET", "PUT", "PATCH", "DELETE"):
        status, body, headers = _request(server, MGMT, method=method)
        _assert_error_envelope(status, body, 405, "method_not_allowed",
                               "never", "not_started")
        assert headers["Allow"] == "POST"

    # 实例端点只注册 DELETE;实例存在才 405,不存在优先 404。
    for method in ("POST", "GET", "PUT"):
        status, body, headers = _request(server, f"{MGMT}/local",
                                         method=method)
        _assert_error_envelope(status, body, 405, "method_not_allowed",
                               "never", "not_started")
        assert headers["Allow"] == "DELETE"

    status, body, _ = _request(server, f"{MGMT}/nope", method="POST", body={})
    _assert_error_envelope(status, body, 404, "not_found", "never", "terminal")
    assert body["message"] == "sandbox nope not found"

    # 未知子资源与尾斜杠均按资源不存在处理。
    status, body, _ = _request(server, f"{MGMT}/local/bogus", method="DELETE")
    _assert_error_envelope(status, body, 404, "not_found", "never", "terminal")
    status, body, _ = _request(server, f"{MGMT}/local/", method="DELETE")
    _assert_error_envelope(status, body, 404, "not_found", "never", "terminal")


def _recv_complete_response(sock) -> bytes:
    """Read one full HTTP response: headers plus the Content-Length body.

    头与体是两次写出、可能分两个 TCP 段到达;不按 Content-Length 读全,
    迟到的 body 段会被误判为下一条响应。EOF 提前出现时返回已读部分。
    """
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            return data
        data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    length = 0
    for line in head.split(b"\r\n"):
        if line.lower().startswith(b"content-length:"):
            length = int(line.split(b":", 1)[1].strip())
    while len(body) < length:
        chunk = sock.recv(4096)
        if not chunk:
            break
        body += chunk
    return head + b"\r\n\r\n" + body


def test_mgmt_405_drops_keepalive_after_unread_put_body(create_server):
    # 405 分支不读请求体:带 body 的 PUT 被拒后必须断开 keep-alive——
    # 否则服务端会把残留 body 当作同连接的下一请求继续解析(串包)。
    # 服务端带未读数据关连接时 TCP 发 RST,405 可能被复位吞掉:
    # 复位/EOF/发送失败都视为"已断开",判据是连接不可复用。
    server, _manager = create_server
    host, port = server.address
    body = b'{"trace_id":"t-1"}'
    with socket.create_connection((host, port), timeout=10) as sock:
        sock.sendall(
            b"PUT " + MGMT.encode() + b" HTTP/1.1\r\n"
            b"Host: localhost\r\nContent-Type: application/json\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
        try:
            response = _recv_complete_response(sock)
        except (ConnectionResetError, ConnectionAbortedError):
            return  # 连接已带未读数据被拆除,断开即正确表现
        assert b" 405 " in response.split(b"\r\n", 1)[0], response
        try:
            sock.sendall(
                b"GET " + MGMT.encode() + b" HTTP/1.1\r\nHost: localhost\r\n\r\n")
        except OSError:
            return  # 对端已关,发送侧直接报错——同样是断开的正确表现
        try:
            trailing = sock.recv(4096)
        except (ConnectionResetError, ConnectionAbortedError):
            trailing = b""
        assert trailing == b"", trailing


# ── 数据面 invoke:路由、6 个 action、错误模型 ────────────────────────────


def test_invoke_path_shapes_and_method(sandbox_server):
    server, _stub = sandbox_server
    payload = {"action": "process.exec", "args": {"cmd": "x"}}
    # 路径形状不符(缺段/多段/非 invoke 尾段)一律 404。
    for path in ("/direct", "/direct/local", "/direct/local/exec",
                 "/direct/local/invoke/extra", "/direct/local/invoke/"):
        status, body, _ = _request(server, path, body=payload)
        assert status == 404
        assert "not found" in body["error"]

    status, body, headers = _request(server, "/direct/local/invoke",
                                     method="HEAD")
    assert status == 405
    assert headers["Allow"] == "POST"
    for method in ("GET", "PUT", "PATCH"):
        status, body, _ = _request(server, "/direct/local/invoke",
                                   method=method)
        assert (status, body["error"]) == (405, "method not allowed")


def test_legacy_prefix_is_not_registered(sandbox_server):
    server, _stub = sandbox_server
    # 旧前缀(未注册)对任何方法一律 404,不带 Allow 头。
    cases = (
        ("POST", "/v1/sandbox/sandboxes"),
        ("GET", "/v1/sandbox/sandboxes/local/files/read?path=/tmp/x"),
        ("PATCH", "/v1/sandbox/sandboxes/local/execute"),
        ("DELETE", "/v1/sandbox/sandboxes/local"),
        ("PUT", "/v1/sandbox/sandboxes/local/files/write?path=/tmp/x"),
    )
    for method, path in cases:
        status, body, headers = _request(
            server, path, method=method,
            body=b"{}" if method in ("POST", "PATCH", "PUT") else None)
        assert status == 404
        assert body["message"] == "endpoint not found"
        assert "Allow" not in headers


def test_invoke_exec_dispatch_and_aliases(sandbox_server):
    server, stub = sandbox_server
    stub.exec_reply = {"returncode": 0, "stdout": "out", "stderr": "err"}
    status, body, _ = _invoke(server, "local", "process.exec",
                              {"cmd": "ls -la", "cwd": "/ws",
                               "envs": {"A": "B"}, "timeout": 5},
                              headers={"X-Trace-ID": "t-1"})
    assert status == 200
    assert body == {"stdout": "out", "stderr": "err", "exit_code": 0}
    call = stub.calls[-1]
    assert call["method"] == "exec"
    assert call["command"] == "ls -la"
    assert call["working_dir"] == "/ws"
    assert call["env"] == {"A": "B"}
    assert call["timeout"] == 5
    assert call["trace_id"] == "t-1"

    # 并列字段:command/working_dir/env + process.run 同义;timeout 缺省不限。
    status, body, _ = _invoke(server, "local", "process.run",
                              {"command": "pwd", "working_dir": "/tmp",
                               "env": {"C": "D"}})
    assert (status, body["exit_code"]) == (200, 0)
    first, second = stub.calls[-2], stub.calls[-1]
    assert second["command"] == "pwd"
    assert second["working_dir"] == "/tmp"
    assert second["env"] == {"C": "D"}
    assert second["timeout"] is None

    # cmd 数组形态(契约外扩展):不经 shell 直接执行,原样透传。
    status, body, _ = _invoke(server, "local", "process.exec",
                              {"cmd": ["ls", "-la"]})
    assert (status, body["exit_code"]) == (200, 0)
    assert stub.calls[-1]["command"] == ["ls", "-la"]


@pytest.mark.parametrize("args, message", [
    ({}, "cmd (or command) is required"),
    ({"cmd": ""}, "cmd (or command) is required"),
    ({"cmd": 5}, "cmd must be a string or an array of strings"),
    ({"cmd": {"a": 1}}, "cmd must be a string or an array of strings"),
    ({"cmd": "x", "cwd": 5}, "cwd must be a string"),
    ({"cmd": "x", "envs": {"A": 1}},
     "envs must be an object containing string values"),
    ({"cmd": "x", "timeout": "5"}, "timeout must be a number"),
    ({"cmd": "x", "timeout": -1}, "timeout must be non-negative"),
])
def test_invoke_exec_rejects_invalid_args(sandbox_server, args, message):
    server, _stub = sandbox_server
    status, body, _ = _invoke(server, "local", "process.exec", args)
    assert status == 400
    assert message in body["error"]


def test_invoke_exec_accepts_zero_timeout(sandbox_server):
    server, stub = sandbox_server
    # 契约 timeout ≥0:0 合法(旧接口要求 >0),且透传到底层调用。
    status, body, _ = _invoke(server, "local", "process.exec",
                              {"cmd": "x", "timeout": 0})
    assert status == 200
    assert stub.calls[-1]["timeout"] == 0


def test_invoke_file_read_write_text_and_hex_roundtrip(sandbox_server,
                                                       tmp_path):
    server, stub = sandbox_server
    text_target = tmp_path / "note.txt"
    status, body, _ = _invoke(server, "local", "file.write",
                              {"path": text_target.as_posix(),
                               "data": "héllo"})
    assert status == 200
    assert body == {"name": "note.txt", "path": text_target.as_posix(),
                    "type": "file", "size": len("héllo".encode("utf-8")),
                    "error": None}
    assert text_target.read_text(encoding="utf-8") == "héllo"

    status, body, _ = _invoke(server, "local", "file.read",
                              {"path": text_target.as_posix()})
    assert (status, body) == (200, {"data": "héllo", "error": None})

    # 二进制:契约编码为 hex(非旧接口的 base64)。
    blob = bytes(range(256))
    bin_target = tmp_path / "blob.bin"
    status, body, _ = _invoke(server, "local", "file.write",
                              {"path": bin_target.as_posix(),
                               "data": blob.hex(), "binary": True})
    assert (status, body["size"], body["error"]) == (200, len(blob), None)
    assert bin_target.read_bytes() == blob

    status, body, _ = _invoke(server, "local", "file.read",
                              {"path": bin_target.as_posix(), "binary": True})
    assert (status, body) == (200, {"data": blob.hex(), "error": None})
    assert stub.calls[-1]["mode"] == "rb"


def test_invoke_file_read_failure_rides_200(sandbox_server, tmp_path):
    server, stub = sandbox_server
    # action 级失败走 HTTP 200、错误在 body(旧接口为 500)。
    stub.fail["read_file"] = FileNotFoundError("No such file or directory")
    status, body, _ = _invoke(server, "local", "file.read",
                              {"path": (tmp_path / "missing.txt").as_posix()})
    assert status == 200
    assert body == {"data": None,
                    "error": "read file failed: No such file or directory"}

    status, body, _ = _invoke(server, "local", "file.read",
                              {"path": "/tmp/x", "binary": "yes"})
    assert (status, body["error"]) == (400, "binary must be a boolean")


def test_invoke_file_write_validation(sandbox_server, tmp_path):
    server, _stub = sandbox_server
    target = (tmp_path / "w.txt").as_posix()
    status, body, _ = _invoke(server, "local", "file.write",
                              {"path": target, "data": "zz", "binary": True})
    assert status == 400
    assert "invalid hex data" in body["error"]

    status, body, _ = _invoke(server, "local", "file.write",
                              {"path": target, "binary": True})
    assert (status, body["error"]) == (400, "data is required")
    status, body, _ = _invoke(server, "local", "file.write",
                              {"path": target, "data": "x", "binary": "yes"})
    assert (status, body["error"]) == (400, "binary must be a boolean")
    status, body, _ = _invoke(server, "local", "file.write",
                              {"path": target, "data": "x", "append": "yes"})
    assert (status, body["error"]) == (400, "append must be a boolean")


def test_invoke_file_list_entry_conversion(sandbox_server, tmp_path):
    server, stub = sandbox_server
    (tmp_path / "sub").mkdir()
    source = tmp_path / "a.py"
    source.write_bytes(b"x" * 10)
    root = tmp_path.as_posix()

    status, body, _ = _invoke(server, "local", "file.list", {"path": root})
    assert status == 200
    entries = {entry["name"]: entry for entry in body["entries"]}
    # 条目对齐 execd:type 类别化、permissions 恒 null(py 底层无权限数据)、
    # modified_time 为 unix 秒。
    assert entries["sub"]["type"] == "dir"
    assert entries["a.py"]["type"] == "file"
    assert entries["a.py"]["size"] == 10
    assert entries["a.py"]["permissions"] is None
    assert entries["a.py"]["modified_time"] == pytest.approx(
        source.stat().st_mtime, abs=1.0)
    assert body["error"] is None

    # depth → 底层 recursive/max_depth 映射:recursive=depth>1、
    # max_depth=depth-1(底层自根 0 层计数、current_depth>max_depth 才停)。
    for depth, (recursive, max_depth) in (
            (2, (True, 1)), (1, (False, 0)), (64, (True, 63))):
        _invoke(server, "local", "file.list", {"path": root, "depth": depth})
        call = stub.calls[-1]
        assert call["recursive"] is recursive
        assert call["max_depth"] == max_depth

    status, body, _ = _invoke(server, "local", "file.list",
                              {"path": root, "depth": "x"})
    assert status == 400


def test_invoke_file_list_depth_boundaries(sandbox_server, tmp_path):
    # 契约 depth=N 返回相对深度 <= N 的条目:a(1)/a/b(2)/a/b/deep.txt(3)。
    server, _stub = sandbox_server
    (tmp_path / "a" / "b").mkdir(parents=True)
    (tmp_path / "a" / "b" / "deep.txt").write_text("x")
    (tmp_path / "top.txt").write_text("y")
    root = tmp_path.as_posix()

    def names_at(depth):
        status, body, _ = _invoke(server, "local", "file.list",
                                  {"path": root, "depth": depth})
        assert status == 200
        return {entry["name"] for entry in body["entries"]}

    assert names_at(1) == {"a", "top.txt"}
    assert names_at(2) == {"a", "b", "top.txt"}
    assert names_at(3) == {"a", "b", "deep.txt", "top.txt"}
    assert names_at(64) == {"a", "b", "deep.txt", "top.txt"}


def test_invoke_file_search_contract_semantics(sandbox_server, tmp_path):
    server, _stub = sandbox_server
    deep = tmp_path / "deep" / "deeper"
    deep.mkdir(parents=True)
    (deep / "a.log").write_text("x")
    (tmp_path / "b.log").write_text("yy")
    (deep / "tmp.log").write_text("z")
    (tmp_path / "c.txt").write_text("w")
    root = tmp_path.as_posix()

    # pattern 可选默认 '*':不传即返回全部文件,按 path 排序。
    status, body, _ = _invoke(server, "local", "file.search", {"path": root})
    assert status == 200
    assert body["error"] is None
    assert [e["name"] for e in body["entries"]] == [
        "b.log", "c.txt", "a.log", "tmp.log"]
    # scanned_entries 为遍历计数:4 个文件 + deep/deeper 2 个目录 = 6。
    assert body["scanned_entries"] == 6

    # 文件名 glob + exclude 只过滤;depth 按相对路径层数收口。
    status, body, _ = _invoke(server, "local", "file.search",
                              {"path": root, "pattern": "*.log",
                               "exclude_patterns": ["tmp*"]})
    assert status == 200
    entries = {entry["name"]: entry for entry in body["entries"]}
    assert set(entries) == {"a.log", "b.log"}
    assert all(entry["type"] == "file" for entry in entries.values())
    # 匹配数为 2,扫描计数仍为 6——scanned_entries 与匹配数无关。
    assert len(body["entries"]) == 2
    assert body["scanned_entries"] == 6

    status, body, _ = _invoke(server, "local", "file.search",
                              {"path": root, "pattern": "*.log", "depth": 1})
    assert status == 200
    assert [e["name"] for e in body["entries"]] == ["b.log"]
    # 深度剪枝:仅扫描第一层(b.log、c.txt 两个文件 + deep 一个目录)。
    assert body["scanned_entries"] == 3

    # 完整路径 glob('*' 跨分隔符):含 / 的 pattern 按整路径匹配。
    status, body, _ = _invoke(server, "local", "file.search",
                              {"path": root, "pattern": "*/deep/*"})
    assert status == 200
    assert [e["name"] for e in body["entries"]] == ["a.log", "tmp.log"]

    # 大小写敏感(契约语义,适配层 fnmatchcase 跨平台一致)。
    (tmp_path / "UPPER.LOG").write_text("u")
    status, body, _ = _invoke(server, "local", "file.search",
                              {"path": root, "pattern": "*.log"})
    names = [e["name"] for e in body["entries"]]
    assert "UPPER.LOG" not in names
    assert "a.log" in names

    # 契约语义:不跟随子目录符号链接(symlink 目录计入扫描,不递归进入)。
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "s.log").write_text("s")
    try:
        (tmp_path / "sub_link").symlink_to(
            tmp_path / "sub", target_is_directory=True)
    except OSError:
        pass  # 宿主不允许建 symlink:跳过该断言组,不影响后续用例
    else:
        status, body, _ = _invoke(server, "local", "file.search",
                                  {"path": root, "pattern": "*.log"})
        names = [e["name"] for e in body["entries"]]
        assert names.count("s.log") == 1  # 真实目录递归可见,经 symlink 不重复
        # 6 文件 + 4 目录(deep/deeper/sub/sub_link)= 10;若跟随 symlink 会是 11。
        assert body["scanned_entries"] == 10

    # max_results 超限:HTTP 429 + 失败形态,无部分结果。
    status, body, _ = _invoke(server, "local", "file.search",
                              {"path": root, "max_results": 2})
    assert (status, body) == (
        429, {"entries": [], "error_code": "RESOURCE_EXHAUSTED",
              "error": "file search max_results exceeded; narrow the search"})

    # max_entries/timeout_ms 经校验接受(类型错误仍 400)。
    status, body, _ = _invoke(server, "local", "file.search",
                              {"path": root, "timeout_ms": 5000,
                               "max_entries": 100})
    assert status == 200
    status, body, _ = _invoke(server, "local", "file.search",
                              {"path": root, "timeout_ms": "x"})
    assert (status, body["error"]) == (400, "timeout_ms must be an integer")

    status, body, _ = _invoke(server, "local", "file.search",
                              {"path": root, "pattern": "*.log", "depth": "x"})
    assert status == 400


def test_invoke_action_failures_ride_http_200(sandbox_server, tmp_path):
    server, stub = sandbox_server
    # 各 action 的 RPC 失败一律 HTTP 200 + body error(契约数据面错误模型)。
    stub.fail["read_file"] = FileNotFoundError("No such file or directory")
    status, body, _ = _invoke(server, "local", "file.read",
                              {"path": (tmp_path / "missing.txt").as_posix()})
    assert status == 200
    assert body["data"] is None
    assert body["error"] == "read file failed: No such file or directory"

    stub.fail["write_file"] = RuntimeError("disk full")
    status, body, _ = _invoke(server, "local", "file.write",
                              {"path": (tmp_path / "w.txt").as_posix(),
                               "data": "x"})
    assert status == 200
    assert (body["name"], body["type"], body["size"]) == ("w.txt", "file", 0)
    assert body["error"] == "write file failed: disk full"

    stub.fail["list_files"] = RuntimeError("boom")
    status, body, _ = _invoke(server, "local", "file.list",
                              {"path": tmp_path.as_posix()})
    assert (status, body) == (200, {"entries": [],
                                    "error": "list files failed: boom"})

    stub.fail["search_files"] = RuntimeError("boom")
    status, body, _ = _invoke(server, "local", "file.search",
                              {"path": tmp_path.as_posix()})
    assert (status, body) == (200, {"entries": [],
                                    "error_code": "FILESYSTEM_ERROR",
                                    "error": "search files failed: boom"})

    stub.fail["exec"] = RuntimeError("rpc down")
    status, body, _ = _invoke(server, "local", "process.exec", {"cmd": "x"})
    assert (status, body) == (200, {"stdout": "",
                                    "stderr": "execute failed: rpc down",
                                    "exit_code": -1})


def test_invoke_rejects_unsupported_action_and_bad_args(sandbox_server):
    server, _stub = sandbox_server
    # 契约枚举外的 action 一律 400(含本服务不实现的 read_chunk/write_chunk)。
    for action in ("file.stat", "process.start", "shell.create",
                   "entrypoint.poll", "file.read_chunk", "file.write_chunk",
                   "bogus.action"):
        status, body, _ = _invoke(server, "local", action, {"path": "/x"})
        assert status == 400
        assert body["error"] == f"unsupported action: {action}"

    status, body, _ = _request(server, "/direct/local/invoke", body={"args": {}})
    assert (status, body["error"]) == (400, "action is required")
    status, body, _ = _request(server, "/direct/local/invoke",
                               body={"action": "file.read", "args": []})
    assert (status, body["error"]) == (400, "args must be an object")
    status, body, _ = _invoke(server, "local", "file.read", {})
    assert (status, body["error"]) == (400, "path is required")
    status, body, _ = _request(server, "/direct/local/invoke",
                               body={"action": "process.exec",
                                     "args": {"cmd": "x"}, "trace_id": "t"})
    assert (status, body["error"]) == (400, "trace_id moved to X-Trace-ID header")


def test_invoke_unknown_instance_returns_404(sandbox_server):
    server, _stub = sandbox_server
    status, body, _ = _invoke(server, "missing", "process.exec", {"cmd": "x"})
    assert (status, body["error"]) == (404, "sandbox missing not found")


def test_invoke_content_type_and_json_validation(sandbox_server):
    server, _stub = sandbox_server
    status, body, _ = _request(server, "/direct/local/invoke",
                               body=b"not-json")
    assert (status, body["error"]) == (400, "request body must be valid JSON")
    status, body, _ = _request(server, "/direct/local/invoke", body=b"{}",
                               headers={"Content-Type": "text/plain"})
    assert (status, body["error"]) == (
        400, "Content-Type must be application/json")
    status, body, _ = _request(server, "/direct/local/invoke", body=b"[1, 2]")
    assert (status, body["error"]) == (400, "request body must be a JSON object")


def test_invoke_request_id_priority_and_invoke_log(sandbox_server, caplog):
    caplog.set_level(logging.INFO)
    server, _stub = sandbox_server
    # requestId 优先级:头(X-ADX-Request-ID) > body > 服务端生成。
    status, body, headers = _request(
        server, "/direct/local/invoke",
        body={"action": "process.exec", "args": {"cmd": "x"},
              "requestId": "body-1"},
        headers={"X-ADX-Request-ID": "hdr-1", "X-Trace-ID": "t-9"})
    assert status == 200
    assert "request_id=hdr-1" in caplog.text
    assert "instance=local action=process.exec" in caplog.text
    assert "trace_id=t-9" in caplog.text

    caplog.clear()
    status, body, _ = _request(
        server, "/direct/local/invoke",
        body={"action": "process.exec", "args": {"cmd": "x"},
              "requestId": "body-2"})
    assert status == 200
    assert "request_id=body-2" in caplog.text


def test_invoke_logs_agent_trace_pairs(sandbox_server, caplog):
    caplog.set_level(logging.INFO)
    server, _stub = sandbox_server
    status, _body, _ = _invoke(server, "local", "process.exec",
                               {"cmd": "x"}, headers={"X-Trace-ID": "t-inv"})
    assert status == 200
    # X-Trace-ID 复用于 agent 进出日志(invoke 路由的 op 取路径最后一段)。
    assert "[agent.invoke.enter] agentexecutor" in caplog.text
    assert "trace_id=t-inv" in caplog.text
    _await_log(caplog, "[agent.invoke.exit] agentexecutor result=HTTP:200")


def test_invoke_request_and_response_size_limits(tmp_path):
    manager = SandboxManager()
    manager.register("local", _StubSandbox())
    target = tmp_path / "large.txt"
    target.write_text("response larger than limit")

    server = _start(manager, max_sandbox_request_size=16)
    try:
        status, body, _ = _invoke(server, "local", "process.exec",
                                  {"cmd": "x"})
        assert status == 413
        assert "request body exceeds max 16" in body["error"]
    finally:
        server.stop()

    server = _start(manager, max_sandbox_response_size=16)
    try:
        status, body, _ = _invoke(server, "local", "file.read",
                                  {"path": target.as_posix()})
        assert status == 413
        assert "response body exceeds max 16" in body["error"]
    finally:
        server.stop()


def test_invoke_limits_independent_of_frontend_file_limit(tmp_path):
    manager = SandboxManager()
    manager.register("local", _StubSandbox())
    # 沙箱 API 的尺寸上限独立于前端文件 API 的 max_file_size:
    # 超过文件上限的 invoke 写入仍须成功。
    server = _start(manager, max_file_size=8)
    try:
        target = tmp_path / "payload.txt"
        status, body, _ = _invoke(server, "local", "file.write",
                                  {"path": target.as_posix(),
                                   "data": "more than eight bytes"})
        assert status == 200
        assert body["error"] is None
        assert target.read_text() == "more than eight bytes"
    finally:
        server.stop()
