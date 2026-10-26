#!/usr/bin/env python3
# coding=UTF-8

import fnmatch
import io
import json
import logging
import os
import stat
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from unittest.mock import Mock

import pytest

from yr.agentexecutor.http_server import ExecutorHTTPServer, _ChunkedReader
from yr.agentexecutor.sandbox_manager import SandboxManager


class _LocalSandbox:
    """Minimal Sandbox backed by the real filesystem and subprocess.

    The executor HTTP server delegates sandbox operations to an injected
    Sandbox instance. After the sandbox moved to a remote RPC package the
    server never constructs one on its own, so tests inject this local stub to
    route file and execute calls against the host without a runtime.
    """

    @staticmethod
    def exec(command, *, working_dir=None, env=None, timeout=None, trace_id=None):
        merged_env = {**os.environ, **(env or {})}
        if isinstance(command, str):
            cmd_args = ["/bin/sh", "-c", command]
        else:
            cmd_args = command
        completed = subprocess.run(
            cmd_args,
            cwd=working_dir,
            env=merged_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        return {
            "returncode": completed.returncode,
            "stdout": completed.stdout.decode("utf-8", "replace"),
            "stderr": completed.stderr.decode("utf-8", "replace"),
        }

    @staticmethod
    def read_file(file_path, mode="rb", trace_id=None):
        with open(file_path, mode) as handle:
            return handle.read()

    @staticmethod
    def write_file(file_path, data, mode="wb", trace_id=None):
        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(file_path, mode) as handle:
            handle.write(data)

    @staticmethod
    def list_files(file_path, *, recursive=False, max_depth=None,
                   include_files=True, include_dirs=True, trace_id=None):
        root = Path(file_path)
        if recursive:
            paths = list(root.rglob("*"))
        else:
            paths = list(root.iterdir()) if root.is_dir() else []
        items = []
        for path in sorted(paths):
            is_directory = path.is_dir()
            if is_directory and not include_dirs:
                continue
            if not is_directory and not include_files:
                continue
            items.append(
                {"name": path.name, "path": str(path), "is_directory": is_directory}
            )
        return {"items": items}

    @staticmethod
    def search_files(file_path, pattern, *, exclude_patterns=None, trace_id=None):
        excludes = exclude_patterns or []
        root = Path(file_path)
        items = []
        for path in sorted(root.rglob("*")):
            if path.is_dir():
                continue
            if not fnmatch.fnmatch(path.name, pattern):
                continue
            if any(fnmatch.fnmatch(path.name, exclude) for exclude in excludes):
                continue
            items.append({"name": path.name, "path": str(path)})
        return {"items": items}


def _server(**overrides):
    """Build an ExecutorHTTPServer with a SandboxManager-backed local sandbox.

    The sandbox is created and owned by AgentExecutorRuntime, which injects a
    SandboxManager into the server. Tests build a manager pre-loaded with a
    _LocalSandbox stub (keyed "local") so sandbox endpoints work against the
    real host without standing up a runtime. Tests that don't need the id
    route the manager's get() to return the stub for any id via overrides.
    """
    manager = SandboxManager()
    manager.register("local", _LocalSandbox())
    kwargs = {"host": "127.0.0.1", "port": 0, "sandbox_manager": manager}
    kwargs.update(overrides)
    return ExecutorHTTPServer(**kwargs)


def _post_json(server, path, payload):
    host, port = server.address
    request = urllib.request.Request(
        f"http://{host}:{port}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        return response.status, json.load(response)


def _get_json(server, path):
    host, port = server.address
    with urllib.request.urlopen(f"http://{host}:{port}{path}") as response:
        return response.status, json.load(response)


def _request_method(server, path, method, data=None):
    """Send a request with an arbitrary method and return raw status/body/headers."""
    host, port = server.address
    request = urllib.request.Request(
        f"http://{host}:{port}{path}", data=data, method=method
    )
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, response.read(), dict(response.headers)
    except urllib.error.HTTPError as caught:
        return caught.code, caught.read(), dict(caught.headers)


def test_patch_non_sandbox_path_returns_404_json():
    server = _server()
    server.start()
    try:
        status, body, headers = _request_method(server, "/healthz", "PATCH")
        assert status == 404
        assert json.loads(body) == {"message": "endpoint not found"}
        assert headers["Content-Type"] == "application/json"
        assert headers.get("Allow") is None
    finally:
        server.stop()


def test_chunked_reader_decodes_forwarded_request_body():
    source = io.BytesIO(b"4\r\ntest\r\n3\r\n123\r\n0\r\nX-Test: done\r\n\r\n")
    reader = _ChunkedReader(source)

    assert reader.read(5) == b"test1"
    assert reader.read() == b"23"
    assert reader.read() == b""


def test_health_and_download(tmp_path):
    target = tmp_path / "file.txt"
    target.write_bytes(b"hello")
    server = _server()
    server.start()
    try:
        host, port = server.address
        with urllib.request.urlopen(f"http://{host}:{port}/healthz") as response:
            assert json.load(response) == {"status": "ready"}
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/files/download?path={target}",
            headers={"Range": "bytes=1-3"},
        )
        with urllib.request.urlopen(request) as response:
            assert response.status == 206
            assert response.read() == b"ell"
    finally:
        server.stop()


def test_empty_file_range_is_not_satisfiable(tmp_path):
    target = tmp_path / "empty.txt"
    target.touch()
    server = _server()
    server.start()
    try:
        host, port = server.address
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/files/download?path={target}",
            headers={"Range": "bytes=0-"},
        )
        with pytest.raises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request)
        assert caught.value.code == 416
        assert caught.value.headers["Content-Range"] == "bytes */0"
    finally:
        server.stop()


def test_download_without_path_is_bad_request():
    server = _server()
    server.start()
    try:
        host, port = server.address
        with pytest.raises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(f"http://{host}:{port}/v1/files/download")
        assert caught.value.code == 400
        assert caught.value.headers.get("Content-Range") is None
    finally:
        server.stop()


def test_existing_file_upload_route_remains_independent(tmp_path):
    server = _server()
    server.start()
    target = tmp_path / "frontend-upload.bin"
    try:
        host, port = server.address
        query = urllib.parse.urlencode({"path": str(target)})
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/files/upload?{query}",
            data=b"frontend payload",
            method="POST",
        )
        with urllib.request.urlopen(request) as response:
            assert json.load(response)["success"] is True
        assert target.read_bytes() == b"frontend payload"
    finally:
        server.stop()


def test_mkdir_route_creates_directory_with_mode(tmp_path):
    server = _server()
    server.start()
    target = tmp_path / "work" / "sub"
    try:
        host, port = server.address
        query = urllib.parse.urlencode(
            {"path": str(target), "mode": "0755", "recursive": "true"}
        )
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/files/mkdir?{query}",
            method="POST",
        )
        with urllib.request.urlopen(request) as response:
            assert response.status == 200
            body = json.load(response)
        assert body["success"] is True
        assert body["created"] is True
        assert target.is_dir()
        assert stat.S_IMODE(target.stat().st_mode) == 0o755

        with urllib.request.urlopen(request) as response:
            assert json.load(response)["created"] is False
    finally:
        server.stop()


def test_mkdir_route_rejects_missing_path():
    server = _server()
    server.start()
    try:
        host, port = server.address
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/files/mkdir",
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request)
        assert caught.value.code == 400
    finally:
        server.stop()


def test_mkdir_route_rejects_non_recursive_with_missing_parent(tmp_path):
    server = _server()
    server.start()
    target = tmp_path / "missing" / "deep"
    try:
        host, port = server.address
        query = urllib.parse.urlencode({"path": str(target), "recursive": "false"})
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/files/mkdir?{query}",
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request)
        assert caught.value.code == 400
    finally:
        server.stop()


def test_mkdir_route_rejects_non_bool_recursive(tmp_path):
    # gitcode openeuler/yuanrong#404: recursive="aaa" 必须被 400 拒绝,
    # 而不是静默降级为 False 后在父目录存在时意外建目录成功。
    server = _server()
    server.start()
    target = tmp_path / "should-not-exist"
    try:
        host, port = server.address
        query = urllib.parse.urlencode({"path": str(target), "recursive": "aaa"})
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/files/mkdir?{query}",
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request)
        assert caught.value.code == 400
        assert b"must be a boolean" in caught.value.read()
        assert not target.exists()
    finally:
        server.stop()


def test_mkdir_route_accepts_case_insensitive_bool_recursive(tmp_path):
    server = _server()
    server.start()
    try:
        host, port = server.address
        for value in ("True", "FALSE"):
            target = tmp_path / f"case-{value}"
            query = urllib.parse.urlencode({"path": str(target), "recursive": value})
            request = urllib.request.Request(
                f"http://{host}:{port}/v1/files/mkdir?{query}",
                method="POST",
            )
            with urllib.request.urlopen(request) as response:
                assert response.status == 200
            assert target.is_dir()
    finally:
        server.stop()


def test_files_list_route_rejects_non_bool_recursive(tmp_path):
    server = _server()
    server.start()
    try:
        host, port = server.address
        # 公开 /v1/files/list 端点只接受 recursive/max_depth。
        query = urllib.parse.urlencode({"path": str(tmp_path), "recursive": "aaa"})
        with pytest.raises(urllib.error.HTTPError) as caught:
            _get_json(server, f"/v1/files/list?{query}")
        assert caught.value.code == 400
        assert b"must be a boolean" in caught.value.read()
        # 沙箱侧的参数校验由 test_sandbox_yaml_api 的 invoke 用例覆盖。
    finally:
        server.stop()


def test_server_rejects_requests_above_concurrency_limit():
    started = threading.Event()
    release = threading.Event()

    class BlockingSandbox:
        @staticmethod
        def exec(*_args, **_kwargs):
            started.set()
            release.wait(timeout=5)
            return {"returncode": 0, "stdout": "", "stderr": ""}

    blocking_manager = SandboxManager()
    blocking_manager.register("local", BlockingSandbox())
    server = _server(sandbox_manager=blocking_manager, max_concurrent_requests=1)
    server.start()
    first_result = []

    def first_request():
        first_result.append(_post_json(
            server, "/direct/local/invoke",
            {"action": "process.exec", "args": {"cmd": "true"}}))

    thread = threading.Thread(target=first_request)
    thread.start()
    try:
        assert started.wait(timeout=2)
        host, port = server.address
        with pytest.raises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(f"http://{host}:{port}/healthz")
        assert caught.value.code == 503
        assert json.load(caught.value) == {"message": "executor is busy"}
    finally:
        release.set()
        thread.join(timeout=2)
        server.stop()

    # invoke 数据面响应形态:exit_code(非旧接口的 returncode)。
    assert first_result == [(200, {"stdout": "", "stderr": "", "exit_code": 0})]


def test_server_does_not_own_sandbox_lifecycle():
    """After the sandbox moved to a remote RPC package the HTTP server only
    holds the injected SandboxManager reference; AgentExecutorRuntime owns the
    lifecycle. stop() must not touch the managed sandboxes.
    """
    sandbox = Mock(spec=_LocalSandbox)
    manager = SandboxManager()
    manager.register("local", sandbox)
    server = _server(sandbox_manager=manager)

    server.start()
    server.stop()

    # The server never constructs a sandbox itself and never terminates one on
    # stop; the runtime that injected the manager owns cleanup.
    assert sandbox.method_calls == []


def test_healthz_emits_no_agent_trace(caplog):
    caplog.set_level(logging.INFO)
    server = _server()
    server.start()
    try:
        host, port = server.address
        with urllib.request.urlopen(f"http://{host}:{port}/healthz") as response:
            assert response.status == 200
        assert "agent." not in caplog.text
    finally:
        server.stop()


def test_file_download_logs_agent_trace(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    target = tmp_path / "file.txt"
    target.write_bytes(b"hello")
    server = _server()
    server.start()
    try:
        host, port = server.address
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/files/download?path={target}",
            headers={"X-Trace-ID": "t-003"},
        )
        with urllib.request.urlopen(request) as response:
            assert response.status == 200
        assert "[agent.download.enter] agentexecutor" in caplog.text
        # The exit line is logged by the server thread after the response is
        # fully sent, so poll briefly instead of asserting immediately.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and (
            "[agent.download.exit] agentexecutor result=HTTP:200" not in caplog.text
        ):
            time.sleep(0.05)
        assert "[agent.download.exit] agentexecutor result=HTTP:200" in caplog.text
    finally:
        server.stop()


def _post_exec(server, payload):
    """POST /v1/exec and return (status, body) handling error responses."""
    host, port = server.address
    request = urllib.request.Request(
        f"http://{host}:{port}/v1/exec",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as caught:
        return caught.code, json.load(caught)


def test_exec_command_returns_result():
    server = _server()
    server.start()
    try:
        status, body = _post_exec(server, {"command": "echo hello"})
        assert status == 200
        assert body["returncode"] == 0
        assert body["stdout"] == "hello\n"
        assert body["stderr"] == ""
    finally:
        server.stop()


def test_exec_rejects_missing_command():
    server = _server()
    server.start()
    try:
        status, body = _post_exec(server, {})
        assert status == 400
        assert body["message"] == "command is required"
    finally:
        server.stop()


def test_exec_accepts_non_loopback():
    # /v1/exec is served through the frontend tunnel and must not be
    # restricted to loopback clients, unlike the sandbox endpoints.
    server = _server(host="0.0.0.0")
    server.start()
    try:
        host, port = server.address
        # Connect via a non-loopback address when available.
        target_host = host
        try:
            import socket

            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
                probe.connect(("8.8.8.8", 80))
                target_host = probe.getsockname()[0]
        except OSError:
            target_host = "127.0.0.1"
        request = urllib.request.Request(
            f"http://{target_host}:{port}/v1/exec",
            data=json.dumps({"command": ["true"]}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request) as response:
            assert response.status == 200
    finally:
        server.stop()


def test_exec_supports_command_list():
    server = _server()
    server.start()
    try:
        status, body = _post_exec(server, {"command": ["/bin/sh", "-c", "echo ok"]})
        assert status == 200
        assert body["returncode"] == 0
        assert body["stdout"] == "ok\n"
    finally:
        server.stop()


def test_exec_timeout_kills_process():
    server = _server()
    server.start()
    try:
        status, body = _post_exec(
            server, {"command": "sleep 30", "timeout": 1}
        )
        assert status == 200
        assert body["returncode"] == -1
        assert "timed out" in body["stderr"]
    finally:
        server.stop()


def test_exec_with_working_dir_and_env(tmp_path):
    server = _server()
    server.start()
    try:
        status, body = _post_exec(
            server,
            {
                "command": "echo var=$MY_VAR",
                "working_dir": str(tmp_path),
                "env": {"MY_VAR": "hello"},
            },
        )
        assert status == 200
        assert body["returncode"] == 0
        assert body["stdout"] == "var=hello\n"
    finally:
        server.stop()
