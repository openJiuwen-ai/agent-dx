#!/usr/bin/env python3
# coding=UTF-8

import base64
import fnmatch
import io
import json
import logging
import os
import stat
import subprocess
import sys
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


def _put_raw(server, path, body: bytes):
    host, port = server.address
    request = urllib.request.Request(
        f"http://{host}:{port}{path}",
        data=body,
        method="PUT",
    )
    with urllib.request.urlopen(request) as response:
        return response.status, json.load(response)


def _delete(server, path):
    host, port = server.address
    request = urllib.request.Request(f"http://{host}:{port}{path}", method="DELETE")
    with urllib.request.urlopen(request) as response:
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


def test_patch_sandbox_url_returns_405_json(tmp_path):
    server = _server()
    server.start()
    try:
        status, body, headers = _request_method(
            server,
            "/v1/sandbox/sandboxes/local/files/write?path=/tmp/t1.txt&mode=w",
            "PATCH",
            data=b"hello files",
        )
        assert status == 405
        payload = json.loads(body)
        assert "not allowed for sandbox resource" in payload["message"]
        # Allow is per-resource: files/write only registers PUT.
        assert headers["Allow"] == "PUT"
        assert headers["Content-Type"] == "application/json"
    finally:
        server.stop()


def test_head_sandbox_url_returns_405_json_without_body():
    server = _server()
    server.start()
    try:
        status, body, headers = _request_method(
            server, "/v1/sandbox/sandboxes/local/files/read?path=/tmp/t1.txt", "HEAD"
        )
        assert status == 405
        # urllib suppresses the body of a HEAD response, so the RFC-required
        # message can only be verified through the advertised Content-Length:
        # it must describe a non-empty JSON body that HEAD itself does not send.
        # Allow is per-resource: the files/read URL only registers GET.
        assert headers["Allow"] == "GET"
        assert headers["Content-Type"] == "application/json"
        assert int(headers["Content-Length"]) > 0
        assert body == b""
    finally:
        server.stop()


def test_patch_create_url_returns_405():
    server = _server()
    server.start()
    try:
        status, body, headers = _request_method(
            server, "/v1/sandbox/sandboxes", "PATCH", data=b'{"sandbox_type":"x"}'
        )
        assert status == 405
        assert "not allowed for sandbox resource" in json.loads(body)["message"]
        # Allow is per-resource: the bare create URL only registers POST.
        assert headers["Allow"] == "POST"
    finally:
        server.stop()


def test_wrong_action_on_registered_shape_returns_404_not_405():
    """URL shape itself is unregistered (bogus action) → 404, not 405."""
    server = _server()
    server.start()
    try:
        status, body, _ = _request_method(
            server, "/v1/sandbox/sandboxes/local/files/bogus?path=/tmp/x", "PATCH"
        )
        assert status == 404
        assert "not found" in json.loads(body)["message"]
    finally:
        server.stop()


def test_bogus_subresource_returns_404_not_405():
    server = _server()
    server.start()
    try:
        status, body, _ = _request_method(
            server, "/v1/sandbox/sandboxes/local/bogus/xyz", "PATCH"
        )
        assert status == 404
        assert "not found" in json.loads(body)["message"]
    finally:
        server.stop()


def test_unregistered_method_on_unknown_instance_returns_404():
    """Instance existence beats method registration: unknown id → 404, not 405."""
    server = _server()
    server.start()
    try:
        status, body, _ = _request_method(
            server,
            "/v1/sandbox/sandboxes/00000000-0000-0000-0000-000000000000/files/write?path=/tmp/x",
            "PATCH",
            data=b"x",
        )
        assert status == 404
        assert "sandbox 00000000" in json.loads(body)["message"]
    finally:
        server.stop()


def test_unregistered_method_on_live_instance_returns_405():
    server = _server()
    server.start()
    try:
        status, body, headers = _request_method(
            server,
            "/v1/sandbox/sandboxes/local/files/bogus?path=/tmp/x",
            "GET",
        )
        # GET is a registered method but this URL shape does not exist → 404;
        # contrast with a live sandbox + registered shape + wrong method → 405.
        assert status == 404

        status, body, headers = _request_method(
            server, "/v1/sandbox/sandboxes/local/files/read?path=/tmp/x", "PATCH"
        )
        assert status == 405
        assert "method PATCH not allowed" in json.loads(body)["message"]
        assert headers["Allow"] == "GET"
    finally:
        server.stop()


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


def test_registered_methods_unaffected(tmp_path):
    server = _server()
    server.start()
    target = tmp_path / "regression.txt"
    try:
        status, result = _put_raw(
            server,
            f"/v1/sandbox/sandboxes/local/files/write?path={urllib.parse.quote(str(target))}&mode=w",
            b"regression",
        )
        assert status == 200
        assert result["success"] is True
        status, body = _get_json(server, "/healthz")
        assert (status, body) == (200, {"status": "ready"})
        status, executed = _post_json(
            server,
            "/v1/sandbox/sandboxes/local/execute",
            {"command": [sys.executable, "-c", "pass"]},
        )
        assert status == 200
        assert executed["returncode"] == 0
        # No DELETE here: the _LocalSandbox stub has no terminate(), and the
        # manager re-raises that as 500 — delete is covered by manager tests.
        status, body = _get_json(
            server,
            f"/v1/sandbox/sandboxes/local/files/read?path={urllib.parse.quote(str(target))}&mode=r",
        )
        assert (status, body["content"]) == (200, "regression")
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


def test_sandbox_execute_and_text_file_operations(tmp_path):
    server = _server()
    server.start()
    target = tmp_path / "nested" / "message.txt"
    try:
        status, written = _put_raw(
            server,
            f"/v1/sandbox/sandboxes/local/files/write?path={urllib.parse.quote(str(target))}&mode=w",
            b"hello sandbox",
        )
        assert status == 200
        assert written == {"success": True, "path": str(target)}

        _, read = _get_json(
            server,
            f"/v1/sandbox/sandboxes/local/files/read?path={urllib.parse.quote(str(target))}&mode=r",
        )
        assert read == {
            "path": str(target),
            "mode": "r",
            "content": "hello sandbox",
            "content_encoding": "text",
        }

        _, executed = _post_json(
            server,
            "/v1/sandbox/sandboxes/local/execute",
            {
                "command": [sys.executable, "-c",
                            "import os,sys; sys.stdout.write(os.environ['VALUE']+':'+os.getcwd())"],
                "working_dir": str(tmp_path),
                "env": {"VALUE": "ok"},
            },
        )
        assert executed == {
            "returncode": 0,
            "stdout": f"ok:{tmp_path}",
            "stderr": "",
        }
    finally:
        server.stop()


def test_sandbox_binary_file_list_and_search_operations(tmp_path):
    server = _server()
    server.start()
    target = tmp_path / "nested" / "payload.bin"
    ignored = tmp_path / "nested" / "ignored.bin"
    try:
        for path, content in ((target, b"\x00\xff"), (ignored, b"ignored")):
            _put_raw(
                server,
                f"/v1/sandbox/sandboxes/local/files/write?path={urllib.parse.quote(str(path))}&mode=wb",
                base64.b64encode(content),
            )

        _, read = _get_json(
            server,
            f"/v1/sandbox/sandboxes/local/files/read?path={urllib.parse.quote(str(target))}",
        )
        assert base64.b64decode(read["content"]) == b"\x00\xff"
        assert read["content_encoding"] == "base64"

        _, listed = _get_json(
            server,
            f"/v1/sandbox/sandboxes/local/files/list?path={urllib.parse.quote(str(tmp_path))}&recursive=true",
        )
        assert {item["name"] for item in listed["items"]} == {
            "nested",
            "payload.bin",
            "ignored.bin",
        }

        _, searched = _get_json(
            server,
            f"/v1/sandbox/sandboxes/local/files/search?path={urllib.parse.quote(str(tmp_path))}"
            "&pattern=*.bin&exclude_patterns=ignored.*",
        )
        assert [item["name"] for item in searched["items"]] == ["payload.bin"]
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

        # 沙箱 files_list 路由额外接受 include_files/include_dirs。
        base = f"/v1/sandbox/sandboxes/local/files/list?path={urllib.parse.quote(str(tmp_path))}"
        for param in ("include_files", "include_dirs"):
            with pytest.raises(urllib.error.HTTPError) as caught:
                _get_json(server, f"{base}&{param}=aaa")
            assert caught.value.code == 400
            assert b"must be a boolean" in caught.value.read()

        # 沙箱路由的 recursive 同样严格校验。
        with pytest.raises(urllib.error.HTTPError) as caught:
            _get_json(server, f"{base}&recursive=aaa")
        assert caught.value.code == 400
        assert b"must be a boolean" in caught.value.read()
    finally:
        server.stop()


def test_sandbox_rejects_invalid_json_and_oversized_body():
    server = _server(max_sandbox_request_size=8)
    server.start()
    try:
        host, port = server.address
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/sandbox/sandboxes/local/execute",
            data=b"not-json",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as invalid:
            urllib.request.urlopen(request)
        assert invalid.value.code == 400

        request = urllib.request.Request(
            f"http://{host}:{port}/v1/sandbox/sandboxes/local/execute",
            data=b'{"command":"true"}',
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as oversized:
            urllib.request.urlopen(request)
        assert oversized.value.code == 413
    finally:
        server.stop()


def test_sandbox_size_limits_are_independent_from_frontend_file_limit(tmp_path):
    server = _server(
        max_file_size=8,
        max_sandbox_request_size=1024,
        max_sandbox_response_size=1024,
    )
    server.start()
    target = tmp_path / "sandbox.txt"
    try:
        status, result = _put_raw(
            server,
            f"/v1/sandbox/sandboxes/local/files/write?path={urllib.parse.quote(str(target))}&mode=w",
            b"more than eight bytes",
        )
        assert status == 200
        assert result["success"] is True
        assert target.read_text() == "more than eight bytes"
    finally:
        server.stop()


def test_sandbox_rejects_oversized_json_response(tmp_path):
    target = tmp_path / "large.txt"
    target.write_text("response larger than limit")
    server = _server(max_sandbox_response_size=16)
    server.start()
    try:
        host, port = server.address
        with pytest.raises(urllib.error.HTTPError) as caught:
            _get_json(
                server,
                f"/v1/sandbox/sandboxes/local/files/read?path={urllib.parse.quote(str(target))}&mode=r",
            )
        assert caught.value.code == 413
        assert "response body exceeds max 16" in json.load(caught.value)["message"]
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
        first_result.append(_post_json(server, "/v1/sandbox/sandboxes/local/execute", {"command": "true"}))

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

    assert first_result == [(200, {"returncode": 0, "stdout": "", "stderr": ""})]


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


def test_sandbox_request_logs_agent_trace_pairs(caplog):
    caplog.set_level(logging.INFO)
    server = _server()
    server.start()
    try:
        host, port = server.address
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/sandbox/sandboxes/local/execute",
            data=json.dumps({"command": ["true"]}).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-Trace-ID": "t-002"},
            method="POST",
        )
        with urllib.request.urlopen(request) as response:
            assert response.status == 200
        # POST sandbox 路由的 op 取路径最后一段(execute)
        assert "[agent.execute.enter] agentexecutor" in caplog.text
        assert "trace_id=t-002" in caplog.text
        # The exit line is logged by the server thread after the response is
        # fully sent, so poll briefly instead of asserting immediately.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and (
            "[agent.execute.exit] agentexecutor result=HTTP:200" not in caplog.text
        ):
            time.sleep(0.05)
        assert "[agent.execute.exit] agentexecutor result=HTTP:200" in caplog.text
    finally:
        server.stop()


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
