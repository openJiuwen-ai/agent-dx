#!/usr/bin/env python3
# coding=UTF-8

import base64
import io
import json
import os
import stat
import threading
import urllib.error
import urllib.parse
import urllib.request
from unittest.mock import Mock

import pytest

from yr.agentexecutor.http_server import ExecutorHTTPServer, _ChunkedReader


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


def test_chunked_reader_decodes_forwarded_request_body():
    source = io.BytesIO(b"4\r\ntest\r\n3\r\n123\r\n0\r\nX-Test: done\r\n\r\n")
    reader = _ChunkedReader(source)

    assert reader.read(5) == b"test1"
    assert reader.read() == b"23"
    assert reader.read() == b""


def test_health_and_download(tmp_path):
    target = tmp_path / "file.txt"
    target.write_bytes(b"hello")
    server = ExecutorHTTPServer("127.0.0.1", 0)
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
    server = ExecutorHTTPServer("127.0.0.1", 0)
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
    server = ExecutorHTTPServer("127.0.0.1", 0)
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
    server = ExecutorHTTPServer("127.0.0.1", 0)
    server.start()
    target = tmp_path / "nested" / "message.txt"
    try:
        status, written = _post_json(
            server,
            "/v1/sandbox/write_file",
            {
                "path": str(target),
                "mode": "w",
                "content": "hello sandbox",
                "content_encoding": "text",
            },
        )
        assert status == 200
        assert written == {"success": True, "path": str(target)}

        _, read = _post_json(
            server,
            "/v1/sandbox/read_file",
            {"path": str(target), "mode": "r"},
        )
        assert read == {
            "path": str(target),
            "mode": "r",
            "content": "hello sandbox",
            "content_encoding": "text",
        }

        _, executed = _post_json(
            server,
            "/v1/sandbox/execute",
            {
                "command": ["/bin/sh", "-c", "printf \"$VALUE:$PWD\""],
                "cwd": str(tmp_path),
                "environment": {"VALUE": "ok"},
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
    server = ExecutorHTTPServer("127.0.0.1", 0)
    server.start()
    target = tmp_path / "nested" / "payload.bin"
    ignored = tmp_path / "nested" / "ignored.bin"
    try:
        for path, content in ((target, b"\x00\xff"), (ignored, b"ignored")):
            _post_json(
                server,
                "/v1/sandbox/write_file",
                {
                    "path": str(path),
                    "data": base64.b64encode(content).decode("ascii"),
                    "mode": "wb",
                    "content_encoding": "base64",
                },
            )

        _, read = _post_json(
            server,
            "/v1/sandbox/read_file",
            {"path": str(target)},
        )
        assert base64.b64decode(read["content"]) == b"\x00\xff"
        assert read["content_encoding"] == "base64"

        _, listed = _post_json(
            server,
            "/v1/sandbox/list_files",
            {"path": str(tmp_path), "recursive": True},
        )
        assert {item["name"] for item in listed["items"]} == {
            "nested",
            "payload.bin",
            "ignored.bin",
        }

        _, searched = _post_json(
            server,
            "/v1/sandbox/search_files",
            {
                "path": str(tmp_path),
                "pattern": "*.bin",
                "exclude_patterns": ["ignored.*"],
            },
        )
        assert [item["name"] for item in searched["items"]] == ["payload.bin"]
    finally:
        server.stop()


def test_existing_file_upload_route_remains_independent(tmp_path):
    server = ExecutorHTTPServer("127.0.0.1", 0)
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
    server = ExecutorHTTPServer("127.0.0.1", 0)
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
    server = ExecutorHTTPServer("127.0.0.1", 0)
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
    server = ExecutorHTTPServer("127.0.0.1", 0)
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


def test_sandbox_rejects_invalid_json_and_oversized_body():
    server = ExecutorHTTPServer("127.0.0.1", 0, max_sandbox_request_size=8)
    server.start()
    try:
        host, port = server.address
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/sandbox/execute",
            data=b"not-json",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as invalid:
            urllib.request.urlopen(request)
        assert invalid.value.code == 400

        request = urllib.request.Request(
            f"http://{host}:{port}/v1/sandbox/execute",
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
    server = ExecutorHTTPServer(
        "127.0.0.1",
        0,
        max_file_size=8,
        max_sandbox_request_size=1024,
        max_sandbox_response_size=1024,
    )
    server.start()
    target = tmp_path / "sandbox.txt"
    try:
        status, result = _post_json(
            server,
            "/v1/sandbox/write_file",
            {
                "path": str(target),
                "mode": "w",
                "content": "more than eight bytes",
                "content_encoding": "text",
            },
        )
        assert status == 200
        assert result["success"] is True
        assert target.read_text() == "more than eight bytes"
    finally:
        server.stop()


def test_sandbox_rejects_oversized_json_response(tmp_path):
    target = tmp_path / "large.txt"
    target.write_text("response larger than limit")
    server = ExecutorHTTPServer(
        "127.0.0.1", 0, max_sandbox_response_size=16
    )
    server.start()
    try:
        host, port = server.address
        request = urllib.request.Request(
            f"http://{host}:{port}/v1/sandbox/read_file",
            data=json.dumps({"path": str(target), "mode": "r"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request)
        assert caught.value.code == 413
        assert "response body exceeds max 16" in json.load(caught.value)["message"]
    finally:
        server.stop()


def test_server_rejects_requests_above_concurrency_limit():
    started = threading.Event()
    release = threading.Event()

    class BlockingSandbox:
        @staticmethod
        def execute(*_args, **_kwargs):
            started.set()
            release.wait(timeout=5)
            return {"returncode": 0, "stdout": "", "stderr": ""}

    server = ExecutorHTTPServer(
        "127.0.0.1", 0, sandbox=BlockingSandbox(), max_concurrent_requests=1
    )
    server.start()
    first_result = []

    def first_request():
        first_result.append(_post_json(server, "/v1/sandbox/execute", {"command": "true"}))

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


def test_server_owns_default_sandbox_lifecycle(monkeypatch):
    sandbox = Mock()
    monkeypatch.setattr(
        "yr.agentexecutor.http_server.SandboxInstance", lambda: sandbox
    )
    server = ExecutorHTTPServer("127.0.0.1", 0)

    server.start()
    server.stop()

    sandbox.cleanup.assert_called_once_with()
