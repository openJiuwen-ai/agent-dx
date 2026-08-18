#!/usr/bin/env python3
# coding=UTF-8

import io
import json
import urllib.error
import urllib.request

import pytest

from yr.agentexecutor.http_server import ExecutorHTTPServer, _ChunkedReader


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
