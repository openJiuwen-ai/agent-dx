#!/usr/bin/env python3
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
"""Real TCP -> Rust RRT -> sandbox-sdk -> HTTP/1 framing checks."""

import gzip
import http.server
import logging
import os
import socket
import subprocess
import threading
import time
from typing import ClassVar

from adx_sandbox.tunnel_client import TunnelClient

LOG = logging.getLogger(__name__)
STARTUP_RETRIES = 50
STARTUP_SLEEP_SECONDS = 0.1


def reserve_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def header_values(headers, name):
    lower = name.lower()
    return [value for key, value in headers if key.lower() == lower]


class InteropHandler(http.server.BaseHTTPRequestHandler):
    """Validate framing for buffered and unknown-length streaming requests."""

    protocol_version = "HTTP/1.1"
    observations: ClassVar[list] = []

    def _record(self):
        lengths = self.headers.get_all("Content-Length", [])
        encodings = self.headers.get_all("Transfer-Encoding", [])
        if encodings:
            assert not lengths, "ambiguous request framing"
            assert encodings == ["chunked"], encodings
            chunks = []
            while True:
                size = int(self.rfile.readline().strip().split(b";", 1)[0], 16)
                assert 0 <= size <= 1024 * 1024, "invalid fixture chunk size"
                if size == 0:
                    while True:
                        trailer = self.rfile.readline()
                        assert trailer, "truncated chunk trailers"
                        if trailer == b"\r\n":
                            break
                    break
                chunk = self.rfile.read(size)
                assert len(chunk) == size
                assert self.rfile.read(2) == b"\r\n"
                chunks.append(chunk)
            body = b"".join(chunks)
        else:
            assert len(lengths) <= 1, lengths
            content_length = int(lengths[0]) if lengths else 0
            body = self.rfile.read(content_length)
            assert len(body) == content_length
        headers = list(self.headers.raw_items())
        type(self).observations.append((self.path, headers, body))
        return body

    def _send(self, status, body=b"", headers=()):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        if status not in (204, 304):
            self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD" and status not in (204, 304):
            self.wfile.write(body)

    def do_GET(self):
        self._record()
        if self.path == "/gzip":
            self._send(
                200,
                gzip.compress(b"gzip-payload"),
                (
                    ("Content-Encoding", "gzip"),
                    ("Set-Cookie", "session=one; Path=/"),
                    ("Set-Cookie", "theme=dark; Path=/"),
                ),
            )
        elif self.path == "/set-cookie":
            self._send(
                200,
                b"cookie-set",
                (("Set-Cookie", "session=must-not-leak; Path=/"),),
            )
        elif self.path == "/inspect-cookie":
            self._send(200, b"cookie-inspected")
        elif self.path == "/no-content":
            self._send(204)
        elif self.path == "/not-modified":
            self.send_response(304)
            self.send_header("Content-Length", "321")
            self.send_header("Connection", "close")
            self.end_headers()
        else:
            self._send(200, f"UPSTREAM-OK:{self.path}".encode())

    def do_POST(self):
        body = self._record()
        self._send(200, b"ECHO:" + body)

    def do_HEAD(self):
        self._record()
        self.send_response(200)
        self.send_header("Content-Length", "123")
        self.send_header("Connection", "close")
        self.end_headers()

    def log_message(self, *_args):
        return


def raw_request(port, request):
    with socket.create_connection(("127.0.0.1", port), timeout=5) as stream:
        stream.settimeout(10)
        stream.sendall(request)
        response = bytearray()
        while True:
            chunk = stream.recv(65536)
            if not chunk:
                break
            response.extend(chunk)
    head, _, body = bytes(response).partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = []
    for line in lines[1:]:
        if ":" in line:
            name, value = line.split(":", 1)
            headers.append((name, value.strip()))
    return status, headers, body


def wait_for_runtime(port):
    for _ in range(STARTUP_RETRIES):
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close()
            return
        except OSError:
            time.sleep(STARTUP_SLEEP_SECONDS)
    raise RuntimeError("rrt-runtime HTTP tunnel port did not become ready")


def check(name, condition, detail, passed):
    if not condition:
        raise AssertionError(f"{name}: {detail}")
    passed.append(name)
    LOG.info("[PASS] %s  %s", name, detail)


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ws_port = reserve_port()
    http_port = reserve_port()
    upstream = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0),
        InteropHandler,
    )
    threading.Thread(target=upstream.serve_forever, daemon=True).start()

    proc = subprocess.Popen(
        [os.environ.get("RRT_RUNTIME", "rrt-runtime")],
        env={
            **os.environ,
            "RRT_TUNNEL_ONLY": "1",
            "RRT_TUNNEL_WS_PORT": str(ws_port),
            "RRT_TUNNEL_HTTP_PORT": str(http_port),
        },
    )
    tunnel_client = None
    passed = []
    try:
        wait_for_runtime(http_port)
        tunnel_client = TunnelClient(upstream=f"127.0.0.1:{upstream.server_port}")
        if not tunnel_client.start(
            f"ws://127.0.0.1:{ws_port}",
            timeout=10,
        ):
            raise RuntimeError("TunnelClient failed to connect to Rust server")

        status, _, body = raw_request(
            http_port,
            b"GET /probe HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
        check(
            "GET via tunnel",
            status == 200 and body == b"UPSTREAM-OK:/probe",
            f"status={status} body={body!r}",
            passed,
        )

        payload = b'{"message":"chunked-through-rrt"}'
        chunked = (
            b"POST /echo HTTP/1.1\r\n"
            b"Host: local\r\n"
            b"Connection: close, X-First-Hop\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Content-Type: application/json\r\n"
            b"X-Dup: first\r\n"
            b"X-Dup: second\r\n"
            b"X-First-Hop: secret\r\n\r\n"
            + f"{len(payload):x}\r\n".encode()
            + payload
            + b"\r\n0\r\n\r\n"
        )
        status, _, body = raw_request(http_port, chunked)
        _, upstream_headers, upstream_body = InteropHandler.observations[-1]
        check(
            "chunked POST decoded",
            status == 200 and body == b"ECHO:" + payload and upstream_body == payload,
            f"status={status} body={body!r}",
            passed,
        )
        check(
            "duplicate request headers",
            header_values(upstream_headers, "X-Dup") == ["first", "second"],
            repr(header_values(upstream_headers, "X-Dup")),
            passed,
        )
        check(
            "unknown-length streaming framing rebuilt",
            header_values(upstream_headers, "Transfer-Encoding") == ["chunked"]
            and not header_values(upstream_headers, "X-First-Hop")
            and not header_values(upstream_headers, "Content-Length"),
            repr(upstream_headers),
            passed,
        )

        # V2 streams unknown-length bodies. A bounded, known-length request
        # still exercises the buffered path and its rebuilt Content-Length.
        status, _, body = raw_request(
            http_port,
            b"POST /buffered HTTP/1.1\r\nHost: local\r\nConnection: close\r\n"
            + f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload,
        )
        _, buffered_headers, buffered_body = InteropHandler.observations[-1]
        check(
            "known-length buffered framing rebuilt",
            status == 200 and body == b"ECHO:" + payload and buffered_body == payload
            and header_values(buffered_headers, "Content-Length") == [str(len(payload))]
            and not header_values(buffered_headers, "Transfer-Encoding"),
            f"status={status} headers={buffered_headers!r} body={body!r}",
            passed,
        )

        status, headers, body = raw_request(
            http_port,
            b"GET /gzip HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
        check(
            "raw gzip response",
            status == 200
            and header_values(headers, "Content-Encoding") == ["gzip"]
            and gzip.decompress(body) == b"gzip-payload",
            f"headers={headers!r} body_prefix={body[:8]!r}",
            passed,
        )
        check(
            "duplicate Set-Cookie",
            header_values(headers, "Set-Cookie")
            == ["session=one; Path=/", "theme=dark; Path=/"],
            repr(header_values(headers, "Set-Cookie")),
            passed,
        )

        raw_request(
            http_port,
            b"GET /set-cookie HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
        raw_request(
            http_port,
            b"GET /inspect-cookie HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
        _, inspect_headers, _ = InteropHandler.observations[-1]
        check(
            "cookie isolation",
            not header_values(inspect_headers, "Cookie"),
            repr(header_values(inspect_headers, "Cookie")),
            passed,
        )

        status, headers, body = raw_request(
            http_port,
            b"HEAD /head HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n",
        )
        check(
            "HEAD representation length",
            status == 200
            and body == b""
            and header_values(headers, "Content-Length") == ["123"],
            f"headers={headers!r} body={body!r}",
            passed,
        )
        for path, expected_status, expected_length in (
            ("/no-content", 204, []),
            ("/not-modified", 304, []),
        ):
            status, headers, body = raw_request(
                http_port,
                (
                    f"GET {path} HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
                ).encode(),
            )
            check(
                f"{expected_status} response semantics",
                status == expected_status
                and body == b""
                and header_values(headers, "Content-Length") == expected_length,
                f"headers={headers!r} body={body!r}",
                passed,
            )
    finally:
        if tunnel_client is not None:
            tunnel_client.stop()
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        upstream.shutdown()
        upstream.server_close()

    LOG.info("INTEROP RESULT pass=%s", len(passed))


if __name__ == "__main__":
    main()
