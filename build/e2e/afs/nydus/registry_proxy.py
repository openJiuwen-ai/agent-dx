#!/usr/bin/env python3
"""Test-only observing/fault proxy in front of a real OCI distribution Registry."""

import http.client
import json
import pathlib
import threading
import time
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

LOG = pathlib.Path("/output/logs/origin.jsonl")
CONTROL = pathlib.Path("/output/origin-control.json")
RELEASE = pathlib.Path("/output/release-origin")
LOCK = threading.Lock()
HOP_HEADERS = {"connection", "keep-alive", "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade"}


class ThreadedServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class RequestHandler(WSGIRequestHandler):
    def get_environ(self):
        environ = super().get_environ()
        environ["afs.raw_target"] = self.path
        environ["afs.request_headers"] = {
            key.lower(): value for key, value in self.headers.items() if key.lower() not in ("host", "connection")
        }
        return environ

    def log_message(self, *_):
        pass


def origin_response(method, path, request_headers, mode):
    if mode == "hold":
        until = time.monotonic() + 90
        while not RELEASE.exists() and time.monotonic() < until:
            time.sleep(0.05)
    blocked_body = mode == "body_503" and method == "GET" and request_headers.get("range") != "bytes=0-0"
    if mode == "503" or blocked_body:
        return 503, "Service Unavailable", {}, b"origin temporarily unavailable"
    conn = http.client.HTTPConnection("10.203.0.10", 5000, timeout=60)
    try:
        conn.request(method, path, headers=request_headers)
        response = conn.getresponse()
        status, reason, headers, body = response.status, response.reason, dict(response.getheaders()), response.read()
    finally:
        conn.close()
    if mode == "short" and body:
        limit = max(1, len(body) // 2)
        body = body[:limit]
    if mode == "corrupt" and body:
        body = bytes([body[0] ^ 1]) + body[1:]
    return status, reason, headers, body


def forward(environ, start_response):
    method, path = environ["REQUEST_METHOD"], environ["afs.raw_target"]
    if method not in ("GET", "HEAD"):
        start_response("501 Not Implemented", [("Content-Length", "0")])
        return []
    request_headers = environ["afs.request_headers"]

    control = json.loads(CONTROL.read_text()) if CONTROL.exists() else {}
    mode_key = "mode" if "/blobs/sha256:" in path else "manifest_mode"
    node = request_headers.get("x-afs-node")
    mode = control.get(mode_key, "ok") if control.get("node") in (None, node) else "ok"
    record = dict(at=time.time(), method=method, path=path, node=node, range=request_headers.get("range"), mode=mode)
    with LOCK, (LOG.parent / "origin-start.jsonl").open("a") as stream:
        stream.write(json.dumps(record) + "\n")
    try:
        if mode == "delay":
            time.sleep(control.get("delay", 3))
        status, reason, headers, body = origin_response(method, path, request_headers, mode)
        record.update(status=status, bytes=len(body))
        skipped_headers = HOP_HEADERS | {"content-length", "server", "date"}
        reply_headers = [(key, value) for key, value in headers.items() if key.lower() not in skipped_headers]
        length = headers.get("Content-Length", str(len(body))) if method == "HEAD" else str(len(body))
        reply_headers.append(("Content-Length", length))
        write = start_response(f"{status} {reason}", reply_headers)
        write(b"" if method == "HEAD" else body)
    except (BrokenPipeError, ConnectionResetError):
        record["client_closed"] = True
    finally:
        with LOCK, LOG.open("a") as stream:
            stream.write(json.dumps(record) + "\n")
    return []


if __name__ == "__main__":
    with make_server("10.203.0.10", 5001, forward, server_class=ThreadedServer, handler_class=RequestHandler) as server:
        server.serve_forever()
