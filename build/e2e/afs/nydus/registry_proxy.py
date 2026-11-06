#!/usr/bin/env python3
"""Test-only observing/fault proxy in front of a real OCI distribution Registry."""

import http.client
import json
import pathlib
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = pathlib.Path("/output/logs/origin.jsonl")
CONTROL = pathlib.Path("/output/origin-control.json")
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_GET(self):  # noqa: N802 - required BaseHTTPRequestHandler callback name
        self.forward()

    def do_HEAD(self):  # noqa: N802 - required BaseHTTPRequestHandler callback name
        self.forward()

    def forward(self):
        control = json.loads(CONTROL.read_text()) if CONTROL.exists() else {}
        is_body = "/blobs/sha256:" in self.path
        mode = control.get("mode", "ok") if is_body else control.get("manifest_mode", "ok")
        if control.get("node") not in (None, self.headers.get("x-afs-node")):
            mode = "ok"
        record = dict(
            at=time.time(),
            method=self.command,
            path=self.path,
            node=self.headers.get("x-afs-node"),
            range=self.headers.get("Range"),
            mode=mode,
        )
        with LOCK, (LOG.parent / "origin-start.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        try:
            if mode == "hold":
                until = time.monotonic() + 90
                while not pathlib.Path("/output/release-origin").exists() and time.monotonic() < until:
                    time.sleep(0.05)
            if mode == "delay":
                time.sleep(control.get("delay", 3))
            if mode == "503":
                status, headers, body = 503, {}, b"origin temporarily unavailable"
            else:
                conn = http.client.HTTPConnection("10.203.0.10", 5000, timeout=60)
                headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "connection")}
                conn.request(self.command, self.path, headers=headers)
                response = conn.getresponse()
                status, headers, body = response.status, dict(response.getheaders()), response.read()
                conn.close()
                if mode == "short" and body:
                    body = body[: max(1, len(body) // 2)]
                if mode == "corrupt" and body:
                    body = bytes([body[0] ^ 1]) + body[1:]
            record.update(status=status, bytes=len(body))
            self.send_response(status)
            for key, value in headers.items():
                if key.lower() not in ("content-length", "transfer-encoding", "connection", "server", "date"):
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            record["client_closed"] = True
        finally:
            with LOCK, LOG.open("a") as stream:
                stream.write(json.dumps(record) + "\n")
            self.close_connection = True


ThreadingHTTPServer(("10.203.0.10", 5001), Handler).serve_forever()
