#!/usr/bin/env python3
"""Spanner-style HTTP/1.0 wire checks with real TLS Ingress, Execd and TunnelClient."""

import argparse
import contextlib
import json
import logging
import os
from pathlib import Path
import select
import shutil
import socket
import socketserver
import ssl
import subprocess
import threading
import time

from websockets.datastructures import Headers
from websockets.http11 import Response
from websockets.sync.client import connect
from websockets.sync.server import serve

from adx_environment.tunnel_client import TunnelClient

LOG = logging.getLogger(__name__)


def generate_tls(directory):
    """Generate short-lived CA and server certificates for this local run."""
    openssl = shutil.which("openssl")
    if openssl is None:
        raise FileNotFoundError("openssl is required to generate fixture certificates")
    openssl = str(Path(openssl).resolve())
    directory.mkdir()
    ca_config = directory / "ca.cnf"
    ca_config.write_text(
        "[req]\ndistinguished_name=dn\n[dn]\n[ca]\n"
        "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n"
        "subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n"
    )
    extensions = directory / "server.cnf"
    extensions.write_text(
        "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
        "subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n"
        "extendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n"
    )
    commands = [
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
         "-subj", "/CN=ADX tunnel test CA", "-config", ca_config, "-extensions", "ca",
         "-keyout", directory / "ca.key", "-out", directory / "ca.pem"],
        ["req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
         "-keyout", directory / "ingress.key", "-out", directory / "ingress.csr"],
        ["x509", "-req", "-in", directory / "ingress.csr", "-CA", directory / "ca.pem",
         "-CAkey", directory / "ca.key", "-CAcreateserial", "-days", "2",
         "-extfile", extensions, "-out", directory / "ingress.pem"],
    ]
    for command in commands:
        subprocess.run(
            [openssl, *map(str, command)], check=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    for name in ["ca.key", "ingress.key"]:
        (directory / name).chmod(0o600)


def reserve_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def read_exact(stream, length):
    data = bytearray()
    while len(data) < length:
        part = stream.recv(length - len(data))
        if not part:
            raise EOFError("truncated fixture stream")
        data.extend(part)
    return bytes(data)


def read_head(stream):
    data = bytearray()
    while not data.endswith(b"\r\n\r\n"):
        data.extend(read_exact(stream, 1))
    return bytes(data)


def reply_body(stream, head):
    headers = {}
    for line in head.decode("latin-1").split("\r\n")[1:]:
        if ":" in line:
            name, value = line.split(":", 1)
            headers[name.lower()] = value.strip()
    return read_exact(stream, int(headers["content-length"]))


class LegacyHandler(socketserver.BaseRequestHandler):
    """Change only the client's WebSocket request-line version on the TLS wire."""

    def handle(self):
        with socket.create_connection(("127.0.0.1", self.server.ingress_port), timeout=10) as tcp:
            with self.server.context.wrap_socket(tcp, server_hostname="localhost") as upstream:
                prior_requests = 0
                while True:
                    head = read_head(self.request)
                    if b"upgrade: websocket\r\n" in head.lower():
                        break
                    upstream.sendall(head)
                    response = read_head(upstream)
                    self.request.sendall(response + reply_body(upstream, response))
                    prior_requests += 1
                wire = head.replace(b" HTTP/1.1\r\n", b" HTTP/1.0\r\n", 1)
                assert wire.split(b"\r\n", 1)[0].endswith(b"HTTP/1.0")
                # Force a request-line split across writes on the real TLS path.
                upstream.sendall(wire[:9])
                upstream.sendall(wire[9:])
                response = read_head(upstream)
                self.server.observations.append({
                    "request": wire.split(b"\r\n", 1)[0].decode(),
                    "response": response.split(b"\r\n", 1)[0].decode(),
                    "prior_requests": prior_requests,
                })
                LOG.info("wire handshake: %s", self.server.observations[-1])
                self.request.sendall(response)
                if not response.startswith(b"HTTP/1.1 101 "):
                    body = reply_body(upstream, response)
                    LOG.error("upgrade failed: %s", body.decode("utf-8", errors="replace"))
                    self.request.sendall(body)
                    return
                while True:
                    readable = [upstream] if upstream.pending() else select.select(
                        [self.request, upstream], [], [], 10,
                    )[0]
                    for source in readable:
                        part = source.recv(65536)
                        if not part:
                            return
                        target = upstream if source is self.request else self.request
                        target.sendall(part)


class LegacyProxy(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, ingress_port, context):
        super().__init__(("127.0.0.1", 0), LegacyHandler)
        self.ingress_port = ingress_port
        self.context = context
        self.observations = []


def wait_port(port, process):
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError(f"fixture process exited: {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"fixture port {port} did not become ready")


@contextlib.contextmanager
def runtime(executable, arguments, environment, log_path):
    with log_path.open("wb") as log:
        process = subprocess.Popen([str(executable), *arguments], env=environment, stdout=log, stderr=log)
        try:
            yield process
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def echo_websocket(connection):
    connection.send("tunnel hello")
    for message in connection:
        connection.send(message)


def serve_http(_connection, request):
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None
    body = b"tunnel http hello"
    return Response(200, "OK", Headers([("Content-Length", str(len(body)))]), body)


def check_messages(connection):
    assert connection.response.status_code == 101
    assert connection.recv(timeout=10) == "tunnel hello"
    text = "GET /inside-frame HTTP/1.0\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"
    connection.send(text)
    assert connection.recv(timeout=10) == text
    payload = b"\x00\xff" + bytes(range(256)) * 1024
    connection.send(payload)
    assert connection.recv(timeout=10) == payload
    assert connection.ping(b"spanner-check").wait(timeout=10)


def check_reused_connection(address):
    stream = socket.create_connection(address, timeout=10)
    try:
        stream.sendall(b"GET /app/probe HTTP/1.1\r\nHost: localhost\r\n\r\n")
        response = read_head(stream)
        assert response.startswith(b"HTTP/1.1 200 ")
        assert reply_body(stream, response) == b"tunnel http hello"
        with connect(
            f"ws://127.0.0.1:{address[1]}/app/echo", sock=stream,
            additional_headers={"Content-Length": "0"}, compression=None, close_timeout=2,
        ) as connection:
            check_messages(connection)
    finally:
        stream.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tls", type=Path, help="existing fixture CA and server certificate directory")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.tls is None:
        args.tls = args.output / "tls"
        generate_tls(args.tls)
    execd = Path(os.environ["EXECD_RUNTIME"]).resolve(strict=True)
    ingress = Path(os.environ["INGRESS_RUNTIME"]).resolve(strict=True)
    ws_port, http_port, ingress_port = reserve_port(), reserve_port(), reserve_port()
    config = args.output / "ingress.json"
    config.write_text(json.dumps({
        "listen": f"127.0.0.1:{ingress_port}", "tunnel_ws_port": ws_port,
        "tunnel_http_port": http_port, "tls": str(args.tls.resolve()),
    }))
    environment = {
        **os.environ, "EXECD_TUNNEL_ONLY": "1", "EXECD_TUNNEL_WS_PORT": str(ws_port),
        "EXECD_TUNNEL_HTTP_PORT": str(http_port), "ADX_TUNNEL_PROTOCOL_VERSION": "2",
    }
    context = ssl.create_default_context(cafile=str(args.tls / "ca.pem"))
    with contextlib.ExitStack() as stack:
        server = stack.enter_context(serve(
            echo_websocket, "127.0.0.1", 0, process_request=serve_http, compression=None,
        ))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        stack.callback(server.shutdown)
        execd_process = stack.enter_context(runtime(execd, [], environment, args.output / "execd.log"))
        wait_port(ws_port, execd_process)
        ingress_process = stack.enter_context(runtime(
            ingress, [str(config)], os.environ.copy(), args.output / "ingress.log",
        ))
        wait_port(ingress_port, ingress_process)
        proxy = stack.enter_context(LegacyProxy(ingress_port, context))
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        stack.callback(proxy.shutdown)
        tunnel = TunnelClient(upstream=f"127.0.0.1:{server.socket.getsockname()[1]}")
        stack.callback(tunnel.stop)
        assert tunnel.start(f"ws://127.0.0.1:{proxy.server_address[1]}/tunnel", timeout=10)
        LOG.info("PASS real SDK control tunnel through HTTP/1.0 -> TLS Ingress -> Execd")
        with connect(f"ws://127.0.0.1:{proxy.server_address[1]}/app/echo", compression=None) as connection:
            check_messages(connection)
        LOG.info("PASS new HTTP/1.0 WebSocket: 101, tunnel hello, text, 256 KiB binary, ping/pong")
        check_reused_connection(proxy.server_address)
        LOG.info("PASS reused connection: tunneled HTTP, then HTTP/1.0 WebSocket with Content-Length: 0")
        # Keep the fixture's TCP route on loopback while verifying the TLS
        # certificate against localhost, independently of proxy/DNS settings.
        with socket.create_connection(("127.0.0.1", ingress_port), timeout=10) as tcp:
            with connect(
                f"wss://localhost:{ingress_port}/app/echo", sock=tcp, ssl=context, compression=None,
            ) as connection:
                check_messages(connection)
        LOG.info("PASS HTTP/1.1 WebSocket baseline on the same real tunnel")
        observations = list(proxy.observations)
        assert len(observations) == 3, observations
        for observation in observations:
            assert observation["request"].endswith("HTTP/1.0"), observation
            assert observation["response"].startswith("HTTP/1.1 101 "), observation
        assert observations[-1]["prior_requests"] == 1, observations
        (args.output / "result.json").write_text(json.dumps({
            "passed": True, "handshakes": observations,
            "checks": ["real-sdk-control", "new-websocket", "reused-websocket", "http11-baseline"],
            "binary_bytes": 262146,
        }, indent=2) + "\n")


if __name__ == "__main__":
    main()
