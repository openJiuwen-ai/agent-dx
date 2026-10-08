"""Tests for process-local SandboxClient HTTP connection pooling."""

import json
import os
import shutil
import ssl
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from adx_sandbox._http_pool import _SHARED_HTTP_CLIENT_REGISTRY
from adx_sandbox._transport import SandboxClient
from adx_sandbox.types import ConnectionConfig


class _EchoHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        with self.server.connection_lock:
            self.server.client_ports.add(self.client_address[1])
        body = json.dumps(
            {
                "token": self.headers.get("X-Auth"),
                "authorization": self.headers.get("Authorization"),
                "cookie": self.headers.get("Cookie"),
                "client_port": self.client_address[1],
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Set-Cookie", "sandbox-session=must-not-persist")
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self) -> None:
        self.do_GET()

    def log_message(self, _format: str, *_args) -> None:
        pass


class _EchoServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, *, tls_context=None, handler=_EchoHandler):
        super().__init__(("127.0.0.1", 0), handler)
        self.client_ports = set()
        self.connection_lock = threading.Lock()
        if tls_context is not None:
            self.socket = tls_context.wrap_socket(self.socket, server_side=True)


@pytest.fixture(autouse=True)
def _clean_shared_registry(monkeypatch):
    _SHARED_HTTP_CLIENT_REGISTRY.close_all()
    monkeypatch.setenv("ADX_HTTP_MAX_CONNECTIONS", "64")
    monkeypatch.setenv("ADX_HTTP_MAX_KEEPALIVE_CONNECTIONS", "32")
    monkeypatch.setenv("ADX_HTTP_KEEPALIVE_EXPIRY", "30")
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    yield
    _SHARED_HTTP_CLIENT_REGISTRY.close_all()


def _start_server(*, tls_context=None):
    server = _EchoServer(tls_context=tls_context)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _stop_server(server, thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def _new_client(monkeypatch, server: str, token: str, *, tls: bool = False):
    monkeypatch.setenv("ADX_TLS", "1" if tls else "0")
    return SandboxClient(server=server, token=token, verify_tls=False)


def test_clients_share_pool_and_close_only_releases_reference(monkeypatch):
    first = _new_client(monkeypatch, "pool.example:443", "token-a")
    second = _new_client(monkeypatch, "pool.example:443", "token-b")

    snapshot = _SHARED_HTTP_CLIENT_REGISTRY.snapshot()
    key = (os.getpid(), "http", "pool.example:443", False)
    shared_client, references = snapshot[key]
    assert references == 2

    first.close()
    assert _SHARED_HTTP_CLIENT_REGISTRY.snapshot()[key] == (shared_client, 1)
    assert not shared_client.is_closed

    second.close()
    assert _SHARED_HTTP_CLIENT_REGISTRY.snapshot()[key] == (shared_client, 0)
    assert not shared_client.is_closed

    _SHARED_HTTP_CLIENT_REGISTRY.close_all()
    assert shared_client.is_closed


def test_tokens_and_cookies_are_request_scoped(monkeypatch):
    server, thread = _start_server()
    address = f"127.0.0.1:{server.server_port}"
    first = _new_client(monkeypatch, address, "token-a")
    second = _new_client(monkeypatch, address, "token-b")
    url = f"http://{address}/echo"
    try:
        first_response = first._http.get(url, timeout=5).json()
        second_response = second._http.get(url, timeout=5).json()
        repeated_response = first._http.get(url, timeout=5).json()

        assert first_response["token"] == "token-a"
        assert second_response["token"] == "token-b"
        assert repeated_response["token"] == "token-a"
        assert first_response["cookie"] is None
        assert second_response["cookie"] is None
        assert repeated_response["cookie"] is None

        first.close()
        assert second._http.get(url, timeout=5).json()["token"] == "token-b"
    finally:
        first.close()
        second.close()
        _stop_server(server, thread)


def test_put_uses_shared_client_and_request_token(monkeypatch):
    server, thread = _start_server()
    address = f"127.0.0.1:{server.server_port}"
    client = _new_client(monkeypatch, address, "put-token")
    url = f"http://{address}/network"
    try:
        response = client._http.put(url, timeout=5).json()

        assert response["token"] == "put-token"
        assert response["cookie"] is None
        snapshot = _SHARED_HTTP_CLIENT_REGISTRY.snapshot()
        key = (os.getpid(), "http", address, False)
        assert snapshot[key][1] == 1
    finally:
        client.close()
        _stop_server(server, thread)


def test_direct_preserves_legacy_auth_alongside_bearer_auth(monkeypatch):
    server, thread = _start_server()
    address = f"127.0.0.1:{server.server_port}"
    client = _new_client(monkeypatch, address, "sandbox-token")
    try:
        control = client._http.get(f"http://{address}/api/v1/sandboxes", timeout=5).json()
        direct = client._http.get(f"http://{address}/direct/instance-id/8080", timeout=5).json()

        assert control["token"] == "sandbox-token"
        assert control["authorization"] is None
        assert direct["token"] == "sandbox-token"
        assert direct["authorization"] == "Bearer sandbox-token"
    finally:
        client.close()
        _stop_server(server, thread)


def test_token_provider_is_resolved_for_every_http_request(monkeypatch):
    server, thread = _start_server()
    address = f"127.0.0.1:{server.server_port}"
    current = {"token": "token-a"}
    connection = ConnectionConfig(
        server_address=address,
        token="initial-token",
        use_tls=False,
        token_provider=lambda: current["token"],
    )
    client = SandboxClient(connection=connection)
    try:
        first = client._http.get(f"http://{address}/echo", timeout=5).json()
        current["token"] = "token-b"
        second = client._http.get(
            f"http://{address}/direct/sandbox/8080", timeout=5
        ).json()

        assert first["token"] == "token-a"
        assert second["token"] == "token-b"
        assert second["authorization"] == "Bearer token-b"
    finally:
        client.close()
        _stop_server(server, thread)


def test_concurrent_clients_share_pool_without_token_leakage(monkeypatch):
    server, thread = _start_server()
    address = f"127.0.0.1:{server.server_port}"
    url = f"http://{address}/echo"
    clients = [
        _new_client(monkeypatch, address, f"token-{index}") for index in range(24)
    ]
    try:

        def request(index: int):
            return clients[index]._http.get(url, timeout=5).json()

        with ThreadPoolExecutor(max_workers=12) as executor:
            responses = list(executor.map(request, range(len(clients))))

        assert [response["token"] for response in responses] == [
            f"token-{index}" for index in range(len(clients))
        ]
        assert all(response["cookie"] is None for response in responses)
    finally:
        for client in clients:
            client.close()
        _stop_server(server, thread)


def test_server_scheme_and_tls_verification_isolate_pools(monkeypatch):
    http_client = _new_client(monkeypatch, "same.example:443", "token", tls=False)
    https_client = _new_client(monkeypatch, "same.example:443", "token", tls=True)
    other_server = _new_client(monkeypatch, "other.example:443", "token", tls=False)
    monkeypatch.setenv("ADX_TLS", "0")
    verified = SandboxClient(server="same.example:443", token="token", verify_tls=True)
    try:
        keys = set(_SHARED_HTTP_CLIENT_REGISTRY.snapshot())
        assert keys == {
            (os.getpid(), "http", "same.example:443", False),
            (os.getpid(), "https", "same.example:443", False),
            (os.getpid(), "http", "other.example:443", False),
            (os.getpid(), "http", "same.example:443", True),
        }
    finally:
        http_client.close()
        https_client.close()
        other_server.close()
        verified.close()


@pytest.mark.parametrize("tls", [False, True])
def test_real_http_service_reuses_connections(monkeypatch, tmp_path, tls):
    tls_context = None
    if tls:
        openssl = shutil.which("openssl")
        if openssl is None:
            pytest.skip("openssl is required for the local HTTPS fixture")
        cert = tmp_path / "cert.pem"
        key = tmp_path / "key.pem"
        subprocess.run(
            [
                openssl,
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(key),
                "-out",
                str(cert),
                "-subj",
                "/CN=127.0.0.1",
                "-days",
                "1",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_context.load_cert_chain(cert, key)

    server, thread = _start_server(tls_context=tls_context)
    address = f"127.0.0.1:{server.server_port}"
    scheme = "https" if tls else "http"
    url = f"{scheme}://{address}/echo"
    clients = [
        _new_client(monkeypatch, address, f"token-{index}", tls=tls)
        for index in range(4)
    ]
    try:
        responses = [
            clients[index % len(clients)]._http.get(url, timeout=5).json()
            for index in range(20)
        ]
        assert len(responses) == 20
        assert 0 < len(server.client_ports) < len(responses)
    finally:
        for client in clients:
            client.close()
        _stop_server(server, thread)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded, use of fork.*:DeprecationWarning"
)
def test_fork_replaces_inherited_pool_and_socket(monkeypatch):
    server, thread = _start_server()
    address = f"127.0.0.1:{server.server_port}"
    client = _new_client(monkeypatch, address, "fork-token")
    url = f"http://{address}/echo"
    parent_response = client._http.get(url, timeout=5).json()
    read_fd, write_fd = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        try:
            os.close(read_fd)
            child_response = client._http.get(url, timeout=5).json()
            child_keys = [
                list(key) for key in _SHARED_HTTP_CLIENT_REGISTRY.snapshot()
            ]
            payload = json.dumps(
                {"response": child_response, "keys": child_keys}
            ).encode()
            os.write(write_fd, payload)
        finally:
            os.close(write_fd)
            os._exit(0)

    os.close(write_fd)
    try:
        payload = os.read(read_fd, 65536)
        _, status = os.waitpid(child_pid, 0)
        assert status == 0
        child = json.loads(payload)
        assert child["response"]["token"] == "fork-token"
        assert child["response"]["client_port"] != parent_response["client_port"]
        assert child["keys"][0][0] == child_pid
    finally:
        os.close(read_fd)
        client.close()
        _stop_server(server, thread)


@pytest.mark.parametrize("auth_header", ["X-Auth", "Authorization"])
def test_direct_requests_work_with_legacy_and_ingress_auth_servers(monkeypatch, auth_header):
    class AuthHandler(_EchoHandler):
        def do_GET(self):
            expected = "sandbox-token" if auth_header == "X-Auth" else "Bearer sandbox-token"
            if self.headers.get(auth_header) != expected:
                self.send_response(401)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            super().do_GET()

    server = _EchoServer(handler=AuthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    address = f"127.0.0.1:{server.server_port}"
    client = _new_client(monkeypatch, address, "sandbox-token")
    url = f"http://{address}/direct/sandbox/50090"
    stale = {"X-Auth": "stale", "Authorization": "Bearer stale"}
    try:
        response = client._http.get(url, headers=stale, timeout=5)
        assert response.status_code == 200
        with client._http.stream("GET", url, headers=stale, timeout=5) as response:
            assert response.status_code == 200
            response.read()
    finally:
        client.close()
        _stop_server(server, thread)
