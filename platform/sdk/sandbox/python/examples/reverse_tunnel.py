"""Reverse Tunnel Example

Create a sandbox that can access a service running on your local machine
via a reverse tunnel.

Architecture:
  [Local Machine]
    Local HTTP Server: 127.0.0.1:8000
         ^ HTTP
    TunnelClient (background thread)
         | WSS via Traefik gateway
         v
  [Cloud Sandbox]
    Port A (:8765) - WebSocket tunnel endpoint (Traefik registered)
    Port B (:8766) - HTTP proxy for sandbox code (127.0.0.1 only)

    sandbox code: curl http://127.0.0.1:8766/... -> reaches local server

Prerequisites:
  - ADX_SERVER_ADDRESS and ADX_TOKEN environment variables must be set.
  - Set TUNNEL_SSL_VERIFY=0 if using a test cluster with self-signed certs.

Usage:
  export ADX_SERVER_ADDRESS=your-server.example.com
  export ADX_TOKEN=your-token
  python reverse_tunnel.py
"""

import http.server
import os
import shlex
import socket
import sys
import tempfile
import threading
import time

# Disable SSL verification for test clusters with self-signed certificates
os.environ.setdefault("TUNNEL_SSL_VERIFY", "0")

from adx_sandbox import Sandbox

LOCAL_PORT = 0  # bind an ephemeral local port to avoid CI port collisions
PROBE_TIMEOUT = 60
PROBE_ATTEMPTS = 5
PROBE_RETRY_DELAY = 2


def start_local_server(port: int):
    """Start a simple HTTP server serving test files.

    Use an in-process server bound to 127.0.0.1 on an ephemeral port.  The
    The readiness check verifies the HTTP health response from this server,
    not just that some process accepted a TCP connection.
    """
    temp_dir = tempfile.mkdtemp(prefix="tunnel_test_")

    # Create a test file
    index = os.path.join(temp_dir, "index.html")
    with open(index, "w") as f:
        f.write(f"<h1>Hello from local machine!</h1>\n<p>{time.ctime()}</p>\n")

    health = os.path.join(temp_dir, "health")
    with open(health, "w") as f:
        f.write("OK")

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=temp_dir, **kwargs)

        def log_message(self, format, *args):  # noqa: A002 - stdlib signature
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), QuietHandler)
    actual_port = server.server_address[1]
    thread = threading.Thread(
        target=server.serve_forever, name="reverse-tunnel-local-http", daemon=True
    )
    thread.start()

    # Wait for our server to bind and serve the file, not merely for any process
    # to accept TCP on the requested port.
    for _ in range(20):
        try:
            with socket.create_connection(
                ("127.0.0.1", actual_port), timeout=0.2
            ) as sock:
                sock.sendall(
                    b"GET /health HTTP/1.1\r\n"
                    b"Host: localhost\r\n"
                    b"Connection: close\r\n\r\n"
                )
                data = sock.recv(1024)
            if b"200" in data and b"OK" in data:
                print(f"[OK] Local HTTP server started on port {actual_port}")
                return server, thread, temp_dir, actual_port
        except OSError:
            time.sleep(0.2)

    server.shutdown()
    server.server_close()
    raise RuntimeError("Failed to start local HTTP server")


def main():
    server, thread, temp_dir, local_port = start_local_server(LOCAL_PORT)
    try:
        with Sandbox(
            cpu=2000,
            memory=4096,
            upstream=f"127.0.0.1:{local_port}",
        ) as sb:
            tunnel_url = sb.get_tunnel_url()
            print(f"Tunnel URL inside sandbox: {tunnel_url}")

            def run_checked(label: str, command: str, expected: str) -> None:
                last_result = None
                for attempt in range(1, PROBE_ATTEMPTS + 1):
                    result = sb.commands.run(command)
                    last_result = result
                    print(f"\n[{label} attempt {attempt}/{PROBE_ATTEMPTS}] exit_code={result.exit_code}:")
                    print(result.stdout)
                    if result.stderr:
                        print(result.stderr, file=sys.stderr)
                    if result.exit_code == 0 and expected in result.stdout:
                        return
                    if attempt < PROBE_ATTEMPTS:
                        # The reverse-tunnel WebSocket can reconnect while the
                        # sandbox-side proxy is already accepting requests. Keep
                        # examples deterministic by bounding curl latency and
                        # retrying brief reconnect windows instead of waiting for
                        # the sandbox command default timeout.
                        time.sleep(PROBE_RETRY_DELAY)
                raise RuntimeError(
                    f"{label} failed: expected {expected!r}, "
                    f"rc={last_result.exit_code}, stdout={last_result.stdout!r}"
                )

            def fetch_command(url: str, expected_status: int = 200) -> str:
                quoted_url = shlex.quote(url)
                return (
                    "set -e; "
                    f"out=$(curl --noproxy '*' -sS -m {PROBE_TIMEOUT} "
                    f"-w '\\nSTATUS:%{{http_code}}' {quoted_url}); "
                    "printf '%s\\n' \"$out\"; "
                    f"printf '%s\\n' \"$out\" | grep -q 'STATUS:{expected_status}'"
                )

            run_checked(
                "Test 1 index.html",
                fetch_command(f"{tunnel_url}/index.html"),
                "Hello from local machine!",
            )
            run_checked(
                "Test 2 health",
                fetch_command(f"{tunnel_url}/health"),
                "OK",
            )
            run_checked("Test 3 404", fetch_command(f"{tunnel_url}/nonexistent", 404), "STATUS:404")

        print("\nSandbox terminated.")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)
        print("[OK] Local server stopped and cleaned up.")


if __name__ == "__main__":
    main()
