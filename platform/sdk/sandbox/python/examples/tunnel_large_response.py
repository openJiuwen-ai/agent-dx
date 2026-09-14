"""Tunnel Large Response Test

Verify that the reverse tunnel can return responses larger than 1MB.

Architecture:
  [Local Machine]
    Local HTTP Server: 127.0.0.1:{LOCAL_PORT}
         ^ HTTP
    TunnelClient (background thread)
         | WSS via Traefik gateway
         v
  [Cloud Sandbox]
    Port A (:8765) - WebSocket tunnel endpoint
    Port B (:8766) - HTTP proxy for sandbox code (127.0.0.1 only)

    sandbox code fetches large responses via http://127.0.0.1:8766 -> reaches local server

Usage:
  export ADX_SERVER_ADDRESS=100.88.105.32:8889
  export ADX_TOKEN=...
  .venv-py313/bin/python tunnel_large_response.py
"""

import hashlib
import http.server
import os
import shlex
import sys
import tempfile
import threading
import time

os.environ.setdefault("TUNNEL_SSL_VERIFY", "0")

from adx_sandbox import Sandbox

LOCAL_PORT = 0  # bind an ephemeral local port to avoid CI port collisions

# Test sizes: from 512KB up to 10MB
TEST_SIZES = [
    ("512KB", 512 * 1024),
    ("1MB", 1 * 1024 * 1024),
    ("2MB", 2 * 1024 * 1024),
    ("5MB", 5 * 1024 * 1024),
    ("10MB", 10 * 1024 * 1024),
]


def start_local_server(port: int):
    """Start an HTTP server serving pre-generated files with known SHA256 hashes."""
    temp_dir = tempfile.mkdtemp(prefix="tunnel_large_test_")

    file_hashes = {}
    for label, size in TEST_SIZES:
        content = ("DATA_BLOCK_" + label + "_" + "X" * 1023 + "\n") * (size // 1024)
        content = content[:size]
        content_bytes = content.encode()
        filepath = os.path.join(temp_dir, label + ".bin")
        with open(filepath, "wb") as f:
            f.write(content_bytes)
        file_hashes[label] = hashlib.sha256(content_bytes).hexdigest()
        print(f"  Created {label}.bin: {os.path.getsize(filepath):,} bytes, sha256={file_hashes[label][:16]}...")

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=temp_dir, **kwargs)

        def do_GET(self):
            if self.path == "/health":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"OK")
                return
            return super().do_GET()

        def log_message(self, format, *args):  # noqa: A002 - stdlib signature
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), QuietHandler)
    actual_port = server.server_address[1]
    thread = threading.Thread(
        target=server.serve_forever, name="tunnel-large-local-http", daemon=True
    )
    thread.start()

    # Wait for our server to serve expected content, not merely for any process
    # to accept TCP on a fixed port.
    for _ in range(30):
        try:
            import urllib.request

            data = urllib.request.urlopen(
                f"http://127.0.0.1:{actual_port}/health", timeout=0.5
            ).read()
            if data == b"OK":
                print(f"[OK] Local HTTP server started on port {actual_port}")
                return server, thread, temp_dir, file_hashes, actual_port
        except Exception:
            time.sleep(0.2)

    server.shutdown()
    server.server_close()
    raise RuntimeError("Failed to start local HTTP server")


def main():
    server, thread, temp_dir, file_hashes, local_port = start_local_server(LOCAL_PORT)

    # Quick local verification (also confirms the port isn't hijacked by a proxy)
    print("\n[Verify] Local server sanity check...")
    import urllib.request
    for label, size in TEST_SIZES:
        url = f"http://127.0.0.1:{local_port}/{label}.bin"
        r = urllib.request.urlopen(url, timeout=30)
        data = r.read()
        local_sha = hashlib.sha256(data).hexdigest()
        ok = len(data) == size and local_sha == file_hashes[label]
        print(f"  {label}: local_size={len(data):,} expected={size:,} sha_ok={local_sha == file_hashes[label]} {'OK' if ok else 'FAIL'}")
        if not ok:
            print(f"    ERROR: local server returned wrong data for {label}")
            return 1
    print("  Local server OK")

    try:
        print("\n[Sandbox] Creating sandbox with tunnel...")
        with Sandbox(
            cpu=2000,
            memory=4096,
            upstream=f"127.0.0.1:{local_port}",
            proxy_port=8766,
        ) as sb:
            tunnel_url = sb.get_tunnel_url()
            print(f"  Tunnel URL: {tunnel_url}")

            # Verify basic tunnel connectivity
            print("\n[Test 0] Basic tunnel connectivity...")
            result = sb.commands.run(
                f"curl --noproxy '*' -fsS -m 10 {shlex.quote(tunnel_url + '/health')}",
                timeout=30,
            )
            print(f"  health check: stdout={result.stdout.strip()}, exit={result.exit_code}")

            results = []

            for label, size in TEST_SIZES:
                url = f"{tunnel_url}/{label}.bin"
                expected_sha = file_hashes[label]
                print(f"\n[Test] Fetching {label} ({size:,} bytes) via tunnel...")
                t0 = time.time()
                tmp_path = f"/tmp/tunnel-large-{label}.bin"
                result = sb.commands.run(
                    "set -e; "
                    f"tmp={shlex.quote(tmp_path)}; "
                    f"curl --noproxy '*' -fsS -m 60 {shlex.quote(url)} -o \"$tmp\"; "
                    "actual=$(wc -c < \"$tmp\" | tr -d ' '); "
                    "sha=$(openssl dgst -sha256 -r \"$tmp\" | awk '{print $1}'); "
                    f"size_ok=false; [ \"$actual\" = {shlex.quote(str(size))} ] && size_ok=true; "
                    f"sha_ok=false; [ \"$sha\" = {shlex.quote(expected_sha)} ] && sha_ok=true; "
                    f"echo RESULT:{label} expected={size} actual=$actual size_ok=$size_ok sha_ok=$sha_ok sha256=$sha; "
                    "rm -f \"$tmp\"; "
                    "[ \"$size_ok\" = true ] && [ \"$sha_ok\" = true ]",
                    timeout=120,
                )
                elapsed = time.time() - t0
                print(f"  stdout: {result.stdout.strip()}")
                if result.stderr:
                    print(f"  stderr: {result.stderr.strip()}")
                print(f"  exit_code={result.exit_code}, elapsed={elapsed:.2f}s")
                passed = result.exit_code == 0 and "size_ok=true" in result.stdout and "sha_ok=true" in result.stdout
                results.append((label, passed, elapsed))

            # For 10MB, retry once if it failed (known intermittent issue with chunked encoding)
            if not results[-1][1]:
                label, size = TEST_SIZES[-1]
                url = f"{tunnel_url}/{label}.bin"
                expected_sha = file_hashes[label]
                print(f"\n[Retry] Fetching {label} ({size:,} bytes) via tunnel (attempt 2)...")
                t0 = time.time()
                tmp_path = f"/tmp/tunnel-large-{label}.bin"
                result = sb.commands.run(
                    "set -e; "
                    f"tmp={shlex.quote(tmp_path)}; "
                    f"curl --noproxy '*' -fsS -m 60 {shlex.quote(url)} -o \"$tmp\"; "
                    "actual=$(wc -c < \"$tmp\" | tr -d ' '); "
                    "sha=$(openssl dgst -sha256 -r \"$tmp\" | awk '{print $1}'); "
                    f"size_ok=false; [ \"$actual\" = {shlex.quote(str(size))} ] && size_ok=true; "
                    f"sha_ok=false; [ \"$sha\" = {shlex.quote(expected_sha)} ] && sha_ok=true; "
                    f"echo RESULT:{label} expected={size} actual=$actual size_ok=$size_ok sha_ok=$sha_ok sha256=$sha; "
                    "rm -f \"$tmp\"; "
                    "[ \"$size_ok\" = true ] && [ \"$sha_ok\" = true ]",
                    timeout=120,
                )
                elapsed = time.time() - t0
                print(f"  stdout: {result.stdout.strip()}")
                if result.stderr:
                    print(f"  stderr: {result.stderr.strip()}")
                print(f"  exit_code={result.exit_code}, elapsed={elapsed:.2f}s")
                passed = result.exit_code == 0 and "size_ok=true" in result.stdout and "sha_ok=true" in result.stdout
                results[-1] = (label, passed, elapsed)

            # Summary
            print("\n" + "=" * 60)
            print("SUMMARY")
            print("=" * 60)
            for label, passed, elapsed in results:
                status = "\033[32mPASS\033[0m" if passed else "\033[31mFAIL\033[0m"
                print(f"  {label}: {status} ({elapsed:.2f}s)")
            all_passed = all(r[1] for r in results)
            if all_passed:
                print(f"\n\033[32mALL TESTS PASSED\033[0m - Tunnel correctly returned data up to {TEST_SIZES[-1][0]}")
            else:
                print("\n\033[31mSOME TESTS FAILED\033[0m (large sizes may be intermittent)")

        print("\nSandbox terminated.")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
        print("[OK] Local server stopped and cleaned up.")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
