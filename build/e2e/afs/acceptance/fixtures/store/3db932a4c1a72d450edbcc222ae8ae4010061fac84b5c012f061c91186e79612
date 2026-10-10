#!/usr/bin/env python3
"""Bounded TCP/UDP/mTLS environment network probe for AFS ENV-01.

This proves only the standalone network preflight surface from
source/docs/acceptance.md section 3.2. It does not claim product acceptance.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import platform
import secrets
import signal
import socket
import ssl
import struct
import sys
import threading
import time
from pathlib import Path
from typing import Any

MAX_FRAME = 65536
MAX_TIMEOUT = 3.0
TOKEN_BYTES = 32
BACKLOG = 16


class ProbeError(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail or reason


def _now() -> float:
    return time.monotonic()


def _deadline(timeout: float) -> float:
    if timeout <= 0 or timeout > MAX_TIMEOUT:
        raise ProbeError("invalid_timeout", f"timeout must be >0 and <= {MAX_TIMEOUT}s")
    return _now() + timeout


def _remaining(deadline: float) -> float:
    left = deadline - _now()
    if left <= 0:
        raise ProbeError("timeout", "deadline expired")
    return min(left, MAX_TIMEOUT)


def _recv_exact(sock: socket.socket, size: int, deadline: float) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        sock.settimeout(_remaining(deadline))
        try:
            chunk = sock.recv(remaining)
        except TimeoutError as exc:
            raise ProbeError("timeout", "timed out while reading frame") from exc
        if not chunk:
            raise ProbeError("short_read", f"peer closed with {remaining} bytes unread")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock: socket.socket, timeout: float) -> bytes:
    deadline = _deadline(timeout)
    header = _recv_exact(sock, 4, deadline)
    size = struct.unpack("!I", header)[0]
    if size > MAX_FRAME:
        raise ProbeError("frame_too_large", f"frame length {size} exceeds {MAX_FRAME}")
    return _recv_exact(sock, size, deadline)


def send_frame(sock: socket.socket, payload: bytes) -> None:
    if len(payload) > MAX_FRAME:
        raise ProbeError("frame_too_large", f"payload length {len(payload)} exceeds {MAX_FRAME}")
    sock.sendall(struct.pack("!I", len(payload)) + payload)


def probe_stream(sock: socket.socket, token: bytes, timeout: float) -> dict[str, Any]:
    try:
        sock.settimeout(timeout)
        send_frame(sock, token)
        echoed = recv_frame(sock, timeout)
        if echoed != token:
            return {"status": "FAIL", "reason": "mismatch", "received_len": len(echoed)}
        return {"status": "PASS", "bytes": len(token)}
    except ProbeError as exc:
        return {"status": "FAIL", "reason": exc.reason, "detail": exc.detail}
    except OSError as exc:
        return {"status": "FAIL", "reason": "socket_error", "detail": repr(exc)}


def _tls_context_server(ca: Path, cert: Path, key: Path) -> ssl.SSLContext:
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_verify_locations(cafile=str(ca))
    ctx.load_cert_chain(certfile=str(cert), keyfile=str(key))
    return ctx


def _tls_context_client(ca: Path, cert: Path | None, key: Path | None) -> ssl.SSLContext:
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=str(ca))
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if cert and key:
        ctx.load_cert_chain(certfile=str(cert), keyfile=str(key))
    return ctx


def _cert_names(cert: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for item in cert.get("subject", ()):
        for key, value in item:
            if key == "commonName":
                names.add(str(value))
    for key, value in cert.get("subjectAltName", ()):
        if key in {"DNS", "IP Address"}:
            names.add(str(value))
    return names


def _peer_allowed(cert: dict[str, Any] | None, expected: str | None) -> bool:
    return not expected or bool(cert and expected in _cert_names(cert))


def classify_tls_failure(exc: BaseException) -> dict[str, str]:
    text = str(exc).lower()
    detail = {"classification": "not_tls_handshake_rejection", "detail": repr(exc)}
    if isinstance(exc, ssl.CertificateError) or isinstance(exc, ssl.SSLCertVerificationError):
        detail = {
            "classification": "certificate_verify_failed",
            "detail": str(exc),
            "verify_code": str(getattr(exc, "verify_code", "")),
            "verify_message": str(getattr(exc, "verify_message", "")),
        }
        if "ip address mismatch" in text or "hostname" in text:
            detail["classification"] = "wrong_hostname"
            return detail
        if "self-signed" in text or "unable to get" in text or "verify failed" in text:
            detail["classification"] = "untrusted_ca"
            return detail
        return detail
    if isinstance(exc, ssl.SSLError):
        if "certificate required" in text or "certificate_required" in text or "peer did not return a certificate" in text:
            return {"classification": "missing_client_cert", "detail": str(exc)}
        if "unknown ca" in text:
            return {"classification": "untrusted_ca", "detail": str(exc)}
        if "bad certificate" in text or "bad_certificate" in text or "handshake failure" in text or "tlsv1 alert" in text:
            return {"classification": "handshake_rejected", "detail": str(exc)}
        return {"classification": "tls_error", "detail": str(exc)}
    if isinstance(exc, OSError) and getattr(exc, "errno", None) in {errno.ECONNRESET, errno.EPIPE}:
        return {"classification": "transport_reset_without_tls_alert", "detail": str(exc)}
    return detail


def _bind_tcp(bind_ip: str, port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((bind_ip, port))
    sock.listen(BACKLOG)
    sock.settimeout(0.5)
    return sock


def _bind_udp(bind_ip: str, port: int) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((bind_ip, port))
    sock.settimeout(0.5)
    return sock


def _serve_tcp(sock: socket.socket, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            conn, _ = sock.accept()
        except socket.timeout:
            continue
        except OSError:
            return
        with conn:
            conn.settimeout(MAX_TIMEOUT)
            try:
                send_frame(conn, recv_frame(conn, MAX_TIMEOUT))
            except (OSError, ProbeError):
                continue


def _serve_udp(sock: socket.socket, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            payload, addr = sock.recvfrom(MAX_FRAME + 1)
        except socket.timeout:
            continue
        except OSError:
            return
        if len(payload) <= MAX_FRAME:
            try:
                sock.sendto(payload, addr)
            except OSError:
                continue


def _serve_tls(sock: socket.socket, ctx: ssl.SSLContext, stop: threading.Event, expected: str | None) -> None:
    while not stop.is_set():
        try:
            conn, _ = sock.accept()
        except socket.timeout:
            continue
        except OSError:
            return
        with conn:
            conn.settimeout(MAX_TIMEOUT)
            try:
                with ctx.wrap_socket(conn, server_side=True) as tls:
                    if not _peer_allowed(tls.getpeercert(), expected):
                        continue
                    send_frame(tls, recv_frame(tls, MAX_TIMEOUT))
            except (OSError, ssl.SSLError, ProbeError):
                continue


def _write_ready(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _self_start_ticks() -> int | None:
    try:
        stat = Path("/proc/self/stat").read_text(encoding="utf-8")
        return int(stat.rsplit(") ", 1)[1].split()[19])
    except (OSError, ValueError, IndexError):
        return None


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_text_or_none(path: str) -> str | None:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return None


def server_main(args: argparse.Namespace) -> int:
    live_linux_arm64_guard()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    sockets: list[socket.socket] = []
    threads: list[threading.Thread] = []
    ready_written = False
    try:
        tcp = _bind_tcp(args.bind_ip, args.port)
        sockets.append(tcp)
        udp = _bind_udp(args.bind_ip, args.port)
        sockets.append(udp)
        tls = _bind_tcp(args.bind_ip, args.tls_port)
        sockets.append(tls)
        ctx = _tls_context_server(args.ca, args.server_cert, args.server_key)
        threads = [
            threading.Thread(target=_serve_tcp, args=(tcp, stop), daemon=False),
            threading.Thread(target=_serve_udp, args=(udp, stop), daemon=False),
            threading.Thread(target=_serve_tls, args=(tls, ctx, stop, args.peer_expected), daemon=False),
        ]
        for thread in threads:
            thread.start()
        ready = {
            "status": "READY",
            "pid": os.getpid(),
            "start_ticks": _self_start_ticks(),
            "hostname": socket.gethostname(),
            "boot_id": _read_text_or_none("/proc/sys/kernel/random/boot_id"),
            "machine_id": _read_text_or_none("/etc/machine-id"),
            "script_sha256": _file_sha256(Path(__file__).resolve()),
            "source_ip": args.bind_ip,
            "tcp": {"bind_ip": args.bind_ip, "port": args.port},
            "udp": {"bind_ip": args.bind_ip, "port": args.port},
            "tls": {"bind_ip": args.bind_ip, "port": args.tls_port, "mtls": True},
            "limits": {"timeout_seconds": MAX_TIMEOUT, "max_frame": MAX_FRAME, "backlog": BACKLOG},
        }
        _write_ready(args.ready_json, ready)
        ready_written = True
        print(json.dumps(ready, sort_keys=True), flush=True)
        while not stop.is_set():
            time.sleep(0.2)
    finally:
        stop.set()
        for sock in sockets:
            sock.close()
        for thread in threads:
            thread.join(MAX_TIMEOUT + 1.0)
        if ready_written:
            print(json.dumps({"status": "STOPPED", "pid": os.getpid()}, sort_keys=True), flush=True)
    return 0


def _connect_tcp(source_ip: str, target_ip: str, port: int, timeout: float) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout)
        sock.bind((source_ip, 0))
        sock.connect((target_ip, port))
        return sock
    except Exception:
        sock.close()
        raise


def check_tcp(args: argparse.Namespace, token: bytes) -> dict[str, Any]:
    started = _now()
    try:
        with _connect_tcp(args.source_ip, args.target_ip, args.port, args.timeout) as sock:
            local = sock.getsockname()
            peer = sock.getpeername()
            result = probe_stream(sock, token, args.timeout)
            result.update({"local": local, "peer": peer, "elapsed_seconds": _now() - started})
            return result
    except TimeoutError:
        return {"status": "FAIL", "reason": "timeout", "elapsed_seconds": _now() - started}
    except OSError as exc:
        return {"status": "FAIL", "reason": "socket_error", "detail": repr(exc), "elapsed_seconds": _now() - started}


def check_udp(args: argparse.Namespace, token: bytes) -> dict[str, Any]:
    started = _now()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    with sock:
        try:
            sock.settimeout(args.timeout)
            sock.bind((args.source_ip, 0))
            local = sock.getsockname()
            sock.sendto(token, (args.target_ip, args.port))
            echoed, addr = sock.recvfrom(MAX_FRAME + 1)
            observed = {"local": local, "sender": addr, "elapsed_seconds": _now() - started}
            if addr != (args.target_ip, args.port):
                return {"status": "FAIL", "reason": "unexpected_sender", **observed}
            if echoed != token:
                return {"status": "FAIL", "reason": "mismatch", "received_len": len(echoed), **observed}
            return {"status": "PASS", "bytes": len(token), **observed}
        except TimeoutError:
            return {"status": "FAIL", "reason": "timeout", "elapsed_seconds": _now() - started}
        except OSError as exc:
            return {"status": "FAIL", "reason": "socket_error", "detail": repr(exc), "elapsed_seconds": _now() - started}


def _tls_attempt(args: argparse.Namespace, ca: Path, cert: Path | None, key: Path | None, hostname: str) -> dict[str, Any]:
    try:
        raw = _connect_tcp(args.source_ip, args.target_ip, args.tls_port, args.timeout)
        with raw:
            ctx = _tls_context_client(ca, cert, key)
            with ctx.wrap_socket(raw, server_hostname=hostname) as tls:
                # TLS 1.3 may deliver a certificate rejection on application I/O.
                # Retain the actual SSL exception for the negative classifier.
                token = secrets.token_bytes(TOKEN_BYTES)
                send_frame(tls, token)
                echoed = recv_frame(tls, args.timeout)
                result = ({"status": "PASS", "bytes": len(token)} if echoed == token
                          else {"status": "FAIL", "reason": "mismatch", "received_len": len(echoed)})
                result["tls_version"] = tls.version()
                result["cipher"] = tls.cipher()[0] if tls.cipher() else None
                return result
    except Exception as exc:  # noqa: BLE001 - serialized for probe JSON.
        cls = classify_tls_failure(exc)
        observed: dict[str, Any] = {"status": "FAIL", "reason": cls["classification"], "detail": cls["detail"]}
        for key in ("verify_code", "verify_message"):
            if cls.get(key):
                observed[key] = cls[key]
        return observed


def check_tls(args: argparse.Namespace, token: bytes) -> dict[str, Any]:
    started = _now()
    try:
        raw = _connect_tcp(args.source_ip, args.target_ip, args.tls_port, args.timeout)
        with raw:
            local = raw.getsockname()
            peer = raw.getpeername()
            ctx = _tls_context_client(args.ca, args.client_cert, args.client_key)
            with ctx.wrap_socket(raw, server_hostname=args.server_hostname) as tls:
                result = probe_stream(tls, token, args.timeout)
                result["local"] = local
                result["peer"] = peer
                result["elapsed_seconds"] = _now() - started
                result["tls_version"] = tls.version()
                result["cipher"] = tls.cipher()[0] if tls.cipher() else None
                result["server_cert_names"] = sorted(_cert_names(tls.getpeercert()))
                return result
    except ssl.SSLError as exc:
        cls = classify_tls_failure(exc)
        result: dict[str, Any] = {"status": "FAIL", "reason": cls["classification"], "detail": cls["detail"]}
        for key in ("verify_code", "verify_message"):
            if cls.get(key):
                result[key] = cls[key]
        result["elapsed_seconds"] = _now() - started
        return result
    except TimeoutError:
        return {"status": "FAIL", "reason": "timeout", "elapsed_seconds": _now() - started}
    except OSError as exc:
        return {"status": "FAIL", "reason": "socket_error", "detail": repr(exc), "elapsed_seconds": _now() - started}


def _negative_result(name: str, observed: dict[str, Any], expected: set[str]) -> dict[str, Any]:
    reason = str(observed.get("reason", ""))
    ok = observed.get("status") == "FAIL" and reason in expected
    return {"status": "PASS" if ok else "FAIL", "negative": name, "observed": observed}


def selected_checks(values: list[str]) -> set[str]:
    if not values or "all" in values:
        return {"tcp", "udp", "tls"}
    return set(values)


def client_main(args: argparse.Namespace) -> int:
    live_linux_arm64_guard()
    _deadline(args.timeout)
    token = secrets.token_bytes(TOKEN_BYTES)
    checks: dict[str, Any] = {}
    selected = selected_checks(args.check)
    if "tcp" in selected:
        checks["tcp"] = check_tcp(args, token)
    if "udp" in selected:
        checks["udp"] = check_udp(args, token)
    if "tls" in selected:
        checks["tls"] = check_tls(args, token)
    if args.untrusted_ca:
        if not args.bad_ca:
            raise ProbeError("missing_bad_ca", "--bad-ca is required for --untrusted-ca")
        observed = _tls_attempt(args, args.bad_ca, args.client_cert, args.client_key, args.server_hostname)
        checks["tls_untrusted_ca"] = _negative_result("untrusted_ca", observed, {"untrusted_ca", "certificate_verify_failed"})
    if args.untrusted_client_cert:
        if not args.bad_client_cert or not args.bad_client_key:
            raise ProbeError("missing_bad_client_cert", "--bad-client-cert and --bad-client-key are required")
        observed = _tls_attempt(args, args.ca, args.bad_client_cert, args.bad_client_key, args.server_hostname)
        checks["tls_untrusted_client_cert"] = _negative_result(
            "untrusted_client_cert", observed, {"untrusted_ca", "handshake_rejected"}
        )
    if args.wrong_hostname:
        observed = _tls_attempt(args, args.ca, args.client_cert, args.client_key, "afs-env-probe.invalid")
        checks["tls_wrong_hostname"] = _negative_result("wrong_hostname", observed, {"wrong_hostname"})
    if args.missing_client_cert:
        observed = _tls_attempt(args, args.ca, None, None, args.server_hostname)
        checks["tls_missing_client_cert"] = _negative_result(
            "missing_client_cert", observed, {"missing_client_cert", "handshake_rejected"}
        )
    ok = bool(checks) and all(item.get("status") == "PASS" for item in checks.values())
    report = {
        "status": "PASS" if ok else "FAIL",
        "target": args.target_ip,
        "source_bind": args.source_ip,
        "timeout_seconds": args.timeout,
        "token_bytes": len(token),
        "token_sha256": hashlib.sha256(token).hexdigest(),
        "checks": checks,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if ok else 1


def live_linux_arm64_guard() -> None:
    machine = platform.machine().lower()
    if platform.system() != "Linux" or machine not in {"aarch64", "arm64"}:
        raise ProbeError("linux_arm64_required", f"live network probe requires Linux ARM64, got {platform.system()} {machine}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    server = sub.add_parser("server", help="serve bounded TCP, UDP and mTLS exact echo")
    server.add_argument("--bind-ip", required=True)
    server.add_argument("--port", required=True, type=int)
    server.add_argument("--tls-port", required=True, type=int)
    server.add_argument("--ca", required=True, type=Path)
    server.add_argument("--server-cert", required=True, type=Path)
    server.add_argument("--server-key", required=True, type=Path)
    server.add_argument("--ready-json", required=True, type=Path)
    server.add_argument("--peer-expected", help="optional client certificate CN/SAN required before echo")
    server.set_defaults(func=server_main)
    client = sub.add_parser("client", help="run explicit source-bound network checks")
    client.add_argument("--source-ip", required=True)
    client.add_argument("--target-ip", required=True)
    client.add_argument("--port", required=True, type=int)
    client.add_argument("--tls-port", required=True, type=int)
    client.add_argument("--ca", required=True, type=Path)
    client.add_argument("--client-cert", required=True, type=Path)
    client.add_argument("--client-key", required=True, type=Path)
    client.add_argument("--server-hostname", required=True)
    client.add_argument("--timeout", type=float, default=MAX_TIMEOUT)
    client.add_argument("--check", choices=["all", "tcp", "udp", "tls"], action="append", default=[])
    client.add_argument("--untrusted-ca", action="store_true")
    client.add_argument("--bad-ca", type=Path)
    client.add_argument("--untrusted-client-cert", action="store_true")
    client.add_argument("--bad-client-cert", type=Path)
    client.add_argument("--bad-client-key", type=Path)
    client.add_argument("--wrong-hostname", action="store_true")
    client.add_argument("--missing-client-cert", action="store_true")
    client.set_defaults(func=client_main)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except ProbeError as exc:
        print(json.dumps({"status": "FAIL", "reason": exc.reason, "detail": exc.detail}, sort_keys=True), file=sys.stderr)
        return 2
    except OSError as exc:
        print(json.dumps({"status": "FAIL", "reason": "os_error", "detail": repr(exc)}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
