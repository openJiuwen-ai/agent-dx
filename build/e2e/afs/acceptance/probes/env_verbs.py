#!/usr/bin/env python3
"""Cross-VM stock rping evidence collector for AFS ENV-01.

The probe proves a bounded rdma-core rping exchange only. SEND messages are
control descriptors; bulk user payload proof comes from validated RDMA
READ/WRITE completions plus full printed payload hashes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import signal
import struct
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any
from ipaddress import ip_address, IPv6Address

RPING = Path("/usr/bin/rping")
EXPECTED_RPING_SHA256 = "d4d82fdd9b78cfb9404bbb4c2a8c07dde9d450d7fd4d7041d5d0ffcf32e3e89f"
MAX_TIMEOUT = 30
MAX_SIZE = 65535
MAX_COUNT = 16
DEFAULT_GID = 1


class VerbsError(ValueError):
    pass


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str | None:
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def read_text(path: Path, limit: int = 65536) -> str | None:
    try:
        data = path.read_bytes()[:limit]
        return data.decode("utf-8", "replace")
    except OSError:
        return None


def run_cmd(argv: list[str], timeout: float = 3.0) -> dict[str, Any]:
    try:
        proc = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
        return {"argv": argv, "returncode": proc.returncode, "stdout": proc.stdout.decode("utf-8", "replace"), "stderr": proc.stderr.decode("utf-8", "replace")}
    except Exception as exc:  # pragma: no cover - exact exception type is not material evidence.
        return {"argv": argv, "error": repr(exc)}


def proc_identity(pid: int) -> dict[str, Any]:
    ident: dict[str, Any] = {"pid": pid}
    stat = read_text(Path(f"/proc/{pid}/stat"))
    if stat and ") " in stat:
        try:
            ident["start_ticks"] = int(stat.rsplit(") ", 1)[1].split()[19])
        except (IndexError, ValueError):
            ident["start_ticks"] = None
    exe = Path(f"/proc/{pid}/exe")
    try:
        ident["exe"] = str(exe.resolve())
        ident["exe_sha256"] = sha256_file(exe)
    except OSError:
        ident["exe"] = None
        ident["exe_sha256"] = None
        ident["limitation"] = "process identity raced before loaded executable could be observed"
    return ident


def wait_rping_identity(pid: int, deadline: float = 1.5) -> dict[str, Any]:
    ident = proc_identity(pid)
    first = dict(ident)
    end = time.monotonic() + deadline
    while time.monotonic() < end and ident.get("exe_sha256") != EXPECTED_RPING_SHA256:
        time.sleep(0.001)
        latest = proc_identity(pid)
        if latest.get("start_ticks") is None and ident.get("start_ticks") is not None:
            latest["start_ticks"] = ident["start_ticks"]
        ident = latest
    if ident.get("exe_sha256") != EXPECTED_RPING_SHA256:
        if ident.get("start_ticks") is None:
            ident["start_ticks"] = first.get("start_ticks")
        ident["limitation"] = "live process executable identity was not observed as the pinned rping binary"
    return ident


def host_identity() -> dict[str, Any]:
    return {"hostname": platform.node(), "kernel": platform.release(), "machine": platform.machine(), "boot_id": read_text(Path("/proc/sys/kernel/random/boot_id")), "machine_id": read_text(Path("/etc/machine-id"))}


def rdma_inventory(bind: str, peer: str, gid_index: int = DEFAULT_GID) -> dict[str, Any]:
    gid_path = Path(f"/sys/class/infiniband/rxe0/ports/1/gids/{gid_index}")
    lib_paths = [Path("/usr/lib/aarch64-linux-gnu/libibverbs.so.1"), Path("/lib/aarch64-linux-gnu/libibverbs.so.1")]
    rxe_paths = list(Path("/usr/lib/aarch64-linux-gnu/libibverbs").glob("*rxe*.so"))
    return {
        "binary": {"path": str(RPING), "sha256": sha256_file(RPING), "expected_sha256": EXPECTED_RPING_SHA256},
        "installed_provider": {
            "ldconfig": run_cmd(["/sbin/ldconfig", "-p"], timeout=2.0),
            "libibverbs": run_cmd(["dpkg-query", "-W", "-f=${Package} ${Version}\\n", "libibverbs1:arm64"], timeout=2.0),
            "rdma_core": run_cmd(["dpkg-query", "-W", "-f=${Package} ${Version}\\n", "rdma-core"], timeout=2.0),
            "libibverbs_so_sha256": next((sha256_file(p) for p in lib_paths if p.exists()), None),
            "rxe_provider_so_sha256": next((sha256_file(p) for p in rxe_paths if p.exists()), None),
            "rdma_rxe_modinfo": run_cmd(["modinfo", "rdma_rxe"], timeout=2.0),
        },
        "rdma_link": run_cmd(["rdma", "link"], timeout=2.0),
        "ibv_devinfo": run_cmd(["ibv_devinfo"], timeout=3.0),
        "route": run_cmd(["ip", "route", "get", peer, "from", bind], timeout=2.0),
        "sysfs": {
            "gid_index": gid_index,
            "gid": read_text(gid_path),
            "eth0_mtu": read_text(Path("/sys/class/net/eth0/mtu")),
            "rxe0_node_type": read_text(Path("/sys/class/infiniband/rxe0/node_type")),
        },
    }


def resources_snapshot() -> dict[str, Any]:
    return {"rdma_link": run_cmd(["rdma", "link"], timeout=2.0), "rdma_resource": run_cmd(["rdma", "resource", "show"], timeout=2.0), "rdma_qp_json": run_cmd(["rdma", "-j", "resource", "show", "qp"], timeout=2.0), "rdma_cm_id_json": run_cmd(["rdma", "-j", "resource", "show", "cm_id"], timeout=2.0)}


def expected_payload(index: int, size: int) -> bytes:
    if size <= 0 or size > MAX_SIZE:
        raise VerbsError(f"size must be 1..{MAX_SIZE}")
    payload = bytearray(size)
    text = f"rdma-ping-{index}: ".encode("ascii")
    n = min(len(text), max(size - 1, 0))
    payload[:n] = text[:n]
    ch = 65 + (index % (122 - 65 + 1))
    for pos in range(n, max(size - 1, 0)):
        payload[pos] = ch
        ch += 1
        if ch > 122:
            ch = 65
    payload[-1] = 0
    return bytes(payload)


def payload_line(index: int, size: int) -> str:
    return expected_payload(index, size)[:-1].decode("ascii", "replace")


def validate_run_args(args: argparse.Namespace) -> None:
    if not (0 < args.timeout <= MAX_TIMEOUT):
        raise VerbsError(f"timeout must be >0 and <= {MAX_TIMEOUT}s")
    if not (32 <= args.size <= MAX_SIZE):
        raise VerbsError(f"size must be 32..{MAX_SIZE}")
    if not (1 <= args.count <= MAX_COUNT):
        raise VerbsError(f"count must be 1..{MAX_COUNT}")
    if not (1024 <= args.port <= 65535):
        raise VerbsError("port must be 1024..65535")
    try:
        uuid.UUID(args.run_id) if args.run_id else None
        bind, peer = ip_address(args.bind), ip_address(args.peer)
    except ValueError as exc:
        raise VerbsError("run-id must be UUID and bind/peer must be IP addresses") from exc
    if bind.version != 4 or peer.version != 4 or bind == peer:
        raise VerbsError("bind and peer must be distinct IPv4 addresses")


def parse_descriptors(text: str) -> dict[str, list[tuple[int, int, int]]]:
    client = [(int(a, 16), int(r, 16), int(s)) for a, r, s in re.findall(r"RDMA addr ([0-9a-fA-F]+) rkey ([0-9a-fA-F]+) len (\d+)", text)]
    server = [(int(a, 16), int(r, 16), int(s)) for r, a, s in re.findall(r"Received rkey ([0-9a-fA-F]+) addr ([0-9a-fA-F]+) len (\d+) from peer", text)]
    return {"client": client, "server": server}


def descriptor_sha(desc: tuple[int, int, int]) -> str:
    return sha256_bytes(struct.pack("!QII", desc[0], desc[1], desc[2]))


def parse_log(text: str, role: str, size: int, count: int) -> dict[str, Any]:
    expected = [payload_line(i, size) for i in range(count)]
    prefix = r"server ping data" if role == "server" else r"ping data"
    payloads = [item.rstrip("\r") for item in re.findall(rf"^{prefix}: (.*)$", text, re.M)]
    completions = {
        "send": len(re.findall(r"^send completion$", text, re.M)),
        "recv": len(re.findall(r"^recv completion$", text, re.M)),
        "read": len(re.findall(r"^rdma read completion$", text, re.M)),
        "write": len(re.findall(r"^rdma write completion$", text, re.M)),
    }
    allowed_tail = "wait for RDMA_READ_ADV state 10"
    bad_line = re.compile(r"\b(error|failed|timeout|mismatch|truncated|removal|bogus|unknown|wait for)\b", re.I)
    error_lines = [line for line in text.splitlines() if bad_line.search(line) and line != allowed_tail]
    return {
        "role": role,
        "payloads_exact": payloads == expected,
        "payload_count": len(payloads),
        "payload_sha256": [sha256_bytes((p + "\x00").encode("ascii", "replace")) for p in payloads],
        "completions": completions,
        "descriptors": parse_descriptors(text),
        "has_established": bool(re.search(r"^cma_event type RDMA_CM_EVENT_ESTABLISHED cma_id 0x[0-9a-f]+ \((parent|child)\)$", text, re.M)),
        "has_disconnect": bool(re.search(r"^cma_event type RDMA_CM_EVENT_DISCONNECTED cma_id 0x[0-9a-f]+ \((parent|child)\)$", text, re.M)),
        "has_cleanup": bool(re.search(r"^rping_free_buffers called on cb 0x[0-9a-f]+$", text, re.M) and re.search(r"^destroy cm_id 0x[0-9a-f]+$", text, re.M)),
        "allowed_server_tail": role == "server" and allowed_tail in text,
        "error_lines": error_lines,
    }


def ipv4_mapped_gid(ip: str) -> str:
    octets = [int(part) for part in ip.split(".")]
    return f"::ffff:{octets[0]:02x}{octets[1]:02x}:{octets[2]:02x}{octets[3]:02x}"


def gid_matches_ipv4(gid: str, ip: str) -> bool:
    try:
        return IPv6Address(gid).ipv4_mapped == ip_address(ip)
    except ValueError:
        return False


def inventory_ok(ep: dict[str, Any], problems: list[str], name: str) -> None:
    inv = ep.get("inventory", {})
    if inv.get("binary", {}).get("sha256") != EXPECTED_RPING_SHA256:
        problems.append(f"{name} unsupported rping binary")
    link = inv.get("rdma_link", {}).get("stdout", "")
    if not re.search(r"link rxe0/\d+ state ACTIVE physical_state LINK_UP netdev eth0", link):
        problems.append(f"{name} rxe0 eth0 ACTIVE identity missing")
    route = inv.get("route", {}).get("stdout", "")
    tokens = route.splitlines()[0].split() if route.splitlines() else []
    fields = {key: tokens[i + 1] for i, key in enumerate(tokens[:-1]) if key in {"from", "src", "prefsrc", "dev"}}
    if not tokens or tokens[0] != ep.get("peer") or fields.get("dev") != "eth0" or not any(fields.get(key) == ep.get("bind") for key in ("from", "src", "prefsrc")):
        problems.append(f"{name} route is not source-bound")
    sysfs = inv.get("sysfs", {})
    if str(sysfs.get("eth0_mtu", "")).strip() != "1500":
        problems.append(f"{name} eth0 MTU 1500 identity missing")
    gid = str(sysfs.get("gid", "")).strip().lower()
    if ep.get("bind") and not gid_matches_ipv4(gid, ep["bind"]):
        problems.append(f"{name} rxe0 gid does not match bind IPv4")
    provider = inv.get("installed_provider", {})
    if not provider.get("libibverbs_so_sha256") or not provider.get("rxe_provider_so_sha256"):
        problems.append(f"{name} installed libibverbs fingerprint missing")


def endpoint_identity_ok(client: dict[str, Any], server: dict[str, Any], problems: list[str]) -> None:
    if client.get("role") != "client" or server.get("role") != "server":
        problems.append("endpoint roles are wrong")
    for field in ("run_id", "port", "size", "count"):
        if client.get(field) != server.get(field):
            problems.append(f"{field} mismatch")
    if client.get("bind") != server.get("peer") or client.get("peer") != server.get("bind"):
        problems.append("bind/peer endpoints are not mutual")
    chost, shost = client.get("host", {}), server.get("host", {})
    if not chost.get("machine_id") or not shost.get("machine_id") or chost.get("machine_id") == shost.get("machine_id"):
        problems.append("distinct guest identities are missing")
    for name, ep in (("client", client), ("server", server)):
        if ep.get("timed_out") is not False:
            problems.append(f"{name} timed out")
        raw = ep.get("raw_log", "")
        if not ep.get("raw_log_sha256") or ep["raw_log_sha256"] != sha256_bytes(raw.encode("utf-8", "replace")):
            problems.append(f"{name} raw log hash mismatch")
        proc = ep.get("process", {})
        if proc.get("exe_sha256") != EXPECTED_RPING_SHA256 or not proc.get("start_ticks"):
            problems.append(f"{name} live pinned rping process identity missing")
        if "resources_before" not in ep or "resources_after" not in ep:
            problems.append(f"{name} before/after resource snapshots missing")
        expected = rping_argv(ep["role"], ep["bind"], ep["peer"], int(ep["port"]), int(ep["size"]), int(ep["count"]))
        argv = ep.get("argv", [])
        if argv != expected and argv != ["/usr/bin/stdbuf", "-oL", "-eL"] + expected:
            problems.append(f"{name} argv is not the exact rping command")


def evaluate_pair(client: dict[str, Any], server: dict[str, Any]) -> dict[str, Any]:
    problems: list[str] = []
    c_desc: list[tuple[int, int, int]] = []
    size, count = 0, 0
    try:
        size, count = int(client.get("size", 0)), int(client.get("count", 0))
        for ep in (client, server):
            if not isinstance(ep.get("run_id"), str) or not ep["run_id"]:
                raise VerbsError("endpoint evidence requires a concrete run UUID")
            uuid.UUID(ep["run_id"])
            validate_run_args(argparse.Namespace(**{key: ep[key] for key in ("run_id", "bind", "peer", "port", "size", "count")}, timeout=ep["timeout_seconds"]))
        endpoint_identity_ok(client, server, problems)
        for name, ep in (("client", client), ("server", server)):
            if ep.get("returncode") != 0:
                problems.append(f"{name} returncode {ep.get('returncode')}")
            inventory_ok(ep, problems, name)
            text = ep.get("raw_log", "")
            parsed = parse_log(text, name, size, count)
            ep["parsed"] = parsed
            if not parsed["payloads_exact"]:
                problems.append(f"{name} payload proof missing")
            if parsed["completions"]["send"] != count * 2 or parsed["completions"]["recv"] != count * 2:
                problems.append(f"{name} SEND/RECV control completions missing")
            if name == "server" and (parsed["completions"]["read"] != count or parsed["completions"]["write"] != count):
                problems.append("server RDMA READ/WRITE completions missing")
            if name == "client" and (parsed["completions"]["read"] or parsed["completions"]["write"]):
                problems.append("client unexpectedly reports RDMA READ/WRITE completions")
            if not parsed["has_established"] or not parsed["has_cleanup"] or (name == "server" and not parsed["has_disconnect"]):
                problems.append(f"{name} connection lifecycle evidence missing")
            if parsed["error_lines"]:
                problems.append(f"{name} error lines present")
        c_desc = client["parsed"]["descriptors"]["client"]
        s_desc = server["parsed"]["descriptors"]["server"]
        if any(not (0 <= a <= 2**64 - 1 and 0 <= r <= 2**32 - 1 and s == size) for descs in (c_desc, s_desc) for a, r, s in descs):
            problems.append("descriptor values out of bounds")
        if len(c_desc) != count * 2 or len(s_desc) != count * 2:
            problems.append("descriptor advertisements missing")
        elif c_desc != s_desc:
            problems.append("descriptor advertisements do not match server receipts")
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        problems.append(f"malformed endpoint evidence: {exc}")
    return {
        "status": "FAIL" if problems else "PASS",
        "problems": problems,
        "descriptor_sha256": [descriptor_sha(d) for d in c_desc[: count * 2] if 0 <= d[0] <= 2**64 - 1 and 0 <= d[1] <= 2**32 - 1 and 0 <= d[2] <= 2**32 - 1],
        "limitations": ["stock rping server does not authorize peer source; client source binding and descriptor receipt are recorded"],
    }


def output_paths(output: Path, run_id: str, role: str) -> tuple[Path, Path]:
    output.mkdir(parents=True, exist_ok=True)
    report = output / f"{run_id}-{role}.json"
    raw = output / f"{run_id}-{role}.raw.log"
    if report.exists() or raw.exists():
        raise VerbsError(f"refusing to overwrite existing output for {run_id} {role}")
    return report, raw


def rping_argv(role: str, bind: str, peer: str, port: int, size: int, count: int) -> list[str]:
    base = [str(RPING), "-d", "-v", "-V", "-S", str(size), "-C", str(count), "-p", str(port)]
    if role == "server":
        return base + ["-s", "-a", bind]
    return base + ["-c", "-I", bind, "-a", peer]


def run_endpoint(args: argparse.Namespace) -> int:
    validate_run_args(args)
    run_id = args.run_id or str(uuid.uuid4())
    report_path, raw_path = output_paths(args.output, run_id, args.role)
    argv = rping_argv(args.role, args.bind, args.peer, args.port, args.size, args.count)
    if sha256_file(RPING) != EXPECTED_RPING_SHA256:
        raise VerbsError("installed /usr/bin/rping does not match the pinned binary hash")
    if Path("/usr/bin/stdbuf").exists():
        argv = ["/usr/bin/stdbuf", "-oL", "-eL"] + argv
    inventory_before = rdma_inventory(args.bind, args.peer)
    resources_before = resources_snapshot()
    started = time.time()
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
    procid = wait_rping_identity(proc.pid)
    chunks: list[bytes] = []

    def reader() -> None:
        assert proc.stdout is not None
        with raw_path.open("xb") as f:
            for chunk in iter(lambda: proc.stdout.readline(), b""):
                chunks.append(chunk)
                f.write(chunk)
                f.flush()

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    timed_out = False
    try:
        rc = proc.wait(timeout=args.timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            rc = proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            rc = proc.wait(timeout=2)
    thread.join(2)
    raw = b"".join(chunks)
    report = {
        "run_id": run_id,
        "role": args.role,
        "bind": args.bind,
        "peer": args.peer,
        "port": args.port,
        "size": args.size,
        "count": args.count,
        "argv": argv,
        "timeout_seconds": args.timeout,
        "timed_out": timed_out,
        "returncode": rc,
        "started_unix": started,
        "ended_unix": time.time(),
        "host": host_identity(),
        "process": procid,
        "inventory": inventory_before,
        "resources_before": resources_before,
        "resources_after": resources_snapshot(),
        "raw_log_path": str(raw_path),
        "raw_log_sha256": sha256_bytes(raw),
        "raw_log": raw.decode("utf-8", "replace"),
    }
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 124 if timed_out else int(rc)


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise VerbsError(f"{path} is not a JSON object")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run")
    run.add_argument("--role", choices=["server", "client"], required=True)
    run.add_argument("--bind", required=True)
    run.add_argument("--peer", required=True)
    run.add_argument("--port", type=int, required=True)
    run.add_argument("--size", type=int, required=True)
    run.add_argument("--count", type=int, required=True)
    run.add_argument("--run-id")
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--timeout", type=int, default=MAX_TIMEOUT)
    ev = sub.add_parser("evaluate-pair")
    ev.add_argument("--client", type=Path, required=True)
    ev.add_argument("--server", type=Path, required=True)
    ev.add_argument("--output", type=Path)
    ns = parser.parse_args(argv)
    if ns.cmd == "run":
        return run_endpoint(ns)
    result = evaluate_pair(load_json(ns.client), load_json(ns.server))
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if ns.output:
        if ns.output.exists():
            raise VerbsError(f"refusing to overwrite {ns.output}")
        ns.output.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except VerbsError as exc:
        print(f"env_verbs: {exc}", file=sys.stderr)
        raise SystemExit(2)
