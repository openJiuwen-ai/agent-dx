#!/usr/bin/env python3
"""Collect observed Linux guest state, never expected readiness values.

Process and environment probes are observations only. They do not imply readiness
or satisfy resource/durability gates. Optional unreadable fields remain UNKNOWN.
They intentionally avoid /proc/<pid>/cmdline and environ because those may
contain secrets.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import platform
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Iterable

REQUIRED_SYSTEM = "Linux"
REQUIRED_MACHINE = "aarch64"
PROC_ROOT = pathlib.Path("/proc")
PROCESS_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class InventoryError(RuntimeError):
    pass


@dataclass(frozen=True)
class ProcessProbe:
    name: str
    pid: int
    source: str


def require_linux_arm64() -> None:
    if platform.system() != REQUIRED_SYSTEM or platform.machine() != REQUIRED_MACHINE:
        raise InventoryError("dedicated ARM64 Linux required")


def command(argv: list[str]) -> dict[str, object]:
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=15)
    except subprocess.TimeoutExpired as error:
        return {
            "argv": argv,
            "returncode": None,
            "status": "UNKNOWN",
            "stdout": "",
            "stderr": str(error),
        }
    except OSError as error:
        return {
            "argv": argv,
            "returncode": 127,
            "status": "UNKNOWN",
            "stdout": "",
            "stderr": str(error),
        }
    return {
        "argv": argv,
        "returncode": result.returncode,
        "status": "OBSERVED" if result.returncode == 0 else "UNKNOWN",
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def file_observation(path: pathlib.Path) -> dict[str, object]:
    """Read only an explicitly selected non-sensitive kernel/system field."""
    try:
        value = path.read_text(encoding="utf-8").strip()
        return {"path": str(path), "status": "OBSERVED", "value": value}
    except OSError as error:
        return {"path": str(path), "status": "UNKNOWN", "value": None, "error": str(error)}


def cgroup_observation(
    pid: int,
    proc_root: pathlib.Path = PROC_ROOT,
    cgroup_root: pathlib.Path = pathlib.Path("/sys/fs/cgroup"),
) -> dict[str, object]:
    membership = file_observation(proc_root / str(pid) / "cgroup")
    result: dict[str, object] = {"membership": membership, "status": "UNKNOWN"}
    value = membership.get("value")
    if not isinstance(value, str):
        return result
    unified = next((line[3:] for line in value.splitlines() if line.startswith("0::")), None)
    if unified is None:
        result["reason"] = "cgroup v1 limits are not collected; no unlimited quota is inferred"
        return result
    root = cgroup_root.resolve()
    leaf = (root / unified.lstrip("/")).resolve()
    if not leaf.is_relative_to(root):
        result["reason"] = "cgroup membership is outside the mounted cgroup hierarchy"
        return result
    # Ancestor limits also apply: the leaf's `max` is not an unlimited budget.
    ancestors: list[dict[str, object]] = []
    current = leaf
    while True:
        ancestors.append({
            "path": str(current),
            "fields": {name: file_observation(current / name) for name in (
                "cpu.max", "cpuset.cpus.effective", "cpuset.mems.effective",
                "memory.max", "memory.high", "memory.current", "memory.swap.max",
                "memory.swap.current", "pids.max", "pids.current",
            )},
        })
        if current == root:
            break
        current = current.parent
    result.update({
        "status": "OBSERVED", "version": 2, "ancestors": ancestors,
        "scope": "visible guest hierarchy only; host quotas are unknown",
    })
    return result


def sysfs_observations(root: pathlib.Path, fields: tuple[str, ...]) -> dict[str, object]:
    try:
        entries = sorted(root.iterdir())
    except OSError as error:
        return {"status": "UNKNOWN", "path": str(root), "error": str(error)}
    return {
        "status": "OBSERVED", "path": str(root),
        "entries": {entry.name: {name: file_observation(entry / name) for name in fields}
                    for entry in entries if entry.is_dir()},
    }


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_pid(value: str, source: str) -> int:
    text = value.strip()
    if not text or not text.isdecimal():
        raise InventoryError(f"{source} must contain a positive decimal PID")
    pid = int(text)
    if pid <= 0:
        raise InventoryError(f"{source} must contain a positive decimal PID")
    return pid


def validate_process_name(name: str) -> str:
    if not PROCESS_NAME_RE.fullmatch(name):
        raise InventoryError(
            f"process name {name!r} must match {PROCESS_NAME_RE.pattern}; do not use paths"
        )
    return name


def parse_process_assignment(value: str) -> ProcessProbe:
    if "=" not in value:
        raise InventoryError("--process expects NAME=PID")
    name, pid_text = value.split("=", 1)
    name = validate_process_name(name)
    return ProcessProbe(name=name, pid=parse_pid(pid_text, f"--process {name}"), source="argv")


def parse_pid_file_assignment(value: str) -> ProcessProbe:
    if "=" not in value:
        raise InventoryError("--pid-file expects NAME=PATH")
    name, path_text = value.split("=", 1)
    name = validate_process_name(name)
    path = pathlib.Path(path_text)
    pid = parse_pid(path.read_text(encoding="utf-8"), str(path))
    return ProcessProbe(name=name, pid=pid, source=str(path))


def parse_start_ticks(stat_text: str) -> int:
    close = stat_text.rfind(")")
    if close == -1:
        raise InventoryError("/proc stat is malformed: missing comm terminator")
    tail = stat_text[close + 2 :].split()
    # proc_pid_stat(5): fields after comm start at field 3, so starttime field
    # 22 is tail index 19.
    if len(tail) <= 19:
        raise InventoryError("/proc stat is malformed: missing starttime")
    try:
        return int(tail[19])
    except ValueError as exc:
        raise InventoryError("/proc stat has non-numeric starttime") from exc


def read_process_fingerprint(pid: int, proc_root: pathlib.Path = PROC_ROOT) -> dict[str, object]:
    proc_dir = proc_root / str(pid)
    stat_path = proc_dir / "stat"
    exe_path = proc_dir / "exe"
    try:
        stat_text = stat_path.read_text(encoding="utf-8")
        exe_target = os.readlink(exe_path)
        exe_stat = exe_path.stat()
    except FileNotFoundError as exc:
        raise InventoryError(f"pid {pid} is not alive") from exc
    except ProcessLookupError as exc:
        raise InventoryError(f"pid {pid} disappeared while reading identity") from exc
    except PermissionError as exc:
        raise InventoryError(f"pid {pid} identity is not readable: {exc}") from exc
    return {
        "pid": pid,
        "start_ticks": parse_start_ticks(stat_text),
        "exe_path": exe_target,
        "exe_dev": exe_stat.st_dev,
        "exe_inode": exe_stat.st_ino,
    }


def assert_same_process(before: dict[str, object], after: dict[str, object], name: str) -> None:
    for key in ("pid", "start_ticks", "exe_dev", "exe_inode"):
        if before[key] != after[key]:
            raise InventoryError(
                f"process {name} changed while probing: {key} {before[key]!r} -> {after[key]!r}"
            )


def collect_process_identity(probe: ProcessProbe, proc_root: pathlib.Path = PROC_ROOT) -> dict[str, object]:
    before = read_process_fingerprint(probe.pid, proc_root)
    exe_proc_path = proc_root / str(probe.pid) / "exe"
    try:
        digest = sha256_file(exe_proc_path)
    except FileNotFoundError as exc:
        raise InventoryError(f"pid {probe.pid} exited before binary hash completed") from exc
    except PermissionError as exc:
        raise InventoryError(f"pid {probe.pid} executable is not readable: {exc}") from exc
    resources = {
        "limits": file_observation(proc_root / str(probe.pid) / "limits"),
        "cgroup": cgroup_observation(probe.pid, proc_root),
    }
    after = read_process_fingerprint(probe.pid, proc_root)
    assert_same_process(before, after, probe.name)
    return {
        "name": probe.name,
        "pid": probe.pid,
        "pid_source": probe.source,
        "exe_path": before["exe_path"],
        "sha256": digest,
        "start_ticks": before["start_ticks"],
        "exe_dev": before["exe_dev"],
        "exe_inode": before["exe_inode"],
        "resources": resources,
    }


def collect_processes(probes: Iterable[ProcessProbe]) -> dict[str, dict[str, object]]:
    processes: dict[str, dict[str, object]] = {}
    for probe in probes:
        if probe.name in processes:
            raise InventoryError(f"duplicate process name {probe.name!r}")
        processes[probe.name] = collect_process_identity(probe)
    return processes


def collect_guest_state(probes: Iterable[ProcessProbe]) -> dict[str, object]:
    rdma = pathlib.Path("/var/lib/afs-acceptance")
    state: dict[str, object] = {
        "observed_at_unix": time.time(),
        "hostname": platform.node(),
        "architecture": platform.machine(),
        "kernel": platform.release(),
        "cpu_count": os.cpu_count(),
        "meminfo": pathlib.Path("/proc/meminfo").read_text(encoding="utf-8"),
        "swap": pathlib.Path("/proc/swaps").read_text(encoding="utf-8"),
        "os_release": pathlib.Path("/etc/os-release").read_text(encoding="utf-8"),
        "fuse_present": pathlib.Path("/dev/fuse").exists(),
        "clock": {
            "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "monotonic_seconds": time.monotonic(),
            "uptime": file_observation(PROC_ROOT / "uptime"),
            "synchronization_accuracy": "UNKNOWN; sync service state does not measure clock error",
        },
        "collector_resources": {
            "limits": file_observation(PROC_ROOT / "self" / "limits"),
            "cgroup": cgroup_observation(os.getpid()),
            "swappiness": file_observation(PROC_ROOT / "sys" / "vm" / "swappiness"),
        },
        "block_metadata": sysfs_observations(pathlib.Path("/sys/block"), (
            "queue/write_cache", "queue/scheduler", "queue/rotational",
            "queue/logical_block_size", "queue/physical_block_size",
        )),
        "host_disk_cache_and_thin_allocation": "UNKNOWN; cannot be established inside a guest",
        "fuse_connections": sysfs_observations(pathlib.Path("/sys/fs/fuse/connections"), (
            "waiting", "max_background", "congestion_threshold",
        )),
        "fuse_module_parameters": {
            name: file_observation(pathlib.Path("/sys/module/fuse/parameters") / name)
            for name in ("max_user_bgreq", "max_user_congthresh")
        },
    }
    for name, argv in {
        "addresses": ["ip", "-j", "address"],
        "routes": ["ip", "-j", "route"],
        "volumes": ["findmnt", "--json", "--real"],
        "disks": ["lsblk", "-J", "-b", "-o", "NAME,SIZE,FSTYPE,MOUNTPOINTS"],
        "disk_space": ["df", "--local", "--block-size=1", "--output=source,fstype,size,used,avail,pcent,target"],
        "block_layout": ["lsblk", "-J", "-b", "-o", "NAME,MAJ:MIN,SIZE,FSTYPE,UUID,MOUNTPOINTS,ROTA,RO,LOG-SEC,PHY-SEC"],
        "time_service": ["timedatectl", "show", "--property=NTPSynchronized", "--property=NTP", "--property=Timezone", "--property=LocalRTC"],
        "kernel_packages": ["dpkg-query", "-W", "-f=${Package} ${Version} ${Status}\n",
                            f"linux-image-{platform.release()}", f"linux-modules-{platform.release()}",
                            f"linux-modules-extra-{platform.release()}"],
        "rdma_links": ["rdma", "link", "show"],
        "rdma_device": ["ibv_devinfo", "-d", "rxe0", "-v"],
        "packages": ["dpkg-query", "-W", "-f=${Package} ${Version}\n"],
    }.items():
        state[name] = command(argv)
    for name in ["gids.txt", "ibv-devinfo.txt"]:
        path = rdma / name
        state[name] = path.read_text(encoding="utf-8") if path.exists() else None
    packages = state["packages"]
    assert isinstance(packages, dict)
    state["packages_sha256"] = hashlib.sha256(str(packages["stdout"]).encode()).hexdigest()
    process_identities = collect_processes(probes)
    if process_identities:
        state["processes"] = process_identities
    return state


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--process",
        action="append",
        default=[],
        metavar="NAME=PID",
        help="capture a live process binary identity from /proc without reading cmdline/environ",
    )
    parser.add_argument(
        "--pid-file",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="read PID from PATH and capture that live process identity",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def probes_from_args(args: argparse.Namespace) -> list[ProcessProbe]:
    probes = [parse_process_assignment(value) for value in args.process]
    probes.extend(parse_pid_file_assignment(value) for value in args.pid_file)
    return probes


def main(argv: list[str] | None = None) -> int:
    try:
        require_linux_arm64()
        args = parse_args(argv)
        state = collect_guest_state(probes_from_args(args))
    except InventoryError as error:
        print(f"inventory error: {error}", file=sys.stderr)
        return 2
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
