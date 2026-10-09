#!/usr/bin/env python3
"""OwnerFs W2 cross-node segmented diagnostics using old S5 HomeFs semantics.

This runner is intentionally a diagnostic report, not a value gate. It expects
an already-running AFS OwnerFs deployment and an already-mounted MooseFS pair in
the three-Linux-VM lab. It reuses the copied `s5_distributed_workload.py` stage
contract from the old HomeFs S5 runner:

A prepare -> B first read -> B repeat read -> B overwrite -> A read updated -> A cleanup

Each round runs the same seed against OwnerFs and MooseFS, alternates backend
order, records raw per-file samples plus per-stage ledgers, and reports p50
OwnerFs/MooseFS ratios without declaring a pass threshold.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import socket
import subprocess
import sys
import time
from typing import Any

SCHEMA = "afs.ownerfs-cross-node-w2-segments.v1"
TIMING_CONTRACT = "dms.s5-w2-cross-node-stages.v1"
BACKENDS = ("ownerfs", "moosefs")
STAGE_NAMES = ("first_read", "repeat_read", "remote_overwrite", "owner_read_after")


class RunnerError(RuntimeError):
    pass


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def nearest_rank(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, int(__import__("math").ceil(len(ordered) * quantile)) - 1)]


class RemoteShell:
    def __init__(self, role: str, host: str, user: str | None, ssh_opts: list[str] | None):
        self.role = role
        self.host = host
        self.user = user
        self.ssh_opts = ssh_opts or []

    @property
    def is_local(self) -> bool:
        return self.host in ("localhost", "127.0.0.1", "::1", socket.gethostname())

    def argv(self, script: str) -> list[str]:
        if self.is_local:
            return ["bash", "-lc", script]
        target = f"{self.user}@{self.host}" if self.user else self.host
        return ["ssh", *self.ssh_opts, target, "bash", "-lc", shlex.quote(script)]

    def run(self, script: str, *, timeout: float = 30.0, check: bool = False) -> subprocess.CompletedProcess[str]:
        try:
            proc = subprocess.run(
                self.argv(script),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout or b"").decode("utf-8", errors="replace")
            stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr or b"").decode("utf-8", errors="replace")
            proc = subprocess.CompletedProcess(self.argv(script), 124, stdout, stderr + f"\ncommand timed out after {timeout}s")
        if check and proc.returncode != 0:
            raise RunnerError(
                f"{self.role} command failed rc={proc.returncode}: {script}\n"
                f"stdout={proc.stdout[-2000:]}\nstderr={proc.stderr[-2000:]}"
            )
        return proc

    def put_text_b64(self, remote_path: str, content: str) -> None:
        payload = base64.b64encode(content.encode("utf-8")).decode("ascii")
        script = (
            f"mkdir -p {shlex.quote(str(Path(remote_path).parent))}\n"
            f"base64 -d > {shlex.quote(remote_path)} <<'OWNERFS_W2_B64'\n"
            f"{payload}\n"
            "OWNERFS_W2_B64\n"
            f"chmod +x {shlex.quote(remote_path)}"
        )
        self.run(script, timeout=20, check=True)


def shell_json(role: RemoteShell, command: list[str], *, timeout: float) -> dict[str, Any]:
    quoted = " ".join(shlex.quote(part) for part in command)
    proc = role.run(quoted, timeout=timeout, check=True)
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RunnerError(f"{role.role} returned non-JSON for {quoted}: {proc.stdout!r}") from exc


def remote_findmnt(role: RemoteShell, path: Path) -> dict[str, Any]:
    proc = role.run(
        f"findmnt -T {shlex.quote(str(path))} -J -o TARGET,SOURCE,FSTYPE,OPTIONS",
        timeout=10,
        check=True,
    )
    payload = json.loads(proc.stdout)
    filesystems = payload.get("filesystems") or []
    if not filesystems:
        raise RunnerError(f"{role.role}: no findmnt filesystem for {path}")
    item = filesystems[0]
    return {
        "target": item.get("target", ""),
        "source": item.get("source", ""),
        "fstype": item.get("fstype", ""),
        "options": item.get("options", ""),
    }


def remote_realpath(role: RemoteShell, path: Path) -> Path:
    proc = role.run(f"realpath -m {shlex.quote(str(path))}", timeout=10, check=True)
    return Path(proc.stdout.strip())


def remote_is_dir(role: RemoteShell, path: Path) -> bool:
    return role.run(f"test -d {shlex.quote(str(path))}", timeout=10).returncode == 0


def remote_mountpoint(role: RemoteShell, path: Path) -> bool:
    return role.run(f"mountpoint -q {shlex.quote(str(path))}", timeout=10).returncode == 0


def validate_ownerfs_workspace(role: RemoteShell, path: Path, expected_workspace: str | None) -> dict[str, Any]:
    if not remote_is_dir(role, path):
        raise ValueError(f"{role.role}: missing ownerfs workspace root: {path}")
    resolved = remote_realpath(role, path)
    mount = remote_findmnt(role, path)
    target = Path(mount["target"])
    try:
        relative = resolved.relative_to(target)
    except ValueError as exc:
        raise ValueError(f"{role.role}: ownerfs path {path} is not under mount target {target}") from exc
    parts = relative.parts
    if not parts or parts[0] != "ownerfs":
        raise ValueError(
            f"{role.role}: ownerfs W2 root must be inside AFS /ownerfs namespace; "
            f"path={path}, mount_target={target}, relative={relative}"
        )
    if len(parts) < 2:
        raise ValueError(f"{role.role}: ownerfs W2 root must be fixed workspace <mount>/ownerfs/<workspace>, not namespace directory: {path}")
    workspace = parts[1]
    if expected_workspace and workspace != expected_workspace:
        raise ValueError(f"{role.role}: ownerfs workspace mismatch: expected {expected_workspace!r}, got {workspace!r}")
    if not mount["fstype"].startswith("fuse"):
        raise ValueError(f"{role.role}: ownerfs W2 root must resolve to AFS FUSE mount, not {mount['fstype'] or 'unknown'}: {mount}")
    if remote_mountpoint(role, path):
        raise ValueError(f"{role.role}: ownerfs workspace must be inside the AFS mount, not a direct mountpoint: {path}")
    return {"path": str(path), "resolved": str(resolved), "findmnt": mount, "namespace_relative": str(relative), "workspace": workspace}


def validate_moosefs_root(role: RemoteShell, path: Path) -> dict[str, Any]:
    if not remote_is_dir(role, path):
        raise ValueError(f"{role.role}: missing moosefs root: {path}")
    if not remote_mountpoint(role, path):
        raise ValueError(f"{role.role}: moosefs root must be a direct mountpoint: {path}")
    return {"path": str(path), "resolved": str(remote_realpath(role, path)), "findmnt": remote_findmnt(role, path), "mountpoint": True}


def validate_roots(a: RemoteShell, b: RemoteShell, roots: dict[str, dict[str, Path]], workspace: str | None) -> dict[str, Any]:
    # Keep validation intentionally remote: local realpath/mountpoint would inspect the runner host, not the lab node.
    return {
        "A": {
            "ownerfs": validate_ownerfs_workspace(a, roots["ownerfs"]["A"], workspace),
            "moosefs": validate_moosefs_root(a, roots["moosefs"]["A"]),
        },
        "B": {
            "ownerfs": validate_ownerfs_workspace(b, roots["ownerfs"]["B"], workspace),
            "moosefs": validate_moosefs_root(b, roots["moosefs"]["B"]),
        },
    }


def rotate_order(round_number: int) -> list[str]:
    order = list(BACKENDS)
    if round_number % 2 == 0:
        order.reverse()
    return order


def summarize_backend(cases: list[dict[str, Any]], backend: str) -> dict[str, Any]:
    totals = [row["cases"][backend]["total_wall_us"] for row in cases]
    stage_samples: dict[str, list[float]] = {stage: [] for stage in STAGE_NAMES}
    stage_wall: dict[str, list[float]] = {stage: [] for stage in STAGE_NAMES}
    for row in cases:
        for stage in STAGE_NAMES:
            item = row["cases"][backend]["stages"][stage]
            stage_wall[stage].append(float(item["phase_ledger"]["wall_us"]))
            stage_samples[stage].extend(float(v) for v in item["operation"]["samples_us"])
    return {
        "total_wall_us": {
            "samples_us": totals,
            "p50_us": nearest_rank(totals, 0.50),
            "p95_us": nearest_rank(totals, 0.95),
            "p99_us": nearest_rank(totals, 0.99),
        },
        "stage_wall_us": {
            stage: {
                "samples_us": values,
                "p50_us": nearest_rank(values, 0.50),
                "p95_us": nearest_rank(values, 0.95),
            }
            for stage, values in stage_wall.items()
        },
        "per_file_operation_us": {
            stage: {
                "sample_count": len(values),
                "p50_us": nearest_rank(values, 0.50),
                "p95_us": nearest_rank(values, 0.95),
                "samples_us": values,
            }
            for stage, values in stage_samples.items()
        },
    }


def run_stage(role: RemoteShell, script: str, action: str, root: Path, seed: int, *, count: int, size: int, timeout: float, **options: str) -> dict[str, Any]:
    command = ["python3", script, action, "--root", str(root), "--seed", str(seed), "--count", str(count), "--size", str(size)]
    for key, value in options.items():
        command.extend(["--" + key.replace("_", "-"), value])
    return shell_json(role, command, timeout=timeout)


def run_round(a: RemoteShell, b: RemoteShell, script: str, roots: dict[str, dict[str, Path]], round_number: int, seed: int, count: int, size: int, timeout: float, workers: int) -> dict[str, Any]:
    order = rotate_order(round_number)
    cases: dict[str, Any] = {}
    for backend in order:
        prepared = run_stage(a, script, "prepare", roots[backend]["A"], seed, count=count, size=size, timeout=timeout)
        first = run_stage(b, script, "read", roots[backend]["B"], seed, count=count, size=size, timeout=timeout, version="original", operation="first_read", workers=str(workers))
        repeat = run_stage(b, script, "read", roots[backend]["B"], seed, count=count, size=size, timeout=timeout, version="original", operation="repeat_read", workers=str(workers))
        write = run_stage(b, script, "overwrite", roots[backend]["B"], seed, count=count, size=size, timeout=timeout, operation="remote_overwrite", workers=str(workers))
        owner = run_stage(a, script, "read", roots[backend]["A"], seed, count=count, size=size, timeout=timeout, version="updated", operation="owner_read_after")
        cleanup = run_stage(a, script, "cleanup", roots[backend]["A"], seed, count=count, size=size, timeout=timeout)
        stages = {"first_read": first, "repeat_read": repeat, "remote_overwrite": write, "owner_read_after": owner}
        if not all(item.get("correctness") for item in [prepared, *stages.values(), cleanup]):
            raise RunnerError(f"W2 correctness failed: backend={backend}, round={round_number}")
        cases[backend] = {
            "total_wall_us": sum(float(item["phase_ledger"]["wall_us"]) for item in stages.values()),
            "prepare": prepared,
            "stages": stages,
            "cleanup": cleanup,
        }
    return {"round": round_number, "order": order, "seed": seed, "cases": cases}


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-a", default="localhost", help="node A host; local by default")
    parser.add_argument("--host-b", required=True, help="node B host")
    parser.add_argument("--ssh-user")
    parser.add_argument("--ssh-option", action="append", default=[])
    parser.add_argument("--ownerfs-a", type=Path, required=True, help="node A fixed workspace: <afs-mount>/ownerfs/<workspace>")
    parser.add_argument("--ownerfs-b", type=Path, required=True, help="node B fixed workspace for the same root")
    parser.add_argument("--ownerfs-workspace", help="expected workspace name under /ownerfs on both nodes")
    parser.add_argument("--moosefs-a", type=Path, required=True, help="node A MooseFS mount root")
    parser.add_argument("--moosefs-b", type=Path, required=True, help="node B MooseFS mount root")
    parser.add_argument("--ownerfs-binary", type=Path, required=True, help="afs-node or package binary used for OwnerFs hashing")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--remote-work-dir", default="/tmp/ownerfs-w2-diagnostics", help="where to stage workload scripts on A/B")
    parser.add_argument("--rounds", type=int, default=6)
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--size", type=int, default=4096)
    parser.add_argument("--workers", type=int, default=1, help="independent B-side file operations in flight; default preserves sequential W2")
    parser.add_argument("--seed", type=int, default=8300)
    parser.add_argument("--stage-timeout", type=float, default=180.0)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if platform.system() != "Linux":
        raise SystemExit("Linux required")
    if args.rounds < 1:
        raise SystemExit("--rounds must be >= 1")
    if args.count < 1:
        raise SystemExit("--count must be >= 1")
    if args.size < 1:
        raise SystemExit("--size must be >= 1")
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")
    if args.output.exists():
        raise SystemExit(f"refusing to overwrite output: {args.output}")
    if not args.ownerfs_binary.exists():
        raise SystemExit(f"missing --ownerfs-binary: {args.ownerfs_binary}")

    a = RemoteShell("A", args.host_a, args.ssh_user, args.ssh_option)
    b = RemoteShell("B", args.host_b, args.ssh_user, args.ssh_option)
    roots = {
        "ownerfs": {"A": args.ownerfs_a, "B": args.ownerfs_b},
        "moosefs": {"A": args.moosefs_a, "B": args.moosefs_b},
    }
    script_path = Path(__file__).with_name("s5_distributed_workload.py")
    posix_path = Path(__file__).with_name("s5_posix_workload.py")
    remote_script = str(Path(args.remote_work_dir) / f"s5_distributed_workload-{os.getpid()}.py")
    remote_posix = str(Path(args.remote_work_dir) / "s5_posix_workload.py")

    args.output.mkdir(parents=True)
    result: dict[str, Any] = {
        "schema": SCHEMA,
        "status": "IN_PROGRESS",
        "timing_contract": TIMING_CONTRACT,
        "scope": "OwnerFs cross-node W2 staged diagnostic; no W2 value threshold is applied",
        "hosts": {"A": args.host_a, "B": args.host_b},
        "params": {"rounds": args.rounds, "count": args.count, "size": args.size, "seed": args.seed, "workers": args.workers},
        "sha256": {
            "runner": digest(Path(__file__)),
            "distributed_workload": digest(script_path),
            "posix_workload": digest(posix_path),
            "ownerfs_binary": digest(args.ownerfs_binary),
        },
        "root_validation": {},
        "rounds": [],
        "summary": {},
        "receipt": {"started_unix_ms": int(time.time() * 1000), "pid": os.getpid(), "argv": sys.argv},
    }
    try:
        result["root_validation"] = validate_roots(a, b, roots, args.ownerfs_workspace)
        distributed_text = script_path.read_text(encoding="utf-8")
        posix_text = posix_path.read_text(encoding="utf-8")
        for role in (a, b):
            role.put_text_b64(remote_posix, posix_text)
            role.put_text_b64(remote_script, distributed_text)
        rows = []
        for round_number in range(1, args.rounds + 1):
            round_seed = args.seed + round_number
            row = run_round(a, b, remote_script, roots, round_number, round_seed, args.count, args.size, args.stage_timeout, args.workers)
            rows.append(row)
            result["rounds"].append(row)
            (args.output / f"round-{round_number}.json").write_text(json.dumps(row, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(
                json.dumps(
                    {"round": round_number, "order": row["order"], "total_wall_us": {name: row["cases"][name]["total_wall_us"] for name in row["order"]}},
                    sort_keys=True,
                ),
                flush=True,
            )
        ownerfs = summarize_backend(rows, "ownerfs")
        moosefs = summarize_backend(rows, "moosefs")
        ratio = ownerfs["total_wall_us"]["p50_us"] / moosefs["total_wall_us"]["p50_us"]
        result["summary"] = {"ownerfs": ownerfs, "moosefs": moosefs, "ownerfs_to_moosefs_p50_total_wall": ratio}
        result["status"] = "PASS"
    except Exception as error:
        result["status"] = "ERROR"
        result["error"] = str(error)
        raise
    finally:
        result["receipt"]["finished_unix_ms"] = int(time.time() * 1000)
        (args.output / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "ratio": result.get("summary", {}).get("ownerfs_to_moosefs_p50_total_wall")}, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
