#!/usr/bin/env python3
"""OwnerFs W1 value acceptance with exact S5 POSIX workload semantics.

This is the OwnerFs equivalent of the old HomeFs W1 bench. It intentionally
imports the copied `s5_posix_workload.py` workload rather than reimplementing a
similar-looking microbenchmark. The acceptance contract is:

* four prepared backend roots are required: ownerfs, moosefs, thin_fuse, native_fs
* ownerfs root is a pre-created fixed workspace: <afs-fuse-mount>/ownerfs/<workspace>
* W1 workload timing contract must be `dms.s5-w1-timing.v2`
* one warmup per backend per session
* two independent sessions, six valid same-scene rounds per session
* per-round backend order rotates and reverses after the first full cycle
* per-session p50(ownerfs batch_wall_us) / p50(moosefs batch_wall_us) <= 0.80
* result JSON contains hashes and a receipt, and old/partial samples never pass
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
from typing import Any

from s5_posix_workload import nearest_rank, run_w1

SCHEMA = "afs.ownerfs-local-w1.v1"
TIMING_CONTRACT = "dms.s5-w1-timing.v2"
BACKEND_ORDER = ("ownerfs", "thin_fuse", "moosefs", "native_fs")
VALUE_RATIO_THRESHOLD = 0.80


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def mounted(path: Path) -> bool:
    return subprocess.run(["mountpoint", "-q", str(path)], check=False).returncode == 0


def find_mount_for(path: Path) -> dict[str, Any]:
    proc = subprocess.run(
        ["findmnt", "-T", str(path), "-J", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise ValueError(f"findmnt -T failed for {path}: {proc.stderr.strip()}")
    try:
        payload = json.loads(proc.stdout)
        filesystems = payload.get("filesystems") or []
        if not filesystems:
            raise ValueError("no filesystems in findmnt output")
        item = filesystems[0]
        return {
            "target": item.get("target", ""),
            "source": item.get("source", ""),
            "fstype": item.get("fstype", ""),
            "options": item.get("options", ""),
        }
    except Exception as exc:
        raise ValueError(f"cannot parse findmnt output for {path}: {proc.stdout!r}") from exc


def validate_ownerfs_root(path: Path, *, expected_workspace: str | None, location_receipt: Path | None, expected_home_node: str | None) -> dict[str, Any]:
    mount = find_mount_for(path)
    target = Path(mount["target"]).resolve()
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(target)
    except ValueError as exc:
        raise ValueError(f"ownerfs path {path} is not under resolved mount target {target}") from exc
    parts = relative.parts
    if not parts or parts[0] != "ownerfs":
        raise ValueError(
            f"ownerfs workload root must be inside the AFS /ownerfs namespace; "
            f"path={path}, mount_target={target}, relative={relative}"
        )
    if len(parts) < 2:
        raise ValueError(
            f"ownerfs workload root must be a pre-created fixed workspace under /ownerfs, "
            f"not the namespace directory itself: path={path}, relative={relative}"
        )
    workspace = parts[1]
    if expected_workspace and workspace != expected_workspace:
        raise ValueError(
            f"ownerfs workspace mismatch: expected {expected_workspace!r}, got {workspace!r}; "
            f"path={path}, relative={relative}"
        )
    fstype = mount["fstype"]
    if not fstype.startswith("fuse"):
        raise ValueError(
            f"ownerfs workload root must resolve to an AFS FUSE mount, not {fstype or 'unknown'}; "
            f"path={path}, mount={mount}"
        )
    if mounted(path):
        raise ValueError(
            f"ownerfs workload root should be a workspace directory inside the AFS mount, "
            f"not a direct mountpoint: {path}"
        )
    detail = {
        "path": str(path),
        "resolved": str(resolved),
        "mountpoint": False,
        "findmnt": mount,
        "namespace_relative": str(relative),
        "workspace": workspace,
        "workspace_relative": str(Path(*parts[2:])) if len(parts) > 2 else ".",
    }
    if location_receipt is not None:
        payload = json.loads(location_receipt.read_text(encoding="utf-8"))
        location = payload.get("location") or payload
        home = location.get("homeNodeId") or location.get("home_node_id")
        found = payload.get("found", True)
        if not found:
            raise ValueError(f"ownerfs workspace location receipt says root is missing: {location_receipt}")
        if expected_home_node and home != expected_home_node:
            raise ValueError(
                f"ownerfs workspace is not owned by expected home node {expected_home_node!r}: "
                f"home={home!r}, receipt={location_receipt}"
            )
        detail["location_receipt"] = {"path": str(location_receipt), "home_node_id": home, "found": found}
    elif expected_home_node:
        raise ValueError("--ownerfs-expected-home-node requires --ownerfs-location-receipt")
    return detail


def rotate_order(names: list[str], round_number: int) -> list[str]:
    offset = (round_number - 1) % len(names)
    order = names[offset:] + names[:offset]
    if round_number > len(names):
        order.reverse()
    return order


def validate_roots(roots: dict[str, Path], *, require_native_mount: bool, expected_ownerfs_workspace: str | None, ownerfs_location_receipt: Path | None, ownerfs_expected_home_node: str | None) -> dict[str, Any]:
    resolved = {name: path.resolve() for name, path in roots.items()}
    if len(set(resolved.values())) != len(resolved):
        duplicates: dict[str, list[str]] = {}
        for name, path in resolved.items():
            duplicates.setdefault(str(path), []).append(name)
        dup = {path: names for path, names in duplicates.items() if len(names) > 1}
        raise ValueError(f"all backend roots must be distinct; duplicates={dup}")
    detail: dict[str, Any] = {}
    for name, path in roots.items():
        if not path.is_dir():
            raise ValueError(f"missing {name} root: {path}")
        if name == "ownerfs":
            detail[name] = validate_ownerfs_root(path, expected_workspace=expected_ownerfs_workspace, location_receipt=ownerfs_location_receipt, expected_home_node=ownerfs_expected_home_node)
            continue
        is_mount = mounted(path)
        mount_info = find_mount_for(path)
        if name in {"thin_fuse", "moosefs"} and not is_mount:
            raise ValueError(f"{name} is not a direct mountpoint: {path}")
        if name == "native_fs" and require_native_mount and not is_mount:
            raise ValueError(f"native_fs is not a mountpoint: {path}")
        detail[name] = {"path": str(path), "resolved": str(resolved[name]), "mountpoint": is_mount, "findmnt": mount_info}
    return detail


def assert_valid_w1_result(name: str, result: dict[str, Any]) -> None:
    if result.get("timing_contract") != TIMING_CONTRACT:
        raise RuntimeError(f"{name} returned wrong timing contract: {result.get('timing_contract')!r}")
    if not result.get("correctness"):
        raise RuntimeError(f"{name} W1 correctness failed")
    if result.get("batch_wall_us", 0) <= 0:
        raise RuntimeError(f"{name} W1 batch_wall_us is not positive")
    business = result.get("business_batch") or {}
    if business.get("scope") != "local-owner-directory-lifecycle":
        raise RuntimeError(f"{name} W1 business scope changed: {business.get('scope')!r}")


def run_session(roots: dict[str, Path], number: int, seed: int) -> dict[str, Any]:
    names = list(BACKEND_ORDER)
    warmup: dict[str, float] = {}
    for index, name in enumerate(names):
        result = run_w1(roots[name], seed=seed + number * 1000 + index, drop_cache=lambda: None)
        assert_valid_w1_result(name, result)
        warmup[name] = float(result["batch_wall_us"])
    rounds: list[dict[str, Any]] = []
    for round_number in range(1, 7):
        order = rotate_order(names, round_number)
        cases: dict[str, Any] = {}
        # Same seed within a round keeps every backend on the same scene.
        round_seed = seed + number * 1000 + 100 + round_number
        for name in order:
            result = run_w1(roots[name], seed=round_seed, drop_cache=lambda: None)
            assert_valid_w1_result(name, result)
            cases[name] = result
        rounds.append({"round": round_number, "order": order, "seed": round_seed, "cases": cases})
        print(
            json.dumps(
                {
                    "session": number,
                    "round": round_number,
                    "order": order,
                    "batch_wall_us": {name: cases[name]["batch_wall_us"] for name in order},
                },
                sort_keys=True,
            ),
            flush=True,
        )
    samples = {name: [row["cases"][name]["batch_wall_us"] for row in rounds] for name in names}
    if any(len(values) != 6 for values in samples.values()):
        raise RuntimeError(f"invalid sample count: { {name: len(values) for name, values in samples.items()} }")
    p50 = {name: nearest_rank(values, 0.50) for name, values in samples.items()}
    p95 = {name: nearest_rank(values, 0.95) for name, values in samples.items()}
    p99 = {name: nearest_rank(values, 0.99) for name, values in samples.items()}
    ratio = p50["ownerfs"] / p50["moosefs"]
    return {
        "session": number,
        "warmup_us": warmup,
        "rounds": rounds,
        "samples_us": samples,
        "p50_us": p50,
        "p95_us": p95,
        "p99_us": p99,
        "ownerfs_to_moosefs_p50": ratio,
        "w1_value_pass": ratio <= VALUE_RATIO_THRESHOLD,
        "valid_rounds_per_backend": {name: len(samples[name]) for name in names},
    }


def environment(roots: dict[str, Path]) -> dict[str, Any]:
    findmnt = subprocess.run(
        ["findmnt", "-rn", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return {
        "uname": platform.uname()._asdict(),
        "python": sys.version,
        "cwd": str(Path.cwd()),
        "roots": {name: str(path) for name, path in roots.items()},
        "mounts": findmnt,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ownerfs", type=Path, required=True, help="prepared OwnerFs fixed workspace path: <afs-fuse-mount>/ownerfs/<workspace>")
    parser.add_argument("--ownerfs-workspace", help="expected workspace directory name under /ownerfs")
    parser.add_argument("--ownerfs-location-receipt", type=Path, help="JSON LookupRoot receipt proving the workspace was pre-created")
    parser.add_argument("--ownerfs-expected-home-node", help="expected home node id in the location receipt, usually node A")
    parser.add_argument("--moosefs", type=Path, required=True, help="prepared MooseFS root path")
    parser.add_argument("--thin-fuse", type=Path, required=True, help="prepared thin FUSE root path")
    parser.add_argument("--native", type=Path, required=True, help="prepared Native FS root path")
    parser.add_argument("--ownerfs-binary", type=Path, required=True, help="afs-node or package binary used for OwnerFs")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=6701)
    parser.add_argument("--require-native-mount", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if platform.system() != "Linux":
        raise SystemExit("Linux required")
    roots = {
        "ownerfs": args.ownerfs,
        "thin_fuse": args.thin_fuse,
        "moosefs": args.moosefs,
        "native_fs": args.native,
    }
    if args.output.exists():
        raise SystemExit(f"refusing to overwrite output: {args.output}")
    args.output.mkdir(parents=True)
    result: dict[str, Any] = {
        "schema": SCHEMA,
        "status": "IN_PROGRESS",
        "timing_contract": TIMING_CONTRACT,
        "scope": "OwnerFs local-home W1; same workload semantics as S5 HomeFs W1",
        "value_threshold": {"ownerfs_to_moosefs_p50_max": VALUE_RATIO_THRESHOLD},
        "environment": {},
        "sha256": {
            "runner": digest(Path(__file__)),
            "workload": digest(Path(__file__).with_name("s5_posix_workload.py")),
            "ownerfs_binary": digest(args.ownerfs_binary),
        },
        "backend_order": list(BACKEND_ORDER),
        "sessions": [],
        "receipt": {
            "started_unix_ms": int(time.time() * 1000),
            "pid": os.getpid(),
            "argv": sys.argv,
        },
    }
    try:
        result["environment"] = environment(roots)
        result["root_validation"] = validate_roots(roots, require_native_mount=args.require_native_mount, expected_ownerfs_workspace=args.ownerfs_workspace, ownerfs_location_receipt=args.ownerfs_location_receipt, ownerfs_expected_home_node=args.ownerfs_expected_home_node)
        for number in (1, 2):
            item = run_session(roots, number, args.seed)
            result["sessions"].append(item)
            (args.output / f"session-{number}.json").write_text(json.dumps(item, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        result["ratios"] = [item["ownerfs_to_moosefs_p50"] for item in result["sessions"]]
        result["status"] = "PASS" if all(item["w1_value_pass"] for item in result["sessions"]) else "FAIL"
    except Exception as error:
        result["status"] = "ERROR"
        result["error"] = str(error)
        raise
    finally:
        result["receipt"]["finished_unix_ms"] = int(time.time() * 1000)
        (args.output / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "ratios": result.get("ratios", [])}, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
