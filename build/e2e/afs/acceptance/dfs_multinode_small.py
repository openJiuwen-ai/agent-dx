#!/usr/bin/env python3
"""Thin DFS R3 multi-node cohort driver for G2.24.

One invocation is one cohort only: three READY events, three START commands,
three C_DONE events, then three FINAL events.  It records a bounded observation;
it does not claim 3FS parity, cache residency, percentiles, or five-round
performance qualification.
"""
from __future__ import annotations

import argparse
import ctypes
import json
import sys
import time
from pathlib import Path

import dfs_manyread_small as sync
import dfs_r3_small as base
import dfs_r3_write_small as write_small

MEMBERS = ("A", "B", "C")
ROLE_INDEX = {name: index for index, name in enumerate(MEMBERS)}
ROUTE_SOURCES = {
    "r1": {"A": "B", "B": "C", "C": "A"},
    "r2": {"A": "C", "B": "A", "C": "B"},
}
SESSION_PROTOCOL = "afs.dfs_multinode_small.v1"
LOWER_CLOCK_FACTOR = 0.99
LOWER_CLOCK_FLOOR_NS = 1_000_000
CLOCK_MONOTONIC_RAW = 4


def require(ok: bool, reason: str) -> None:
    if not ok:
        raise ValueError(reason)


def member_index(member: str) -> int:
    require(member in ROLE_INDEX, f"unknown member: {member!r}")
    return ROLE_INDEX[member]


def write_index(batch: int, member: str) -> int:
    require(type(batch) is int and batch in (0, 1), "batch must be integer 0 or 1")
    return 3 * batch + member_index(member)


def write_tuple(batch: int, member: str) -> dict[str, object]:
    index = write_index(batch, member)
    return {
        "batch": batch,
        "member": member,
        "index": index,
        "generation": index + 1,
        "measured": batch == 1,
        "relative_path": write_small.relative_path(index),
    }


def read_source(route: str, reader: str) -> str:
    route = normalize_route(route)
    require(route in ROUTE_SOURCES, f"unknown read route: {route!r}")
    require(reader in ROLE_INDEX, f"unknown reader: {reader!r}")
    return ROUTE_SOURCES[route][reader]


def read_tuple(batch: int, route: str, reader: str) -> dict[str, object]:
    route = normalize_route(route)
    source = read_source(route, reader)
    source_row = write_tuple(batch, source)
    row = dict(source_row, member=reader, route=route, reader=reader, source_member=source)
    return row


def normalize_route(route: str | None) -> str | None:
    if route in ("1", "2"):
        return "r" + route
    return route


def clock_info() -> dict[str, object]:
    info = time.get_clock_info("monotonic")
    return {key: getattr(info, key) for key in ("implementation", "monotonic", "adjustable", "resolution")}


class Timespec(ctypes.Structure):
    _fields_ = [("tv_sec", ctypes.c_long), ("tv_nsec", ctypes.c_long)]


def monotonic_raw_ns() -> int | None:
    libc = ctypes.CDLL(None, use_errno=True)
    value = Timespec()
    if libc.clock_gettime(CLOCK_MONOTONIC_RAW, ctypes.byref(value)) != 0:
        return None
    return int(value.tv_sec) * 1_000_000_000 + int(value.tv_nsec)


def emit(event: dict[str, object]) -> None:
    sync.emit_json_line(sys.stdout, event)


def read_control(timeout: float) -> dict[str, object]:
    return sync.read_json_line(sync.JsonLineReader(sys.stdin), timeout)


def validate_identity(value: object, expected: object | None = None) -> dict[str, object]:
    base.validate_identity(value, expected)
    require(isinstance(value, dict), "identity must be object")
    return value


def prepare_common(args: argparse.Namespace) -> tuple[Path, Path, Path, dict[str, object], dict[str, object]]:
    sync.verify_linux_aarch64_root()
    output = sync.require_absolute_path(args.output, "output")
    sync.validate_output_path(output)
    output.mkdir(mode=0o700)
    root = sync.require_absolute_path(args.dfs_root, "dfs-root")
    sync.require_existing_directory(root, "dfs-root")
    mount = sync.find_mount(root)
    sync.require_dfs_mount(mount, root)
    candidate = sync.read_json(sync.require_absolute_path(args.candidate, "candidate")) if args.candidate else None
    identity = validate_identity(sync.read_json(Path(args.identity)), candidate)
    tool = sync.require_absolute_path(args.io_tool, "io-tool")
    require(tool.is_file() and not tool.is_symlink(), "probe must be an actual file")
    require(sync.sha256_file(tool) == identity["io_sha256"], "probe identity mismatch")
    require(not (root / write_small.RELATIVE).is_symlink(), "payload directory is a symlink")
    (root / write_small.RELATIVE).mkdir(mode=0o755, exist_ok=True)
    return output, root, tool, identity, {"mount": mount, "dfs_root": sync.stat_identity(root), "tool": sync.stat_identity(tool)}


def validate_start(event: dict[str, object], args: argparse.Namespace, cohort: dict[str, object]) -> str:
    require(event.get("event") == "START", "expected START")
    for key in ("session_token", "operation", "batch", "cohort_token"):
        require(event.get(key) == cohort[key], f"START {key} mismatch")
    require(event.get("member", event.get("reader_id")) == cohort["member"], "START member mismatch")
    if args.operation == "read":
        require(event.get("route") == cohort["route"], "START route mismatch")
        require(event.get("source_member") == cohort["source_member"], "START source mismatch")
    token = event.get("start_token")
    require(isinstance(token, str) and token, "missing start token")
    return token


def run_timed(tool: Path, payload: Path, operation: str, generation: int, timeout: float) -> dict[str, object]:
    before = time.monotonic_ns()
    raw_before = monotonic_raw_ns()
    sample = write_small.run_sample(tool, payload, operation, generation, timeout)
    after = time.monotonic_ns()
    raw_after = monotonic_raw_ns()
    sample.update({
        "py_monotonic_before_ns": before,
        "py_monotonic_after_ns": after,
        "py_monotonic_elapsed_ns": after - before,
        "py_monotonic_raw_before_ns": raw_before,
        "py_monotonic_raw_after_ns": raw_after,
        "py_monotonic_raw_elapsed_ns": raw_after - raw_before if raw_before is not None and raw_after is not None else None,
        "clock_info": clock_info(),
    })
    return sample


def validate_single_write_manifest(path: str, identity: dict[str, object], expected: dict[str, object]) -> dict[str, object]:
    manifest = sync.read_json(sync.require_absolute_path(path, "write-manifest"))
    require(isinstance(manifest, dict), "write manifest must be object")
    require(manifest.get("event") == "FINAL" or manifest.get("role") == "worker", "write manifest must be worker FINAL/summary")
    require(manifest.get("status") == "DATA_RECORDED", "write manifest status invalid")
    require(manifest.get("operation") == "write", "manifest operation must be write")
    require(manifest.get("member") == expected["source_member"], "manifest writer/source mismatch")
    for key in ("batch", "index", "generation"):
        require(type(manifest.get(key)) is int and manifest.get(key) == expected[key], f"manifest {key} mismatch")
    require(isinstance(manifest.get("relative_path"), str) and manifest.get("relative_path") == expected["relative_path"],
            "manifest relative_path mismatch")
    require(type(manifest.get("measured")) is bool and manifest.get("measured") == expected["measured"],
            "manifest measured mismatch")
    require(manifest.get("identity") == identity, "read identity mismatch")
    require(manifest.get("content") == write_small.expected_content(int(expected["generation"])), "manifest content mismatch")
    write_small.validate_sample(manifest.get("sample", {}).get("result", {}), "write", int(expected["generation"]))
    write_small.validate_content_verify(manifest.get("content_verify", {}), manifest["content"])
    require(manifest.get("parent_dir_fsync") is True and manifest.get("root_dir_fsync") is True,
            "manifest missing parent/root fsync proof")
    return {"record": manifest, "path": str(path)}


def worker(args: argparse.Namespace) -> dict[str, object]:
    output = None
    result: dict[str, object] = {"role": "worker", "status": "BLOCKED", "operation": args.operation,
                                 "member": args.member, "session_token": args.session_token}
    try:
        output, root, tool, identity, fs_identity = prepare_common(args)
        cohort = write_tuple(args.batch, args.member) if args.operation == "write" else read_tuple(args.batch, args.route, args.member)
        cohort.update({"operation": args.operation, "session_token": args.session_token,
                       "cohort_token": args.cohort_token})
        if args.operation == "read":
            manifest = validate_single_write_manifest(args.manifest, identity, cohort)
            source = manifest["record"]
            content = source["content"]
            result["write_manifest"] = {"path": manifest["path"], "source_member": cohort["source_member"],
                                        "index": cohort["index"], "generation": cohort["generation"]}
        else:
            content = write_small.expected_content(cohort["generation"])
        payload = root / str(cohort["relative_path"])
        require(not payload.is_symlink(), "payload is a symlink")
        pre_read_content_verify = None
        if args.operation == "read":
            pre_read_content_verify = write_small.verify_content(payload, content)
        ready = {"event": "READY", **cohort, "reader_id": args.member, "protocol": SESSION_PROTOCOL, "identity": identity,
                 "fs": fs_identity, "pid": sync.os.getpid(), "clock_info": clock_info()}
        emit(ready)
        start_token = validate_start(read_control(args.round_timeout), args, cohort)
        sample = run_timed(tool, payload, args.operation, int(cohort["generation"]), args.round_timeout)
        result.update(cohort, identity=identity, fs=fs_identity, start_token=start_token, sample=sample)
        sync.write_json(output / "probe-sample.json", sample)
        c_done = {"event": "C_DONE", **cohort, "reader_id": args.member, "start_token": start_token, "status": sample.get("status"),
                  "identity": identity,
                  "rc": sample.get("rc"), "result": sample.get("result"),
                  "py_monotonic_elapsed_ns": sample["py_monotonic_elapsed_ns"],
                  "py_monotonic_raw_elapsed_ns": sample["py_monotonic_raw_elapsed_ns"],
                  "clock_info": sample["clock_info"]}
        emit(c_done)
        require(sample["status"] == "PASS", "C probe failed")
        if args.operation == "write":
            sync.fsync_directory(payload.parent)
            sync.fsync_directory(root)
        post_content_verify = write_small.verify_content(payload, content)
        final = {"event": "FINAL", **cohort, "reader_id": args.member, "start_token": start_token, "status": "DATA_RECORDED",
                 "sample": sample, "content": content, "content_verify": post_content_verify,
                 "identity": identity, "fs": fs_identity, "parent_dir_fsync": args.operation == "write",
                 "root_dir_fsync": args.operation == "write",
                 "pre_read_content_verify": pre_read_content_verify,
                 "source_member": cohort.get("source_member")}
        result.update(final)
        sync.write_json(output / "summary.json", result)
        emit(final)
    except Exception as error:
        result["error"] = repr(error)
        if output is not None:
            sync.write_json(output / "summary.json", result)
        emit({"event": "FINAL", "operation": args.operation, "member": args.member,
              "session_token": args.session_token, "cohort_token": args.cohort_token,
              "status": "BLOCKED", "error": repr(error)})
    return result


def expected_tuple(args: argparse.Namespace, member: str) -> dict[str, object]:
    return write_tuple(args.batch, member) if args.operation == "write" else read_tuple(args.batch, args.route, member)


def require_event(event: dict[str, object], name: str, args: argparse.Namespace, seen: set[str] | None = None) -> dict[str, object]:
    require(event.get("event") == name, f"expected {name}, got {event.get('event')!r}")
    require(event.get("session_token") == args.session_token, "session mismatch")
    require(event.get("cohort_token") == args.cohort_token, "cohort mismatch")
    require(event.get("operation") == args.operation, "operation mismatch")
    require(type(event.get("batch")) is int and event.get("batch") == args.batch, "batch mismatch")
    member = event.get("member", event.get("reader_id"))
    require(member in ROLE_INDEX, "unexpected member")
    event["member"] = member
    expected = expected_tuple(args, member)
    for key in ("index", "generation"):
        require(type(event.get(key)) is int and event.get(key) == expected[key], f"{name} {key} mismatch")
    require(isinstance(event.get("relative_path"), str) and event.get("relative_path") == expected["relative_path"],
            f"{name} relative_path mismatch")
    require(type(event.get("measured")) is bool and event.get("measured") == expected["measured"],
            f"{name} measured mismatch")
    require(event.get("member", member) == expected["member"], f"{name} member mismatch")
    if seen is not None:
        require(member not in seen, f"duplicate {name} member")
        seen.add(member)
    if args.operation == "read":
        for key in ("route", "reader", "source_member"):
            require(event.get(key) == expected[key], f"{name} {key} mismatch")
    return event


def validate_c_done_event(event: dict[str, object], expected_start_token: str | None = None) -> None:
    require(event.get("status") == "PASS" and event.get("rc") == 0, "C_DONE probe did not pass")
    if expected_start_token is not None:
        require(event.get("start_token") == expected_start_token, "C_DONE start token mismatch")
    operation = event.get("operation")
    barrier = "fdatasync" if operation == "write" else "close"
    write_small.validate_sample(event.get("result", {}), str(operation), int(event.get("generation")))
    require(event["result"].get("barrier") == barrier, "C_DONE barrier mismatch")
    require(isinstance(event.get("identity"), dict), "C_DONE missing identity")


def validate_final_event(final: dict[str, object], ready: dict[str, object], c_done: dict[str, object], args: argparse.Namespace) -> None:
    require(final.get("status") == "DATA_RECORDED", "worker FINAL failed")
    require(final.get("start_token") == c_done.get("start_token"), "FINAL start token mismatch")
    require(final.get("identity") == ready.get("identity") == c_done.get("identity"), "identity mismatch")
    content = final.get("content")
    require(content == write_small.expected_content(int(final["generation"])), "FINAL content oracle mismatch")
    write_small.validate_content_verify(final.get("content_verify", {}), content)
    sample = final.get("sample")
    require(isinstance(sample, dict), "FINAL missing sample")
    require(sample.get("result") == c_done.get("result"), "FINAL/C_DONE result mismatch")
    if args.operation == "write":
        require(final.get("parent_dir_fsync") is True, "write FINAL missing directory fsync")
        require(final.get("root_dir_fsync") is True, "write FINAL missing root fsync")
    else:
        require(final.get("source_member") == expected_tuple(args, final["member"])["source_member"], "read FINAL source mismatch")
        require(isinstance(final.get("pre_read_content_verify"), dict), "read FINAL missing content verification")
        write_small.validate_content_verify(final.get("pre_read_content_verify", {}), content)


def conservative_window(done_events: list[dict[str, object]]) -> dict[str, object]:
    rows = []
    inner_starts = []
    inner_ends = []
    for event in done_events:
        result = event.get("result")
        require(isinstance(result, dict), "C_DONE missing result")
        c_wall = result.get("wall_ns")
        mono_elapsed = event.get("py_monotonic_elapsed_ns")
        raw_elapsed = event.get("py_monotonic_raw_elapsed_ns")
        lower_ctl_l = event.get("ctl_start_send_before_ns")
        upper_ctl_u = event.get("ctl_c_done_receive_ns")
        ctl_raw_l = event.get("ctl_start_send_raw_before_ns")
        ctl_raw_u = event.get("ctl_c_done_receive_raw_ns")
        require(type(c_wall) is int and c_wall > 0, "missing C wall time")
        require(type(mono_elapsed) is int and mono_elapsed > 0, "missing MONOTONIC elapsed")
        lower = max(0, int(c_wall * LOWER_CLOCK_FACTOR) - LOWER_CLOCK_FLOOR_NS)
        if type(lower_ctl_l) is int and type(upper_ctl_u) is int:
            inner_starts.append(upper_ctl_u - lower)
            inner_ends.append(lower_ctl_l + lower)
        ratio = mono_elapsed / raw_elapsed if type(raw_elapsed) is int and raw_elapsed > 0 else None
        ctl_ratio = ((upper_ctl_u - lower_ctl_l) / (ctl_raw_u - ctl_raw_l)
                     if type(lower_ctl_l) is int and type(upper_ctl_u) is int and
                     type(ctl_raw_l) is int and type(ctl_raw_u) is int and ctl_raw_u > ctl_raw_l else None)
        rows.append({"member": event["member"], "c_wall_ns": c_wall, "monotonic_elapsed_ns": mono_elapsed,
                     "monotonic_raw_elapsed_ns": raw_elapsed,
                     "ctl_monotonic_elapsed_ns": upper_ctl_u - lower_ctl_l if type(lower_ctl_l) is int and type(upper_ctl_u) is int else None,
                     "ctl_monotonic_raw_elapsed_ns": ctl_raw_u - ctl_raw_l if type(ctl_raw_l) is int and type(ctl_raw_u) is int else None,
                     "duration_lower_bound_ns": lower,
                     "worker_monotonic_to_raw_ratio": ratio,
                     "ctl_monotonic_to_raw_ratio": ctl_ratio})
    complete = len(rows) == 3 and len(inner_starts) == 3 and len(inner_ends) == 3
    common_inner_lower = max(0, min(inner_ends) - max(inner_starts)) if complete else 0
    observed = complete and all(row["worker_monotonic_to_raw_ratio"] is not None and
                                row["ctl_monotonic_to_raw_ratio"] is not None for row in rows)
    rate_ok = observed and all(0.99 <= row["worker_monotonic_to_raw_ratio"] <= 1.01 and
                               0.99 <= row["ctl_monotonic_to_raw_ratio"] <= 1.01 for row in rows)
    proved = bool(common_inner_lower > 0 and rate_ok and complete)
    status = "PROVED_CONDITIONAL" if proved else "OBSERVED_NOT_PROVED"
    return {"status": status, "observed": observed, "proved_overlap": proved,
            "rate_observation_ok": rate_ok, "common_inner_lower_bound_ns": common_inner_lower,
            "proved_common_inner_overlap": proved, "declared_rate_condition": "MONOTONIC and CLOCK_MONOTONIC_RAW elapsed ratio must be within 1%",
            "method": "per participant inner interval is [ctl_C_DONE_receive - duration_lower, ctl_START_send + duration_lower]; cross-VM clock origins are not compared",
            "members": rows}


def coordinator(args: argparse.Namespace) -> dict[str, object]:
    output = sync.require_absolute_path(args.output, "output")
    sync.validate_output_path(output)
    output.mkdir(mode=0o700)
    result: dict[str, object] = {"role": "coordinator", "status": "BLOCKED", "session_token": args.session_token,
                                 "cohort_token": args.cohort_token, "operation": args.operation,
                                 "batch": args.batch, "route": args.route, "clock_info": clock_info()}
    raw_events: list[dict[str, object]] = []
    reader = sync.JsonLineReader(sys.stdin)
    try:
        ready_seen: set[str] = set()
        ready = []
        ready_by_member = {}
        while len(ready_seen) < 3:
            event = sync.read_json_line(reader, args.round_timeout)
            raw_events.append(event)
            item = require_event(event, "READY", args, ready_seen)
            ready.append(item)
            ready_by_member[item["member"]] = item
        start_token = f"{args.session_token}:{args.cohort_token}:start"
        start_send_before_ns = time.monotonic_ns()
        start_send_raw_before_ns = monotonic_raw_ns()
        for member in MEMBERS:
            command = {"event": "START", "member": member, "session_token": args.session_token,
                       "reader_id": member,
                       "cohort_token": args.cohort_token, "operation": args.operation, "batch": args.batch,
                       "start_token": start_token}
            if args.operation == "read":
                command.update({"route": args.route, "source_member": read_source(args.route, member)})
            emit(command)
        c_done_by_member = {}
        final_by_member = {}
        c_done = []
        final = []
        while len(final_by_member) < 3:
            event = sync.read_json_line(reader, args.round_timeout)
            raw_events.append(event)
            if event.get("event") == "C_DONE":
                item = require_event(event, "C_DONE", args)
                member = item["member"]
                require(member not in c_done_by_member, "duplicate C_DONE member")
                item["ctl_start_send_before_ns"] = start_send_before_ns
                item["ctl_start_send_raw_before_ns"] = start_send_raw_before_ns
                item["ctl_c_done_receive_ns"] = time.monotonic_ns()
                item["ctl_c_done_receive_raw_ns"] = monotonic_raw_ns()
                validate_c_done_event(item, start_token)
                require(item.get("identity") == ready_by_member[member].get("identity"), "C_DONE identity mismatch")
                c_done_by_member[member] = item
                c_done.append(item)
            elif event.get("event") == "FINAL":
                item = require_event(event, "FINAL", args)
                member = item["member"]
                require(member in c_done_by_member, "FINAL before matching C_DONE")
                require(member not in final_by_member, "duplicate FINAL member")
                validate_final_event(item, ready_by_member[member], c_done_by_member[member], args)
                final_by_member[member] = item
                final.append(item)
            else:
                raise ValueError(f"expected C_DONE or FINAL, got {event.get('event')!r}")
        c_done_receive_after_ns = max(item["ctl_c_done_receive_ns"] for item in c_done)
        result.update({"status": "DATA_RECORDED", "ready": ready, "c_done": c_done, "final": final,
                       "ctl_start_send_before_ns": start_send_before_ns,
                       "ctl_start_send_raw_before_ns": start_send_raw_before_ns,
                       "ctl_c_done_receive_after_ns": c_done_receive_after_ns,
                       "ctl_elapsed_start_to_c_done_ns": c_done_receive_after_ns - start_send_before_ns,
                       "overlap_diagnostic": conservative_window(c_done),
                       "qualified_threefs_parity": False, "latency_percentile_claim": False})
        emit({"event": "SUMMARY", "session_token": args.session_token, "cohort_token": args.cohort_token,
              "status": result["status"], "overlap": result["overlap_diagnostic"]})
    except Exception as error:
        result["error"] = repr(error)
        emit({"event": "SUMMARY", "session_token": args.session_token, "cohort_token": args.cohort_token,
              "status": "BLOCKED", "error": repr(error)})
    finally:
        sync.write_json(output / "events.json", raw_events)
        sync.write_json(output / "summary.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="role", required=True)
    for role in ("worker", "coordinator"):
        item = sub.add_parser(role)
        item.add_argument("--operation", choices=("write", "read"), required=True)
        item.add_argument("--batch", type=int, choices=(0, 1), required=True)
        item.add_argument("--route", choices=("1", "2", "r1", "r2"))
        item.add_argument("--session-token", required=True)
        item.add_argument("--cohort-token", required=True)
        item.add_argument("--round-timeout", type=float, default=90.0)
        item.add_argument("--output", required=True)
    sub.choices["worker"].add_argument("--member", "--node", dest="member", choices=MEMBERS, required=True)
    for name in ("dfs-root", "io-tool", "identity"):
        sub.choices["worker"].add_argument("--" + name, required=True)
    sub.choices["worker"].add_argument("--candidate")
    sub.choices["worker"].add_argument("--manifest", "--write-manifest", dest="manifest")
    args = parser.parse_args()
    args.route = normalize_route(args.route)
    if args.operation == "read":
        require(args.route in ROUTE_SOURCES, "read requires --route")
        if args.role == "worker":
            require(args.manifest, "read worker requires --manifest")
    if args.operation == "write":
        require(args.route is None, "write does not take --route")
    result = worker(args) if args.role == "worker" else coordinator(args)
    print(json.dumps(result, indent=2, sort_keys=True), file=sys.stderr)
    return 0 if result.get("status") == "DATA_RECORDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
