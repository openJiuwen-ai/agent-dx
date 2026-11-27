#!/usr/bin/env python3
"""Verify OwnerFs FabricMem on an externally staged, isolated pair of A3 mounts."""

import argparse
from decimal import Decimal
from itertools import count as poll_attempts
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
import uuid

from a3_fabricmem_worker import MIB, SIZE, WINDOW, require

COUNTS = (1, 8, 32, 64)
DRAIN_SECONDS = 45
DRAIN_ERROR = f"HIXL slots did not drain within {DRAIN_SECONDS} seconds"
POOL_BYTES = 2 * 1024 * MIB  # 4 MiB × (256 + 32 × 8) per lane
PAYLOAD = "afs_ownerfiles_payload_bytes_total"
VALIDATED = "afs_hixl_validated_read_bytes_total"
LANE_VALIDATED = "afs_hixl_lane_validated_read_bytes_total"
NATIVE_CALLS = "afs_hixl_reader_native_batch_calls_total"
NATIVE_BYTES = "afs_hixl_reader_native_batch_bytes_total"
OPERATION = "afs_hixl_operation_duration_seconds_count"
OUTCOMES = "afs_ownerfs_hixl_reads_total"
FAMILIES = {
    PAYLOAD,
    VALIDATED,
    LANE_VALIDATED,
    NATIVE_CALLS,
    NATIVE_BYTES,
    OPERATION,
    OUTCOMES,
}
SAMPLE = re.compile(r"([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)(?:\s+\S+)?")
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([a-zA-Z0-9_]+)"')
TARGET = re.compile(r"[A-Za-z0-9_.@:-]+")
WORKER = Path(__file__).with_name("a3_fabricmem_worker.py").read_text(encoding="utf-8")


def at_least_release(value, minimum):
    """Attest numeric releases; reject ambiguous package or prerelease labels."""
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{1,5}(?:\.[0-9]{1,5}){1,3}", value
    ):
        return False
    version = tuple(map(int, value.split(".")))
    return version + (0,) * (4 - len(version)) >= minimum + (0,) * (4 - len(minimum))


def save(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def drain_remaining(deadline):
    remaining = deadline - time.monotonic()
    require(remaining > 0, DRAIN_ERROR)
    return remaining


def remote(host, action, evidence, *extra, deadline=None):
    command = [
        *host.get("remote_prefix", []),
        "python3",
        "-I",
        "-",
        action,
        "--mount",
        host["mount"],
        *extra,
    ]
    argv = [
        "ssh",
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        host["ssh_target"],
        shlex.join(command),
    ]
    timeout = 20 if action == "snapshot" else 900
    worker = WORKER.encode()
    if deadline is not None:
        timeout = min(timeout, drain_remaining(deadline))
    try:
        completed = subprocess.run(
            argv,
            input=worker,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        if deadline is not None and time.monotonic() >= deadline:
            raise ValueError(DRAIN_ERROR) from error
        raise
    if completed.stderr:
        evidence.with_suffix(".stderr").write_bytes(completed.stderr)
    if completed.returncode:
        evidence.with_suffix(".stdout").write_bytes(completed.stdout)
        raise ValueError(
            f"{host['id']}: {action} failed ({completed.returncode}); see {evidence.parent}"
        )
    try:
        value = json.loads(completed.stdout)
    except ValueError:
        evidence.with_suffix(".stdout").write_bytes(completed.stdout)
        raise
    save(evidence, value)
    return value


def health(host, value):
    report = value["health"]
    checks = report["checks"]
    registration = checks["node_registration"]
    mount = checks["mounts"]["ownerfs"]
    hixl = checks["hixl"]
    require(
        report["id"] == host["id"]
        and report["status"] == "ready"
        and report["ownerfs"] is True
        and report["dfs"] is False
        and checks["meta_persistence"]["persistent_ready"] is True
        and registration["ready"] is True
        and registration["session_id"],
        "wrong Node or not fully ready",
    )
    require(
        mount["configured"] is True
        and mount["ready"] is True
        and mount["path"] == host["mount"]
        and isinstance(mount["startup_mount_id"], str)
        and value["mount"] == {"id": mount["startup_mount_id"], "ownerfs": True},
        "worker is not on the Node's active OwnerFs mount",
    )
    require(
        hixl["configured"] is True
        and hixl["ready"] is True
        and hixl["data_mode"] == "hixl"
        and hixl["startup_fallback"] is False
        and hixl["transport"] == "hccs"
        and hixl["buffer_pool"] is False,
        "expected explicit HIXL/HCCS without relay or fallback",
    )
    lanes = hixl["lanes"]
    require(
        hixl["lane_count"] == len(lanes) == len(host["device_ids"]),
        "configured lane count mismatch",
    )
    require(
        hixl["allocated_bytes"] == POOL_BYTES * len(lanes),
        "unexpected group allocation",
    )
    domain = hixl["hccs_domain_id"]
    require(type(domain) is int and domain >= 0, "missing verified HCCS domain")
    generations = []
    for lane in (hixl, *lanes):
        require(
            lane["state"] == "running"
            and lane["actual_mode"] == "hccs_fabric"
            and lane["fabric_mem"] is True
            and lane["internal_protocol_version"] == 1
            and lane["relay_hbm_budget_bytes"] == lane["quarantined_bytes"] == 0
            and lane["hccs_domain_id"] == domain
            and type(lane["engine_generation"]) is int
            and lane["engine_generation"] > 0,
            "lane/group not running Fabric v1 in the expected domain",
        )
        generations.append(lane["engine_generation"])
    for index, (lane, device) in enumerate(zip(lanes, host["device_ids"])):
        require(
            lane["lane_id"] == index
            and lane["device_id"] == device
            and lane["allocated_bytes"] == POOL_BYTES
            and lane["engine_id"]
            and lane["device_listen_port"] is None,
            "unexpected Fabric lane layout",
        )
    require(
        len({lane["engine_id"] for lane in lanes}) == len(lanes),
        "duplicate engine listener",
    )
    return (
        value["boot_id"],
        mount["startup_mount_id"],
        registration["session_id"],
        domain,
        tuple(generations),
        tuple(lane["engine_id"] for lane in lanes),
    )


def counters(text):
    results = {}
    for line in text.splitlines():
        if (
            not line
            or line.startswith("#")
            or re.split(r"[\s{]", line, 1)[0] not in FAMILIES
        ):
            continue
        match = SAMPLE.fullmatch(line)
        require(match is not None, f"malformed counter sample: {line}")
        name, raw, number = match.groups()
        labels = [LABEL.fullmatch(part) for part in raw.split(",")] if raw else []
        require(all(labels), f"malformed counter labels: {line}")
        label_map = dict(label.groups() for label in labels)
        key = name, tuple(sorted(label_map.items()))
        value = Decimal(number)
        require(
            key not in results
            and len(labels) == len(label_map)
            and value.is_finite()
            and value >= 0
            and value == value.to_integral_value(),
            f"invalid or duplicate counter: {line}",
        )
        results[key] = int(value)
    return results


def sample(values, name, *, required=False, **labels):
    key = name, tuple(sorted(labels.items()))
    if required:
        require(key in values, f"missing counter {name} {labels}")
    return values.get(key, 0)


def assess(before, after, home, reader, work, lane_count):
    read_bytes = sum(row["read_bytes"] for row in work)
    write_bytes = sum(row["write_bytes"] for row in work)
    for role in (home, reader):
        old, new = before[role], after[role]
        for key, value in old.items():
            require(
                key in new and new[key] >= value,
                f"{role}: counter disappeared or reset: {key}",
            )
    delta = {
        role: {
            key: value - before[role].get(key, 0) for key, value in after[role].items()
        }
        for role in (home, reader)
    }
    summary = {}
    for role, side, opposite in (
        (home, "server", "client"),
        (reader, "client", "server"),
    ):
        for direction in ("read", "write"):
            require(
                sample(
                    delta[role],
                    PAYLOAD,
                    side=opposite,
                    direction=direction,
                    plane="hixl",
                )
                == 0,
                f"{role}: unexpected reverse-side HIXL activity; check workspace placement and isolation",
            )
            for plane in ("grpc", "rdma"):
                for observed_side in ("client", "server"):
                    require(
                        sample(
                            delta[role],
                            PAYLOAD,
                            required=True,
                            side=observed_side,
                            direction=direction,
                            plane=plane,
                        )
                        == 0,
                        f"{role}: unexpected {observed_side}/{direction}/{plane} OwnerFiles payload",
                    )
        summary[role] = {
            "hixl_read_bytes": sample(
                delta[role],
                PAYLOAD,
                required=True,
                side=side,
                direction="read",
                plane="hixl",
            ),
            "hixl_write_bytes": sample(
                delta[role],
                PAYLOAD,
                required=True,
                side=side,
                direction="write",
                plane="hixl",
            ),
            "negotiate_ok_delta": sample(
                delta[role], OPERATION, phase="negotiate", result="ok"
            ),
            "negotiate_ok_total": sample(
                after[role], OPERATION, phase="negotiate", result="ok"
            ),
            "close_ok_delta": sample(
                delta[role], OPERATION, phase="close", result="ok"
            ),
            "release_ok_delta": sample(
                delta[role], OPERATION, phase="release", result="ok"
            ),
        }
        require(
            summary[role]["hixl_read_bytes"] >= SIZE * len(work),
            f"{role}: insufficient HIXL read payload",
        )
        require(
            summary[role]["hixl_write_bytes"] == write_bytes,
            f"{role}: HIXL committed write bytes mismatch",
        )
        require(
            summary[role]["negotiate_ok_total"] > 0,
            f"{role}: no successful HIXL session negotiation",
        )
        require(
            sample(delta[role], OUTCOMES, outcome="fallback") == 0,
            f"{role}: HIXL read fallback",
        )
        for phase in (
            "negotiate",
            "connect",
            "read",
            "validate",
            "release",
            "close",
            "expire",
        ):
            require(
                sample(delta[role], OPERATION, phase=phase, result="error") == 0,
                f"{role}: HIXL phase error: {phase}",
            )
        for result in ("ok", "error"):
            require(
                sample(delta[role], OPERATION, phase="fallback", result=result) == 0,
                f"{role}: HIXL fallback",
            )
    for metric in (NATIVE_CALLS, NATIVE_BYTES, VALIDATED):
        sample(before[reader], metric, required=True)
    native = delta[reader]
    calls = sample(native, NATIVE_CALLS, required=True)
    validated = sample(native, VALIDATED, required=True)
    lane_bytes = [
        sample(native, LANE_VALIDATED, lane=str(index)) for index in range(lane_count)
    ]
    require(
        calls > 0
        and sample(native, NATIVE_BYTES, required=True) >= validated >= SIZE * len(work)
        and sum(lane_bytes) == validated,
        "reader native batch/validated/per-lane bytes do not account for source reads",
    )
    return {
        "nodes": summary,
        "reader_native_calls": calls,
        "validated_bytes_by_lane": lane_bytes,
        "application_read_bytes": read_bytes,
        "application_write_bytes": write_bytes,
    }


def idle(value):
    runtime = value["health"]["checks"]["hixl"]
    return all(
        item["receive_slots_in_use"] == item["export_slots_in_use"] == 0
        for item in (runtime, *runtime["lanes"])
    )


def verify(hosts, inventory, counts, output, run_id):
    identity = {}

    def snapshots(directory, name, *, deadline=None):
        result = {}
        for role, host in hosts.items():
            value = remote(
                host,
                "snapshot",
                directory / f"{name}-{role}.json",
                "--rest",
                host["rest"],
                deadline=deadline,
            )
            observed = health(host, value)
            require(
                observed[0] == inventory[role]["boot_id"],
                f"{role}: host boot ID is not the attested boot ID",
            )
            require(
                identity.setdefault(role, observed) == observed,
                f"{role}: host, mount or engine changed",
            )
            result[role] = value
        a, b = identity["a"], identity["b"]
        require(
            a[0] != b[0] and a[3] == b[3] and set(a[-1]).isdisjoint(b[-1]),
            "expected different boot IDs and engine listeners in the same HCCS domain",
        )
        return result

    start = snapshots(output, "initial")
    require(
        all(idle(value) for value in start.values()),
        "expected idle isolated mounts at start",
    )
    for count in counts:
        for home, reader in (("a", "b"), ("b", "a")):
            stem = f"afs-fm-{run_id}-{home}-{count}"
            row = output / f"{home}-home-{count}"
            row.mkdir()
            sys.stdout.write(
                f"Home={home} reader={reader} processes={count} workspace={stem}\n"
            )
            sys.stdout.flush()

            def operation(role, action, *, row=row, stem=stem, count=count):
                return remote(
                    hosts[role],
                    action,
                    row / f"{action}-{role}.json",
                    "--mount-id",
                    identity[role][1],
                    "--stem",
                    stem,
                    "--processes",
                    str(count),
                )

            operation(home, "seed")
            before = snapshots(row, "before")
            require(
                all(idle(value) for value in before.values()),
                "outstanding slots before workload",
            )
            work = operation(reader, "work")
            require(
                [item["index"] for item in work] == list(range(count))
                and len({item["pid"] for item in work}) == count,
                "expected one distinct process per file pair",
            )
            readback = operation(home, "verify")
            require(
                [(item["index"], item["target_sha256"]) for item in work]
                == [(item["index"], item["sha256"]) for item in readback],
                "reader and Home target digests differ",
            )
            deadline = time.monotonic() + DRAIN_SECONDS
            for attempt in poll_attempts():
                after = snapshots(row, f"after-{attempt:02}", deadline=deadline)
                drained = all(idle(value) for value in after.values())
                remaining = drain_remaining(deadline)
                if drained:
                    break
                time.sleep(min(1, remaining))
            stats = assess(
                {role: counters(value["metrics"]) for role, value in before.items()},
                {role: counters(value["metrics"]) for role, value in after.items()},
                home,
                reader,
                work,
                len(hosts[reader]["device_ids"]),
            )
            save(
                row / "passed.json",
                {"workspace": stem, "home": home, "processes": count, **stats},
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pair", type=Path, required=True, help="staged pair JSON (see the runbook)"
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="new evidence directory; parent must exist",
    )
    parser.add_argument("--phase", required=True, choices=("smoke", "scale", "all"))
    parser.add_argument(
        "--inventory",
        type=Path,
        required=True,
        help="host driver inventory attestation JSON",
    )
    args = parser.parse_args()
    hosts = json.loads(args.pair.read_text(encoding="utf-8"))
    inventory = json.loads(args.inventory.read_text(encoding="utf-8"))
    require(set(hosts) == {"a", "b"}, "pair must contain exactly a and b")
    require(set(inventory) == {"a", "b"}, "inventory must attest exactly a and b")
    driver_evidence = {}
    for role, attestation in inventory.items():
        require(
            attestation["verified_hdk"] == hosts[role]["hdk"]
            and at_least_release(attestation["verified_hdk"], (25, 5))
            and at_least_release(attestation.get("verified_cann"), (9, 0, 0))
            and all(
                isinstance(attestation[field], str) and attestation[field].strip()
                for field in (
                    "boot_id",
                    "physical_host",
                    "reviewer",
                    "driver_command",
                    "release_basis",
                )
            ),
            f"{role}: expected matching driver evidence and CANN >= 9.0.0 / HDK >= 25.5",
        )
        uuid.UUID(attestation["boot_id"])
        evidence = args.inventory.parent / attestation["driver_evidence_file"]
        driver_evidence[role] = evidence.read_text(encoding="utf-8")
        require(driver_evidence[role].strip(), f"{role}: empty host driver evidence")
    for host in hosts.values():
        target, prefix, devices = (
            host["ssh_target"],
            host.get("remote_prefix", []),
            host["device_ids"],
        )
        require(
            TARGET.fullmatch(target) and not target.startswith("-"),
            "invalid SSH target",
        )
        require(
            isinstance(prefix, list)
            and all(
                isinstance(arg, str)
                and arg
                and "\n" not in arg
                and "\r" not in arg
                and "\0" not in arg
                for arg in prefix
            ),
            "invalid literal remote_prefix argv",
        )
        require(
            isinstance(devices, list)
            and 1 <= len(devices) <= 16
            and all(type(device) is int and device >= 0 for device in devices)
            and len(set(devices)) == len(devices),
            "specify expected ACL-visible device IDs in lane order",
        )
    a, b = hosts["a"], hosts["b"]
    require(
        a["id"] != b["id"]
        and a["ssh_target"] != b["ssh_target"]
        and len(a["device_ids"]) == len(b["device_ids"]),
        "use distinct Nodes and SSH targets with matching lane counts",
    )
    counts = {"smoke": COUNTS[:1], "scale": COUNTS[1:], "all": COUNTS}[args.phase]
    args.out.mkdir(mode=0o700)
    save(args.out / "inventory.json", inventory)
    for role, raw in driver_evidence.items():
        (args.out / f"driver-{role}.txt").write_text(raw, encoding="utf-8")
    run_id = uuid.uuid4().hex
    save(
        args.out / "run.json",
        {
            "pair": hosts,
            "process_counts": counts,
            "run_id": run_id,
            "window_bytes": WINDOW,
            "seed_bytes": SIZE,
        },
    )
    try:
        verify(hosts, inventory, counts, args.out, run_id)
    except Exception as error:
        save(args.out / "failed.json", {"error": str(error)})
        raise
    save(args.out / "workloads-passed.json", {"shutdown_review_required": True})


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        sys.exit(f"FabricMem verification failed: {error}")
