#!/usr/bin/env python3
"""Conservative AFS environment preparation evaluator."""

from __future__ import annotations

import argparse
import copy
import datetime
import hashlib
import json
import math
import platform
import re
import shlex
import sys
import tarfile
import uuid
from ipaddress import IPv6Address, ip_address
from pathlib import Path
from typing import Any

try:
    from probes import env_verbs as verbs_probe
except Exception:  # pragma: no cover - absence is reported as BLOCKED by evaluator.
    verbs_probe = None

STATUS_ORDER = {"PASS": 0, "BLOCKED": 1, "FAIL": 2}
EXPECTED_KERNEL = "6.8.0-142-generic"
EXPECTED_IMAGE_SHA = "1ea801e659d2f5035ac294e0faab0aac9b6ba66753df933ba5c7beab0c689bd0"
EXPECTED_NETWORK_PROBE_SHA = "3db932a4c1a72d450edbcc222ae8ae4010061fac84b5c012f061c91186e79612"
EXPECTED_NETWORK_FAULT_SOURCE_SHA = "a485c56bf184087f4cbdc2b3dc63b3b3085a537f43f743a2b992ba4c78813353"
EXPECTED_VERBS_CHECKER_SHA = "421ac95282a0eebe52af8bb8122fc46a8925d60682e7222bd493f6e2ff4f3200"
EXPECTED_VERBS_COLLECTOR_SHA = "c8ea5535f2cb0f85fe80e0e9d2104e77229c35ed478a865fdd2231e6dabab130"
EXPECTED_VERBS_OBSERVER_SHA = "f8594925f8ff741f15cd54c98b04c9a4126fa23f2094e708e89c9f8222739935"
EXPECTED_RPING_SHA = "d4d82fdd9b78cfb9404bbb4c2a8c07dde9d450d7fd4d7041d5d0ffcf32e3e89f"
GIB = 1024**3
EXPECTED_VMS = {
    "afs-accept-ctl": {"cpus": 2, "memory": 4 * GIB, "disk": 24 * GIB, "volume": "afsctlstate", "volume_gib": 8, "ip": "192.168.109.11", "inventory": "inventory-ctl.json"},
    "afs-accept-a": {"cpus": 2, "memory": 6 * GIB, "disk": 24 * GIB, "volume": "afsadata", "volume_gib": 32, "ip": "192.168.109.12", "inventory": "inventory-a.json"},
    "afs-accept-b": {"cpus": 2, "memory": 6 * GIB, "disk": 24 * GIB, "volume": "afsbdata", "volume_gib": 32, "ip": "192.168.109.13", "inventory": "inventory-b.json"},
    "afs-accept-c": {"cpus": 2, "memory": 6 * GIB, "disk": 24 * GIB, "volume": "afscdata", "volume_gib": 32, "ip": "192.168.109.14", "inventory": "inventory-c-rxe.json"},
}
NETWORK_NODES = {
    "ctl": {"vm": "afs-accept-ctl", "hostname": "lima-afs-accept-ctl", "ip": "192.168.109.11", "dns": "afs-env-ctl"},
    "a": {"vm": "afs-accept-a", "hostname": "lima-afs-accept-a", "ip": "192.168.109.12", "dns": "afs-env-a"},
    "b": {"vm": "afs-accept-b", "hostname": "lima-afs-accept-b", "ip": "192.168.109.13", "dns": "afs-env-b"},
    "c": {"vm": "afs-accept-c", "hostname": "lima-afs-accept-c", "ip": "192.168.109.14", "dns": "afs-env-c"},
}
DEFERRED = {
    "network-tls-fault-recovery": "complete four-way TCP/UDP, TLS negative and controlled fault recovery semantics are not validated",
    "durable-backend-restart": "etcd/Redis durable backend restart semantics are not validated",
    "cross-vm-verbs": "independent cross-VM verbs transfer is not validated here",
    "ext4-reference-accounting": "pinned ext4 reference suite selection, dependencies and runnable evidence are not validated",
    "actual-moosefs-mount-io": "stock MooseFS mount read/write evidence is not validated",
    "actual-3fs-mount-io": "stock 3FS mount read/write evidence is not validated",
    "complete-frozen-inputs": "complete source/binary/tool/suite/runner frozen-input contract is not validated",
    "run-contracts": "frozen case selections, parameters, exclusions, seeds and result schemas are not validated",
    "clock-accuracy": "actual time-synchronization sources/state and sampling uncertainty are not validated",
    "cgroup-mount-cache-thin-allocation": "cgroup quotas, mount cache mode and host thin-allocation/cache semantics are not evaluated",
}


class InvalidEvidence(ValueError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def check(name: str, status: str, detail: str, evidence: Any = None) -> dict[str, Any]:
    item = {"name": name, "status": status, "detail": detail}
    if evidence is not None:
        item["evidence"] = evidence
    return item


def worst_status(checks: list[dict[str, Any]]) -> str:
    return max((item["status"] for item in checks), key=lambda status: STATUS_ORDER[status], default="BLOCKED")


def checked_path(root: Path, rel: str) -> Path:
    if not rel or "\x00" in rel or rel.startswith("/") or ".." in Path(rel).parts:
        raise InvalidEvidence(f"path escapes evidence root: {rel}")
    path = (root / rel).resolve()
    if not path.is_relative_to(root.resolve()):
        raise InvalidEvidence(f"path escapes evidence root: {rel}")
    return path


def refs_from_bundle(bundle: dict[str, Any]) -> dict[str, str]:
    refs = bundle.get("artifact_references")
    if refs is None:
        refs = bundle.get("files")
    if not isinstance(refs, dict):
        raise InvalidEvidence("bundle artifact_references/files must be an object")
    clean: dict[str, str] = {}
    for rel, digest in refs.items():
        if not isinstance(rel, str) or not isinstance(digest, str):
            raise InvalidEvidence("artifact references must map string paths to string sha256 values")
        if not rel or "\x00" in rel or Path(rel).is_absolute() or ".." in Path(rel).parts:
            raise InvalidEvidence(f"invalid artifact reference path: {rel!r}")
        if len(digest) != 64:
            raise InvalidEvidence(f"artifact reference sha256 is malformed: {rel}")
        clean[rel] = digest
    return clean


def read_json_ref(root: Path, refs: dict[str, str], rel: str) -> tuple[str, Any | None]:
    if rel not in refs:
        return "MISSING", None
    path = checked_path(root, rel)
    if not path.is_file():
        return "MISSING", None
    if sha256_file(path) != refs[rel]:
        return "TAMPERED", None
    try:
        value = load_json(path)
        return ("OK", value) if isinstance(value, dict) else ("MALFORMED", None)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "MALFORMED", None


def read_jsonl_ref(root: Path, refs: dict[str, str], rel: str) -> tuple[str, list[dict[str, Any]]]:
    if rel not in refs:
        return "MISSING", []
    path = checked_path(root, rel)
    if not path.is_file():
        return "MISSING", []
    if sha256_file(path) != refs[rel]:
        return "TAMPERED", []
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                row = json.loads(line)
                if not isinstance(row, dict):
                    return "MALFORMED", []
                rows.append(row)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "MALFORMED", []
    return "OK", rows


def read_text_ref(root: Path, refs: dict[str, str], rel: str) -> tuple[str, str]:
    if rel not in refs:
        return "MISSING", ""
    path = checked_path(root, rel)
    if not path.is_file():
        return "MISSING", ""
    if sha256_file(path) != refs[rel]:
        return "TAMPERED", ""
    try:
        return "OK", path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return "MALFORMED", ""


def parse_stdout_json(record: Any) -> Any | None:
    if not isinstance(record, dict) or not isinstance(record.get("stdout"), str) or record.get("returncode") != 0 or record.get("status") != "OBSERVED":
        return None
    try:
        return json.loads(record["stdout"])
    except json.JSONDecodeError:
        return None


def is_positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def is_finite_seconds(value: Any, maximum: float = 3.0) -> bool:
    return type(value) in {int, float} and math.isfinite(float(value)) and 0 <= float(value) <= maximum


def port_tuple(value: Any, ip: str, port: int | None = None) -> bool:
    return isinstance(value, list) and len(value) == 2 and value[0] == ip and type(value[1]) is int and 1 <= value[1] <= 65535 and (port is None or value[1] == port)


def is_sha256_hex(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def normalize_iptables(text: str) -> list[str]:
    return [line for line in text.splitlines() if line and not line.startswith("#")]


def valid_iptables_save(text: Any) -> bool:
    if not isinstance(text, str):
        return False
    rows = normalize_iptables(text)
    table = None
    completed = 0
    for row in rows:
        if row.startswith("*") and table is None:
            table = row[1:]
            if table not in {"nat", "filter", "mangle", "raw", "security"}:
                return False
        elif row == "COMMIT" and table is not None:
            table = None
            completed += 1
        elif table is None or not row.startswith((":", "-A ")):
            return False
    return completed > 0 and table is None


def valid_preserved_observation(value: dict[str, Any]) -> bool:
    processes = value.get("processes")
    mountinfo = value.get("mountinfo")
    if not isinstance(processes, list) or not isinstance(mountinfo, str) or not mountinfo.strip() or not valid_iptables_save(value.get("iptables")):
        return False
    for process in processes:
        if not isinstance(process, dict) or not is_positive_int(process.get("pid")) or not isinstance(process.get("start_ticks"), str) or not process["start_ticks"].isdigit() or int(process["start_ticks"]) <= 0 or not isinstance(process.get("exe"), str) or not process["exe"].startswith("/") or not is_sha256_hex(process.get("exe_sha256")) or not isinstance(process.get("configs"), list):
            return False
        if any(not isinstance(config, dict) or not isinstance(config.get("path"), str) or not config["path"].startswith("/") or not is_sha256_hex(config.get("sha256")) for config in process["configs"]):
            return False
    return True


def network_pair_rel(prefix: str, src: str, dst: str) -> str:
    return f"{prefix}/{src}/logs/pair-{src}-{dst}.json"


def add_network_problem(problems: list[dict[str, str]], status: str, detail: str) -> None:
    problems.append({"status": status, "detail": detail})


def network_status_from(problems: list[dict[str, str]]) -> str:
    if any(problem["status"] == "FAIL" for problem in problems):
        return "FAIL"
    if problems:
        return "BLOCKED"
    return "PASS"


def require_json_ref(root: Path, refs: dict[str, str], rel: str, problems: list[dict[str, str]]) -> dict[str, Any] | None:
    status, value = read_json_ref(root, refs, rel)
    if status == "OK" and isinstance(value, dict):
        return value
    add_network_problem(problems, "BLOCKED" if status == "MISSING" else "FAIL", f"{rel}: {status}")
    return None


def require_text_ref(root: Path, refs: dict[str, str], rel: str, problems: list[dict[str, str]]) -> str | None:
    status, value = read_text_ref(root, refs, rel)
    if status == "OK":
        return value
    add_network_problem(problems, "BLOCKED" if status == "MISSING" else "FAIL", f"{rel}: {status}")
    return None


def shell_command(row: dict[str, Any], expected_vm: str, expected_rc: int) -> str | None:
    argv = row.get("argv")
    if not isinstance(argv, list) or row.get("returncode") != expected_rc:
        return None
    expected_prefix = ["limactl", "shell", "--workdir", "/home/lzc.guest", expected_vm, "--", "sudo", "bash", "-lc"]
    if len(argv) != len(expected_prefix) + 1 or argv[:len(expected_prefix)] != expected_prefix:
        return None
    shell = argv[-1]
    if not isinstance(shell, str) or shell.lstrip().startswith("echo "):
        return None
    return shell


def shell_tokens(shell: str) -> list[str] | None:
    try:
        return shlex.split(shell)
    except ValueError:
        return None


def exact_client_command(row: dict[str, Any], src: str, dst: str, output: str, expected_rc: int, *, require_negatives: bool, wrong_client: bool = False) -> bool:
    shell = shell_command(row, NETWORK_NODES[src]["vm"], expected_rc)
    if shell is None:
        return False
    tokens = shell_tokens(shell)
    if tokens is None:
        return False
    expected = ["python3", "/var/lib/afs-acceptance/network-v67-r2/env_network.py", "client"]
    expected_values = {
        "--source-ip": NETWORK_NODES[src]["ip"],
        "--target-ip": NETWORK_NODES[dst]["ip"],
        "--port": "19566",
        "--tls-port": "19567",
        "--ca": "/var/lib/afs-acceptance/network-v67-r2/tls/ca.pem",
        "--client-cert": f"/var/lib/afs-acceptance/network-v67-r2/tls/{src}.pem",
        "--client-key": f"/var/lib/afs-acceptance/network-v67-r2/tls/{src}.key",
        "--server-hostname": NETWORK_NODES[dst]["dns"],
        "--timeout": "2",
        "--check": "tls" if wrong_client else "all",
    }
    for flag, value in expected_values.items():
        expected.extend([flag, value])
    if require_negatives:
        expected.extend(["--untrusted-ca", "--bad-ca", "/var/lib/afs-acceptance/network-v67-r2/tls/untrusted.pem", "--wrong-hostname", "--missing-client-cert"])
    if wrong_client:
        expected.extend(["--untrusted-client-cert", "--bad-client-cert", "/var/lib/afs-acceptance/network-v67-r2/tls/rogue.pem", "--bad-client-key", "/var/lib/afs-acceptance/network-v67-r2/tls/rogue.key"])
    expected.extend([">", f"/var/lib/afs-acceptance/network-v67-r2/{output}", "2>", f"/var/lib/afs-acceptance/network-v67-r2/{output.removesuffix('.json')}.stderr"])
    # This predicate supports the frozen collector's literal invocation, with
    # no duplicate options, shell suffix, or result-rewriting command.
    return tokens == expected


def network_check_positive(result: dict[str, Any], src: str, dst: str, problems: list[dict[str, str]], rel: str, *, require_negatives: bool = True) -> bool:
    src_ip, dst_ip = NETWORK_NODES[src]["ip"], NETWORK_NODES[dst]["ip"]
    if result.get("status") != "PASS" or result.get("source_bind") != src_ip or result.get("target") != dst_ip:
        add_network_problem(problems, "FAIL", f"{rel}: source/target/status mismatch")
        return False
    if type(result.get("token_bytes")) is not int or result.get("token_bytes") != 32 or not is_sha256_hex(result.get("token_sha256")) or not is_finite_seconds(result.get("timeout_seconds")) or result["timeout_seconds"] <= 0:
        add_network_problem(problems, "FAIL", f"{rel}: invalid nonce")
        return False
    checks = result.get("checks")
    if not isinstance(checks, dict):
        add_network_problem(problems, "FAIL", f"{rel}: missing checks")
        return False
    ok = True
    tcp = checks.get("tcp")
    if not isinstance(tcp, dict) or tcp.get("status") != "PASS" or tcp.get("bytes") != 32 or not port_tuple(tcp.get("local"), src_ip) or not port_tuple(tcp.get("peer"), dst_ip, 19566) or not is_finite_seconds(tcp.get("elapsed_seconds")):
        add_network_problem(problems, "FAIL", f"{rel}: invalid TCP exchange")
        ok = False
    udp = checks.get("udp")
    if not isinstance(udp, dict) or udp.get("status") != "PASS" or udp.get("bytes") != 32 or not port_tuple(udp.get("local"), src_ip) or not port_tuple(udp.get("sender"), dst_ip, 19566) or not is_finite_seconds(udp.get("elapsed_seconds")):
        add_network_problem(problems, "FAIL", f"{rel}: invalid UDP exchange")
        ok = False
    tls = checks.get("tls")
    names = tls.get("server_cert_names") if isinstance(tls, dict) else None
    if not isinstance(tls, dict) or tls.get("status") != "PASS" or tls.get("bytes") != 32 or not port_tuple(tls.get("local"), src_ip) or not port_tuple(tls.get("peer"), dst_ip, 19567) or tls.get("tls_version") not in {"TLSv1.2", "TLSv1.3"} or not is_finite_seconds(tls.get("elapsed_seconds")) or not isinstance(names, list) or dst_ip not in names or NETWORK_NODES[dst]["dns"] not in names:
        add_network_problem(problems, "FAIL", f"{rel}: invalid mTLS exchange")
        ok = False
    if require_negatives:
        expected_negatives = {
            "tls_untrusted_ca": ("untrusted_ca", "19"),
            "tls_wrong_hostname": ("wrong_hostname", "62"),
            "tls_missing_client_cert": ("missing_client_cert", None),
        }
        for key, (reason, verify_code) in expected_negatives.items():
            negative = checks.get(key)
            observed = negative.get("observed") if isinstance(negative, dict) else None
            detail = observed.get("detail", "") if isinstance(observed, dict) else ""
            if not isinstance(negative, dict) or negative.get("status") != "PASS" or negative.get("negative") != reason or not isinstance(observed, dict) or observed.get("status") != "FAIL" or observed.get("reason") != reason:
                add_network_problem(problems, "FAIL", f"{rel}: invalid {key}")
                ok = False
            if verify_code is not None and isinstance(observed, dict) and observed.get("verify_code") != verify_code:
                add_network_problem(problems, "FAIL", f"{rel}: invalid {key} verify code")
                ok = False
            if key == "tls_missing_client_cert" and "certificate required" not in str(detail).lower():
                add_network_problem(problems, "FAIL", f"{rel}: missing-client negative lacks certificate-required alert")
                ok = False
    return ok


def network_check_fault_result(result: dict[str, Any], expected_status: str, problems: list[dict[str, str]], rel: str) -> None:
    if expected_status == "PASS":
        network_check_positive(result, "a", "b", problems, rel, require_negatives=False)
        return
    if result.get("status") != "FAIL" or result.get("source_bind") != NETWORK_NODES["a"]["ip"] or result.get("target") != NETWORK_NODES["b"]["ip"]:
        add_network_problem(problems, "FAIL", f"{rel}: fault result source/target/status mismatch")
        return
    if type(result.get("token_bytes")) is not int or result.get("token_bytes") != 32 or not is_sha256_hex(result.get("token_sha256")) or not is_finite_seconds(result.get("timeout_seconds")) or result["timeout_seconds"] <= 0:
        add_network_problem(problems, "FAIL", f"{rel}: invalid fault nonce or timeout")
    checks = result.get("checks")
    if not isinstance(checks, dict):
        add_network_problem(problems, "FAIL", f"{rel}: missing fault checks")
        return
    for key in ("tcp", "udp", "tls"):
        item = checks.get(key)
        if not isinstance(item, dict) or item.get("status") != "FAIL" or item.get("reason") != "timeout" or not is_finite_seconds(item.get("elapsed_seconds"), 3.0):
            add_network_problem(problems, "FAIL", f"{rel}: invalid bounded {key} fault")


def evaluate_network(bundle: dict[str, Any], artifact_root: Path, refs: dict[str, str]) -> dict[str, Any]:
    try:
        return _evaluate_network(bundle, artifact_root, refs)
    except (InvalidEvidence, OSError) as exc:
        return check("network-tls-fault-recovery", "BLOCKED", str(exc))
    except (TypeError, ValueError, AttributeError, KeyError, OverflowError) as exc:
        return check("network-tls-fault-recovery", "FAIL", f"malformed network observation: {exc}")


def _evaluate_network(bundle: dict[str, Any], artifact_root: Path, refs: dict[str, str]) -> dict[str, Any]:
    problems: list[dict[str, str]] = []
    evidence: dict[str, Any] = {"scope": "network/TLS and directed fault preparation predicate only"}
    network = bundle.get("network")
    if not isinstance(network, dict):
        return check("network-tls-fault-recovery", "BLOCKED", "network evidence bundle is missing")
    prefix = network.get("prefix")
    if prefix != "network":
        return check("network-tls-fault-recovery", "BLOCKED", "network.prefix must be 'network'", {"prefix": prefix})
    for field in ("probe_source", "commands", "fault_source"):
        rel = network.get(field)
        if not isinstance(rel, str):
            return check("network-tls-fault-recovery", "BLOCKED", f"network.{field} is missing")
        try:
            checked_path(artifact_root, rel)
        except InvalidEvidence as exc:
            return check("network-tls-fault-recovery", "BLOCKED", str(exc))
        if rel not in refs:
            add_network_problem(problems, "BLOCKED", f"{rel}: MISSING")
    probe_path = checked_path(artifact_root, network["probe_source"])
    if not probe_path.is_file():
        add_network_problem(problems, "BLOCKED", "network probe source file is missing")
    else:
        probe_sha = sha256_file(probe_path)
        if network["probe_source"] in refs and probe_sha != refs[network["probe_source"]]:
            add_network_problem(problems, "FAIL", "network probe source reference is tampered")
        if probe_sha != EXPECTED_NETWORK_PROBE_SHA:
            add_network_problem(problems, "FAIL", "network probe source is not the frozen observed probe")
    if network["fault_source"] in refs:
        fault_path = checked_path(artifact_root, network["fault_source"])
        if not fault_path.is_file():
            add_network_problem(problems, "BLOCKED", "fault source file is missing")
        else:
            fault_sha = sha256_file(fault_path)
            if fault_sha != refs[network["fault_source"]]:
                add_network_problem(problems, "FAIL", "fault source reference is tampered")
            if fault_sha != EXPECTED_NETWORK_FAULT_SOURCE_SHA:
                add_network_problem(problems, "FAIL", "fault source is not the supported directed-fault recipe")
    command_status, commands = read_jsonl_ref(artifact_root, refs, network["commands"])
    if command_status != "OK" or not commands:
        add_network_problem(problems, "BLOCKED" if command_status == "MISSING" else "FAIL", f"{network['commands']}: {command_status or 'EMPTY'}")
    commands_available = command_status == "OK" and bool(commands)
    commands_malformed = any(not isinstance(row.get("argv"), list) or any(not isinstance(part, str) for part in row.get("argv", [])) or not is_finite_seconds(row.get("time_unix_ms"), 10**13) for row in commands)
    if commands_malformed:
        add_network_problem(problems, "FAIL", "command transcript has malformed argv or timestamp")
    if commands_available and not commands_malformed and any(commands[i]["time_unix_ms"] > commands[i + 1]["time_unix_ms"] for i in range(len(commands) - 1)):
        add_network_problem(problems, "FAIL", "command transcript timestamps are not ordered")

    ready: dict[str, dict[str, Any]] = {}
    boots: set[str] = set()
    machines: set[str] = set()
    for name, node in NETWORK_NODES.items():
        rel = f"{prefix}/{name}/ready.json"
        item = require_json_ref(artifact_root, refs, rel, problems)
        if item is None:
            continue
        ready[name] = item
        script_rel = f"{prefix}/{name}/env_network.py"
        script_path = checked_path(artifact_root, script_rel)
        if script_rel not in refs or not script_path.is_file():
            add_network_problem(problems, "BLOCKED", f"{script_rel}: MISSING")
        elif sha256_file(script_path) != refs[script_rel] or sha256_file(script_path) != EXPECTED_NETWORK_PROBE_SHA:
            add_network_problem(problems, "FAIL", f"{script_rel}: unexpected source")
        probe_input = require_text_ref(artifact_root, refs, f"{prefix}/{name}/logs/probe-input.sha256", problems)
        if probe_input is not None and EXPECTED_NETWORK_PROBE_SHA not in probe_input:
            add_network_problem(problems, "FAIL", f"{name}: probe input SHA mismatch")
        boot, machine = item.get("boot_id"), item.get("machine_id")
        if item.get("status") != "READY" or item.get("hostname") != node["hostname"] or item.get("source_ip") != node["ip"] or not is_positive_int(item.get("pid")) or not is_positive_int(item.get("start_ticks")) or not isinstance(boot, str) or not boot or not isinstance(machine, str) or not machine:
            add_network_problem(problems, "FAIL", f"{rel}: malformed ready identity")
        if item.get("script_sha256") != EXPECTED_NETWORK_PROBE_SHA:
            add_network_problem(problems, "FAIL", f"{rel}: script SHA mismatch")
        for key, port in (("tcp", 19566), ("udp", 19566), ("tls", 19567)):
            value = item.get(key)
            if not isinstance(value, dict) or value.get("bind_ip") != node["ip"] or value.get("port") != port:
                add_network_problem(problems, "FAIL", f"{rel}: invalid {key} listener")
        if not isinstance(item.get("tls"), dict) or item["tls"].get("mtls") is not True:
            add_network_problem(problems, "FAIL", f"{rel}: mTLS disabled")
        if isinstance(boot, str):
            boots.add(boot)
        if isinstance(machine, str):
            machines.add(machine)
    if len(ready) == len(NETWORK_NODES) and (len(boots) != len(NETWORK_NODES) or len(machines) != len(NETWORK_NODES)):
        add_network_problem(problems, "FAIL", "guest boot and machine identities must be nonempty and unique")

    nonces: set[str] = set()
    pair_missing = False
    for src in NETWORK_NODES:
        for dst in NETWORK_NODES:
            if src == dst:
                continue
            rel = network_pair_rel(prefix, src, dst)
            item = require_json_ref(artifact_root, refs, rel, problems)
            if item is None:
                pair_missing = True
            elif network_check_positive(item, src, dst, problems, rel):
                nonce = item["token_sha256"]
                if nonce in nonces:
                    add_network_problem(problems, "FAIL", f"{rel}: duplicate nonce")
                nonces.add(nonce)
    if not pair_missing and len(nonces) != 12:
        add_network_problem(problems, "FAIL", "expected twelve unique pair nonces")

    wrong = require_json_ref(artifact_root, refs, f"{prefix}/a/logs/wrong-client.json", problems)
    if wrong is not None:
        checks = wrong.get("checks")
        negative = checks.get("tls_untrusted_client_cert") if isinstance(checks, dict) else None
        observed = negative.get("observed") if isinstance(negative, dict) else None
        tls = checks.get("tls") if isinstance(checks, dict) else None
        names = tls.get("server_cert_names") if isinstance(tls, dict) else None
        if wrong.get("status") != "PASS" or wrong.get("source_bind") != NETWORK_NODES["a"]["ip"] or wrong.get("target") != NETWORK_NODES["b"]["ip"] or type(wrong.get("token_bytes")) is not int or wrong.get("token_bytes") != 32 or not is_sha256_hex(wrong.get("token_sha256")):
            add_network_problem(problems, "FAIL", "wrong-client positive identity is malformed")
        if not isinstance(tls, dict) or tls.get("status") != "PASS" or tls.get("bytes") != 32 or not port_tuple(tls.get("local"), NETWORK_NODES["a"]["ip"]) or not port_tuple(tls.get("peer"), NETWORK_NODES["b"]["ip"], 19567) or tls.get("tls_version") not in {"TLSv1.2", "TLSv1.3"} or not is_finite_seconds(tls.get("elapsed_seconds")) or not isinstance(names, list) or NETWORK_NODES["b"]["ip"] not in names or NETWORK_NODES["b"]["dns"] not in names:
            add_network_problem(problems, "FAIL", "wrong-client mTLS positive exchange is malformed")
        if not isinstance(negative, dict) or negative.get("status") != "PASS" or negative.get("negative") != "untrusted_client_cert" or not isinstance(observed, dict) or observed.get("status") != "FAIL" or observed.get("reason") != "untrusted_ca" or "unknown ca" not in str(observed.get("detail", "")).lower():
            add_network_problem(problems, "FAIL", "wrong-client rejection is not unknown-CA mTLS failure")

    for rel, expected in (("fault-before.json", "PASS"), ("fault-injected.json", "FAIL"), ("fault-restored.json", "PASS")):
        item = require_json_ref(artifact_root, refs, f"{prefix}/a/logs/{rel}", problems)
        if item is not None:
            network_check_fault_result(item, expected, problems, f"{prefix}/a/logs/{rel}")

    hit = require_text_ref(artifact_root, refs, f"{prefix}/b/logs/iptables-hit.txt", problems)
    if hit is not None:
        tagged = [line for line in hit.splitlines() if "DROP" in line and "afs-env-v67-only" in line]
        pattern = r"\s*([1-9][0-9]*)\s+([1-9][0-9]*)\s+DROP\s+(6|17)\s+--\s+\*\s+\*\s+192\.168\.109\.12\s+192\.168\.109\.13\s+(multiport dports 19566,19567|udp dpt:19566)\s+/\* afs-env-v67-only \*/\s*"
        matches = [re.fullmatch(pattern, line) for line in tagged]
        scopes = {(match.group(3), match.group(4)) for match in matches if match is not None}
        if len(tagged) != 2 or any(match is None for match in matches) or scopes != {("6", "multiport dports 19566,19567"), ("17", "udp dpt:19566")}:
            add_network_problem(problems, "FAIL", "directed DROP counters are not exact and nonzero")
    before_rules = require_text_ref(artifact_root, refs, f"{prefix}/b/logs/iptables-before.txt", problems)
    final_rules = require_text_ref(artifact_root, refs, f"{prefix}/b/logs/iptables-final-restored.txt", problems)
    if before_rules is not None and final_rules is not None:
        if not valid_iptables_save(before_rules) or not valid_iptables_save(final_rules) or normalize_iptables(before_rules) != normalize_iptables(final_rules):
            add_network_problem(problems, "FAIL", "iptables final rules differ from original rules")

    for name, node in NETWORK_NODES.items():
        before = require_json_ref(artifact_root, refs, f"{prefix}/{name}/logs/preserved-before.json", problems)
        after = require_json_ref(artifact_root, refs, f"{prefix}/{name}/logs/preserved-after.json", problems)
        if before is not None and after is not None:
            if not valid_preserved_observation(before) or not valid_preserved_observation(after):
                add_network_problem(problems, "FAIL", f"{name}: preserved observations lack typed processes/mounts/rules")
            for key in ("processes", "mountinfo", "iptables"):
                left = normalize_iptables(before.get(key, "")) if key == "iptables" and isinstance(before.get(key), str) else before.get(key)
                right = normalize_iptables(after.get(key, "")) if key == "iptables" and isinstance(after.get(key), str) else after.get(key)
                if left != right:
                    add_network_problem(problems, "FAIL", f"{name}: preserved {key} changed")
            if before.get("hostname") != node["hostname"] or after.get("hostname") != node["hostname"] or (name in ready and (before.get("boot_id") != ready[name].get("boot_id") or after.get("boot_id") != ready[name].get("boot_id") or before.get("machine_id") != ready[name].get("machine_id") or after.get("machine_id") != ready[name].get("machine_id"))):
                add_network_problem(problems, "FAIL", f"{name}: preserved identity mismatch")
        live = require_json_ref(artifact_root, refs, f"{prefix}/{name}/logs/server-live-after.json", problems)
        stopped = require_json_ref(artifact_root, refs, f"{prefix}/{name}/logs/server-stopped.json", problems)
        if live is not None and name in ready:
            if live.get("status") != "LIVE" or live.get("pid") != ready[name].get("pid") or live.get("start_ticks") != ready[name].get("start_ticks") or live.get("boot_id") != ready[name].get("boot_id") or live.get("script_sha256") != EXPECTED_NETWORK_PROBE_SHA:
                add_network_problem(problems, "FAIL", f"{name}: live server identity mismatch")
        if stopped is not None and name in ready:
            if stopped.get("status") != "STOPPED" or stopped.get("pid") != ready[name].get("pid") or stopped.get("start_ticks") != ready[name].get("start_ticks"):
                add_network_problem(problems, "FAIL", f"{name}: server stop identity mismatch")
        listeners = require_text_ref(artifact_root, refs, f"{prefix}/{name}/logs/listeners-after.txt", problems)
        if listeners is not None and (":19566" in listeners or ":19567" in listeners):
            add_network_problem(problems, "FAIL", f"{name}: probe listeners still present after cleanup")

    if commands_available and not commands_malformed:
        if not any(exact_client_command(row, "a", "b", "logs/wrong-client.json", 0, require_negatives=False, wrong_client=True) for row in commands):
            add_network_problem(problems, "FAIL", "missing exact untrusted-client command transcript")
        for src in NETWORK_NODES:
            for dst in NETWORK_NODES:
                if src == dst:
                    continue
                output = f"logs/pair-{src}-{dst}.json"
                if not any(exact_client_command(row, src, dst, output, 0, require_negatives=True) for row in commands):
                    add_network_problem(problems, "FAIL", f"missing exact command transcript for {src}->{dst}")
        required_fault = [
            (lambda row: exact_client_command(row, "a", "b", "logs/fault-before.json", 0, require_negatives=False), "fault-before"),
            (lambda row: shell_command(row, NETWORK_NODES["b"]["vm"], 0) == "bash /tmp/afs-v67-fault.sh install", "fault-install"),
            (lambda row: exact_client_command(row, "a", "b", "logs/fault-injected.json", 1, require_negatives=False), "fault-injected"),
            (lambda row: shell_command(row, NETWORK_NODES["b"]["vm"], 0) == "bash /tmp/afs-v67-fault.sh inspect; bash /tmp/afs-v67-fault.sh restore", "fault-restore"),
            (lambda row: exact_client_command(row, "a", "b", "logs/fault-restored.json", 0, require_negatives=False), "fault-restored"),
        ]
        pos = -1
        for predicate, name in required_fault:
            matches = [i for i, row in enumerate(commands) if i > pos and predicate(row)]
            if not matches:
                add_network_problem(problems, "FAIL", f"missing ordered command transcript step: {name}")
                break
            pos = matches[0]

    evidence["problems"] = problems[:20]
    evidence["pair_nonces"] = len(nonces)
    status = network_status_from(problems)
    detail = "hash-bound raw network/TLS exchanges and directed fault restoration validated" if status == "PASS" else "network evidence is incomplete or inconsistent"
    return check("network-tls-fault-recovery", status, detail, evidence)


def verbs_add_problem(problems: list[dict[str, str]], status: str, detail: str) -> None:
    problems.append({"status": status, "detail": detail})


def verbs_status_from(problems: list[dict[str, str]]) -> str:
    if any(problem["status"] == "FAIL" for problem in problems):
        return "FAIL"
    if problems:
        return "BLOCKED"
    return "PASS"


def verbs_rel(verbs: dict[str, Any], prefix: str, field: str, default_suffix: str) -> str | None:
    value = verbs.get(field)
    if value is None:
        return f"{prefix}/{default_suffix}"
    return value if isinstance(value, str) else None


def require_verbs_json(root: Path, refs: dict[str, str], rel: str, problems: list[dict[str, str]]) -> dict[str, Any] | None:
    status, value = read_json_ref(root, refs, rel)
    if status == "OK" and isinstance(value, dict):
        return value
    verbs_add_problem(problems, "BLOCKED" if status == "MISSING" else "FAIL", f"{rel}: {status}")
    return None


def require_verbs_text(root: Path, refs: dict[str, str], rel: str, problems: list[dict[str, str]]) -> str | None:
    status, value = read_text_ref(root, refs, rel)
    if status == "OK":
        return value
    verbs_add_problem(problems, "BLOCKED" if status == "MISSING" else "FAIL", f"{rel}: {status}")
    return None


def require_verbs_jsonl(root: Path, refs: dict[str, str], rel: str, problems: list[dict[str, str]]) -> list[dict[str, Any]] | None:
    status, rows = read_jsonl_ref(root, refs, rel)
    if status == "OK" and rows:
        return rows
    verbs_add_problem(problems, "BLOCKED" if status == "MISSING" else "FAIL", f"{rel}: {status if status != 'OK' else 'EMPTY'}")
    return None


def verbs_expected_pairs() -> list[tuple[str, str, int]]:
    pairs = [(src, dst, 256) for src in NETWORK_NODES for dst in NETWORK_NODES if src != dst]
    pairs.append(("a", "b", 65535))
    return pairs


def verbs_label(client: str, server: str, size: int) -> str:
    return f"{client}-{server}-{size}"


def verbs_runtime_report_rel(prefix: str, node: str, label: str, run_id: str, role: str) -> str:
    return f"{prefix}/runtime/{node}/pairs/{label}/{run_id}-{role}.json"


def verbs_runtime_raw_rel(prefix: str, node: str, label: str, run_id: str, role: str) -> str:
    return f"{prefix}/runtime/{node}/pairs/{label}/{run_id}-{role}.raw.log"


def verbs_negative_report_rel(prefix: str, run_id: str) -> str:
    return f"{prefix}/runtime/a/negative-no-listener/{run_id}-client.json"


def verbs_negative_raw_rel(prefix: str, run_id: str) -> str:
    return f"{prefix}/runtime/a/negative-no-listener/{run_id}-client.raw.log"


def valid_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


def valid_finite_time(value: Any) -> bool:
    return type(value) in {int, float} and math.isfinite(float(value)) and float(value) > 0


def verbs_lima_prefix(node: str) -> list[str]:
    return ["limactl", "shell", "--workdir", "/home/lzc.guest", NETWORK_NODES[node]["vm"], "--"]


def verbs_exact_install_command(row: dict[str, Any], node: str) -> bool:
    argv = row.get("argv")
    expected = verbs_lima_prefix(node) + ["bash", "-lc", "cat > /tmp/afs-verbs-v69/env_verbs.py; sha256sum /tmp/afs-verbs-v69/env_verbs.py"]
    return argv == expected and row.get("node") == node and row.get("returncode") == 0 and row.get("stdin_sha256") == EXPECTED_VERBS_COLLECTOR_SHA and isinstance(row.get("stdout"), str) and row["stdout"].startswith(EXPECTED_VERBS_COLLECTOR_SHA + "  /tmp/afs-verbs-v69/env_verbs.py")


def verbs_exact_probe_command(row: dict[str, Any], role: str, node: str, bind: str, peer: str, size: int, run_id: str, output: str, rc: int, timeout: int) -> bool:
    argv = row.get("argv")
    expected = verbs_lima_prefix(node) + [
        "python3", "/tmp/afs-verbs-v69/env_verbs.py", "run",
        "--role", role,
        "--bind", bind,
        "--peer", peer,
        "--port", "19669",
        "--size", str(size),
        "--count", "3",
        "--run-id", run_id,
        "--output", output,
        "--timeout", str(timeout),
    ]
    return argv == expected and row.get("node") == node and row.get("returncode") == rc


def verbs_exact_listener_command(row: dict[str, Any], server: str, label: str, run_id: str) -> bool:
    argv = row.get("argv")
    raw = f"/tmp/afs-verbs-v69/pairs/{label}/{run_id}-server.raw.log"
    expected = verbs_lima_prefix(server) + ["bash", "-lc", f"test -r {raw} && grep -Fx rdma_listen {raw}"]
    return argv == expected and row.get("node") == server and row.get("returncode") == 0 and row.get("stdout") == "rdma_listen\n"


def verbs_exact_observe_after_command(row: dict[str, Any], node: str) -> bool:
    argv = row.get("argv")
    expected = verbs_lima_prefix(node) + ["sudo", "python3", "/tmp/afs-verbs-v69/observe.py", "/tmp/afs-verbs-v69/protected-after.json"]
    return argv == expected and row.get("node") == node and row.get("returncode") == 0


def parse_rfc3339_z(value: Any) -> datetime.datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo is not None else None
    except ValueError:
        return None


def verbs_command_covers_endpoint(row: dict[str, Any], endpoint: dict[str, Any]) -> bool:
    started, ended = parse_rfc3339_z(row.get("started")), parse_rfc3339_z(row.get("ended"))
    first, last = endpoint.get("started_unix"), endpoint.get("ended_unix")
    if started is None or ended is None or not valid_finite_time(first) or not valid_finite_time(last):
        return False
    # Record matching allows 50 ms of host/guest clock skew. This does not
    # qualify the separate acceptance time-synchronization requirement.
    tolerance = 0.05
    return started <= ended and first <= last and started.timestamp() <= first + tolerance and ended.timestamp() >= last - tolerance


def verbs_command_rows_valid(commands: list[dict[str, Any]], problems: list[dict[str, str]]) -> bool:
    ok = True
    seen_ids: set[int] = set()
    for idx, row in enumerate(commands):
        argv = row.get("argv")
        if not isinstance(argv, list) or any(not isinstance(part, str) for part in argv) or type(row.get("returncode")) is not int or not isinstance(row.get("node"), str) or row.get("node") not in NETWORK_NODES:
            verbs_add_problem(problems, "FAIL", f"commands.jsonl row {idx}: malformed argv/node/returncode")
            ok = False
            continue
        row_id = row.get("id")
        if not is_positive_int(row_id) or row_id in seen_ids:
            verbs_add_problem(problems, "FAIL", f"commands.jsonl row {idx}: malformed or duplicate id")
            ok = False
        else:
            seen_ids.add(row_id)
        started, ended = parse_rfc3339_z(row.get("started")), parse_rfc3339_z(row.get("ended"))
        if started is None or ended is None or started > ended:
            verbs_add_problem(problems, "FAIL", f"commands.jsonl row {idx}: malformed timestamps")
            ok = False
    return ok


def verbs_read_report(root: Path, refs: dict[str, str], rel: str, raw_rel: str, problems: list[dict[str, str]]) -> dict[str, Any] | None:
    report = require_verbs_json(root, refs, rel, problems)
    raw = require_verbs_text(root, refs, raw_rel, problems)
    if report is None or raw is None:
        return None
    if report.get("raw_log") != raw:
        verbs_add_problem(problems, "FAIL", f"{rel}: embedded raw log does not match raw artifact")
    if report.get("raw_log_sha256") != hashlib.sha256(raw.encode("utf-8")).hexdigest():
        verbs_add_problem(problems, "FAIL", f"{rel}: raw log sha256 mismatch")
    return report


def verbs_endpoint_precheck(ep: dict[str, Any], node: str, peer: str, role: str, run_id: str, size: int, problems: list[dict[str, str]], rel: str, protected_ids: dict[str, dict[str, str]], *, returncode: int = 0, timeout: int = 25) -> None:
    node_info = NETWORK_NODES[node]
    peer_info = NETWORK_NODES[peer]
    if ep.get("role") != role or ep.get("run_id") != run_id or ep.get("bind") != node_info["ip"] or ep.get("peer") != peer_info["ip"]:
        verbs_add_problem(problems, "FAIL", f"{rel}: endpoint role/run/bind/peer mismatch")
    if ep.get("port") != 19669 or ep.get("size") != size or ep.get("count") != 3 or type(ep.get("returncode")) is not int or ep.get("returncode") != returncode or ep.get("timed_out") is not False or ep.get("timeout_seconds") != timeout:
        verbs_add_problem(problems, "FAIL", f"{rel}: endpoint run parameters mismatch")
    if not valid_finite_time(ep.get("started_unix")) or not valid_finite_time(ep.get("ended_unix")) or float(ep.get("ended_unix", -1)) < float(ep.get("started_unix", 0)):
        verbs_add_problem(problems, "FAIL", f"{rel}: invalid endpoint timing")
    host = ep.get("host")
    protected = protected_ids.get(node, {})
    if not isinstance(host, dict) or host.get("hostname") != node_info["hostname"] or host.get("kernel") != EXPECTED_KERNEL or not isinstance(host.get("boot_id"), str) or not host["boot_id"].strip() or not isinstance(host.get("machine_id"), str) or not host["machine_id"].strip():
        verbs_add_problem(problems, "FAIL", f"{rel}: guest identity mismatch")
    elif protected and (host["boot_id"].strip() != protected.get("boot_id") or host["machine_id"].strip() != protected.get("machine_id")):
        verbs_add_problem(problems, "FAIL", f"{rel}: endpoint identity does not match protected host identity")
    proc = ep.get("process")
    if not isinstance(proc, dict) or proc.get("exe_sha256") != EXPECTED_RPING_SHA or not is_positive_int(proc.get("pid")) or not is_positive_int(proc.get("start_ticks")):
        verbs_add_problem(problems, "FAIL", f"{rel}: live rping identity missing")
    inv = ep.get("inventory")
    if not isinstance(inv, dict) or inv.get("binary", {}).get("sha256") != EXPECTED_RPING_SHA:
        verbs_add_problem(problems, "FAIL", f"{rel}: pinned rping inventory missing")
    else:
        sysfs = inv.get("sysfs", {})
        if not isinstance(sysfs, dict) or str(sysfs.get("eth0_mtu", "")).strip() != "1500":
            verbs_add_problem(problems, "FAIL", f"{rel}: MTU identity missing")
        gid = str(sysfs.get("gid", "")).strip().lower()
        try:
            if IPv6Address(gid).ipv4_mapped != ip_address(node_info["ip"]):
                verbs_add_problem(problems, "FAIL", f"{rel}: GID does not match endpoint IPv4")
        except ValueError:
            verbs_add_problem(problems, "FAIL", f"{rel}: malformed GID")
        if not re.search(r"link rxe0/\d+ state ACTIVE physical_state LINK_UP netdev eth0", inv.get("rdma_link", {}).get("stdout", "")):
            verbs_add_problem(problems, "FAIL", f"{rel}: active rxe0 link missing")
    after = ep.get("resources_after")
    owned = {proc["pid"]} if isinstance(proc, dict) and is_positive_int(proc.get("pid")) else set()
    if not verbs_no_probe_resources(after, owned):
        verbs_add_problem(problems, "FAIL", f"{rel}: rping resource cleanup missing")


def verbs_runtime_sources_ok(root: Path, refs: dict[str, str], prefix: str, problems: list[dict[str, str]]) -> None:
    for rel, expected, label in [
        (f"{prefix}/inputs/env_verbs-checker.py", EXPECTED_VERBS_CHECKER_SHA, "checker source"),
        (f"{prefix}/inputs/env_verbs.py", EXPECTED_VERBS_COLLECTOR_SHA, "collector source"),
        (f"{prefix}/inputs/observe.py", EXPECTED_VERBS_OBSERVER_SHA, "observer source"),
    ]:
        text = require_verbs_text(root, refs, rel, problems)
        if text is not None and hashlib.sha256(text.encode("utf-8")).hexdigest() != expected:
            verbs_add_problem(problems, "FAIL", f"{label} is not the frozen supported source")
    local_probe = Path(__file__).resolve().parent / "probes" / "env_verbs.py"
    if not local_probe.is_file():
        verbs_add_problem(problems, "BLOCKED", "local verbs checker source is missing")
    elif sha256_file(local_probe) != EXPECTED_VERBS_CHECKER_SHA or verbs_probe is None or not hasattr(verbs_probe, "evaluate_pair"):
        verbs_add_problem(problems, "FAIL", "local verbs checker is not the supported final checker")
    for node in NETWORK_NODES:
        for rel, expected in [(f"{prefix}/runtime/{node}/env_verbs.py", EXPECTED_VERBS_COLLECTOR_SHA), (f"{prefix}/runtime/{node}/observe.py", EXPECTED_VERBS_OBSERVER_SHA)]:
            text = require_verbs_text(root, refs, rel, problems)
            if text is not None and hashlib.sha256(text.encode("utf-8")).hexdigest() != expected:
                verbs_add_problem(problems, "FAIL", f"{rel}: unexpected runtime source")


def verbs_validate_manifest(root: Path, refs: dict[str, str], prefix: str, manifest_rel: str, problems: list[dict[str, str]]) -> dict[str, str] | None:
    manifest = require_verbs_json(root, refs, manifest_rel, problems)
    if manifest is None:
        return None
    files = manifest.get("files")
    if manifest.get("algorithm") != "sha256" or not isinstance(files, dict) or type(manifest.get("file_count")) is not int or manifest.get("file_count") != len(files):
        verbs_add_problem(problems, "FAIL", f"{manifest_rel}: malformed manifest")
        return None
    clean: dict[str, str] = {}
    for rel, digest in files.items():
        if not isinstance(rel, str) or not is_sha256_hex(digest):
            verbs_add_problem(problems, "FAIL", f"{manifest_rel}: malformed file entry")
            continue
        full_rel = f"{prefix}/{rel}"
        if full_rel not in refs:
            verbs_add_problem(problems, "BLOCKED", f"{full_rel}: missing artifact reference")
            continue
        path = checked_path(root, full_rel)
        if not path.is_file():
            verbs_add_problem(problems, "BLOCKED", f"{full_rel}: missing file")
            continue
        actual = sha256_file(path)
        if actual != digest or refs[full_rel] != digest:
            verbs_add_problem(problems, "FAIL", f"{full_rel}: manifest/reference/content sha mismatch")
        clean[rel] = digest
    return clean


def verbs_validate_matrix(matrix: dict[str, Any], problems: list[dict[str, str]]) -> None:
    rows = matrix.get("pairs")
    if matrix.get("probe_sha256") != EXPECTED_VERBS_COLLECTOR_SHA or not isinstance(rows, list):
        verbs_add_problem(problems, "FAIL", "matrix does not bind the frozen collector and pair list")
        return
    got: list[tuple[str, str, int]] = []
    for row in rows:
        if not isinstance(row, dict) or row.get("client") not in NETWORK_NODES or row.get("server") not in NETWORK_NODES or row.get("client") == row.get("server") or type(row.get("size")) is not int:
            verbs_add_problem(problems, "FAIL", "matrix contains malformed pair")
            continue
        got.append((row["client"], row["server"], row["size"]))
    if sorted(got) != sorted(verbs_expected_pairs()):
        verbs_add_problem(problems, "FAIL", "matrix pair set is not the fixed 12 directions plus A-B large run")


def verbs_validate_runs(rows: list[dict[str, Any]], problems: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    runs: dict[str, dict[str, Any]] = {}
    if len(rows) != len(verbs_expected_pairs()):
        verbs_add_problem(problems, "FAIL", "runs.jsonl does not contain exactly 13 runs")
    for row in rows:
        if not isinstance(row, dict):
            verbs_add_problem(problems, "FAIL", "runs.jsonl contains non-object row")
            continue
        client, server, size = row.get("client"), row.get("server"), row.get("size")
        label = row.get("label")
        run_id = row.get("run_id")
        if client not in NETWORK_NODES or server not in NETWORK_NODES or type(size) is not int or client == server or label != verbs_label(client, server, size) or not valid_uuid(run_id):
            verbs_add_problem(problems, "FAIL", f"runs.jsonl malformed run: {label!r}")
            continue
        if label in runs:
            verbs_add_problem(problems, "FAIL", f"runs.jsonl duplicate run label: {label}")
            continue
        if row.get("port") != 19669 or row.get("count") != 3 or row.get("client_exit") != 0 or row.get("server_exit") != 0:
            verbs_add_problem(problems, "FAIL", f"{label}: run parameters or exits are invalid")
        if row.get("client_report") != f"/tmp/afs-verbs-v69/pairs/{label}/{run_id}-client.json" or row.get("server_report") != f"/tmp/afs-verbs-v69/pairs/{label}/{run_id}-server.json":
            verbs_add_problem(problems, "FAIL", f"{label}: report paths are not fixed")
        runs[label] = row
    expected_labels = {verbs_label(c, s, z) for c, s, z in verbs_expected_pairs()}
    if set(runs) != expected_labels:
        verbs_add_problem(problems, "FAIL", "runs.jsonl label set does not match expected pair matrix")
    return runs


def verbs_validate_commands(commands: list[dict[str, Any]], runs: dict[str, dict[str, Any]], negative: dict[str, Any] | None, problems: list[dict[str, str]]) -> None:
    if not verbs_command_rows_valid(commands, problems):
        return
    for node in NETWORK_NODES:
        if not any(verbs_exact_install_command(row, node) for row in commands):
            verbs_add_problem(problems, "FAIL", f"missing exact verbs collector install transcript for {node}")
        if not any(verbs_exact_observe_after_command(row, node) for row in commands):
            verbs_add_problem(problems, "FAIL", f"missing exact protected-after observer transcript for {node}")
    for label, run in runs.items():
        client, server, size, run_id = run["client"], run["server"], run["size"], run["run_id"]
        output = f"/tmp/afs-verbs-v69/pairs/{label}"
        if not any(verbs_exact_listener_command(row, server, label, run_id) for row in commands):
            verbs_add_problem(problems, "FAIL", f"{label}: missing exact rdma_listen transcript")
        if not any(verbs_exact_probe_command(row, "client", client, NETWORK_NODES[client]["ip"], NETWORK_NODES[server]["ip"], size, run_id, output, 0, 25) for row in commands):
            verbs_add_problem(problems, "FAIL", f"{label}: missing exact client run transcript")
        if not any(verbs_exact_probe_command(row, "server", server, NETWORK_NODES[server]["ip"], NETWORK_NODES[client]["ip"], size, run_id, output, 0, 25) for row in commands):
            verbs_add_problem(problems, "FAIL", f"{label}: missing exact server run transcript")
    if negative is not None and valid_uuid(negative.get("run_id")):
        run_id = negative["run_id"]
        if not any(verbs_exact_probe_command(row, "client", "a", NETWORK_NODES["a"]["ip"], NETWORK_NODES["b"]["ip"], 256, run_id, "/tmp/afs-verbs-v69/negative-no-listener", 255, 2) for row in commands):
            verbs_add_problem(problems, "FAIL", "missing exact negative no-listener transcript")


def verbs_validate_pair(root: Path, refs: dict[str, str], prefix: str, run: dict[str, Any], problems: list[dict[str, str]], protected_ids: dict[str, dict[str, str]], commands: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    client, server, size, run_id = run["client"], run["server"], run["size"], run["run_id"]
    label = verbs_label(client, server, size)
    client_rel = verbs_runtime_report_rel(prefix, client, label, run_id, "client")
    server_rel = verbs_runtime_report_rel(prefix, server, label, run_id, "server")
    client_raw_rel = verbs_runtime_raw_rel(prefix, client, label, run_id, "client")
    server_raw_rel = verbs_runtime_raw_rel(prefix, server, label, run_id, "server")
    client_ep = verbs_read_report(root, refs, client_rel, client_raw_rel, problems)
    server_ep = verbs_read_report(root, refs, server_rel, server_raw_rel, problems)
    if client_ep is None or server_ep is None:
        return None
    verbs_endpoint_precheck(client_ep, client, server, "client", run_id, size, problems, client_rel, protected_ids)
    verbs_endpoint_precheck(server_ep, server, client, "server", run_id, size, problems, server_rel, protected_ids)
    for role, node, peer, endpoint in (("client", client, server, client_ep), ("server", server, client, server_ep)):
        if commands is not None and not any(verbs_exact_probe_command(row, role, node, NETWORK_NODES[node]["ip"], NETWORK_NODES[peer]["ip"], size, run_id, f"/tmp/afs-verbs-v69/pairs/{label}", 0, 25) and verbs_command_covers_endpoint(row, endpoint) for row in commands):
            verbs_add_problem(problems, "FAIL", f"{label}: exact {role} transcript does not cover endpoint time")
    if verbs_probe is None:
        verbs_add_problem(problems, "BLOCKED", "local verbs checker module is unavailable")
        return None
    result = verbs_probe.evaluate_pair(copy.deepcopy(client_ep), copy.deepcopy(server_ep))
    if not isinstance(result, dict) or result.get("status") != "PASS":
        verbs_add_problem(problems, "FAIL", f"{label}: verbs checker rejected pair")
    desc = result.get("descriptor_sha256") if isinstance(result, dict) else None
    if not isinstance(desc, list) or len(desc) != 6 or any(not is_sha256_hex(item) for item in desc):
        verbs_add_problem(problems, "FAIL", f"{label}: descriptor receipts are incomplete")
    return result if isinstance(result, dict) else None


def verbs_validate_negative(root: Path, refs: dict[str, str], prefix: str, negative: dict[str, Any], problems: list[dict[str, str]], protected_ids: dict[str, dict[str, str]], commands: list[dict[str, Any]] | None) -> None:
    run_id = negative.get("run_id")
    if not valid_uuid(run_id) or negative.get("returncode") != 255 or negative.get("report") != f"/tmp/afs-verbs-v69/negative-no-listener/{run_id}-client.json":
        verbs_add_problem(problems, "FAIL", "negative.json does not describe the fixed no-listener failure")
        return
    rel = verbs_negative_report_rel(prefix, run_id)
    raw_rel = verbs_negative_raw_rel(prefix, run_id)
    ep = verbs_read_report(root, refs, rel, raw_rel, problems)
    if ep is None:
        return
    verbs_endpoint_precheck(ep, "a", "b", "client", run_id, 256, problems, rel, protected_ids, returncode=255, timeout=2)
    if commands is not None and not any(verbs_exact_probe_command(row, "client", "a", NETWORK_NODES["a"]["ip"], NETWORK_NODES["b"]["ip"], 256, run_id, "/tmp/afs-verbs-v69/negative-no-listener", 255, 2) and verbs_command_covers_endpoint(row, ep) for row in commands):
        verbs_add_problem(problems, "FAIL", "exact negative transcript does not cover endpoint time")
    if ep.get("role") != "client" or ep.get("bind") != NETWORK_NODES["a"]["ip"] or ep.get("peer") != NETWORK_NODES["b"]["ip"] or ep.get("port") != 19669 or ep.get("size") != 256 or ep.get("count") != 3 or ep.get("returncode") != 255 or ep.get("timed_out") is not False or ep.get("timeout_seconds") != 2:
        verbs_add_problem(problems, "FAIL", "negative endpoint parameters are invalid")
    raw = ep.get("raw_log")
    if not isinstance(raw, str) or "RDMA_CM_EVENT_REJECTED" not in raw:
        verbs_add_problem(problems, "FAIL", "negative raw log does not contain the real reject event")
    forbidden = ("ping data", "server ping data", "rdma read completion", "rdma write completion")
    if isinstance(raw, str) and any(token in raw for token in forbidden):
        verbs_add_problem(problems, "FAIL", "negative raw log contains successful payload/completion evidence")


def verbs_no_probe_resources(value: Any, owned_pids: set[int] | None = None) -> bool:
    if not isinstance(value, dict):
        return False
    cm = value.get("rdma_cm_id_json") or value.get("cm_id")
    qp = value.get("rdma_qp_json") or value.get("qp")
    resource = value.get("rdma_resource")
    if not isinstance(cm, dict) or not isinstance(qp, dict):
        return False
    rows = {}
    for name, record in (("cm_id", cm), ("qp", qp)):
        if type(record.get("returncode")) is not int or record["returncode"] != 0 or not isinstance(record.get("stdout"), str):
            return False
        try:
            rows[name] = json.loads(record["stdout"])
        except json.JSONDecodeError:
            return False
        if not isinstance(rows[name], list) or any(not isinstance(row, dict) for row in rows[name]):
            return False
    if rows["cm_id"] or any(row.get("comm") == "rping" or row.get("pid") in (owned_pids or set()) for row in rows["qp"]):
        return False
    if isinstance(resource, dict):
        text = str(resource.get("stdout", ""))
        if type(resource.get("returncode")) is not int or resource["returncode"] != 0 or " rping" in text or "cm_id 0" not in text:
            return False
    return True



def verbs_valid_protected_processes(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    for proc in value:
        if not isinstance(proc, dict):
            return False
        if not is_positive_int(proc.get("pid")) or not is_positive_int(proc.get("startticks")):
            return False
        if not isinstance(proc.get("exe"), str) or not proc["exe"].startswith("/") or not is_sha256_hex(proc.get("binary_sha256")):
            return False
        if not isinstance(proc.get("argv"), list) or any(not isinstance(arg, str) for arg in proc["argv"]):
            return False
        configs = proc.get("configs")
        if not isinstance(configs, dict) or any(not isinstance(path, str) or not path.startswith("/") or not is_sha256_hex(digest) for path, digest in configs.items()):
            return False
    return True


def verbs_protected_identities(root: Path, refs: dict[str, str], prefix: str, problems: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    identities: dict[str, dict[str, str]] = {}
    for node in NETWORK_NODES:
        before = require_verbs_json(root, refs, f"{prefix}/runtime/{node}/protected-before.json", problems)
        after = require_verbs_json(root, refs, f"{prefix}/runtime/{node}/protected-after.json", problems)
        if before is None or after is None:
            continue
        if before.get("boot_id") == after.get("boot_id") and before.get("machine_id") == after.get("machine_id"):
            identities[node] = {"boot_id": before.get("boot_id", ""), "machine_id": before.get("machine_id", "")}
    return identities

def verbs_validate_protected(root: Path, refs: dict[str, str], prefix: str, problems: list[dict[str, str]], min_start: float | None, max_end: float | None) -> None:
    for node, info in NETWORK_NODES.items():
        before = require_verbs_json(root, refs, f"{prefix}/runtime/{node}/protected-before.json", problems)
        after = require_verbs_json(root, refs, f"{prefix}/runtime/{node}/protected-after.json", problems)
        if before is None or after is None:
            continue
        for label, item in (("before", before), ("after", after)):
            if item.get("schema_version") != 1 or item.get("hostname") != info["hostname"] or item.get("kernel") != EXPECTED_KERNEL or not isinstance(item.get("boot_id"), str) or not item["boot_id"] or not isinstance(item.get("machine_id"), str) or not item["machine_id"] or not is_positive_int(item.get("observed_unix_ns")):
                verbs_add_problem(problems, "FAIL", f"{node} protected {label}: invalid identity")
            if not verbs_valid_protected_processes(item.get("processes")) or not isinstance(item.get("afs_mounts"), list) or any(not isinstance(mount, str) for mount in item.get("afs_mounts", [])):
                verbs_add_problem(problems, "FAIL", f"{node} protected {label}: invalid processes or mounts")
        if before.get("boot_id") != after.get("boot_id") or before.get("machine_id") != after.get("machine_id"):
            verbs_add_problem(problems, "FAIL", f"{node}: protected host identity changed")
        if before.get("processes") != after.get("processes") or before.get("afs_mounts") != after.get("afs_mounts"):
            verbs_add_problem(problems, "FAIL", f"{node}: protected processes or mounts were not preserved")
        if min_start is not None and before.get("observed_unix_ns", 10**30) > int(min_start * 1_000_000_000):
            verbs_add_problem(problems, "FAIL", f"{node}: protected-before was captured after run start")
        if max_end is not None and after.get("observed_unix_ns", 0) < int(max_end * 1_000_000_000):
            verbs_add_problem(problems, "FAIL", f"{node}: protected-after was captured before run end")
        commands = after.get("commands")
        if not isinstance(commands, dict) or commands.get("cm_id", {}).get("stdout") != "[]\n" or not re.search(r"link rxe0/\d+ state ACTIVE", commands.get("rdma_link", {}).get("stdout", "")):
            verbs_add_problem(problems, "FAIL", f"{node}: protected-after RDMA cleanup/link evidence is invalid")
        if not verbs_no_probe_resources(commands):
            verbs_add_problem(problems, "FAIL", f"{node}: protected-after contains probe resources")


def evaluate_verbs(bundle: dict[str, Any], artifact_root: Path, refs: dict[str, str]) -> dict[str, Any]:
    try:
        return _evaluate_verbs(bundle, artifact_root, refs)
    except (InvalidEvidence, OSError) as exc:
        return check("cross-vm-verbs", "BLOCKED", str(exc))
    except (TypeError, ValueError, AttributeError, KeyError, OverflowError) as exc:
        return check("cross-vm-verbs", "FAIL", f"malformed verbs observation: {exc}")


def _evaluate_verbs(bundle: dict[str, Any], artifact_root: Path, refs: dict[str, str]) -> dict[str, Any]:
    verbs = bundle.get("verbs")
    if not isinstance(verbs, dict):
        return check("cross-vm-verbs", "BLOCKED", "verbs evidence bundle is missing")
    prefix = verbs.get("prefix")
    if prefix != "verbs":
        return check("cross-vm-verbs", "BLOCKED", "verbs.prefix must be 'verbs'", {"prefix": prefix})
    problems: list[dict[str, str]] = []
    evidence: dict[str, Any] = {
        "scope": "hash-bound cross-VM verbs preparation only",
        "limitations": [
            "observer protected-before capture has no historical command transcript in this bundle",
            "stock rping server source authorization is limited to endpoint route/GID and descriptor receipt evidence",
            "this does not qualify full ENV, watchdog recovery, product transport integration, or release acceptance",
        ],
    }
    fields = {
        "manifest": "artifacts-manifest.json",
        "matrix": "matrix.json",
        "runs": "runs.jsonl",
        "negative": "negative.json",
        "commands": "commands.jsonl",
        "checker_source": "inputs/env_verbs-checker.py",
        "collector_source": "inputs/env_verbs.py",
        "observer_source": "inputs/observe.py",
        "protected": "protected-files.json",
        "audit": "audit.json",
    }
    rels: dict[str, str] = {}
    for field, default_suffix in fields.items():
        rel = verbs_rel(verbs, prefix, field, default_suffix)
        if not isinstance(rel, str):
            return check("cross-vm-verbs", "BLOCKED", f"verbs.{field} is malformed")
        checked_path(artifact_root, rel)
        rels[field] = rel
        if rel not in refs:
            verbs_add_problem(problems, "BLOCKED", f"{rel}: missing artifact reference")

    manifest_files = verbs_validate_manifest(artifact_root, refs, prefix, rels["manifest"], problems)
    verbs_runtime_sources_ok(artifact_root, refs, prefix, problems)

    matrix = require_verbs_json(artifact_root, refs, rels["matrix"], problems)
    if matrix is not None:
        verbs_validate_matrix(matrix, problems)
    run_rows = require_verbs_jsonl(artifact_root, refs, rels["runs"], problems)
    runs = verbs_validate_runs(run_rows, problems) if run_rows is not None else {}
    negative = require_verbs_json(artifact_root, refs, rels["negative"], problems)
    commands = require_verbs_jsonl(artifact_root, refs, rels["commands"], problems)
    if commands is not None:
        verbs_validate_commands(commands, runs, negative, problems)

    audit = require_verbs_json(artifact_root, refs, rels["audit"], problems)
    if audit is not None:
        summary = audit.get("summary")
        checks = audit.get("checks")
        if not isinstance(summary, dict) or summary.get("FAIL") != 0 or summary.get("PASS") != 127 or not isinstance(checks, list) or any(not isinstance(item, dict) or item.get("status") != "PASS" for item in checks):
            verbs_add_problem(problems, "FAIL", "audit summary is not the fixed successful checker output")

    protected = require_verbs_json(artifact_root, refs, rels["protected"], problems)
    if protected is not None and not isinstance(protected.get("files"), dict):
        verbs_add_problem(problems, "FAIL", "protected-files.json is malformed")

    protected_ids = verbs_protected_identities(artifact_root, refs, prefix, problems)
    min_start: float | None = None
    max_end: float | None = None
    pair_results = 0
    large_pair = False
    for label in sorted(runs):
        run = runs[label]
        result = verbs_validate_pair(artifact_root, refs, prefix, run, problems, protected_ids, commands)
        if result is not None and result.get("status") == "PASS":
            pair_results += 1
        if run.get("size") == 65535:
            large_pair = True
        for role, node in (("client", run["client"]), ("server", run["server"])):
            rel = verbs_runtime_report_rel(prefix, node, label, run["run_id"], role)
            status, ep = read_json_ref(artifact_root, refs, rel)
            if status == "OK" and isinstance(ep, dict):
                start, end = ep.get("started_unix"), ep.get("ended_unix")
                if valid_finite_time(start):
                    min_start = float(start) if min_start is None else min(min_start, float(start))
                if valid_finite_time(end):
                    max_end = float(end) if max_end is None else max(max_end, float(end))
    if negative is not None:
        verbs_validate_negative(artifact_root, refs, prefix, negative, problems, protected_ids, commands)
        rel = verbs_negative_report_rel(prefix, negative.get("run_id", "")) if valid_uuid(negative.get("run_id")) else None
        if rel:
            status, ep = read_json_ref(artifact_root, refs, rel)
            if status == "OK" and isinstance(ep, dict):
                for key, reducer in (("started_unix", min), ("ended_unix", max)):
                    value = ep.get(key)
                    if valid_finite_time(value):
                        if key == "started_unix":
                            min_start = float(value) if min_start is None else min(min_start, float(value))
                        else:
                            max_end = float(value) if max_end is None else max(max_end, float(value))
    verbs_validate_protected(artifact_root, refs, prefix, problems, min_start, max_end)

    evidence.update({
        "manifest_files": len(manifest_files) if manifest_files is not None else None,
        "pair_count": pair_results,
        "large_pair": large_pair,
        "problems": problems[:20],
    })
    status = verbs_status_from(problems)
    detail = "hash-bound cross-VM verbs preparation validated" if status == "PASS" else "verbs evidence is incomplete or inconsistent"
    return check("cross-vm-verbs", status, detail, evidence)


def readiness_ref(bundle: dict[str, Any], target: str) -> dict[str, Any] | None:
    readiness = bundle.get("readiness_evidence")
    if not isinstance(readiness, dict):
        return None
    value = readiness.get(target)
    return value if isinstance(value, dict) else None


def load_readiness_json(artifact_root: Path, refs: dict[str, str], rel: Any, problems: list[dict[str, str]], label: str) -> dict[str, Any] | None:
    if not isinstance(rel, str):
        problems.append({"status": "BLOCKED", "detail": f"{label}: missing relative path"})
        return None
    status, value = read_json_ref(artifact_root, refs, rel)
    if status == "OK" and isinstance(value, dict):
        return value
    problems.append({"status": "BLOCKED" if status == "MISSING" else "FAIL", "detail": f"{rel}: {status}"})
    return None


def artifact_entry_sha(entry: Any) -> str | None:
    if isinstance(entry, str):
        return entry if is_sha256_hex(entry) else None
    if isinstance(entry, dict) and is_sha256_hex(entry.get("sha256")):
        return entry["sha256"]
    return None


def artifact_manifest_has(manifest: dict[str, Any], required: list[str], problems: list[dict[str, str]], label: str) -> None:
    files = manifest.get("files")
    if not isinstance(files, dict):
        problems.append({"status": "FAIL", "detail": f"{label}: artifact manifest lacks files object"})
        return
    for rel in required:
        if artifact_entry_sha(files.get(rel)) is None:
            problems.append({"status": "FAIL", "detail": f"{label}: missing hash for {rel}"})


def validate_manifest_files(base: Path, manifest: dict[str, Any], problems: list[dict[str, str]], label: str) -> None:
    files = manifest.get("files")
    if not isinstance(files, dict):
        problems.append({"status": "FAIL", "detail": f"{label}: artifact manifest lacks files object"})
        return
    base_resolved = base.resolve()
    for rel, entry in files.items():
        if not isinstance(rel, str) or not rel or "\x00" in rel or Path(rel).is_absolute() or ".." in Path(rel).parts:
            problems.append({"status": "FAIL", "detail": f"{label}: invalid artifact path {rel!r}"})
            continue
        path = base / rel
        try:
            current = path
            symlink = False
            while True:
                if current.is_symlink():
                    symlink = True
                    break
                if current == base or current.parent == current:
                    break
                current = current.parent
            if symlink:
                problems.append({"status": "FAIL", "detail": f"{label}: symlink rejected for {rel}"})
                continue
            resolved = path.resolve()
            if not resolved.is_relative_to(base_resolved):
                problems.append({"status": "FAIL", "detail": f"{label}: artifact escapes packet root {rel}"})
                continue
            if not path.is_file():
                problems.append({"status": "BLOCKED", "detail": f"{label}: missing artifact {rel}"})
                continue
            expected_sha = artifact_entry_sha(entry)
            expected_bytes = entry.get("bytes") if isinstance(entry, dict) else None
            if expected_sha is None:
                problems.append({"status": "FAIL", "detail": f"{label}: malformed sha256 for {rel}"})
                continue
            actual_bytes = path.stat().st_size
            if type(expected_bytes) is int and actual_bytes != expected_bytes:
                problems.append({"status": "FAIL", "detail": f"{label}: byte count mismatch for {rel}"})
            actual_sha = sha256_file(path)
            if actual_sha != expected_sha:
                problems.append({"status": "FAIL", "detail": f"{label}: sha256 mismatch for {rel}"})
        except OSError as exc:
            problems.append({"status": "BLOCKED", "detail": f"{label}: cannot read {rel}: {exc}"})


def load_packet_json(packet_root: Path, rel: str, problems: list[dict[str, str]], label: str) -> dict[str, Any] | None:
    path = packet_root / rel
    if not path.is_file():
        problems.append({"status": "BLOCKED", "detail": f"{label}: missing raw JSON {rel}"})
        return None
    return load_readiness_json(packet_root, {rel: sha256_file(path)}, rel, problems, label)


def validate_audit_ref_hash(artifact_root: Path, refs: dict[str, str], audit_rel: str, manifest: dict[str, Any], manifest_key: str | None, problems: list[dict[str, str]]) -> None:
    if manifest_key is None:
        return
    files = manifest.get("files")
    expected = artifact_entry_sha(files.get(manifest_key) if isinstance(files, dict) else None)
    if expected is None:
        problems.append({"status": "FAIL", "detail": f"artifact manifest missing audit hash for {manifest_key}"})
        return
    if sha256_file(checked_path(artifact_root, audit_rel)) != expected:
        problems.append({"status": "FAIL", "detail": "audit.json hash differs from artifact manifest"})


def safe_tar_entries(path: Path, problems: list[dict[str, str]], label: str) -> dict[str, bytes]:
    entries: dict[str, bytes] = {}
    try:
        with tarfile.open(path, "r:gz") as archive:
            total = 0
            for member in archive.getmembers():
                pure = Path(member.name)
                if pure.is_absolute() or ".." in pure.parts or not member.name:
                    problems.append({"status": "FAIL", "detail": f"{label}: unsafe tar path {member.name!r}"})
                    continue
                if member.isdir():
                    continue
                if not member.isfile() or member.issym() or member.islnk() or member.isdev():
                    problems.append({"status": "FAIL", "detail": f"{label}: non-regular tar member {member.name}"})
                    continue
                if member.name in entries:
                    problems.append({"status": "FAIL", "detail": f"{label}: duplicate tar member {member.name}"})
                    continue
                total += member.size
                if total > 256 * 1024 * 1024:
                    problems.append({"status": "FAIL", "detail": f"{label}: tar exceeds bounded audit size"})
                    break
                handle = archive.extractfile(member)
                if handle is None:
                    problems.append({"status": "FAIL", "detail": f"{label}: unreadable tar member {member.name}"})
                    continue
                entries[member.name] = handle.read()
    except (OSError, tarfile.TarError) as exc:
        problems.append({"status": "BLOCKED", "detail": f"{label}: cannot open tar: {exc}"})
    return entries


def tar_json(entries: dict[str, bytes], member: str, problems: list[dict[str, str]], label: str) -> dict[str, Any] | None:
    raw = entries.get(member)
    if raw is None:
        problems.append({"status": "FAIL", "detail": f"{label}: missing tar JSON {member}"})
        return None
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        problems.append({"status": "FAIL", "detail": f"{label}: malformed tar JSON {member}: {exc}"})
        return None
    if not isinstance(value, dict):
        problems.append({"status": "FAIL", "detail": f"{label}: tar JSON {member} must be object"})
        return None
    return value


def readiness_boundary_ok(record: dict[str, Any]) -> bool:
    return record.get("status") == "PASS" and record.get("formal_acceptance") == "NOT_RUN" and record.get("environment") == "PREPARING"


def tar_action_rows(entries: dict[str, bytes], problems: list[dict[str, str]], label: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for member, raw in entries.items():
        if not member.endswith(".json"):
            continue
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(value, dict) and isinstance(value.get("action"), str):
            rows.append({"member": member, "value": value})
    if not rows:
        problems.append({"status": "FAIL", "detail": f"{label}: no action JSON receipts in raw archive"})
    return rows


def validate_backend_raw(packet_root: Path, audit: dict[str, Any], problems: list[dict[str, str]]) -> None:
    product_sha = {
        "node": "a3fe6573fc5f5a41c30b823855cfe2fdd1428950f878f1e29756e7bfc0d7e9d9",
        "meta": "895f39fd660b7f7d9735eaaa4f3a082c3692a409d954c4aa8b5b0cc515a700ad",
    }
    lanes = audit.get("lanes")
    if not isinstance(lanes, dict):
        return
    for backend in ("etcd", "redis"):
        for role in ("ctl", "a", "b"):
            rel = f"{backend}/{role}/runtime-raw.tar.gz"
            entries = safe_tar_entries(packet_root / rel, problems, f"backend {backend}/{role}")
            if not entries:
                continue
            config = "etc/meta.toml" if role == "ctl" else "etc/node.toml"
            if config not in entries:
                problems.append({"status": "FAIL", "detail": f"backend {backend}/{role}: missing {config}"})
            rows = tar_action_rows(entries, problems, f"backend {backend}/{role}")
            actions = [row["value"].get("action") for row in rows if readiness_boundary_ok(row["value"])]
            if actions.count("start") != 2 or actions.count("stop") != 2 or actions.count("cleanup") != 1:
                problems.append({"status": "FAIL", "detail": f"backend {backend}/{role}: normal start/stop/cleanup receipts incomplete"})
            expected_role = "meta" if role == "ctl" else "node"
            for row in rows:
                value = row["value"]
                if not readiness_boundary_ok(value):
                    problems.append({"status": "FAIL", "detail": f"backend {backend}/{role}: receipt boundary mismatch {row['member']}"})
                    continue
                action = value.get("action")
                result = value.get("result")
                identity = result.get("identity") if isinstance(result, dict) else None
                if action in {"start", "restart"}:
                    if not isinstance(identity, dict):
                        problems.append({"status": "FAIL", "detail": f"backend {backend}/{role}: product identity missing for {action}"})
                    elif identity.get("sha256") != product_sha[expected_role]:
                        problems.append({"status": "FAIL", "detail": f"backend {backend}/{role}: product binary sha mismatch"})
                if action == "stop":
                    controller = result.get("controller") if isinstance(result, dict) else None
                    if not isinstance(controller, dict) or controller.get("exit") != 0 or "stopped" not in str(controller.get("stdout", "")):
                        problems.append({"status": "FAIL", "detail": f"backend {backend}/{role}: normal stop receipt malformed"})
                read = result.get("read") if isinstance(result, dict) else None
                if isinstance(read, dict) and (read.get("bytes") != 4194321 or not is_sha256_hex(read.get("sha256"))):
                    problems.append({"status": "FAIL", "detail": f"backend {backend}/{role}: content read receipt malformed"})
                if role != "ctl":
                    physical = result.get("physical") if isinstance(result, dict) else None
                    if physical is not None and (not isinstance(physical, list) or sorted(item.get("chunk", {}).get("bytes") for item in physical if isinstance(item, dict)) != [17, 4194304]):
                        problems.append({"status": "FAIL", "detail": f"backend {backend}/{role}: physical chunk receipts malformed"})
            if role == "ctl":
                retained = tar_json(entries, "evidence/retained-snapshot-restart.json", problems, f"backend {backend}/ctl")
                lane_retained = lanes.get(backend, {}).get("retained_snapshot") if isinstance(lanes.get(backend), dict) else None
                if retained != lane_retained:
                    problems.append({"status": "FAIL", "detail": f"backend {backend}: retained snapshot raw/audit mismatch"})


def validate_moose_raw(packet_root: Path, audit: dict[str, Any], problems: list[dict[str, str]]) -> None:
    archives = {role: safe_tar_entries(packet_root / role / "runtime-raw.tar.gz", problems, f"MooseFS {role}") for role in ("ctl", "a", "b")}
    for item in audit.get("content_receipts", []):
        if not isinstance(item, dict):
            problems.append({"status": "FAIL", "detail": "MooseFS content receipt entry malformed"})
            continue
        role, receipt = item.get("role"), item.get("receipt")
        if role not in archives or not isinstance(receipt, str):
            problems.append({"status": "FAIL", "detail": "MooseFS content receipt role/path malformed"})
            continue
        record = tar_json(archives[role], receipt, problems, f"MooseFS {role}")
        if record is None:
            continue
        if record.get("formal_acceptance") != "NOT_RUN" or record.get("environment") != "PREPARING" or record.get("blocker") != "B001":
            problems.append({"status": "FAIL", "detail": f"MooseFS {receipt}: boundary mismatch"})
        size = record.get("length", record.get("bytes", record.get("size")))
        if item.get("phase") in {"pre", "post", "write"} and (size != 32 * 1024**2 or record.get("sha256") != "626f47ade3da8112941c844a3a1d8a02b94c5e134a9c3391a3375aa22cd1dbc7"):
            problems.append({"status": "FAIL", "detail": f"MooseFS {receipt}: content identity mismatch"})
        if item.get("phase") in {"pre", "post"} and (not isinstance(record.get("ranges"), list) or len(record["ranges"]) != 64):
            problems.append({"status": "FAIL", "detail": f"MooseFS {receipt}: range receipts missing"})
    for role, stop in audit.get("final_normal_stop_receipts", {}).items():
        if role in archives and isinstance(stop, dict) and isinstance(stop.get("receipt"), str):
            record = tar_json(archives[role], stop["receipt"], problems, f"MooseFS {role}")
            if record is not None and (record.get("formal_acceptance") != "NOT_RUN" or record.get("environment") != "PREPARING"):
                problems.append({"status": "FAIL", "detail": f"MooseFS final stop {role}: boundary mismatch"})
    for label, value in audit.get("process_incarnations", {}).items():
        role = label.split("/", 1)[0]
        if role not in archives or not isinstance(value, dict):
            continue
        for key in ("initial_receipt", "restart_receipt"):
            receipt = value.get(key)
            if isinstance(receipt, str):
                record = tar_json(archives[role], receipt, problems, f"MooseFS {label}")
                identity = record.get("identity") if isinstance(record, dict) else None
                top_level_identity = isinstance(record, dict) and all(key in record for key in ("pid", "start_ticks", "boot_id"))
                if record is not None and not isinstance(identity, dict) and not top_level_identity:
                    problems.append({"status": "FAIL", "detail": f"MooseFS {label}: missing raw process identity"})


def payload_3fs() -> bytes:
    return b"".join(hashlib.sha256(f"round3-3fs-v84-{i}".encode()).digest() * 32768 for i in range(32))


def validate_3fs_raw(packet_root: Path, audit: dict[str, Any], problems: list[dict[str, str]]) -> None:
    data = payload_3fs()
    digest = hashlib.sha256(data).hexdigest()
    write = load_packet_json(packet_root, "a/v84-write-r2.json", problems, "3FS write")
    if write is not None:
        if write.get("status") != "PASS" or write.get("bytes") != len(data) or write.get("observed_sha256") != digest:
            problems.append({"status": "FAIL", "detail": "3FS write content receipt mismatch"})
        writes = write.get("writes_1mib")
        if not isinstance(writes, list) or len(writes) != 32:
            problems.append({"status": "FAIL", "detail": "3FS write lacks 32 exact 1MiB writes"})
    for rel in ("a/v84-read.json", "b/v84-read.json", "b/v84-read-after-restart.json"):
        record = load_packet_json(packet_root, rel, problems, f"3FS {rel}")
        if record is None:
            continue
        if record.get("status") != "PASS" or record.get("bytes") != len(data) or record.get("sha256") != digest:
            problems.append({"status": "FAIL", "detail": f"3FS {rel}: content identity mismatch"})
        if not isinstance(record.get("fixed_ranges_exact"), list) or len(record["fixed_ranges_exact"]) != 64:
            problems.append({"status": "FAIL", "detail": f"3FS {rel}: fixed range receipts missing"})
    for plan_rel, expected_slots in (("preparation/physical-plan-before.json", audit.get("physical_slots_before")), ("preparation/physical-plan-after-r3.json", audit.get("physical_slots_after"))):
        plan = load_packet_json(packet_root, plan_rel, problems, f"3FS {plan_rel}")
        if plan is None:
            continue
        roles = plan.get("roles")
        if not isinstance(roles, dict) or sum(len(v) for v in roles.values() if isinstance(v, list)) != expected_slots:
            problems.append({"status": "FAIL", "detail": f"3FS {plan_rel}: physical slot count mismatch"})
    for role in ("ctl", "a", "b", "c"):
        entries = safe_tar_entries(packet_root / role / "runtime-raw.tar.gz", problems, f"3FS {role}")
        if entries and not any(name.startswith("evidence/") or "/evidence/" in name for name in entries):
            problems.append({"status": "FAIL", "detail": f"3FS {role}: raw archive lacks evidence receipts"})


def readiness_status_from(problems: list[dict[str, str]]) -> str:
    if any(problem["status"] == "FAIL" for problem in problems):
        return "FAIL"
    if problems:
        return "BLOCKED"
    return "PASS"


def evaluate_durable_backend_restart(bundle: dict[str, Any], artifact_root: Path, refs: dict[str, str]) -> dict[str, Any]:
    target = "durable-backend-restart"
    spec = readiness_ref(bundle, target)
    if spec is None:
        return check(target, "BLOCKED", "readiness_evidence.durable-backend-restart is missing")
    problems: list[dict[str, str]] = []
    audit_rel = spec.get("audit")
    audit = load_readiness_json(artifact_root, refs, audit_rel, problems, "backend audit")
    manifest = load_readiness_json(artifact_root, refs, spec.get("artifact_hashes"), problems, "backend artifact hashes")
    if audit is not None and manifest is not None and isinstance(audit_rel, str):
        packet_root = checked_path(artifact_root, audit_rel).parent
        validate_manifest_files(packet_root, manifest, problems, "backend artifacts")
        validate_audit_ref_hash(artifact_root, refs, audit_rel, manifest, "audit.json", problems)
        artifact_manifest_has(manifest, [
            "README.md",
            "audit.py",
            "etcd/ctl/runtime-raw.tar.gz",
            "etcd/a/runtime-raw.tar.gz",
            "etcd/b/runtime-raw.tar.gz",
            "redis/ctl/runtime-raw.tar.gz",
            "redis/a/runtime-raw.tar.gz",
            "redis/b/runtime-raw.tar.gz",
            "preparation/etcd-native.json",
            "preparation/redis-native.json",
            "preparation/original-serialization-failure.json",
        ], problems, "backend artifacts")
        checks = audit.get("checks")
        validation = audit.get("validation")
        lanes = audit.get("lanes")
        preserved = audit.get("preserved_failures")
        if audit.get("status") != "PASS" or audit.get("scope") != "RETAINED_SCOPED_NORMAL_BACKEND_INTEGRATION" or audit.get("formal_acceptance") != "NOT_RUN" or audit.get("environment") != "PREPARING":
            problems.append({"status": "FAIL", "detail": "backend audit status/scope boundary mismatch"})
        if not isinstance(checks, list) or len(checks) < 500 or any(not isinstance(item, dict) or item.get("status") != "PASS" for item in checks):
            problems.append({"status": "FAIL", "detail": "backend audit checks are incomplete or failing"})
        if not isinstance(validation, dict) or validation.get("r2", {}).get("tests") != 17:
            problems.append({"status": "FAIL", "detail": "backend Linux helper validation identity is missing"})
        if not isinstance(lanes, dict) or set(lanes) != {"etcd", "redis"}:
            problems.append({"status": "FAIL", "detail": "backend lanes must include exactly etcd and Redis"})
        else:
            for backend in ("etcd", "redis"):
                lane = lanes[backend]
                retained = lane.get("retained_snapshot") if isinstance(lane, dict) else None
                guests = lane.get("guests") if isinstance(lane, dict) else None
                if not isinstance(retained, dict) or retained.get("status") != "PASS" or retained.get("before") != retained.get("after"):
                    problems.append({"status": "FAIL", "detail": f"{backend}: retained native snapshot mismatch"})
                if not isinstance(guests, dict) or set(guests) != {"ctl", "a", "b"}:
                    problems.append({"status": "FAIL", "detail": f"{backend}: guest lane set mismatch"})
                    continue
                for role in ("ctl", "a", "b"):
                    guest = guests[role]
                    if not isinstance(guest, dict) or type(guest.get("final_available_bytes")) is not int or guest["final_available_bytes"] < 4 * GIB:
                        problems.append({"status": "FAIL", "detail": f"{backend}/{role}: final ext4 reserve missing"})
        if not isinstance(preserved, list) or len(preserved) != 1 or preserved[0].get("classification") != "ORIGINAL_COLLECTOR_FAILURE":
            problems.append({"status": "FAIL", "detail": "expected original collector failure is not retained exactly once"})
        validate_backend_raw(packet_root, audit, problems)
    status = readiness_status_from(problems)
    detail = "hash-bound etcd/Redis normal backend restart evidence validated" if status == "PASS" else "durable backend restart evidence is incomplete or inconsistent"
    return check(target, status, detail, {"problems": problems[:20]})


def evaluate_actual_moosefs_mount_io(bundle: dict[str, Any], artifact_root: Path, refs: dict[str, str]) -> dict[str, Any]:
    target = "actual-moosefs-mount-io"
    spec = readiness_ref(bundle, target)
    if spec is None:
        return check(target, "BLOCKED", "readiness_evidence.actual-moosefs-mount-io is missing")
    problems: list[dict[str, str]] = []
    audit_rel = spec.get("audit")
    audit = load_readiness_json(artifact_root, refs, audit_rel, problems, "MooseFS audit")
    manifest = load_readiness_json(artifact_root, refs, spec.get("artifact_hashes"), problems, "MooseFS artifact hashes")
    if audit is not None and manifest is not None and isinstance(audit_rel, str):
        packet_root = checked_path(artifact_root, audit_rel).parent
        validate_manifest_files(packet_root, manifest, problems, "MooseFS artifacts")
        validate_audit_ref_hash(artifact_root, refs, audit_rel, manifest, "audit.json", problems)
        artifact_manifest_has(manifest, [
            "README.md",
            "audit.py",
            "ctl/runtime-raw.tar.gz",
            "a/runtime-raw.tar.gz",
            "b/runtime-raw.tar.gz",
            "probes/round3-moose-policy.py",
            "probes/r3/round3-moose-read.py",
            "probes/r3/test_round3_moose_read.py",
        ], problems, "MooseFS artifacts")
        receipts = audit.get("content_receipts")
        stops = audit.get("final_normal_stop_receipts")
        incarnations = audit.get("process_incarnations")
        policy = audit.get("goal1_policy")
        if audit.get("result") != "PASS_ARCHIVED_ONE_COPY_POLICY_READ_AND_NORMAL_RESTART_CHECKS" or audit.get("formal_acceptance") != "NOT_RUN" or audit.get("environment") != "PREPARING":
            problems.append({"status": "FAIL", "detail": "MooseFS audit result/boundary mismatch"})
        if audit.get("strong_durable_write") != "BLOCKED" or audit.get("blocker") != "B001" or audit.get("fair_performance_comparison") != "NOT_RUN":
            problems.append({"status": "FAIL", "detail": "MooseFS durability/performance boundary changed"})
        if audit.get("length") != 32 * 1024**2 or audit.get("sha256") != "626f47ade3da8112941c844a3a1d8a02b94c5e134a9c3391a3375aa22cd1dbc7" or audit.get("ranges_per_command") != 64:
            problems.append({"status": "FAIL", "detail": "MooseFS content identity mismatch"})
        if not isinstance(policy, dict) or policy.get("status") != "QUALIFIED_FOR_NEW_V89_FIXTURE" or policy.get("fields", {}).get("create_labels") != "*" or policy.get("fields", {}).get("keep_labels") != "*":
            problems.append({"status": "FAIL", "detail": "MooseFS one-copy class proof missing"})
        if not isinstance(receipts, list) or {(r.get("role"), r.get("phase")) for r in receipts if isinstance(r, dict)} != {("a", "write"), ("a", "pre"), ("a", "post"), ("b", "pre"), ("b", "post")}:
            problems.append({"status": "FAIL", "detail": "MooseFS A/B write/read receipt set mismatch"})
        if not isinstance(stops, dict) or set(stops) != {"ctl", "a", "b"}:
            problems.append({"status": "FAIL", "detail": "MooseFS final normal stop receipts missing"})
        if not isinstance(incarnations, dict) or set(incarnations) != {"ctl/master", "a/chunk", "a/fuse", "b/fuse"}:
            problems.append({"status": "FAIL", "detail": "MooseFS process incarnation set mismatch"})
        else:
            for label, value in incarnations.items():
                before, after = value.get("before"), value.get("after")
                if not isinstance(before, list) or not isinstance(after, list) or len(before) != 5 or len(after) != 5 or before == after or before[2:] != after[2:]:
                    problems.append({"status": "FAIL", "detail": f"MooseFS {label}: restart identity mismatch"})
        validate_moose_raw(packet_root, audit, problems)
    status = readiness_status_from(problems)
    detail = "hash-bound stock MooseFS mount write/read/restart evidence validated" if status == "PASS" else "MooseFS mount IO evidence is incomplete or inconsistent"
    return check(target, status, detail, {"problems": problems[:20]})


def evaluate_actual_3fs_mount_io(bundle: dict[str, Any], artifact_root: Path, refs: dict[str, str]) -> dict[str, Any]:
    target = "actual-3fs-mount-io"
    spec = readiness_ref(bundle, target)
    if spec is None:
        return check(target, "BLOCKED", "readiness_evidence.actual-3fs-mount-io is missing")
    problems: list[dict[str, str]] = []
    audit = load_readiness_json(artifact_root, refs, spec.get("audit"), problems, "3FS audit")
    manifest = load_readiness_json(artifact_root, refs, spec.get("artifact_hashes"), problems, "3FS artifact hashes")
    if audit is not None and manifest is not None:
        audit_rel = spec.get("audit")
        if isinstance(audit_rel, str):
            packet_root = checked_path(artifact_root, audit_rel).parent
            validate_manifest_files(packet_root, manifest, problems, "3FS artifacts")
        else:
            packet_root = artifact_root
        artifact_manifest_has(manifest, [
            "README.md",
            "a/runtime-raw.tar.gz",
            "b/runtime-raw.tar.gz",
            "c/runtime-raw.tar.gz",
            "ctl/runtime-raw.tar.gz",
            "a/v84-write-r2.json",
            "a/v84-read.json",
            "b/v84-read.json",
            "b/v84-read-after-restart.json",
            "preparation/physical-plan-before.json",
            "preparation/physical-plan-after-r3.json",
            "probes/audit.py",
            "probes/round3-3fs.py",
            "probes/round3-3fs-physical.py",
        ], problems, "3FS artifacts")
        if audit.get("status") != "PASS" or audit.get("scope") != "patched-reference normal IO/physical copies/retained-state restart only":
            problems.append({"status": "FAIL", "detail": "3FS audit status/scope mismatch"})
        if audit.get("formal_acceptance") != "NOT_RUN" or audit.get("environment") != "PREPARING" or audit.get("strong_durable_comparison") != "BLOCKED":
            problems.append({"status": "FAIL", "detail": "3FS boundary changed"})
        if audit.get("physical_slots_before") != 192 or audit.get("physical_slots_after") != 192 or audit.get("range_checks") != 192:
            problems.append({"status": "FAIL", "detail": "3FS physical/range proof counts mismatch"})
        if audit.get("compiler_inputs_reused") != 143 or type(audit.get("checks")) is not int or audit["checks"] < 900:
            problems.append({"status": "FAIL", "detail": "3FS audit count/compiler identity mismatch"})
        resources = audit.get("process_resources")
        if not isinstance(resources, dict) or set(resources) != {"ctl", "a", "b", "c"}:
            problems.append({"status": "FAIL", "detail": "3FS process resource roles missing"})
        elif not all(isinstance(resources.get(role), dict) and resources[role] for role in ("ctl", "a", "b", "c")):
            problems.append({"status": "FAIL", "detail": "3FS process resource snapshots incomplete"})
        validate_3fs_raw(packet_root, audit, problems)
    status = readiness_status_from(problems)
    detail = "hash-bound patched 3FS mount write/read/restart evidence validated" if status == "PASS" else "3FS mount IO evidence is incomplete or inconsistent"
    return check(target, status, detail, {"problems": problems[:20]})


def memtotal_bytes(inventory: dict[str, Any]) -> int | None:
    meminfo = inventory.get("meminfo")
    if not isinstance(meminfo, str):
        return None
    for line in meminfo.splitlines():
        if line.startswith("MemTotal:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                return int(parts[1]) * 1024
    return None


def os_is_ubuntu_2404(inventory: dict[str, Any]) -> bool:
    os_release = inventory.get("os_release")
    if not isinstance(os_release, str):
        return False
    fields = dict(line.split("=", 1) for line in os_release.splitlines() if "=" in line and not line.startswith("#"))
    return fields.get("ID", "").strip('"') == "ubuntu" and fields.get("VERSION_ID", "").strip('"') == "24.04"


def address_mtu(inventory: dict[str, Any], expected_ip: str) -> tuple[bool, Any]:
    data = parse_stdout_json(inventory.get("addresses"))
    if not isinstance(data, list):
        return False, "missing ip address JSON"
    for iface in data:
        if isinstance(iface, dict) and iface.get("ifname") == "eth0":
            addresses = iface.get("addr_info")
            if not isinstance(addresses, list):
                return False, "missing addr_info array"
            ips = [a.get("local") for a in addresses if isinstance(a, dict) and a.get("family") == "inet"]
            return expected_ip in ips and iface.get("mtu") == 1500, {"ips": ips, "mtu": iface.get("mtu")}
    return False, "missing eth0"


def flatten_blocks(devices: Any) -> list[dict[str, Any]]:
    flat: list[dict[str, Any]] = []
    if not isinstance(devices, list):
        return flat
    for item in devices:
        if isinstance(item, dict):
            flat.append(item)
            flat.extend(flatten_blocks(item.get("children")))
    return flat


def has_ext4_mount(inventory: dict[str, Any], prefix: str, min_size: int) -> tuple[bool, Any]:
    data = parse_stdout_json(inventory.get("block_layout"))
    devices = flatten_blocks(data.get("blockdevices") if isinstance(data, dict) else None)
    rows = []
    for device in devices:
        mountpoints = device.get("mountpoints")
        mounts = [m for m in mountpoints if isinstance(m, str)] if isinstance(mountpoints, list) else []
        if device.get("fstype") == "ext4" and prefix in mounts:
            rows.append({"name": device.get("name"), "size": device.get("size"), "mountpoints": mounts})
    return any(isinstance(row.get("size"), int) and row["size"] >= min_size for row in rows), rows


def df_available(inventory: dict[str, Any], target: str) -> tuple[int | None, dict[str, Any]]:
    record = inventory.get("disk_space")
    stdout = record.get("stdout", "") if isinstance(record, dict) else ""
    if not isinstance(stdout, str):
        return None, {"error": "disk_space stdout must be text"}
    rows = []
    for line in stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 7:
            row = {"source": parts[0], "fstype": parts[1], "available": parts[4], "target": parts[6]}
            rows.append(row)
            if parts[6] == target and parts[1] == "ext4" and parts[4].isdigit():
                return int(parts[4]), {"row": row, "df_status": record.get("status"), "returncode": record.get("returncode")}
    return None, {"rows": rows, "df_status": record.get("status") if isinstance(record, dict) else None}


def image_digest_ok(row: dict[str, Any], lock: dict) -> bool:
    images = row.get("config", {}).get("images") if isinstance(row.get("config"), dict) else None
    expected = lock.get("image") if isinstance(lock.get("image"), dict) else {}
    locations = [expected.get("url")]
    if isinstance(expected.get("local_cache"), str):
        locations.append("file://" + expected["local_cache"])
    return isinstance(images, list) and any(isinstance(img, dict) and img.get("arch") == "aarch64" and img.get("location") in [p for p in locations if isinstance(p, str)] and img.get("digest") == f"sha256:{EXPECTED_IMAGE_SHA}" for img in images)


def check_contract(lock: dict, bundle: dict, root: Path, checks: list[dict[str, Any]]) -> None:
    expected = lock.get("contract", {}).get("sha256") if isinstance(lock.get("contract"), dict) else None
    contract = bundle.get("contract")
    if not isinstance(expected, str) or len(expected) != 64:
        checks.append(check("acceptance-contract-sha256", "BLOCKED", "lock.contract.sha256 is missing"))
        return
    if not isinstance(contract, dict) or not isinstance(contract.get("path"), str) or not isinstance(contract.get("sha256"), str):
        checks.append(check("acceptance-contract-sha256", "BLOCKED", "bundle.contract {path, sha256} is missing"))
        return
    path = checked_path(root, contract["path"])
    if not path.is_file():
        checks.append(check("acceptance-contract-sha256", "BLOCKED", "bundle contract file is missing"))
        return
    actual = sha256_file(path)
    checks.append(check("acceptance-contract-sha256", "PASS" if actual == contract["sha256"] == expected else "FAIL", "actual acceptance contract SHA-256 is bound", {"expected": expected, "bundle": contract["sha256"], "actual": actual}))


def _evaluate_environment(lock: dict, bundle: dict, artifact_root: Path) -> dict:
    checks: list[dict[str, Any]] = []
    limitations: list[str] = []
    try:
        refs = refs_from_bundle(bundle)
    except InvalidEvidence as exc:
        return {"schema_version": 1, "status": "BLOCKED", "checks": [check("bundle-shape", "BLOCKED", str(exc))], "limitations": [str(exc)], "summary": {"pass": 0, "blocked": 1, "fail": 0}, "notes": ["Invalid evidence shape; no ENV qualification made."]}
    check_contract(lock, bundle, artifact_root, checks)

    host_status, host = read_json_ref(artifact_root, refs, "host.json")
    if host_status == "OK" and isinstance(host, dict):
        if not isinstance(host.get("arch"), str) or any(type(host.get(field)) is not int for field in ("cpu_count", "ram_bytes", "available_bytes")):
            raise InvalidEvidence("host arch/cpu/ram/available fields are missing or malformed")
        host_ok = host.get("arch") in {"aarch64", "arm64"} and host.get("cpu_count", 0) >= 10 and host.get("ram_bytes", 0) >= 32 * GIB
        avail_ok = host.get("available_bytes", 0) >= 40 * GIB
        initial = host.get("initial_available_bytes")
        checks.append(check("host-actual-observed", "PASS" if host_ok and avail_ok else "FAIL", "hash-bound host arch/cpu/ram/current reserve observed", host))
        checks.append(check("host-initial-reserve", "PASS" if isinstance(initial, int) and initial >= 100 * GIB else "BLOCKED", "initial host reserve must be at least 100 GiB; absent value is BLOCKED", {"initial_available_bytes": initial}))
    else:
        checks.append(check("host-actual-observed", "BLOCKED" if host_status == "MISSING" else "FAIL", "hash-bound host.json observation is required", {"status": host_status}))

    lima_status, lima_rows = read_jsonl_ref(artifact_root, refs, "lima-after.jsonl")
    checks.append(check("lima-after-jsonl", "PASS" if lima_status == "OK" else ("BLOCKED" if lima_status == "MISSING" else "FAIL"), "hash-bound Lima topology observation", {"status": lima_status}))
    lima = {row.get("name"): row for row in lima_rows if isinstance(row.get("name"), str)}

    for name, expected in EXPECTED_VMS.items():
        row = lima.get(name, {})
        if row and (any(type(row.get(field)) is not int for field in ("cpus", "memory", "disk")) or not isinstance(row.get("additionalDisks"), list)):
            raise InvalidEvidence(f"{name} Lima resource/disk fields are malformed")
        inv_status, inv = read_json_ref(artifact_root, refs, expected["inventory"])
        inventory = inv if inv_status == "OK" and isinstance(inv, dict) else {}
        config_ok = row.get("status") == "Running" and row.get("hostname") == f"lima-{name}" and row.get("arch") == "aarch64" and row.get("cpus") == expected["cpus"] and row.get("memory") == expected["memory"] and row.get("disk") == expected["disk"]
        volume_ok = any(isinstance(d, dict) and d.get("name") == expected["volume"] and d.get("format") is True and d.get("fsType") == "ext4" for d in row.get("additionalDisks", []))
        checks.append(check(f"{name}-lima-config", "BLOCKED" if not row else ("PASS" if config_ok and volume_ok and image_digest_ok(row, lock) else "FAIL"), "Lima running topology, hostname and configured image match lock", {"status": row.get("status"), "hostname": row.get("hostname"), "arch": row.get("arch"), "cpus": row.get("cpus"), "memory": row.get("memory"), "disk": row.get("disk"), "images": row.get("config", {}).get("images") if isinstance(row.get("config"), dict) else None, "configured_image_ok": image_digest_ok(row, lock)}))

        mem = memtotal_bytes(inventory)
        missing = inv_status == "MISSING" or not inventory or mem is None or any(inventory.get(field) is None for field in ("hostname", "architecture", "cpu_count", "kernel", "os_release"))
        mismatch = bool(inventory) and (inventory.get("hostname") != f"lima-{name}" or inventory.get("architecture") != "aarch64" or inventory.get("cpu_count") != expected["cpus"] or inventory.get("kernel") != EXPECTED_KERNEL or not os_is_ubuntu_2404(inventory))
        mem_ok = mem is not None and mem >= int(expected["memory"] * 0.90)
        status = "FAIL" if inv_status in {"TAMPERED", "MALFORMED"} else ("BLOCKED" if missing else ("FAIL" if mismatch or not mem_ok else "PASS"))
        checks.append(check(f"{name}-guest-identity", status, "guest hostname, arch, CPU, RAM, Ubuntu 24.04 and kernel observed", {"inventory_status": inv_status, "hostname": inventory.get("hostname"), "architecture": inventory.get("architecture"), "cpu_count": inventory.get("cpu_count"), "memtotal_bytes": mem, "kernel": inventory.get("kernel"), "ubuntu_2404": os_is_ubuntu_2404(inventory)}))

        ip_ok, ip_ev = address_mtu(inventory, expected["ip"])
        checks.append(check(f"{name}-ip-mtu", "PASS" if ip_ok else "BLOCKED", "fixed IPv4 and MTU 1500 observed", ip_ev))
        mount_prefix = f"/mnt/lima-{expected['volume']}"
        ext4_ok, ext4_rows = has_ext4_mount(inventory, mount_prefix, expected["volume_gib"] * GIB - 32 * 1024 * 1024)
        block_observed = isinstance(parse_stdout_json(inventory.get("block_layout")), dict)
        checks.append(check(f"{name}-guest-ext4-volume", "BLOCKED" if not block_observed else ("PASS" if ext4_ok else "FAIL"), "dedicated guest ext4 volume observed", ext4_rows))
        avail, avail_ev = df_available(inventory, mount_prefix)
        if name == "afs-accept-a":
            checks.append(check(f"{name}-data-reserve", "PASS" if avail is not None and avail >= 4 * GIB else "BLOCKED", "data volume has at least 4 GiB free; historical FUSE df errors do not hide valid ext4 row", avail_ev))
        elif name != "afs-accept-ctl":
            checks.append(check(f"{name}-data-reserve-observed", "PASS" if avail is not None and avail >= 4 * GIB else "BLOCKED", "data volume reserve observed", avail_ev))
        swap_off = isinstance(inventory.get("swap"), str) and "SwapTotal:" not in inventory["swap"] and len(inventory["swap"].strip().splitlines()) <= 1
        checks.append(check(f"{name}-swap-off", "PASS" if swap_off else "BLOCKED", "swap is absent for performance preparation", inventory.get("swap")))
        checks.append(check(f"{name}-fuse-present", "PASS" if inventory.get("fuse_present") is True else "BLOCKED", "FUSE device/module presence observed", inventory.get("fuse_present")))
        rdma_records = [inventory.get(k) for k in ("rdma_device", "rdma_links")]
        rdma_valid = all(isinstance(record, dict) and isinstance(record.get("stdout"), str) and record.get("status") == "OBSERVED" and record.get("returncode") == 0 for record in rdma_records)
        rdma_text = " ".join(record["stdout"] for record in rdma_records) if rdma_valid else ""
        checks.append(check(f"{name}-rxe-device-observed", "PASS" if all(token in rdma_text for token in ("rxe0", "ACTIVE", "RoCE v2")) else "BLOCKED", "RXE device metadata observed; not cross-VM verbs proof"))

    checks.append(check("topology-total-quota", "PASS" if sum((lima.get(n, {}).get("cpus") or 0) for n in EXPECTED_VMS) == 8 and sum((lima.get(n, {}).get("memory") or 0) for n in EXPECTED_VMS) == 22 * GIB else "FAIL", "fixed topology totals are 8 vCPU and 22 GiB"))
    for name, detail in DEFERRED.items():
        if name == "network-tls-fault-recovery":
            network_check = evaluate_network(bundle, artifact_root, refs)
            checks.append(network_check)
            if network_check["status"] != "PASS":
                limitations.append(detail)
            continue
        if name == "durable-backend-restart":
            backend_check = evaluate_durable_backend_restart(bundle, artifact_root, refs)
            checks.append(backend_check)
            if backend_check["status"] != "PASS":
                limitations.append(detail)
            continue
        if name == "cross-vm-verbs":
            verbs_check = evaluate_verbs(bundle, artifact_root, refs)
            checks.append(verbs_check)
            if verbs_check["status"] != "PASS":
                limitations.append(detail)
            continue
        if name == "actual-moosefs-mount-io":
            moose_check = evaluate_actual_moosefs_mount_io(bundle, artifact_root, refs)
            checks.append(moose_check)
            if moose_check["status"] != "PASS":
                limitations.append(detail)
            continue
        if name == "actual-3fs-mount-io":
            threefs_check = evaluate_actual_3fs_mount_io(bundle, artifact_root, refs)
            checks.append(threefs_check)
            if threefs_check["status"] != "PASS":
                limitations.append(detail)
            continue
        checks.append(check(name, "BLOCKED", detail))
        limitations.append(detail)
    status = worst_status(checks)
    return {"schema_version": 1, "status": status, "checks": checks, "limitations": limitations, "summary": {"pass": sum(c["status"] == "PASS" for c in checks), "blocked": sum(c["status"] == "BLOCKED" for c in checks), "fail": sum(c["status"] == "FAIL" for c in checks)}, "notes": ["Bounded preparation evaluation only.", "Generic PASS/text receipts do not satisfy semantic ENV-01 predicates.", "Do not mark ENV driver READY or acceptance.lock.json FROZEN from this report."]}


def evaluate_environment(lock: dict, bundle: dict, artifact_root: Path) -> dict:
    try:
        if not isinstance(lock, dict) or not isinstance(bundle, dict):
            raise InvalidEvidence("lock and bundle must be objects")
        return _evaluate_environment(lock, bundle, artifact_root)
    except (InvalidEvidence, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return {"schema_version": 1, "status": "BLOCKED", "checks": [check("invalid-environment-evidence", "BLOCKED", str(exc))], "limitations": ["Invalid evidence prevents evaluation; no ENV qualification."], "summary": {"pass": 0, "blocked": 1, "fail": 0}, "notes": []}


def qualification_errors(lock: dict, lock_path: Path) -> list[str]:
    if not isinstance(lock, dict):
        return ["environment lock must be an object"]
    evidence = lock.get("environment_evidence")
    if not isinstance(evidence, dict):
        return ["lock.environment_evidence is missing"]
    rel, expected_sha = evidence.get("path"), evidence.get("sha256")
    if not isinstance(rel, str) or not isinstance(expected_sha, str):
        return ["lock.environment_evidence requires path and sha256"]
    try:
        bundle_path = checked_path(lock_path.parent.resolve(), rel)
        if not bundle_path.is_file():
            return ["environment evidence bundle is missing"]
        actual_sha = sha256_file(bundle_path)
        if actual_sha != expected_sha:
            return [f"environment evidence bundle sha256 mismatch: {actual_sha}"]
        bundle = load_json(bundle_path)
    except InvalidEvidence as exc:
        return [str(exc)]
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return [f"environment evidence bundle is malformed: {exc}"]
    if not isinstance(bundle, dict):
        return ["environment evidence bundle is malformed: root must be object"]
    report = evaluate_environment(lock, bundle, bundle_path.parent)
    return [f"{item['status']} {item['name']}: {item['detail']}" for item in report.get("checks", []) if item.get("status") != "PASS"]


def is_linux_arm64() -> bool:
    return sys.platform.startswith("linux") and platform.machine().lower() in {"aarch64", "arm64"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate AFS environment preparation evidence")
    parser.add_argument("--lock", required=True)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--contract", help="acceptance.md path; CLI hashes it and records bundle.contract")
    args = parser.parse_args(argv)
    lock_path, bundle_path, output_path = Path(args.lock).resolve(), Path(args.bundle).resolve(), Path(args.output).resolve()
    lock = load_json(lock_path)
    bundle = load_json(bundle_path)
    if not isinstance(lock, dict) or not isinstance(bundle, dict):
        raise SystemExit("lock and bundle must be JSON objects")
    if args.contract:
        cpath = Path(args.contract).resolve()
        try:
            rel = str(cpath.relative_to(bundle_path.parent))
        except ValueError:
            rel = cpath.name
        bundle = dict(bundle)
        bundle["contract"] = {"path": rel, "sha256": sha256_file(cpath)}
    report = evaluate_environment(lock, bundle, bundle_path.parent)
    if not is_linux_arm64():
        report["checks"].append(check("cli-linux-arm64-guard", "BLOCKED", "environment preparation CLI must run on Linux ARM64"))
        report["status"] = worst_status(report["checks"])
        report["summary"] = {"pass": sum(c["status"] == "PASS" for c in report["checks"]), "blocked": sum(c["status"] == "BLOCKED" for c in report["checks"]), "fail": sum(c["status"] == "FAIL" for c in report["checks"])}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
