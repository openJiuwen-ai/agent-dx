#!/usr/bin/env python3
"""Deployment acceptance driver for AFS package lifecycle slices.

The driver reports case-specific smoke evidence for DEP-01..DEP-07 where a
single-node package slice can prove the requested behavior. It deliberately
returns BLOCKED for full release matrices and for topology/fault coverage that
is not implemented yet. A PASS from this driver is therefore scoped to the
requested smoke case and its emitted checks only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

STATUS_PASS = "PASS"
STATUS_BLOCKED = "BLOCKED"
STATUS_FAIL = "FAIL"

SUPPORTED_CASES = {"DEP-01", "DEP-02", "DEP-03", "DEP-04", "DEP-05", "DEP-06", "DEP-07"}

CASE_CHECKS: dict[str, list[str]] = {
    "DEP-01": [
        "package-sha256",
        "work-root-ext4",
        "install-checksum-and-config",
        "tls-one-node-bootstrap",
        "separate-mounts-configs",
        "one-node-two-mount-io",
    ],
    "DEP-03": [
        "repeat-install-preserves-config-and-binaries",
        "duplicate-start-rejected",
    ],
    "DEP-04": [
        "start-status-restart",
        "stop-timeout-nonzero",
        "negative-pid-identity-not-killed",
    ],
    "DEP-05": [
        "bad-digest-rejected",
        "port-conflict-rejected",
    ],
    "DEP-06": [
        "offline-package-manifest",
        "install-checksum-and-config",
        "tls-one-node-bootstrap",
        "one-node-two-mount-io",
    ],
    "DEP-07": [
        "confined-destructive-paths",
        "program-only-uninstall",
    ],
}

CASE_LIMITATIONS: dict[str, list[str]] = {
    "DEP-01": [
        "Smoke uses an existing prepared Linux guest and local package path; it does not prove a pristine VM without source/compiler or one-command download.",
        "Smoke uses the local-file default package topology, not etcd and Redis matrices.",
    ],
    "DEP-02": [
        "Cluster topology deployment for ctl/A/B/C and etcd/Redis is not implemented in this driver.",
    ],
    "DEP-03": [
        "Smoke verifies repeated install and duplicate start rejection; conflicting configuration rejection is not covered.",
    ],
    "DEP-04": [
        "Smoke verifies CLI lifecycle and PID identity safety; hard-kill recovery and Rust drain-failure propagation are not covered.",
    ],
    "DEP-05": [
        "Smoke verifies one bad digest and one port conflict path; bad backend, permission denied, missing FUSE and missing RDMA are not covered.",
    ],
    "DEP-06": [
        "Smoke inspects the offline package manifest and installs from a local tarball; network-disabled execution and user-provided etcd/Redis instances are not covered.",
    ],
    "DEP-07": [
        "Smoke verifies default program-only uninstall; explicit persistent data deletion is intentionally not implemented for first-stage scripts.",
    ],
}


def utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(argv: list[str], artifact: Path, timeout: int = 30, check: bool = False) -> dict[str, Any]:
    started = time.time()
    artifact.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(argv, shell=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
    record = {
        "argv": argv,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "duration_seconds": round(time.time() - started, 3),
    }
    write_json(artifact, record)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed rc={proc.returncode}: {' '.join(argv)}; see {artifact}")
    return record


def proof(
    case_id: str,
    profile: str,
    matrix: dict[str, str],
    status: str,
    checks: list[dict[str, Any]],
    reason: str = "",
    limitations: list[str] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "case_id": case_id,
        "profile": profile,
        "matrix": matrix,
        "status": status,
        "reason": reason,
        "checks": checks,
        "limitations": limitations if limitations is not None else CASE_LIMITATIONS.get(case_id, []),
        "generated_at": utc(),
    }
    if profile == "smoke":
        result["coverage"] = {
            "profile": "smoke",
            "case_id": case_id,
            "checks": [check["name"] for check in checks],
        }
    return result


def blocked(case_id: str, profile: str, matrix: dict[str, str], reason: str) -> dict[str, Any]:
    return proof(
        case_id,
        profile,
        matrix,
        STATUS_BLOCKED,
        [{"name": "deployment-driver-preflight", "status": STATUS_BLOCKED, "evidence": reason}],
        reason,
    )


def require_linux() -> str | None:
    if not sys.platform.startswith("linux"):
        return f"deployment driver must run on Linux, observed {sys.platform}"
    machine = platform.machine().lower()
    if machine not in {"aarch64", "arm64"}:
        return f"deployment driver requires ARM64 Linux guest, observed {machine}"
    return None


def findmnt(path: Path, artifact: Path) -> dict[str, Any]:
    return run(["findmnt", "-T", str(path), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"], artifact, timeout=10)


def fstype_from_findmnt(record: dict[str, Any]) -> str | None:
    try:
        filesystems = json.loads(record["stdout"]).get("filesystems", [])
        if filesystems:
            return filesystems[0].get("fstype")
    except Exception:  # noqa: BLE001
        return None
    return None


def sudo_prefix() -> list[str]:
    if os.geteuid() == 0:
        return []
    sudo = shutil.which("sudo")
    if sudo is None:
        raise RuntimeError("root or sudo is required for install/mount lifecycle checks")
    return [sudo]


def safe_extract(package: Path, dest: Path) -> Path:
    dest.mkdir(parents=True, exist_ok=False)
    with tarfile.open(package, "r:gz") as tf:
        root_names = {member.name.split("/", 1)[0] for member in tf.getmembers() if member.name and not member.name.startswith("/")}
        for member in tf.getmembers():
            target = (dest / member.name).resolve()
            target.relative_to(dest.resolve())
        tf.extractall(dest)
    roots = sorted(path for name in root_names for path in [dest / name] if path.is_dir())
    if len(roots) != 1:
        raise RuntimeError(f"package must contain exactly one root directory, found {roots}")
    return roots[0]


def replace_text_file(path: Path, text: str, sudo: list[str]) -> None:
    if os.access(path, os.W_OK):
        path.write_text(text, encoding="utf-8")
        return
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
        handle.write(text)
        temp_name = handle.name
    try:
        subprocess.run(sudo + ["install", "-m", "0644", temp_name, str(path)], shell=False, check=True)
    finally:
        Path(temp_name).unlink(missing_ok=True)


def patch_ports(configs: list[Path], base: int, sudo: list[str]) -> None:
    replacements = {
        "127.0.0.1:7400": f"127.0.0.1:{base}",
        "127.0.0.1:7401": f"127.0.0.1:{base + 1}",
        "127.0.0.1:7500": f"127.0.0.1:{base + 2}",
        "127.0.0.1:7501": f"127.0.0.1:{base + 3}",
    }
    for cfg in configs:
        text = cfg.read_text(encoding="utf-8")
        for old, new in replacements.items():
            text = text.replace(old, new)
        replace_text_file(cfg, text, sudo)


def read_manifest(package_root: Path) -> dict[str, Any]:
    return json.loads((package_root / "manifest.json").read_text(encoding="utf-8"))


def binary_hashes(prefix: Path) -> dict[str, str]:
    return {
        "afs-meta": sha256_file(prefix / "bin" / "afs-meta"),
        "afs-node": sha256_file(prefix / "bin" / "afs-node"),
    }


def add_check(checks: list[dict[str, Any]], name: str, status: str, evidence: Any, artifact: str | None = None) -> None:
    check = {"name": name, "status": status, "evidence": evidence}
    if artifact:
        check["artifact"] = artifact
    checks.append(check)


def assert_under(root: Path, path: Path) -> None:
    resolved_root = root.resolve()
    resolved_path = path.resolve()
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise RuntimeError(f"destructive path is outside work root: {resolved_path} not under {resolved_root}") from exc


def assert_confined_paths(work_root: Path, paths: list[Path]) -> None:
    if str(work_root.resolve()) in {"/", ""}:
        raise RuntimeError(f"unsafe work root: {work_root}")
    for path in paths:
        assert_under(work_root, path)


def select_case_checks(all_checks: list[dict[str, Any]], case_id: str) -> list[dict[str, Any]]:
    expected = CASE_CHECKS.get(case_id)
    if expected is None:
        raise RuntimeError(f"case {case_id} has no implemented smoke check mapping")
    by_name = {check["name"]: check for check in all_checks}
    missing = [name for name in expected if name not in by_name]
    if missing:
        raise RuntimeError(f"internal driver error: missing checks for {case_id}: {missing}")
    return [by_name[name] for name in expected]


def verify_manifest_for_offline(manifest: dict[str, Any], package_root: Path) -> dict[str, Any]:
    source_commit = str(manifest.get("source_commit", ""))
    binaries = manifest.get("binaries", {})
    contains = set(manifest.get("contains", []))
    required = {"afs-meta", "afs-node", "afs-processctl", "dep02-smoke.sh"}
    if not source_commit or source_commit == "unknown":
        raise RuntimeError("package manifest lacks auditable source_commit")
    if not required.issubset(contains):
        raise RuntimeError(f"package manifest missing required contents: {sorted(required - contains)}")
    if not isinstance(binaries, dict) or "afs-meta" not in binaries or "afs-node" not in binaries:
        raise RuntimeError("package manifest lacks binary sha256 records")
    rust_toolchain = str(manifest.get("rust_toolchain", ""))
    if not rust_toolchain or rust_toolchain == "unknown":
        raise RuntimeError("package manifest lacks auditable rust_toolchain")
    deps = package_root / "DEPENDENCIES.md"
    if not deps.exists():
        raise RuntimeError("package lacks DEPENDENCIES.md")
    return {
        "source_commit": source_commit,
        "features": manifest.get("features"),
        "rust_toolchain": manifest.get("rust_toolchain"),
        "binary_sha256": binaries,
        "dependencies": str(deps),
        "notes": manifest.get("notes"),
    }


def verify_bad_digest_rejected(extract_root: Path, work_root: Path, sudo: list[str], artifact_dir: Path) -> dict[str, Any]:
    bad_root = work_root / "bad-digest-extract"
    if bad_root.exists():
        shutil.rmtree(bad_root)
    shutil.copytree(extract_root, bad_root, symlinks=True)
    target = bad_root / "bin" / "dep02-smoke.sh"
    with target.open("a", encoding="utf-8") as handle:
        handle.write("\n# corrupt package payload for DEP-05\n")
    bad_prefix = work_root / "bad-digest-opt"
    bad_cfg = work_root / "bad-digest-etc"
    bad_state = work_root / "bad-digest-state"
    bad_run = work_root / "bad-digest-run"
    bad_log = work_root / "bad-digest-log"
    bad_mount = work_root / "bad-digest-mnt"
    assert_confined_paths(work_root, [bad_prefix, bad_cfg, bad_state, bad_run, bad_log, bad_mount])
    record = run(
        sudo
        + [
            str(bad_root / "install.sh"),
            "--prefix",
            str(bad_prefix),
            "--config-dir",
            str(bad_cfg),
            "--state-dir",
            str(bad_state),
            "--run-dir",
            str(bad_run),
            "--log-dir",
            str(bad_log),
            "--mount-root",
            str(bad_mount),
        ],
        artifact_dir / "bad-digest-install.json",
        timeout=60,
    )
    if record["returncode"] == 0:
        raise RuntimeError("bad digest install unexpectedly succeeded")
    if (bad_prefix / "bin" / "afs-meta").exists() or (bad_cfg / "meta.toml").exists():
        raise RuntimeError("bad digest install left program/config artifacts")
    return {"returncode": record["returncode"], "stderr": record["stderr"], "program_files_absent": True}


def verify_port_conflict_rejected(common: list[str], port: int, run_path: Path, artifact_dir: Path) -> dict[str, Any]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
        sock.listen(1)
        record = run(common + ["start", "meta"], artifact_dir / "port-conflict-meta.json", timeout=20)
    finally:
        sock.close()
    if record["returncode"] == 0 or "port already in use" not in (record.get("stderr") or ""):
        raise RuntimeError(f"port conflict did not fail as expected: rc={record['returncode']} stderr={record.get('stderr')!r}")
    if (run_path / "meta.pid").exists() or (run_path / "meta.identity").exists():
        raise RuntimeError("port conflict left meta pid/identity files")
    return {"returncode": record["returncode"], "stderr": record["stderr"], "pid_files_absent": True}


def run_deployment_slice(args: argparse.Namespace, case_id: str, profile: str, matrix: dict[str, str], run_dir: Path) -> dict[str, Any]:
    if case_id not in SUPPORTED_CASES:
        return blocked(case_id, profile, matrix, f"unsupported deployment case: {case_id}")
    package = Path(args.package).expanduser().resolve()
    if not package.is_file():
        return blocked(case_id, profile, matrix, f"package does not exist: {package}")
    if profile != "smoke":
        return blocked(case_id, profile, matrix, "deployment driver implements smoke slices only; full DEP matrix remains TODO")
    if case_id == "DEP-02":
        return blocked(case_id, profile, matrix, "DEP-02 requires multi-node topology deployment; this driver only owns single-node package lifecycle slices")
    linux_error = require_linux()
    if linux_error:
        return blocked(case_id, profile, matrix, linux_error)

    work_root = Path(args.work_root).expanduser().resolve() if args.work_root else Path(tempfile.mkdtemp(prefix="afs-deploy-driver-"))
    work_root.mkdir(parents=True, exist_ok=True)
    artifact_dir = run_dir / "deploy-artifacts" / uuid.uuid4().hex[:12]
    artifact_dir.mkdir(parents=True, exist_ok=False)

    all_checks: list[dict[str, Any]] = []
    write_json(artifact_dir / "inputs.json", {"package": str(package), "work_root": str(work_root), "ports_base": args.ports_base, "case_id": case_id})
    package_sha = sha256_file(package)
    add_check(all_checks, "package-sha256", STATUS_PASS, {"sha256": package_sha}, "deploy-artifacts")

    mount_record = findmnt(work_root, artifact_dir / "work-root-findmnt.json")
    fstype = fstype_from_findmnt(mount_record)
    if fstype != "ext4":
        return blocked(case_id, profile, matrix, f"work root must be on guest ext4, observed fstype={fstype!r}; see {artifact_dir/'work-root-findmnt.json'}")
    add_check(all_checks, "work-root-ext4", STATUS_PASS, {"fstype": fstype, "path": str(work_root)}, str((artifact_dir / "work-root-findmnt.json").relative_to(run_dir)))

    sudo = sudo_prefix()
    extract_root = safe_extract(package, work_root / "extract")
    manifest = read_manifest(extract_root)
    write_json(artifact_dir / "manifest.json", manifest)
    offline_evidence = verify_manifest_for_offline(manifest, extract_root)
    add_check(all_checks, "offline-package-manifest", STATUS_PASS, offline_evidence, str((artifact_dir / "manifest.json").relative_to(run_dir)))

    bad_digest = verify_bad_digest_rejected(extract_root, work_root, sudo, artifact_dir)
    add_check(all_checks, "bad-digest-rejected", STATUS_PASS, bad_digest, str((artifact_dir / "bad-digest-install.json").relative_to(run_dir)))

    prefix = work_root / "opt"
    config = work_root / "etc"
    state = work_root / "state"
    run_path = work_root / "run"
    log = work_root / "log"
    mount_root = work_root / "mnt"
    destructive_paths = [prefix, config, state, run_path, log, mount_root]
    assert_confined_paths(work_root, destructive_paths)
    add_check(all_checks, "confined-destructive-paths", STATUS_PASS, {"work_root": str(work_root), "paths": [str(path) for path in destructive_paths]})

    install_cmd = sudo + [
        str(extract_root / "install.sh"),
        "--prefix",
        str(prefix),
        "--config-dir",
        str(config),
        "--state-dir",
        str(state),
        "--run-dir",
        str(run_path),
        "--log-dir",
        str(log),
        "--mount-root",
        str(mount_root),
    ]
    run(install_cmd, artifact_dir / "install.json", timeout=60, check=True)
    patch_ports([config / "meta.toml", config / "node.toml"], args.ports_base, sudo)
    add_check(all_checks, "install-checksum-and-config", STATUS_PASS, "install.sh verified SHA256SUMS and generated config", str((artifact_dir / "install.json").relative_to(run_dir)))

    tls_files = [config / "tls" / name for name in ("ca.pem", "meta.pem", "meta-key.pem", "node-a.pem", "node-a-key.pem")]
    missing_tls = [str(path) for path in tls_files if not path.exists()]
    if missing_tls:
        raise RuntimeError(f"missing TLS bootstrap files: {missing_tls}")
    add_check(all_checks, "tls-one-node-bootstrap", STATUS_PASS, {"files": [str(path) for path in tls_files]})

    node_text = (config / "node.toml").read_text(encoding="utf-8")
    if str(mount_root / "dfs") not in node_text or str(mount_root / "ownerfs") not in node_text:
        raise RuntimeError("node config does not contain separate DFS and OwnerFs mount paths")
    add_check(all_checks, "separate-mounts-configs", STATUS_PASS, {"dfs_mount": str(mount_root / "dfs"), "ownerfs_mount": str(mount_root / "ownerfs"), "node_config": str(config / "node.toml")})

    hashes_before = binary_hashes(prefix)
    write_json(artifact_dir / "binary-hashes-before.json", hashes_before)

    ctl = prefix / "bin" / "afs-processctl"
    dep_smoke = prefix / "bin" / "dep02-smoke.sh"
    common = sudo + [str(ctl), "--prefix", str(prefix), "--config-dir", str(config), "--run-dir", str(run_path), "--log-dir", str(log), "--timeout", str(args.timeout)]

    port_conflict = verify_port_conflict_rejected(common, args.ports_base, run_path, artifact_dir)
    add_check(all_checks, "port-conflict-rejected", STATUS_PASS, port_conflict, str((artifact_dir / "port-conflict-meta.json").relative_to(run_dir)))

    run(common + ["start", "all"], artifact_dir / "start-all.json", timeout=60, check=True)
    run(common + ["status", "all"], artifact_dir / "status-start.json", timeout=20, check=True)
    run(sudo + [str(dep_smoke), "--mount", str(mount_root / "dfs"), "--name", "driver-dfs"], artifact_dir / "smoke-dfs.json", timeout=60, check=True)
    run(sudo + [str(dep_smoke), "--mount", str(mount_root / "ownerfs"), "--name", "driver-owner"], artifact_dir / "smoke-owner.json", timeout=60, check=True)
    add_check(all_checks, "one-node-two-mount-io", STATUS_PASS, "DFS and OwnerFs dep02-smoke passed on exact AFS FUSE mounts", str((artifact_dir / "smoke-dfs.json").relative_to(run_dir)))

    before_cfg = {"meta": sha256_file(config / "meta.toml"), "node": sha256_file(config / "node.toml"), "ca": sha256_file(config / "tls" / "ca.pem")}
    run(install_cmd, artifact_dir / "reinstall.json", timeout=60, check=True)
    after_cfg = {"meta": sha256_file(config / "meta.toml"), "node": sha256_file(config / "node.toml"), "ca": sha256_file(config / "tls" / "ca.pem")}
    if before_cfg != after_cfg:
        raise RuntimeError(f"reinstall changed config/TLS digest: before={before_cfg} after={after_cfg}")
    hashes_after_reinstall = binary_hashes(prefix)
    if hashes_before != hashes_after_reinstall:
        raise RuntimeError("binary hashes changed across reinstall")
    add_check(all_checks, "repeat-install-preserves-config-and-binaries", STATUS_PASS, {"config_tls_sha256": before_cfg, "binary_sha256": hashes_before})

    duplicate_meta = run(common + ["start", "meta"], artifact_dir / "duplicate-meta.json", timeout=20)
    duplicate_node = run(common + ["start", "node"], artifact_dir / "duplicate-node.json", timeout=20)
    if duplicate_meta["returncode"] == 0 or duplicate_node["returncode"] == 0:
        raise RuntimeError("duplicate start unexpectedly succeeded")
    add_check(all_checks, "duplicate-start-rejected", STATUS_PASS, {"meta_rc": duplicate_meta["returncode"], "node_rc": duplicate_node["returncode"]}, str((artifact_dir / "duplicate-meta.json").relative_to(run_dir)))

    fake = work_root / "fake-identity"
    assert_confined_paths(work_root, [fake])
    (fake / "prefix" / "bin").mkdir(parents=True)
    (fake / "etc").mkdir(parents=True)
    (fake / "run").mkdir(parents=True)
    (fake / "log").mkdir(parents=True)
    fake_node = fake / "prefix" / "bin" / "afs-node"
    fake_node.write_text("#!/usr/bin/env bash\nwhile true; do sleep 60; done\n", encoding="utf-8")
    fake_node.chmod(0o755)
    (fake / "etc" / "node.toml").write_text(f'rest_listen = "127.0.0.1:0"\ngrpc_listen = "127.0.0.1:0"\ndfs_mount = "{fake}/mnt-dfs"\nownerfs_mount = "{fake}/mnt-owner"\n', encoding="utf-8")
    proc = subprocess.Popen([str(fake_node), "--config", str(fake / "etc" / "node.toml")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        time.sleep(1)
        (fake / "run" / "node.pid").write_text(f"{proc.pid}\n", encoding="utf-8")
        (fake / "run" / "node.identity").write_text(f"pid={proc.pid}\nexe={fake_node.resolve()}\nconfig={(fake/'etc'/'node.toml').resolve()}\nstart_ticks=1\ncmdline=wrong\n", encoding="utf-8")
        neg = run([str(ctl), "--prefix", str(fake / "prefix"), "--config-dir", str(fake / "etc"), "--run-dir", str(fake / "run"), "--log-dir", str(fake / "log"), "--timeout", "1", "stop", "node"], artifact_dir / "negative-pid.json", timeout=10)
        if neg["returncode"] == 0 or proc.poll() is not None:
            raise RuntimeError("negative PID identity test failed: processctl killed or accepted mismatched pid")
    finally:
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait()
    add_check(all_checks, "negative-pid-identity-not-killed", STATUS_PASS, "mismatched live PID was refused and remained alive", str((artifact_dir / "negative-pid.json").relative_to(run_dir)))

    run(common + ["restart", "all"], artifact_dir / "restart-all.json", timeout=60, check=True)
    run(common + ["status", "all"], artifact_dir / "status-restart.json", timeout=20, check=True)
    add_check(all_checks, "start-status-restart", STATUS_PASS, "status/start/restart lifecycle succeeded", str((artifact_dir / "restart-all.json").relative_to(run_dir)))

    timeout_stop = run(sudo + [str(ctl), "--prefix", str(prefix), "--config-dir", str(config), "--run-dir", str(run_path), "--log-dir", str(log), "--timeout", "0", "stop", "node"], artifact_dir / "stop-timeout.json", timeout=10)
    if timeout_stop["returncode"] == 0 or "did not stop within" not in timeout_stop.get("stderr", ""):
        raise RuntimeError("stop timeout did not surface non-zero timeout")
    add_check(all_checks, "stop-timeout-nonzero", STATUS_PASS, {"returncode": timeout_stop["returncode"], "stderr": timeout_stop["stderr"]}, str((artifact_dir / "stop-timeout.json").relative_to(run_dir)))

    assert_confined_paths(work_root, destructive_paths)
    run(common + ["stop", "all"], artifact_dir / "stop-all.json", timeout=60, check=True)
    run(common + ["start", "all"], artifact_dir / "start-before-uninstall.json", timeout=60, check=True)
    run(common + ["uninstall", "all"], artifact_dir / "uninstall-all.json", timeout=60, check=True)
    removed = [prefix / "bin" / name for name in ("afs-meta", "afs-node", "afs-processctl", "dep02-smoke.sh")]
    still_present = [str(path) for path in removed if path.exists()]
    preserved = [config / "meta.toml", config / "node.toml", state]
    missing_preserved = [str(path) for path in preserved if not path.exists()]
    if still_present or missing_preserved:
        raise RuntimeError(f"uninstall verification failed; still_present={still_present}; missing_preserved={missing_preserved}")
    mounts_after = run(["findmnt", "-rn", "--mountpoint", str(mount_root / "dfs"), "-o", "SOURCE,FSTYPE,TARGET"], artifact_dir / "mounts-after-dfs.json")
    mounts_owner_after = run(["findmnt", "-rn", "--mountpoint", str(mount_root / "ownerfs"), "-o", "SOURCE,FSTYPE,TARGET"], artifact_dir / "mounts-after-owner.json")
    if mounts_after["stdout"].strip() or mounts_owner_after["stdout"].strip():
        raise RuntimeError("uninstall left AFS mounts behind")
    add_check(all_checks, "program-only-uninstall", STATUS_PASS, "program files removed; config/state preserved; mounts absent", str((artifact_dir / "uninstall-all.json").relative_to(run_dir)))

    write_json(artifact_dir / "all-checks.json", all_checks)
    selected = select_case_checks(all_checks, case_id)
    write_json(artifact_dir / f"{case_id.lower()}-selected-checks.json", selected)
    return proof(case_id, profile, matrix, STATUS_PASS, selected)


def main() -> int:
    parser = argparse.ArgumentParser(description="AFS deployment acceptance driver")
    parser.add_argument("--package", required=True, help="AFS release tar.gz package")
    parser.add_argument("--work-root", help="Guest ext4 work root for isolated install")
    parser.add_argument("--ports-base", type=int, default=18900, help="Base port for meta grpc/rest and node grpc/rest")
    parser.add_argument("--timeout", type=int, default=25)
    args = parser.parse_args()

    case_id = os.environ.get("AFS_ACCEPTANCE_CASE_ID", "DEP-01")
    profile = os.environ.get("AFS_ACCEPTANCE_PROFILE", "smoke")
    matrix = json.loads(os.environ.get("AFS_ACCEPTANCE_MATRIX", "{}"))
    run_dir = Path(os.environ.get("AFS_ACCEPTANCE_RUN_DIR", ".")).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    try:
        result = run_deployment_slice(args, case_id, profile, matrix, run_dir)
    except Exception as exc:  # noqa: BLE001 - final proof captures exact blocker/failure
        result = proof(
            case_id,
            profile,
            matrix,
            STATUS_FAIL,
            [{"name": "deployment-driver-exception", "status": STATUS_FAIL, "evidence": f"{type(exc).__name__}: {exc}"}],
            f"{type(exc).__name__}: {exc}",
        )
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == STATUS_PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
