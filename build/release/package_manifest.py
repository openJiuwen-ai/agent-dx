#!/usr/bin/env python3
"""Verify one ADX release package with the same contract at build and install time."""

import argparse
import hashlib
import json
import platform
import stat
from pathlib import Path

BINARIES = (
    "adxctl",
    "adx-inspect",
    "adx-coordinator",
    "adxlet",
    "adx-apiserver",
    "adx-activator",
    "adx-ingress",
    "adx-relay",
)
SANDBOXD_BINARIES = ("sandboxd", "sbox", "runc", "runc-shim", "sandbox-logger", "redis-cli")
RUNSC_HELPERS = (
    "checkpointgofer",
    "gvisor-sentry-prewarmer",
    "gvisor_sentry",
    "runsc-fd-parking",
    "runsc-metric-server",
)
SANDBOXD_DEPLOYMENT_FILES = (
    "sandboxd.service",
    "sandboxd.toml",
    "oss.json",
    "registry.json",
    "oss_auths.json",
    "registry_auths.json",
)
AFS_BINARIES = ("afs-meta", "afs-node")
AFS_REQUIRED_FILES = {
    "etc/examples/afs/meta.toml",
    "etc/examples/afs/node.toml",
    "etc/examples/afs/deployment-ownerfs-local.yaml",
    "etc/examples/afs/prepare-local-tls.sh",
    "third_party/afs-source/LICENSE",
    "third_party/afs-source/NOTICE",
}
ENVD_PROVENANCE_FILES = {
    "runtime/envd/manifest.json",
    "runtime/envd/LICENSE",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(directory: Path, *, enforce_host_architecture: bool = False) -> dict:
    root = directory.resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("manifest.json is missing")
    manifest = json.loads(manifest_path.read_text())
    files = manifest.get("files")
    if manifest.get("schema_version") != 1 or not isinstance(files, dict):
        raise ValueError("invalid package manifest")
    if "with_dfs" in manifest:
        raise ValueError("legacy with_dfs manifest; rebuild with --with-afs")
    if "with_afs" in manifest and not isinstance(manifest["with_afs"], bool):
        raise ValueError("with_afs must be a boolean")

    expected = set(files)
    required = {f"bin/{binary}" for binary in BINARIES} | {
        "bin/redis-server",
        "bin/redis-cli",
        "install.sh",
        "lib/package_manifest.py",
        "runtime/adx-execd",
    }
    target = manifest.get("target", "")
    if manifest.get("profile") == "release" and "linux" in target:
        required.add("runtime/adx-runtime-rootfs.img")
    with_sandboxd = manifest.get("with_sandboxd", False)
    if not isinstance(with_sandboxd, bool):
        raise ValueError("with_sandboxd must be a boolean")
    backend_prefix = "runtime/sandboxd/"
    backend_files = {backend_prefix + name for name in (*SANDBOXD_BINARIES, "manifest.json")}
    backend_files.update(
        backend_prefix + "gvisor-bin/" + name for name in RUNSC_HELPERS
    )
    backend_files.update(
        f"etc/examples/sandboxd/{name}" for name in SANDBOXD_DEPLOYMENT_FILES
    )
    backend_files.update({"lib/sandboxd_preflight.py", "runtime/sandboxd/runsc"})
    if with_sandboxd:
        required.update(backend_files)
        required.add("third_party/sandboxd/source.json")
    elif expected & backend_files:
        raise ValueError("sandboxd artifacts are present without with_sandboxd")
    envd_artifacts = expected & ENVD_PROVENANCE_FILES
    if envd_artifacts and envd_artifacts != ENVD_PROVENANCE_FILES:
        raise ValueError("incomplete envd provenance")
    afs_artifacts = {f"bin/{binary}" for binary in AFS_BINARIES} & expected
    afs_artifacts.update(path for path in expected if path.startswith("etc/examples/afs/"))
    afs_artifacts.update(path for path in expected if path.startswith("third_party/afs-source/"))
    if manifest.get("with_afs") is True:
        required.update({f"bin/{binary}" for binary in AFS_BINARIES})
        required.update(AFS_REQUIRED_FILES)
    elif afs_artifacts:
        raise ValueError("AFS artifacts are present in a default package")
    if not required.issubset(expected) or not any(
        name.startswith("sdk/adx_environment-") and name.endswith(".whl") for name in expected
    ):
        raise ValueError("incomplete package")

    actual = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"package contains a symlink: {path.relative_to(root)}")
        if path.is_file() and path != manifest_path:
            actual.add(path.relative_to(root).as_posix())
    if actual != expected:
        raise ValueError("package file list does not match manifest")
    for name, digest in files.items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or sha256(root / relative) != digest:
            raise ValueError(f"package integrity check failed: {name}")

    if with_sandboxd:
        backend = json.loads((root / backend_prefix / "manifest.json").read_text())
        pin = json.loads((root / "third_party/sandboxd/source.json").read_text())
        machine = {"x86_64-unknown-linux-gnu": 62, "aarch64-unknown-linux-gnu": 183}.get(target)
        if machine is None or backend["target"] != target or backend["sandboxd_revision"] != pin["revision"]:
            raise ValueError("sandboxd revision or target mismatch")
        if backend["files"] != {name: files[backend_prefix + name] for name in SANDBOXD_BINARIES}:
            raise ValueError("sandboxd file hashes differ from backend manifest")
        for name in (*SANDBOXD_BINARIES, "runsc"):
            executable = root / backend_prefix / name
            with executable.open("rb") as binary:
                header = binary.read(20)
            if header[:6] != b"\x7fELF\x02\x01" or int.from_bytes(header[18:20], "little") != machine:
                raise ValueError(f"sandboxd executable architecture mismatch: {name}")
            if not executable.stat().st_mode & stat.S_IXUSR:
                raise ValueError(f"sandboxd executable is not executable: {name}")
        for name in RUNSC_HELPERS:
            helper = root / backend_prefix / "gvisor-bin" / name
            if not helper.is_file() or not helper.stat().st_mode & stat.S_IXUSR:
                raise ValueError(f"gVisor helper is missing or not executable: {name}")
            with helper.open("rb") as binary:
                header = binary.read(20)
            if header[:6] != b"\x7fELF\x02\x01" or int.from_bytes(header[18:20], "little") != machine:
                raise ValueError(f"gVisor helper architecture mismatch: {name}")

    commit = manifest.get("commit", "")
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise ValueError("manifest commit must be a 40-character lowercase hexadecimal Git SHA")
    if enforce_host_architecture:
        machine = {"x86_64": "x86_64", "aarch64": "aarch64"}.get(platform.machine())
        if machine is None or not target.startswith(machine + "-") or "linux" not in target:
            raise ValueError(f"package target {target!r} does not match Linux host {platform.machine()!r}")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-architecture", action="store_true")
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    try:
        manifest = verify(args.directory, enforce_host_architecture=args.host_architecture)
    except (OSError, ValueError) as error:
        raise SystemExit(str(error)) from error
    print(f"package verified: {manifest['commit']} ({manifest.get('target', '')})")


if __name__ == "__main__":
    main()
