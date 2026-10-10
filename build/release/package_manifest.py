#!/usr/bin/env python3
"""Verify one ADX release package with the same contract at build and install time."""

import argparse
import hashlib
import json
import platform
from pathlib import Path

BINARIES = (
    "adxctl",
    "adx-inspect",
    "adx-coordinator",
    "adxlet",
    "adx-apiserver",
    "adx-ingress",
    "adx-relay",
)
AFS_BINARIES = ("afs-meta", "afs-node")
AFS_REQUIRED_FILES = {
    "etc/examples/afs/meta.toml",
    "etc/examples/afs/node.toml",
    "etc/examples/afs/prepare-local-tls.sh",
    "third_party/afs-source/LICENSE",
    "third_party/afs-source/NOTICE",
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
    afs_artifacts = {f"bin/{binary}" for binary in AFS_BINARIES} & expected
    afs_artifacts.update(path for path in expected if path.startswith("etc/examples/afs/"))
    afs_artifacts.update(path for path in expected if path.startswith("third_party/afs-source/"))
    if manifest.get("with_afs") is True:
        required.update({f"bin/{binary}" for binary in AFS_BINARIES})
        required.update(AFS_REQUIRED_FILES)
    elif afs_artifacts:
        raise ValueError("AFS artifacts are present in a default package")
    if not required.issubset(expected) or not any(
        name.startswith("sdk/adx_sandbox-") and name.endswith(".whl") for name in expected
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

    commit = manifest.get("commit", "")
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise ValueError("manifest commit must be a 40-character lowercase hexadecimal Git SHA")
    if enforce_host_architecture:
        machine = {"x86_64": "x86_64", "aarch64": "aarch64"}.get(platform.machine())
        if machine is None or not target.startswith(machine + "-") or "linux" not in target:
            raise ValueError(
                f"package target {target!r} does not match Linux host {platform.machine()!r}"
            )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-architecture", action="store_true")
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    try:
        manifest = verify(args.directory, enforce_host_architecture=args.host_architecture)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise SystemExit(str(error)) from error
    print(f"package verified: {manifest['commit']} ({manifest.get('target', '')})")


if __name__ == "__main__":
    main()
