#!/usr/bin/env python3
"""Assemble explicit build artifacts; never compile or fetch during deployment."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
BINARIES = ("adxctl", "adx-master", "adx-node-manager", "adx-api-server", "adx-edge-frontend", "adx-node-proxy", "adx-data-plane-forward")

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def assemble(binary_dir, redis, wheel, output, commit, dirty, target, profile):
    if output.exists():
        raise ValueError("output already exists")
    inputs = {f"bin/{name}": binary_dir / name for name in BINARIES}
    inputs["runtime/rrt-runtime"] = binary_dir / "rrt-runtime"
    # Native Linux release builder supplies the EROFS payload. Debug/native macOS
    # packages retain binary-only development support.
    runtime_root = binary_dir / "adx-runtime-rootfs.img"
    if profile == "release" and "linux" in target:
        inputs["runtime/adx-runtime-rootfs.img"] = runtime_root
    elif runtime_root.is_file():
        inputs["runtime/adx-runtime-rootfs.img"] = runtime_root
    inputs["bin/redis-server"] = redis
    if not wheel.name.startswith("adx_sandbox-") or wheel.suffix != ".whl":
        raise ValueError("an adx_sandbox wheel is required")
    inputs[f"sdk/{wheel.name}"] = wheel
    for source in inputs.values():
        if source.is_symlink() or not source.is_file():
            raise ValueError("a required artifact is missing or is a symlink")
    if not target or profile not in ("debug", "release") or len(commit) != 40:
        raise ValueError("build identity required")
    version = subprocess.check_output([str(redis.resolve()), "--version"], text=True)
    if "v=7.2.5 " not in version:
        raise ValueError("Redis version differs from pinned 7.2.5")
    for path in (ROOT / "build/config/examples").iterdir():
        if path.is_file():
            inputs[f"etc/examples/{path.name}"] = path
    for component in ("redis", "sandboxd"):
        for name in ("source.json", "LICENSE"):
            inputs[f"third_party/{component}/{name}"] = ROOT / "third_party" / component / name
    sandboxd_source = json.loads((ROOT / "third_party/sandboxd/source.json").read_text())
    for patch in sandboxd_source.get("patches", []):
        relative = Path(patch["path"])
        if relative.is_absolute() or ".." in relative.parts or relative.parts[:3] != ("third_party", "sandboxd", "patches"):
            raise ValueError("invalid sandboxd patch path")
        source = ROOT / relative
        if sha(source) != patch["sha256"]:
            raise ValueError("sandboxd patch integrity mismatch")
        inputs[relative.as_posix()] = source
    inputs["LICENSE"] = ROOT / "LICENSE"
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".adx-package-", dir=output.parent) as temp:
        stage = Path(temp) / "package"
        stage.mkdir()
        for name, source in inputs.items():
            dest = stage / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, dest)
            if name.startswith(("bin/", "runtime/")):
                dest.chmod(0o755)
        manifest = {"schema_version": 1, "commit": commit, "dirty": dirty,
                    "target": target, "profile": profile, "redis_version": "7.2.5",
                    "files": {name: sha(stage / name) for name in sorted(inputs)}}
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        stage.rename(output)
    verify(output)
    return manifest

def verify(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest.get("schema_version") != 1 or not isinstance(manifest.get("files"), dict):
        raise ValueError("invalid package manifest")
    expected = set(manifest["files"])
    required = {f"bin/{b}" for b in BINARIES} | {"bin/redis-server", "runtime/rrt-runtime"}
    if manifest.get("profile") == "release" and "linux" in manifest.get("target", ""):
        required.add("runtime/adx-runtime-rootfs.img")
    if not required.issubset(expected) or not any(n.startswith("sdk/adx_sandbox-") and n.endswith(".whl") for n in expected):
        raise ValueError("incomplete package")
    actual = set()
    for path in directory.rglob("*"):
        if path.is_symlink():
            raise ValueError("package contains a symlink")
        if path.is_file() and path != directory / "manifest.json":
            actual.add(path.relative_to(directory).as_posix())
    if actual != expected:
        raise ValueError("package file list mismatch")
    for name, digest in manifest["files"].items():
        path = Path(name)
        if path.is_absolute() or ".." in path.parts or sha(directory / path) != digest:
            raise ValueError("package integrity check failed")
    return manifest

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("assemble")
    for arg in ("binary-dir", "redis", "wheel", "output"):
        p.add_argument("--" + arg, type=Path, required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--profile", choices=("debug", "release"), required=True)
    v = sub.add_parser("verify")
    v.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.command == "verify":
        verify(args.directory)
    else:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        status = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
        dirty = bool(status)
        if os.getenv("BUILDKITE"):
            if commit != os.environ.get("BUILDKITE_COMMIT"):
                raise ValueError("CI checkout differs from the requested commit")
            if dirty:
                raise ValueError("CI source checkout changed during build:\n" + status)
        assemble(args.binary_dir, args.redis, args.wheel, args.output, commit, dirty, args.target, args.profile)
    print("package verified")

if __name__ == "__main__":
    main()
