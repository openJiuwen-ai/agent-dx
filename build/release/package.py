#!/usr/bin/env python3
"""Assemble explicit build artifacts; never compile or fetch during deployment."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))
from package_manifest import AFS_BINARIES, BINARIES  # noqa: E402
from package_manifest import verify as verify_manifest


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def assemble(binary_dir, redis, redis_cli, wheel, output, commit, dirty, target, profile, with_afs=False):
    if output.exists():
        raise ValueError("output already exists")
    inputs = {f"bin/{name}": binary_dir / name for name in BINARIES}
    if with_afs:
        inputs.update({f"bin/{name}": binary_dir / name for name in AFS_BINARIES})
    inputs["runtime/adx-execd"] = binary_dir / "adx-execd"
    envd_dir = binary_dir / "envd"
    envd_component = (binary_dir / "envd-manifest.json", binary_dir / "envd-LICENSE")
    if envd_dir.exists() and any(path.exists() for path in envd_component):
        raise ValueError("envd provenance has multiple sources")
    if envd_dir.exists():
        if envd_dir.is_symlink() or not envd_dir.is_dir():
            raise ValueError("envd build output must be a directory")
        envd_manifest = envd_dir / "manifest.json"
        envd_license = envd_dir / "LICENSE"
    elif any(path.exists() for path in envd_component):
        envd_manifest, envd_license = envd_component
    else:
        envd_manifest = envd_license = None
    if envd_manifest is not None:
        if any(path.is_symlink() or not path.is_file() for path in (envd_manifest, envd_license)):
            raise ValueError("envd build manifest and license are required")
        envd = json.loads(envd_manifest.read_text())
        if not isinstance(envd.get("sha256"), str) or len(envd["sha256"]) != 64 or any(
            character not in "0123456789abcdef" for character in envd["sha256"].lower()
        ):
            raise ValueError("envd build manifest is invalid")
        inputs["runtime/envd/manifest.json"] = envd_manifest
        inputs["runtime/envd/LICENSE"] = envd_license
    # Native Linux release builder supplies the EROFS payload. Debug/native macOS
    # packages retain binary-only development support.
    runtime_root = binary_dir / "adx-runtime-rootfs.img"
    if profile == "release" and "linux" in target:
        inputs["runtime/adx-runtime-rootfs.img"] = runtime_root
    elif runtime_root.is_file():
        inputs["runtime/adx-runtime-rootfs.img"] = runtime_root
    inputs["bin/redis-server"] = redis
    inputs["bin/redis-cli"] = redis_cli
    inputs["install.sh"] = ROOT / "build/release/install.sh"
    inputs["lib/package_manifest.py"] = ROOT / "build/release/package_manifest.py"
    if not wheel.name.startswith("adx_environment-") or wheel.suffix != ".whl":
        raise ValueError("an adx_environment wheel is required")
    inputs[f"sdk/{wheel.name}"] = wheel
    for source in inputs.values():
        if source.is_symlink() or not source.is_file():
            raise ValueError("a required artifact is missing or is a symlink")
    if not target or profile not in ("debug", "release") or len(commit) != 40:
        raise ValueError("build identity required")
    version = subprocess.check_output([str(redis.resolve()), "--version"], text=True)
    if "v=7.2.5 " not in version:
        raise ValueError("Redis version differs from pinned 7.2.5")
    cli_version = subprocess.check_output([str(redis_cli.resolve()), "--version"], text=True)
    if cli_version.strip() != "redis-cli 7.2.5":
        raise ValueError("Redis CLI version differs from pinned 7.2.5")
    for path in (ROOT / "build/config/examples").iterdir():
        if path.is_file():
            inputs[f"etc/examples/{path.name}"] = path
    if with_afs:
        for path in (ROOT / "build/config/examples/afs").iterdir():
            if path.is_file():
                inputs[f"etc/examples/afs/{path.name}"] = path
        for path in (ROOT / "docs/migration/licenses/afs-source").iterdir():
            if path.is_file():
                inputs[f"third_party/afs-source/{path.name}"] = path
    for component in ("redis", "sandboxd"):
        for name in ("source.json", "LICENSE"):
            inputs[f"third_party/{component}/{name}"] = ROOT / "third_party" / component / name
    sandboxd_source = json.loads((ROOT / "third_party/sandboxd/source.json").read_text())
    for patch in sandboxd_source.get("patches", []):
        relative = Path(patch["path"])
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or relative.parts[:3] != ("third_party", "sandboxd", "patches")
        ):
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
            if name.startswith(("bin/", "runtime/")) or name in {
                "install.sh",
                "etc/examples/afs/prepare-local-tls.sh",
            }:
                dest.chmod(0o755)
        manifest = {
            "schema_version": 1,
            "commit": commit,
            "dirty": dirty,
            "target": target,
            "profile": profile,
            "redis_version": "7.2.5",
            "files": {name: sha(stage / name) for name in sorted(inputs)},
        }
        if with_afs:
            manifest["with_afs"] = True
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        stage.rename(output)
    verify(output)
    return manifest


def verify(directory):
    return verify_manifest(directory)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("assemble")
    for arg in ("binary-dir", "redis", "redis-cli", "wheel", "output"):
        p.add_argument("--" + arg, type=Path, required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--profile", choices=("debug", "release"), required=True)
    p.add_argument("--with-afs", action="store_true", help="include OwnerFs and DistributedFs binaries")
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
        assemble(
            args.binary_dir,
            args.redis,
            args.redis_cli,
            args.wheel,
            args.output,
            commit,
            dirty,
            args.target,
            args.profile,
            args.with_afs,
        )
    print("package verified")


if __name__ == "__main__":
    main()
