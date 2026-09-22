#!/usr/bin/env python3
"""Create and verify immutable manifests for split ADX build artifacts."""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path


COMPONENTS = ("platform", "gateway", "rrt")
REQUIRED_FILES = {
    "platform": {"adxctl", "adx-master", "adx-node-manager"},
    "gateway": {
        "adx-api-server",
        "adx-edge-frontend",
        "adx-node-proxy",
        "adx-data-plane-forward",
    },
    "rrt": {"rrt-runtime", "adx-runtime-rootfs.img"},
}


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(commit, target):
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise ValueError("a full lowercase Git commit is required")
    if not target:
        raise ValueError("a build target is required")


def _regular_files(directory):
    files = {}
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            raise ValueError("component contains a symlink")
        if path.is_file() and path.name != "manifest.json":
            files[path.relative_to(directory).as_posix()] = sha256(path)
    if not files:
        raise ValueError("component contains no artifacts")
    return files


def create_manifest(component, directory, commit, target):
    if component not in COMPONENTS:
        raise ValueError("unknown component")
    _identity(commit, target)
    directory = Path(directory)
    files = _regular_files(directory)
    if set(files) != REQUIRED_FILES[component]:
        raise ValueError("component artifact file set is incomplete or unexpected")
    manifest = {
        "schema_version": 1,
        "component": component,
        "commit": commit,
        "target": target,
        "files": files,
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def verify_manifest(directory, component=None, commit=None, target=None):
    directory = Path(directory)
    manifest_path = directory / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("component manifest is missing")
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema_version") != 1 or manifest.get("component") not in COMPONENTS:
        raise ValueError("invalid component manifest")
    _identity(manifest.get("commit", ""), manifest.get("target", ""))
    if component is not None and manifest["component"] != component:
        raise ValueError("component identity mismatch")
    if commit is not None and manifest["commit"] != commit:
        raise ValueError("component commit mismatch")
    if target is not None and manifest["target"] != target:
        raise ValueError("component target mismatch")
    if set(manifest.get("files", {})) != REQUIRED_FILES[manifest["component"]]:
        raise ValueError("component artifact file set is incomplete or unexpected")
    if manifest.get("files") != _regular_files(directory):
        raise ValueError("component artifact integrity check failed")
    return manifest


def _artifact(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError("summarized artifact is missing or is a symlink")
    return {"name": path.name, "sha256": sha256(path)}


def create_build_manifest(
    component_root,
    commit,
    target,
    package_manifest,
    release_archive,
    wheel,
    backend_manifest,
    backend_archive,
):
    _identity(commit, target)
    component_root = Path(component_root)
    components = {}
    for name in COMPONENTS:
        directory = component_root / name
        manifest = verify_manifest(directory, component=name, commit=commit, target=target)
        components[name] = {
            "manifest_sha256": sha256(directory / "manifest.json"),
            "files": manifest["files"],
        }
    package_manifest = Path(package_manifest)
    package = json.loads(package_manifest.read_text())
    if package.get("commit") != commit or package.get("target") not in (None, target):
        raise ValueError("package identity differs from component identity")
    return {
        "schema_version": 1,
        "commit": commit,
        "target": target,
        "components": components,
        "package": {
            "manifest": _artifact(package_manifest),
            "archive": _artifact(release_archive),
        },
        "sdk": _artifact(wheel),
        "backend": {
            "manifest": _artifact(backend_manifest),
            "archive": _artifact(backend_archive),
        },
    }


def _verify_artifact(record, path, label):
    path = Path(path)
    if not isinstance(record, dict):
        raise ValueError(f"{label} record is missing")
    if record.get("name") != path.name or record.get("sha256") != sha256(path):
        raise ValueError(f"{label} artifact integrity check failed")


def verify_build_manifest(
    manifest_path,
    commit,
    target,
    package_manifest,
    release_archive,
    wheel,
    backend_manifest,
    backend_archive,
):
    _identity(commit, target)
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema_version") != 1:
        raise ValueError("invalid build manifest")
    if manifest.get("commit") != commit or manifest.get("target") != target:
        raise ValueError("build manifest identity mismatch")
    if set(manifest.get("components", {})) != set(COMPONENTS):
        raise ValueError("build manifest component set is incomplete")
    for name, record in manifest["components"].items():
        if set(record.get("files", {})) != REQUIRED_FILES[name]:
            raise ValueError(f"{name} component file set is incomplete")
        digest = record.get("manifest_sha256", "")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError(f"{name} component manifest digest is invalid")
    _verify_artifact(manifest.get("package", {}).get("manifest"), package_manifest, "package manifest")
    _verify_artifact(manifest.get("package", {}).get("archive"), release_archive, "release archive")
    _verify_artifact(manifest.get("sdk"), wheel, "SDK wheel")
    _verify_artifact(manifest.get("backend", {}).get("manifest"), backend_manifest, "backend manifest")
    _verify_artifact(manifest.get("backend", {}).get("archive"), backend_archive, "backend archive")
    return manifest


def _git_commit():
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create")
    create.add_argument("--component", choices=COMPONENTS, required=True)
    create.add_argument("--directory", type=Path, required=True)
    create.add_argument("--commit", default=None)
    create.add_argument("--target", required=True)

    verify = subparsers.add_parser("verify")
    verify.add_argument("--component", choices=COMPONENTS)
    verify.add_argument("--directory", type=Path, required=True)
    verify.add_argument("--commit")
    verify.add_argument("--target")

    aggregate = subparsers.add_parser("aggregate")
    aggregate.add_argument("--component-root", type=Path, required=True)
    aggregate.add_argument("--commit", default=None)
    aggregate.add_argument("--target", required=True)
    aggregate.add_argument("--package-manifest", type=Path, required=True)
    aggregate.add_argument("--release-archive", type=Path, required=True)
    aggregate.add_argument("--wheel", type=Path, required=True)
    aggregate.add_argument("--backend-manifest", type=Path, required=True)
    aggregate.add_argument("--backend-archive", type=Path, required=True)
    aggregate.add_argument("--output", type=Path, required=True)

    verify_build = subparsers.add_parser("verify-build")
    verify_build.add_argument("--manifest", type=Path, required=True)
    verify_build.add_argument("--commit", default=None)
    verify_build.add_argument("--target", required=True)
    verify_build.add_argument("--package-manifest", type=Path, required=True)
    verify_build.add_argument("--release-archive", type=Path, required=True)
    verify_build.add_argument("--wheel", type=Path, required=True)
    verify_build.add_argument("--backend-manifest", type=Path, required=True)
    verify_build.add_argument("--backend-archive", type=Path, required=True)

    arguments = parser.parse_args()
    commit = getattr(arguments, "commit", None) or _git_commit()
    if arguments.command == "create":
        create_manifest(arguments.component, arguments.directory, commit, arguments.target)
    elif arguments.command == "verify":
        verify_manifest(arguments.directory, arguments.component, arguments.commit, arguments.target)
    elif arguments.command == "aggregate":
        manifest = create_build_manifest(
            arguments.component_root,
            commit,
            arguments.target,
            arguments.package_manifest,
            arguments.release_archive,
            arguments.wheel,
            arguments.backend_manifest,
            arguments.backend_archive,
        )
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(json.dumps(manifest, indent=2) + "\n")
    else:
        verify_build_manifest(
            arguments.manifest,
            commit,
            arguments.target,
            arguments.package_manifest,
            arguments.release_archive,
            arguments.wheel,
            arguments.backend_manifest,
            arguments.backend_archive,
        )
    print("component artifacts verified")


if __name__ == "__main__":
    main()
