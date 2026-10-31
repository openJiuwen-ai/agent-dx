"""Stage and verify opt-in AFS test inputs without starting product processes."""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil

BINARIES = ("afs-meta", "afs-node")
MACHINES = {"x86_64-unknown-linux-gnu": 62, "aarch64-unknown-linux-gnu": 183}
REQUIRED = {"run.py", "fixtures/common.py", "fixtures/worker.py", "inputs/artifact-identity.json"}


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def full_digest(value, size):
    if not isinstance(value, str):
        raise ValueError("explicit hexadecimal identity required")
    if not re.fullmatch("[0-9a-f]{" + str(size) + "}", value):
        raise ValueError("invalid hexadecimal identity")
    return value


def enabled(package):
    if not isinstance(package, dict):
        raise ValueError("package identity must be an object")
    value = package.get("with_afs", False)
    if not isinstance(value, bool):
        raise ValueError("package with_afs must be boolean")
    return value


def artifact_identity(package, harness_commit):
    if not enabled(package):
        raise ValueError("AFS test inputs require an AFS ON package")
    full_digest(harness_commit, 40)
    full_digest(package.get("commit"), 40)
    target = package.get("target")
    if target not in MACHINES:
        raise ValueError("unsupported AFS product target")
    files = package.get("files")
    if not isinstance(files, dict):
        raise ValueError("product file digest inventory required")
    binaries = {name: full_digest(files.get("bin/" + name), 64) for name in BINARIES}
    return {
        "schema_version": 1,
        "commit": package["commit"],
        "target": target,
        "binaries": binaries,
        "harness_commit": harness_commit,
    }


def asset_name(name):
    if not isinstance(name, str):
        raise ValueError("asset path must be a string")
    path = PurePosixPath(name)
    if not path.parts or path.is_absolute() or str(path) != name or ".." in path.parts:
        raise ValueError("asset path escapes its root")
    if any(value in name for value in ("\\", "\n", "\r")):
        raise ValueError("unsafe asset path")
    if name in ("run.py", "README.md", "inputs/artifact-identity.json"):
        return name
    if path.parts[0] not in ("fixtures", "suites", "tools", "runner_tests") or "__pycache__" in path.parts:
        raise ValueError("unexpected runtime asset")
    if path.suffix != ".py" and not name.endswith(".example.json"):
        raise ValueError("only runtime Python and example configuration assets are supported")
    return name


def source_files(source):
    source = Path(source)
    if source.is_symlink() or not source.is_dir():
        raise ValueError("AFS test source directory is missing or symlinked")
    files = {}
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if "__pycache__" in relative.parts:
            continue
        if path.is_symlink():
            raise ValueError("symlinked test source is not accepted")
        if not path.is_file():
            continue
        try:
            name = asset_name(relative.as_posix())
        except ValueError:
            continue
        if name.startswith("inputs/"):
            raise ValueError("test source must not supply generated product identities")
        files[name] = path
    if not (REQUIRED - {"inputs/artifact-identity.json"}) <= files.keys():
        raise ValueError("AFS runner/worker source inventory is incomplete")
    if not any(name.startswith("suites/") for name in files):
        raise ValueError("AFS suites are missing")
    return files


def stage(source, destination, package, harness_commit):
    """Copy runtime test sources and bind their identities to the candidate."""
    identity = artifact_identity(package, harness_commit)
    files = source_files(source)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    digests = {}
    for name, source_path in files.items():
        output = destination / name
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, output)
        digests[name] = sha(output)
    identity_path = destination / "inputs/artifact-identity.json"
    identity_path.parent.mkdir()
    identity_path.write_bytes(json_bytes(identity))
    digests["inputs/artifact-identity.json"] = sha(identity_path)
    manifest = {
        "schema_version": 1,
        "harness_commit": harness_commit,
        "product_commit": package["commit"],
        "target": package["target"],
        "files": digests,
    }
    (destination / "manifest.json").write_bytes(json_bytes(manifest))
    return manifest


def validate_manifest(manifest, package, harness_commit=None):
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise ValueError("unsupported AFS asset schema")
    recorded_commit = manifest.get("harness_commit")
    identity = artifact_identity(package, recorded_commit)
    if harness_commit is not None and recorded_commit != harness_commit:
        raise ValueError("AFS test harness commit mismatch")
    if manifest.get("product_commit") != identity["commit"] or manifest.get("target") != identity["target"]:
        raise ValueError("AFS test assets refer to another product")
    files = manifest.get("files")
    if not isinstance(files, dict) or not REQUIRED <= files.keys():
        raise ValueError("incomplete AFS asset file inventory")
    for name, digest in files.items():
        asset_name(name)
        full_digest(digest, 64)
    if not any(name.startswith("suites/") for name in files):
        raise ValueError("AFS suite file inventory is missing")
    expected = hashlib.sha256(json_bytes(identity)).hexdigest()
    if files["inputs/artifact-identity.json"] != expected:
        raise ValueError("runner product identity is not derived from the candidate")
    return identity


def verify_bundle(directory, bundle, expected_mode=None):
    """Check small handoff manifests before any image or cluster operation."""
    directory = Path(directory)
    package = bundle.get("package", {})
    mode = enabled(package)
    if expected_mode is not None and mode != expected_mode:
        raise ValueError("AFS candidate mode differs from ADX_WITH_AFS")
    record = bundle.get("afs_tests")
    path = directory / "afs-tests.json"
    if not mode:
        if record is not None or path.exists() or path.is_symlink():
            raise ValueError("AFS OFF bundle must not include AFS test assets")
        return None
    if not isinstance(record, dict) or record.get("manifest") != "afs-tests.json":
        raise ValueError("AFS ON bundle lacks its test asset handoff; prepare it with current tooling")
    if path.is_symlink() or not path.is_file():
        raise ValueError("AFS test manifest is missing or symlinked")
    if sha(path) != full_digest(record.get("sha256"), 64):
        raise ValueError("AFS test manifest digest mismatch")
    manifest = json.loads(path.read_text())
    validate_manifest(manifest, package)
    return manifest


def verify_payload(assets, manifest, links):
    actual = {}
    for path in assets.rglob("*"):
        name = path.relative_to(assets).as_posix()
        if path.is_symlink():
            if name not in links or path.resolve() != links[name]:
                raise ValueError("unowned symlink in AFS test inputs")
            continue
        if path.is_file() and name != "manifest.json":
            actual[name] = sha(path)
    if actual != manifest["files"]:
        raise ValueError("AFS test files differ from their manifest")


def materialize(assets, package_directory, harness_commit, product_commit, target):
    """Verify real candidate bytes, then reference them without modifying the package."""
    assets = Path(assets)
    package_directory = Path(package_directory).resolve()
    manifest_path = assets / "manifest.json"
    product_path = package_directory / "manifest.json"
    if assets.is_symlink() or manifest_path.is_symlink() or product_path.is_symlink():
        raise ValueError("handoff roots/manifests must not be symlinks")
    manifest = json.loads(manifest_path.read_text())
    package = json.loads(product_path.read_text())
    if package.get("commit") != product_commit or package.get("target") != target:
        raise ValueError("materialized product identity mismatch")
    identity = validate_manifest(manifest, package, harness_commit)
    binary_root = package_directory / "bin"
    if binary_root.is_symlink():
        raise ValueError("product binary directory must not be symlinked")
    links = {"inputs/" + name: binary_root / name for name in BINARIES}
    verify_payload(assets, manifest, links)
    # Validate both binaries and existing link ownership before creating anything.
    for name in BINARIES:
        path = binary_root / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("regular product binary required: " + name)
        if sha(path) != identity["binaries"][name] or not path.stat().st_mode & 0o111:
            raise ValueError("AFS product binary digest/mode mismatch: " + name)
        with path.open("rb") as stream:
            header = stream.read(20)
        if len(header) != 20 or header[:6] != b"\x7fELF\x02\x01":
            raise ValueError("AFS Linux ELF64 artifact required")
        if int.from_bytes(header[18:20], "little") != MACHINES[target]:
            raise ValueError("AFS product architecture mismatch")
    for name in BINARIES:
        link = assets / "inputs" / name
        if not link.is_symlink():
            link.symlink_to(binary_root / name)
    return identity


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("verify-bundle")
    check.add_argument("--bundle", type=Path, required=True)
    check.add_argument("--with-afs", choices=("0", "1"), required=True)
    inputs = commands.add_parser("materialize")
    for name in ("assets", "package"):
        inputs.add_argument("--" + name, type=Path, required=True)
    for name in ("harness-commit", "product-commit", "target"):
        inputs.add_argument("--" + name, required=True)
    args = parser.parse_args()
    if args.command == "verify-bundle":
        verify_bundle(args.bundle.parent, json.loads(args.bundle.read_text()), args.with_afs == "1")
    else:
        materialize(args.assets, args.package, args.harness_commit, args.product_commit, args.target)


if __name__ == "__main__":
    main()
