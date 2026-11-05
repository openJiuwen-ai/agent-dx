#!/usr/bin/env python3
"""Download a checksum-pinned gVisor runtime kit for the native Linux target."""

import argparse
import hashlib
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

# These archives are the gVisor release-20261012.0-akernel.3 values pinned by
# third_party/runtime-versions.env at the sandboxd source revision.
HELPERS = (
    "checkpointgofer",
    "gvisor-sentry-prewarmer",
    "gvisor_sentry",
    "runsc-fd-parking",
    "runsc-metric-server",
)

ARTIFACTS = {
    "x86_64-unknown-linux-gnu": {
        "url": "https://agent-dx.obs.cn-southwest-2.myhuaweicloud.com/adx/release/"
        "runsc-kit/release-20261012.0-akernel.3/amd64/"
        "685a28e6bc1d325c16e48df8e11d8f19bd04a7f103d2ec54aead5ba77e1ce88f/"
        "gvisor-amd64.tar.bz2",
        "sha512": "a57be064d37652422e52982d87ba01ccb2d263bb0c6f31b17fb8caa509f6afe5e"
        "42154cce7ac553cb90161700cd95d613837e1ee9a1ef8bb3b268b32e89c6022",
        "machine": 62,
        "files": {
            "runsc": "ea1b17130da880bf7e5c99876cbf3dd8de356cfd0e06102c7753b730c48588ac",
            "gvisor-bin/checkpointgofer": "2630b9790b7369bb9d150fab7ac3ab53b18fa0a327503cf04938b32084f96356",
            "gvisor-bin/gvisor-sentry-prewarmer": "46d5c028723ac5fbe40b863f95100f15a00d2aa0cd39c69d892500a988c930bd",
            "gvisor-bin/gvisor_sentry": "3d15603f0e0881cad7ba548498e25a989062cef768fc6e47bb80393d9a5c9c67",
            "gvisor-bin/runsc-fd-parking": "03af90d07ea466c10ecc3e3ba839b2a59540959dd3286aea3886917cba850c11",
            "gvisor-bin/runsc-metric-server": "ee0c0d873660998f483b8d91df89e5358ab5e0a1ce4b87d4f352c485361b9c29",
        },
    },
    "aarch64-unknown-linux-gnu": {
        "url": "https://agent-dx.obs.cn-southwest-2.myhuaweicloud.com/adx/release/"
        "runsc-kit/release-20261012.0-akernel.3/arm64/"
        "262f5d0848ed30f2d1c4c167d2c38734b4e7eb4c75c15315c87db3ecfa395e6f/"
        "gvisor-arm64.tar.bz2",
        "sha512": "51f7ac4b502b4c7d5d4b302a5eb2f41682a823b6fb104624e3fc9edda4e8ca175"
        "b9fb1e6c7fb37b901b4f504704f36486e773314f7f003514fc5218c9d360452",
        "machine": 183,
        "files": {
            "runsc": "447f84293fdeaaf3f0a80d56a3ada101d160a002519728da04e894b6e90141a1",
            "gvisor-bin/checkpointgofer": "36cdc2e75340b229c2313ce46c4e6821adf66e4d5ef93dba8bf10fd23807b7d8",
            "gvisor-bin/gvisor-sentry-prewarmer": "3ac0099cc7492865a44a1c871fb3cf8b64e58aba773ece13fc9c0421d8375848",
            "gvisor-bin/gvisor_sentry": "287c22ef290921489c209613961d5b78df3d8558c34365100da46a5bbfc25821",
            "gvisor-bin/runsc-fd-parking": "e41db2da554154f282f2829a664a2c4234c163b9da74fc9b4388126223c7090c",
            "gvisor-bin/runsc-metric-server": "4682a850aeba9536226b11c3b52d0caa6cb5233c65f0c37e5913f28588559899",
        },
    },
}


def safe_member(member):
    path = PurePosixPath(member.name)
    return (
        bool(member.name)
        and not path.is_absolute()
        and ".." not in path.parts
        and str(path) == member.name.rstrip("/")
    )


def verify_runtime(runsc, target):
    """Verify a runtime kit staged beside *runsc* and return its file hashes."""
    artifact = ARTIFACTS[target]
    runsc = Path(runsc)
    helpers = runsc.parent / "gvisor-bin"
    if helpers.is_symlink() or not helpers.is_dir():
        raise ValueError("gVisor helper directory is missing or is a symlink")
    actual = {}
    for name, digest in artifact["files"].items():
        path = runsc if name == "runsc" else runsc.parent / name
        if path.is_symlink() or not path.is_file() or not path.stat().st_mode & 0o100:
            raise ValueError("gVisor runtime file is missing or not executable: " + name)
        actual[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual[name] != digest:
            raise ValueError("gVisor runtime file checksum mismatch: " + name)
        with path.open("rb") as binary:
            header = binary.read(20)
        if header[:6] != b"\x7fELF\x02\x01" or int.from_bytes(header[18:20], "little") != artifact["machine"]:
            raise ValueError("gVisor runtime architecture mismatch: " + name)
    return actual


def extract_runtime(archive, destination, target):
    artifact = ARTIFACTS[target]
    expected = set(artifact["files"])
    allowed = expected | {"containerd-shim-runsc-v1"}
    with tarfile.open(archive, "r:bz2") as bundle:
        members = {}
        for member in bundle.getmembers():
            if not safe_member(member):
                raise ValueError("unsafe gVisor archive member: " + member.name)
            if member.isdir() and member.name.rstrip("/") == "gvisor-bin":
                continue
            if not member.isfile() or member.name not in allowed or member.name in members:
                raise ValueError("unsafe or duplicate gVisor archive member: " + member.name)
            members[member.name] = member
        if not expected.issubset(members):
            raise ValueError("gVisor archive is missing required runtime helpers")
        for name, member in members.items():
            if name not in expected:
                continue
            output_path = destination / name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with bundle.extractfile(member) as source, output_path.open("wb") as output:
                shutil.copyfileobj(source, output)
            output_path.chmod(0o755)
    verify_runtime(destination / "runsc", target)


def download(target, output):
    artifact = ARTIFACTS[target]
    helpers = output.parent / "gvisor-bin"
    if output.exists() or helpers.exists():
        raise ValueError("runsc output already exists")
    with tempfile.TemporaryDirectory(dir=output.parent) as directory:
        staged = Path(directory) / "gvisor.tar.bz2"
        curl = shutil.which("curl")
        if curl is None:
            raise FileNotFoundError("curl is required to download runsc")
        subprocess.run(
            [curl, "--fail", "--location", "--silent", "--show-error", "--proto", "=https",
             "--proto-redir", "=https", "--retry", "3", "--max-time", "300", "--output", str(staged), artifact["url"]],
            check=True,
        )
        data = staged.read_bytes()
        if hashlib.sha512(data).hexdigest() != artifact["sha512"]:
            raise ValueError("gVisor archive SHA512 mismatch")
        extracted = Path(directory) / "runtime"
        extract_runtime(staged, extracted, target)
        (extracted / "gvisor-bin").rename(helpers)
        (extracted / "runsc").rename(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=ARTIFACTS, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    download(args.target, args.output)
