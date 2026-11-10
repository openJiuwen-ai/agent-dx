#!/usr/bin/env python3
"""Install the checksum-pinned gVisor release and its adjacent runtime helpers."""

import argparse
import hashlib
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

from package_manifest import GVISOR_BINARIES

# Exact published release bytes, built from akernel-dev/gvisor@49590af731d7.
RELEASE = "release-20261012.0-akernel.4"
ARTIFACTS = {
    "x86_64-unknown-linux-gnu": (
        "amd64",
        "427e78daf83d784c52527312f196baf991b73f9eb4daf9e69e474eb82650dc26",
        "8d90dfe8a7c91927030a52066473be1f895880463346ec8c63fe6ed4d22c77ba018"
        "69e06ae65dad5aa776f9122a16b17096b11d77fe2b3ae3442560d5063cfc3",
        62,
    ),
    "aarch64-unknown-linux-gnu": (
        "arm64",
        "4a8d5ec86c1e7ced8aee8960362aa53897c69dfc19e14518dce4aba78fc95e1b",
        "52447bf2b84abab428e23ae77c96c7f3fa941aed7cc833a14e25ff41cd7bcf1e931"
        "c85aaa7c6e88a74b6ee0cb412e1774f96cfe204a98de20d230cb8ca0a505b",
        183,
    ),
}


def download(target, output):
    arch, sha256, digest, machine = ARTIFACTS[target]
    destinations = [output.parent / name for name in ("gvisor-bin", "containerd-shim-runsc-v1")] + [output]
    if any(path.exists() or path.is_symlink() for path in destinations):
        raise ValueError("gVisor output already exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as directory:
        staging = Path(directory)
        archive = staging / "gvisor.tar.bz2"
        url = (f"https://agent-dx.obs.cn-southwest-2.myhuaweicloud.com/adx/release/gvisor/"
               f"{RELEASE}/linux/{arch}/{sha256}/gvisor-{arch}.tar.bz2")
        curl = shutil.which("curl")
        if curl is None:
            raise FileNotFoundError("curl is required to download runsc")
        subprocess.run(
            [curl, "--fail", "--location", "--silent", "--show-error", "--proto", "=https",
             "--proto-redir", "=https", "--retry", "3", "--max-time", "600", "--output", str(archive), url],
            check=True,
        )
        data = archive.read_bytes()
        if hashlib.sha256(data).hexdigest() != sha256:
            raise ValueError("gVisor SHA256 mismatch")
        if hashlib.sha512(data).hexdigest() != digest:
            raise ValueError("gVisor SHA512 mismatch")
        payload = staging / "payload"
        payload.mkdir()
        expected = set(GVISOR_BINARIES)
        seen = set()
        with tarfile.open(archive, "r:bz2") as bundle:
            for member in bundle:
                path = PurePosixPath(member.name)
                name = member.name.removeprefix("./")
                if path.is_absolute() or ".." in path.parts:
                    raise ValueError("unsafe gVisor archive entry: " + member.name)
                if member.isdir() and name in (".", "", "gvisor-bin", "gvisor-bin/"):
                    continue
                if not member.isfile() or name not in expected or name in seen:
                    raise ValueError("unsafe or duplicate gVisor archive entry: " + member.name)
                seen.add(name)
                destination = payload / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                with bundle.extractfile(member) as source, destination.open("wb") as dest:
                    shutil.copyfileobj(source, dest)
                with destination.open("rb") as binary:
                    header = binary.read(20)
                if header[:6] != b"\x7fELF\x02\x01" or int.from_bytes(header[18:20], "little") != machine:
                    raise ValueError("gVisor architecture mismatch: " + name)
                destination.chmod(0o755)
        if seen != expected:
            raise ValueError("gVisor archive file set mismatch")
        published = []
        try:
            for destination in destinations:
                source = payload / ("runsc" if destination == output else destination.name)
                source.rename(destination)
                published.append(destination)
        except OSError:
            for destination in reversed(published):
                if destination.is_dir():
                    shutil.rmtree(destination)
                else:
                    destination.unlink()
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=ARTIFACTS, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    download(args.target, args.output)
