#!/usr/bin/env python3
"""Download a checksum-pinned runsc binary for the native Linux target."""

import argparse
import hashlib
import shutil
import subprocess
import tempfile
from pathlib import Path

# Built from the AKernel runsc integration. The arm64 input was taken from
# akernel-all-in-one:kill-retry-fix-arm64. Its image ID is
# sha256:5d895f6f34404246e087a9e0323f18ae2d03a26dafe701cc5608559dc3829056.
# The amd64 input came from cluster-all-in-one@sha256:8d9b2e1278eee889be71ba12100357505f0f9ba6902fe78073dfc1fd8f3224e7.
ARTIFACTS = {
    "x86_64-unknown-linux-gnu": (
        "amd64/392a88191ffd4bb44b0d7f4a5527b316fb8b8e0f07ec15847fe62d4eac08a86f",
        "42a31cd92ebd70d206ee31558978079e67958d8a298be13d2a047853d930fd44"
        "d5c58f730a91e4a25a11b04b67c2b17a30128e0f7db2f528c32744da6afae7e0",
        62,
    ),
    "aarch64-unknown-linux-gnu": (
        "arm64/dd611b721ede7f6eb8f543067a5382a66ba48255726b0d2d2bc70493583d56d7",
        "4e94ee139ec51ece0266b1b767316465a0251337019f0349a3b7a5930e79ff58c"
        "d5af6d97dd2a9c11f905c06107c2a4ef9cbbb32aeb6906789b6df2417604fa0",
        183,
    ),
}


def download(target, output):
    arch, digest, machine = ARTIFACTS[target]
    if output.exists():
        raise ValueError("runsc output already exists")
    with tempfile.TemporaryDirectory(dir=output.parent) as directory:
        staged = Path(directory) / "runsc"
        url = f"https://agent-dx.obs.cn-southwest-2.myhuaweicloud.com/adx/release/runsc/{arch}/runsc"
        curl = shutil.which("curl")
        if curl is None:
            raise FileNotFoundError("curl is required to download runsc")
        subprocess.run(
            [curl, "--fail", "--location", "--silent", "--show-error", "--proto", "=https",
             "--proto-redir", "=https", "--retry", "3", "--max-time", "300", "--output", str(staged), url],
            check=True,
        )
        data = staged.read_bytes()
        if hashlib.sha512(data).hexdigest() != digest:
            raise ValueError("runsc SHA512 mismatch")
        if data[:6] != b"\x7fELF\x02\x01" or int.from_bytes(data[18:20], "little") != machine:
            raise ValueError("runsc architecture mismatch")
        staged.chmod(0o755)
        staged.rename(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=ARTIFACTS, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    download(args.target, args.output)
