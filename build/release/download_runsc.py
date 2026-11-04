#!/usr/bin/env python3
"""Download the pinned gVisor runsc release for the native Linux target."""

import argparse
import hashlib
import shutil
import subprocess
import tempfile
from pathlib import Path

RELEASE = "20260817.0"
ARTIFACTS = {
    "x86_64-unknown-linux-gnu": (
        "x86_64",
        "84936438d583ec976800f464e75a83e1515f0890b451b9b4db219c4472b54ca9b"
        "106a6772ee683f1e64cce2128871d7637b14d800591f8451b8137f6c39fb2ef",
        62,
    ),
    "aarch64-unknown-linux-gnu": (
        "aarch64",
        "6394fd161a4af0dc9a2c29f75c3016d05275a55744f124e12023fa7666a9f161"
        "c68d6ce3803ad49205c6a7b5bee0ad2ccf48edff340db344fdafec678c788aa4",
        183,
    ),
}


def download(target, output):
    arch, digest, machine = ARTIFACTS[target]
    if output.exists():
        raise ValueError("runsc output already exists")
    with tempfile.TemporaryDirectory(dir=output.parent) as directory:
        staged = Path(directory) / "runsc"
        url = f"https://storage.googleapis.com/gvisor/releases/release/{RELEASE}/{arch}/runsc"
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
