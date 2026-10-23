#!/usr/bin/env python3
"""Prepare the pinned Jiuwenbox wheel and a fresh supervisor server venv."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

LOCK = Path(__file__).with_name("prebuilt-lock.json")


def verify(path, artifact):
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(chunk)
    digest = checksum.hexdigest()
    if path.stat().st_size != artifact["size"] or digest != artifact["sha256"]:
        raise ValueError("pinned artifact differs: " + artifact["file"])


def cached(cache, artifact):
    relative = Path(artifact["cache_path"])
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("invalid pinned cache path")
    return cache / relative


def extract_wheel(archive, output, artifact):
    with tarfile.open(archive) as bundle:
        matches = [m for m in bundle.getmembers() if Path(m.name).name == artifact["file"]]
        if len(matches) != 1 or not matches[0].isfile() or matches[0].size != artifact["size"]:
            raise ValueError("archive must contain exactly one expected wheel")
        with bundle.extractfile(matches[0]) as source, output.open("wb") as destination:
            shutil.copyfileobj(source, destination)
    verify(output, artifact)


def prepare(cache, output):
    cache, output = cache.resolve(), output.resolve()
    cache.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=False)
    lock = json.loads(LOCK.read_text())
    archive = next(a for a in lock["archives"] if a["name"] == "jiuwenswarm-daily")
    wheel = lock["jiuwenbox"]["artifact"]
    archive_path, wheel_path = cached(cache, archive), cached(cache, wheel)
    with tempfile.TemporaryDirectory(prefix=".jiuwenbox-", dir=cache) as tmp:
        tmp = Path(tmp)
        if not archive_path.exists():
            downloaded = tmp / archive["file"]
            curl = shutil.which("curl")
            if curl is None:
                raise FileNotFoundError("curl is required to download pinned artifacts")
            subprocess.run(
                [
                    curl,
                    "--fail",
                    "--location",
                    "--proto",
                    "=https",
                    "--proto-redir",
                    "=https",
                    "--retry",
                    "3",
                    "--connect-timeout",
                    "15",
                    "--max-time",
                    "300",
                    "--output",
                    str(downloaded),
                    archive["source_url"],
                ],
                check=True,
            )
            verify(downloaded, archive)
            archive_path.parent.mkdir(parents=True, exist_ok=True)
            downloaded.replace(archive_path)
        verify(archive_path, archive)
        if not wheel_path.exists():
            extracted = tmp / wheel["file"]
            extract_wheel(archive_path, extracted, wheel)
            wheel_path.parent.mkdir(parents=True, exist_ok=True)
            extracted.replace(wheel_path)
        verify(wheel_path, wheel)
    (output / "pinned").symlink_to(cache)
    venv = output / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
    python = str(venv / "bin/python")
    subprocess.run(
        [
            python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "-r",
            str(Path(__file__).with_name("requirements-supervisor.txt")),
        ],
        check=True,
    )
    subprocess.run(
        [python, "-m", "pip", "install", "--disable-pip-version-check", "--no-deps", str(wheel_path)],
        check=True,
    )
    sites = list((venv / "lib").glob("python*/site-packages"))
    if len(sites) != 1 or not (sites[0] / "jiuwenbox").is_dir():
        raise ValueError("Jiuwenbox wheel installation is missing")
    link = output / "jiuwenbox/src/jiuwenbox"
    link.parent.mkdir(parents=True)
    link.symlink_to(sites[0] / "jiuwenbox")
    (output / "artifacts").mkdir()
    (output / "prepared.json").write_text(
        json.dumps(
            {
                "distribution": "jiuwenswarm",
                "version": lock["jiuwenbox"]["version"],
                "wheel_sha256": wheel["sha256"],
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    prepare(args.cache, args.output)
