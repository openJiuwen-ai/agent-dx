#!/usr/bin/env python3
"""Resolve Cargo cache locations shared by ADX Git worktrees."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess


def command(*argv: str) -> str:
    return subprocess.check_output(argv, text=True).strip()


def sanitize(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-.")
    return cleaned or "default"


def parse_rustc_info(text: str) -> tuple[str, str]:
    values = {}
    for line in text.splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            values[key] = value
    if not values.get("host") or not values.get("release"):
        raise ValueError("rustc -vV did not report host and release")
    return values["host"], values["release"]


@dataclass(frozen=True)
class Layout:
    repo: Path
    primary: Path
    cache_root: Path
    cache_key: str

    @property
    def shared_target(self) -> Path:
        return self.cache_root / "cargo-target" / self.cache_key

    @property
    def isolated_target(self) -> Path:
        digest = hashlib.sha256(str(self.repo).encode()).hexdigest()[:8]
        worktree = f"{sanitize(self.repo.name)}-{digest}"
        return self.cache_root / "cargo-target-isolated" / self.cache_key / worktree

    @property
    def sccache(self) -> Path:
        return self.cache_root / "sccache"


def discover(repo: Path, cache_root: Path | None = None, rustc: str = "rustc") -> Layout:
    repo = Path(command("git", "-C", str(repo), "rev-parse", "--show-toplevel")).resolve()
    common = Path(
        command("git", "-C", str(repo), "rev-parse", "--path-format=absolute", "--git-common-dir")
    ).resolve()
    primary = common.parent
    configured = cache_root or (Path(os.environ["ADX_BUILD_CACHE_ROOT"]) if os.environ.get("ADX_BUILD_CACHE_ROOT") else None)
    root = configured.expanduser().resolve() if configured else primary / ".adx-cache"
    host, release = parse_rustc_info(command(rustc, "-vV"))
    key = f"{sanitize(host)}-rust{sanitize(release)}"
    return Layout(repo=repo, primary=primary, cache_root=root, cache_key=key)


def environment(layout: Layout, mode: str) -> dict[str, str]:
    target = layout.shared_target if mode == "shared" else layout.isolated_target
    values = {
        "ADX_BUILD_CACHE_ROOT": str(layout.cache_root),
        "CARGO_TARGET_DIR": str(target),
        "CARGO_INCREMENTAL": "0",
        "SCCACHE_DIR": str(layout.sccache),
        "SCCACHE_CACHE_SIZE": os.environ.get("SCCACHE_CACHE_SIZE", "20G"),
    }
    wrapper = shutil.which("sccache")
    if wrapper:
        values["RUSTC_WRAPPER"] = wrapper
    return values


def allocated_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    blocks = int(command("du", "-sk", str(path)).split()[0])
    return blocks * 1024


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.1f}{unit}"
        value /= 1024
    raise AssertionError("unreachable")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--cache-root", type=Path)
    parser.add_argument("--rustc", default="rustc")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("cache-root")
    subparsers.add_parser("cache-key")
    target = subparsers.add_parser("target-dir")
    target.add_argument("--mode", choices=("shared", "isolated"), default="shared")
    env = subparsers.add_parser("env")
    env.add_argument("--mode", choices=("shared", "isolated"), default="shared")
    env.add_argument("--format", choices=("shell", "json"), default="shell")
    status = subparsers.add_parser("status")
    status.add_argument("--target-dir", type=Path)
    status.add_argument("--json", action="store_true")
    args = parser.parse_args()

    layout = discover(args.repo, args.cache_root, args.rustc)
    if args.command == "cache-root":
        print(layout.cache_root)
        return 0
    if args.command == "cache-key":
        print(layout.cache_key)
        return 0
    if args.command == "target-dir":
        print(layout.shared_target if args.mode == "shared" else layout.isolated_target)
        return 0
    if args.command == "env":
        values = environment(layout, args.mode)
        if args.format == "json":
            print(json.dumps(values, indent=2, sort_keys=True))
        else:
            for key, value in sorted(values.items()):
                print(f"export {key}={shlex.quote(value)}")
        return 0

    target_dir = (args.target_dir or layout.shared_target).expanduser().resolve()
    report = {
        "cache_root": str(layout.cache_root),
        "cache_key": layout.cache_key,
        "target_dir": str(target_dir),
        "target_bytes": allocated_bytes(target_dir),
        "incremental_bytes": allocated_bytes(target_dir / "debug" / "incremental"),
        "deps_bytes": allocated_bytes(target_dir / "debug" / "deps"),
        "sccache_dir": str(layout.sccache),
        "sccache_bytes": allocated_bytes(layout.sccache),
        "sccache_available": bool(shutil.which("sccache")),
    }
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for key, value in report.items():
            print(f"{key}={human_size(value) if key.endswith('_bytes') else value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
