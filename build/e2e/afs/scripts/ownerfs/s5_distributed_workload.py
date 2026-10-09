#!/usr/bin/env python3
"""Cross-node W2 stages for the S5 DMS versus MooseFS confirmation track."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import Any


def load_posix_workload():
    path = Path(__file__).with_name("s5_posix_workload.py")
    spec = importlib.util.spec_from_file_location("s5_posix_workload", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load workload module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


POSIX = load_posix_workload()
W2_FILE_COUNT = 200
W2_FILE_SIZE = 4096


def w2_directory(root: Path, seed: int) -> Path:
    return root / f"w2-{seed}"


def w2_path(root: Path, seed: int, index: int) -> Path:
    return w2_directory(root, seed) / f"shared-{index:04d}.bin"


def w2_bytes(seed: int, index: int, version: str, size: int) -> bytes:
    return POSIX.deterministic_bytes(f"w2:{version}:{index}", size, seed)


def prepare_w2(root: Path, *, seed: int, count: int = W2_FILE_COUNT, size: int = W2_FILE_SIZE) -> dict[str, Any]:
    directory = w2_directory(root, seed)
    if directory.exists():
        raise RuntimeError(f"W2 directory already exists: {directory}")
    directory.mkdir(parents=True)
    POSIX.sync_directory(directory.parent)
    for index in range(count):
        path = w2_path(root, seed, index)
        data = w2_bytes(seed, index, "original", size)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        try:
            POSIX.write_all(descriptor, data)
            os.fdatasync(descriptor)
        finally:
            os.close(descriptor)
    POSIX.sync_directory(directory)
    return {"correctness": True, "files": count, "bytes_per_file": size}


def read_w2(
    root: Path,
    *,
    seed: int,
    version: str,
    operation: str,
    count: int = W2_FILE_COUNT,
    size: int = W2_FILE_SIZE,
    metrics_address: str | None = None,
    workers: int = 1,
) -> dict[str, Any]:
    def read_one(index: int) -> float:
        path = w2_path(root, seed, index)
        expected = w2_bytes(seed, index, version, size)
        return POSIX.measure(lambda: POSIX.read_exact(path, expected))

    def read_all() -> list[float]:
        if workers == 1:
            return [read_one(index) for index in range(count)]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            return list(pool.map(read_one, range(count)))

    samples, ledger = POSIX.measured_phase(operation, metrics_address, read_all)
    return {
        "operation": POSIX.summarize_samples(samples),
        "phase_ledger": ledger,
        "correctness": True,
    }


def overwrite_w2(
    root: Path,
    *,
    seed: int,
    operation: str = "remote_overwrite",
    count: int = W2_FILE_COUNT,
    size: int = W2_FILE_SIZE,
    metrics_address: str | None = None,
    workers: int = 1,
) -> dict[str, Any]:
    def overwrite_one(index: int) -> float:
        path = w2_path(root, seed, index)
        data = w2_bytes(seed, index, "updated", size)

        def overwrite() -> None:
            descriptor = os.open(path, os.O_RDWR)
            try:
                written = os.pwrite(descriptor, data, 0)
                if written != len(data):
                    raise RuntimeError(f"short overwrite: {path}: {written}/{len(data)}")
                os.fdatasync(descriptor)
            finally:
                os.close(descriptor)

        return POSIX.measure(overwrite)

    def overwrite_all() -> list[float]:
        if workers == 1:
            return [overwrite_one(index) for index in range(count)]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            return list(pool.map(overwrite_one, range(count)))

    samples, ledger = POSIX.measured_phase(operation, metrics_address, overwrite_all)
    return {
        "operation": POSIX.summarize_samples(samples),
        "phase_ledger": ledger,
        "correctness": True,
    }


def cleanup_w2(root: Path, *, seed: int) -> dict[str, Any]:
    directory = w2_directory(root, seed)
    shutil.rmtree(directory)
    POSIX.sync_directory(directory.parent)
    return {"correctness": not directory.exists()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "read", "overwrite", "cleanup"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--count", type=int, default=W2_FILE_COUNT)
    parser.add_argument("--size", type=int, default=W2_FILE_SIZE)
    parser.add_argument("--version", choices=("original", "updated"), default="original")
    parser.add_argument("--operation", default="first_read")
    parser.add_argument("--metrics-address")
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    if args.action == "prepare":
        result = prepare_w2(args.root, seed=args.seed, count=args.count, size=args.size)
    elif args.action == "read":
        result = read_w2(
            args.root,
            seed=args.seed,
            version=args.version,
            operation=args.operation,
            count=args.count,
            size=args.size,
            metrics_address=args.metrics_address,
            workers=args.workers,
        )
    elif args.action == "overwrite":
        result = overwrite_w2(
            args.root,
            seed=args.seed,
            operation=args.operation,
            count=args.count,
            size=args.size,
            metrics_address=args.metrics_address,
            workers=args.workers,
        )
    else:
        result = cleanup_w2(args.root, seed=args.seed)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
