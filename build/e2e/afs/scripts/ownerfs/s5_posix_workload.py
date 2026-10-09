#!/usr/bin/env python3
"""S5 equal-durability POSIX workloads with raw samples and phase ledgers."""

from __future__ import annotations

import hashlib
import math
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any, Callable, Iterable
from urllib.request import ProxyHandler, build_opener


W1_LAYOUT = ((4096, 160), (64 * 1024, 40))
W3_FILES = (("1m", 1024 * 1024), ("512m", 512 * 1024 * 1024))
W3_OPERATIONS_PER_SIZE = 6
PATCH_SIZE = 4096
IO_CHUNK = 8 * 1024 * 1024


def nearest_rank(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


def summarize_samples(samples: Iterable[float]) -> dict[str, Any]:
    values = [float(value) for value in samples]
    if not values:
        raise ValueError("cannot summarize an empty sample set")
    return {
        "sample_count": len(values),
        "samples_us": values,
        "p50_us": nearest_rank(values, 0.50),
        "p95_us": nearest_rank(values, 0.95),
    }


def measure(action: Callable[[], None]) -> float:
    started = time.perf_counter_ns()
    action()
    return (time.perf_counter_ns() - started) / 1000.0


def deterministic_bytes(label: str, size: int, seed: int) -> bytes:
    digest = hashlib.sha256(f"{seed}:{label}".encode()).digest()
    return (digest * ((size + len(digest) - 1) // len(digest)))[:size]


def write_all(descriptor: int, data: bytes, *, sync_after_write: bool = False) -> int:
    view = memoryview(data)
    calls = 0
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise RuntimeError("write made no progress")
        calls += 1
        if sync_after_write:
            os.fdatasync(descriptor)
        view = view[written:]
    return calls


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def read_exact(path: Path, expected: bytes) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        offset = 0
        while offset < len(expected):
            chunk = os.read(descriptor, min(IO_CHUNK, len(expected) - offset))
            if not chunk:
                break
            if chunk != expected[offset : offset + len(chunk)]:
                raise RuntimeError(f"content mismatch: {path}")
            offset += len(chunk)
    finally:
        os.close(descriptor)
    if offset != len(expected):
        raise RuntimeError(f"short content: {path}: {offset}/{len(expected)}")


def drop_linux_page_cache() -> None:
    subprocess.run(["sync"], check=True)
    subprocess.run(
        ["sudo", "-n", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def scrape_metrics(address: str | None) -> dict[str, float]:
    if address is None:
        return {}
    opener = build_opener(ProxyHandler({}))
    with opener.open(f"http://{address}/metrics", timeout=5) as response:
        text = response.read().decode("utf-8")
    values: dict[str, float] = {}
    for line in text.splitlines():
        if not line.startswith("dms_") or " " not in line:
            continue
        metric, raw = line.rsplit(maxsplit=1)
        try:
            values[metric] = float(raw)
        except ValueError:
            continue
    return values


def metric_delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    changed: dict[str, float] = {}
    for metric in set(before) | set(after):
        delta = after.get(metric, 0.0) - before.get(metric, 0.0)
        if delta != 0.0:
            changed[metric] = delta
    return dict(sorted(changed.items()))


def measured_phase(
    name: str,
    metrics_address: str | None,
    action: Callable[[], list[float]],
) -> tuple[list[float], dict[str, Any]]:
    before = scrape_metrics(metrics_address) if metrics_address is not None else {}
    started = time.perf_counter_ns()
    samples = action()
    wall_us = (time.perf_counter_ns() - started) / 1000.0
    after = scrape_metrics(metrics_address) if metrics_address is not None else {}
    return samples, {
        "phase": name,
        "wall_us": wall_us,
        "metrics_delta": metric_delta(before, after),
    }


def run_w1(
    root: Path,
    *,
    seed: int,
    metrics_address: str | None = None,
    layout: tuple[tuple[int, int], ...] = W1_LAYOUT,
    drop_cache: Callable[[], None] = drop_linux_page_cache,
) -> dict[str, Any]:
    """Measure an uninterrupted local lifecycle, then a separate cold/path diagnostic.

    The business batch has no cache reset or HTTP scrape inside its wall timer.
    Diagnostic operation samples and counters belong to a separate invocation;
    they must never be added to or used to explain the business batch's wall time.
    Both invocations use the same input and preserve the cumulative server state.
    """
    before = scrape_metrics(metrics_address) if metrics_address is not None else {}
    business = _run_w1_pass(root, seed=seed, layout=layout, diagnostic=False)
    after = scrape_metrics(metrics_address) if metrics_address is not None else {}
    diagnostic = _run_w1_pass(
        root, seed=seed, layout=layout, diagnostic=True,
        metrics_address=metrics_address, drop_cache=drop_cache,
    )
    return {
        **diagnostic,
        "timing_contract": "dms.s5-w1-timing.v2",
        "batch_wall_us": business["batch_wall_us"],
        "diagnostic_batch_wall_us": diagnostic["batch_wall_us"],
        "business_batch": {
            **business,
            "cache_setup": "none",
            "metrics_delta": metric_delta(before, after),
            "scope": "local-owner-directory-lifecycle",
        },
        "operation_sample_semantics": "fdatasync-per-write-directory-sync-per-namespace-operation",
        "correctness": business["correctness"] and diagnostic["correctness"],
    }


def _run_w1_pass(
    root: Path,
    *,
    seed: int,
    layout: tuple[tuple[int, int], ...],
    diagnostic: bool,
    metrics_address: str | None = None,
    drop_cache: Callable[[], None] = drop_linux_page_cache,
) -> dict[str, Any]:
    directory = root / f"w1-{seed}-{'diagnostic' if diagnostic else 'business'}"
    if directory.exists():
        raise RuntimeError(f"W1 directory already exists: {directory}")
    expected: dict[Path, bytes] = {}
    operations: dict[str, list[float]] = {}
    phase_ledgers: list[dict[str, Any]] = []
    # Prepare deterministic inputs and expected results before business timing.
    inputs = []
    patch = deterministic_bytes("w1-patch", PATCH_SIZE, seed)
    for size, count in layout:
        for item in range(count):
            index = len(inputs)
            path = directory / f"skill-{index:04d}.bin"
            data = deterministic_bytes(f"w1:{size}:{item}", size, seed)
            offset = max(0, len(data) // 2 - len(patch) // 2)
            updated = data[:offset] + patch + data[offset + len(patch):]
            inputs.append((path, data, offset, updated, directory / f"renamed-{index:04d}.bin"))
    batch_started = time.perf_counter_ns()
    directory.mkdir(parents=True)
    sync_directory(directory.parent)

    def create_all() -> list[float]:
        samples: list[float] = []
        for path, data, _, _, _ in inputs:
            def create() -> None:
                descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                try:
                    write_all(descriptor, data)
                    os.fdatasync(descriptor)
                finally:
                    os.close(descriptor)
                sync_directory(directory)

            samples.append(measure(create))
            expected[path] = data
        return samples

    samples, ledger = measured_phase("create_write", metrics_address, create_all)
    operations["create_write"] = samples
    phase_ledgers.append(ledger)

    drop_cache_us = 0.0
    if diagnostic:
        drop_cache_started = time.perf_counter_ns()
        drop_cache()
        drop_cache_us = (time.perf_counter_ns() - drop_cache_started) / 1000.0

    def read_all() -> list[float]:
        return [measure(lambda path=path, data=data: read_exact(path, data)) for path, data in expected.items()]

    first_read_name = "cold_reopen_read" if diagnostic else "first_reopen_read"
    samples, ledger = measured_phase(first_read_name, metrics_address, read_all)
    operations[first_read_name] = samples
    phase_ledgers.append(ledger)
    samples, ledger = measured_phase("hot_reopen_read", metrics_address, read_all)
    operations["hot_reopen_read"] = samples
    phase_ledgers.append(ledger)

    def patch_all() -> list[float]:
        samples: list[float] = []
        for path, _, offset, _, _ in inputs:

            def patch_file() -> None:
                descriptor = os.open(path, os.O_RDWR)
                try:
                    written = os.pwrite(descriptor, patch, offset)
                    if written != len(patch):
                        raise RuntimeError(f"short patch: {path}: {written}/{len(patch)}")
                    os.fdatasync(descriptor)
                finally:
                    os.close(descriptor)

            samples.append(measure(patch_file))
        return samples

    samples, ledger = measured_phase("middle_patch", metrics_address, patch_all)
    operations["middle_patch"] = samples
    phase_ledgers.append(ledger)
    for path, _, _, updated, _ in inputs:
        read_exact(path, updated)

    renamed: dict[Path, bytes] = {}

    def rename_all() -> list[float]:
        samples: list[float] = []
        for path, _, _, data, destination in inputs:
            def rename() -> None:
                os.rename(path, destination)
                sync_directory(directory)

            samples.append(measure(rename))
            renamed[destination] = data
        return samples

    samples, ledger = measured_phase("rename", metrics_address, rename_all)
    operations["rename"] = samples
    phase_ledgers.append(ledger)
    for path, data in renamed.items():
        read_exact(path, data)

    def unlink_all() -> list[float]:
        samples = []
        for path in renamed:
            def unlink() -> None:
                os.unlink(path)
                sync_directory(directory)

            samples.append(measure(unlink))
        return samples

    samples, ledger = measured_phase("unlink", metrics_address, unlink_all)
    operations["unlink"] = samples
    phase_ledgers.append(ledger)
    if any(path.exists() for path in renamed):
        raise RuntimeError("W1 unlink left visible files")
    directory.rmdir()
    sync_directory(directory.parent)
    batch_wall_us = (time.perf_counter_ns() - batch_started) / 1000.0
    return {
        "batch_wall_us": batch_wall_us,
        "drop_cache_setup_us": drop_cache_us,
        "operations": {name: summarize_samples(values) for name, values in operations.items()},
        "phase_ledgers": phase_ledgers,
        "correctness": True,
    }


def _pattern_chunk(label: str, size: int, seed: int) -> bytes:
    return deterministic_bytes(label, min(size, IO_CHUNK), seed)


def _write_pattern(path: Path, size: int, chunk: bytes) -> int:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    try:
        remaining = size
        syncs = 0
        while remaining:
            part = chunk[: min(len(chunk), remaining)]
            syncs += write_all(descriptor, part, sync_after_write=True)
            remaining -= len(part)
        return syncs
    finally:
        os.close(descriptor)


def _write_pattern_with_namespace_sync(path: Path, size: int, chunk: bytes) -> int:
    syncs = _write_pattern(path, size, chunk)
    sync_directory(path.parent)
    return syncs


def _verify_pattern(path: Path, size: int, chunk: bytes, patch_offset: int | None, patch: bytes) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        offset = 0
        while offset < size:
            data = os.read(descriptor, min(IO_CHUNK, size - offset))
            if not data:
                break
            chunk_offset = offset % len(chunk)
            expected = bytearray(chunk[chunk_offset : chunk_offset + len(data)])
            while len(expected) < len(data):
                expected.extend(chunk[: min(len(chunk), len(data) - len(expected))])
            if patch_offset is not None:
                start = max(offset, patch_offset)
                end = min(offset + len(data), patch_offset + len(patch))
                if start < end:
                    expected[start - offset : end - offset] = patch[start - patch_offset : end - patch_offset]
            if data != expected:
                raise RuntimeError(f"content mismatch: {path} at {offset}")
            offset += len(data)
    finally:
        os.close(descriptor)
    if offset != size:
        raise RuntimeError(f"short content: {path}: {offset}/{size}")


def run_w3(
    root: Path,
    *,
    seed: int,
    metrics_address: str | None = None,
    files: tuple[tuple[str, int], ...] = W3_FILES,
    operations_per_size: int = W3_OPERATIONS_PER_SIZE,
    drop_cache: Callable[[], None] = drop_linux_page_cache,
) -> dict[str, Any]:
    """Run sequential I/O and 4 KiB patch samples for both S5 guard sizes."""
    directory = root / f"w3-{seed}"
    if directory.exists():
        raise RuntimeError(f"W3 directory already exists: {directory}")
    directory.mkdir(parents=True)
    sync_directory(directory.parent)
    operations: dict[str, dict[str, Any]] = {}
    phase_ledgers: list[dict[str, Any]] = []
    batch_started = time.perf_counter_ns()

    for label, size in files:
        paths = [directory / f"{label}-{index}.bin" for index in range(operations_per_size)]
        chunk = _pattern_chunk(f"w3:{label}", size, seed)
        patch = deterministic_bytes(f"w3:{label}:patch", PATCH_SIZE, seed)
        patch_offset = size // 2 - PATCH_SIZE // 2

        write_calls: list[int] = []

        def write_samples() -> list[float]:
            values = []
            for path in paths:
                def save() -> None:
                    write_calls.append(_write_pattern_with_namespace_sync(path, size, chunk))

                values.append(measure(save))
            return values

        samples, ledger = measured_phase(
            f"write_{label}",
            metrics_address,
            write_samples,
        )
        operations[f"write_{label}"] = {
            **summarize_samples(samples),
            "durability": {
                "file_syncs": sum(write_calls),
                "directory_syncs": len(samples),
            },
            "application_write_calls": sum(write_calls),
            "write_calls_by_sample": write_calls,
            "file_size_bytes": size,
        }
        phase_ledgers.append(ledger)
        drop_cache()

        samples, ledger = measured_phase(
            f"read_{label}",
            metrics_address,
            lambda: [
                measure(lambda path=path: _verify_pattern(path, size, chunk, None, patch)) for path in paths
            ],
        )
        operations[f"read_{label}"] = {
            **summarize_samples(samples),
            "durability": {"file_syncs": 0, "directory_syncs": 0},
        }
        phase_ledgers.append(ledger)

        def patch_all() -> list[float]:
            values: list[float] = []
            for path in paths:
                def patch_file() -> None:
                    descriptor = os.open(path, os.O_RDWR)
                    try:
                        written = os.pwrite(descriptor, patch, patch_offset)
                        if written != len(patch):
                            raise RuntimeError(f"short patch: {path}: {written}/{len(patch)}")
                        os.fdatasync(descriptor)
                    finally:
                        os.close(descriptor)

                values.append(measure(patch_file))
            return values

        samples, ledger = measured_phase(f"patch_{label}_4k", metrics_address, patch_all)
        operations[f"patch_{label}_4k"] = {
            **summarize_samples(samples),
            "durability": {"file_syncs": len(samples), "directory_syncs": 0},
        }
        phase_ledgers.append(ledger)
        for path in paths:
            _verify_pattern(path, size, chunk, patch_offset, patch)
            path.unlink()
        sync_directory(directory)

    directory.rmdir()
    sync_directory(directory.parent)
    return {
        "batch_wall_us": (time.perf_counter_ns() - batch_started) / 1000.0,
        "write_sync_semantics": "fdatasync-per-write",
        "io_chunk_bytes": IO_CHUNK,
        "operations": operations,
        "phase_ledgers": phase_ledgers,
        "correctness": True,
    }


def remove_workload_root(root: Path) -> None:
    shutil.rmtree(root, ignore_errors=True)
