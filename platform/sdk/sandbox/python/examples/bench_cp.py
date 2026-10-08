"""Benchmark: copy_from_local vs fs_write

Compares upload performance between the two file transfer methods.

Prerequisites:
  - ADX_SERVER_ADDRESS and ADX_TOKEN environment variables must be set.

Usage:
  export ADX_SERVER_ADDRESS=your-server.example.com
  export ADX_TOKEN=your-token
  python bench_cp.py
"""

import os
import tempfile
import time
import sys
from adx_sandbox import Sandbox

# Test file sizes in bytes
FILE_SIZES = [
    (1 * 1024, "1KB"),
    (64 * 1024, "64KB"),
    (256 * 1024, "256KB"),
    (512 * 1024, "512KB"),
    (1 * 1024 * 1024, "1MB"),
    (4 * 1024 * 1024, "4MB"),
    (16 * 1024 * 1024, "16MB"),
    (32 * 1024 * 1024, "32MB"),
]

ITERATIONS = 5


def generate_file(path: str, size: int) -> None:
    """Generate a file filled with pseudo-random printable bytes."""
    import random
    random.seed(42)
    chunk = bytes(random.randint(32, 126) for _ in range(min(size, 64 * 1024)))
    with open(path, "wb") as f:
        written = 0
        while written < size:
            f.write(chunk[: size - written])
            written += len(chunk)


def bench_copy_from_local(sb, local_path: str, remote_path: str) -> float:
    start = time.perf_counter()
    sb.files.copy_from_local(local_path, remote_path)
    return time.perf_counter() - start


def bench_fs_write(sb, local_path: str, remote_path: str) -> float:
    with open(local_path, "rb") as f:
        data = f.read()
    start = time.perf_counter()
    sb.files.write(remote_path, data)
    return time.perf_counter() - start


def format_time(seconds: float) -> str:
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.1f}us"
    if seconds < 1:
        return f"{seconds * 1_000:.1f}ms"
    return f"{seconds:.3f}s"


def main():
    with Sandbox(cpu=2000, memory=4096) as sb:
        print(f"Sandbox: {sb.id}")
        print(f"{'Size':<8} {'Method':<22} {'Iter':>5} {'Min':>10} {'Avg':>10} {'Max':>10}")
        print("-" * 73)

        for size, label in FILE_SIZES:
            with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
                local_path = f.name
            generate_file(local_path, size)

            remote_base = f"/tmp/bench_{label}"

            methods = [
                ("copy_from_local (tar)", bench_copy_from_local),
                ("fs_write (auto)", bench_fs_write),
            ]

            for method_name, bench_fn in methods:
                times = []
                for i in range(ITERATIONS):
                    remote_path = f"{remote_base}_{i}.bin"
                    try:
                        elapsed = bench_fn(sb, local_path, remote_path)
                        times.append(elapsed)
                    except Exception as exc:
                        print(f"  {method_name} iter {i} ERROR: {exc}")
                        times = []
                        break

                if times:
                    avg = sum(times) / len(times)
                    print(
                        f"{label:<8} {method_name:<22} {len(times):>5} "
                        f"{format_time(min(times)):>10} {format_time(avg):>10} {format_time(max(times)):>10}"
                    )

                # Cleanup remote files
                for i in range(ITERATIONS):
                    remote_path = f"{remote_base}_{i}.bin"
                    try:
                        sb.files.remove(remote_path)
                    except Exception:
                        pass

            os.remove(local_path)

    print("\nDone.")


if __name__ == "__main__":
    main()
