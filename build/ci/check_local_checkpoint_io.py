#!/usr/bin/env python3
"""Linux syscall contract for local-only checkpoint publication.

Build the adxlet local_checkpoint test executable before running this check.
strace observes the real store, including retain and reopening sealed files.
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    trace = args.output / "syscalls.log"
    with (args.output / "test.log").open("w") as log:
        result = subprocess.run(
            [
                "strace",
                "-f",
                "-yy",
                "-o",
                str(trace),
                "-e",
                "trace=openat,fsync,fdatasync,sync,syncfs,msync,sync_file_range",
                str(args.test_binary.resolve()),
                "--exact",
                "local_only_publication_is_readable_without_forced_writeback",
                "--nocapture",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    lines = trace.read_text().splitlines() if trace.exists() else []
    observed = any("openat(" in line and "adx-local-checkpoint-io-" in line for line in lines)
    forced = []
    for line in lines:
        artifact_sync = re.search(r"\b(fsync|fdatasync|sync_file_range)\(", line) and "adx-local-checkpoint-io-" in line
        global_sync = re.search(r"\b(sync|syncfs|msync)\(", line)
        if artifact_sync or global_sync:
            forced.append(line)
    passed = result.returncode == 0 and observed and not forced
    report = {
        "passed": passed,
        "test_exit_code": result.returncode,
        "artifact_io_observed": observed,
        "forced_writeback_calls": forced,
    }
    (args.output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
