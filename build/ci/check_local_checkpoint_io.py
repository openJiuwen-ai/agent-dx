#!/usr/bin/env python3
"""Linux syscall contract for local-only checkpoint publication.

Build the adxlet local_checkpoint test executable before running this check.
strace observes the real store, including retain and reusable-snapshot copy.
"""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


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
                "strace", "-f", "-yy", "-o", str(trace),
                "-e", "trace=openat,fsync,fdatasync,sync,syncfs,msync,sync_file_range",
                str(args.test_binary.resolve()),
                "--exact", "local_only_publication_is_readable_without_forced_writeback",
                "--nocapture",
            ],
            stdout=log, stderr=subprocess.STDOUT, check=False,
        )
    lines = trace.read_text().splitlines() if trace.exists() else []
    observed = any("openat(" in line and "adx-local-checkpoint-io-" in line for line in lines)
    forced = [
        line for line in lines
        if (re.search(r"\b(fsync|fdatasync|sync_file_range)\(", line)
            and "adx-local-checkpoint-io-" in line)
        or re.search(r"\b(sync|syncfs|msync)\(", line)
    ]
    passed = result.returncode == 0 and observed and not forced
    report = {"passed": passed, "test_exit_code": result.returncode,
              "artifact_io_observed": observed, "forced_writeback_calls": forced}
    (args.output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
