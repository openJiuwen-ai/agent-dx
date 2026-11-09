#!/usr/bin/env bash
# R75: a 128 MiB file, then 4 threads each reading through one O_DIRECT handle for R75_SECS (default 120 s),
# well past the 5 s read-view reuse window. Pass: no read error.
set -u
dd if=/dev/urandom of="$ROOT/mnt/big" bs=1M count=128 conv=fsync status=none
python3 "$FAULTS/long_read.py" "$ROOT/mnt/big" 4 "${R75_SECS:-120}"
