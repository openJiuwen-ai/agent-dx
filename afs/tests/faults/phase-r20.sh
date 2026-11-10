#!/usr/bin/env bash
# R20: RENAME_NOREPLACE after an unsupported rename flag, a pre-1970 timestamp, and O_APPEND across two Nodes
# (r20_posix.py). Pass: NOREPLACE keeps working and the append offsets agree on both Nodes.
set -u
out=$(python3 "$FAULTS/r20_posix.py" "$ROOT/mnt" "$ROOT/mnt-b" 2>&1); rc=$?; echo "$out"
[ $rc -eq 0 ] && grep -q '^R20-1 NOREPLACE still works' <<<"$out" && grep -q '^R20-3 consistent' <<<"$out"
