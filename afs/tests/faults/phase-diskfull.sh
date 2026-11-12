#!/usr/bin/env bash
# R71a: a real full disk still reports ENOSPC. ROOT lives on a small tmpfs; write past it.
set -u
dd if=/dev/zero of="$ROOT/mnt/fill" bs=1M count=400 conv=fsync 2>&1 | tail -2
RC=${PIPESTATUS[0]}; echo "dd rc=$RC"
grep -o '"errno":[0-9]*' "$OUT/node.log" | sort | uniq -c
# Pass: the full disk reaches the writer as ENOSPC (errno 28), not as success or another error.
[ "$RC" -ne 0 ] && grep -q '"errno":28' "$OUT/node.log"
