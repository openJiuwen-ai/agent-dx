#!/usr/bin/env bash
# R72: list a directory of R72_ENTRIES (default 40000) entries, then a 20000-entry one while another thread
# creates and removes names in it. Pass: every entry listed once, none missing, no duplicates.
set -u
RC=0
python3 "$FAULTS/big_dir.py" "$ROOT/mnt/big" "${R72_ENTRIES:-40000}" 8 || RC=1
python3 "$FAULTS/big_dir.py" "$ROOT/mnt/bigc" "${R72_CONCURRENT_ENTRIES:-20000}" 8 --concurrent || RC=1
grep -E 'VmHWM|VmRSS' /proc/"$(cat "$ROOT/node.pid")"/status
exit $RC
