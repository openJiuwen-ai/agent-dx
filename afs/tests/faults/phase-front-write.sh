#!/usr/bin/env bash
# R76: many 4 KiB FUSE writes from parallel threads into separate files (native). The front-write admission
# must wait, never fail a write with EAGAIN. Shapes FILES:THREADS (default 2:2 and 4:4), one 16 MiB file each.
set -u
RC=0
for shape in ${R76_SHAPES:-2:2 4:4}; do f=${shape%%:*}; t=${shape##*:}
  "$BIN/afs-bench" "$ROOT/mnt" --sock "$ROOT/node/run/node.sock" --json "$OUT/r76-f$f-t$t.json" --arm fuse --mode write \
    --files "$f" --file-bytes 16M --block 4K --threads "$t" --rounds 1 --settle-secs 3 > "$OUT/r76-f$f-t$t.txt" 2>&1
  rc=$?; echo "files=$f threads=$t bench rc=$rc"; tail -2 "$OUT/r76-f$f-t$t.txt"; [ $rc -eq 0 ] || RC=1
  rm -rf "$ROOT"/mnt/afs-bench* 2>/dev/null
done
curl -fsS http://127.0.0.1:7501/metrics | grep -E '^afs_fuse_front_write_admissions_total'
exit $RC
