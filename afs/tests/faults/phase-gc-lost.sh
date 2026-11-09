#!/usr/bin/env bash
# R55 (S0 Q02 gen2): control c1,c2 reclaimed on both nodes; then SIGKILL node-b and delete l1, l2:
# both must reclaim on node-a. Evidence: du and file counts (independent of the code) plus Meta's GC page and metrics.
set -u
count() { printf 'node-a files=%s kib=%s | node-b files=%s kib=%s\n' \
  "$(find "$ROOT/node/state" -type f | wc -l)" "$(du -sk "$ROOT/node/state" | cut -f1)" \
  "$(find "$ROOT/node-b/state" -type f | wc -l)" "$(du -sk "$ROOT/node-b/state" | cut -f1)"; }
gc() { curl -fsS http://127.0.0.1:7401/v1/store | python3 -c 'import json,sys; print(json.load(sys.stdin).get("gc"))'; curl -fsS http://127.0.0.1:7401/metrics | grep -E '^afs_meta_dfs_gc_' | tr '\n' ' '; echo; }
write() { for i in $(seq 1 4); do dd if=/dev/urandom of="$ROOT/mnt/$1-$i" bs=1M count=8 conv=fsync status=none || echo "write $1-$i failed rc=$?"; done; }
wait_report() { for t in $(seq 1 "$2"); do sleep 5; echo "[$1 t=$((t*5))s] $(count)"; done; gc; }
echo "start $(date -u +%T) $(count)"
write c1; write c2; echo "[c1+c2 written] $(count)"
rm -f "$ROOT"/mnt/c1-*; echo "[c1 deleted $(date -u +%T)]"; wait_report c1 10
rm -f "$ROOT"/mnt/c2-*; echo "[c2 deleted $(date -u +%T)]"; wait_report c2 10
BASE=$(find "$ROOT/node/state" -type f | wc -l); echo "[baseline] node-a files=$BASE"
write l1; write l2; echo "[l1+l2 written] $(count)"
kill -KILL "$(cat "$ROOT/node-b.pid")"; echo "[node-b SIGKILL $(date -u +%T)]"
rm -f "$ROOT"/mnt/l1-*; echo "[l1 deleted $(date -u +%T)]"; wait_report l1 12
rm -f "$ROOT"/mnt/l2-*; echo "[l2 deleted $(date -u +%T)]"; wait_report l2 16
curl -fsS http://127.0.0.1:7401/metrics | grep -E '^afs_meta_dfs_gc_'
findmnt -n "$ROOT/mnt-b" >/dev/null 2>&1 && fusermount3 -u -z "$ROOT/mnt-b"; rm -f "$ROOT/node-b.pid"
END=$(find "$ROOT/node/state" -type f | wc -l)
echo "end $(date -u +%T) node-a files=$END baseline=$BASE"
# l1, l2 must be reclaimed on node-a although node-b (the other participant) is dead.
[ "$END" -le "$BASE" ]
