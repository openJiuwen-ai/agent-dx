#!/usr/bin/env bash
# R05 on MinIO: delete, overwrite and a crash after uploads; bucket object count and bytes (MinIO's own files, not product
# counters) must end at exactly the kept file's objects, and the kept file stays intact.
set -u
BK="$ROOT/minio/afs-chunks"
objs() { printf 'objects=%s kib=%s meta_store=%s\n' "$(find "$BK" -name xl.meta 2>/dev/null | wc -l)" "$(du -sk "$BK" | cut -f1)" "$(curl -fsS http://127.0.0.1:7401/v1/store | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("gc"))')"; }
wait_until() { # label expected limit_secs
  local t=0; while [ "$t" -lt "$3" ]; do sleep 5; t=$((t+5)); n=$(find "$BK" -name xl.meta 2>/dev/null | wc -l); echo "[$1 t=${t}s] $(objs)"; [ "$n" -eq "$2" ] && { echo "[$1] reached $2 objects at t=${t}s"; return 0; }; done; echo "[$1] NOT reached: expected $2, $(objs)"; return 1; }
RC=0
dd if=/dev/urandom of="$ROOT/mnt/keep" bs=1M count=8 conv=fsync status=none; KEEP=$(sha256sum "$ROOT/mnt/keep" | cut -d' ' -f1)
sync; sleep 6; KEPT=$(find "$BK" -name xl.meta | wc -l); echo "[keep written, sha $KEEP] kept objects=$KEPT $(objs)"
# Delete.
for i in 1 2 3; do dd if=/dev/urandom of="$ROOT/mnt/del-$i" bs=1M count=16 conv=fsync status=none; done; echo "[del-* written] $(objs)"
rm -f "$ROOT"/mnt/del-*; echo "[del-* deleted $(date -u +%T)]"; wait_until delete "$KEPT" 180 || RC=1
# Overwrite: the same file rewritten twice; only the last version may stay.
for round in 1 2 3; do dd if=/dev/urandom of="$ROOT/mnt/over" bs=1M count=16 conv=fsync,notrunc status=none; done; echo "[over rewritten $(date -u +%T)] $(objs)"
OVER=$(sha256sum "$ROOT/mnt/over" | cut -d' ' -f1)
rm -f "$ROOT"/mnt/over; wait_until overwrite "$KEPT" 180 || RC=1
# Same content deleted while being rewritten: must not lose the new file.
dd if=/dev/urandom of="$ROOT/same.src" bs=1M count=8 status=none; SAME=$(sha256sum "$ROOT/same.src" | cut -d' ' -f1)
cp "$ROOT/same.src" "$ROOT/mnt/same-old"; sync "$ROOT/mnt/same-old"
cp "$ROOT/same.src" "$ROOT/mnt/same-new"; rm -f "$ROOT/mnt/same-old"; sync "$ROOT/mnt/same-new"; sleep 45
[ "$(sha256sum "$ROOT/mnt/same-new" | cut -d' ' -f1)" = "$SAME" ] && echo "[same content] new file intact" || { echo "[same content] LOST DATA"; RC=1; }
rm -f "$ROOT/mnt/same-new"; wait_until same-content "$KEPT" 180 || RC=1
# Crash after uploads: never committed, Meta never heard of them; listing has to find them.
python3 - "$ROOT/mnt/crash" <<'PY' &
import os,sys,time
f=os.open(sys.argv[1],os.O_CREAT|os.O_WRONLY,0o644); b=os.urandom(4*1024*1024)
for _ in range(8): os.write(f,b+b'')
time.sleep(600)
PY
WP=$!; sleep 8; echo "[crash writer uploaded] $(objs)"
kill -KILL "$(cat "$ROOT/node.pid")"; kill -KILL "$WP" 2>/dev/null; echo "[node-a SIGKILL $(date -u +%T)]"
fusermount3 -u -z "$ROOT/mnt" 2>/dev/null; sleep 2
"$BIN/afs-node" --config "$ROOT/node.toml" >>"$OUT/node.log" 2>&1 &
echo $! > "$ROOT/node.pid"
for _ in $(seq 1 240); do findmnt -n "$ROOT/mnt" >/dev/null 2>&1 && break; sleep 0.25; done
echo "[node-a restarted $(date -u +%T)] $(objs)"
rm -f "$ROOT/mnt/crash"; wait_until crash "$KEPT" 300 || RC=1
[ "$(sha256sum "$ROOT/mnt/keep" | cut -d' ' -f1)" = "$KEEP" ] && echo "[keep] intact" || { echo "[keep] CORRUPT"; RC=1; }
echo "final $(objs)"; curl -fsS http://127.0.0.1:7401/metrics | grep -E '^afs_meta_dfs_gc_'
exit $RC
