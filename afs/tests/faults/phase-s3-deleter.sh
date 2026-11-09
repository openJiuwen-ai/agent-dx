#!/usr/bin/env bash
# S4-9 (S2 leftover a): S3 mode, node-a and node-b; node-a is the only GC deleter (first live participant).
# Write N small files through node-b, delete them, and once node-a's generation is deleting, either SIGKILL node-a
# (V=lost) or freeze its S3 connection while it stays registered (V=stuck, via freeze-proxy.py). Measure until the
# bucket holds only the kept file's objects again (MinIO xl.meta count and du, not product counters).
set -u
V=${S49_VARIANT:-lost}; N=${S49_FILES:-3000}; LIMIT=${S49_LIMIT:-900}; FREEZE=$ROOT/freeze
BK="$ROOT/minio/afs-chunks"
objs() { find "$BK" -name xl.meta 2>/dev/null | wc -l; }
gcj() { curl -fsS http://127.0.0.1:7401/v1/store | python3 -c 'import json,sys; g=json.load(sys.stdin).get("gc",{}); print(g.get("generation"), g.get("pending_chunks"), ",".join(g.get("waiting_nodes",[])), g.get("generation_age_seconds"))'; }
stalled() { curl -fsS http://127.0.0.1:7401/metrics | awk '/^afs_meta_dfs_gc_stalled_seconds /{print $2}'; }
dd if=/dev/urandom of="$ROOT/mnt-b/keep" bs=1M count=8 conv=fsync status=none; sleep 6; KEPT=$(objs)
echo "[keep] objects=$KEPT gc=$(gcj)"
SZ=${S49_BYTES:-65536}; for i in $(seq 1 $N); do head -c $SZ /dev/urandom > "$ROOT/mnt-b/d-$i"; done; sync; sleep 6
TOTAL=$(objs); echo "[written $N] objects=$TOTAL kib=$(du -sk "$BK" | cut -f1) gc=$(gcj)"
rm -f "$ROOT"/mnt-b/d-*; t0=$(date +%s); echo "[deleted at $t0]"
for _ in $(seq 1 600); do
  read -r gen pend wait age <<<"$(gcj)"; n=$(objs)
  if [ "${pend:-0}" -gt 0 ] && [[ ",$wait," == *",node-a,"* ]] && [ "$n" -lt "$TOTAL" ]; then break; fi
  sleep 0.2
done
echo "[node-a deleting: objects=$(objs) gc=$(gcj)]"
case $V in
  lost) kill -KILL "$(cat "$ROOT/node.pid")"; fusermount3 -u -z "$ROOT/mnt" 2>/dev/null ;;
  stuck) touch $FREEZE ;;
esac
tk=$(date +%s); echo "[$V at t=$((tk-t0))s] objects=$(objs)"
RC=1
while [ $(( $(date +%s)-tk )) -lt $LIMIT ]; do
  sleep 10; n=$(objs); echo "[t=$(( $(date +%s)-tk ))s] objects=$n kib=$(du -sk "$BK" | cut -f1) gc=$(gcj) stalled=$(stalled)"
  [ "$n" -le "$KEPT" ] && { RC=0; echo "[reclaimed to $n objects at t=$(( $(date +%s)-tk ))s after $V]"; break; }
done
if [ "$V" = stuck ]; then rm -f $FREEZE; echo "[unfrozen]"; for _ in $(seq 1 60); do sleep 10; n=$(objs); echo "[after unfreeze] objects=$n gc=$(gcj)"; [ "$n" -le "$KEPT" ] && { echo "[reclaimed after unfreeze]"; break; }; done; fi
[ "$(objs)" -ge "$KEPT" ] || echo "[ERROR] fewer objects than the kept file needs"
cmp -s <(cat "$ROOT/mnt-b/keep") "$ROOT/mnt-b/keep" && echo "[keep readable]"
exit $RC
