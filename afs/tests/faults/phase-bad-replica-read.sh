#!/usr/bin/env bash
# S4-0: is a bad replica handed to the application? Three Nodes, two replicas (node-a writer + node-b).
# node-c starts after the write so it holds no copy and must read remotely. Flip one byte in every
# node-b chunk file, then read from node-c and compare sha256 with what was written.
# Run with s3-fault3.sh, MODE=r2 COPIES_R2=2; NODE_EXTRA selects dfs_read_crc_mode.
set -u
RC=0
dd if=/dev/urandom of="$ROOT/mnt/f" bs=1M count=32 conv=fsync status=none; sync; sleep 2
W=$(sha256sum < "$ROOT/mnt/f" | cut -d' ' -f1); echo "written sha256 $W"
cd "$ROOT"; sed -e '/trusted_node_certs/!s/node-b/node-c/g' -e 's/7510/7520/g' -e 's/7511/7521/g' -e 's#mnt-b#mnt-c#g' node-b.toml > node-c.toml
"$BIN/afs-node" --config node-c.toml >"$OUT/node-c.log" 2>&1 & echo $! > "$ROOT/node-c.pid"
for i in $(seq 1 40); do findmnt -n "$ROOT/mnt-c" >/dev/null 2>&1 && break; sleep 0.5; done; sleep 3
NA=$(find "$ROOT/node/state/dfs" -type f -size +1M | wc -l); NB=$(find "$ROOT/node-b/state/dfs" -type f -size +1M | wc -l); NC=$(find "$ROOT/node-c/state/dfs" -type f -size +1M 2>/dev/null | wc -l)
echo "chunk files: node-a=$NA node-b=$NB node-c=$NC"
mapfile -t CH < <(find "$ROOT/node-b/state/dfs" -type f -size +1M)
for f in "${CH[@]}"; do python3 - "$f" <<'PY'
import sys; f=open(sys.argv[1],'r+b'); f.seek(4096); b=f.read(1); f.seek(4096); f.write(bytes([b[0]^0xff])); f.close()
PY
done
echo "flipped one byte at offset 4096 in ${#CH[@]} node-b chunk files"
metrics() { for p in 7501 7511 7521; do echo "-- :$p"; curl -fsS http://127.0.0.1:$p/metrics | grep -E '^afs_(dfs_)?(peer|read)[a-z_]*(serve|source|remote|peer|corrupt|suspect)[a-z_]*(\{.*\})? [1-9]' | head -20; done; }
readc() { # label
  echo 3 > /proc/sys/vm/drop_caches
  local rc; dd if="$ROOT/mnt-c/f" of=$ROOT/s40.read bs=1M iflag=direct status=none 2>$ROOT/s40.err; rc=$?
  local s; s=$(sha256sum < $ROOT/s40.read | cut -d' ' -f1); local n; n=$(stat -c %s $ROOT/s40.read); rm -f $ROOT/s40.read
  [ "$rc" = 0 ] || echo "  read error: $(cat $ROOT/s40.err)"
  local v=same; [ "$s" = "$W" ] || v=DIFFERENT
  echo "[$1] read rc=$rc bytes=$n sha256 $s -> $v"
  [ "$v" = DIFFERENT ] && [ "$rc" = 0 ] && BAD=$((BAD+1))
  [ "$v" = same ] && [ "$rc" = 0 ] && case $1 in both*) GOOD=$((GOOD+1));; esac
}
BAD=0; GOOD=0
for i in 1 2 3 4 5; do readc "both up #$i"; done
metrics
echo "--- stop node-a (only node-b's bad copy remains reachable)"
kill -TERM "$(cat "$ROOT/node.pid")"; for i in $(seq 1 60); do kill -0 "$(cat "$ROOT/node.pid")" 2>/dev/null || break; sleep 0.5; done
findmnt -n "$ROOT/mnt" >/dev/null 2>&1 && fusermount3 -u "$ROOT/mnt"; mv "$ROOT/node.pid" "$ROOT/node.pid.stopped"
for i in 1 2 3; do readc "node-a down #$i"; done
metrics
grep -cE 'local_corruption_reported|suspect_copy' "$OUT/node-b.log" "$OUT/node-c.log" "$OUT/node.log" 2>/dev/null
echo "BAD_DELIVERIES=$BAD"
echo "GOOD_READS_BOTH_UP=$GOOD"
RC=0; [ "$BAD" -eq 0 ] && [ "$GOOD" -eq 5 ] || RC=1
kill -TERM "$(cat "$ROOT/node-c.pid")"; for i in $(seq 1 40); do kill -0 "$(cat "$ROOT/node-c.pid")" 2>/dev/null || break; sleep 0.5; done; findmnt -n "$ROOT/mnt-c" >/dev/null 2>&1 && fusermount3 -u "$ROOT/mnt-c"
exit $RC
