#!/usr/bin/env bash
# S4-16: the S4b latent bad replica (phase-s410-latent.sh) with the background scrub. Two Nodes, two replicas,
# default read CRC mode. Write through node-a (it holds one copy and serves every read locally), flip one byte
# in each chunk on node-b (the copy nobody reads), keep reading through node-a. With the scrub on, node-b finds
# and quarantines its copies within one round and repair restores them from node-a. Then node-a loses its disk
# (killed, data wiped, restarted): repair copies from node-b, and every chunk must be recoverable.
# Run with NODE_EXTRA setting dfs_scrub_* (short start delay and round interval). Pass: node-b copies restored,
# every read correct, and the final read after node-a lost its disk returns the written data.
set -u
exec > >(tee "$OUT/scrub.txt") 2>&1
health() { for id in $IDS; do curl -fsS "http://127.0.0.1:7401/v1/dfs/chunks/$id/replication" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("health"))'; done | sort | uniq -c | tr '\n' ' '; }
bsum() { for id in $IDS; do sha256sum "$ROOT/node-b/state/dfs/chunks/$id" 2>/dev/null | cut -c1-12; done | tr '\n' ' '; }
dd if=/dev/urandom of="$ROOT/src" bs=1M count=32 status=none; SHA=$(sha256sum "$ROOT/src" | cut -d' ' -f1)
cp "$ROOT/src" "$ROOT/mnt/f"; sync "$ROOT/mnt/f"; sleep 3
IDS=$(find "$ROOT/node-b/state/dfs/chunks" -type f -size +100k -printf '%f\n' | sort -u)
[ -n "$IDS" ] || { echo "no chunk files found"; exit 1; }
echo "chunks: $(echo $IDS | wc -w) health: $(health)"
GOOD=$(bsum)
for id in $IDS; do f="$ROOT/node-b/state/dfs/chunks/$id"; printf '\xff' | dd of="$f" bs=1 seek=4096 count=1 conv=notrunc status=none; done
echo "[node-b copies flipped] t0=$(date +%s)"; t0=$(date +%s)
for i in 1 2 3; do echo 3 > /proc/sys/vm/drop_caches; [ "$(sha256sum "$ROOT/mnt/f" | cut -d' ' -f1)" = "$SHA" ] && echo "read $i via node-a: ok" || echo "read $i via node-a: MISMATCH"; done
# Wait for one scrub round to find them (B: start delay 5 s, interval 20 s, 32 MiB/s => < 30 s).
for t in $(seq 1 ${WAIT_S:-60}); do
  found=$(grep -c 'dfs.scrub_corrupt_found' "$OUT/node-b.log" 2>/dev/null); found=${found:-0}
  [ "$found" -ge "$(echo $IDS | wc -w)" ] && break; sleep 1; done
echo "[after $(( $(date +%s) - t0 )) s] scrub corrupt_found on node-b: $(grep -c 'dfs.scrub_corrupt_found' "$OUT/node-b.log"); corruption reports: $(grep -c 'local_corruption_reported' "$OUT/node-b.log"); health: $(health)"
for t in $(seq 1 60); do [ "$(bsum)" = "$GOOD" ] && break; sleep 1; done
echo "[after $(( $(date +%s) - t0 )) s] node-b copies match the originals: $([ "$(bsum)" = "$GOOD" ] && echo yes || echo no); health: $(health)"
curl -fsS http://127.0.0.1:7511/metrics 2>/dev/null | grep '^afs_dfs_scrub_total' | tr '\n' ' '; echo
kill -KILL "$(cat "$ROOT/node.pid")"; fusermount3 -u -z "$ROOT/mnt" 2>/dev/null
rm -rf "$ROOT/node/state"/*; echo "[node-a disk lost]"; sleep 40
cd "$ROOT"; "$BIN/afs-node" --config node.toml >>"$OUT/node.log" 2>&1 & echo $! > "$ROOT/node.pid"
for _ in $(seq 1 240); do findmnt -n "$ROOT/mnt" >/dev/null 2>&1 && break; sleep 0.25; done
echo "[node-a restarted empty]"
for t in 10 30 60 90; do sleep $(( t - ${prev:-0} )); prev=$t; echo "[t=${t}s after restart] health: $(health)"; done
echo 3 > /proc/sys/vm/drop_caches
if out=$(sha256sum "$ROOT/mnt/f" 2>&1); then [ "${out%% *}" = "$SHA" ] && echo "final read: ok" || echo "final read: WRONG DATA"; else echo "final read: error ($out)"; fi
echo "recoverable: $(health)"
grep -hoE '"msg":"dfs\.[a-z_]*(corrupt|source|scrub)[a-z_]*"' "$OUT/node.log" "$OUT/node-b.log" | sort | uniq -c
sleep 1
grep -q "node-b copies match the originals: yes" "$OUT/scrub.txt" && grep -q "final read: ok" "$OUT/scrub.txt" && ! grep -q MISMATCH "$OUT/scrub.txt"
