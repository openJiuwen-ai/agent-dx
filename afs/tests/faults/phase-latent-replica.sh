#!/usr/bin/env bash
# S4-10: a latent bad replica. Two Nodes, two replicas, default read CRC mode. Write through node-a (it holds
# one copy and serves every read locally), flip one byte in each chunk on node-b (the copy nobody reads), keep
# reading through node-a: nothing notices. Then node-a loses its disk (killed, data wiped, restarted): repair
# can only copy from node-b, whose copy is bad. Pass: no read ever returns wrong data (an error is allowed:
# without the scrub the bad copies are only found when node-a is gone).
set -u
exec > >(tee "$OUT/latent.txt") 2>&1
health() { for id in $IDS; do curl -fsS "http://127.0.0.1:7401/v1/dfs/chunks/$id/replication" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("health"))'; done | sort | uniq -c | tr '\n' ' '; }
dd if=/dev/urandom of="$ROOT/src" bs=1M count=32 status=none; SHA=$(sha256sum "$ROOT/src" | cut -d' ' -f1)
cp "$ROOT/src" "$ROOT/mnt/f"; sync "$ROOT/mnt/f"; sleep 3
IDS=$(find "$ROOT/node-b/state/dfs/chunks" -type f -size +100k -printf '%f\n' | sort -u)
[ -n "$IDS" ] || { echo "no chunk files found"; exit 1; }
echo "chunks: $(echo $IDS | wc -w) health: $(health)"
for id in $IDS; do f="$ROOT/node-b/state/dfs/chunks/$id"; printf '\xff' | dd of="$f" bs=1 seek=4096 count=1 conv=notrunc status=none; done
echo "[node-b copies flipped]"
for i in 1 2 3; do echo 3 > /proc/sys/vm/drop_caches; [ "$(sha256sum "$ROOT/mnt/f" | cut -d' ' -f1)" = "$SHA" ] && echo "read $i via node-a: ok" || echo "read $i via node-a: MISMATCH"; done
sleep 30; echo "[after 30 s, nobody read node-b] health: $(health); corruption reports: $(grep -c 'local_corruption_reported' "$OUT/node-b.log")"
kill -KILL "$(cat "$ROOT/node.pid")"; fusermount3 -u -z "$ROOT/mnt" 2>/dev/null; t0=$(date +%s)
rm -rf "$ROOT/node/state"/*; echo "[node-a disk lost]"; sleep 40
cd "$ROOT"; "$BIN/afs-node" --config node.toml >>"$OUT/node.log" 2>&1 & echo $! > "$ROOT/node.pid"
for _ in $(seq 1 240); do findmnt -n "$ROOT/mnt" >/dev/null 2>&1 && break; sleep 0.25; done
echo "[node-a restarted empty]"
for t in 10 30 60 90; do sleep $(( t - ${prev:-0} )); prev=$t; echo "[t=${t}s after restart] health: $(health)"; done
echo 3 > /proc/sys/vm/drop_caches
if out=$(sha256sum "$ROOT/mnt/f" 2>&1); then [ "${out%% *}" = "$SHA" ] && echo "final read: ok" || echo "final read: WRONG DATA"; else echo "final read: error ($out)"; fi
grep -hoE '"msg":"dfs\.[a-z_]*(corrupt|source)[a-z_]*"' "$OUT/node.log" "$OUT/node-b.log" | sort | uniq -c
sleep 1; ! grep -qE "MISMATCH|WRONG DATA" "$OUT/latent.txt"
