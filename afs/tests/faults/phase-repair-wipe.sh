#!/usr/bin/env bash
# R61: two Nodes, two replicas. Kill node-b, let its session expire, wipe its data and restart it: every chunk lacks a copy until repair re-replicates; time until all are Satisfied.
set -u
health() { for id in $IDS; do curl -fsS "http://127.0.0.1:7401/v1/dfs/chunks/$id/replication" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("health"))'; done | sort | uniq -c | tr '\n' ' '; }
for i in 1 2 3 4; do dd if=/dev/urandom of="$ROOT/mnt/f$i" bs=1M count=8 conv=fsync status=none; done; sync; sleep 3
IDS=$(find "$ROOT/node/state/dfs/chunks" -type f -size +1M -printf '%f\n' | sort -u)
[ -n "$IDS" ] || { echo "no chunk files found"; exit 1; }
echo "chunks: $(echo $IDS | wc -w) health: $(health)"
kill -KILL "$(cat "$ROOT/node-b.pid")"; t0=$(date +%s); echo "[node-b SIGKILL]"
findmnt -n "$ROOT/mnt-b" >/dev/null 2>&1 && fusermount3 -u -z "$ROOT/mnt-b"
for t in $(seq 1 12); do sleep 5; echo "[down t=$(( $(date +%s)-t0 ))s] $(health)"; done
rm -rf "$ROOT/node-b/state"/*; echo "[node-b data wiped: its copies are gone]"; cd "$ROOT"; "$BIN/afs-node" --config node-b.toml >>"$OUT/node-b.log" 2>&1 & echo $! > "$ROOT/node-b.pid"; t1=$(date +%s); echo "[node-b restarted]"
RC=1
for t in $(seq 1 150); do sleep 2; H=$(health); echo "[up t=$(( $(date +%s)-t1 ))s] $H"; case "$H" in *Under*|*Blocked*) ;; *) RC=0; echo "[all Satisfied at t=$(( $(date +%s)-t1 ))s after restart]"; break;; esac; done
echo dump-state; for id in $IDS; do curl -fsS "http://127.0.0.1:7401/v1/dfs/chunks/$id/replication" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("chunk_id")[-8:], d.get("health"), [(c["record"]["state"],c["available"],c["record"]["location"].get("Node",{}).get("node_id")) for c in d.get("copies",[])], [(t["state"],t["attempt"],(t.get("claim") or {}).get("worker_node_id"),t.get("last_error")) for t in d.get("tasks",[])])' ; done
curl -fsS http://127.0.0.1:7401/metrics | grep -E 'ClaimReplication|repair_placements' 
exit $RC
