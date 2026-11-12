#!/usr/bin/env bash
# R61: three Nodes, two replicas. Kill node-b: its copies stay recorded Satisfied until the session expires; the patrol must find the debt and node-c receives a new copy. Time from SIGKILL until all chunks are Satisfied.
set -u
health() { for id in $IDS; do curl -fsS "http://127.0.0.1:7401/v1/dfs/chunks/$id/replication" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("health"))'; done | sort | uniq -c | tr '\n' ' '; }
for i in 1 2 3 4; do dd if=/dev/urandom of="$ROOT/mnt/f$i" bs=1M count=8 conv=fsync status=none; done; sync; sleep 3
IDS=$(find "$ROOT/node/state/dfs/chunks" -type f -size +1M -printf '%f\n' | sort -u)
[ -n "$IDS" ] || { echo "no chunk files found"; exit 1; }
echo "chunks: $(echo $IDS | wc -w) health: $(health)"
cd "$ROOT"; sed -e '/trusted_node_certs/!s/node-b/node-c/g' -e 's/7510/7520/g' -e 's/7511/7521/g' -e 's#mnt-b#mnt-c#g' node-b.toml > node-c.toml; "$BIN/afs-node" --config node-c.toml >"$OUT/node-c.log" 2>&1 & echo $! > "$ROOT/node-c.pid"
for i in $(seq 1 40); do findmnt -n "$ROOT/mnt-c" >/dev/null 2>&1 && break; sleep 0.5; done; sleep 12
kill -KILL "$(cat "$ROOT/node-b.pid")"; t0=$(date +%s); echo "[node-b SIGKILL]"
findmnt -n "$ROOT/mnt-b" >/dev/null 2>&1 && fusermount3 -u -z "$ROOT/mnt-b"; rm -f "$ROOT/node-b.pid"
# Repair finishes within one poll of session expiry, often between samples, so success is judged by node-c holding every chunk.
RC=1
for t in $(seq 1 120); do sleep 2; n=$(find "$ROOT/node-c/state/dfs/chunks" -type f -size +1M -printf '%f\n' 2>/dev/null | sort -u | wc -l); H=$(health); echo "[t=$(( $(date +%s)-t0 ))s] node-c chunks=$n $H"; if [ "$n" = "$(echo $IDS | wc -w)" ]; then case "$H" in *Under*|*Blocked*) ;; *) RC=0; echo "[all on node-c and Satisfied at t=$(( $(date +%s)-t0 ))s after SIGKILL]"; break;; esac; fi; done
echo dump-state; for id in $IDS; do curl -fsS "http://127.0.0.1:7401/v1/dfs/chunks/$id/replication" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("chunk_id")[-8:], d.get("health"), [(c["record"]["state"],c["available"],c["record"]["location"].get("Node",{}).get("node_id")) for c in d.get("copies",[])], [(t["state"],t["attempt"],(t.get("claim") or {}).get("worker_node_id"),t.get("last_error")) for t in d.get("tasks",[])])' ; done
curl -fsS http://127.0.0.1:7401/metrics | grep -E 'ClaimReplication|repair_placements'
kill -TERM "$(cat "$ROOT/node-c.pid")"; for i in $(seq 1 40); do kill -0 "$(cat "$ROOT/node-c.pid")" 2>/dev/null || break; sleep 0.5; done; findmnt -n "$ROOT/mnt-c" >/dev/null 2>&1 && fusermount3 -u -z "$ROOT/mnt-c"
exit $RC
