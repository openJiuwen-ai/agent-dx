#!/usr/bin/env bash
# Q08: two Nodes, two replicas, N small files (one chunk each). Kill node-b, let its session expire and the
# patrol start recording debt (1 per claim), then wipe and restart node-b BEFORE all debt is recorded: the
# restart opens a new patrol lap over chunks still waiting in the list. Time until every chunk is Satisfied.
set -u
N=${Q08_FILES:-300}; WAIT=${Q08_DOWN_SECS:-45}
health() { for id in $IDS; do curl -fsS "http://127.0.0.1:7401/v1/dfs/chunks/$id/replication" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("health"))'; done | sort | uniq -c | tr '\n' ' '; }
debt() { curl -fsS http://127.0.0.1:7401/metrics | awk '/^afs_meta_dfs_repair_placements_checked_total/{print "checked="$2}'; }
for i in $(seq 1 $N); do head -c 1048576 /dev/urandom > "$ROOT/mnt/f$i"; done; sync; sleep 3
IDS=$(find "$ROOT/node/state/dfs/chunks" -type f -size +500k -printf '%f\n' | sort -u)
[ -n "$IDS" ] || { echo "no chunk files found"; exit 1; }
echo "chunks: $(echo $IDS | wc -w) health: $(health)"
kill -KILL "$(cat "$ROOT/node-b.pid")"; t0=$(date +%s); echo "[node-b SIGKILL]"
findmnt -n "$ROOT/mnt-b" >/dev/null 2>&1 && fusermount3 -u -z "$ROOT/mnt-b"
while [ $(( $(date +%s)-t0 )) -lt $WAIT ]; do sleep 5; echo "[down t=$(( $(date +%s)-t0 ))s] $(health) $(debt)"; done
rm -rf "$ROOT/node-b/state"/*; echo "[node-b data wiped]"; cd "$ROOT"; "$BIN/afs-node" --config node-b.toml >>"$OUT/node-b.log" 2>&1 & echo $! > "$ROOT/node-b.pid"; t1=$(date +%s); echo "[node-b restarted]"
RC=1
while [ $(( $(date +%s)-t1 )) -lt ${Q08_UP_SECS:-700} ]; do sleep 10; H=$(health); echo "[up t=$(( $(date +%s)-t1 ))s] $H $(debt)"; case "$H" in *Under*|*Blocked*) ;; *) RC=0; echo "[all Satisfied at t=$(( $(date +%s)-t1 ))s after restart]"; break;; esac; done
exit $RC
