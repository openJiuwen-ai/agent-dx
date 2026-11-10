#!/usr/bin/env bash
# R12: two Nodes, two synchronous replicas. A writer on node-a fsyncs one 4 MiB file every ~1.5 s; node-b is
# SIGKILLed 6 s in, so writes span the dead peer (refused connections) and its session expiry (~30 s).
# Then node-b restarts on its data: every chunk must return to Satisfied, and all files read back intact.
# Meta policy comes from META_EXTRA (default degrade). R12_FILES files. R12_FAULT=stop: SIGSTOP/SIGCONT instead of kill/restart.
set -u
N=${R12_FILES:-40}
health() { for id in $(find "$ROOT/node/state/dfs/chunks" -type f -size +100k -printf '%f\n' | sort -u); do curl -fsS "http://127.0.0.1:7401/v1/dfs/chunks/$id/replication" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("health"))'; done | sort | uniq -c | tr '\n' ' '; }
S=$ROOT/src-files; rm -rf $S; mkdir -p $S
for i in $(seq 1 $N); do head -c 4194304 /dev/urandom > $S/s$i; done
( for i in $(seq 1 $N); do t=$(date +%s%N); dd if=$S/s$i of="$ROOT/mnt/w$i" bs=1M conv=fsync status=none; rc=$?; echo "w$i rc=$rc ms=$(( ($(date +%s%N)-t)/1000000 )) at=$(date +%s)"; sleep 1.5; done ) > "$OUT/writer.txt" 2>&1 &
W=$!
FAULT=${R12_FAULT:-kill}
if [ "$FAULT" = stop ]; then
  # A hung peer: connections stay open, nothing answers, its session expires (~30 s).
  sleep 6; kill -STOP "$(cat "$ROOT/node-b.pid")"; t0=$(date +%s); echo "[node-b SIGSTOP at $t0]"
else
  sleep 6; kill -KILL "$(cat "$ROOT/node-b.pid")"; t0=$(date +%s); echo "[node-b SIGKILL at $t0]"
  findmnt -n "$ROOT/mnt-b" >/dev/null 2>&1 && fusermount3 -u -z "$ROOT/mnt-b"
fi
for _ in $(seq 1 ${R12_WRITER_LIMIT:-180}); do kill -0 $W 2>/dev/null || break; sleep 1; done
if kill -0 $W 2>/dev/null; then HUNG=1; echo "[writer still running after ${R12_WRITER_LIMIT:-180} s: $(ps -o etime=,wchan:24=,args= -p $(pgrep -x dd | head -1) 2>/dev/null)]"; fi
echo "writer: $(grep -c 'rc=0' "$OUT/writer.txt") ok, $(grep -vc 'rc=0' "$OUT/writer.txt") failed of $N; max ms $(sed 's/.*ms=\([0-9]*\).*/\1/' "$OUT/writer.txt" | sort -n | tail -1)"
awk -v t0=$t0 '{split($4,a,"="); split($3,b,"="); if (a[2]>=t0) {n++; if (b[2]>m) m=b[2]}} END {print "after kill: " n " writes, max ms " m}' "$OUT/writer.txt"
grep -v 'rc=0' "$OUT/writer.txt" | head -3
echo "[health with node-b down] $(health)"
if [ "$FAULT" = stop ]; then
  # Its session expired while it hung, so Meta fenced it: recovery is a restart on the same data.
  kill -KILL "$(cat "$ROOT/node-b.pid")"; findmnt -n "$ROOT/mnt-b" >/dev/null 2>&1 && fusermount3 -u -z "$ROOT/mnt-b"; sleep 1
  cd "$ROOT"; "$BIN/afs-node" --config node-b.toml >>"$OUT/node-b.log" 2>&1 & echo $! > "$ROOT/node-b.pid"; t1=$(date +%s); echo "[hung node-b restarted on its data]"
else
  cd "$ROOT"; "$BIN/afs-node" --config node-b.toml >>"$OUT/node-b.log" 2>&1 & echo $! > "$ROOT/node-b.pid"; t1=$(date +%s); echo "[node-b restarted on its data]"
fi
RC=1
while [ $(( $(date +%s)-t1 )) -lt ${R12_UP_SECS:-300} ]; do sleep 5; H=$(health); echo "[up t=$(( $(date +%s)-t1 ))s] $H"; case "$H" in *Under*|*Blocked*) ;; *) RC=0; echo "[all Satisfied at t=$(( $(date +%s)-t1 ))s after restart]"; break;; esac; done
if [ -n "${HUNG:-}" ]; then for _ in $(seq 1 120); do kill -0 $W 2>/dev/null || break; sleep 1; done; kill -0 $W 2>/dev/null && echo "[writer still hung after node-b returned]" || echo "[writer finished after node-b returned]"; grep -vc 'rc=0' "$OUT/writer.txt" | sed 's/^/failed writes: /'; fi
echo "node-a log lines: $(wc -l < "$OUT/node.log"), background_error_recorded: $(grep -c background_error_recorded "$OUT/node.log")"
bad=0; ok=0; for i in $(seq 1 $N); do [ -e "$ROOT/mnt/w$i" ] || continue; if cmp -s "$S/s$i" "$ROOT/mnt/w$i"; then ok=$((ok+1)); else bad=$((bad+1)); fi; done
echo "readback via node-a: $ok match, $bad differ"
# Both replicas on disk: each chunk named in node-a's store also exists in node-b's.
miss=0; for id in $(find "$ROOT/node/state/dfs/chunks" -type f -size +100k -printf '%f\n'); do [ -e "$ROOT/node-b/state/dfs/chunks/$id" ] || miss=$((miss+1)); done
echo "chunks missing on node-b after repair: $miss"
grep -c 'dfs.replica_write_degraded' "$OUT/node.log" | sed 's/^/degraded-write log lines: /'
[ $bad -eq 0 ] && [ $ok -eq $N ] || RC=1
rm -rf $S
exit $RC
