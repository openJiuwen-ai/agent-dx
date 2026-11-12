#!/usr/bin/env bash
# R18: an SDK client (afs-bench --arm sdk: path open, iov, register, ring) is SIGKILLed mid-write.
# Independent counts on node-a before and after: memfd mappings in /proc/<node>/maps, afs-usrbio-ring-* threads,
# VmRSS; plus the Node's reclaim counters. Then a new SDK client runs normally on the same file.
set -u
B=${BIN:?}; NP=$(cat "$ROOT/node.pid")
snap() { echo "memfd_maps=$(grep -c memfd /proc/$NP/maps) ring_threads=$(cat /proc/$NP/task/*/comm 2>/dev/null | grep -c '^afs-usrbio-ring') rss_kib=$(awk '/VmRSS/{print $2}' /proc/$NP/status) $(curl -fsS 127.0.0.1:7501/metrics | awk '/^afs_usrbio_ring_io_total\{event="reclaimed_/{gsub(/afs_usrbio_ring_io_total\{event="/,""); gsub(/"\}/,"="); printf "%s%s ", $1, $2}')"; }
echo "[before] $(snap)"
"$B/afs-bench" "$ROOT/mnt" --sock "$ROOT/node/run/node.sock" --arm sdk --mode write --files 4 --file-bytes 256M --block 1M --threads 4 --rounds 50 --settle-secs 0 > "$OUT/sdk1.txt" 2>&1 &
C=$!; for _ in $(seq 1 200); do [ "$(cat /proc/$NP/task/*/comm 2>/dev/null | grep -c '^afs-usrbio-ring')" -gt 0 ] && break; sleep 0.1; done; sleep 2; echo "[client running pid=$C] $(snap)"
# The crash must hit a live SDK client: without rings there is nothing to reclaim and the check below is empty.
[ "$(cat /proc/$NP/task/*/comm 2>/dev/null | grep -c '^afs-usrbio-ring')" -gt 0 ] || { echo "[client never registered a ring]"; tail -3 "$OUT/sdk1.txt"; kill -KILL $C 2>/dev/null; exit 1; }
kill -KILL $C; wait $C 2>/dev/null; tk=$(date +%s%N); echo "[client SIGKILL]"
RC=1
for i in $(seq 1 100); do s=$(snap); case "$s" in memfd_maps=0\ ring_threads=0*) RC=0; echo "[released after $(( ($(date +%s%N)-tk)/1000000 )) ms] $s"; break;; esac; sleep 0.1; done
[ $RC -eq 0 ] || echo "[NOT released after 10 s] $(snap)"
sleep 1; echo "[settled] $(snap)"
"$B/afs-bench" "$ROOT/mnt" --sock "$ROOT/node/run/node.sock" --arm sdk --mode write --files 4 --file-bytes 64M --block 1M --threads 4 --rounds 1 --settle-secs 0 > "$OUT/sdk2.txt" 2>&1
SECOND=$?; echo "[second client rc=$SECOND] $(tail -1 "$OUT/sdk2.txt" | cut -c1-120)"; [ $SECOND -eq 0 ] || RC=1; echo "[after second client] $(snap)"
grep -c 'usrbio.client_reclaimed' "$OUT/node.log" | sed 's/^/reclaim log lines: /'
exit $RC
