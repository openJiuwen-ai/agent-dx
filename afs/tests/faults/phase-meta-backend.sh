#!/usr/bin/env bash
# R16: Meta on a shared-service log (META_BACKEND=redis|etcd, see s2-fault.sh). One Node, native.
#  1. kill -9 Meta, restart: recovery from the backend, content identical, writes continue.
#  2. a second Meta process opens the same backend/prefix: the first is fenced (META_LOG_FENCED), nothing lost.
#  3. kill -9 the backend under fsync'd writes, restart backend and Meta: every acknowledged (fsync'd) file intact.
set -u
exec > >(tee "$OUT/backend.txt") 2>&1
F=$ROOT/mnt; cd "$ROOT"
sums() { (cd $F/d && find . -type f | sort | xargs -r sha256sum) | sha256sum | cut -c1-16; }
meta_up() { "$BIN/afs-meta" --config "$1" >>"$OUT/$2.log" 2>&1 & echo $! > "$ROOT/$2.pid"; for _ in $(seq 1 120); do curl -fsS "http://127.0.0.1:$3/health" >/dev/null 2>&1 && return 0; sleep 0.25; done; echo "$2 not healthy"; return 1; }
node_up() { kill -0 "$(cat node.pid)" 2>/dev/null && return 0; fusermount3 -u -z "$F" 2>/dev/null; "$BIN/afs-node" --config node.toml >>"$OUT/node.log" 2>&1 & echo $! > node.pid; for _ in $(seq 1 240); do findmnt -n "$F" >/dev/null 2>&1 && { echo "    (node restarted)"; return 0; }; sleep 0.25; done; echo "node not mounted"; return 1; }
wait_ok() { for _ in $(seq 1 ${2:-120}); do timeout 10 bash -c "$1" >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }
mkdir -p $F/d/sub; for i in $(seq 1 200); do head -c $(( (i * 7919) % 300000 + 1 )) /dev/urandom > $F/d/f$i; done
for i in $(seq 1 50); do mv $F/d/f$i $F/d/sub/m$i; done; sync
S1=$(sums); echo "[1] wrote 200 files, 50 renamed: $S1"
kill -KILL "$(cat meta.pid)"; sleep 1; t0=$(date +%s.%N)
meta_up meta.toml meta 7401
echo "[1] Meta killed and restarted from the backend: healthy after $(python3 -c "import time;print(round(time.time()-$t0,2))") s"
echo 3 > /proc/sys/vm/drop_caches
wait_ok "touch $F/d/after-restart" 120 && echo "[1] write after restart: ok" || echo "[1] write after restart: FAILED"
rm -f $F/d/after-restart; S2=$(sums); [ "$S1" = "$S2" ] && echo "[1] content identical: yes" || echo "[1] content identical: NO ($S2)"
for i in $(seq 1 50); do head -c 4096 /dev/urandom > $F/d/g$i; done; sync; S3=$(sums)
# 2. second Meta on the same backend and prefix
sed -e 's/7400/7402/; s/7401/7403/' -e "s#$ROOT/meta/state#$ROOT/meta2/state#" meta.toml > meta2.toml; mkdir -p meta2/state
meta_up meta2.toml meta2 7403 && echo "[2] second Meta opened the same log"
r=$(timeout 20 touch $F/d/fence-probe 2>&1; echo "rc=$?"); echo "[2] write through the first Meta: $r" | tr '\n' ' '; echo
echo "[2] fenced replies seen by the Node: $(grep -c 'writer fenced' "$OUT/node.log")"
sleep 20; echo "[2] node alive after its Meta was fenced: $(kill -0 "$(cat node.pid)" 2>/dev/null && echo yes || echo 'no (heartbeat failure ends the Node)')"
kill -KILL "$(cat meta2.pid)"; kill -KILL "$(cat meta.pid)"; sleep 1; meta_up meta.toml meta 7401; node_up
wait_ok "touch $F/d/after-fence" 120 && echo "[2] first Meta restarted, write: ok" || echo "[2] write after fence: FAILED"
rm -f $F/d/after-fence $F/d/fence-probe; S4=$(sums); [ "$S3" = "$S4" ] && echo "[2] content identical: yes" || echo "[2] content identical: NO"
# 3. kill -9 the backend under fsync'd writes
timeout 90 python3 - "$F/d" "$ROOT/acked.txt" <<'PY' &
import hashlib, os, sys, time
d, acked = sys.argv[1], sys.argv[2]
end = time.time() + 20
with open(acked, "w") as out:
    i = 0
    while time.time() < end:
        data = os.urandom(8192 + i)
        try:
            fd = os.open(f"{d}/w{i}", os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
            os.write(fd, data); os.fsync(fd); os.close(fd)
        except OSError as e:
            out.write(f"# stop at {i}: {e}\n"); out.flush(); break
        out.write(f"w{i} {hashlib.sha256(data).hexdigest()}\n"); out.flush(); i += 1
PY
W=$!; sleep 3; kill -KILL "$(cat backend.pid)"; echo "[3] backend killed at t=3 s"
wait $W; echo "[3] writer: $(grep -vc '^#' acked.txt) acknowledged; $(grep '^#' acked.txt | head -1)"
bash backend.cmd & echo $! > backend.pid; sleep 3
kill -KILL "$(cat meta.pid)" 2>/dev/null; sleep 1; meta_up meta.toml meta 7401 && echo "[3] backend and Meta restarted"; node_up
echo 3 > /proc/sys/vm/drop_caches
wait_ok "ls $F/d >/dev/null" 120
bad=0; n=0; while read -r name hash; do case $name in \#*) continue;; esac; n=$((n+1)); [ "$(sha256sum $F/d/$name 2>/dev/null | cut -d' ' -f1)" = "$hash" ] || bad=$((bad+1)); done < acked.txt
echo "[3] acknowledged files checked: $n, missing or wrong: $bad"
wait_ok "touch $F/d/after-backend" 120 && echo "[3] write after backend restart: ok" || echo "[3] write after backend restart: FAILED"
grep -hoE '"msg":"meta\.[a-z_.]*"' "$OUT/meta.log" | sort | uniq -c | sort -rn | head -8
sleep 1
# Pass: recovery from the backend twice with identical content, every acknowledged file intact after the
# backend was killed, and writes work after each restart.
[ "$(grep -c 'content identical: yes' "$OUT/backend.txt")" -eq 2 ] && grep -q 'missing or wrong: 0$' "$OUT/backend.txt" && \
  [ "$(grep -cE 'write.*: ok' "$OUT/backend.txt")" -eq 3 ] && ! grep -qE 'FAILED|NO \(' "$OUT/backend.txt"
