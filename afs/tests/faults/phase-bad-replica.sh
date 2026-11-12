#!/usr/bin/env bash
# R59: one replica on node-b; corrupt it on disk behind its catalog; read from node-a in receiver CRC mode.
# Node-b must quarantine and self-report, and Meta must mark the copy bad and record repair debt.
set -u
RC=0
dd if=/dev/urandom of="$ROOT/mnt-b/file" bs=1M count=8 conv=fsync status=none; sync "$ROOT/mnt-b/file"; sleep 2
sha256sum "$ROOT/mnt-b/file"
mapfile -t CH < <(find "$ROOT/node-b/state/dfs" -type f -size +1M | head -4); echo "chunk files: ${CH[*]}"
[ "${#CH[@]}" -gt 0 ] || { echo "no chunk file"; exit 1; }
for f in "${CH[@]}"; do python3 - "$f" <<'PY'
import sys; f=open(sys.argv[1],'r+b'); f.seek(4096); b=f.read(1); f.seek(4096); f.write(bytes([b[0]^0xff])); f.close()
PY
done
ID=$(basename "${CH[0]}"); echo "chunk $ID corrupted on node-b"
dd if="$ROOT/mnt/file" of=/dev/null bs=1M status=none; echo "read from node-a rc=$? (a read with no other copy fails)"
for t in $(seq 1 24); do sleep 5
  A=$(grep -c dfs.suspect_copy_invalid "$OUT/node.log"); B=$(grep -c dfs.local_corruption_reported "$OUT/node-b.log")
  ID=$(grep -h dfs.local_corruption_reported "$OUT/node-b.log" | head -1 | python3 -c 'import json,sys; print(json.loads(sys.stdin.readline())["chunk"])' 2>/dev/null || echo "$ID"); [ -n "$ID" ] || ID=$(basename "${CH[0]}")
  R=$(curl -fsS "http://127.0.0.1:7401/v1/dfs/chunks/$ID/replication" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("health"), [c["record"]["state"] for c in d.get("copies",[])], [t["state"] for t in d.get("tasks",[])])' 2>&1)
  echo "[t=$((t*5))s] node-a suspect_invalid=$A node-b reported=$B meta: $R"
  case "$R" in *Corrupt*) echo "[marked bad at t=$((t*5))s]"; break;; esac
done
case "$R" in *Corrupt*) ;; *) echo "NOT marked bad"; RC=1;; esac
curl -fsS http://127.0.0.1:7501/metrics | grep -E 'suspect_copy' ; curl -fsS http://127.0.0.1:7511/metrics | grep -cE 'review'
exit $RC
