#!/usr/bin/env bash
# Run inside Node B. The corresponding A memory lane must already trust B.
set -euo pipefail
RUN=${RUN:-/mnt/lima-afsbdata/afs-delivery/p2-memory-peer}
META_ENDPOINT=${META_ENDPOINT:-https://192.168.109.12:17800}
NODE_PORT=${NODE_PORT:-17804}
REST_PORT=${REST_PORT:-17805}
TLS_DIR=${TLS_DIR:-/mnt/lima-afsbdata/afs-delivery/p1b/tls}
EXPECTED_NODE_SHA=${EXPECTED_NODE_SHA:?identified candidate SHA required}
[ "$(id -u)" = 0 ] || { echo 'requires root' >&2; exit 2; }
[ "$(findmnt -T "$RUN" -n -o FSTYPE)" = ext4 ] || exit 3
actual_sha=$(sha256sum "$RUN/bin/afs-node" | awk '{print $1}')
[ "$actual_sha" = "$EXPECTED_NODE_SHA" ] || { echo 'candidate SHA mismatch' >&2; exit 4; }
if [ "${1:-}" = --stop ]; then
  if [ -f "$RUN/run/node.pid" ]; then
    pid=$(cat "$RUN/run/node.pid")
    if kill -0 "$pid" 2>/dev/null; then
      [ "$(readlink "/proc/$pid/exe")" = "$RUN/bin/afs-node" ] || exit 5
      [ "$(sha256sum "/proc/$pid/exe" | awk '{print $1}')" = "$EXPECTED_NODE_SHA" ] || exit 6
      kill -TERM "$pid"
      for attempt in {1..100}; do kill -0 "$pid" 2>/dev/null || break; sleep .1; done
      if kill -0 "$pid" 2>/dev/null; then echo 'Node failed to stop; preserved for inspection' >&2; exit 7; fi
    fi
  fi
  findmnt -rn --mountpoint "$RUN/mount-dfs" > "$RUN/logs/stop-mount-dfs.txt" || true
  exit 0
fi
[ -z "${1:-}" ] || exit 2
if [ -f "$RUN/run/node.pid" ] && kill -0 "$(cat "$RUN/run/node.pid")" 2>/dev/null; then
  echo 'existing memory peer must be stopped explicitly' >&2; exit 8
fi
if ss -ltnH | awk '{print $4}' | grep -Eq ":($NODE_PORT|$REST_PORT)$"; then
  echo 'isolated ports occupied' >&2; exit 9
fi
for mount in mount-ownerfs mount-dfs; do
  if mountpoint -q "$RUN/$mount"; then echo 'existing mount requires inspection' >&2; exit 10; fi
done
for cert in ca.pem node-a.pem node-b.pem node-b-key.pem; do [ -s "$TLS_DIR/$cert" ] || exit 11; done
mkdir -p "$RUN"/{run,logs,state/node,mount-ownerfs,mount-dfs}
cat > "$RUN/node.toml" <<CONFIG
id = 'memory-node-b'
fs = 'all'
meta_endpoint = '$META_ENDPOINT'
advertise_endpoint = 'https://192.168.109.13:$NODE_PORT'
grpc_listen = '0.0.0.0:$NODE_PORT'
rest_listen = '0.0.0.0:$REST_PORT'
data_dir = '$RUN/state/node'
uds_path = '$RUN/run/node.sock'
ownerfs_mount = '$RUN/mount-ownerfs'
dfs_mount = '$RUN/mount-dfs'
data_mode = 'grpc'
tls_ca_certificate = '$TLS_DIR/ca.pem'
tls_identity_certificate = '$TLS_DIR/node-b.pem'
tls_identity_private_key = '$TLS_DIR/node-b-key.pem'
tls_server_name = 'afs-cluster'
trusted_node_certs = { memory-node-a = '$TLS_DIR/node-a.pem', memory-node-b = '$TLS_DIR/node-b.pem' }
log_level = 'info'
trace_enabled = false
CONFIG
nohup "$RUN/bin/afs-node" --config "$RUN/node.toml" > "$RUN/logs/node.log" 2>&1 < /dev/null &
pid=$!
echo "$pid" > "$RUN/run/node.pid"
for attempt in {1..150}; do
  kill -0 "$pid" || { cat "$RUN/logs/node.log" >&2; exit 12; }
  if curl -fsS "http://127.0.0.1:$REST_PORT/health" > "$RUN/logs/health.json" 2> "$RUN/logs/health.err" &&
      mountpoint -q "$RUN/mount-ownerfs" && mountpoint -q "$RUN/mount-dfs"; then
    sha256sum "/proc/$pid/exe" > "$RUN/logs/node-proc.sha256"
    readlink "/proc/$pid/exe" > "$RUN/logs/node-proc-exe.txt"
    findmnt -rn --mountpoint "$RUN/mount-ownerfs" -o SOURCE,FSTYPE,TARGET,OPTIONS > "$RUN/logs/mount-ownerfs.txt"
    findmnt -rn --mountpoint "$RUN/mount-dfs" -o SOURCE,FSTYPE,TARGET,OPTIONS > "$RUN/logs/mount-dfs.txt"
    cat "$RUN/logs/health.json"
    exit 0
  fi
  sleep .2
done
echo 'memory peer not ready; logs retained' >&2
exit 13
