#!/usr/bin/env bash
# AFS DFS fault-scenario harness (suite local-fault, see suites/dfs/test_faults.py). One Meta and up to two
# DFS Nodes on 127.0.0.1 (node-a FUSE at $ROOT/mnt, node-b at $ROOT/mnt-b), MinIO for MODE=s3, an optional
# Redis or etcd Meta log; then PHASES (a phase script from this directory) runs against the live cluster with
# ROOT, OUT, BIN and FAULTS in its environment. Certificates for node-c are issued too: phases that need a
# third Node derive node-c.toml from node-b.toml and start it themselves.
#
# Env: BIN (afs-meta, afs-node, afs-bench), OUT (evidence, created), ROOT (new working directory on a native
#   Linux filesystem), PHASES; MODE native|r2|s3 (r2 = two Nodes; COPIES_R2 replicas, default 2);
#   NODE_EXTRA / META_EXTRA (extra TOML lines), S3_TWO=1 (node-b in s3 mode too), NODE_A_S3_PORT (node-a dials
#   S3 through another port), FREEZE_PROXY=1 (a TCP proxy 9100 -> 9000 that stalls while $ROOT/freeze exists),
#   META_BACKEND redis|etcd (needs REDIS_SERVER / REDIS_CLI or ETCD), MINIO (for MODE=s3).
# Ports 7400-7521, 9000-9001, 9100, 6380, 2389-2390 on 127.0.0.1. Runs as root. Stops services gracefully
# (SIGTERM) and unmounts normally; never lazy-unmounts. Exit status is the phase script's.
set -euo pipefail
MODE=${MODE:-native}
BIN=${BIN:?BIN}
OUT=${OUT:?OUT}
ROOT=${ROOT:?ROOT}
PHASES=${PHASES:?PHASES}
export FAULTS=${FAULTS:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy

[ -e "$ROOT" ] && { echo "refusing to reuse existing $ROOT" >&2; exit 2; }
stop() { [ -f "$1.pid" ] || return 0; local p; p=$(cat "$1.pid"); kill -TERM "$p" 2>/dev/null || true; for _ in $(seq 1 120); do kill -0 "$p" 2>/dev/null || { rm -f "$1.pid"; return 0; }; sleep 0.25; done; echo "$1 did not exit" >&2; return 1; }
# Whatever fails, stop what this run started (pid files under $ROOT) and unmount normally.
cleanup() {
  cd "$ROOT" 2>/dev/null || return 0
  for n in node node-b node-c; do
    stop "$n" || true
    m=mnt; [ "$n" != node ] && m=mnt-${n#node-}
    findmnt -n "$ROOT/$m" >/dev/null 2>&1 && fusermount3 -u "$ROOT/$m" 2>/dev/null
  done
  stop meta || true; stop minio || true; stop backend || true
  rm -f "$ROOT/freeze"; stop freeze-proxy || true
}
trap cleanup EXIT
if ss -ltn 2>/dev/null | grep -qE '127.0.0.1:(7400|7401|7500|7501|7510|7511|9000)\b'; then
  echo "ports 7400/7401/7500/7501/7510/7511/9000 busy; a previous run is still alive" >&2; exit 2
fi
mkdir -p "$ROOT"/{tls,meta/state,node/state,node/run,mnt,minio,node-b/state,node-b/run,mnt-b,node-c/state,node-c/run,mnt-c} "$OUT"
cd "$ROOT"

# --- mTLS identities (required whenever DFS is enabled) ---
ossl() { openssl "$@" >/dev/null 2>&1; }
ossl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=afs-test-ca -keyout tls/ca.key -out tls/ca.pem
printf 'subjectAltName=DNS:afs-meta,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n' > tls/ext
for n in meta node-a node-b node-c; do
  cn=$n; [ "$n" = meta ] && cn=afs-meta
  ossl req -newkey rsa:2048 -nodes -subj "/CN=$cn" -keyout tls/$n.key -out tls/$n.csr
  ossl x509 -req -in tls/$n.csr -CA tls/ca.pem -CAkey tls/ca.key -CAcreateserial -days 2 -extfile tls/ext -out tls/$n.pem
done
chmod 600 tls/*.key

common() {
  cat <<EOF
data_dir = "$ROOT/$1/state"
log_level = "info"
trace_enabled = false
tls_ca_certificate = "$ROOT/tls/ca.pem"
tls_identity_certificate = "$ROOT/tls/$2.pem"
tls_identity_private_key = "$ROOT/tls/$2.key"
tls_server_name = "afs-meta"
trusted_node_certs = { node-a = "$ROOT/tls/node-a.pem", node-b = "$ROOT/tls/node-b.pem", node-c = "$ROOT/tls/node-c.pem" }
EOF
}

s3_table=""
COPIES=${COPIES:-1}; [ "$MODE" = r2 ] && COPIES=${COPIES_R2:-2}
if [ "$MODE" = s3 ]; then
  MINIO=${MINIO:?MINIO is required for MODE=s3}
  export MINIO_ROOT_USER=s0bench MINIO_ROOT_PASSWORD=s0bench-secret-key
  "$MINIO" server "$ROOT/minio" --address 127.0.0.1:9000 --console-address 127.0.0.1:9001 >"$OUT/minio.log" 2>&1 &
  echo $! > minio.pid
  for _ in $(seq 1 60); do curl -fsS http://127.0.0.1:9000/minio/health/ready >/dev/null 2>&1 && break; sleep 0.5; done
  python3 - <<'PY'
import datetime, hashlib, hmac, urllib.request
host, region, key, secret = "127.0.0.1:9000", "us-east-1", "s0bench", "s0bench-secret-key"
now = datetime.datetime.now(datetime.timezone.utc)
amz, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
payload = hashlib.sha256(b"").hexdigest()
canon = f"PUT\n/afs-chunks\n\nhost:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{amz}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload}"
scope = f"{day}/{region}/s3/aws4_request"
sts = f"AWS4-HMAC-SHA256\n{amz}\n{scope}\n" + hashlib.sha256(canon.encode()).hexdigest()
k = ("AWS4" + secret).encode()
for part in (day, region, "s3", "aws4_request"):
    k = hmac.new(k, part.encode(), hashlib.sha256).digest()
sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
req = urllib.request.Request(f"http://{host}/afs-chunks", method="PUT", headers={
    "x-amz-date": amz, "x-amz-content-sha256": payload,
    "Authorization": f"AWS4-HMAC-SHA256 Credential={key}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"})
urllib.request.urlopen(req, timeout=10)
PY
  printf '{"access_key_id":"s0bench","secret_access_key":"s0bench-secret-key"}' > s3-cred.json; chmod 600 s3-cred.json
  s3_table=$(cat <<EOF

[dfs_external_backend]
store_id = "s0bench"
generation = "g1"
endpoint = "http://127.0.0.1:9000"
bucket = "afs-chunks"
region = "us-east-1"
allow_http = true
credentials_file = "$ROOT/s3-cred.json"
EOF
)
fi

backend_toml='meta_store = "local-file"'
case "${META_BACKEND:-}" in
  redis)
    mkdir -p "$ROOT/backend"; : "${REDIS_SERVER:?}" "${REDIS_CLI:?}"
    echo "exec $REDIS_SERVER --port 6380 --bind 127.0.0.1 --dir $ROOT/backend --appendonly yes --appendfsync always --no-appendfsync-on-rewrite no --maxmemory-policy noeviction --save '' --daemonize no >>$OUT/redis.log 2>&1" > backend.cmd
    bash backend.cmd &
    echo $! > backend.pid
    for _ in $(seq 1 40); do $REDIS_CLI -p 6380 ping >/dev/null 2>&1 && break; sleep 0.25; done
    backend_toml=$(printf 'meta_store = "redis"\nmeta_redis_url = "redis://127.0.0.1:6380/"\nmeta_store_prefix = "afs/fault"');;
  etcd)
    mkdir -p "$ROOT/backend"; : "${ETCD:?}"
    echo "exec $ETCD --data-dir $ROOT/backend/etcd --listen-client-urls http://127.0.0.1:2389 --advertise-client-urls http://127.0.0.1:2389 --listen-peer-urls http://127.0.0.1:2390 --initial-advertise-peer-urls http://127.0.0.1:2390 --initial-cluster default=http://127.0.0.1:2390 --auto-compaction-mode periodic --auto-compaction-retention 5m --quota-backend-bytes 8589934592 ${ETCD_EXTRA:-} >>$OUT/etcd.log 2>&1" > backend.cmd
    bash backend.cmd &
    echo $! > backend.pid
    for _ in $(seq 1 40); do curl -fsS http://127.0.0.1:2389/health >/dev/null 2>&1 && break; sleep 0.25; done
    backend_toml=$(printf 'meta_store = "etcd"\nmeta_etcd_endpoints = ["http://127.0.0.1:2389"]\nmeta_store_prefix = "afs/fault"');;
esac
{ cat <<EOF
id = "meta-ctl"
fs = "dfs"
$backend_toml
grpc_listen = "127.0.0.1:7400"
rest_listen = "127.0.0.1:7401"
dfs_desired_copies = $COPIES
dfs_sync_required_copies = $COPIES
dfs_min_distinct_nodes = $COPIES
dfs_min_distinct_failure_domains = 1
dfs_local_copy = "required"
${META_EXTRA:-}
EOF
  common meta meta; echo "$s3_table"; } > meta.toml

{ cat <<EOF
id = "node-a"
fs = "dfs"
grpc_listen = "127.0.0.1:7500"
rest_listen = "127.0.0.1:7501"
meta_endpoint = "https://127.0.0.1:7400"
advertise_endpoint = "https://127.0.0.1:7500"
data_mode = "grpc"
uds_path = "$ROOT/node/run/node.sock"
dfs_mount = "$ROOT/mnt"
EOF
  common node node-a; echo "$s3_table" | sed "s#127.0.0.1:9000#127.0.0.1:${NODE_A_S3_PORT:-9000}#"; printf "%b" "${NODE_EXTRA:-}"; } > node.toml

"$BIN/afs-meta" --config meta.toml --print-config > "$OUT/meta-effective.json"
"$BIN/afs-node" --config node.toml --print-config > "$OUT/node-effective.json"
if [ "$MODE" = r2 ] || [ -n "${S3_TWO:-}" ]; then
  { cat <<EOF
id = "node-b"
fs = "dfs"
grpc_listen = "127.0.0.1:7510"
rest_listen = "127.0.0.1:7511"
meta_endpoint = "https://127.0.0.1:7400"
advertise_endpoint = "https://127.0.0.1:7510"
data_mode = "grpc"
uds_path = "$ROOT/node-b/run/node.sock"
dfs_mount = "$ROOT/mnt-b"
EOF
    common node-b node-b; echo "$s3_table"; printf "%b" "${NODE_EXTRA:-}"; } > node-b.toml
  "$BIN/afs-node" --config node-b.toml --print-config > "$OUT/node-b-effective.json"
  cp node-b.toml "$OUT/"
fi
cp meta.toml node.toml "$OUT/"

if [ -n "${FREEZE_PROXY:-}" ]; then
  python3 "$FAULTS/freeze_proxy.py" 9100 9000 "$ROOT/freeze" >"$OUT/freeze-proxy.log" 2>&1 &
  echo $! > freeze-proxy.pid
fi
"$BIN/afs-meta" --config meta.toml >"$OUT/meta.log" 2>&1 &
echo $! > meta.pid
for _ in $(seq 1 120); do curl -fsS http://127.0.0.1:7401/health >/dev/null 2>&1 && break; sleep 0.25; done
if [ "$MODE" = r2 ] || [ -n "${S3_TWO:-}" ]; then
  "$BIN/afs-node" --config node-b.toml >"$OUT/node-b.log" 2>&1 &
  echo $! > node-b.pid
  for _ in $(seq 1 240); do findmnt -n "$ROOT/mnt-b" >/dev/null 2>&1 && break; sleep 0.25; done
fi
"$BIN/afs-node" --config node.toml >"$OUT/node.log" 2>&1 &
echo $! > node.pid
for _ in $(seq 1 240); do findmnt -n "$ROOT/mnt" >/dev/null 2>&1 && break; sleep 0.25; done
findmnt -n -o SOURCE,FSTYPE,OPTIONS "$ROOT/mnt" | tee "$OUT/mount.txt"

set +e
ROOT=$ROOT OUT=$OUT bash "$PHASES" >"$OUT/phases.txt" 2>&1
rc=$?
set -e
echo "phases rc=$rc" | tee "$OUT/bench.rc"
du -sk "$ROOT/node/state" "$ROOT/node-b/state" "$ROOT/meta/state" "$ROOT/minio" > "$OUT/du.txt" 2>/dev/null || true

# --- graceful stop (also from the EXIT trap) ---
exit $rc
