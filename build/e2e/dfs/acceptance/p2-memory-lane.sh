#!/usr/bin/env bash
set -euo pipefail

MODE=short
case "${1:-}" in
  --keep-running) MODE=keep ;;
  --stop) MODE=stop ;;
  "") ;;
  -h|--help)
    cat <<'USAGE'
Usage: p2-memory-lane.sh [--keep-running|--stop]

Default runs an isolated memory-backend short FUSE proof, saves per-run evidence,
and stops the isolated services afterward.

--keep-running starts the same isolated memory Meta/Node lane and leaves the FUSE
mounts and services live for follow-up standards probes.

--stop stops only the isolated lane after verifying pid, executable path and SHA.
USAGE
    exit 0
    ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac

ROOT=${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
VM=${VM:-afs-accept-a}
GUEST_RUN=${GUEST_RUN:-/mnt/lima-afsadata/afs-delivery/p2-memory-lane}
EVIDENCE_BASE=${EVIDENCE_BASE:-$ROOT/evidence/afs-delivery/p2-memory-lane}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
EVIDENCE=${EVIDENCE:-$EVIDENCE_BASE/runs/$RUN_ID}
META_PORT=${META_PORT:-17800}
META_REST=${META_REST:-17801}
NODE_PORT=${NODE_PORT:-17802}
NODE_REST=${NODE_REST:-17803}
META_BIN=${META_BIN:-$ROOT/.local/p3a-binaries/afs-meta}
NODE_BIN=${NODE_BIN:-$ROOT/.local/p3a-binaries/afs-node}
META_SHA=$(sha256sum "$META_BIN" | awk '{print $1}')
NODE_SHA=$(sha256sum "$NODE_BIN" | awk '{print $1}')

mkdir -p "$EVIDENCE_BASE/runs"
guest() { limactl shell "$VM" -- bash -lc "$1"; }

copy_evidence() {
  mkdir -p "$EVIDENCE/logs" "$EVIDENCE/config"
  sha256sum "$META_BIN" "$NODE_BIN" > "$EVIDENCE/host-binaries.sha256"
  limactl copy "$VM:$GUEST_RUN/logs" "$EVIDENCE/logs" >/dev/null
  limactl copy "$VM:$GUEST_RUN/meta.toml" "$EVIDENCE/config/meta.toml" >/dev/null
  limactl copy "$VM:$GUEST_RUN/node.toml" "$EVIDENCE/config/node.toml" >/dev/null
  EVIDENCE_DIR="$EVIDENCE" python3 - <<'PY'
from pathlib import Path
import os
import shutil
root=Path(os.environ['EVIDENCE_DIR'])
for base in [root/'logs']:
    nested=base/'logs'
    if nested.exists():
        for child in nested.iterdir():
            target=base/child.name
            if target.exists():
                if target.is_dir(): shutil.rmtree(target)
                else: target.unlink()
            shutil.move(str(child), str(target))
        nested.rmdir()
PY
}

write_readme() {
  local state_note=$1
  README_EVIDENCE="$EVIDENCE" README_RUN_ID="$RUN_ID" README_MODE="$MODE" README_STATE="$state_note" \
  README_VM="$VM" README_PORTS="$META_PORT/$META_REST/$NODE_PORT/$NODE_REST" README_GUEST_RUN="$GUEST_RUN" \
  README_META_BIN="$META_BIN" README_NODE_BIN="$NODE_BIN" \
  python3 - <<'PY'
from pathlib import Path
import os
root = Path(os.environ['README_EVIDENCE'])
sha_path = root / 'host-binaries.sha256'
sha_text = sha_path.read_text() if sha_path.exists() else ''
text = f"""# P2 memory FUSE lane evidence

Run id: `{os.environ['README_RUN_ID']}`.
Mode: `{os.environ['README_MODE']}`.
State after script: {os.environ['README_STATE']}.

This is a bounded, memory-backend, single-node functional lane on `{os.environ['README_VM']}`.
It uses isolated ports `{os.environ['README_PORTS']}` and isolated guest runtime `{os.environ['README_GUEST_RUN']}`.
It does not use or modify existing ctl `17500/17501` or NodeA `17400/17401` services.

Candidate binaries staged from host `{os.environ['README_META_BIN']}` and `{os.environ['README_NODE_BIN']}`:

```text
{sha_text}```

Meta backend: `memory`. This lane intentionally does not prove restart durability.

Validation performed when present in logs:

- Meta health ready.
- Node health ready.
- DFS and OwnerFs FUSE mounts present.
- Actual process `/proc/<pid>/exe` SHA captured for Meta and Node without sudo-wrapper PID capture.
- OwnerFs create/write/fsync/close/reopen short proof.
- DFS create/write/fsync/close/reopen short proof.
- OwnerFs root Home REST query against memory Meta.

This is not a full acceptance pass and does not run pjdfstest/performance suites.
"""
(root / 'README.md').write_text(text)
PY
}

stop_lane() {
  local state_note="stopped"
  guest "set -euo pipefail
RUN='$GUEST_RUN'
META_SHA='$META_SHA'
NODE_SHA='$NODE_SHA'
mkdir -p \"\$RUN/logs\"
stop_one() {
  role=\$1
  pid_file=\$2
  expected_path=\$3
  expected_sha=\$4
  sudo_prefix=\$5
  [ -f \"\$pid_file\" ] || { echo \"\$role no-pid\"; return 0; }
  pid=\$(cat \"\$pid_file\")
  [ -n \"\$pid\" ] || { echo \"\$role empty-pid\"; return 0; }
  if ! \$sudo_prefix kill -0 \"\$pid\" 2>/dev/null; then echo \"\$role not-running pid=\$pid\"; return 0; fi
  exe=\$(\$sudo_prefix readlink /proc/\$pid/exe 2>/dev/null || true)
  sha=\$(\$sudo_prefix sha256sum /proc/\$pid/exe 2>/dev/null | awk '{print \$1}' || true)
  echo \"\$role pid=\$pid exe=\$exe sha=\$sha\" >> \"\$RUN/logs/stop-identity.txt\"
  if [ \"\$exe\" != \"\$expected_path\" ] || [ \"\$sha\" != \"\$expected_sha\" ]; then
    echo \"refuse to stop \$role: pid=\$pid exe=\$exe sha=\$sha\" >&2
    exit 21
  fi
  \$sudo_prefix kill -TERM \"\$pid\"
  for i in {1..100}; do \$sudo_prefix kill -0 \"\$pid\" 2>/dev/null || return 0; sleep .1; done
  \$sudo_prefix kill -KILL \"\$pid\" 2>/dev/null || true
}
stop_one node \"\$RUN/run/node.pid\" \"\$RUN/bin/afs-node\" \"\$NODE_SHA\" sudo
stop_one meta \"\$RUN/run/meta.pid\" \"\$RUN/bin/afs-meta\" \"\$META_SHA\" \"\" "
  guest "set +e
{ echo ===ports-after===; ss -ltnp | grep -E ':(($META_PORT)|($META_REST)|($NODE_PORT)|($NODE_REST))' || true; echo ===mounts-after===; findmnt -rn --mountpoint '$GUEST_RUN/mount-dfs' -o SOURCE,FSTYPE,TARGET || true; findmnt -rn --mountpoint '$GUEST_RUN/mount-ownerfs' -o SOURCE,FSTYPE,TARGET || true; } > '$GUEST_RUN/logs/cleanup-state.txt'
"
  copy_evidence
  write_readme "$state_note"
  printf '%s\n' "$EVIDENCE" > "$EVIDENCE_BASE/latest.txt"
  echo "evidence=$EVIDENCE"
  cat "$EVIDENCE/logs/cleanup-state.txt" 2>/dev/null || true
}

if [ "$MODE" = stop ]; then
  stop_lane
  exit 0
fi

sha256sum "$META_BIN" "$NODE_BIN"

guest "set -euo pipefail
if ss -ltnH | awk '{print \$4}' | grep -Eq ':($META_PORT|$META_REST|$NODE_PORT|$NODE_REST)\$'; then
  echo 'isolated lane is already running; stop with its original binary identity first' >&2
  exit 10
fi
for name in mount-dfs mount-ownerfs; do
  if mountpoint -q '$GUEST_RUN/'\$name; then
    echo 'isolated lane mount is still present; inspect before replacing binaries' >&2
    exit 11
  fi
done
sudo install -d -o \$(id -u) -g \$(id -g) '$GUEST_RUN/bin'
"
limactl copy "$META_BIN" "$VM:$GUEST_RUN/bin/afs-meta"
limactl copy "$NODE_BIN" "$VM:$GUEST_RUN/bin/afs-node"

guest "set -euo pipefail
sudo install -d -o \$(id -u) -g \$(id -g) '$GUEST_RUN'
if ss -ltnp | grep -E ':(($META_PORT)|($META_REST)|($NODE_PORT)|($NODE_REST))' > '$GUEST_RUN/preflight-ports.tmp'; then
  cat '$GUEST_RUN/preflight-ports.tmp' >&2
  exit 10
fi
for name in mount-dfs mount-ownerfs; do
  if mountpoint -q '$GUEST_RUN/'\$name; then sudo umount '$GUEST_RUN/'\$name; fi
done
for name in state/meta state/node run logs mount-ownerfs mount-dfs; do
  sudo rm -rf '$GUEST_RUN/'\$name
  mkdir -p '$GUEST_RUN/'\$name
done
rm -f '$GUEST_RUN/preflight-ports.tmp'
chmod +x '$GUEST_RUN/bin/afs-meta' '$GUEST_RUN/bin/afs-node'
cat > '$GUEST_RUN/meta.toml' <<EOF_META
id = 'memory-meta-a'
fs = 'all'
meta_store = 'memory'
data_dir = '$GUEST_RUN/state/meta'
uds_path = '$GUEST_RUN/run/meta.sock'
grpc_listen = '0.0.0.0:$META_PORT'
rest_listen = '0.0.0.0:$META_REST'
tls_ca_certificate = '/mnt/lima-afsadata/afs-delivery/p1b/tls/ca.pem'
tls_identity_certificate = '/mnt/lima-afsadata/afs-delivery/p1b/tls/node-a.pem'
tls_identity_private_key = '/mnt/lima-afsadata/afs-delivery/p1b/tls/node-a-key.pem'
tls_server_name = 'afs-cluster'
trusted_node_certs = { memory-node-a = '/mnt/lima-afsadata/afs-delivery/p1b/tls/node-a.pem', memory-node-b = '/mnt/lima-afsadata/afs-delivery/p1b/tls/node-b.pem' }
log_level = 'info'
trace_enabled = false
EOF_META
cat > '$GUEST_RUN/node.toml' <<EOF_NODE
id = 'memory-node-a'
fs = 'all'
meta_endpoint = 'https://192.168.109.12:$META_PORT'
advertise_endpoint = 'https://192.168.109.12:$NODE_PORT'
grpc_listen = '0.0.0.0:$NODE_PORT'
rest_listen = '0.0.0.0:$NODE_REST'
data_dir = '$GUEST_RUN/state/node'
uds_path = '$GUEST_RUN/run/node.sock'
ownerfs_mount = '$GUEST_RUN/mount-ownerfs'
dfs_mount = '$GUEST_RUN/mount-dfs'
data_mode = 'grpc'
tls_ca_certificate = '/mnt/lima-afsadata/afs-delivery/p1b/tls/ca.pem'
tls_identity_certificate = '/mnt/lima-afsadata/afs-delivery/p1b/tls/node-a.pem'
tls_identity_private_key = '/mnt/lima-afsadata/afs-delivery/p1b/tls/node-a-key.pem'
tls_server_name = 'afs-cluster'
trusted_node_certs = { memory-node-a = '/mnt/lima-afsadata/afs-delivery/p1b/tls/node-a.pem', memory-node-b = '/mnt/lima-afsadata/afs-delivery/p1b/tls/node-b.pem' }
log_level = 'info'
trace_enabled = false
EOF_NODE
sha256sum '$GUEST_RUN/bin/afs-meta' '$GUEST_RUN/bin/afs-node' > '$GUEST_RUN/logs/staged-binaries.sha256'
openssl x509 -in /mnt/lima-afsadata/afs-delivery/p1b/tls/node-a.pem -noout -subject -issuer -ext subjectAltName > '$GUEST_RUN/logs/tls-node-a.txt'
"

guest "set -euo pipefail
'$GUEST_RUN/bin/afs-meta' --config '$GUEST_RUN/meta.toml' > '$GUEST_RUN/logs/meta.log' 2>&1 < /dev/null &
echo \$! > '$GUEST_RUN/run/meta.pid'
"

guest "set -euo pipefail
ok=0
for i in {1..100}; do
  pid=\$(cat '$GUEST_RUN/run/meta.pid')
  kill -0 \$pid
  if curl -fsS http://127.0.0.1:$META_REST/health > '$GUEST_RUN/logs/meta-health.json' 2> '$GUEST_RUN/logs/meta-health.err'; then
    sha256sum /proc/\$pid/exe > '$GUEST_RUN/logs/meta-proc.sha256'
    readlink /proc/\$pid/exe > '$GUEST_RUN/logs/meta-proc-exe.txt'
    ok=1
    break
  fi
  sleep .2
done
if [ \$ok -ne 1 ]; then cat '$GUEST_RUN/logs/meta.log' >&2; exit 1; fi
"

guest "set -euo pipefail
cat > '$GUEST_RUN/run/start-node.sh' <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
'$GUEST_RUN/bin/afs-node' --config '$GUEST_RUN/node.toml' > '$GUEST_RUN/logs/node.log' 2>&1 < /dev/null &
echo \$! > '$GUEST_RUN/run/node.pid'
EOS
sudo bash '$GUEST_RUN/run/start-node.sh'
"

guest "set -euo pipefail
ok=0
for i in {1..150}; do
  pid=\$(cat '$GUEST_RUN/run/node.pid')
  sudo kill -0 \$pid
  if curl -fsS http://127.0.0.1:$NODE_REST/health > '$GUEST_RUN/logs/node-health.json' 2> '$GUEST_RUN/logs/node-health.err' && mountpoint -q '$GUEST_RUN/mount-dfs' && mountpoint -q '$GUEST_RUN/mount-ownerfs'; then
    sudo sha256sum /proc/\$pid/exe > '$GUEST_RUN/logs/node-proc.sha256'
    sudo readlink /proc/\$pid/exe > '$GUEST_RUN/logs/node-proc-exe.txt'
    findmnt -rn --mountpoint '$GUEST_RUN/mount-dfs' -o SOURCE,FSTYPE,TARGET,OPTIONS > '$GUEST_RUN/logs/mount-dfs.txt'
    findmnt -rn --mountpoint '$GUEST_RUN/mount-ownerfs' -o SOURCE,FSTYPE,TARGET,OPTIONS > '$GUEST_RUN/logs/mount-ownerfs.txt'
    ok=1
    break
  fi
  sleep .2
done
if [ \$ok -ne 1 ]; then cat '$GUEST_RUN/logs/node.log' >&2; exit 1; fi
"

guest "set -euo pipefail
RUN='$GUEST_RUN'
STAMP=memory-lane-\$(date -u +%Y%m%dT%H%M%SZ)
printf '%s' \$STAMP > '$GUEST_RUN/logs/workspace.txt'
sudo RUN=\$RUN STAMP=\$STAMP python3 - <<'PY' > '$GUEST_RUN/logs/io-proof.json'
import json, os
from pathlib import Path
run=Path(os.environ['RUN']); stamp=os.environ['STAMP']
owner=run/'mount-ownerfs'; dfs=run/'mount-dfs'
ws='owner-'+stamp
owner_root=owner/ws
owner_dir=owner_root/'dir'
owner_file=owner_dir/'hello.txt'
dfs_file=dfs/('dfs-'+stamp+'.txt')
owner_root.mkdir(mode=0o755)
owner_dir.mkdir(mode=0o755)
owner_data=('owner-memory-'+stamp+'\\n').encode()
fd=os.open(owner_file, os.O_CREAT|os.O_TRUNC|os.O_RDWR, 0o644)
os.write(fd, owner_data)
os.fsync(fd)
os.close(fd)
owner_read=owner_file.read_bytes()
dfs_data=('dfs-memory-'+stamp+'\\n').encode()
fd=os.open(dfs_file, os.O_CREAT|os.O_TRUNC|os.O_RDWR, 0o644)
os.write(fd, dfs_data)
os.fsync(fd)
os.close(fd)
dfs_read=dfs_file.read_bytes()
print(json.dumps({
  'workspace_name': ws,
  'owner_file': str(owner_file),
  'owner_read': owner_read.decode(),
  'dfs_file': str(dfs_file),
  'dfs_read': dfs_read.decode(),
}, sort_keys=True))
PY
ROOT_ID=root-\$(printf '%s' owner-\$STAMP | xxd -p -c 256)
printf '%s' \$ROOT_ID > '$GUEST_RUN/logs/root-id.txt'
curl -fsS http://127.0.0.1:$META_REST/v1/roots/\$ROOT_ID > '$GUEST_RUN/logs/root-location.json'
"

guest "set +e
{ echo ===health===; cat '$GUEST_RUN/logs/meta-health.json'; echo; cat '$GUEST_RUN/logs/node-health.json'; echo ===proc===; cat '$GUEST_RUN/logs/meta-proc.sha256'; cat '$GUEST_RUN/logs/meta-proc-exe.txt'; cat '$GUEST_RUN/logs/node-proc.sha256'; cat '$GUEST_RUN/logs/node-proc-exe.txt'; echo ===mounts===; cat '$GUEST_RUN/logs/mount-dfs.txt'; cat '$GUEST_RUN/logs/mount-ownerfs.txt'; echo ===io===; cat '$GUEST_RUN/logs/io-proof.json'; echo ===root===; cat '$GUEST_RUN/logs/root-location.json'; echo ===ports===; ss -ltnp | grep -E ':(($META_PORT)|($META_REST)|($NODE_PORT)|($NODE_REST))' || true; } > '$GUEST_RUN/logs/final-state.txt'
"

if [ "$MODE" = keep ]; then
  copy_evidence
  write_readme "running"
  printf '%s\n' "$EVIDENCE" > "$EVIDENCE_BASE/latest.txt"
  echo "evidence=$EVIDENCE"
  cat "$EVIDENCE/logs/final-state.txt"
  exit 0
fi

stop_lane
