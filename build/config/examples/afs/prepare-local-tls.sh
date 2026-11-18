#!/usr/bin/env bash
# prepare-local-tls.sh — issue the mTLS material one AFS host needs.
#
# Idempotent: existing files are KEPT, never overwritten. Only missing pieces are created, so a
# repeated install (upgrade, re-render, retry) succeeds instead of aborting. A half-written pair
# (<ID>.pem without <ID>-key.pem, or the reverse) is an error: that state cannot be repaired
# without destroying a private key.
#
# Production note: the material is only as good as the CA it chains to. Sharing ca-key.pem
# across hosts (--ca-dir) is what lets every host issue its own identity, so that file must be
# distributed over a trusted channel; Node private keys must never leave their host.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: prepare-local-tls.sh [DEST] [--node-id ID] [--days N] [--ca-dir DIR]

  DEST          target directory, absolute and not "/". Default: /opt/adx/config/afs/tls
  --node-id ID  issue the identity <ID>.pem / <ID>-key.pem (the AFS Node id).
                Also read from AFS_TLS_NODE_ID. Without it the legacy single-host example is
                reproduced: identities "meta" and "node-a" (30-day local trial material).
  --days N      validity of the CA and the identities, in days (default 30).
                Also read from AFS_TLS_DAYS.
  --ca-dir DIR  import the CA from DIR (needs ca.pem and ca-key.pem) instead of creating one.
                This is how peer hosts join a single cluster trust domain.
                Also read from AFS_TLS_CA_DIR.

Existing files are kept as-is; only missing material is created (safe to rerun).
EOF
}

die() {
  echo "prepare-local-tls: $*" >&2
  exit 1
}

destination=""
node_id="${AFS_TLS_NODE_ID:-}"
days="${AFS_TLS_DAYS:-30}"
ca_dir="${AFS_TLS_CA_DIR:-}"

while (($#)); do
  case "$1" in
    --node-id) [[ $# -ge 2 ]] || die "--node-id needs a value"; node_id=$2; shift 2 ;;
    --node-id=*) node_id=${1#*=}; shift ;;
    --days) [[ $# -ge 2 ]] || die "--days needs a value"; days=$2; shift 2 ;;
    --days=*) days=${1#*=}; shift ;;
    --ca-dir) [[ $# -ge 2 ]] || die "--ca-dir needs a value"; ca_dir=$2; shift 2 ;;
    --ca-dir=*) ca_dir=${1#*=}; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) die "unknown option: $1 (try --help)" ;;
    *)
      [[ -z $destination ]] || die "unexpected argument: $1 (DEST already set to $destination)"
      destination=$1
      shift
      ;;
  esac
done

destination=${destination:-/opt/adx/config/afs/tls}
[[ $destination == /* && $destination != / ]] || die "TLS destination must be an absolute directory other than /"
[[ $days =~ ^[1-9][0-9]*$ ]] || die "--days must be a positive integer (got: $days)"
if [[ -n $node_id ]]; then
  [[ $node_id =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] \
    || die "--node-id must be a plain name (letters, digits, '.', '_', '-'), got: $node_id"
fi
command -v openssl >/dev/null || die "openssl is required"

# Identities to make sure exist. With --node-id only that one: deploy renders
# tls_identity_certificate as <node id>.pem for both Meta and Node.
if [[ -n $node_id ]]; then
  identities=("$node_id")
else
  identities=(meta node-a)
fi

install -d -m 0700 "$destination"
umask 077

# Pre-flight the identity pairs before touching the CA: failing after the CA was generated would
# leave a directory that still needs manual cleanup.
for name in "${identities[@]}"; do
  cert="$destination/$name.pem"
  key="$destination/$name-key.pem"
  if [[ -f $cert && -f $key ]]; then
    continue
  fi
  if [[ -f $cert || -f $key ]]; then
    die "half-written identity for $name in $destination: remove both ${name}.pem and ${name}-key.pem, then rerun"
  fi
done

work=""
cleanup() { [[ -z $work ]] || rm -rf -- "$work"; }
trap cleanup EXIT INT TERM
scratch() {
  if [[ -z $work ]]; then
    work=$(mktemp -d "${TMPDIR:-/tmp}/adx-afs-tls.XXXXXX")
  fi
  echo "$work"
}

ca_imported=false
if [[ -f $destination/ca.pem && -f $destination/ca-key.pem ]]; then
  echo "CA already present: $destination/ca.pem (kept)"
elif [[ -n $ca_dir ]]; then
  [[ -f $ca_dir/ca.pem && -f $ca_dir/ca-key.pem ]] \
    || die "--ca-dir must contain ca.pem and ca-key.pem (got: $ca_dir)"
  install -m 0644 "$ca_dir/ca.pem" "$destination/ca.pem"
  install -m 0600 "$ca_dir/ca-key.pem" "$destination/ca-key.pem"
  ca_imported=true
  echo "CA imported from $ca_dir"
else
  dir=$(scratch)
  openssl req -x509 -newkey rsa:3072 -nodes -days "$days" -sha256 \
    -subj "/CN=ADX AFS local example CA" \
    -keyout "$dir/ca-key.pem" -out "$dir/ca.pem" >/dev/null 2>&1
  install -m 0644 "$dir/ca.pem" "$destination/ca.pem"
  install -m 0600 "$dir/ca-key.pem" "$destination/ca-key.pem"
  echo "CA generated: $destination/ca.pem ($days-day lifetime)"
fi

issue_identity() {
  local name=$1 cert="$destination/$1.pem" key="$destination/$1-key.pem" dir san
  if [[ -f $cert && -f $key ]]; then
    echo "identity $name already present (kept)"
    return 0
  fi
  if [[ -f $cert || -f $key ]]; then
    die "half-written identity for $name in $destination: remove both ${name}.pem and ${name}-key.pem, then rerun"
  fi
  dir=$(scratch)

  san="subjectAltName=DNS:afs-cluster,DNS:$name,IP:127.0.0.1"
  # node-<a.b.c.d> (the deploy convention) also gets the address as an IP SAN so a peer that
  # verifies against the host address instead of the cluster DNS name still matches.
  if [[ $name =~ ^node-([0-9]{1,3})-([0-9]{1,3})-([0-9]{1,3})-([0-9]{1,3})$ ]]; then
    san+=",IP:${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}.${BASH_REMATCH[4]}"
  fi

  openssl req -new -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=$name" -keyout "$dir/$name-key.pem" -out "$dir/$name.csr" >/dev/null 2>&1
  printf '%s\n' "$san" "extendedKeyUsage=serverAuth,clientAuth" >"$dir/$name.ext"
  openssl x509 -req -days "$days" -sha256 -in "$dir/$name.csr" \
    -CA "$destination/ca.pem" -CAkey "$destination/ca-key.pem" -CAcreateserial \
    -extfile "$dir/$name.ext" -out "$dir/$name.pem" >/dev/null 2>&1
  openssl verify -CAfile "$destination/ca.pem" "$dir/$name.pem" >/dev/null
  install -m 0600 "$dir/$name-key.pem" "$key"
  install -m 0644 "$dir/$name.pem" "$cert"
  echo "identity $name issued: $cert ($days-day lifetime)"
}

for name in "${identities[@]}"; do
  issue_identity "$name"
done

# openssl's serial bookkeeping created by our own signing; tighten it but never delete it.
[[ ! -f $destination/ca.srl ]] || chmod 0600 "$destination/ca.srl"

if [[ $ca_imported == true ]]; then
  echo "TLS material ready in $destination (CA imported). Copy this host's <node-id>.pem into"
  echo "every peer's trusted_node_certs; never copy <node-id>-key.pem anywhere."
else
  echo "TLS material ready in $destination."
fi