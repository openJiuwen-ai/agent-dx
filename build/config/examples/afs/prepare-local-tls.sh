#!/usr/bin/env bash
set -euo pipefail

destination=${1:-/opt/adx/config/afs/tls}
[[ $destination == /* && $destination != / ]] || {
  echo "TLS destination must be an absolute directory other than /" >&2
  exit 2
}
command -v openssl >/dev/null || {
  echo "openssl is required" >&2
  exit 1
}

files=(ca.pem ca-key.pem meta.pem meta-key.pem node-a.pem node-a-key.pem)
for name in "${files[@]}"; do
  [[ ! -e $destination/$name ]] || {
    echo "refusing to replace existing TLS material: $destination/$name" >&2
    exit 1
  }
done

install -d -m 0700 "$destination"
work=$(mktemp -d "${TMPDIR:-/tmp}/adx-afs-tls.XXXXXX")
cleanup() {
  rm -rf -- "$work"
}
trap cleanup EXIT INT TERM
umask 077

openssl req -x509 -newkey rsa:3072 -nodes -days 30 -sha256 \
  -subj "/CN=ADX AFS local example CA" \
  -keyout "$work/ca-key.pem" -out "$work/ca.pem" >/dev/null 2>&1

issue_identity() {
  local name=$1
  openssl req -new -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=$name" -keyout "$work/$name-key.pem" \
    -out "$work/$name.csr" >/dev/null 2>&1
  printf '%s\n' \
    "subjectAltName=DNS:afs-cluster,DNS:$name,IP:127.0.0.1" \
    "extendedKeyUsage=serverAuth,clientAuth" >"$work/$name.ext"
  openssl x509 -req -days 30 -sha256 -in "$work/$name.csr" \
    -CA "$work/ca.pem" -CAkey "$work/ca-key.pem" -CAcreateserial \
    -extfile "$work/$name.ext" -out "$work/$name.pem" >/dev/null 2>&1
  openssl verify -CAfile "$work/ca.pem" "$work/$name.pem" >/dev/null
}

issue_identity meta
issue_identity node-a
install -m 0600 "$work/ca-key.pem" "$destination/ca-key.pem"
install -m 0644 "$work/ca.pem" "$destination/ca.pem"
for name in meta node-a; do
  install -m 0600 "$work/$name-key.pem" "$destination/$name-key.pem"
  install -m 0644 "$work/$name.pem" "$destination/$name.pem"
done
echo "Created local AFS example identities in $destination (30-day lifetime)."
