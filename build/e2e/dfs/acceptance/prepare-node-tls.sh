#!/usr/bin/env bash
set -euo pipefail
node_id=${1:?node identity}
ip=${2:?advertised IPv4}
run=${3:?guest ext4 runtime path}
case "$node_id" in node-[abc]) ;; *) echo 'unexpected dedicated Node identity'; exit 2;; esac
[ "$(findmnt -T "$(dirname "$run")" -n -o FSTYPE)" = ext4 ] || { echo 'runtime must be on guest ext4'; exit 1; }
mkdir -p "$run"/{bin,tls,state,run,logs,mount-dfs,mount-ownerfs}
if [ ! -e "$run/tls/$node_id-key.pem" ]; then
  umask 077
  openssl req -new -newkey rsa:2048 -nodes -subj "/CN=$node_id" -keyout "$run/tls/$node_id-key.pem" -out "$run/tls/$node_id.csr" 2>/dev/null
fi
[ -e "$run/tls/$node_id.csr" ] || { echo 'existing private key without CSR; refusing replacement'; exit 1; }
printf '%s\n' "subjectAltName=DNS:afs-cluster,DNS:$node_id,IP:$ip" 'extendedKeyUsage=serverAuth,clientAuth' > "$run/tls/$node_id.ext"
openssl req -in "$run/tls/$node_id.csr" -noout -verify
chmod 600 "$run/tls/$node_id-key.pem"
