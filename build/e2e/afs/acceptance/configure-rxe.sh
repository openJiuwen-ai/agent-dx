#!/usr/bin/env bash
set -euo pipefail
[[ $(uname -s) == Linux && $(uname -m) == aarch64 && $EUID == 0 ]] || { echo 'root on dedicated ARM64 Linux required' >&2; exit 2; }
[[ $(uname -r) == 6.8.0-142-generic ]] || { echo 'boot frozen kernel first' >&2; exit 2; }
modprobe rdma_rxe
ip link set eth0 mtu 1500
if ! rdma link show | grep -q '^link rxe0/'; then rdma link add rxe0 type rxe netdev eth0; fi
mkdir -p /var/lib/afs-acceptance
rdma link show > /var/lib/afs-acceptance/rdma-links.txt
ibv_devinfo -d rxe0 -v > /var/lib/afs-acceptance/ibv-devinfo.txt
ip -brief address > /var/lib/afs-acceptance/ip-address.txt
for gid in /sys/class/infiniband/rxe0/ports/1/gids/*; do
 i=${gid##*/}
 value=$(cat "$gid")
 [[ $value != 0000:0000:0000:0000:0000:0000:0000:0000 ]] || continue
 type=$(cat /sys/class/infiniband/rxe0/ports/1/gid_attrs/types/"$i")
 printf '%s %s %s\n' "$i" "$value" "$type"
done > /var/lib/afs-acceptance/gids.txt
printf '* soft memlock unlimited\n* hard memlock unlimited\n' > /etc/security/limits.d/afs-acceptance.conf
cat /var/lib/afs-acceptance/rdma-links.txt /var/lib/afs-acceptance/gids.txt
