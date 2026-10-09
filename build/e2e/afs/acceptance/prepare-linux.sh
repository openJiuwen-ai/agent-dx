#!/usr/bin/env bash
set -euo pipefail
[[ $(uname -s) == Linux && $(uname -m) == aarch64 ]] || { echo 'Dedicated ARM64 Linux VM required' >&2; exit 2; }
[[ $EUID == 0 ]] || { echo 'Run with sudo in the dedicated acceptance VM' >&2; exit 2; }
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y ca-certificates curl git build-essential pkg-config python3 python3-venv python3-pip jq rsync ripgrep openssl fuse3 fio iproute2 iputils-ping tcpdump iperf3 rdma-core ibverbs-providers ibverbs-utils rdmacm-utils perftest
apt-get install -y linux-image-6.8.0-142-generic linux-modules-extra-6.8.0-142-generic
# Product mounts expose POSIX access to multiple application UIDs. Root
# deployment needs no fusermount privilege exception; non-root development
# mounts still need this guest setting in addition to default_permissions.
touch /etc/fuse.conf
if ! rg -q '^\s*user_allow_other\s*(#.*)?$' /etc/fuse.conf; then
  printf '\nuser_allow_other\n' >> /etc/fuse.conf
fi
# These are VM reproducibility settings, not a product systemd deployment dependency.
systemctl disable --now apt-daily.timer apt-daily-upgrade.timer || true
swapoff -a
mkdir -p /var/lib/afs-acceptance
uname -a > /var/lib/afs-acceptance/uname-before-reboot.txt
dpkg-query -W -f='${Package} ${Version}\n' > /var/lib/afs-acceptance/packages.txt
printf 'Reboot into frozen kernel, then run network/RXE preflight. No acceptance PASS is implied.\n'
