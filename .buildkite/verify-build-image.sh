#!/usr/bin/env bash
set -euo pipefail

source /etc/os-release
[[ $ID == ubuntu && $VERSION_ID == 20.04 ]]

for command in autoconf automake libtoolize pkg-config musl-gcc readelf busybox \
  rustc cargo rustup rustfmt clippy-driver go protoc protoc-gen-go protoc-gen-go-grpc \
  mkfs.erofs fsck.erofs redis-server redis-cli python3; do
  command -v "$command" >/dev/null || { echo "missing build command: $command" >&2; exit 1; }
done

rustc --version | grep -F 'rustc 1.95.0 '
go version | grep -F 'go1.25.5 linux/amd64'
[[ $(go env GOROOT) == /usr/local/go ]]
rustup target list --installed | grep -Fx 'x86_64-unknown-linux-musl'
redis-server --version | grep -F 'v=7.2.5'
mkfs.erofs --version | grep -F '1.8.10'
fsck.erofs --version | grep -F '1.8.10'
python3 -c 'import build, yaml'
[[ $(python3 -c 'import sys; print(sys.prefix)') == /opt/adx-build-tools/python ]]
