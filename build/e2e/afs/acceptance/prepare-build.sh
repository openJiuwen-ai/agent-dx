#!/usr/bin/env bash
set -euo pipefail
[[ $(uname -s) == Linux && $(uname -m) == aarch64 ]] || exit 2
sudo env DEBIAN_FRONTEND=noninteractive apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y build-essential pkg-config ca-certificates curl git rsync ripgrep python3 python3-venv python3-yaml jq protobuf-compiler libssl-dev libibverbs-dev rdma-core ibverbs-providers fuse3 cmake clang-14 lld-14 libaio-dev libdouble-conversion-dev libgflags-dev libgoogle-glog-dev libevent-dev liblz4-dev liblzma-dev libsnappy-dev libunwind-dev libfmt-dev libuv1-dev libzstd-dev autoconf automake libtool flex bison
sudo install -d -o "$(id -u)" -g "$(id -g)" "$HOME/afs-build"
mkdir -p "$HOME/afs-build/setup" "$HOME/afs-build/work" "$HOME/afs-build/target"
curl --fail --location https://sh.rustup.rs --output "$HOME/afs-build/setup/rustup-init.sh"
sha256sum "$HOME/afs-build/setup/rustup-init.sh" > "$HOME/afs-build/setup/rustup-script.sha256"
sh "$HOME/afs-build/setup/rustup-init.sh" -y --profile minimal --default-toolchain 1.95.0
"$HOME/.cargo/bin/rustup" component add --toolchain 1.95.0 clippy rustfmt
dpkg-query -W -f='${Package} ${Version}\n' > "$HOME/afs-build/setup/packages.txt"
"$HOME/.cargo/bin/rustc" +1.95.0 -Vv > "$HOME/afs-build/setup/rustc.txt"
printf 'Linux build runtime prepared. No product gate implied.\n'
