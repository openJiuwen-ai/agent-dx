#!/usr/bin/env bash
set -euo pipefail
# Linux-only; caller controls CARGO_TARGET_DIR. Meta can be built without either backend.
if [[ $(uname -s) != Linux ]]; then echo 'Linux required' >&2; exit 1; fi
cargo check --locked -p afs --no-default-features --bin afs-meta
for fs in ownerfs dfs; do
  cargo test --locked -p afs --no-default-features --features "$fs" --test config_contract
  cargo check --locked -p afs --no-default-features --features "$fs" --all-targets
 done
cargo check --locked -p afs-transport --no-default-features
cargo tree --locked -p afs-client -e features > /tmp/afs-client-feature-tree.txt
if grep 'afs-transport feature "rdma"' /tmp/afs-client-feature-tree.txt; then
  echo 'SDK unexpectedly enables RDMA' >&2; exit 1
fi
