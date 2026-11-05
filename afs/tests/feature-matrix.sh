#!/usr/bin/env bash
# AFS feature matrix: every supported feature combination must check and build its tests.
# Linux-only (see afs/AGENTS.md); the caller controls CARGO_TARGET_DIR. CARGO_JOBS defaults to 6.
# It also runs the single-backend config_contract. --run-lib adds the single-backend lib
# tests, which need TMPDIR on a native Linux filesystem with flock, xattr and over 1 GiB free:
# not the Mac-shared directories (flock, xattr) or 64 MiB /dev/shm of the adx-vm build
# container. There, build them with --lib --no-run and run the binaries on a runtime node.
set -euo pipefail
run_lib=0
case "${1:-}" in
  '') ;;
  --run-lib) run_lib=1 ;;
  *)
    echo "usage: $0 [--run-lib]" >&2
    exit 2
    ;;
esac
if [[ $(uname -s) != Linux ]]; then
  echo 'Linux required' >&2
  exit 1
fi
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
jobs=(-j "${CARGO_JOBS:-6}")

matrix=(
  'default|'
  'dfs|--no-default-features --features dfs'
  'ownerfs|--no-default-features --features ownerfs'
  'rdma|--features rdma'
)
for entry in "${matrix[@]}"; do
  name=${entry%%|*}
  read -r -a flags <<<"${entry#*|}"
  echo "== afs [$name]: check"
  cargo check "${jobs[@]}" --locked -p afs "${flags[@]}"
  echo "== afs [$name]: tests build"
  cargo test "${jobs[@]}" --locked -p afs "${flags[@]}" --tests --no-run
done

# Single-backend builds have no other gate.
for fs in ownerfs dfs; do
  echo "== afs [$fs]: config_contract"
  cargo test "${jobs[@]}" --locked -p afs --no-default-features --features "$fs" --test config_contract
  if [[ $run_lib == 1 ]]; then
    echo "== afs [$fs]: lib tests"
    cargo test "${jobs[@]}" --locked -p afs --no-default-features --features "$fs" --lib
  fi
done

# Meta builds without either filesystem backend.
echo '== afs-meta without backends'
cargo check "${jobs[@]}" --locked -p afs --no-default-features --bin afs-meta
echo '== afs-transport without features'
cargo check "${jobs[@]}" --locked -p afs-transport --no-default-features

# The SDK must not pull RDMA in through feature unification.
echo '== afs-client must not enable RDMA'
tree=$(cargo tree --locked -p afs-client -e features)
if grep -F 'afs-transport feature "rdma"' <<<"$tree"; then
  echo 'SDK unexpectedly enables RDMA' >&2
  exit 1
fi
echo 'feature matrix passed'
