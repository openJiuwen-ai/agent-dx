#!/usr/bin/env bash
set -euo pipefail

for legacy_flag in ADX_WITH_DFS ADX_DFS_ALL_FEATURES; do
  if [[ ${!legacy_flag+x} ]]; then
    echo "$legacy_flag was replaced by ${legacy_flag/DFS/AFS}; update the caller" >&2
    exit 2
  fi
done
# Linux release builder. sandboxd is supplied independently by the runtime environment.
: "${CARGO_TARGET_DIR:?set persistent Cargo cache}"
: "${ADX_REDIS_SERVER:?provide pinned Redis 7.2.5 binary}"
: "${ADX_REDIS_CLI:?provide matching Redis 7.2.5 CLI binary}"
: "${ADX_RELEASE_TARGET:?set artifact target triple}"
: "${ADX_RELEASE_OUTPUT:?set new output package directory}"
JOBS=${JOBS:-2}
PYTHON=${PYTHON:-python3}
ADX_WITH_AFS=${ADX_WITH_AFS:-0}
case "$ADX_WITH_AFS" in
  0|1) ;;
  *) echo 'ADX_WITH_AFS must be 0 or 1' >&2; exit 2 ;;
esac
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
host=$(rustc -vV | sed -n 's/^host: //p')
[[ "$host" == "$ADX_RELEASE_TARGET" ]] || { echo 'release target must match the native builder' >&2; exit 1; }
case "$host" in
 x86_64-unknown-linux-gnu) ;;
 aarch64-unknown-linux-gnu) ;;
 aarch64-apple-darwin) ;;
 *) echo 'unsupported native release target' >&2; exit 1 ;;
esac
if [[ "$ADX_WITH_AFS" == "1" && "$host" != *-linux-gnu ]]; then
  echo 'ADX_WITH_AFS=1 requires a Linux release builder' >&2
  exit 2
fi
stage=$(mktemp -d "${TMPDIR:-/tmp}/adx-build.XXXXXX")
trap 'rm -rf "$stage"' EXIT
echo "--- :rust: Compile control plane, gateway and EXECD"
cargo build --locked --release -j "$JOBS" -p adx-apiserver -p adx-deployment -p adx-coordinator -p adxlet -p adx-execd --bins
cargo build --locked --release -j "$JOBS" \
  -p data-plane-gateway --features agent-api --bin adx-ingress --bin adx-relay
for name in adx-apiserver adx-ingress adx-relay adxctl adx-inspect adx-coordinator adxlet adx-execd; do
 cp "$CARGO_TARGET_DIR/release/$name" "$stage/$name"
done
if [[ "$ADX_WITH_AFS" == "1" ]]; then
  echo "--- :file_folder: Compile Agent FS (AFS)"
  cargo build --locked --release -j "$JOBS" -p afs --bins
  for name in afs-meta afs-node; do
    cp "$CARGO_TARGET_DIR/release/$name" "$stage/$name"
  done
fi
if [[ "$host" == *-linux-gnu ]]; then
  musl_target="${host%-gnu}-musl"
  echo "--- :package: Static EXECD and local EROFS runtime"
  cargo build --locked --release --target "$musl_target" -j "$JOBS" -p adx-execd --bin adx-execd
  cp "$CARGO_TARGET_DIR/$musl_target/release/adx-execd" "$stage/adx-execd"
  "$PYTHON" build/runtime/rootfs.py --binary "$stage/adx-execd" --output "$stage/adx-runtime-rootfs.img"
fi
echo "--- :python: Build Sandbox SDK wheel"
PYTHON="$PYTHON" bash platform/sdk/sandbox/python/build.sh "$stage/sdk"
wheel=("$stage"/sdk/adx_sandbox-*.whl)
[[ ${#wheel[@]} == 1 && -f "${wheel[0]}" ]]
package_args=(
  build/release/package.py
  assemble
  --binary-dir "$stage"
  --redis "$ADX_REDIS_SERVER"
  --redis-cli "$ADX_REDIS_CLI"
  --wheel "${wheel[0]}"
  --target "$ADX_RELEASE_TARGET"
  --profile release
  --output "$ADX_RELEASE_OUTPUT"
)
if [[ "$ADX_WITH_AFS" == "1" ]]; then
  package_args+=(--with-afs)
fi
"$PYTHON" "${package_args[@]}"
