#!/usr/bin/env bash
set -euo pipefail

component=${1:?component is required}
: "${BUILDKITE_COMMIT:?Buildkite revision required}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]

export ADX_RELEASE_TARGET=x86_64-unknown-linux-gnu
source .buildkite/bootstrap-build.sh
host=$(rustc -vV | sed -n 's/^host: //p')
[[ $host == "$ADX_RELEASE_TARGET" ]] || { echo "builder target mismatch: $host" >&2; exit 1; }

output="out/buildkite/components/$component"
rm -rf "$output"
mkdir -p "$output"
jobs=${JOBS:-4}

case "$component" in
  platform)
    echo "--- :rust: Compile Platform"
    cargo build --locked --release -j "$jobs" \
      -p adx-deployment -p adx-master -p adx-node-manager --bins
    for binary in adxctl adx-master adx-node-manager; do
      cp "$CARGO_TARGET_DIR/release/$binary" "$output/$binary"
    done
    ;;
  gateway)
    echo "--- :rust: Compile Gateway"
    cargo build --locked --release -j "$jobs" \
      -p adx-api-server -p data-plane-gateway --bins
    for binary in adx-api-server adx-edge-frontend adx-node-proxy adx-data-plane-forward; do
      cp "$CARGO_TARGET_DIR/release/$binary" "$output/$binary"
    done
    ;;
  rrt)
    echo "--- :rust: Compile static RRT and runtime filesystem"
    musl_target=x86_64-unknown-linux-musl
    cargo build --locked --release --target "$musl_target" -j "$jobs" \
      -p rrt-daemon --bin rrt-runtime
    cp "$CARGO_TARGET_DIR/$musl_target/release/rrt-runtime" "$output/rrt-runtime"
    python3 build/runtime/rootfs.py \
      --binary "$output/rrt-runtime" \
      --output "$output/adx-runtime-rootfs.img"
    ;;
  *)
    echo "unknown build component: $component" >&2
    exit 2
    ;;
esac

python3 build/release/component.py create \
  --component "$component" \
  --directory "$output" \
  --commit "$BUILDKITE_COMMIT" \
  --target "$ADX_RELEASE_TARGET"
tar -czf "out/buildkite/components/$component.tar.gz" -C "$output" .
