#!/usr/bin/env bash
set -euo pipefail

component=${1:?component is required}
: "${BUILDKITE_COMMIT:?Buildkite revision required}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]

source .buildkite/build-architecture.sh
source .buildkite/bootstrap-build.sh
host=$(rustc -vV | sed -n 's/^host: //p')
[[ $host == "$ADX_RELEASE_TARGET" ]] || { echo "builder target mismatch: $host" >&2; exit 1; }

output="out/buildkite/components/$component"
rm -rf "$output"
mkdir -p "$output"
jobs=${JOBS:-4}

case "${ADX_COMPONENT_TESTS:-1}" in
  1) bash .buildkite/component-tests.sh "$component" ;;
  0) echo "Component tests disabled: $component" ;;
  *) echo 'ADX_COMPONENT_TESTS must be 0 or 1' >&2; exit 2 ;;
esac

case "$component" in
  platform)
    echo "--- :rust: Compile Platform"
    cargo build --locked --release -j "$jobs" \
      -p adx-deployment -p adx-coordinator -p adxlet --bins
    for binary in adxctl adx-inspect adx-coordinator adxlet; do
      cp "$CARGO_TARGET_DIR/release/$binary" "$output/$binary"
    done
    ;;
  gateway)
    echo "--- :rust: Compile Gateway"
    cargo build --locked --release -j "$jobs" \
      -p adx-apiserver --bin adx-apiserver
    cargo build --locked --release -j "$jobs" \
      -p data-plane-gateway --features agent-api --bin adx-ingress --bin adx-relay
    for binary in adx-apiserver adx-ingress adx-relay; do
      cp "$CARGO_TARGET_DIR/release/$binary" "$output/$binary"
    done
    ;;
  execd)
    echo "--- :rust: Compile static EXECD and runtime filesystem"
    musl_target=$ADX_MUSL_TARGET
    cargo build --locked --release --target "$musl_target" -j "$jobs" \
      -p adx-execd --bin adx-execd
    cp "$CARGO_TARGET_DIR/$musl_target/release/adx-execd" "$output/adx-execd"
    python3 build/runtime/rootfs.py \
      --binary "$output/adx-execd" \
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

if [[ ${ADX_COMPONENT_LOCAL:-0} != 1 ]]; then
  bash .buildkite/component-transfer.sh upload "$component"
fi
