#!/usr/bin/env bash
set -euo pipefail
# Linux release builder. sandboxd is supplied independently by the runtime environment.
: "${CARGO_TARGET_DIR:?set persistent Cargo cache}"
: "${GOCACHE:?set persistent Go build cache}"
: "${GOMODCACHE:?set persistent Go module cache}"
: "${ADX_REDIS_SERVER:?provide pinned Redis 7.2.5 binary}"
: "${ADX_RELEASE_TARGET:?set artifact target triple}"
: "${ADX_RELEASE_OUTPUT:?set new output package directory}"
JOBS=${JOBS:-2}
PYTHON=${PYTHON:-python3}
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
host=$(rustc -vV | sed -n 's/^host: //p')
[[ "$host" == "$ADX_RELEASE_TARGET" ]] || { echo 'release target must match the native builder' >&2; exit 1; }
case "$host" in
 x86_64-unknown-linux-gnu) export GOOS=linux GOARCH=amd64 ;;
 aarch64-unknown-linux-gnu) export GOOS=linux GOARCH=arm64 ;;
 aarch64-apple-darwin) export GOOS=darwin GOARCH=arm64 ;;
 *) echo 'unsupported native release target' >&2; exit 1 ;;
esac
stage=$(mktemp -d "${TMPDIR:-/tmp}/adx-build.XXXXXX")
trap 'rm -rf "$stage"' EXIT
cargo build --locked --release -j "$JOBS" -p adx-control-cli -p adx-master -p adx-node-manager -p data-plane-gateway -p rrt-daemon --bins
bash build/codegen/go.sh
(cd platform/control-plane/sandbox-api && go build -mod=readonly -p "$JOBS" -o "$stage/adx-sandbox-api" ./cmd/adx-sandbox-api)
for name in adxctl adx-master adx-node-manager adx-edge-frontend adx-node-proxy adx-data-plane-forward rrt-runtime; do
 cp "$CARGO_TARGET_DIR/release/$name" "$stage/$name"
done
PYTHON="$PYTHON" bash platform/sdk/sandbox/python/build.sh "$stage/sdk"
wheel=("$stage"/sdk/adx_sandbox-*.whl)
[[ ${#wheel[@]} == 1 && -f "${wheel[0]}" ]]
"$PYTHON" build/release/package.py assemble --binary-dir "$stage" --redis "$ADX_REDIS_SERVER" --wheel "${wheel[0]}" --target "$ADX_RELEASE_TARGET" --profile release --output "$ADX_RELEASE_OUTPUT"
