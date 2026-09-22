#!/usr/bin/env bash
set -euo pipefail

config=build/images/build-environment.json
readarray -t values < <(python3 - "$config" <<'PY'
import json, sys
config = json.load(open(sys.argv[1]))
print(config['ci_image'])
print(config['platform'])
PY
)
image=${values[0]}
platform=${values[1]}
[[ $image == *@sha256:* ]]
[[ -n ${1:-} ]] || set -- bash

exec docker run --rm -it --platform "$platform" \
  -v "$PWD:/workspace" -w /workspace \
  -v adx-cargo-home:/mnt/paas/build-cache/adx/cargo-home \
  -v adx-cargo-target:/mnt/paas/build-cache/adx/cargo-target \
  -v adx-go-build:/mnt/paas/build-cache/adx/go-build \
  -v adx-go-mod:/mnt/paas/build-cache/adx/go-mod \
  -e ADX_CARGO_HOME=/mnt/paas/build-cache/adx/cargo-home \
  -e CARGO_TARGET_DIR=/mnt/paas/build-cache/adx/cargo-target/amd64-rust1950 \
  -e GOCACHE=/mnt/paas/build-cache/adx/go-build/amd64 \
  -e GOMODCACHE=/mnt/paas/build-cache/adx/go-mod \
  "$image" "$@"
