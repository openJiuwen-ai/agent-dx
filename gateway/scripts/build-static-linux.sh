#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
gateway_dir=$(cd "${script_dir}/.." && pwd)
repo_root=$(cd "${gateway_dir}/.." && pwd)

arch=${ADX_DATA_PLANE_ARCH:-}
if [ -z "$arch" ]; then
    case "$(uname -m)" in
        x86_64|amd64) arch=amd64 ;;
        arm64|aarch64) arch=arm64 ;;
        *) echo "unsupported build architecture: $(uname -m)" >&2; exit 1 ;;
    esac
fi

case "$arch" in
    amd64) target=x86_64-unknown-linux-musl ;;
    arm64) target=aarch64-unknown-linux-musl ;;
    *) echo "ADX_DATA_PLANE_ARCH must be amd64 or arm64" >&2; exit 2 ;;
esac

output_dir=${ADX_DATA_PLANE_OUTPUT_DIR:-${repo_root}/out/gateway/bin}
image=${ADX_DATA_PLANE_STATIC_BUILDER_IMAGE:-adx-data-plane-static-builder:1.97.1-${arch}}
platform=linux/${arch}

docker build \
    --platform "$platform" \
    --build-arg "RUST_TARGET=${target}" \
    -t "$image" \
    -f "${repo_root}/build/images/Dockerfile.gateway-static" \
    "${repo_root}/build/images"

mkdir -p "$output_dir"
docker run --rm --platform "$platform" \
    -v "${repo_root}:/workspace" \
    -v "${output_dir}:/out" \
    -v adx-data-plane-cargo-home:/cargo \
    -v "adx-data-plane-static-target-${arch}:/target" \
    -e CARGO_HOME=/cargo \
    -e CARGO_TARGET_DIR=/target \
    -e CC=musl-gcc \
    -e CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
    -e CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
    -w /workspace \
    "$image" \
    bash -euo pipefail -c '
        find /out -mindepth 1 -maxdepth 1 -type f -delete
        export RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }-C target-feature=+crt-static -C link-self-contained=yes -C relocation-model=static"
        cargo --config '\''source.crates-io.replace-with="rsproxy-sparse"'\'' \
            --config '\''source.rsproxy-sparse.registry="sparse+https://rsproxy.cn/index/"'\'' \
            build --locked -p data-plane-gateway --release --all-features --bins --target '"$target"'
        for binary in adx-node-proxy adx-edge-frontend adx-data-plane-forward; do
            strip "/target/'"$target"'/release/${binary}"
            install -m 0755 "/target/'"$target"'/release/${binary}" "/out/${binary}"
        done
        gateway/scripts/verify-static-linux.sh /out/adx-node-proxy /out/adx-edge-frontend /out/adx-data-plane-forward
    '

"${script_dir}/verify-static-linux.sh" \
    "${output_dir}/adx-node-proxy" \
    "${output_dir}/adx-edge-frontend" \
    "${output_dir}/adx-data-plane-forward"
