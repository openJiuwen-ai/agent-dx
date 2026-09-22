#!/usr/bin/env bash
# Sourced before any Cargo invocation in the shared Rust worker.
set -euo pipefail
# Some shared worker profile scripts reset CARGO_HOME after step env injection.
export CARGO_HOME=${ADX_CARGO_HOME:-/mnt/paas/build-cache/adx/cargo-home}
: "${CARGO_TARGET_DIR:?persistent Cargo compilation cache required}"
export RUSTUP_TOOLCHAIN=${RUSTUP_TOOLCHAIN:-stable}
export RUSTUP_AUTO_INSTALL=0
expected=$(sed -n 's/^channel = "\([^"]*\)"/\1/p' rust-toolchain.toml)
actual=$(rustc --version | awk '{print $2}')
[[ "$actual" == "$expected" ]] || { echo "Rust image version mismatch: expected $expected, found $actual" >&2; return 1; }
# Build images carry every required component. CI must fail on image drift
# instead of mutating the toolchain or reaching the network during bootstrap.
installed=$(rustup component list --installed --toolchain "$RUSTUP_TOOLCHAIN")
grep -q '^rustfmt-' <<<"$installed" || { echo 'Rust image is missing rustfmt' >&2; return 1; }
grep -q '^clippy-' <<<"$installed" || { echo 'Rust image is missing Clippy' >&2; return 1; }
mkdir -p "$CARGO_HOME" "$CARGO_TARGET_DIR"
# CARGO_HOME overrides otherwise hide the source configuration baked in the image.
cat > "$CARGO_HOME/config.toml.tmp.$$" <<'CONFIG'
[source.crates-io]
replace-with = "rsproxy-sparse"
[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"
[net]
git-fetch-with-cli = true
CONFIG
mv "$CARGO_HOME/config.toml.tmp.$$" "$CARGO_HOME/config.toml"
export CARGO_INCREMENTAL=0
export SCCACHE_DIR=${SCCACHE_DIR:-/mnt/paas/build-cache/adx/sccache}
export SCCACHE_CACHE_SIZE=${SCCACHE_CACHE_SIZE:-20G}
export SCCACHE_IDLE_TIMEOUT=0
wrapper=$(command -v sccache || true)
if [[ -z "$wrapper" && -x /mnt/paas/build-cache/bin/sccache ]]; then
  wrapper=/mnt/paas/build-cache/bin/sccache
fi
if [[ -n "$wrapper" ]]; then
  mkdir -p "$SCCACHE_DIR"
  export RUSTC_WRAPPER="$wrapper"
fi
printf 'Rust=%s Cargo source=rsproxy-sparse\nCARGO_HOME=%s\nCARGO_TARGET_DIR=%s\nRUSTC_WRAPPER=%s\n' "$actual" "$CARGO_HOME" "$CARGO_TARGET_DIR" "${RUSTC_WRAPPER:-none}"
