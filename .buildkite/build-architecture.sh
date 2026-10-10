#!/usr/bin/env bash
# Sourced by native Linux builders; reject unsupported targets before any write.
case "${ADX_BUILD_ARCH:-amd64}" in
  amd64) export ADX_BUILD_ARCH=amd64 ADX_RELEASE_TARGET=x86_64-unknown-linux-gnu ADX_MUSL_TARGET=x86_64-unknown-linux-musl ;;
  arm64) export ADX_BUILD_ARCH=arm64 ADX_RELEASE_TARGET=aarch64-unknown-linux-gnu ADX_MUSL_TARGET=aarch64-unknown-linux-musl ;;
  *) echo 'ADX_BUILD_ARCH must be amd64 or arm64' >&2; return 2 2>/dev/null || exit 2 ;;
esac
