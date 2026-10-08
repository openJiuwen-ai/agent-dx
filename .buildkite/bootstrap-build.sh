#!/usr/bin/env bash
# Sourced by build-e2e.sh. The digest-pinned build image owns all tools;
# bootstrap only binds persistent caches and verifies image identity.
set -euo pipefail

export GOROOT=/usr/local/go
export GOTOOLCHAIN=local
export PATH="/opt/adx-build-tools/python/bin:/opt/adx-build-tools/erofs-utils-1.8.10/bin:/usr/local/go/bin:/root/.cargo/bin:/opt/buildtools/protoc/bin:$PATH"
export GOPROXY=${GOPROXY:-https://goproxy.cn,direct}
export GOCACHE=${GOCACHE:-/mnt/paas/build-cache/adx/go-build/amd64}
export GOMODCACHE=${GOMODCACHE:-/mnt/paas/build-cache/adx/go-mod}
export ADX_REDIS_SERVER=/usr/local/bin/redis-server
export ADX_REDIS_CLI=/usr/local/bin/redis-cli

mkdir -p out/buildkite/logs "$GOCACHE" "$GOMODCACHE"
source .buildkite/setup-cargo.sh
adx-verify-build-image

printf 'Go cache=%s\nGo module cache=%s\nRedis=%s\n' \
  "$GOCACHE" "$GOMODCACHE" "$($ADX_REDIS_SERVER --version)"
