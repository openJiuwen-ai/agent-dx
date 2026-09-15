#!/usr/bin/env bash
# Sourced by build-e2e.sh; prepare ADX dependencies in the existing CI worker.
set -euo pipefail
cache=${ADX_TOOL_CACHE:-/mnt/paas/build-cache/adx/tools/amd64}
mkdir -p "$cache" out/buildkite/logs
export PATH="$cache/go/bin:$cache/bin:/root/.cargo/bin:/opt/buildtools/protoc/bin:$PATH"
export GOROOT="$cache/go"
export GOTOOLCHAIN=local
export GOPROXY=${GOPROXY:-https://goproxy.cn,direct}
export GOBIN="$cache/bin"
exec 9>"$cache/bootstrap.lock"
flock 9
fetch() {
  local url=$1 checksum=$2 dest=$3
  if ! echo "$checksum  $dest" | sha256sum --check --status 2>/dev/null; then
    curl --fail --location --retry 3 --connect-timeout 20 --max-time 600 "$url" -o "$dest.part"
    echo "$checksum  $dest.part" | sha256sum --check
    mv "$dest.part" "$dest"
  fi
}
if [[ ! -x "$cache/go/bin/go" ]] || [[ $("$cache/go/bin/go" version) != *go1.25.5* ]]; then
  fetch https://go.dev/dl/go1.25.5.linux-amd64.tar.gz 9e9b755d63b36acf30c12a9a3fc379243714c1c6d3dd72861da637f336ebb35b "$cache/go1.25.5.tar.gz"
  tar -xzf "$cache/go1.25.5.tar.gz" -C "$cache"
fi
if [[ -z ${ADX_REDIS_SERVER:-} || -z ${ADX_REDIS_CLI:-} ]]; then
  if [[ ! -f "$cache/redis-7.2.5/.adx-libc-plain" || ! -x "$cache/redis-7.2.5/src/redis-server" || ! -x "$cache/redis-7.2.5/src/redis-cli" ]]; then
    fetch https://download.redis.io/releases/redis-7.2.5.tar.gz 5981179706f8391f03be91d951acafaeda91af7fac56beffb2701963103e423d "$cache/redis-7.2.5.tar.gz"
    tar -xzf "$cache/redis-7.2.5.tar.gz" -C "$cache"
    make -C "$cache/redis-7.2.5" distclean
    make -C "$cache/redis-7.2.5" -j "${JOBS:-2}" MALLOC=libc BUILD_TLS=no REDIS_CFLAGS= REDIS_LDFLAGS= redis-server redis-cli
    touch "$cache/redis-7.2.5/.adx-libc-plain"
  fi
  export ADX_REDIS_SERVER="$cache/redis-7.2.5/src/redis-server"
  export ADX_REDIS_CLI="$cache/redis-7.2.5/src/redis-cli"
fi
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
if [[ ! -x "$cache/python/bin/python3" ]]; then
  python3 -m venv "$cache/python"
fi
export PATH="$cache/python/bin:$PATH"
export PYTHON="$cache/python/bin/python3"
python3 -m pip install 'PyYAML==6.0.2' 'build==1.2.2.post1'
flock -u 9
rustc --version
cargo --version
go version
protoc --version
"$ADX_REDIS_SERVER" --version
