#!/usr/bin/env bash
set -euo pipefail
mkdir -p out/buildkite/logs
buildkite-agent artifact download 'out/buildkite/bundle/*.json' . --step platform-images --build 01a195f3-ed61-4ad7-8a86-c612f7701be1
daemon_pid=''
cleanup() {
  if [[ -n "$daemon_pid" ]]; then kill -TERM "$daemon_pid" 2>/dev/null || true; wait "$daemon_pid" 2>/dev/null || true; fi
}
trap cleanup EXIT
if ! docker info >/dev/null 2>&1; then
  dockerd --host=unix:///var/run/docker.sock --storage-driver=overlay2 > out/buildkite/logs/dockerd.log 2>&1 &
  daemon_pid=$!
  for _ in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then break; fi
    kill -0 "$daemon_pid"
    sleep 1
  done
  docker info >/dev/null
fi
python3 .buildkite/with_registry.py --docker -- python3 build/e2e/export_images.py \
  --bundle out/buildkite/bundle/bundle.json \
  --registry-images out/buildkite/bundle/registry-images.json \
  --output out/buildkite/offline-images
