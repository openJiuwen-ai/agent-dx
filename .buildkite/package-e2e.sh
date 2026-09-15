#!/usr/bin/env bash
set -euo pipefail
: "${ADX_E2E_IMAGE_REPOSITORY:?set the registry repository}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]
mkdir -p out/buildkite/logs
buildkite-agent artifact download 'out/buildkite/adx-release.tar.gz' . --step platform-build
buildkite-agent artifact download 'out/buildkite/adx-release.tar.gz.sha256' . --step platform-build
(cd out/buildkite && sha256sum --check adx-release.tar.gz.sha256)
mkdir -p out/buildkite/package
tar -xzf out/buildkite/adx-release.tar.gz -C out/buildkite/package
buildkite-agent artifact download 'out/buildkite/backend/*' . --step platform-build
daemon_pid=''
cleanup() {
  if [[ -n "$daemon_pid" ]]; then
    kill -TERM "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT
if ! docker info >/dev/null 2>&1; then
  dockerd --host="${DOCKER_HOST:-unix:///var/run/docker.sock}" --storage-driver="${DOCKER_DRIVER:-overlay2}" > out/buildkite/logs/dockerd.log 2>&1 &
  daemon_pid=$!
  for _ in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then break; fi
    kill -0 "$daemon_pid" 2>/dev/null || { echo 'Docker daemon exited; see dockerd.log'; exit 1; }
    sleep 1
  done
  docker info >/dev/null
fi
source .buildkite/bootstrap-images.sh > out/buildkite/logs/base-images.log 2>&1
python3 build/e2e/prepare.py --package out/buildkite/package --backend out/buildkite/backend --runtime-base "$ADX_E2E_RUNTIME_BASE" --rrt-base "$ADX_E2E_RRT_BASE" --output out/buildkite/bundle > out/buildkite/logs/prepare.log 2>&1
python3 build/e2e/kubernetes/publish_images.py --bundle out/buildkite/bundle --repository "$ADX_E2E_IMAGE_REPOSITORY" > out/buildkite/logs/publish-images.log 2>&1
