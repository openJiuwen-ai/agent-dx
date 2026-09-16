#!/usr/bin/env bash
set -euo pipefail
: "${ADX_E2E_IMAGE_REPOSITORY:?set the registry repository}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]
mkdir -p out/buildkite/logs
buildkite-agent artifact download 'out/buildkite/summaries/release.json' . --step platform-build
echo "--- :package: Verify release artifact handoff"
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
echo "--- :docker: Resolve runtime base images"
source .buildkite/bootstrap-images.sh > >(tee out/buildkite/logs/base-images.log) 2>&1
echo "--- :docker: Resolve pinned external Collector"
if [[ -z ${ADX_COLLECTOR_IMAGE:-} ]]; then
  collector_digest=$(python3 -c 'import json; print(json.load(open("build/observability/source.json"))["image"].split("@",1)[1])')
  export ADX_COLLECTOR_IMAGE="${ADX_COLLECTOR_MIRROR:-docker.m.daocloud.io/otel/opentelemetry-collector-contrib}@$collector_digest"
fi
docker pull "$ADX_COLLECTOR_IMAGE"
echo "--- :docker: Build node and RRT images"
fc_args=()
if [[ ${ADX_E2E_CHECKPOINT:-0} == 1 ]]; then
  if [[ -n ${ADX_FC_KIT_ARTIFACT_BUILD:-} ]]; then
    buildkite-agent artifact download 'out/buildkite/firecracker-kit/**/*' . --build "$ADX_FC_KIT_ARTIFACT_BUILD"
    export ADX_FC_KIT_DIR="$PWD/out/buildkite/firecracker-kit"
  fi
  : "${ADX_FC_KIT_DIR:?checkpoint profile requires a verified native Firecracker kit directory or artifact build}"
  fc_args=(--firecracker-kit "$ADX_FC_KIT_DIR")
fi
python3 -u build/e2e/prepare.py --package out/buildkite/package --backend out/buildkite/backend --runtime-base "$ADX_E2E_RUNTIME_BASE" --rrt-base "$ADX_E2E_RRT_BASE" --output out/buildkite/bundle "${fc_args[@]}" 2>&1 | tee out/buildkite/logs/prepare.log
echo "--- :docker: Push immutable image references"
python3 -u build/e2e/kubernetes/publish_images.py --bundle out/buildkite/bundle --repository "$ADX_E2E_IMAGE_REPOSITORY" 2>&1 | tee out/buildkite/logs/publish-images.log
