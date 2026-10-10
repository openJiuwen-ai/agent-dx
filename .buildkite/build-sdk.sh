#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
: "${ADX_SDK_TEST_IMAGE:?set a digest-pinned Python 3.10+ test image}"
[[ "$ADX_SDK_TEST_IMAGE" == *@sha256:* ]] || { echo 'SDK test image must be digest pinned' >&2; exit 1; }
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required' >&2; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]
package=${1:-sdk}
[[ $package == sdk || $package == admin ]] || { echo "unknown Python package" >&2; exit 2; }
rm -rf "out/buildkite/$package"
mkdir -p out/buildkite/logs /mnt/paas/build-cache/adx/pip

daemon_pid=''
cleanup() {
  if [[ -n "$daemon_pid" ]]; then
    kill -TERM "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT
if ! docker info >/dev/null 2>&1; then
  dockerd --host="${DOCKER_HOST:-unix:///var/run/docker.sock}" --storage-driver="${DOCKER_DRIVER:-overlay2}" \
    > out/buildkite/logs/sdk-dockerd.log 2>&1 &
  daemon_pid=$!
  for _ in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then break; fi
    kill -0 "$daemon_pid" 2>/dev/null || { echo 'Docker daemon exited' >&2; exit 1; }
    sleep 1
  done
fi
docker info >/dev/null
docker pull "$ADX_SDK_TEST_IMAGE"
python_env=(-e PIP_CACHE_DIR=/root/.cache/pip)
for name in PIP_INDEX_URL PIP_EXTRA_INDEX_URL PIP_DEFAULT_TIMEOUT; do
  if [[ -n ${!name:-} ]]; then python_env+=(-e "$name"); fi
done
docker run --rm "${python_env[@]}" \
  -e BUILDKITE_COMMIT -e BUILDKITE_BUILD_ID \
  -v "$PWD:/workspace" -v /mnt/paas/build-cache/adx/pip:/root/.cache/pip \
  -w /workspace "$ADX_SDK_TEST_IMAGE" bash "build/$package/ci.sh"
