#!/usr/bin/env bash
set -euo pipefail
: "${ADX_E2E_IMAGE_REPOSITORY:?set the registry repository}"
: "${ADX_BASE_PACKAGE_BUILD_ID:?set the immutable base-package Buildkite build UUID}"
: "${ADX_SDK_BUILD_ID:?set the immutable Python SDK Buildkite build UUID}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]
mkdir -p out/buildkite/logs
echo "--- :package: Verify release artifact handoff"
case "${ADX_BASE_ARTIFACT_TRANSPORT:-buildkite}" in
  obs)
    source .buildkite/obs-python.sh
    BUILDKITE_BUILD_ID="$ADX_BASE_PACKAGE_BUILD_ID" "$OBS_PYTHON" build/release/ci_transfer.py download release out/buildkite
    ;;
  buildkite)
    for artifact in adx-release.tar.gz adx-release.tar.gz.sha256 build-manifest.json backend.tar.gz; do
      buildkite-agent artifact download "out/buildkite/$artifact" . --step platform-build --build "$ADX_BASE_PACKAGE_BUILD_ID"
    done
    ;;
  *) echo 'ADX_BASE_ARTIFACT_TRANSPORT must be obs or buildkite' >&2; exit 2 ;;
esac
(cd out/buildkite && sha256sum --check adx-release.tar.gz.sha256)
mkdir -p out/buildkite/package
tar -xzf out/buildkite/adx-release.tar.gz -C out/buildkite/package
mkdir -p out/buildkite/backend
tar -xzf out/buildkite/backend.tar.gz -C out/buildkite/backend
python3 build/e2e/verify_backend.py \
  --directory out/buildkite/backend \
  --target x86_64-unknown-linux-gnu
base_wheels=(out/buildkite/package/sdk/adx_sandbox-*.whl)
[[ ${#base_wheels[@]} == 1 && -f ${base_wheels[0]} ]] || { echo 'base package SDK wheel is missing' >&2; exit 1; }
python3 build/release/component.py verify-build \
  --manifest out/buildkite/build-manifest.json \
  --commit "$BUILDKITE_COMMIT" \
  --target x86_64-unknown-linux-gnu \
  --package-manifest out/buildkite/package/manifest.json \
  --release-archive out/buildkite/adx-release.tar.gz \
  --wheel "${base_wheels[0]}" \
  --backend-manifest out/buildkite/backend/manifest.json \
  --backend-archive out/buildkite/backend.tar.gz
echo "--- :python: Verify independent Sandbox SDK handoff"
buildkite-agent artifact download 'out/buildkite/sdk/sdk-candidate.json' . --step sdk-package --build "$ADX_SDK_BUILD_ID"
buildkite-agent artifact download 'out/buildkite/sdk/adx_sandbox-*.whl' . --step sdk-package --build "$ADX_SDK_BUILD_ID"
sdk_wheels=(out/buildkite/sdk/adx_sandbox-*.whl)
[[ ${#sdk_wheels[@]} == 1 && -f ${sdk_wheels[0]} ]] || { echo 'exactly one SDK wheel is required' >&2; exit 1; }
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
  if [[ -n ${ADX_COLLECTOR_MIRROR:-} ]]; then
    collector_digest=$(python3 -c 'import json; print(json.load(open("build/observability/source.json"))["image"].split("@",1)[1])')
    export ADX_COLLECTOR_IMAGE="$ADX_COLLECTOR_MIRROR@$collector_digest"
  else
    export ADX_COLLECTOR_IMAGE=$(python3 -c 'import json; print(json.load(open("build/observability/source.json"))["ci_image"])')
  fi
fi
docker pull "$ADX_COLLECTOR_IMAGE"
echo "--- :docker: Build node, EXECD and entrypoint fixture images"
fc_args=()
runtime_args=()
if [[ -n ${ADX_E2E_RUNSC_BIN:-} ]]; then
  : "${ADX_E2E_RUNSC_SHA256:?a pinned runsc SHA256 is required}"
  printf '%s  %s\n' "$ADX_E2E_RUNSC_SHA256" "$ADX_E2E_RUNSC_BIN" | sha256sum --check -
  runtime_args=(--runsc-bin "$ADX_E2E_RUNSC_BIN")
fi
if [[ ${ADX_E2E_CHECKPOINT:-0} == 1 ]]; then
  if [[ -n ${ADX_FC_KIT_ARTIFACT_BUILD:-} ]]; then
    buildkite-agent artifact download 'out/buildkite/firecracker-kit/**/*' . --build "$ADX_FC_KIT_ARTIFACT_BUILD"
    export ADX_FC_KIT_DIR="$PWD/out/buildkite/firecracker-kit"
  fi
  : "${ADX_FC_KIT_DIR:?checkpoint profile requires a verified native Firecracker kit directory or artifact build}"
  fc_args=(--firecracker-kit "$ADX_FC_KIT_DIR")
fi
python3 -u build/e2e/prepare.py --package out/buildkite/package --backend out/buildkite/backend \
  --sdk-wheel "${sdk_wheels[0]}" --sdk-candidate out/buildkite/sdk/sdk-candidate.json \
  --runtime-base "$ADX_E2E_RUNTIME_BASE" --execd-base "$ADX_E2E_EXECD_BASE" \
  --output out/buildkite/bundle "${fc_args[@]}" "${runtime_args[@]}" 2>&1 | tee out/buildkite/logs/prepare.log
echo "--- :docker: Push immutable image references"
python3 -u build/e2e/kubernetes/publish_images.py --bundle out/buildkite/bundle --repository "$ADX_E2E_IMAGE_REPOSITORY" 2>&1 | tee out/buildkite/logs/publish-images.log
