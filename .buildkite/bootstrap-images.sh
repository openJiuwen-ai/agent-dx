#!/usr/bin/env bash
# Sourced by the packaging job after Docker and private registry auth are ready.
set -euo pipefail
if [[ -z ${ADX_E2E_RUNTIME_BASE:-} ]]; then
  base_tag="${ADX_E2E_IMAGE_REPOSITORY}:runtime-${BUILDKITE_COMMIT:0:12}-${BUILDKITE_BUILD_NUMBER}"
  docker build --provenance=false -f build/images/Dockerfile.e2e-runtime -t "$base_tag" .
  docker push "$base_tag"
  ADX_E2E_RUNTIME_BASE=$(docker image inspect "$base_tag" --format '{{index .RepoDigests 0}}')
  [[ "$ADX_E2E_RUNTIME_BASE" == *@sha256:* ]]
fi
export ADX_E2E_RUNTIME_BASE
export ADX_E2E_RRT_BASE=${ADX_E2E_RRT_BASE:-$ADX_E2E_RUNTIME_BASE}
