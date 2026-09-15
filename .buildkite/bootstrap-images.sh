#!/usr/bin/env bash
# Sourced after Docker and private registry auth are ready.
set -euo pipefail
if [[ -z ${ADX_E2E_RUNTIME_BASE:-} ]]; then
  # Bootstrap the SWR cache through a regional mirror, verifying the upstream ID.
  ubuntu_digest=sha256:a61567bd31828687156d735ea8eb01ba4e37636e225dd6a48ba94136a70d9d61
  ubuntu_id=sha256:b2b7ea366714195a1e1c5b2b578ece85c0b3920381a8654d038d9684f009613c
  ubuntu_tag="${ADX_E2E_IMAGE_REPOSITORY}:ubuntu2404-amd64-a61567bd31828687"
  if ! docker pull "$ubuntu_tag"; then
    mirror="${ADX_UBUNTU_MIRROR:-docker.m.daocloud.io/library/ubuntu}@$ubuntu_digest"
    docker pull "$mirror"
    [[ $(docker image inspect "$mirror" --format '{{.Id}}') == "$ubuntu_id" ]]
    docker tag "$mirror" "$ubuntu_tag"
    docker push "$ubuntu_tag"
  fi
  [[ $(docker image inspect "$ubuntu_tag" --format '{{.Id}}') == "$ubuntu_id" ]]
  ubuntu_base=$(docker image inspect "$ubuntu_tag" --format '{{index .RepoDigests 0}}')
  recipe=$(sha256sum build/images/Dockerfile.e2e-runtime .buildkite/bootstrap-images.sh | sha256sum | cut -c1-20)
  base_tag="${ADX_E2E_IMAGE_REPOSITORY}:runtime-amd64-$recipe"
  if ! docker pull "$base_tag"; then
    docker build --provenance=false --build-arg "BASE=$ubuntu_base" -f build/images/Dockerfile.e2e-runtime -t "$base_tag" build/images
    docker push "$base_tag"
  fi
  ADX_E2E_RUNTIME_BASE=$(docker image inspect "$base_tag" --format '{{index .RepoDigests 0}}')
  [[ "$ADX_E2E_RUNTIME_BASE" == *@sha256:* ]]
fi
export ADX_E2E_RUNTIME_BASE
export ADX_E2E_RRT_BASE=${ADX_E2E_RRT_BASE:-$ADX_E2E_RUNTIME_BASE}
