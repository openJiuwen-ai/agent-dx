#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
: "${BUILDKITE_BUILD_ID:?Buildkite build ID required}"
: "${BUILDKITE_BUILD_URL:?Buildkite build URL required}"

arch=${ADX_BUILD_ARCH:-amd64}
step=publish-amd64
artifact_root=out/buildkite
index=out/buildkite/index.html
if [[ $arch == arm64 ]]; then
  export ADX_OBS_UPLOAD=${ADX_ARM_OBS_UPLOAD:-${ADX_OBS_UPLOAD:-1}}
  step=publish-arm64
  artifact_root=out/buildkite/arm64
  index=out/buildkite/index-arm64.html
fi
args=()
case "${ADX_OBS_UPLOAD:-1}" in
  1)
    buildkite-agent artifact download "$artifact_root/obs/manifest.json" . --step "$step"
    args=(--manifest "$artifact_root/obs/manifest.json")
    ;;
  0) ;;
  *) echo 'ADX_OBS_UPLOAD must be 0 or 1' >&2; exit 2 ;;
esac

python3 build/release/artifact_index.py \
  "${args[@]}" \
  --output "$index" \
  --commit "$BUILDKITE_COMMIT" \
  --build-id "$BUILDKITE_BUILD_ID" \
  --build-url "$BUILDKITE_BUILD_URL"
echo "Artifact summary: $index"
