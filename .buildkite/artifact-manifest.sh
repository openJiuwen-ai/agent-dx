#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
: "${BUILDKITE_BUILD_ID:?Buildkite build ID required}"
: "${BUILDKITE_BUILD_URL:?Buildkite build URL required}"

args=()
case "${ADX_OBS_UPLOAD:-1}" in
  1)
    buildkite-agent artifact download 'out/buildkite/obs/manifest.json' . --step platform-build
    args=(--manifest out/buildkite/obs/manifest.json)
    ;;
  0) ;;
  *) echo 'ADX_OBS_UPLOAD must be 0 or 1' >&2; exit 2 ;;
esac

python3 build/release/artifact_index.py \
  "${args[@]}" \
  --output out/buildkite/index.html \
  --commit "$BUILDKITE_COMMIT" \
  --build-id "$BUILDKITE_BUILD_ID" \
  --build-url "$BUILDKITE_BUILD_URL"
echo 'Artifact summary: out/buildkite/index.html'
