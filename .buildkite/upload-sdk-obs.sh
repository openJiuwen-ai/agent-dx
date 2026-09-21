#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
: "${BUILDKITE_BUILD_ID:?Buildkite build ID required}"
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
rm -rf out/buildkite/sdk out/buildkite/sdk-obs
mkdir -p out/buildkite/sdk out/buildkite/sdk-obs out/buildkite/logs
exec > >(tee out/buildkite/logs/step-sdk-obs.log) 2>&1

buildkite-agent artifact download 'out/buildkite/sdk/*' . --step sdk-package
python=${OBS_PYTHON:-/opt/buildtools/python3.9/bin/python3}
[[ -x "$python" ]] || python=${PYTHON:-python3}
"$python" build/sdk/candidate.py --verify --directory out/buildkite/sdk
candidate_commit=$("$python" -c 'import json; print(json.load(open("out/buildkite/sdk/sdk-candidate.json"))["commit"])')
[[ "$candidate_commit" == "$BUILDKITE_COMMIT" ]] || { echo 'SDK candidate commit differs from build' >&2; exit 1; }

channel=${ADX_OBS_UPLOAD_CHANNEL:-daily}
version_args=()
if [[ $channel == release ]]; then
  version=${ADX_RELEASE_VERSION:-${BUILDKITE_TAG:-}}
  version=${version#refs/tags/}; version=${version#sdk-v}; version=${version#v}
  [[ -n $version ]] || { echo 'SDK release upload requires a version' >&2; exit 1; }
  version_args=(--version "$version")
fi
timestamp=${ADX_OBS_UPLOAD_TIMESTAMP:-$(date -u '+%Y%m%d%H%M%S')}
artifacts=(out/buildkite/sdk/sdk-candidate.json)
while IFS= read -r path; do artifacts+=("$path"); done < <(find out/buildkite/sdk -maxdepth 1 -type f \( -name '*.whl' -o -name '*.tar.gz' \) -print | sort)
[[ ${#artifacts[@]} == 3 ]] || { echo 'SDK wheel/source/candidate set is incomplete' >&2; exit 1; }

"$python" -c 'from obs import ObsClient' >/dev/null
"$python" build/release/obs_upload.py \
  --output out/buildkite/sdk-obs/manifest.json --channel "$channel" "${version_args[@]}" \
  --platform python --arch any --timestamp "$timestamp" --commit "$BUILDKITE_COMMIT" \
  --build-id "$BUILDKITE_BUILD_ID" "${artifacts[@]}"
