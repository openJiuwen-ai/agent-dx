#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
: "${BUILDKITE_BUILD_ID:?Buildkite build ID required}"

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
output=out/buildkite/obs
rm -rf "$output" out/buildkite/backend
mkdir -p "$output" out/buildkite/logs

exec > >(tee out/buildkite/logs/step-obs.log) 2>&1

echo "--- :arrow_down: Download verified build artifacts"
buildkite-agent artifact download 'out/buildkite/adx-release.tar.gz' . --step platform-build
buildkite-agent artifact download 'out/buildkite/adx-release.tar.gz.sha256' . --step platform-build
buildkite-agent artifact download 'out/buildkite/release-manifest.json' . --step platform-build
buildkite-agent artifact download 'out/buildkite/backend/*' . --step platform-build

(cd out/buildkite && sha256sum --check adx-release.tar.gz.sha256)
python=${OBS_PYTHON:-/opt/buildtools/python3.9/bin/python3}
if [[ ! -x "$python" ]]; then python=${PYTHON:-python3}; fi
verify_dir=$(mktemp -d out/buildkite/obs-package-check.XXXXXX)
trap 'rm -rf "$verify_dir"' EXIT
tar -xzf out/buildkite/adx-release.tar.gz -C "$verify_dir"
"$python" build/release/package.py verify "$verify_dir"
rm -rf "$verify_dir"
"$python" build/e2e/verify_backend.py \
  --directory out/buildkite/backend \
  --target x86_64-unknown-linux-gnu

commit_short=${BUILDKITE_COMMIT:0:12}
runtime_archive="$output/adx-runtime-runc-${commit_short}-linux-amd64.tar.gz"
tar -czf "$runtime_archive" -C out/buildkite/backend .
sha256sum "$runtime_archive" > "$runtime_archive.sha256"

channel=${ADX_OBS_UPLOAD_CHANNEL:-daily}
version_args=()
if [[ $channel == release ]]; then
  version=${ADX_RELEASE_VERSION:-${BUILDKITE_TAG:-}}
  version=${version#refs/tags/}
  version=${version#v}
  [[ -n $version ]] || { echo 'ADX_RELEASE_VERSION or BUILDKITE_TAG is required for release upload' >&2; exit 1; }
  version_args=(--version "$version")
fi
timestamp=${ADX_OBS_UPLOAD_TIMESTAMP:-$(date -u '+%Y%m%d%H%M%S')}

artifacts=(
  out/buildkite/adx-release.tar.gz
  out/buildkite/adx-release.tar.gz.sha256
  out/buildkite/release-manifest.json
  "$runtime_archive"
  "$runtime_archive.sha256"
)
[[ ${#artifacts[@]} -eq 5 ]] || { echo 'base package artifact set is incomplete' >&2; exit 1; }

echo "--- :cloud: Upload ADX artifacts to Huawei Cloud OBS"
"$python" -c 'from obs import ObsClient' >/dev/null 2>&1 || {
  echo "OBS Python SDK is unavailable in $python" >&2
  exit 1
}
"$python" build/release/obs_upload.py \
  --output "$output/manifest.json" \
  --channel "$channel" \
  "${version_args[@]}" \
  --platform linux \
  --arch amd64 \
  --timestamp "$timestamp" \
  --commit "$BUILDKITE_COMMIT" \
  --build-id "$BUILDKITE_BUILD_ID" \
  "${artifacts[@]}"

"$python" - "$output/manifest.json" <<'PY'
import json, pathlib, sys
manifest = json.loads(pathlib.Path(sys.argv[1]).read_text())
pathlib.Path(sys.argv[1]).with_name('urls.txt').write_text(
    ''.join(f"{item['name']}\t{item['url']}\n" for item in manifest['artifacts'])
    + f"manifest.json\t{manifest['manifest_url']}\n"
)
PY

buildkite-agent meta-data set obs-manifest-url "$("$python" -c 'import json; print(json.load(open("out/buildkite/obs/manifest.json"))["manifest_url"])')"
echo "OBS manifest: $(tail -n 1 "$output/urls.txt" | cut -f2-)"
