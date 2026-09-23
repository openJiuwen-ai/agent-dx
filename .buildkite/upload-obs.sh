#!/usr/bin/env bash
set -euo pipefail
case "${ADX_OBS_UPLOAD:-1}" in
  0) echo 'OBS publication disabled (ADX_OBS_UPLOAD=0)'; exit 0 ;;
  1) ;;
  *) echo 'ADX_OBS_UPLOAD must be 0 or 1' >&2; exit 2 ;;
esac

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
: "${BUILDKITE_BUILD_ID:?Buildkite build ID required}"

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
output=out/buildkite/obs
rm -rf "$output"
mkdir -p "$output" out/buildkite/logs

exec > >(tee out/buildkite/logs/step-obs.log) 2>&1

(cd out/buildkite && sha256sum --check adx-release.tar.gz.sha256)
python=${OBS_PYTHON:-/opt/buildtools/python3.9/bin/python3}
if [[ ! -x "$python" ]]; then python=${PYTHON:-python3}; fi
verify_dir=$(mktemp -d out/buildkite/obs-package-check.XXXXXX)
trap 'rm -rf "$verify_dir"' EXIT
tar -xzf out/buildkite/adx-release.tar.gz -C "$verify_dir"
"$python" build/release/package.py verify "$verify_dir"
mkdir -p out/buildkite/backend
tar -xzf out/buildkite/backend.tar.gz -C out/buildkite/backend
"$python" build/e2e/verify_backend.py \
  --directory out/buildkite/backend \
  --target x86_64-unknown-linux-gnu
base_wheels=("$verify_dir"/sdk/adx_sandbox-*.whl)
[[ ${#base_wheels[@]} == 1 && -f ${base_wheels[0]} ]] || { echo 'base package SDK wheel is missing' >&2; exit 1; }
"$python" build/release/component.py verify-build \
  --manifest out/buildkite/build-manifest.json \
  --commit "$BUILDKITE_COMMIT" \
  --target x86_64-unknown-linux-gnu \
  --package-manifest "$verify_dir/manifest.json" \
  --release-archive out/buildkite/adx-release.tar.gz \
  --wheel "${base_wheels[0]}" \
  --backend-manifest out/buildkite/backend/manifest.json \
  --backend-archive out/buildkite/backend.tar.gz
rm -rf "$verify_dir"

commit_short=${BUILDKITE_COMMIT:0:12}
runtime_archive="$output/adx-runtime-runc-${commit_short}-linux-amd64.tar.gz"
cp out/buildkite/backend.tar.gz "$runtime_archive"
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
  out/buildkite/adx-execd.tar.gz
  out/buildkite/adx-execd.tar.gz.sha256
  out/buildkite/adx-release.tar.gz
  out/buildkite/adx-release.tar.gz.sha256
  out/buildkite/release-manifest.json
  out/buildkite/build-manifest.json
  "$runtime_archive"
  "$runtime_archive.sha256"
)
python3 build/admin/candidate.py --verify --directory out/buildkite/admin
artifacts+=(out/buildkite/sdk/*.whl out/buildkite/sdk/*.tar.gz out/buildkite/sdk/sdk-candidate.json)
artifacts+=(out/buildkite/admin/*.whl out/buildkite/admin/*.tar.gz out/buildkite/admin/admin-candidate.json)

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
