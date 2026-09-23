#!/usr/bin/env bash
set -euo pipefail
case "${ADX_SDK_PYPI_UPLOAD:-0}" in
  0) echo 'PyPI publication disabled (ADX_SDK_PYPI_UPLOAD=0)'; exit 0 ;;
  1) ;;
  *) echo 'ADX_SDK_PYPI_UPLOAD must be 0 or 1' >&2; exit 2 ;;
esac

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
: "${BUILDKITE_BUILD_ID:?Buildkite build ID required}"
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
output=out/buildkite/sdk
publish_output=out/buildkite/sdk-publish
logs=out/buildkite/logs
venv=${TMPDIR:-/tmp}/adx-sdk-publish-${BUILDKITE_BUILD_ID}
python=${PYTHON:-python3}
rm -rf "$output" "$publish_output" "$venv"
mkdir -p "$output" "$publish_output" "$logs"

buildkite-agent artifact download 'out/buildkite/sdk/*' . --step sdk-package
"$python" -c 'import sys; assert sys.version_info >= (3, 10), "SDK publish requires Python 3.10+"'
"$python" build/sdk/candidate.py --verify --directory "$output" >/dev/null
candidate_commit=$("$python" -c 'import json; print(json.load(open("out/buildkite/sdk/sdk-candidate.json"))["commit"])')
candidate_build=$("$python" -c 'import json; print(json.load(open("out/buildkite/sdk/sdk-candidate.json"))["build_id"])')
version=$("$python" -c 'import json; print(json.load(open("out/buildkite/sdk/sdk-candidate.json"))["version"])')
[[ "$candidate_commit" == "$BUILDKITE_COMMIT" ]] || { echo 'SDK candidate commit differs from build' >&2; exit 1; }
[[ "$candidate_build" == "$BUILDKITE_BUILD_ID" ]] || { echo 'SDK candidate build ID differs from build' >&2; exit 1; }

tag=${BUILDKITE_TAG:-}
tag=${tag#refs/tags/}
[[ "$tag" == "sdk-v${version}" ]] || { echo "publishing requires tag sdk-v${version}" >&2; exit 1; }

repository=${ADX_SDK_PYPI_REPOSITORY:-pypi}
case "$repository" in
  pypi)
    token=${ADX_SANDBOX_PYPI_TOKEN:-}
    upload_url=https://upload.pypi.org/legacy/
    ;;
  testpypi)
    token=${ADX_SANDBOX_TEST_PYPI_TOKEN:-}
    upload_url=https://test.pypi.org/legacy/
    ;;
  *)
    echo 'ADX_SDK_PYPI_REPOSITORY must be pypi or testpypi' >&2
    exit 1
    ;;
esac
[[ -n "$token" ]] || { echo "adx-sandbox ${repository} API token is not configured" >&2; exit 1; }

"$python" -m venv "$venv"
"$venv/bin/python" -m pip install --disable-pip-version-check \
  --cache-dir "${PIP_CACHE_DIR:-/tmp/adx-pip-cache}" 'twine==6.2.0' \
  > "$logs/sdk-publish-bootstrap.log" 2>&1
"$venv/bin/python" -m twine check "$output"/*.whl "$output"/*.tar.gz

export TWINE_USERNAME=__token__
export TWINE_PASSWORD="$token"
export TWINE_NON_INTERACTIVE=1
"$venv/bin/python" -m twine upload --non-interactive --repository-url "$upload_url" \
  "$output"/*.whl "$output"/*.tar.gz
unset TWINE_PASSWORD token

"$python" build/python/verify_index.py \
  --candidate "$output/sdk-candidate.json" \
  --repository "$repository" \
  --output "$publish_output/publish.json"
