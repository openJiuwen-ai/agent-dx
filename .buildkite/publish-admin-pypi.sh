#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
: "${BUILDKITE_BUILD_ID:?Buildkite build ID required}"
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
output=out/buildkite/admin
publish_output=out/buildkite/admin-publish
logs=out/buildkite/logs
venv=${TMPDIR:-/tmp}/adxadmin-publish-${BUILDKITE_BUILD_ID}
python=${PYTHON:-python3}
rm -rf "$output" "$publish_output" "$venv"
mkdir -p "$output" "$publish_output" "$logs"

buildkite-agent artifact download 'out/buildkite/admin/*' . --step admin-package
"$python" -c 'import sys; assert sys.version_info >= (3, 10), "adxadmin publish requires Python 3.10+"'
"$python" build/admin/candidate.py --verify --directory "$output" >/dev/null
candidate_commit=$("$python" -c 'import json; print(json.load(open("out/buildkite/admin/admin-candidate.json"))["commit"])')
candidate_build=$("$python" -c 'import json; print(json.load(open("out/buildkite/admin/admin-candidate.json"))["build_id"])')
version=$("$python" -c 'import json; print(json.load(open("out/buildkite/admin/admin-candidate.json"))["version"])')
[[ "$candidate_commit" == "$BUILDKITE_COMMIT" ]] || { echo 'admin candidate commit differs from build' >&2; exit 1; }
[[ "$candidate_build" == "$BUILDKITE_BUILD_ID" ]] || { echo 'admin candidate build ID differs from build' >&2; exit 1; }

tag=${BUILDKITE_TAG:-}
tag=${tag#refs/tags/}
[[ "$tag" == "adxadmin-v${version}" ]] || { echo "publishing requires tag adxadmin-v${version}" >&2; exit 1; }

repository=${ADX_ADMIN_PYPI_REPOSITORY:-pypi}
case "$repository" in
  pypi)
    token=${ADX_ADMIN_PYPI_TOKEN:-}
    upload_url=https://upload.pypi.org/legacy/
    ;;
  testpypi)
    token=${ADX_ADMIN_TEST_PYPI_TOKEN:-}
    upload_url=https://test.pypi.org/legacy/
    ;;
  *)
    echo 'ADX_ADMIN_PYPI_REPOSITORY must be pypi or testpypi' >&2
    exit 1
    ;;
esac
[[ -n "$token" ]] || { echo "${repository} API token is not configured" >&2; exit 1; }

"$python" -m venv "$venv"
"$venv/bin/python" -m pip install --disable-pip-version-check \
  --cache-dir "${PIP_CACHE_DIR:-/tmp/adx-pip-cache}" 'twine==6.2.0' \
  > "$logs/admin-publish-bootstrap.log" 2>&1
"$venv/bin/python" -m twine check "$output"/*.whl "$output"/*.tar.gz

export TWINE_USERNAME=__token__
export TWINE_PASSWORD="$token"
export TWINE_NON_INTERACTIVE=1
"$venv/bin/python" -m twine upload --non-interactive --repository-url "$upload_url" \
  "$output"/*.whl "$output"/*.tar.gz
unset TWINE_PASSWORD token

"$python" build/python/verify_index.py \
  --candidate "$output/admin-candidate.json" \
  --repository "$repository" \
  --output "$publish_output/publish.json"
