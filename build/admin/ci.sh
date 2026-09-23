#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
# The outer build-sdk.sh runner validates the clean, exact checkout.

output=out/buildkite/admin
logs=out/buildkite/logs
venv=${TMPDIR:-/tmp}/adxadmin-build-${BUILDKITE_BUILD_ID:-local}
install_venv=${TMPDIR:-/tmp}/adxadmin-install-${BUILDKITE_BUILD_ID:-local}
python=${PYTHON:-python3}
rm -rf "$output" "$venv" "$install_venv"
mkdir -p "$output" "$logs"

"$python" -c 'import sys; assert sys.version_info >= (3, 10), "adxadmin CI requires Python 3.10+"'
"$python" -m venv "$venv"
"$venv/bin/python" -m pip install --disable-pip-version-check \
  --cache-dir "${PIP_CACHE_DIR:-/tmp/adx-pip-cache}" \
  'build==1.4.4' 'twine==6.2.0' 'httpx==0.28.1' \
  'setuptools==82.0.1' 'wheel==0.48.0' \
  > "$logs/admin-bootstrap.log" 2>&1

"$venv/bin/python" -m unittest discover -s tools/admin/tests -p 'test_*.py' -v \
  2>&1 | tee "$logs/admin-tests.log"
"$venv/bin/python" -m unittest discover -s build/admin -p 'test_*.py' -v \
  2>&1 | tee "$logs/admin-release-tests.log"
"$venv/bin/python" -m unittest discover -s build/python -p 'test_*.py' -v \
  2>&1 | tee "$logs/python-release-tests.log"
"$venv/bin/python" -m build --no-isolation --wheel --sdist --outdir "$output" tools/admin \
  2>&1 | tee "$logs/admin-build.log"
"$venv/bin/python" -m twine check "$output"/*.whl "$output"/*.tar.gz \
  2>&1 | tee "$logs/admin-twine-check.log"
"$venv/bin/python" build/admin/candidate.py --directory "$output" \
  --commit "$BUILDKITE_COMMIT" --build-id "${BUILDKITE_BUILD_ID:-local}" \
  > "$logs/admin-candidate.log"

"$python" -m venv "$install_venv"
"$install_venv/bin/python" -m pip install --disable-pip-version-check \
  --cache-dir "${PIP_CACHE_DIR:-/tmp/adx-pip-cache}" "$output"/*.whl \
  > "$logs/admin-install.log" 2>&1
(
  cd "${TMPDIR:-/tmp}"
  "$install_venv/bin/python" -c 'import importlib.metadata; assert importlib.metadata.version("adxadmin")'
  "$install_venv/bin/adxadmin" --help >/dev/null
)
"$install_venv/bin/python" - "$output/install-smoke.json" <<'PY'
import importlib.metadata
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
path.write_text(json.dumps({
    "status": "passed",
    "distribution": "adxadmin",
    "version": importlib.metadata.version("adxadmin"),
}, indent=2) + "\n")
PY

"$venv/bin/python" build/admin/candidate.py --verify --directory "$output"
