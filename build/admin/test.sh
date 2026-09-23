#!/usr/bin/env bash
# Run admin unit tests with their supported interpreter and isolated dependencies.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
python=${PYTHON:-python3}
"$python" -c 'import sys; print(sys.version); assert sys.version_info >= (3, 10), "adxadmin tests require Python 3.10+"'
venv=$(mktemp -d "${TMPDIR:-/tmp}/adxadmin-tests.XXXXXX")
trap 'rm -rf "$venv"' EXIT
"$python" -m venv "$venv"
"$venv/bin/python" -m pip install --disable-pip-version-check \
  --cache-dir "${PIP_CACHE_DIR:-/tmp/adx-pip-cache}" 'httpx==0.28.1'
"$venv/bin/python" -m unittest discover -s tools/admin/tests -v
