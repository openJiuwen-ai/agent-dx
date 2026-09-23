#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
source .buildkite/python-env.sh
output=out/buildkite/sdk
mkdir -p "$output" out/buildkite/logs

python3 - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit('ADX Sandbox SDK requires Python 3.10 or newer')
print(sys.version)
PY

python3 -m venv /tmp/adx-sdk-build
source /tmp/adx-sdk-build/bin/activate
python -m pip install --disable-pip-version-check 'build==1.4.4' 'twine==6.2.0' 'pytest>=8,<9'
python -m pip install --disable-pip-version-check -e platform/sdk/sandbox/python

echo '--- :test_tube: Sandbox SDK tests'
python build/sdk/test_candidate.py 2>&1 | tee out/buildkite/logs/sdk-candidate-tests.log
python -m unittest discover -s build/python -p 'test_*.py' -v \
  2>&1 | tee out/buildkite/logs/sdk-release-tests.log
PYTHONPATH=platform/sdk/sandbox/python python -m pytest -q \
  -c platform/sdk/sandbox/pytest.ini platform/sdk/sandbox/python/tests \
  --junitxml="$output/junit.xml" 2>&1 | tee out/buildkite/logs/sdk-tests.log

echo '--- :package: Build wheel and source distribution'
python -m build --wheel --sdist --outdir "$output" platform/sdk/sandbox/python \
  2>&1 | tee out/buildkite/logs/sdk-build.log
python -m twine check "$output"/*.whl "$output"/*.tar.gz \
  2>&1 | tee out/buildkite/logs/sdk-twine-check.log
python build/sdk/candidate.py --directory "$output" --commit "$BUILDKITE_COMMIT" \
  --build-id "${BUILDKITE_BUILD_ID:-local}" | tee out/buildkite/logs/sdk-candidate.log

echo '--- :white_check_mark: Install candidate without source imports'
python3 -m venv /tmp/adx-sdk-install
/tmp/adx-sdk-install/bin/python -m pip install --disable-pip-version-check "$output"/*.whl \
  > out/buildkite/logs/sdk-install.log 2>&1
(cd /tmp && /tmp/adx-sdk-install/bin/python - <<'PY'
import importlib.metadata
import json
import adx_sandbox
version = importlib.metadata.version('adx-sandbox')
candidate = json.load(open('/workspace/out/buildkite/sdk/sdk-candidate.json'))
assert version == candidate['version'], (version, candidate['version'])
assert adx_sandbox.__file__ and '/workspace/' not in adx_sandbox.__file__
print(version, adx_sandbox.__file__)
PY
)
(cd /tmp && /tmp/adx-sdk-install/bin/adx-sandbox --help) >> out/buildkite/logs/sdk-install.log
/tmp/adx-sdk-install/bin/python - <<'PY' > "$output/install-smoke.json"
import json
import platform

print(json.dumps({
    'status': 'passed',
    'python': platform.python_version(),
    'source_imports': False,
}))
PY
