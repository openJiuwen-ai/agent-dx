#!/usr/bin/env bash
set -euo pipefail
python3 -c 'import sys; assert sys.version_info >= (3, 10)'
check=$(mktemp -d)
trap 'rm -rf "$check"' EXIT
python3 -m venv "$check/venv"
"$check/venv/bin/python" -m pip install --disable-pip-version-check --no-index \
  --find-links /opt/adx-python-wheels -r /opt/adx-python-wheels/requirements.txt
"$check/venv/bin/python" -m pip check
"$check/venv/bin/python" -c 'import build,twine,pytest,httpx,websockets'
