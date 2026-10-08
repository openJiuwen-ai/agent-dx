#!/usr/bin/env bash
# Shared by package and publication jobs; every virtualenv remains isolated.
set -euo pipefail
export PIP_CACHE_DIR=${PIP_CACHE_DIR:-/root/.cache/pip}
wheels=${ADX_PYTHON_WHEELHOUSE:-/opt/adx-python-wheels}
if [[ -d "$wheels" ]]; then
  cmp -s build/images/python-requirements.txt "$wheels/requirements.txt" || {
    echo 'Python wheelhouse recipe changed; rebuild and pin the Python image' >&2
    exit 1
  }
  export PIP_NO_INDEX=1 PIP_FIND_LINKS="$wheels"
  echo 'Python dependencies: offline image wheelhouse'
else
  echo "Python dependencies: configured index, cache=$PIP_CACHE_DIR"
fi
