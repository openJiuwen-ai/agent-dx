#!/usr/bin/env bash
# Source to select the OBS SDK interpreter used by builders and image packagers.
set -euo pipefail
OBS_PYTHON=${OBS_PYTHON:-/opt/buildtools/python3.9/bin/python3}
if [[ ! -x "$OBS_PYTHON" ]]; then OBS_PYTHON=python3; fi
if ! "$OBS_PYTHON" -c 'from obs import ObsClient' >/dev/null 2>&1; then
  obs_venv=${TMPDIR:-/tmp}/adx-obs-${BUILDKITE_JOB_ID:-local}
  python3 -m venv "$obs_venv"
  "$obs_venv/bin/python" -m pip install --disable-pip-version-check --timeout 30 --retries 2 'esdk-obs-python==3.25.8'
  OBS_PYTHON="$obs_venv/bin/python"
fi
export OBS_PYTHON
