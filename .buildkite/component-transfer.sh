#!/usr/bin/env bash
set -euo pipefail
operation=${1:?upload or download}
component=${2:?component}
case "${ADX_ARTIFACT_TRANSPORT:-obs}" in
  obs)
    python=${OBS_PYTHON:-/opt/buildtools/python3.9/bin/python3}
    if [[ ! -x "$python" ]]; then python=${PYTHON:-python3}; fi
    if [[ $operation == upload ]]; then
      "$python" build/release/ci_transfer.py upload "$component" "out/buildkite/components/$component.tar.gz"
    elif [[ $operation == download ]]; then
      "$python" build/release/ci_transfer.py download "$component" out/buildkite/components
    else
      echo 'invalid transfer operation' >&2; exit 2
    fi
    ;;
  buildkite)
    if [[ $operation == upload ]]; then
      buildkite-agent artifact upload "out/buildkite/components/$component.tar.gz"
    elif [[ $operation == download ]]; then
      buildkite-agent artifact download "out/buildkite/components/$component.tar.gz" . --step "build-$component"
    else
      echo 'invalid transfer operation' >&2; exit 2
    fi
    ;;
  *) echo 'ADX_ARTIFACT_TRANSPORT must be obs or buildkite' >&2; exit 2 ;;
esac
