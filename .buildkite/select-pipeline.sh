#!/usr/bin/env bash
set -euo pipefail

if [[ ${ADX_COLLECTOR_SYNC_ONLY:-0} == 1 || ${ADX_K8S_NODE_PREPARE_ONLY:-0} == 1 || ${ADX_BUILD_IMAGE_SYNC_ONLY:-0} == 1 ]]; then
  file=.buildkite/pipeline-maintenance.yml
else
  case "${BUILDKITE_PIPELINE_SLUG:-agent-dx}" in
    agent-dx) file=.buildkite/pipeline-package.yml ;;
    agent-dx-python-sdk) file=.buildkite/pipeline-sdk.yml ;;
    agent-dx-full-test) file=.buildkite/pipeline-full.yml ;;
    *) echo "unsupported ADX Buildkite pipeline: ${BUILDKITE_PIPELINE_SLUG:-unset}" >&2; exit 2 ;;
  esac
fi

echo "Uploading $file for ${BUILDKITE_PIPELINE_SLUG:-local}"
buildkite-agent pipeline upload "$file"
