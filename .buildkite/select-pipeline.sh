#!/usr/bin/env bash
set -euo pipefail

for legacy_flag in ADX_WITH_DFS ADX_DFS_ALL_FEATURES; do
  if [[ ${!legacy_flag+x} ]]; then
    echo "$legacy_flag was replaced by ${legacy_flag/DFS/AFS}; update the caller" >&2
    exit 2
  fi
done

for flag in ADX_WITH_AFS ADX_ARM_TESTS ADX_OBS_UPLOAD ADX_ARM_OBS_UPLOAD ADX_ADMIN_PYPI_UPLOAD ADX_SDK_PYPI_UPLOAD; do
  value=${!flag:-0}
  [[ $value == 0 || $value == 1 ]] || { echo "$flag must be 0 or 1" >&2; exit 2; }
done
if [[ ${ADX_ARM_BUILD:-1} != 0 && ${ADX_ARM_BUILD:-1} != 1 ]]; then
  echo 'ADX_ARM_BUILD must be 0 or 1' >&2
  exit 2
fi
case "${ADX_ARTIFACT_TRANSPORT:-obs}" in
  obs|buildkite) ;;
  *) echo 'ADX_ARTIFACT_TRANSPORT must be obs or buildkite' >&2; exit 2 ;;
esac

if [[ ${ADX_PYTHON_IMAGE_SYNC_ONLY:-0} == 1 || ${ADX_COLLECTOR_SYNC_ONLY:-0} == 1 || ${ADX_K8S_NODE_PREPARE_ONLY:-0} == 1 || ${ADX_BUILD_IMAGE_SYNC_ONLY:-0} == 1 ]]; then
  file=.buildkite/pipeline-maintenance.yml
else
  case "${BUILDKITE_PIPELINE_SLUG:-agent-dx}" in
    agent-dx) file=.buildkite/pipeline-package.yml ;;
    agent-dx-python-sdk) file=.buildkite/pipeline-sdk.yml ;;
    agent-dx-admin) file=.buildkite/pipeline-admin.yml ;;
    agent-dx-full-test) file=.buildkite/pipeline-full.yml ;;
    *) echo "unsupported ADX Buildkite pipeline: ${BUILDKITE_PIPELINE_SLUG:-unset}" >&2; exit 2 ;;
  esac
fi

echo "Uploading $file for ${BUILDKITE_PIPELINE_SLUG:-local}"
buildkite-agent pipeline upload "$file"
