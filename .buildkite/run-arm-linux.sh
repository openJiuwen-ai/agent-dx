#!/usr/bin/env bash
# Native ARM Linux jobs share the component pipeline and OBS staging protocol.
set -euo pipefail
phase=${1:?build or package}
component=${2:-}
case "$phase:$component" in
  build:platform|build:gateway|build:execd|build:afs|package:) ;;
  *) echo 'expected build <platform|gateway|execd|afs> or package' >&2; exit 2 ;;
esac
export ADX_BUILD_ARCH=arm64
source .buildkite/build-architecture.sh
[[ $(uname -s) == Linux && $(uname -m) == aarch64 ]] || { echo 'native Linux ARM worker required' >&2; exit 1; }
[[ $(rustc -vV | sed -n 's/^host: //p') == "$ADX_RELEASE_TARGET" ]]
export ADX_COMPONENT_LOCAL=0 ADX_CANDIDATE_LOCAL=0 ADX_EXTERNAL_BACKEND=0 ADX_DEFER_PUBLICATION=1
export ADX_COMPONENT_TESTS=${ADX_ARM_TESTS:-0} ADX_PACKAGE_TESTS=${ADX_ARM_TESTS:-0}
mkdir -p out/buildkite/arm64 out/buildkite/logs
finish() {
  status=$?
  trap - EXIT
  for item in logs components package sdk admin obs adx-release.tar.gz adx-release.tar.gz.sha256 adx-execd.tar.gz adx-execd.tar.gz.sha256 release-manifest.json build-manifest.json; do
    if [[ -e out/buildkite/$item ]]; then
      mv "out/buildkite/$item" out/buildkite/arm64/ || { if [[ $status == 0 ]]; then status=1; fi; }
    fi
  done
  if [[ $status == 0 && $phase == package && ${ADX_ARTIFACT_TRANSPORT:-obs} == buildkite ]]; then
    buildkite-agent artifact upload 'out/buildkite/arm64/*.tar.gz' || status=$?
  fi
  exit "$status"
}
trap finish EXIT
case "$phase" in
  build) bash .buildkite/step.sh "build-$component-arm64" bash .buildkite/build-component.sh "$component" ;;
  package) bash .buildkite/step.sh package-arm64 bash .buildkite/package-components.sh ;;
esac
