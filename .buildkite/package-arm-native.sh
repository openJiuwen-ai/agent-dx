#!/usr/bin/env bash
# Native Linux ARM execution of the same component and assembly scripts as x86.
set -euo pipefail
source .buildkite/build-architecture.sh
[[ $ADX_BUILD_ARCH == arm64 ]]
[[ $(uname -m) == aarch64 ]]
[[ $(rustc -vV | sed -n 's/^host: //p') == "$ADX_RELEASE_TARGET" ]]
export ADX_COMPONENT_LOCAL=1 ADX_CANDIDATE_LOCAL=1 ADX_EXTERNAL_BACKEND=0
export ADX_COMPONENT_TESTS=0 ADX_PACKAGE_TESTS=0 ADX_DEFER_PUBLICATION=1
export ADX_COMPONENT_TESTS=${ADX_ARM_TESTS:-0} ADX_PACKAGE_TESTS=${ADX_ARM_TESTS:-0}
case "${1:?build or package}" in
  build) bash .buildkite/build-component.sh "${2:?component}" ;;
  package) bash .buildkite/package-components.sh ;;
  *) exit 2 ;;
esac
