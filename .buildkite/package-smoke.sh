#!/usr/bin/env bash
set -euo pipefail
# L0 consumes this build's tested base and SDK; Full still requires explicit input IDs.
export ADX_BASE_PACKAGE_BUILD_ID=${BUILDKITE_BUILD_ID:?build ID required}
export ADX_SDK_BUILD_ID=$BUILDKITE_BUILD_ID
export ADX_BASE_ARTIFACT_TRANSPORT=${ADX_ARTIFACT_TRANSPORT:-obs}
buildkite-agent artifact download 'out/buildkite/summaries/release.json' . --step platform-build
exec bash .buildkite/package-e2e.sh
