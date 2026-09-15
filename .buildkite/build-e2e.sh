#!/usr/bin/env bash
set -euo pipefail
: "${BUILDKITE_COMMIT:?Buildkite revision required}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]
export ADX_RELEASE_OUTPUT="$PWD/out/buildkite/package"
export ADX_RELEASE_TARGET=x86_64-unknown-linux-gnu
mkdir -p out/buildkite/logs
source .buildkite/bootstrap-build.sh > out/buildkite/logs/bootstrap.log 2>&1
python3 -m unittest discover -s build/e2e/tests -v > out/buildkite/logs/driver-tests.log 2>&1
bash build/release/build.sh > out/buildkite/logs/release.log 2>&1
# One archive preserves top-level metadata, nested files and executable modes.
tar -czf out/buildkite/adx-release.tar.gz -C out/buildkite/package .
(cd out/buildkite && sha256sum adx-release.tar.gz > adx-release.tar.gz.sha256)
if [[ -n ${ADX_BACKEND_ARTIFACT_BUILD:-} ]]; then
  buildkite-agent artifact download 'out/buildkite/backend/*' . --step platform-build --build "$ADX_BACKEND_ARTIFACT_BUILD" > out/buildkite/logs/backend.log 2>&1
  python3 build/e2e/verify_backend.py --directory out/buildkite/backend --target "$ADX_RELEASE_TARGET" >> out/buildkite/logs/backend.log 2>&1
else
  python3 build/e2e/build_backend.py --output out/buildkite/backend --redis-cli "$ADX_REDIS_CLI" --jobs "${JOBS:-2}" > out/buildkite/logs/backend.log 2>&1
fi
