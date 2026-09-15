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
python3 build/e2e/build_backend.py --output out/buildkite/backend --redis-cli "$ADX_REDIS_CLI" --jobs "${JOBS:-2}" > out/buildkite/logs/backend.log 2>&1
