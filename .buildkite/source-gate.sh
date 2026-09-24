#!/usr/bin/env bash
set -euo pipefail

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]

source .buildkite/bootstrap-build.sh
mkdir -p out/buildkite/logs

echo "--- :test_tube: CI driver tests"
PYTHONPATH="$PWD/build${PYTHONPATH:+:$PYTHONPATH}" \
  python3 -u -m unittest discover -s build/e2e/tests -v \
  2>&1 | tee out/buildkite/logs/driver-tests.log
echo "--- :test_tube: Release tooling tests"
python3 -u -m unittest discover -s build/release/tests -v \
  2>&1 | tee out/buildkite/logs/release-tests.log
echo "--- :rust: Rust guideline gate"
make rust-check JOBS="${JOBS:-4}" \
  2>&1 | tee out/buildkite/logs/rust-check.log
