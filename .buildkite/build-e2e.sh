#!/usr/bin/env bash
set -euo pipefail
: "${BUILDKITE_COMMIT:?Buildkite revision required}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]
export ADX_RELEASE_OUTPUT="$PWD/out/buildkite/package"
export ADX_RELEASE_TARGET=x86_64-unknown-linux-gnu
mkdir -p out/buildkite/logs
echo "--- :gear: Toolchain and persistent caches"
source .buildkite/bootstrap-build.sh > >(tee out/buildkite/logs/bootstrap.log) 2>&1
echo "--- :test_tube: CI driver tests"
PYTHONPATH="$PWD/build${PYTHONPATH:+:$PYTHONPATH}" \
  python3 -u -m unittest discover -s build/e2e/tests -v \
  2>&1 | tee out/buildkite/logs/driver-tests.log
echo "--- :test_tube: Release tooling tests"
python3 -u -m unittest discover -s build/release/tests -v 2>&1 | tee out/buildkite/logs/release-tests.log
echo "--- :rust: Rust guideline gate"
make rust-check JOBS="${JOBS:-2}" 2>&1 | tee out/buildkite/logs/rust-check.log
echo "--- :package: Rust platform and Sandbox SDK release"
bash build/release/build.sh 2>&1 | tee out/buildkite/logs/release.log
# One archive preserves top-level metadata, nested files and executable modes.
tar -czf out/buildkite/adx-release.tar.gz -C out/buildkite/package .
(cd out/buildkite && sha256sum adx-release.tar.gz > adx-release.tar.gz.sha256)
cp out/buildkite/package/manifest.json out/buildkite/release-manifest.json
echo "--- :package: sandboxd backend artifacts"
if [[ -n ${ADX_BACKEND_ARTIFACT_BUILD:-} ]]; then
  buildkite-agent artifact download 'out/buildkite/backend/*' . --step platform-build --build "$ADX_BACKEND_ARTIFACT_BUILD" 2>&1 | tee out/buildkite/logs/backend.log
  python3 build/e2e/verify_backend.py --directory out/buildkite/backend --target "$ADX_RELEASE_TARGET" 2>&1 | tee -a out/buildkite/logs/backend.log
else
  python3 build/e2e/build_backend.py --output out/buildkite/backend --redis-cli "$ADX_REDIS_CLI" --jobs "${JOBS:-2}" 2>&1 | tee out/buildkite/logs/backend.log
fi
