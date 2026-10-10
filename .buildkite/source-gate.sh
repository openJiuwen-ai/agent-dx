#!/usr/bin/env bash
set -euo pipefail

for legacy_flag in ADX_WITH_DFS ADX_DFS_ALL_FEATURES; do
  if [[ ${!legacy_flag+x} ]]; then
    echo "$legacy_flag was replaced by ${legacy_flag/DFS/AFS}; update the caller" >&2
    exit 2
  fi
done

ADX_WITH_AFS=${ADX_WITH_AFS:-0}
case "$ADX_WITH_AFS" in
  0|1) ;;
  *) echo 'ADX_WITH_AFS must be 0 or 1' >&2; exit 2 ;;
esac

ADX_AFS_ALL_FEATURES=${ADX_AFS_ALL_FEATURES:-0}
case "$ADX_AFS_ALL_FEATURES" in
  0|1) ;;
  *) echo 'ADX_AFS_ALL_FEATURES must be 0 or 1' >&2; exit 2 ;;
esac

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
ADX_WITH_AFS="$ADX_WITH_AFS" ADX_AFS_ALL_FEATURES="$ADX_AFS_ALL_FEATURES" \
  make rust-check JOBS="${JOBS:-4}" \
  2>&1 | tee out/buildkite/logs/rust-check.log
