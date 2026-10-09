#!/usr/bin/env bash
set -euo pipefail

for legacy_flag in ADX_WITH_DFS ADX_DFS_ALL_FEATURES; do
  if [[ ${!legacy_flag+x} ]]; then
    echo "$legacy_flag was replaced by ${legacy_flag/DFS/AFS}; update the caller" >&2
    exit 2
  fi
done

: "${BUILDKITE_COMMIT:?Buildkite revision required}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]

base_ref=${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-refactor}
if git rev-parse --verify "origin/$base_ref" >/dev/null 2>&1; then
  merge_base=$(git merge-base HEAD "origin/$base_ref")
elif git rev-parse --verify "${BUILDKITE_COMMIT}^" >/dev/null 2>&1; then
  merge_base="${BUILDKITE_COMMIT}^"
else
  merge_base=''
fi

if [[ -n "$merge_base" ]]; then
  changed=$(git diff --name-only "$merge_base"...HEAD)
else
  echo 'No merge base available; running AFS optional gate conservatively.'
  changed='dfs/__unknown_base__'
fi

if ! grep -Eq '^(dfs/|build/e2e/dfs/|build/config/examples/afs/|docs/(development/dfs-plan\.md|migration/2026-10-09-dfs-snapshot\.md|migration/sources\.json)|Cargo\.toml|Cargo\.lock|rust-toolchain\.toml|rustfmt\.toml|Makefile|\.buildkite/afs-gate\.sh|build/ci/run\.py|build/release/|platform/deployment/)' <<<"$changed"; then
  echo 'No AFS-related changes detected; skipping AFS optional gate.'
  exit 0
fi

source .buildkite/bootstrap-build.sh
mkdir -p out/buildkite/logs

echo "--- :rust: Optional AFS gate"
ADX_WITH_AFS=1 ADX_AFS_ALL_FEATURES=1 make afs-check JOBS="${JOBS:-2}" \
  2>&1 | tee out/buildkite/logs/afs-check.log
