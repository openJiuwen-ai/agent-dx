#!/usr/bin/env bash
set -euo pipefail
export ADX_KUBECONFIG="${ADX_KUBECONFIG:-/var/run/yr-k8s/target/kubeconfig}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]
artifact_build=${ADX_E2E_ARTIFACT_BUILD:-}
artifact_commit=${ADX_E2E_ARTIFACT_COMMIT:-}
if [[ -n $artifact_build ]]; then
  [[ $artifact_build =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]] || {
    echo 'ADX_E2E_ARTIFACT_BUILD must be a Buildkite build UUID' >&2
    exit 1
  }
  [[ $artifact_commit =~ ^[0-9a-f]{40}$ ]] || {
    echo 'ADX_E2E_ARTIFACT_COMMIT must be the 40-character product commit' >&2
    exit 1
  }
else
  artifact_commit=${artifact_commit:-$BUILDKITE_COMMIT}
  [[ $artifact_commit =~ ^[0-9a-f]{40}$ ]] || {
    echo 'ADX_E2E_ARTIFACT_COMMIT must be the 40-character product commit' >&2
    exit 1
  }
fi
export ADX_E2E_ARTIFACT_COMMIT=$artifact_commit
download_image_artifact() {
  if [[ -n $artifact_build ]]; then
    buildkite-agent artifact download "$1" . --step platform-images --build "$artifact_build"
  else
    buildkite-agent artifact download "$1" . --step platform-images
  fi
}
download_image_artifact 'out/buildkite/summaries/images.json'
echo "--- :kubernetes: Deploy and run public SDK acceptance"
download_image_artifact 'out/buildkite/bundle/*.json'
args=(--bundle out/buildkite/bundle/bundle.json --registry-images out/buildkite/bundle/registry-images.json --kubeconfig "$ADX_KUBECONFIG")
if [[ -n ${ADX_E2E_TARGET_CASE:-} && -n ${ADX_E2E_TARGET_CASES:-} ]]; then
  echo 'Select ADX_E2E_TARGET_CASE or ADX_E2E_TARGET_CASES, not both' >&2
  exit 1
fi
if [[ -n ${ADX_E2E_REGISTRY_AUTH_FILE:-} && -f "$ADX_E2E_REGISTRY_AUTH_FILE" ]]; then
  args+=(--registry-auth "$ADX_E2E_REGISTRY_AUTH_FILE")
fi
if [[ -n ${ADX_KUBE_CONTEXT:-} ]]; then
  args+=(--context "$ADX_KUBE_CONTEXT")
fi
if [[ -n ${ADX_E2E_NODE_NAMES:-} ]]; then
  IFS=',' read -r -a node_names <<< "$ADX_E2E_NODE_NAMES"
  for node in "${node_names[@]}"; do args+=(--node-name "$node"); done
fi
if [[ -n ${ADX_E2E_TARGET_CASES:-} ]]; then
  [[ ${ADX_E2E_PROFILE:-full} == full ]] || {
    echo 'Targeted suite requires ADX_E2E_PROFILE=full' >&2
    exit 1
  }
  [[ -z ${ADX_E2E_REDIS_STORAGE_CLASS:-} ]] || {
    echo 'Run the Redis PVC diagnostic as a single targeted case' >&2
    exit 1
  }
  [[ $ADX_E2E_TARGET_CASES =~ ^[a-z0-9-]+(,[a-z0-9-]+)*$ ]] || {
    echo 'ADX_E2E_TARGET_CASES must be comma-separated case names' >&2
    exit 1
  }
  campaign_budget=${ADX_E2E_BUDGET_SECONDS:-7200}
  [[ $campaign_budget =~ ^[0-9]+$ ]] && (( campaign_budget >= 660 && campaign_budget <= 7800 )) || {
    echo 'Buildkite targeted suite budget must be 660..7800 seconds' >&2
    exit 1
  }
  IFS=',' read -r -a target_cases <<< "$ADX_E2E_TARGET_CASES"
  for target_case in "${target_cases[@]}"; do
    args+=(--case "$target_case")
  done
  python3 -u build/e2e/kubernetes/targeted_suite.py "${args[@]}" \
    --output out/buildkite/acceptance \
    --budget-seconds "$campaign_budget"
else
  args+=(--output out/buildkite/acceptance --profile "${ADX_E2E_PROFILE:-k8s-basic}")
  if [[ -n ${ADX_E2E_TARGET_CASE:-} ]]; then
    args+=(--case "$ADX_E2E_TARGET_CASE")
  fi
  if [[ -n ${ADX_E2E_REDIS_STORAGE_CLASS:-} ]]; then
    args+=(--redis-storage-class "$ADX_E2E_REDIS_STORAGE_CLASS")
  fi
  python3 -u build/e2e/kubernetes/run.py "${args[@]}"
fi
