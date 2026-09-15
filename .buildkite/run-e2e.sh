#!/usr/bin/env bash
set -euo pipefail
export ADX_KUBECONFIG="${ADX_KUBECONFIG:-/var/run/yr-k8s/target/kubeconfig}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]
buildkite-agent artifact download 'out/buildkite/bundle/*.json' . --step platform-images
args=(--bundle out/buildkite/bundle/bundle.json --registry-images out/buildkite/bundle/registry-images.json --kubeconfig "$ADX_KUBECONFIG" --output out/buildkite/acceptance)
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
python3 build/e2e/kubernetes/run.py "${args[@]}"
