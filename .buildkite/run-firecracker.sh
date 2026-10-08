#!/usr/bin/env bash
set -euo pipefail
: "${ADX_FC_K8S_NODE:?select a KVM-capable target worker hostname}"
: "${ADX_KUBECONFIG:?select the target kubeconfig}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required'; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]]
buildkite-agent artifact download 'out/buildkite/bundle/*.json' . --step platform-images
args=(--bundle out/buildkite/bundle/bundle.json --registry-images out/buildkite/bundle/registry-images.json --kubeconfig "$ADX_KUBECONFIG" --node-name "$ADX_FC_K8S_NODE" --output out/buildkite/firecracker)
[[ -z ${ADX_KUBE_CONTEXT:-} ]] || args+=(--context "$ADX_KUBE_CONTEXT")
[[ -z ${ADX_E2E_REGISTRY_AUTH_FILE:-} ]] || args+=(--registry-auth "$ADX_E2E_REGISTRY_AUTH_FILE")
mkdir -p out/buildkite/logs
python3 -u build/e2e/firecracker/kubernetes.py "${args[@]}" 2>&1 | tee out/buildkite/logs/firecracker-kubernetes.log
