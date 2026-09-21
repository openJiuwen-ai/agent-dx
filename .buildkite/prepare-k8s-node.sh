#!/usr/bin/env bash
set -euo pipefail
: "${BUILDKITE_BUILD_NUMBER:?Buildkite build number required}"
: "${ADX_E2E_ARTIFACT_BUILD:?Source image build UUID required}"
: "${ADX_K8S_PREPARE_NODE_NAMES:?Explicit comma-separated target node names required}"
export ADX_KUBECONFIG="${ADX_KUBECONFIG:-/var/run/yr-k8s/target/kubeconfig}"
[[ -f $ADX_KUBECONFIG ]] || { echo 'target kubeconfig not found' >&2; exit 1; }

output=out/buildkite/node-prepare
mkdir -p "$output"
buildkite-agent artifact download 'out/buildkite/bundle/registry-images.json' . \
  --step platform-images --build "$ADX_E2E_ARTIFACT_BUILD"
image=$(python3 -c 'import json; print(json.load(open("out/buildkite/bundle/registry-images.json"))["references"]["node"])')
[[ $image =~ @sha256:[0-9a-f]{64}$ ]] || { echo 'immutable node image required' >&2; exit 1; }

namespace="adx-node-prep-${BUILDKITE_BUILD_NUMBER}"
kube=(kubectl --kubeconfig "$ADX_KUBECONFIG")
cleanup() {
  "${kube[@]}" delete namespace "$namespace" --ignore-not-found --wait=true --timeout=120s >/dev/null 2>&1 || true
}
trap cleanup EXIT
"${kube[@]}" create namespace "$namespace"
"${kube[@]}" label namespace "$namespace" adx.e2e.maintenance="$namespace"
pull_secret=false
if [[ -n ${ADX_E2E_REGISTRY_AUTH_FILE:-} && -f $ADX_E2E_REGISTRY_AUTH_FILE ]]; then
  "${kube[@]}" -n "$namespace" create secret generic adx-test-registry \
    --type=kubernetes.io/dockerconfigjson \
    --from-file=.dockerconfigjson="$ADX_E2E_REGISTRY_AUTH_FILE" >/dev/null
  pull_secret=true
fi

IFS=',' read -r -a nodes <<< "$ADX_K8S_PREPARE_NODE_NAMES"
(( ${#nodes[@]} > 0 )) || { echo 'at least one node is required' >&2; exit 1; }
prepared=()
index=0
for node in "${nodes[@]}"; do
  [[ $node =~ ^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$ ]] || { echo "invalid node name: $node" >&2; exit 1; }
  index=$((index + 1))
  pod="prepare-${index}"
  NODE="$node" POD="$pod" NAMESPACE="$namespace" IMAGE="$image" PULL_SECRET="$pull_secret" \
    python3 - <<'PY' > "$output/$pod.json"
import json, os
command = r'''
mkdir -p /host/etc/modules-load.d /host/etc/sysctl.d
if [ ! -e /proc/sys/net/bridge/bridge-nf-call-iptables ]; then
  chroot /host /bin/sh -ceu 'PATH=/usr/sbin:/usr/bin:/sbin:/bin; modprobe br_netfilter'
fi
sysctl -w net.bridge.bridge-nf-call-iptables=1
test "$(cat /proc/sys/net/bridge/bridge-nf-call-iptables)" = 1
printf 'br_netfilter\n' > /host/etc/modules-load.d/adx-br-netfilter.conf
printf 'net.bridge.bridge-nf-call-iptables = 1\n' > /host/etc/sysctl.d/99-adx-bridge-netfilter.conf
printf 'node=%s bridge_netfilter=%s\n' "$ADX_TARGET_NODE" "$(cat /proc/sys/net/bridge/bridge-nf-call-iptables)"
'''
spec = {
    'restartPolicy': 'Never', 'automountServiceAccountToken': False,
    'nodeName': os.environ['NODE'], 'hostNetwork': True, 'hostPID': True,
    'containers': [{
        'name': 'prepare', 'image': os.environ['IMAGE'], 'imagePullPolicy': 'IfNotPresent',
        'command': ['/bin/sh', '-ceu', command],
        'env': [{'name': 'ADX_TARGET_NODE', 'value': os.environ['NODE']}],
        'securityContext': {'privileged': True, 'runAsUser': 0},
        'volumeMounts': [{'name': 'host-root', 'mountPath': '/host'}],
    }],
    'volumes': [{'name': 'host-root', 'hostPath': {'path': '/', 'type': 'Directory'}}],
}
if os.environ['PULL_SECRET'] == 'true':
    spec['imagePullSecrets'] = [{'name': 'adx-test-registry'}]
print(json.dumps({'apiVersion': 'v1', 'kind': 'Pod',
                  'metadata': {'name': os.environ['POD'], 'namespace': os.environ['NAMESPACE']},
                  'spec': spec}))
PY
  "${kube[@]}" create -f "$output/$pod.json"
  phase=''
  for _ in $(seq 1 90); do
    phase=$("${kube[@]}" -n "$namespace" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ $phase == Succeeded || $phase == Failed ]] && break
    sleep 2
  done
  "${kube[@]}" -n "$namespace" logs "$pod" | tee "$output/$node.log"
  [[ $phase == Succeeded ]] || {
    "${kube[@]}" -n "$namespace" describe pod "$pod" > "$output/$node-describe.log" 2>&1 || true
    echo "node preparation failed: $node phase=$phase" >&2
    exit 1
  }
  prepared+=("$node")
done

PREPARED=$(IFS=,; echo "${prepared[*]}") python3 - <<'PY' > "$output/result.json"
import json, os
print(json.dumps({'status': 'passed', 'nodes': os.environ['PREPARED'].split(','),
                  'bridge_netfilter': True}, indent=2))
PY
cat "$output/result.json"
