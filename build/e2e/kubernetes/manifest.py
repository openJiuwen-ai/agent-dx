"""Kubernetes resources for an isolated process deployment of the full platform."""
import re

LABEL = 'adx.e2e.run'

def resources(namespace, image, architecture, registry_auth=False, node_names=()):
    if not re.fullmatch(r'adx-e2e-[a-z0-9-]{1,40}', namespace):
        raise ValueError('a unique adx-e2e namespace is required')
    if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', image):
        raise ValueError('a digest-pinned node image is required')
    if architecture not in ('amd64', 'arm64'):
        raise ValueError('unsupported Kubernetes architecture')
    if any(not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,251}[a-z0-9]|[a-z0-9]", name) for name in node_names):
        raise ValueError("invalid target node name")
    objects = []
    for node, service in [('node1', 'master'), ('node2', 'node2')]:
        labels = {LABEL: namespace, 'adx.e2e.node': node}
        volumes = [
            {'name': 'state', 'emptyDir': {}},
            {'name': 'evidence', 'emptyDir': {}},
            {'name': 'images', 'emptyDir': {'medium': 'Memory', 'sizeLimit': '1Gi'}},
            {'name': 'credentials', 'secret': {'secretName': 'adx-test-credentials', 'defaultMode': 0o400}},
        ]
        mounts = [
            {'name': 'state', 'mountPath': '/tmp/adx-e2e'},
            {'name': 'images', 'mountPath': '/tmp/adx-e2e/sandboxd/image_manager'},
            {'name': 'evidence', 'mountPath': '/evidence'},
            {'name': 'credentials', 'mountPath': '/secrets', 'readOnly': True},
        ]
        # Secret keys cannot contain slashes; project certificate files into tls/.
        certificates = ['ca.pem']
        for name in ('master', 'node', 'node2', 'frontend', 'edge'):
            certificates += [name + ext for ext in ('.pem', '.key', '.der')]
        volumes[-1]['secret']['items'] = [
            {'key': name, 'path': 'tls/' + name} for name in certificates
        ] + [{'key': name, 'path': name} for name in ('api-key', 'other-key', 'admin-key', 'redis-key', 'image')]
        container = {
            'name': 'platform', 'image': image, 'imagePullPolicy': 'IfNotPresent',
            'command': ['sleep', 'infinity'],
            'securityContext': {'privileged': True, 'runAsUser': 0},
            'resources': {'requests': {'cpu': '2', 'memory': '2Gi'},
                          'limits': {'cpu': '3', 'memory': '4Gi'}},
            'env': [{'name': 'ADX_E2E_KUBERNETES', 'value': '1'},
                    {'name': 'ADX_E2E_NODE_IP', 'valueFrom': {'fieldRef': {'fieldPath': 'status.podIP'}}}],
            'volumeMounts': mounts,
        }
        spec = {
            'restartPolicy': 'Never', 'automountServiceAccountToken': False,
            'terminationGracePeriodSeconds': 90,
            'nodeSelector': {'kubernetes.io/os': 'linux', 'kubernetes.io/arch': architecture},
            'containers': [container, {
                'name':'collector','image':image,'imagePullPolicy':'IfNotPresent',
                'command':['python3','/opt/adx/e2e/telemetry.py','run'],
                'securityContext':{'runAsUser':0,'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},
                'resources':{'requests':{'cpu':'100m','memory':'128Mi'},'limits':{'cpu':'1','memory':'256Mi'}},
                'volumeMounts':[dict(m) for m in mounts if m['name'] in ('state','evidence','credentials')]
            }], 'volumes': volumes,
            'affinity': {'podAntiAffinity': {'preferredDuringSchedulingIgnoredDuringExecution': [{
                'weight': 100, 'podAffinityTerm': {
                    'labelSelector': {'matchLabels': {LABEL: namespace}},
                    'topologyKey': 'kubernetes.io/hostname'}}]}},
        }
        if node_names:
            spec["affinity"]["nodeAffinity"] = {
                "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{
                    "matchFields": [{"key": "metadata.name", "operator": "In", "values": list(node_names)}]
                }]}}
        if registry_auth:
            spec['imagePullSecrets'] = [{'name': 'adx-test-registry'}]
            volumes.append({'name': 'registry-auth', 'secret': {'secretName': 'adx-test-registry'}})
            mounts.append({'name': 'registry-auth', 'mountPath': '/registry-auth', 'readOnly': True})
        objects.append({'apiVersion': 'v1', 'kind': 'Pod',
                        'metadata': {'name': node, 'namespace': namespace, 'labels': labels}, 'spec': spec})
        ports = [17001, 18443]
        if node == 'node1':
            ports += [6379, 17000, 8443]
        objects.append({'apiVersion': 'v1', 'kind': 'Service',
                        'metadata': {'name': service, 'namespace': namespace, 'labels': labels},
                        'spec': {'selector': labels, 'ports': [
                            {'name': 'tcp-' + str(port), 'port': port, 'targetPort': port} for port in ports]}})
    return objects
