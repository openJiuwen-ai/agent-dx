"""Enter an owned, immutable Kubernetes role without copying cluster credentials."""

import json
import re
import subprocess

from fixtures.common import Blocked, Channel, HERE


def connect(config):
    prefix = config.get('kubectl')
    if not isinstance(prefix, list) or not prefix or not all(isinstance(x, str) and x for x in prefix):
        raise ValueError('literal kubectl argv is required')
    namespace, pod = config.get('namespace', ''), config.get('pod', '')
    if not re.fullmatch(r'adx-e2e-[a-z0-9-]{1,40}', namespace) or not re.fullmatch(r'afs-[mabc]', pod):
        raise ValueError('an owned AFS namespace and role Pod are required')
    if config.get('container') != 'platform':
        raise ValueError('AFS worker container must be platform')
    if not all(isinstance(config.get(name), str) and config[name] for name in ('namespace_uid', 'pod_uid', 'pod_ip')):
        raise ValueError('frozen Kubernetes identities are required')
    ns = json.loads(subprocess.check_output(prefix + ['get', 'namespace', namespace, '-o', 'json'],
                                            text=True, timeout=30))
    item = json.loads(subprocess.check_output(prefix + ['-n', namespace, 'get', 'pod', pod, '-o', 'json'],
                                              text=True, timeout=30))
    if any(meta.get('labels', {}).get('adx.e2e.run') != namespace for meta in (ns['metadata'], item['metadata'])):
        raise Blocked('Kubernetes role ownership changed')
    if ns['metadata'].get('uid') != config['namespace_uid'] or item['metadata'].get('uid') != config['pod_uid']:
        raise Blocked('Kubernetes namespace or Pod was replaced')
    if item.get('status', {}).get('phase') != 'Running' or item['status'].get('podIP') != config['pod_ip']:
        raise Blocked('Kubernetes role address or running state changed')
    if 'platform' not in [c['name'] for c in item['spec']['containers']]:
        raise Blocked('Kubernetes role container is missing')
    command = prefix + ['-n', namespace, 'exec', '-i', pod, '-c', 'platform', '--', 'env']
    for name in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'):
        command += ['-u', name]
    command += ['unshare', '--mount', '--propagation', 'private', '--', 'python3', '-u', '-c',
                (HERE / 'worker.py').read_text()]
    channel = Channel(command, timeout=60)
    channel.kubernetes_identity = {
        'namespace': namespace, 'namespace_uid': config['namespace_uid'],
        'pod': pod, 'pod_uid': config['pod_uid'], 'pod_ip': config['pod_ip'], 'node': item['spec']['nodeName'],
    }
    return channel
