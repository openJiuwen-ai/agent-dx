"""Enter a frozen test-owned Docker role; never invoke a Kubernetes client."""

import json
import re
import subprocess

from fixtures.common import Blocked, Channel, HERE


def connect(config):
    prefix = config.get('docker')
    run_id, name = config.get('run_id', ''), config.get('container', '')
    mount = config.get('mount_namespace', 'private')
    if not isinstance(prefix, list) or not prefix or not all(isinstance(s, str) and s for s in prefix):
        raise ValueError('literal Docker argv required')
    if not re.fullmatch(r'adx-e2e-[a-z0-9-]{1,40}', run_id):
        raise ValueError('owned Docker run ID required')
    if name not in {run_id + '-' + role for role in ('afs-m', 'afs-a', 'afs-b', 'afs-c', 'node1', 'node2')}:
        raise ValueError('owned AFS role container required')
    if mount not in ('private', 'container'):
        raise ValueError('mount_namespace must be private or container')
    item = json.loads(subprocess.check_output(prefix + ['inspect', name], text=True, timeout=30))[0]
    labels = item.get('Config', {}).get('Labels') or {}
    network = item.get('NetworkSettings', {}).get('Networks', {}).get(run_id, {})
    if labels.get('adx.e2e.run') != run_id or item.get('Id') != config.get('container_id'):
        raise Blocked('Docker role ownership or container identity changed')
    if item.get('Name') != '/' + name or item.get('State', {}).get('Running') is not True:
        raise Blocked('Docker role name or running state changed')
    if network.get('NetworkID') != config.get('network_id') or network.get('IPAddress') != config.get('address'):
        raise Blocked('Docker network identity or role address changed')
    if mount == 'container' and labels.get('afs.e2e.data') != 'true':
        raise Blocked('sharing mount namespace requires an opted-in SDK data container')
    command = prefix + ['exec', '-i', item['Id'], 'env']
    for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy'):
        command += ['-u', key]
    if mount == 'private':
        command += ['unshare', '--mount', '--propagation', 'private', '--']
    command += ['python3', '-u', '-c', (HERE / 'worker.py').read_text()]
    channel = Channel(command, timeout=60)
    channel.docker_identity = {'run_id': run_id, 'container_id': item['Id'],
                               'network_id': network['NetworkID'], 'address': network['IPAddress']}
    return channel
