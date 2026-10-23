"""Private fixture configuration and process control for real Activator deployments."""

import json
import os
import secrets
import signal
import subprocess
import time
import urllib.request
from pathlib import Path

MODES = ('standalone', 'embedded')


def configure(mode, node, services, redis_url, private, state):
    if mode not in MODES:
        raise ValueError('invalid Activator E2E mode')
    namespace = 'acceptance-agent'
    sandbox_network = {
        'sandbox_cidrs': ['10.231.16.0/20', '10.231.32.0/20'],
        'internal_cidrs': [], 'public_egress': False, 'egress_allow': [],
    }
    token_file = private / 'activator-key'
    if not token_file.exists():
        token_file.write_text(secrets.token_hex(32))
        token_file.chmod(0o600)
    if mode == 'standalone':
        agent = {'activator': {'urls': ['http://coordinator:18091', 'http://node2:18091'], 'token_env': 'ADX_ACTIVATOR_SERVICE_TOKEN', 'allow_plaintext': True}}
        activator = {
            'listen': '0.0.0.0:18091', 'redis_url': redis_url, 'namespace': namespace,
            'sandbox_network': sandbox_network,
            'environment_url': 'http://127.0.0.1:8888', 'environment_api_key_envs': {'e2e': 'ADX_PLATFORM_TENANT_API_KEY'},
            'environment_ca_file': None, 'allow_plaintext_transport': True, 'request_timeout_seconds': 120,
            'registration': {'instance_id': node, 'advertise_url': f'http://{"coordinator" if node == "node1" else "node2"}:18091', 'lease_seconds': 15, 'heartbeat_seconds': 5},
        }
        path = state / 'activator.json'
        path.write_text(json.dumps(activator))
        path.chmod(0o600)
    else:
        agent = {'embedded': {'redis_url': redis_url, 'namespace': namespace, 'sandbox_network': sandbox_network}}
    agent['timeout_seconds'] = 120
    path = state / 'agent.json'
    path.write_text(json.dumps(agent))
    path.chmod(0o600)
    for service in services:
        if service['role'] in ('apiserver', 'ingress'):
            service.setdefault('env', {}).update({
                'ADX_AGENT_CONFIG': str(path),
                'ADX_ACTIVATOR_SERVICE_TOKEN': token_file.read_text().strip(),
            })


def start(node, restart=False):
    state, private = Path('/tmp/adx-e2e'), Path('/secrets')
    marker = state / 'activator-process.json'
    previous = None
    if restart:
        previous = json.loads(marker.read_text())
        if Path(f'/proc/{previous["pid"]}/stat').read_text().split()[21] != previous['start_ticks']:
            raise RuntimeError('Activator process identity changed before restart')
        os.kill(previous['pid'], signal.SIGTERM)
        deadline = time.monotonic() + 20
        while Path(f'/proc/{previous["pid"]}').exists():
            stat = Path(f'/proc/{previous["pid"]}/stat')
            if not stat.exists() or stat.read_text().split()[2] == 'Z':
                break
            if time.monotonic() >= deadline:
                raise TimeoutError('Activator did not stop')
            time.sleep(0.2)
    env = {**os.environ, 'ADX_ACTIVATOR_CONFIG': str(state / 'activator.json'),
           'ADX_ACTIVATOR_SERVICE_TOKEN': (private / 'activator-key').read_text().strip(),
           'ADX_PLATFORM_TENANT_API_KEY': (private / 'api-key').read_text().strip()}
    with (Path('/evidence') / f'activator-{node}.log').open('a') as log:
        child = subprocess.Popen(['/opt/adx/package/bin/adx-activator'], env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    process = {'pid': child.pid, 'start_ticks': Path(f'/proc/{child.pid}/stat').read_text().split()[21]}
    marker.write_text(json.dumps(process))
    deadline = time.monotonic() + 30
    while True:
        if child.poll() is not None:
            raise RuntimeError('Activator exited; inspect its log')
        try:
            with urllib.request.urlopen('http://127.0.0.1:18091/health/ready', timeout=1) as response:
                if response.status == 204:
                    break
        except OSError:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError('Activator readiness timeout')
        time.sleep(0.2)
    if previous:
        process['previous_pid'] = previous['pid']
        (Path('/evidence') / f'activator-restart-{node}.json').write_text(json.dumps(process))
    return process
