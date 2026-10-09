#!/usr/bin/env python3
"""Real installed SDK + sandboxd/FC + in-runtime Unix checkpoint regression."""

import argparse
import json
import os
import shlex
import shutil
import subprocess
import time
import traceback
from pathlib import Path


def _executable(name, environment=None, cwd=None):
    """Resolve an external command using the child's execution environment."""
    directory = os.getcwd() if cwd is None else os.path.abspath(cwd)
    search_path = os.pathsep.join(
        os.path.abspath(os.path.join(directory, entry)) for entry in os.get_exec_path(environment)
    )
    executable = shutil.which(name, path=search_path)
    if executable is None:
        raise FileNotFoundError(f"required executable not found: {name}")
    return os.path.abspath(executable)


p = argparse.ArgumentParser()
p.add_argument('--run', type=Path, required=True)
p.add_argument('--base', type=Path, required=True)
p.add_argument('--image', required=True)
a = p.parse_args()
os.environ['SSL_CERT_FILE'] = str(a.run / 'secrets/tls/ca.pem')
from adx_sandbox import (
    ConnectionConfig,
    DNSPolicy,
    NetworkPolicy,
    NetworkRule,
    Sandbox,
    TrafficPolicy,
)

connection = ConnectionConfig(
    server_address='127.0.0.1:8443',
    token=(a.run / 'secrets/api-key').read_text().strip(),
    use_tls=True,
    verify_tls=True,
)
result = {'status': 'failed', 'runtime': 'firecracker', 'control_transport': 'grpc', 'cases': []}
s = None
out = a.run / 'evidence/sdk-control-stream.json'


def passed(name, **fields):
    result['cases'].append({'name': name, 'passed': True, **fields})
    print('PASS', name, fields, flush=True)


def command(script):
    r = s.commands.run(script)
    if not (r.exit_code == 0):
        raise AssertionError((r.exit_code, r.stdout, r.stderr))
    return r.stdout.strip()


def catalog():
    env = {**os.environ, 'REDISCLI_AUTH': (a.run / 'secrets/redis-key').read_text().strip()}
    return json.loads(
        subprocess.check_output(
            [a.base / 'package/bin/redis-cli', '--json', 'HGETALL', 'adx:{acceptance}:control:v1'], env=env, text=True
        )
    )


def deny_all(mode='stateful'):
    return NetworkPolicy(
        traffic=TrafficPolicy(
            ingress_default_action='deny',
            egress_default_action='deny',
            mode=mode,
            rules=[NetworkRule(action='deny', direction='both', protocol='any', cidr='0.0.0.0/0', priority=4294967294)],
        ),
        dns=DNSPolicy(default_action='deny'),
    )


def egress_probe():
    host_local = os.environ['ADX_FC_EGRESS_PROBE_HOST']
    port_local = int(os.environ['ADX_FC_EGRESS_PROBE_PORT'])
    script = (
        f'exec 3<>/dev/tcp/'
        f'{host_local}'
        f'/'
        f'{port_local}'
        f'; printf "GET / HTTP/1.0\\r\\nHost: '
        f'{host_local}'
        f'\\r\\n\\r\\n" >&3; IFS= read -r line <&3; [[ "$line" == '
        f'HTTP/* ]]'
    )
    return s.commands.run('/usr/bin/timeout 3 /bin/bash -c ' + shlex.quote(script)).exit_code


def checkpoint():
    r = s.commands.run('/tmp/checkpoint-probe /run/adx/execd.sock', timeout=180)
    if not (r.exit_code == 0):
        raise AssertionError((r.stdout, r.stderr))
    if not (r.stdout.startswith('HTTP/1.1 200')):
        raise AssertionError(r.stdout)
    record_local = json.loads(catalog()['environment:' + s.id])['result']
    if not (record_local['checkpoint'] and record_local['state'] == 'Running' and record_local['resources_held']):
        raise AssertionError()
    if not (int(command('cat /tmp/counter')) >= before):
        raise AssertionError()
    if not (command('cat /tmp/pid') == pid):
        raise AssertionError()
    return record_local


try:
    policy = deny_all()
    result['network_policy'] = policy.to_dict()
    t = time.monotonic()
    s = Sandbox(
        image=a.image,
        runtime='firecracker',
        cpu=500,
        memory=512,
        network=policy,
        idle_timeout=0,
        connection=connection,
        create_timeout=120,
    )
    passed(
        'create with Execd-initiated Ready',
        environment_id=s.id,
        seconds=time.monotonic() - t,
        network='deny all IPv4, stateful',
    )
    # A temporary narrow allow proves the test peer is alive. Restore deny-all
    # before checkpoint; never remove the platform's required control exceptions.
    if not (egress_probe() != 0):
        raise AssertionError('deny-all allowed ordinary egress')
    host = os.environ['ADX_FC_EGRESS_PROBE_HOST']
    port = int(os.environ['ADX_FC_EGRESS_PROBE_PORT'])
    s.update_network_policy(
        NetworkPolicy(
            traffic=TrafficPolicy(
                ingress_default_action='deny',
                egress_default_action='deny',
                rules=[
                    NetworkRule(action='allow', direction='egress', protocol='tcp', cidr=host + '/32', port_range=port)
                ],
            ),
            dns=DNSPolicy(default_action='deny'),
        )
    )
    if not (egress_probe() == 0):
        raise AssertionError('egress test peer is unavailable')
    s.update_network_policy(policy)
    if not (egress_probe() != 0):
        raise AssertionError('restored deny-all allowed ordinary egress')
    passed('deny all IPv4 blocks egress while SDK command and file routes stay usable', peer=host, port=port)
    payload = b'control-stream-checkpoint\x00\xff' * 2000
    s.files.write('/tmp/marker', payload)
    if not (s.files.read('/tmp/marker', format='bytes') == payload):
        raise AssertionError()
    command(
        (
            "sh -c 'echo $$ >/tmp/pid; c=0; while true; do c=$((c+1)); ec"
            'ho $c >/tmp/counter.next; mv /tmp/counter.next /tmp/counter;'
            " sleep 0.1; done' >/tmp/counter.log 2>&1 &"
        )
    )
    time.sleep(0.3)
    pid = command('cat /tmp/pid')
    before = int(command('cat /tmp/counter'))
    s.files.write('/tmp/checkpoint-probe', (a.base / 'checkpoint-probe').read_bytes())
    command('chmod +x /tmp/checkpoint-probe')
    record = checkpoint()
    passed(
        'in-runtime Unix checkpoint over control stream, durable metadata and live source',
        checkpoint_id=record['checkpoint']['id'],
        runtime_id=record['runtime']['id'],
        network='deny all IPv4, stateful',
    )
    s.update_network_policy(deny_all('stateless'))
    if not (egress_probe() != 0):
        raise AssertionError('stateless deny-all allowed ordinary egress')
    record = checkpoint()
    passed(
        'stateless deny all IPv4 preserves control stream checkpoint and final Unix acknowledgement',
        checkpoint_id=record['checkpoint']['id'],
    )
    s.files.write('/tmp/marker', b'after-checkpoint')
    s.reload()
    if not (s.files.read('/tmp/marker', format='bytes') == payload):
        raise AssertionError()
    if not (command('cat /tmp/pid') == pid):
        raise AssertionError()
    if not (int(command('cat /tmp/counter')) >= before):
        raise AssertionError()
    restored = json.loads(catalog()['environment:' + s.id])['result']
    if not (restored['runtime']['id'] != record['runtime']['id']):
        raise AssertionError()
    if not (egress_probe() != 0):
        raise AssertionError('reload lost deny-all policy')
    passed('reload refreshes execution identity and reconnects', runtime_id=restored['runtime']['id'])
    paused = s.pause(ttl_seconds=600, timeout_seconds=120)
    if not (paused.size > 0):
        raise AssertionError()
    # Restart only adxlet while paused; preserve Coordinator/Redis/sandboxd.
    subprocess.run(
        [_executable('python3'), str(a.base / 'e2e/firecracker/restart_paused.py'), str(a.run), s.id],
        check=True,
        timeout=120,
    )
    s.resume()
    if not (command('cat /tmp/pid') == pid):
        raise AssertionError()
    if not (s.files.read('/tmp/marker', format='bytes') == payload):
        raise AssertionError()
    if not (egress_probe() != 0):
        raise AssertionError('resume lost deny-all policy')
    passed('pause, adxlet-only restart and resume with new control connection')
    s.kill()
    s.close()
    sid = s.id
    s = None
    deadline = time.monotonic() + 30
    while 'environment:' + sid in catalog():
        if time.monotonic() >= deadline:
            raise TimeoutError('deleted record retained')
        time.sleep(0.1)
    inventory = subprocess.check_output(
        ['/opt/adx-fc/bin/sbox', '-a', str(a.run / 'sandboxd/sandboxd.sock'), 'list'], text=True
    )
    if not (len(inventory.splitlines()) <= 1):
        raise AssertionError(inventory)
    passed('delete releases Redis record and physical backend')
    result['status'] = 'passed'
except Exception as error:
    result['error'] = repr(error)
    traceback.print_exc()
finally:
    if s:
        try:
            s.kill()
        except Exception as error:
            result['cleanup_error'] = repr(error)
        s.close()
    out.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
raise SystemExit(result['status'] != 'passed')
