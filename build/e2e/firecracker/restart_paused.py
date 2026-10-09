#!/usr/bin/env python3
import json
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import time


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


P = pathlib.Path(sys.argv[1])
INSTANCE_ID = sys.argv[2]
BASE = pathlib.Path(os.environ.get('ADX_FC_BASE', '/opt/adx'))
B = BASE / 'package/bin'
E = P / 'evidence'
env = {**os.environ, 'REDISCLI_AUTH': (P / 'secrets/redis-key').read_text().strip()}


def catalog():
    return json.loads(
        subprocess.check_output(
            [str(BASE / 'tools/redis-cli'), '--json', 'HGETALL', 'adx:{acceptance}:control:v1'],
            env=env,
            text=True,
            timeout=5,
        )
    )


c = catalog()
key = 'environment:' + INSTANCE_ID
if key not in c:
    raise AssertionError('paused Environment is absent from the authoritative catalog')
records = {key: json.loads(c[key])}
if not (records[key]['result']['state'] == 'Paused' and not records[key]['result']['resources_held']):
    raise AssertionError()
(E / 'catalog-paused.json').write_text(json.dumps(records, indent=2))
for r in records.values():
    artifact = r['result']['checkpoint']['artifact']
    if not (artifact['storage'] == 'shared'):
        raise AssertionError(artifact)
    subprocess.run(
        [
            _executable('python3'),
            str(pathlib.Path(os.environ.get('ADX_FC_BASE', '/opt/adx')) / 'e2e/firecracker/s3_probe.py'),
            str(P),
            '--artifact',
            artifact['location'],
        ],
        check=True,
    )
    if not (not [p for p in (P / 'checkpoints').rglob('*') if p.is_file()]):
        raise AssertionError('pause left local data; remote-only recovery was not established')
inventory = subprocess.check_output(
    ['/opt/adx-fc/bin/sbox', '-a', str(P / 'sandboxd/sandboxd.sock'), 'list'], text=True
)
if not (len(inventory.strip().splitlines()) == 1):
    raise AssertionError(inventory)
(E / 'inventory-paused.txt').write_text(inventory)
status = json.loads(
    subprocess.check_output([str(B / 'adxctl'), 'status', '--config', str(P / 'deployment.yaml')], text=True)
)
node = [s for s in status['services'] if s['role'] == 'adxlet']
if not (len(node) == 1):
    raise AssertionError()
session = json.loads(c['node:node1'])['session']['id']
from orphan_fixture import OrphanFixture

orphan = OrphanFixture(P, artifact["location"], session)
os.kill(node[0]['pid'], signal.SIGKILL)
end = time.monotonic() + 90
while time.monotonic() < end:
    c = catalog()
    n = json.loads(c['node:node1'])
    if n['session']['id'] != session and n['session']['routable'] and n['node']['available']:
        print('PASS Adxlet restarted and reconciled persisted Paused instance', flush=True)
        break
    time.sleep(0.5)
else:
    raise TimeoutError('Adxlet did not reconcile after restart')

orphan.verify()
