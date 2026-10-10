"""One live source-field rejection; called by the managed Linux lifecycle driver."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re


PAYLOAD = b'source-rejection-ok\n'
PROOF_NAME = 'source-rejection-proof'


def verify_rejection(response):
    if (response.get('status') != 'ERROR' or response.get('production_ready') is not False
            or not isinstance(response.get('error'), str)
            or not re.match(r"^unknown field [`\"']source[`\"'](?:,|$)", response['error'])):
        raise ValueError('source must be rejected specifically as an unknown field')


def verify_unchanged(before, after):
    def positive_int(value):
        return type(value) is int and value > 0

    status = before.get('status', {})
    if (status.get('state') != 'FinalVerified' or status.get('production_ready') is not False
            or not all(status.get(k) for k in ('container', 'root', 'home_node', 'home_session'))
            or not all(positive_int(status.get(k))
                       for k in ('epoch', 'access_generation'))):
        raise ValueError('missing current verified grant')
    runtime = before.get('runtime', {})
    if (runtime.get('id') != status['container'] or runtime.get('status') != 'running'
            or not positive_int(runtime.get('pid'))
            or runtime.get('pid') != before.get('process', {}).get('pid')):
        raise ValueError('runtime does not match verified container')
    for name in ('process', 'namespace', 'source', 'mount', 'runtime_commands'):
        if not before.get(name):
            raise ValueError('missing identity: ' + name)
    process = before['process']
    if (not all(positive_int(process.get(k)) for k in ('pid', 'starttick'))
            or not isinstance(process.get('boot_id'), str) or not process['boot_id']):
        raise ValueError('invalid process identity')
    for name in ('namespace', 'source'):
        identity = before[name]
        if (not isinstance(identity, dict) or type(identity.get('dev')) is not int
                or identity['dev'] < 0 or not positive_int(identity.get('ino'))):
            raise ValueError('invalid ' + name + ' identity')
    mount = before['mount']
    fields = mount.split() if isinstance(mount, str) else []
    if (len(fields) < 10 or not fields[0].isdigit() or int(fields[0]) <= 0
            or not fields[1].isdigit() or int(fields[1]) <= 0
            or not re.fullmatch(r'\d+:\d+', fields[2]) or fields[4] != '/workspace'
            or '-' not in fields[6:] or fields.index('-') + 3 >= len(fields)):
        raise ValueError('invalid workspace mount row')
    artifacts = before['runtime_commands']
    if (not isinstance(artifacts, dict) or not artifacts
            or any(not isinstance(k, str) or not k.startswith('command-')
                   or not isinstance(v, str) or not re.fullmatch(r'[0-9a-f]{64}', v)
                   for k, v in artifacts.items())):
        raise ValueError('invalid complete runtime command artifact map')
    if before != after:
        raise ValueError('source rejection changed grant, process, mount or runtime commands')


def verify_content(content):
    if content != PAYLOAD:
        raise ValueError('legal exec content mismatch')


def command_artifacts(control):
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(control.glob('command-*')) if p.is_file()}


def snapshot(run, label, container):
    status = run.native(label + '-status', 'status')
    runtime = json.loads(run.command(['/usr/local/sbin/runc', '--root',
                                     run.root / 'control/runtime-state', 'state', container]))
    pid = runtime['pid']
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise ValueError('invalid container pid')
    proc = Path(f'/proc/{pid}')
    starttick = int((proc / 'stat').read_text().rsplit(')', 1)[1].split()[19])
    ns = os.stat(proc / 'ns/mnt')
    source = os.stat(proc / 'root/workspace')
    mounts = []
    for line in (proc / 'mountinfo').read_text().splitlines():
        fields = line.split()
        if fields[4] == '/workspace':
            mounts.append(line)
    if len(mounts) != 1:
        raise ValueError('expected exactly one final workspace mount')
    value = {'status': status,
             'runtime': {k: runtime[k] for k in ('id', 'status', 'pid', 'bundle', 'rootfs') if k in runtime},
             'process': {'pid': pid, 'starttick': starttick,
                         'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()},
             'namespace': {'dev': ns.st_dev, 'ino': ns.st_ino},
             'source': {'dev': source.st_dev, 'ino': source.st_ino},
             'mount': mounts[0], 'runtime_commands': command_artifacts(run.root / 'control')}
    run.save('source-rejection-' + label + '.json', value)
    return value


def execute(run, start, workspace):
    spec = importlib.util.spec_from_file_location('native_source_client', run.args.controller)
    client = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(client)
    before = snapshot(run, 'before', start['container'])
    request = {'operation': 'start', 'id': 'source-injection',
               'workspace': 'workspace', 'source': '/etc'}
    run.save('source-rejection-request.json', request)
    response = client.exchange(run.root / 'control/control.sock', request)
    run.save('source-rejection-response.json', response)
    after = snapshot(run, 'after', start['container'])
    verify_rejection(response)
    verify_unchanged(before, after)
    run.check('source-field-rejected-without-side-effects', True, response)
    proof = workspace / PROOF_NAME
    if proof.exists():
        raise ValueError('proof must be a fresh path')
    result = run.native('source-legal-exec', 'exec', '--', '/bin/sh', '-ec',
                        "set -C; printf 'source-rejection-ok\\n' > /workspace/" + PROOF_NAME
                        + '; /bin/busybox sync -f /workspace/' + PROOF_NAME
                        + '; test "$(/bin/busybox cat /workspace/' + PROOF_NAME + ')" = source-rejection-ok')
    run.check('source-legal-exec', result.get('status') == 'Executed', result)
    content = proof.read_bytes()
    verify_content(content)
    run.check('source-legal-exec-content', True,
              {'size': len(content), 'sha256': hashlib.sha256(content).hexdigest()})
    run.save('source-rejection-result.json', {'status': 'PASS', 'request': request,
             'response': response, 'before': before, 'after': after,
             'content': {'size': len(content), 'sha256': hashlib.sha256(content).hexdigest()},
             'scope': 'source-field rejection only; lifecycle closure is recorded by parent driver'})
