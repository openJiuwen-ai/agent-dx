"""Declared fixed-package specials; functional assertions, never an HA/performance SLA."""

import json
import math
from pathlib import Path
import sys
import tempfile
import time
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
COMPONENT_CASES = (
    'ownerfs.cluster.restart', 'dfs.cluster.restart',
    'dfs.write.cluster.bounded_sync_progress', 'dfs.write.cluster.required_replica_recovery',
    'dfs.write.cluster.failed_close_retirement',
)
CAMPAIGN_CASES = ('afs-component-specials', 'afs-sdk-restart', 'afs-sdk-pressure')
SDK_CHECKS = {
    'restart': ('mount-identity', 'confirmed-before-restart', 'workload-during-control-pause',
                'adxlet-session-changed', 'backend-identity-preserved', 'confirmed-after-restart',
                'publish-after-restart', 'environment-cleanup'),
    'pressure': ('mount-identity', 'bounded-concurrent-publish', 'exclusive-create-error',
                 'bounded-workload', 'environment-cleanup'),
}


def validate_sdk(report, mode, special):
    cases = report.get('cases', [])
    if (report.get('status') != 'passed' or report.get('mode') != mode or report.get('special') != special
            or report.get('cleanup_errors') or len(cases) != len(SDK_CHECKS[special])
            or {c.get('id') for c in cases} != set(SDK_CHECKS[special])
            or any(c.get('status') != 'passed' for c in cases)):
        raise ValueError('special SDK assertions or normal cleanup are incomplete')
    if mode == 'dfs':
        peer = report.get('peer_reads', {})
        values = (peer.get('before'), peer.get('after'))
        if (any(not isinstance(v, (int, float)) or isinstance(v, bool) or not math.isfinite(v) or v < 0
                for v in values) or values[1] <= values[0]):
            raise ValueError('DFS special did not prove the remote peer read path')
    if special == 'pressure':
        p = report.get('pressure', {})
        if (any(p.get(k) != v for k, v in {'workers': 4, 'operations': 32, 'payload_bytes': 65536,
                                           'verified_bytes': 2097152, 'exclusive_errno': 17}.items())
                or p.get('errors') != []):
            raise ValueError('bounded pressure did not complete its exact workload and errno checks')
    else:
        restart = report.get('restart', {})
        if set(restart) != {'node1', 'node2'}:
            raise ValueError('both Adxlet restart identities required')
        for value in restart.values():
            if (not isinstance(value.get('before_pid'), int) or value['before_pid'] <= 1
                    or not isinstance(value.get('after_pid'), int) or value['after_pid'] <= 1
                    or value['before_pid'] == value['after_pid'] or not value.get('before_session')
                    or not value.get('after_session') or value['before_session'] == value['after_session']
                    or not value.get('before_backend') or value['before_backend'] != value.get('after_backend')):
                raise ValueError('Adxlet process/session did not change or its live backend was replaced')
        workload = report.get('workload', {})
        if (workload.get('paused_state') != 'T' or workload.get('paused_pid') != restart['node2']['before_pid']
                or workload.get('before_restart', 0) < 1
                or workload.get('at_ready', -1) < workload.get('before_restart', 0)
                or workload.get('after_restart', 0) <= workload.get('at_ready', 0)):
            raise ValueError('SDK-launched mounted workload did not progress during pause and after restart')


def _phase(product, config, phase, output):
    run = product.run
    prefix = 'afs-special-' + config['special'] + '-' + config['mode']
    remote_input, remote_output = '/evidence/' + prefix + '-input.json', '/evidence/' + prefix + '-' + phase + '.json'
    run.execute('node1', 'python3', '-c',
                'from pathlib import Path;import sys;Path(sys.argv[1]).write_text(sys.argv[2])',
                remote_input, json.dumps(config))
    try:
        run.execute('node1', '/opt/adx/client/bin/python', '-B', '/opt/adx/e2e/afs_special_sdk.py',
                    '--input', remote_input, '--phase', phase, '--output', remote_output, timeout=600)
    finally:
        product.copy_from('node1', remote_output, output / ('sdk-' + phase + '.json'))
    return json.loads((output / ('sdk-' + phase + '.json')).read_text())


def _progress(cluster, minimum):
    deadline = time.monotonic() + 30
    while True:
        result = cluster.python('B',
            'from pathlib import Path\np=Path(sys.argv[1]);print(p.read_text() if p.exists() else "0")',
            cluster.workspace('B') + '/pk2-progress')
        value = int(result['stdout'])
        if value > minimum:
            return value
        if time.monotonic() >= deadline:
            raise ValueError('mounted background workload did not make the required progress')
        time.sleep(0.1)


def _restart(product, cluster):
    run = product.run
    run.helper('node1', 'sessions')
    paused = None
    try:
        run.helper('node2', 'freeze', 'node2')
        paused = json.loads(run.execute('node2', 'python3', '-c',
            'from pathlib import Path;import json;pid=int(Path("/tmp/adx-e2e/frozen-manager.pid").read_text());'
            'state=Path(f"/proc/{pid}/stat").read_text().rsplit(")",1)[1].split()[0];'
            'assert state=="T",state;print(json.dumps({"paused_pid":pid,"paused_state":state}))'))
        cluster.write('A', 'pk2-go', b'go')
        paused['before_restart'] = _progress(cluster, 0)
        # Confirm it is still paused after the workload has committed real I/O.
        run.execute('node2', 'python3', '-c',
                    'from pathlib import Path;import sys;'
                    'assert Path(f"/proc/{sys.argv[1]}/stat").read_text().rsplit(")",1)[1].split()[0]=="T"',
                    str(paused['paused_pid']))
    finally:
        run.helper('node2', 'thaw', 'node2')
    for node in run.nodes:
        run.helper(node, 'restart', node)
    run.helper('node1', 'ready', 'restart', timeout=120)
    identities = {}
    for node in run.nodes:
        run.helper(node, 'adxlet-session-changed', node, timeout=90)
        run.helper(node, 'unchanged', node)
        before = json.loads((run.output / ('adxlet-before-' + node + '.json')).read_text())
        after = json.loads((run.output / ('adxlet-after-' + node + '.json')).read_text())
        identities[node] = {
            'before_pid': before['pid'], 'after_pid': after['pid_after'],
            'before_session': before['session_id'], 'after_session': after['session_after'],
            'before_backend': json.loads((run.output / ('backend-before-' + node + '.json')).read_text()),
            'after_backend': json.loads((run.output / ('backend-after-' + node + '.json')).read_text()),
        }
    paused['at_ready'] = _progress(cluster, paused['before_restart'] - 1)
    paused['after_restart'] = _progress(cluster, paused['at_ready'])
    cluster.write('A', 'pk2-stop', b'stop')
    return identities, paused


def scenarios(product, checks, special):
    """Keep original AFS fixture ownership/cleanup and installed-SDK helpers."""
    run = product.run
    product.sdk({'mode': 'warm'}, run.output / 'afs-sdk-warm.json')
    path = str(ROOT / 'afs/tests')
    sys.path.insert(0, path)
    try:
        from fixtures.common import Case
        from fixtures.multivm import Cluster
        from suites.operations import peer_counter
        with tempfile.TemporaryDirectory(prefix='adx-special-afs-') as temporary:
            inputs = Path(temporary)
            for name in ('afs-meta', 'afs-node', 'artifact-identity.json'):
                base = '/opt/adx/e2e/afs-tests/inputs/' if name.endswith('.json') else '/opt/adx/package/bin/'
                product.copy_from('node1', base + name, inputs / name)
                if not name.endswith('.json'):
                    (inputs / name).chmod(0o755)
            for mode in ('ownerfs', 'dfs'):
                output = run.output / 'afs-specials' / special / mode
                output.mkdir(parents=True)
                case = Case('afs.sdk.' + special + '.' + mode, ('cluster-sdk',), mode, (), None)
                cluster = Cluster(product.config, inputs, output, case, 'afs-test-' + uuid.uuid4().hex)
                product.active = cluster
                config = {'owner': run.id, 'special': special, 'mode': mode}
                report = dict(config, status='failed', cases=[], cleanup_errors=[])
                with run.case('afs-sdk-' + mode + '-' + special, checks) as record:
                    try:
                        cluster.prepare()
                        peer_before = peer_counter(cluster, 'B') if mode == 'dfs' else None
                        config['paths'] = {r: cluster.workspace(r) for r in ('A', 'B')}
                        config['identities'] = {}
                        for role in ('A', 'B'):
                            value = cluster.python(role,
                                'from pathlib import Path\npid=int(Path("/tmp/adx-e2e/sandboxd.pid").read_text());'
                                'assert os.readlink("/proc/self/ns/mnt")==os.readlink(f"/proc/{pid}/ns/mnt");'
                                's=os.stat(sys.argv[1]);print(json.dumps([s.st_dev,s.st_ino]))',
                                cluster.workspace(role), owner=False)
                            config['identities'][role] = json.loads(value['stdout'])
                        prepared = _phase(product, config, 'prepare', output)
                        report['cases'] += prepared['cases']
                        report['instances'] = prepared['instances']
                        if special == 'restart':
                            report['restart'], report['workload'] = _restart(product, cluster)
                            report['cases'] += [{'id': n, 'status': 'passed'} for n in (
                                'workload-during-control-pause', 'adxlet-session-changed',
                                'backend-identity-preserved')]
                            report['cases'] += _phase(product, config, 'verify', output)['cases']
                        else:
                            pressure = _phase(product, config, 'pressure', output)
                            report['cases'] += pressure['cases']
                            report['pressure'] = pressure['pressure']
                        if mode == 'dfs':
                            peer_after = peer_counter(cluster, 'B')
                            report['peer_reads'] = {'before': peer_before, 'after': peer_after}
                            if peer_after <= peer_before:
                                raise ValueError('DFS special never read from its remote peer')
                            cluster.record('sdk.peer_reads', report['peer_reads'])
                    except Exception as exc:
                        report['error'] = f'{type(exc).__name__}: {exc}'
                    finally:
                        try:
                            cleanup = _phase(product, config, 'cleanup', output)
                            report['cases'] += cleanup['cases']
                            for node in run.nodes:
                                run.helper(node, 'empty', node)
                        except Exception as exc:
                            product.cleanup_errors.append('SDK special Environment cleanup: ' + str(exc))
                            for node in reversed(run.nodes):
                                try:
                                    run.helper(node, 'cleanup', node, timeout=180)
                                except Exception as error:
                                    product.cleanup_errors.append('runtime cleanup: ' + str(error))
                        cleanup = cluster.cleanup()
                        product.active = None
                        if cleanup['status'] != 'PASS':
                            product.cleanup_errors += cleanup['errors'] or ['normal AFS cleanup unconfirmed']
                        report['cleanup_errors'] = list(product.cleanup_errors)
                        report['status'] = 'failed' if report.get('error') or product.cleanup_errors else 'passed'
                        (output / 'sdk-result.json').write_text(json.dumps(report, indent=2) + '\n')
                        record['subcases'] = report['cases']
                    validate_sdk(report, mode, special)
    finally:
        sys.path.remove(path)


def verify_part(directory, case):
    """Zero exit and container deletion cannot substitute for declared assertions."""
    directory = Path(directory)
    cleanup = json.loads((directory / 'docker-cleanup.json').read_text())
    if cleanup.get('status') != 'PASS' or cleanup.get('errors'):
        raise ValueError('Docker normal cleanup unconfirmed')
    if case == 'afs-component-specials':
        from afs_component import validate_result
        validate_result(json.loads((directory / 'afs/result.json').read_text()),
                        json.loads((directory / 'afs/cleanup.json').read_text()), case_ids=COMPONENT_CASES)
    else:
        special = case.removeprefix('afs-sdk-')
        cleanup = json.loads((directory / 'afs-sdk-cleanup.json').read_text())
        if cleanup.get('status') != 'PASS' or cleanup.get('errors'):
            raise ValueError('SDK special cleanup unconfirmed')
        for mode in ('ownerfs', 'dfs'):
            root = directory / 'afs-specials' / special / mode
            validate_sdk(json.loads((root / 'sdk-result.json').read_text()), mode, special)
            normal = json.loads((root / 'cleanup.json').read_text())
            if normal.get('status') != 'PASS' or normal.get('errors'):
                raise ValueError('AFS special normal cleanup unconfirmed')
