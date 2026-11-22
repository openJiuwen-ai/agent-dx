"""Verify real cross-protocol rejection and matching-node recovery."""

from hashlib import sha256
import json
from posixpath import join as remote_join
import re
import time

from fixtures.common import Blocked
from suites.operations import wait_read
from tools.version_switch import install, running_identity, run_phase

REJECTION_CODE = 'ErrorCode(50659329)'
REJECTION_VERSIONS = re.compile(r'unsupported format version ([0-9]+), this binary supports version ([0-9]+)\b')
SEED = b'acknowledged before incompatible B starts\n' * 2048
DURING = b'acknowledged after Meta rejected B\n' * 2048


def protocol_versions(value):
    if not re.fullmatch(r'[1-9][0-9]{0,9},[1-9][0-9]{0,9}', value or ''):
        raise ValueError('admission requires protocol numbers as <baseline>,<candidate>')
    old, new = map(int, value.split(','))
    if old == new or max(old, new) > 0xFFFFFFFF:
        raise ValueError('admission requires two distinct u32 protocol numbers')
    return {'old': old, 'new': new}


def rejection_reason(log, incoming, expected):
    markers = (REJECTION_CODE, 'Node-Meta protocol', 'node-b')
    for line in log.splitlines():
        if not all(marker in line for marker in markers):
            continue
        versions = REJECTION_VERSIONS.search(line)
        if versions and tuple(map(int, versions.groups())) == (incoming, expected):
            return line
    return ''


def sessions(cluster, expected):
    counters = json.loads(cluster.health('M', '/v1/store'))['entities']
    # Both releases store the session by ID and as the Node's current session.
    expected_entries = 2 * expected
    actual = counters.get('node_session', 0)
    cluster.record('admission.sessions', {'entries': actual, 'expected_entries': expected_entries,
                                         'expected_nodes': expected})
    if actual != expected_entries:
        raise AssertionError(f'expected {expected_entries} Node session entries, got {actual}')


def verify_b_unmounted(cluster):
    worker = cluster.channel('B').call('snapshot')
    if any(line.split()[4] == cluster.mount('B') for line in worker['mounts']):
        raise AssertionError('incompatible Node B mounted its filesystem')
    return worker['processes'].get('B')


def reject_b(cluster, healthy, incompatible, protocols):
    identities = cluster.version_identities
    peer = identities[incompatible]
    expected, incoming = protocols[healthy], protocols[incompatible]
    if verify_b_unmounted(cluster) is not None:
        raise AssertionError('incompatible admission probe requires a fresh, stopped B')
    install(cluster, cluster.version_inputs[incompatible], peer, roles=('B',))
    binary = remote_join(cluster.root('B'), 'bin/afs-node')
    observed = cluster.channel('B').call('hash', path=binary)['sha256']
    if observed != peer['binaries']['afs-node']:
        raise AssertionError('wrong incompatible Node B binary')
    cluster.record('admission.attempt.B', {'commit': peer['commit'], 'sha256': observed,
                                          'node_protocol': incoming, 'meta_protocol': expected})
    try:
        cluster.start('B')
    except RuntimeError as exc:
        state = verify_b_unmounted(cluster)
        if state is None or state['returncode'] in (None, 0):
            raise
        log = cluster.python(
            'B', 'from pathlib import Path\nprint(Path(sys.argv[1]).read_text())',
            remote_join(cluster.root('B'), 'B.log'), owner=False,
        )['stdout']
        reason = rejection_reason(log, incoming, expected)
        cluster.record('admission.rejection.B', {'exit': state, 'reason': reason, 'mounted': False})
        if not reason:
            raise AssertionError(f'Node B failed without the expected native protocol refusal: {log[-1500:]}') from exc
        sessions(cluster, 1)
        try:
            cluster.stop('B')
        except RuntimeError:
            if verify_b_unmounted(cluster) is not None:
                raise
        if verify_b_unmounted(cluster) is not None:
            raise AssertionError('rejected Node B was not reaped') from exc
    else:
        raise AssertionError('incompatible Node B reached readiness')


def file_chunk(cluster, body):
    probe = ('from pathlib import Path\n'
             'root=Path(sys.argv[1]); n=int(sys.argv[2]); digest=sys.argv[3]\n'
             'matches=[p.name for p in root.iterdir() if p.is_file() and p.stat().st_size==n '
             'and hashlib.sha256(p.read_bytes()).hexdigest()==digest]\n'
             'print(json.dumps(matches))')
    result = cluster.python('A', probe, remote_join(cluster.role_dir('A'), 'state/dfs/chunks'),
                            len(body), sha256(body).hexdigest(), owner=False)
    chunks = json.loads(result['stdout'])
    if len(chunks) != 1:
        raise AssertionError(f'expected one exact DFS chunk for the confirmed write: {chunks}')
    return chunks[0]


def placement(cluster, body, expected, *, timeout=0):
    chunk = file_chunk(cluster, body)
    wanted = set(expected)
    deadline = time.monotonic() + timeout
    while True:
        result = json.loads(cluster.health('M', f'/v1/dfs/chunks/{chunk}/replication'))
        copies = {copy['record']['location']['Node']['node_id']: copy['available']
                  for copy in result['copies'] if 'Node' in copy['record']['location']}
        nodes = {node for node, available in copies.items() if available}
        if copies.keys() == nodes == wanted or time.monotonic() >= deadline:
            break
        time.sleep(0.5)
    cluster.record('admission.placement', {'chunk': chunk, 'recorded': sorted(copies),
                                          'nodes': sorted(nodes), 'report': result})
    if copies.keys() != wanted or nodes != wanted:
        raise AssertionError(f'expected DFS placement {expected}, found {copies}')


def setup(cluster):
    cluster.prepare(ownerfs_bind=False, start_peers=False)
    sessions(cluster, 1)
    cluster.write('A', 'admission-before', SEED)
    wait_read(cluster, 'A', 'admission-before', SEED)


def continued_service(cluster, a_pid):
    if cluster.channel('A').call('status')['A']['pid'] != a_pid:
        raise AssertionError('healthy Node A restarted during the incompatible admission attempt')
    wait_read(cluster, 'A', 'admission-before', SEED)
    cluster.write('A', 'admission-after-reject', DURING)
    wait_read(cluster, 'A', 'admission-after-reject', DURING)
    sessions(cluster, 1)
    if cluster.mode == 'dfs':
        placement(cluster, DURING, ('node-a',))


def recover(cluster, healthy, replica_data):
    install(cluster, cluster.version_inputs[healthy], cluster.version_identities[healthy], roles=('B',))
    cluster.start('B')
    running_identity(cluster, 'B', cluster.version_identities[healthy])
    cluster.topology()
    sessions(cluster, 2)
    if cluster.mode == 'dfs':
        cluster.write('A', 'admission-replica', replica_data)
        for body in (SEED, DURING, replica_data):
            placement(cluster, body, ('node-a', 'node-b'), timeout=60)
        cluster.stop('A')
        if 'A' in cluster.channel('A').call('status'):
            raise AssertionError('DFS replica read requires writer A to be stopped')
        for name, body in (('admission-before', SEED), ('admission-after-reject', DURING),
                           ('admission-replica', replica_data)):
            wait_read(cluster, 'B', name, body)
            cluster.record('admission.replica_read', {'writer_stopped': 'A', 'reader': 'B', 'name': name,
                                                      'sha256': sha256(body).hexdigest()})
    else:
        wait_read(cluster, 'B', 'admission-before', SEED)
        wait_read(cluster, 'B', 'admission-after-reject', DURING)
        cluster.case.function(cluster)


def exercise_admission_lane(cluster, phases, protocols):
    if cluster.meta_store_toml != 'meta_store = "local-file"\n':
        raise Blocked('protocol admission probes require a fresh private Meta store')
    healthy = cluster.role_versions['M']
    incompatible = 'new' if healthy == 'old' else 'old'
    run_phase(phases, 'same_protocol_baseline', lambda: setup(cluster))
    if phases['same_protocol_baseline']['status'] != 'passed':
        return
    a_pid = cluster.channel('A').call('status')['A']['pid']
    run_phase(phases, 'reject_incompatible_b', lambda: reject_b(cluster, healthy, incompatible, protocols))
    if phases['reject_incompatible_b']['status'] != 'passed':
        return
    run_phase(phases, 'healthy_a_read_write', lambda: continued_service(cluster, a_pid))
    if phases['healthy_a_read_write']['status'] != 'passed':
        return
    run_phase(phases, 'recover_same_protocol_b',
              lambda: recover(cluster, healthy, b'strict placement proof after admission\n' * 2048))
