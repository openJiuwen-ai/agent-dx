"""Verify real cross-protocol rejection and matching-node recovery."""

from hashlib import sha256
import json
from posixpath import join as remote_join
import time

from fixtures.common import Blocked
from suites.operations import wait_read
from tools.version_switch import install, running_identity, run_phase

FROZEN_PROTOCOLS = {
    '18f9c673812dbc988185136011023ea28bdd38f7': 7,
    'be16d72463bb30ae355178795fb4476adacf4c6d': 10,
}
REJECTION_CODE = 'ErrorCode(50659329)'
SEED = b'acknowledged before incompatible B starts\n' * 2048
DURING = b'acknowledged after Meta rejected B\n' * 2048


def sessions(cluster, expected):
    counters = json.loads(cluster.health('M', '/v1/store'))['entities']
    actual = counters.get('node_session', 0)
    cluster.record('admission.sessions', {'actual': actual, 'expected': expected})
    if actual != expected:
        raise AssertionError(f'expected {expected} registered Node sessions, got {actual}')


def verify_b_unmounted(cluster):
    worker = cluster.channel('B').call('snapshot')
    if any(line.split()[4] == cluster.mount('B') for line in worker['mounts']):
        raise AssertionError('incompatible Node B mounted its filesystem')
    return worker['processes'].get('B')


def reject_b(cluster, healthy, incompatible):
    identities = cluster.version_identities
    local = identities[healthy]
    peer = identities[incompatible]
    expected, incoming = (FROZEN_PROTOCOLS[item['commit']] for item in (local, peer))
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
    except RuntimeError:
        state = verify_b_unmounted(cluster)
        if state is None or state['returncode'] in (None, 0):
            raise
        log = cluster.python(
            'B', 'from pathlib import Path\nprint(Path(sys.argv[1]).read_text())',
            remote_join(cluster.root('B'), 'B.log'), owner=False,
        )['stdout']
        markers = (REJECTION_CODE, 'Node-Meta protocol', 'node-b',
                   f'unsupported format version {incoming}', f'supports version {expected}')
        reason = next((line for line in log.splitlines() if all(marker in line for marker in markers)), '')
        cluster.record('admission.rejection.B', {'exit': state, 'reason': reason, 'mounted': False})
        if not reason:
            raise AssertionError(f'Node B failed without the expected native protocol refusal: {log[-1500:]}')
        sessions(cluster, 1)
        try:
            cluster.stop('B')
        except RuntimeError:
            if verify_b_unmounted(cluster) is not None:
                raise
        if verify_b_unmounted(cluster) is not None:
            raise AssertionError('rejected Node B was not reaped')
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


def exercise_admission_lane(cluster, phases):
    if cluster.meta_store_toml != 'meta_store = "local-file"\n':
        raise Blocked('protocol admission probes require a fresh private Meta store')
    healthy = cluster.role_versions['M']
    incompatible = 'new' if healthy == 'old' else 'old'
    if set(identity['commit'] for identity in cluster.version_identities.values()) != set(FROZEN_PROTOCOLS):
        raise ValueError('protocol admission probes require the exact #280 and #294 artifacts')
    run_phase(phases, 'same_protocol_baseline', lambda: setup(cluster))
    if phases['same_protocol_baseline']['status'] != 'passed':
        return
    a_pid = cluster.channel('A').call('status')['A']['pid']
    run_phase(phases, 'reject_incompatible_b', lambda: reject_b(cluster, healthy, incompatible))
    if phases['reject_incompatible_b']['status'] != 'passed':
        return
    run_phase(phases, 'healthy_a_read_write', lambda: continued_service(cluster, a_pid))
    if phases['healthy_a_read_write']['status'] != 'passed':
        return
    run_phase(phases, 'recover_same_protocol_b',
              lambda: recover(cluster, healthy, b'strict placement proof after admission\n' * 2048))
