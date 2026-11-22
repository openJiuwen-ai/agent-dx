"""AFS fresh-state interoperability, with frozen binary identity per role."""

import json
import math
import signal
import time
import traceback
import uuid

from fixtures.common import Blocked, Case, DfsProfile, write_json
from fixtures.multivm import Cluster, NAMES, validate
from suites.operations import peer_counter, permissions, wait_read
from tools.protocol_admission import exercise_admission_lane, protocol_versions
from tools.version_switch import artifact_pair, install, owner_identity, run_phase, running_identity


ROLE_LAYOUTS = {
    'all-old': ('old', 'old', 'old'),
    'all-new': ('new', 'new', 'new'),
    'new-meta': ('new', 'old', 'old'),
    'old-meta': ('old', 'new', 'new'),
    'old-source': ('new', 'old', 'new'),
    'new-source': ('new', 'new', 'old'),
}
DATA_A = b'mixed protocol alpha\n' * 4096
DATA_B = b'mixed protocol beta\n' * 4096


def matrix():
    return {
        f'{mode}.protocol.{operation}.{layout}': (mode, operation, role_versions)
        for mode, operation in (('ownerfs', 'files'), ('dfs', 'peer-read'), ('dfs', 'replica'))
        for layout, role_versions in ROLE_LAYOUTS.items()
    }


def selected_cases(requested):
    available = matrix()
    if requested is None:
        return available
    names = requested.split(',')
    if set(names) - available.keys() or len(set(names)) != len(names):
        raise ValueError('expected comma-separated distinct AFS protocol case IDs: ' + ', '.join(available))
    return {name: available[name] for name in names}


def admission_cases(direction):
    if direction not in ('old-meta', 'new-meta'):
        raise ValueError('protocol admission direction must be old-meta or new-meta')
    healthy = direction.split('-')[0]
    return {
        f'{mode}.protocol.admission.{direction}': (mode, operation, (healthy,) * 3)
        for mode, operation in (('ownerfs', 'files'), ('dfs', 'replica'))
    }


class MixedCluster(Cluster):
    def __init__(self, config, output, case, inputs, identities, role_versions):
        super().__init__(config, inputs['old'], output, case, 'afs-test-' + uuid.uuid4().hex)
        self.version_inputs = inputs
        self.version_identities = identities
        self.role_versions = dict(zip(('M', 'A', 'B'), role_versions))
        if self.roles != ['M', 'A', 'B']:
            raise Blocked('mixed gRPC protocol cases require the M/A/B topology')

    def _configs(self):
        # Cluster.prepare stages the old release on every host. Replace only
        # the designated binaries before either --print-config or startup.
        new_roles = [role for role, version in self.role_versions.items() if version == 'new']
        install(self, self.version_inputs['new'], self.version_identities['new'], roles=new_roles)
        super()._configs()

    def start(self, role):
        report = super().start(role)
        running_identity(self, role, self.version_identities[self.role_versions[role]])
        return report


def registration_status(cluster, role):
    body = json.loads(cluster.health(role))
    status = body['checks']['node_registration']
    ready = status['required'] and status['ready'] and status['session_id']
    if body['id'] != NAMES[role] or not ready:
        raise AssertionError(f'Node-Meta registration not ready for {role}: {body}')
    return status


def registration(cluster, role):
    return cluster.record(f'protocol.registration.{role}', registration_status(cluster, role))


def heartbeat(cluster):
    previous = registration_status(cluster, 'B')
    deadline = time.monotonic() + 18
    while time.monotonic() < deadline:
        time.sleep(1)
        current = registration_status(cluster, 'B')
        if current['session_id'] != previous['session_id']:
            raise AssertionError('Node-Meta heartbeat changed the running B session')
        if current['last_success_age_ms'] < previous['last_success_age_ms']:
            cluster.record('protocol.heartbeat.B', {'before': previous, 'after': current})
            return
        previous = current
    raise AssertionError('no Node-Meta heartbeat renewed the running B session')


def reconnect(cluster):
    previous = registration(cluster, 'B')
    cluster.restart(['B'])
    current = registration(cluster, 'B')
    if current['session_id'] == previous['session_id']:
        raise AssertionError('Node-Meta reconnect reused the previous B session')
    cluster.record('protocol.reconnected.B', {'previous': previous['session_id'], 'current': current['session_id']})
    wait_read(cluster, 'B', 'protocol-seed', DATA_A)


def owner_payload(cluster, role, side, direction):
    metric = 'afs_ownerfiles_payload_bytes_total{'
    expected = {f'side="{side}"', f'direction="{direction}"', 'plane="grpc"'}
    values = []
    for line in cluster.health(role, '/metrics').splitlines():
        if line.startswith(metric):
            labels, _, sample = line[len(metric):].partition('}')
            if set(labels.split(',')) == expected:
                values.append(float(sample.strip()))
    if len(values) != 1 or not math.isfinite(values[0]) or values[0] < 0:
        raise AssertionError(f'expected one OwnerFiles gRPC payload counter for {role}/{side}/{direction}')
    return values[0]


def owner_files(cluster):
    home = owner_identity(cluster)
    if home['home_node_id'] != NAMES['A']:
        raise AssertionError(f'protocol workspace did not choose A as Home: {home}')
    cluster.record('protocol.ownerfs.home', home)
    cluster.write('A', 'protocol-seed', DATA_A)
    for direction, count in (('read', len(DATA_A)), ('write', len(DATA_B))):
        before = (owner_payload(cluster, 'A', 'server', direction), owner_payload(cluster, 'B', 'client', direction))
        if direction == 'read':
            wait_read(cluster, 'B', 'protocol-seed', DATA_A)
        else:
            cluster.write('B', 'protocol-reverse', DATA_B)
            wait_read(cluster, 'A', 'protocol-reverse', DATA_B)
        after = (owner_payload(cluster, 'A', 'server', direction), owner_payload(cluster, 'B', 'client', direction))
        cluster.record('protocol.ownerfs.grpc.' + direction,
                       {'server': 'A', 'client': 'B', 'before': before, 'after': after, 'bytes': count})
        if any(current - old < count for old, current in zip(before, after)):
            raise AssertionError(f'OwnerFiles {direction} did not cross B→A over gRPC: {before} → {after}')


def dfs_peer_read(cluster):
    for source, reader, name, data in (('A', 'B', 'protocol-seed', DATA_A), ('B', 'A', 'protocol-reverse', DATA_B)):
        cluster.write(source, name, data)
        before = peer_counter(cluster, reader)
        wait_read(cluster, reader, name, data)
        after = peer_counter(cluster, reader)
        cluster.record('protocol.dfs.peer_read.' + reader, {'source': source, 'before': before, 'after': after})
        if after <= before:
            raise AssertionError(f'DFS read on {reader} did not exercise the remote peer {source}')


def dfs_replica(cluster):
    # Required two copies, with the local copy required. Only B can hold the
    # other copy. Open on B for the first time after isolating the writer A.
    cluster.write('A', 'protocol-seed', DATA_A)
    cluster.stop('A')
    if 'A' in cluster.channel('A').call('status'):
        raise AssertionError('DFS source isolation failed: writer A is still running')
    cluster.record('protocol.dfs.writer_stopped', 'A')
    wait_read(cluster, 'B', 'protocol-seed', DATA_A)


def registration_pair(cluster):
    for role in ('A', 'B'):
        registration(cluster, role)


def exercise_lane(cluster, operation, phases):
    if cluster.meta_store_toml != 'meta_store = "local-file"\n':
        raise Blocked('mixed gRPC cases require an isolated fresh local-file Meta store')
    run_phase(phases, 'startup', lambda: cluster.prepare(ownerfs_bind=False))
    if phases['startup']['status'] != 'passed':
        return
    run_phase(phases, 'registration', lambda: registration_pair(cluster))
    if cluster.role_versions['A'] == cluster.role_versions['B']:
        run_phase(phases, 'heartbeat', lambda: heartbeat(cluster))
    run_phase(phases, operation, lambda: cluster.case.function(cluster))
    if operation != 'replica' and phases[operation]['status'] == 'passed':
        run_phase(phases, 'reconnect', lambda: reconnect(cluster))
        if cluster.mode == 'ownerfs':
            run_phase(phases, 'permissions', lambda: permissions(cluster))


def exercise(config, old_inputs, new_inputs, output, selection, *, admission=None, admission_versions=None):
    protocols = protocol_versions(admission_versions) if admission else None
    output.mkdir()
    old, new = artifact_pair(old_inputs, new_inputs)
    identities = {'old': old, 'new': new}
    inputs = {'old': old_inputs, 'new': new_inputs}
    write_json(output / 'versions.json', identities)
    entries, errors = [], []
    workloads = {'files': owner_files, 'peer-read': dfs_peer_read, 'replica': dfs_replica}
    for name, (mode, operation, layout) in selection.items():
        case_output = output / name
        case_output.mkdir()
        profile = None
        if operation == 'replica':
            profile = DfsProfile(copies=2, replica_failure='degrade' if admission else 'strict')
        workload = workloads.get(operation)
        if workload is None:
            raise ValueError(f'unknown mixed-protocol operation: {operation}')
        case = Case(name, ('cluster-protocol',), mode, (), workload, profile=profile)
        validate(config, [case])
        cluster = MixedCluster(config, case_output, case, inputs, identities, layout)
        record = {'id': name, 'status': 'failed', 'phases': {}, 'versions': cluster.role_versions}
        entries.append(record)
        phases = record['phases']
        try:
            role_identities = {role: identities[version] for role, version in cluster.role_versions.items()}
        except KeyError as exc:
            raise ValueError(f'unknown mixed-protocol artifact identity: {exc}') from exc
        cluster.record('protocol.versions', role_identities)
        signal.alarm(240)
        try:
            if protocols:
                exercise_admission_lane(cluster, phases, protocols)
            else:
                exercise_lane(cluster, operation, phases)
            if all(p['status'] == 'passed' for p in phases.values()):
                record['status'] = 'passed'
        except (InterruptedError, TimeoutError):
            raise
        except Exception as exc:
            record['error'] = f'{type(exc).__name__}: {exc}'
            record['traceback'] = traceback.format_exc()
            if isinstance(exc, Blocked):
                record['status'] = 'blocked'
        finally:
            signal.alarm(0)
            cleanup = cluster.cleanup()
            if cleanup['status'] != 'PASS':
                record['status'] = 'failed'
                errors.extend(cleanup['errors'])
            write_json(output / 'result.json', {'cases': entries, 'cleanup_errors': errors})
            write_json(output / 'cleanup.json', {'status': 'FAIL' if errors else 'PASS', 'errors': errors})
        if errors:
            break
    return entries, errors
