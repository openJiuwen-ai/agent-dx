"""One continuous installed-SDK workload; two stable AFS mounts and bounded churn."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import threading
import time

from afs_special_sdk import CONFIRMED, _cleanup, _save, run_phase
from afs_sdk import command, perl
from mixed_soak import exercise, verify_assignment
PAYLOAD_STAGE = '/tmp/afs-soak-input'
PAYLOAD_TRANSPORT = 'sdk-stage-short-copy-fsync-v2'


def prepare_payload(environment):
    environment.files.write(PAYLOAD_STAGE, b'')
    command(environment, perl('chmod(0644,shift)==1 or die $!;', PAYLOAD_STAGE, uid=0))


def publish_payload(environment, payload):
    """Keep the 16 KiB data out of the one-hour retained command registry."""
    if len(payload) != 16384:
        raise ValueError('soak publication requires exactly 16 KiB')
    environment.files.write(PAYLOAD_STAGE, payload)
    operation = perl(
        'open(my $r,"<",shift) or die $!;binmode($r);'
        'defined(my $n=read($r,my $d,16384)) or die $!;die "short staging data" unless $n==16384;'
        'defined(my $extra=read($r,my $tail,1)) or die $!;die "trailing staging data" if $extra;close($r) or die $!;'
        'open(my $w,">",shift) or die $!;binmode($w);print $w $d or die $!;'
        '$w->flush() or die $!;$w->sync() or die $!;close($w) or die $!;', PAYLOAD_STAGE, '/afs/soak-current')
    if len(operation.encode()) > 2048:
        raise ValueError('soak publication control command exceeded 2 KiB')
    command(environment, operation)
    return len(operation.encode())


def run(connection, image, config, directory, stop):
    from adx_environment import Environment
    from node import catalog

    state_path = directory / 'state.json'
    progress = directory / 'progress.json'
    control = directory / 'stop.json'
    owned = {}
    handles = []
    started = None
    report = {'status': 'failed', 'reason': 'error', 'errors': [], 'cleanup_errors': [],
              'requested_seconds': config['seconds'], 'elapsed_seconds': 0, 'cycles': 0,
              'operations': {key: 0 for key in ('command', 'file', 'create', 'delete')},
              'verified_bytes': 0, 'instances': {}, 'remaining': [], 'owner': config['owner'],
              'mode': config['mode'], 'qualification_only': config.get('acceptance', 'qualification') != 'one-hour',
              'contract_sha256': config.get('contract_sha256'),
              'payload_transport': PAYLOAD_TRANSPORT, 'maximum_publish_command_bytes': 0,
              'injected_failure_after_cycles': config['fail_after_cycles']}

    def publish():
        report['elapsed_seconds'] = round(time.monotonic() - started, 6) if started is not None else 0
        _save(progress, report)

    def one(environment, marker):
        exercise(environment, marker)
        environment.files.remove('/tmp/adx-soak-' + marker + '.bin')
        for name in ('command', 'file'):
            report['operations'][name] += 1

    def delete(identity):
        _cleanup(connection, {'instances': {'owned': identity}}, Environment, catalog)
        report['operations']['delete'] += 1
        owned.pop(identity)

    try:
        prepared = run_phase(connection, image, config, 'prepare', state_path, directory / 'prepare.json')
        # Preserve IDs even if preparation failed after creating only one Environment.
        report['instances'] = prepared['instances']
        owned.update({identity: True for identity in prepared['instances'].values()})
        if prepared['status'] != 'passed':
            raise AssertionError('SDK mount preparation failed: ' + str(prepared.get('error')))
        report['operations']['create'] += len(owned)
        for role in ('A', 'B'):
            handles.append(Environment.from_id(prepared['instances'][role], connection=connection))
            prepare_payload(handles[-1])
        report['sdk_source'] = prepared['sdk_source']
        report['sdk_version'] = prepared['sdk_version']
        started = time.monotonic()
        report['status'] = 'running'
        deadline = started + config['seconds']
        next_churn = started
        while time.monotonic() < deadline and not stop.is_set() and not control.exists():
            cycle = report['cycles']
            left, right = handles if cycle % 2 == 0 else list(reversed(handles))
            # A fixed small working set prevents the test itself from filling the data disk.
            payload = hashlib.sha256(f'afs-soak-{cycle}'.encode()).digest() * 512
            for handle in handles:
                if handle.files.read('/afs/confirmed', format='bytes') != CONFIRMED:
                    raise AssertionError('previously confirmed content changed')
            size = publish_payload(left, payload)
            report['maximum_publish_command_bytes'] = max(report['maximum_publish_command_bytes'], size)
            if right.files.read('/afs/soak-current', format='bytes') != payload:
                raise AssertionError('cross-role synced content changed')
            report['verified_bytes'] += len(payload)
            one(left, 'anchor-' + str(cycle))
            if time.monotonic() >= next_churn:
                node = 'node1' if report['operations']['create'] % 2 == 0 else 'node2'
                transient = Environment(image=image, runtime='runc', node_id=node, cpu=250, memory=128,
                                        idle_timeout=0, create_timeout=150, connection=connection)
                # Journal the ID before any assertion or work on the new Environment.
                owned[transient.id] = True
                _save(directory / 'owned.json', {'owner': config['owner'], 'instances': list(owned)})
                try:
                    verify_assignment(transient.id, node, catalog)
                    report['operations']['create'] += 1
                    one(transient, 'churn-' + str(cycle))
                    delete(transient.id)
                finally:
                    transient.close()
                next_churn = time.monotonic() + 5
            report['cycles'] += 1
            publish()
            if config['fail_after_cycles'] and report['cycles'] >= config['fail_after_cycles']:
                raise AssertionError('deliberate qualification failure after committed I/O')
            stop.wait(0.25)
        publish()
        cancelled = stop.is_set() or control.exists()
        report['reason'] = 'cancelled' if cancelled else 'duration-complete'
        report['status'] = 'cancelled' if cancelled else 'passed'
    except Exception as exc:
        report['errors'].append(type(exc).__name__ + ': ' + str(exc))
        report['status'] = 'failed'
    finally:
        # Work duration excludes cleanup; anchor deletion is part of lifecycle accounting.
        publish()
        for handle in handles:
            try:
                handle.close()
            except Exception as exc:
                report['cleanup_errors'].append(str(exc))
        # A partially failed prepare still owns the IDs in its persisted state.
        if state_path.exists():
            state = json.loads(state_path.read_text())
            if any(state.get(k) != config[k] for k in ('owner', 'mode', 'special')):
                report['cleanup_errors'].append('foreign prepare state; refusing deletion')
            else:
                owned.update({identity: True for identity in state['instances'].values()
                              if 'environment:' + identity in catalog()})
        for identity in list(reversed(owned)):
            try:
                delete(identity)
            except Exception as exc:
                report['cleanup_errors'].append(str(exc))
        report['remaining'] = list(owned)
        if report['cleanup_errors'] or report['remaining']:
            report['status'] = 'failed'
        _save(directory / 'workload.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, type=Path)
    args = parser.parse_args()
    config = json.loads(args.input.read_text())
    if config['special'] != 'soak' or config['mode'] not in ('ownerfs', 'dfs'):
        parser.error('explicit soak mode required')
    directory = args.input.parent
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda _signum, _frame: stop.set())
    from adx_environment import ConnectionConfig
    secrets = Path('/secrets')
    os.environ['SSL_CERT_FILE'] = str(secrets / 'tls/ca.pem')
    connection = ConnectionConfig(server_address='127.0.0.1:8443', token=(secrets / 'api-key').read_text().strip(),
                                  use_tls=True, verify_tls=True)
    code = 1
    try:
        report = run(connection, (secrets / 'image').read_text().strip(), config, directory, stop)
        code = 0 if report['status'] == 'passed' else 1
    finally:
        _save(directory / 'ended.json', {'owner': config['owner'], 'exit_code': code, 'pid': os.getpid()})
    return code


if __name__ == '__main__':
    raise SystemExit(main())
