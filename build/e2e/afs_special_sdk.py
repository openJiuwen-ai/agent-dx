"""Installed SDK phases across controller faults; remote Environment IDs stay explicit."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.metadata
import json
import logging
import sys
import os
from pathlib import Path
import time

CONFIRMED = b'pk2-confirmed:' * 1024
WORKER_ID = 'afs-special-mounted-io'
WORKER = (
    'my $end=time()+180;open(my $ready,">","/afs/pk2-worker-ready") or die $!;'
    'print $ready "ready" or die $!;$ready->flush() or die $!;$ready->sync() or die $!;'
    'close($ready) or die $!;'
    'while(!-e "/afs/pk2-go"){die "go timeout" if time()>$end;select(undef,undef,undef,0.1);}'
    'my $n=0;while(!-e "/afs/pk2-stop") {'
    'die "workload timeout" if time()>$end;'
    'open(my $r,"<","/afs/confirmed") or die $!;local $/;my $d=<$r>;close($r);'
    'die "confirmed bytes changed" unless $d eq ("pk2-confirmed:" x 1024);'
    'open(my $w,">","/afs/during") or die $!;print $w $d or die $!;'
    '$w->flush() or die $!;$w->sync() or die $!;close($w) or die $!;'
    '$n++;open(my $p,">","/afs/pk2-progress.tmp") or die $!;print $p $n or die $!;'
    '$p->flush() or die $!;$p->sync() or die $!;close($p) or die $!;'
    'rename("/afs/pk2-progress.tmp","/afs/pk2-progress") or die $!;select(undef,undef,undef,0.1);'
    '}die "no workload" unless $n>0;print "MOUNTED_IO_OK $n";'
)


def save_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def owned_state(config, path):
    if not path.exists():
        return {'owner': config['owner'], 'mode': config['mode'], 'special': config['special'], 'instances': {}}
    state = json.loads(path.read_text())
    if any(state.get(k) != config[k] for k in ('owner', 'mode', 'special')):
        raise ValueError('special state belongs to a different run/mode')
    if (not set(state.get('instances', {})) <= {'A', 'B'}
            or len(set(state['instances'].values())) != len(state['instances'])):
        raise ValueError('special Environment role identity is ambiguous')
    return state


def cleanup_environments(connection, state, environment, catalog):
    """from_id handles are detached: kill() must never stand in for explicit delete()."""
    for identity in reversed(list(state['instances'].values())):
        if 'environment:' + identity in catalog():
            environment.delete(identity, connection=connection)
    deadline = time.monotonic() + 30
    while True:
        remaining = [v for v in state['instances'].values() if 'environment:' + v in catalog()]
        if not remaining:
            return
        if time.monotonic() >= deadline:
            raise ValueError('special Environment records remain: ' + repr(remaining))
        time.sleep(0.1)


def pressure_worker(connection, instances, worker):
    from adx_environment import Environment
    from afs_sdk import command, perl
    handles = []
    results = []
    try:
        for role in ('A', 'B'):
            handles.append(Environment.from_id(instances[role], connection=connection))
        left, right = handles if worker % 2 == 0 else reversed(handles)
        for generation in range(8):
            path = f'/afs/pressure-{worker}-{generation}'
            data = bytes((n + worker + generation) % 251 for n in range(65536))
            left.files.write(path, data)
            command(left, perl('open(my $f,"+<",shift) or die $!;$f->sync() or die $!;close($f) or die $!;',
                               path, uid=0))
            actual = right.files.read(path, format='bytes')
            if actual != data:
                raise AssertionError('pressure content mismatch: ' + path)
            results.append({'worker': worker, 'generation': generation, 'bytes': len(actual),
                            'sha256': hashlib.sha256(actual).hexdigest()})
            right.files.remove(path)
        return results
    finally:
        for handle in handles:
            handle.close()


def run_phase(connection, image, config, phase, state_path, output):
    import adx_environment
    from adx_environment import Environment, Mount
    from afs_sdk import command, perl, write_synced
    from node import catalog
    if not str(Path(adx_environment.__file__).resolve()).startswith('/opt/adx/client/'):
        raise ValueError('special requires the installed candidate SDK')
    if phase == 'prepare' and state_path.exists():
        raise ValueError('special prepare state already exists; refusing replay')
    state = owned_state(config, state_path)
    report = {'status': 'failed', 'phase': phase, 'mode': config['mode'], 'special': config['special'],
              'cases': [], 'cleanup_errors': [], 'sdk_source': adx_environment.__file__,
              'sdk_version': importlib.metadata.version('adx-environment')}
    handles = []

    def passed(name):
        report['cases'].append({'id': name, 'status': 'passed'})
    try:
        if phase == 'cleanup':
            if config['special'] == 'restart' and 'B' in state['instances']:
                try:
                    observer = Environment.from_id(state['instances']['B'], connection=connection)
                    handles.append(observer)
                    result = observer.commands.get(WORKER_ID).wait(timeout=0.1)
                    report['background_diagnostic'] = {
                        'exit_code': result.exit_code, 'stdout': result.stdout, 'stderr': result.stderr,
                    }
                except Exception as exc:
                    report['background_diagnostic'] = {'unavailable': str(exc)}
            cleanup_environments(connection, state, Environment, catalog)
            passed('environment-cleanup')
        elif phase == 'prepare':
            save_json(state_path, state)
            for role in ('A', 'B'):
                handle = Environment(image=image, runtime='runc', node_id='node1' if role == 'A' else 'node2',
                                     cpu=250, memory=128, idle_timeout=0, create_timeout=150, connection=connection,
                                     mounts=[Mount(target='/afs', host_path=config['paths'][role], options=('rw',))])
                handles.append(handle)
                state['instances'][role] = handle.id
                save_json(state_path, state)
                assignment = json.loads(catalog()['environment:' + handle.id])['assignment']['node_id']
                if not (assignment == ('node1' if role == 'A' else 'node2')):
                    raise AssertionError('required test condition failed')
                if not (command(handle, perl('print $>;')).strip() == '10000'):
                    raise AssertionError('required test condition failed')
                actual = command(handle, perl('my @s=stat("/afs");die $! unless @s;print "$s[0] $s[1]";'))
                if not ([int(v) for v in actual.split()] == config['identities'][role]):
                    raise AssertionError('required test condition failed')
                mounts = [line for line in command(handle, 'cat /proc/self/mountinfo').splitlines()
                          if line.split()[4] == '/afs']
                expected = 'ext4' if config['mode'] == 'ownerfs' and role == 'A' else 'fuse'
                if not (len(mounts) == 1 and mounts[0].split(' - ', 1)[1].split()[0] == expected):
                    raise AssertionError(mounts)
            passed('mount-identity')
            write_synced(handles[0], '/afs/confirmed', CONFIRMED)
            if not (handles[1].files.read('/afs/confirmed', format='bytes') == CONFIRMED):
                raise AssertionError('required test condition failed')
            if config['special'] == 'restart':
                passed('confirmed-before-restart')
                handles[1].commands.run(perl(WORKER), background=True, timeout=180, command_id=WORKER_ID)
                deadline = time.monotonic() + 15
                while True:
                    result = handles[1].commands.get(WORKER_ID).wait(timeout=0.01)
                    if result.exit_code is not None:
                        raise AssertionError(('background exited before fault', result.exit_code,
                                              result.stdout, result.stderr))
                    ready = command(handles[1], perl('print(-e "/afs/pk2-worker-ready" ? "1" : "0");'))
                    if ready == '1':
                        break
                    if time.monotonic() >= deadline:
                        raise ValueError('background SDK workload did not acknowledge startup')
                    time.sleep(0.1)
        else:
            if set(state['instances']) != {'A', 'B'}:
                raise ValueError('special needs exactly two previously created Environments')
            if phase == 'verify':
                for role in ('A', 'B'):
                    handles.append(Environment.from_id(state['instances'][role], connection=connection))
                result = handles[1].commands.get(WORKER_ID).wait(timeout=30)
                if not (result.exit_code == 0 and result.stdout.startswith('MOUNTED_IO_OK ')):
                    raise AssertionError((
                    result.exit_code, result.stdout, result.stderr))
                for handle in handles:
                    if not (handle.files.read('/afs/confirmed', format='bytes') == CONFIRMED):
                        raise AssertionError('required test condition failed')
                    if not (handle.files.read('/afs/during', format='bytes') == CONFIRMED):
                        raise AssertionError('required test condition failed')
                passed('confirmed-after-restart')
                after = b'after-restart\x00\xff' * 512
                write_synced(handles[1], '/afs/after-restart', after)
                if not (handles[0].files.read('/afs/after-restart', format='bytes') == after):
                    raise AssertionError('required test condition failed')
                passed('publish-after-restart')
                report['background_exit_code'] = result.exit_code
            elif phase == 'pressure':
                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures = [pool.submit(pressure_worker, connection, state['instances'], n) for n in range(4)]
                    results = [item for future in futures for item in future.result()]
                if not (len(results) == 32):
                    raise AssertionError('required test condition failed')
                passed('bounded-concurrent-publish')
                handle = Environment.from_id(state['instances']['A'], connection=connection)
                handles.append(handle)
                errno = command(handle, perl(
                    'use Fcntl qw(O_WRONLY O_CREAT O_EXCL);my $p="/afs/exclusive";'
                    'sysopen(my $a,$p,O_WRONLY|O_CREAT|O_EXCL,0600) or die $!;close($a) or die $!;'
                    'my $ok=sysopen(my $b,$p,O_WRONLY|O_CREAT|O_EXCL,0600);my $e=0+$!;'
                    'die "exclusive create unexpectedly succeeded" if $ok;'
                    'die "wrong errno $e" unless $e==17;unlink($p) or die $!;print $e;'))
                if not (errno.strip() == '17'):
                    raise AssertionError('required test condition failed')
                passed('exclusive-create-error')
                verified = sum(v['bytes'] for v in results)
                if not (verified == 4 * 8 * 65536):
                    raise AssertionError('required test condition failed')
                report['pressure'] = {'workers': 4, 'operations': len(results), 'payload_bytes': 65536,
                                      'verified_bytes': verified, 'exclusive_errno': int(errno), 'errors': [],
                                      'files': results, 'capacity_exhaustion_claimed': False,
                                      'performance_acceptance': False}
                passed('bounded-workload')
            else:
                raise ValueError('unknown special SDK phase')
        report['status'] = 'passed'
    except Exception as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        if phase == 'prepare':
            try:
                cleanup_environments(connection, state, Environment, catalog)
            except Exception as error:
                report['cleanup_errors'].append(str(error))
    finally:
        for handle in handles:
            try:
                handle.close()
            except Exception as exc:
                report['cleanup_errors'].append(str(exc))
        report['instances'] = state['instances']
        if report['cleanup_errors']:
            report['status'] = 'failed'
        save_json(output, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--phase', required=True, choices=('prepare', 'verify', 'pressure', 'cleanup'))
    args = parser.parse_args()
    config = json.loads(args.input.read_text())
    if config.get('special') not in ('restart', 'pressure') or config.get('mode') not in ('ownerfs', 'dfs'):
        parser.error('explicit supported special and AFS mode required')
    from adx_environment import ConnectionConfig
    secrets = Path('/secrets')
    os.environ['SSL_CERT_FILE'] = str(secrets / 'tls/ca.pem')
    connection = ConnectionConfig(server_address='127.0.0.1:8443', token=(secrets / 'api-key').read_text().strip(),
                                  use_tls=True, verify_tls=True)
    state = Path('/evidence') / ('afs-special-' + config['special'] + '-' + config['mode'] + '-state.json')
    report = run_phase(connection, (secrets / 'image').read_text().strip(), config, args.phase, state, args.output)
    logging.basicConfig(level=logging.INFO, format='%(message)s', stream=sys.stdout, force=True)
    logging.info('%s', json.dumps(report))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
