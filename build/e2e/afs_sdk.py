"""Installed SDK -> Environment -> real AFS mount acceptance."""

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shlex
import time

SDK_CASES = ('mount-identity', 'bidirectional-publish', 'permissions', 'readonly', 'remove', 'environment-cleanup')


def perl(code, *arguments):
    return ' '.join(shlex.quote(str(v)) for v in ('perl', '-MIO::Handle', '-e', code, *arguments))


def command(instance, code):
    result = instance.commands.run(code, timeout=60)
    if result.exit_code != 0:
        raise AssertionError((instance.id, result.exit_code, result.stdout, result.stderr))
    return result.stdout


def write_synced(instance, path, data):
    command(instance, perl(
        'open(my $f, ">", shift) or die $!;binmode($f);print $f pack("H*",shift) or die $!;'
        '$f->flush() or die $!;$f->sync() or die $!;close($f) or die $!;', path, data.hex(),
    ))


def denied(instance, path, expected, write=False):
    mode = '>' if write else '<'
    command(instance, perl(
        'my $p=shift;my $ok=open(my $f,"' + mode + '",$p);'
        'my $e=0+$!;if($ok){close($f);die "unexpected access";}die "wrong errno $e" unless $e==shift;',
        path, expected,
    ))


def run(connection, image, config, output):
    import adx_environment
    from adx_environment import Environment, Mount
    from node import catalog

    mode = config['mode']
    report = {'status': 'failed', 'mode': mode, 'cases': [], 'instances': [], 'placements': [],
              'cleanup_errors': [], 'sdk_version': importlib.metadata.version('adx-environment'),
              'sdk_source': adx_environment.__file__, 'mounts': {}, 'versions': []}
    if not str(Path(adx_environment.__file__).resolve()).startswith('/opt/adx/client/'):
        raise ValueError('acceptance requires the installed candidate SDK')
    handles, killed = [], set()
    error = None

    def create(role, uid=10000, readonly=False):
        mounts = [] if mode == 'warm' else [Mount(
            target='/afs', host_path=config['paths'][role], options=('ro',) if readonly else ('rw',),
        )]
        handle = Environment(
            image=image, runtime='runc', node_id='node1' if role == 'A' else 'node2',
            cpu=250, memory=128, idle_timeout=0, create_timeout=150, connection=connection,
            mounts=mounts, user=str(uid), group=str(uid),
        )
        handles.append(handle)
        report['instances'].append(handle.id)
        record = json.loads(catalog()['environment:' + handle.id])
        actual = record['assignment']['node_id']
        assert actual == ('node1' if role == 'A' else 'node2'), record['assignment']
        assert command(handle, 'id -u').strip() == str(uid)
        report['placements'].append({'id': handle.id, 'node': actual, 'uid': uid})
        return handle

    def passed(name):
        report['cases'].append({'id': name, 'status': 'passed', 'seconds': 0})

    try:
        left, right = create('A'), create('B')
        if mode == 'warm':
            for handle in (left, right):
                assert command(handle, perl('print "SDK-WARM"')) == 'SDK-WARM'
            passed('runtime-warmup')
        else:
            for role, handle in (('A', left), ('B', right)):
                stat = command(handle, perl('my @s=stat(shift);die $! unless @s;print "$s[0] $s[1]";', '/afs'))
                assert [int(v) for v in stat.split()] == config['identities'][role], stat
                mountinfo = command(handle, 'cat /proc/self/mountinfo')
                lines = [line for line in mountinfo.splitlines() if line.split()[4] == '/afs']
                assert len(lines) == 1, lines
                expected_fs = 'ext4' if mode == 'ownerfs' and role == 'A' else 'fuse'
                assert lines[0].split(' - ', 1)[1].split()[0] == expected_fs, lines
                report['mounts'][role] = {'source': config['paths'][role], 'stat': stat, 'mountinfo': lines[0]}
            passed('mount-identity')
            for generation in range(3):
                data = bytes((i + generation) % 251 for i in range(16384))
                write_synced(left, '/afs/published.bin', data)
                actual = right.files.read('/afs/published.bin', format='bytes')
                assert actual == data
                reverse = data[::-1]
                write_synced(right, '/afs/reverse.bin', reverse)
                assert left.files.read('/afs/reverse.bin', format='bytes') == reverse
                report['versions'].append({'generation': generation, 'bytes': len(actual),
                                           'sha256': hashlib.sha256(actual).hexdigest()})
            left.files.write('/afs/sdk-api.bin', b'sdk-files-api\x00\xff')
            command(left, perl('open(my $f,"+<",shift) or die $!;$f->sync() or die $!;', '/afs/sdk-api.bin'))
            assert right.files.read('/afs/sdk-api.bin', format='bytes') == b'sdk-files-api\x00\xff'
            passed('bidirectional-publish')
            other = create('B', uid=10001)
            denied(other, '/afs/published.bin', 13)
            other.kill()
            killed.add(other.id)
            passed('permissions')
            readonly = create('B', readonly=True)
            denied(readonly, '/afs/forbidden.bin', 30, write=True)
            readonly.kill()
            killed.add(readonly.id)
            passed('readonly')
            left.files.remove('/afs/published.bin')
            denied(right, '/afs/published.bin', 2)
            right.files.remove('/afs/reverse.bin')
            left.files.remove('/afs/sdk-api.bin')
            passed('remove')
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
    finally:
        for handle in reversed(handles):
            try:
                if handle.id not in killed:
                    handle.kill()
                    killed.add(handle.id)
            except Exception as exc:
                report['cleanup_errors'].append(f'{handle.id}: {exc}')
            finally:
                try:
                    handle.close()
                except Exception as exc:
                    report['cleanup_errors'].append(f'{handle.id} close: {exc}')
        deadline = time.monotonic() + 30
        while True:
            remaining = catalog()
            if not any('environment:' + identity in remaining for identity in report['instances']):
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(0.1)
        for identity in report['instances']:
            if 'environment:' + identity in remaining:
                report['cleanup_errors'].append('Environment record remains: ' + identity)
        if not report['cleanup_errors'] and mode != 'warm':
            passed('environment-cleanup')
        report['error'] = error
        report['status'] = 'passed' if not error and not report['cleanup_errors'] else 'failed'
        output.write_text(json.dumps(report, indent=2) + '\n')
    return report


def main():
    from adx_environment import ConnectionConfig
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    secrets = Path('/secrets')
    os.environ['SSL_CERT_FILE'] = str(secrets / 'tls/ca.pem')
    connection = ConnectionConfig(server_address='127.0.0.1:8443', token=(secrets / 'api-key').read_text().strip(),
                                  use_tls=True, verify_tls=True)
    report = run(connection, (secrets / 'image').read_text().strip(), json.loads(args.input.read_text()), args.output)
    print(json.dumps(report), flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
