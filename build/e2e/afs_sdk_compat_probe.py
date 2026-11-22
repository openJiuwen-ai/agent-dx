"""Run frozen Environment SDK operations against a real AFS-backed mount."""

import argparse
from dataclasses import asdict
from hashlib import sha256
import importlib.metadata
from inspect import signature
import json
import os
from pathlib import Path
from posixpath import join as remote_join
import time
import traceback

from afs_sdk import command, denied, perl, write_synced
from node import catalog


def require(condition, *detail):
    if not condition:
        raise AssertionError(*detail)


def environment_for(handles, role):
    handle = handles.get(role)
    if handle is None:
        raise KeyError(role)
    return handle


def verify_mount(handle, role, config):
    stat = command(handle, perl('my @s=stat(shift);die $! unless @s;print "$s[0] $s[1]";', '/afs'))
    require([int(item) for item in stat.split()] == config['identities'][role], stat)
    lines = [line for line in command(handle, 'cat /proc/self/mountinfo').splitlines()
             if line.split()[4] == '/afs']
    require(len(lines) == 1, lines)
    expected_fs = 'ext4' if config['mode'] == 'ownerfs' and role == 'A' else 'fuse'
    require(lines[0].split(' - ', 1)[1].split()[0] == expected_fs, lines)
    return {'stat': stat, 'mountinfo': lines[0], 'source': config['paths'][role]}


def mkdir_capability(handle):
    # Both calls use a path whose parent does not exist.
    parent = '/afs/sdk-compat-nonrecursive'
    leaf = remote_join(parent, 'child')
    require(not handle.files.exists(parent))
    result = {}
    try:
        result['returned'] = handle.files.make_dir(leaf, recursive=False)
    except Exception as exc:
        result['error'] = f'{type(exc).__name__}: {exc}'
    result['parent_exists'] = handle.files.exists(parent)
    result['leaf_exists'] = handle.files.exists(leaf)
    error = result.get('error', '').lower()
    rejected = 'no such file or directory' in error or ('recursive' in error and 'unsupported' in error)
    result['status'] = 'passed' if rejected and not result['parent_exists'] and not result['leaf_exists'] else 'failed'
    # An ordinary mkdir must still work on the same Environment.
    require(handle.files.make_dir('/afs/sdk-compat-default/child') is True)
    require(handle.files.exists('/afs/sdk-compat-default/child'))
    result['default_mkdir'] = 'passed'
    return result


def check_mkdir_capability(handle, evidence, *, required):
    supported = 'recursive' in signature(handle.files.make_dir).parameters
    evidence['sdk_capabilities'] = {'recursive_false': supported}
    if not supported:
        if required:
            evidence['recursive_false'] = {
                'status': 'failed',
                'error': 'required SDK capability recursive-false is missing from Files.make_dir',
            }
        return
    try:
        evidence['recursive_false'] = mkdir_capability(handle)
    except Exception:
        evidence['recursive_false'] = {'status': 'failed', 'error': traceback.format_exc()}


def files(left, right, connection, config, evidence):
    from adx_environment import Environment

    body = bytes(range(251)) * 128
    forward, reverse = '/afs/sdk-compat-a', '/afs/sdk-compat-b'
    write_synced(left, forward, body)
    require(right.files.read(forward, format='bytes') == body)
    write_synced(right, reverse, body[::-1])
    require(left.files.read(reverse, format='bytes') == body[::-1])
    evidence['cross_node'] = {'bytes_each_way': len(body), 'sha256_a': sha256(body).hexdigest(),
                              'sha256_b': sha256(body[::-1]).hexdigest()}
    payload = b'sdk-file-api\x00\xff'
    left.files.write('/afs/sdk-compat-file', payload)
    command(left, perl('open(my $f,"+<",shift) or die $!;$f->sync() or die $!;', '/afs/sdk-compat-file', uid=0))
    require(right.files.read('/afs/sdk-compat-file', format='bytes') == payload)
    attached = Environment.from_id(right.id, connection=connection)
    try:
        info = attached.get_info()
        require(info.id == right.id and info.state == 'running', info)
        require(attached.files.read(forward, format='bytes') == body)
        evidence['reattach'] = asdict(info)
    finally:
        attached.close()
    right.files.remove('/afs/sdk-compat-file')
    denied(left, '/afs/sdk-compat-file', 2, uid=0)
    evidence['file_api'] = {'write_read_sha256': sha256(payload).hexdigest(), 'removed_errno': 2}
    if config.get('replica_seed'):
        write_synced(left, '/afs/sdk-compat-replica', config['replica_seed'].encode() * 2048)
    check_mkdir_capability(left, evidence, required='recursive-false' in config.get('required_capabilities', ()))


def probe(config, report):
    import adx_environment
    from adx_environment import ConnectionConfig, Environment, Mount

    secrets = Path('/secrets')
    os.environ['SSL_CERT_FILE'] = str(secrets / 'tls/ca.pem')
    connection = ConnectionConfig(server_address='127.0.0.1:8443', token=(secrets / 'api-key').read_text().strip(),
                                  use_tls=True, verify_tls=True)
    sdk_version = importlib.metadata.version('adx-environment')
    imported = Path(adx_environment.__file__).resolve()
    require(sdk_version == config['sdk']['version'], sdk_version)
    require(imported.is_relative_to(config['sdk']['install_dir']), str(imported))
    report['sdk'] = {**config['sdk'], 'imported': str(imported)}
    image = (secrets / 'image').read_text().strip()
    handles = {}
    phase = config['phase']
    try:
        for role in ('B',) if phase == 'replica' else ('A', 'B'):
            node = {'A': 'node1', 'B': 'node2'}[role]
            mounts = [] if phase == 'warm' else [Mount(target='/afs', host_path=config['paths'][role],
                                                       options=('rw',))]
            handle = Environment(image=image, runtime='runc', node_id=node, cpu=250, memory=128,
                                 idle_timeout=0, create_timeout=150, connection=connection, mounts=mounts)
            handles[role] = handle
            info = handle.get_info()
            placement = json.loads(catalog()['environment:' + handle.id])['assignment']['node_id']
            require(info.id == handle.id and info.state == 'running' and placement == node, (info, placement))
            report['environments'][role] = {'info': asdict(info), 'node': placement}
            if phase != 'warm':
                report['mounts'][role] = verify_mount(handle, role, config)
        if phase == 'warm':
            require(all(command(handle, perl('print "SDK-WARM"')) == 'SDK-WARM' for handle in handles.values()))
        elif phase == 'replica':
            body = environment_for(handles, 'B').files.read('/afs/sdk-compat-replica', format='bytes')
            require(sha256(body).hexdigest() == config['replica_sha256'], len(body))
            report['replica_read'] = {'reader': 'B', 'bytes': len(body), 'sha256': sha256(body).hexdigest()}
        else:
            files(environment_for(handles, 'A'), environment_for(handles, 'B'), connection, config, report)
    finally:
        for handle in reversed(list(handles.values())):
            try:
                handle.kill()
                handle.close()
            except Exception as exc:
                report['cleanup_errors'].append(f'{handle.id}: {exc}')
        deadline = time.monotonic() + 30
        owned = {'environment:' + handle.id for handle in handles.values()}
        remaining = owned.intersection(catalog())
        while remaining and time.monotonic() < deadline:
            time.sleep(0.25)
            remaining = owned.intersection(catalog())
        report['cleanup_errors'].extend('Environment record remains: ' + item for item in sorted(remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.input.read_text())
    report = {'status': 'failed', 'mode': config['mode'], 'phase': config['phase'],
              'environments': {}, 'mounts': {}, 'cleanup_errors': []}
    try:
        probe(config, report)
        if not report['cleanup_errors']:
            report['status'] = 'passed'
    except Exception:
        report['error'] = traceback.format_exc()
    args.output.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
