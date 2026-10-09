#!/usr/bin/env python3
"""Restart an already-stopped host-bind fixture without recreating its confirmed data."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import stat
import tomllib

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('epoch_case', HERE / 'workspace-bind-epoch-linux.py')
epoch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(epoch)
host = epoch.host
PAYLOAD = bytes(range(256)) * 16
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()


def proof(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        data = os.pread(descriptor, 4096, 0)
        eof = os.pread(descriptor, 1, 4096) == b''
        info = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    value = dict(size=info.st_size, sha256=hashlib.sha256(data).hexdigest(),
        content_matches=data == PAYLOAD, eof=eof, mode=stat.S_IMODE(info.st_mode),
        uid=info.st_uid, gid=info.st_gid, device=info.st_dev, inode=info.st_ino)
    if not stat.S_ISREG(info.st_mode) or not value['content_matches'] or not eof or info.st_size != 4096:
        raise ValueError('pre-existing complete 4096B proof/EOF missing')
    return value


def verify_proof(before, after):
    if (before != after or before.get('size') != 4096 or before.get('sha256') != DIGEST
            or before.get('content_matches') is not True or before.get('eof') is not True):
        raise ValueError('confirmed data, EOF, ownership, mode or physical inode changed')


def verify_session(previous, replacement, current):
    epoch.verify_transition(previous, current)
    actual = current['checks']['node_registration']
    replaced = replacement['checks']['node_registration']
    if (type(replaced['lease_epoch']) is not int or actual['lease_epoch'] <= replaced['lease_epoch']
            or actual['session_id'] == replaced['session_id']
            or current['checks']['meta_persistence']['persistent_ready'] is not True):
        raise ValueError('fresh recovered persistent session must exceed both prior epochs')


def immutable_inputs(root):
    return {str(p.relative_to(root)): host.checks.sha(p)
            for directory in ('etc', 'prefix') for p in sorted((root / directory).rglob('*')) if p.is_file()}


class Run(epoch.Run):
    def inputs(self):
        expected = json.loads(self.args.inputs.read_text())
        actual = {name: host.checks.sha(HERE / name) for name in expected['tools']}
        self.check('exact-tool-inputs', actual == expected['tools'], actual)
        self.check('tool-map-sha', host.checks.sha(self.args.inputs) == self.args.inputs_sha256,
                   host.checks.sha(self.args.inputs))
        return expected

    def preflight(self):
        self.check('Linux-ARM64-root', platform.system() == 'Linux'
                   and platform.machine() == 'aarch64' and os.geteuid() == 0, platform.uname()._asdict())
        needed = ('bash', 'python3', 'openssl', 'findmnt', 'fusermount3', 'ldd', 'sha256sum',
                  'ss', 'curl', 'flock', 'sed', 'awk', 'grep', 'mountpoint', 'readlink', 'setsid', 'timeout')
        dependencies = {name: shutil.which(name) for name in needed}
        self.check('dependencies', all(dependencies.values()), dependencies)
        self.check('owned-existing-root', self.root.parent == Path('/opt') and self.root.is_dir()
                   and self.args.transport.parent == Path('/var/tmp')
                   and self.out.is_relative_to(self.args.transport), str(self.root))
        self.inputs()
        self.check('previous-index-sha', host.checks.sha(self.args.previous_index) == self.args.previous_index_sha256,
                   host.checks.sha(self.args.previous_index))
        index = json.loads(self.args.previous_index.read_text())['files']
        for name in ('result.json', 'original-health.json', 'replacement-health.json',
                     'original-identity.json', 'binding-before.json', 'original-authority-exit.json'):
            path = self.args.previous_results / name
            self.check('previous-' + name, host.checks.sha(path) == index['r2/results/' + name], host.checks.sha(path))
        self.previous = json.loads((self.args.previous_results / 'original-health.json').read_text())
        self.replaced = json.loads((self.args.previous_results / 'replacement-health.json').read_text())
        self.previous_identity = json.loads((self.args.previous_results / 'original-identity.json').read_text())
        previous_result = json.loads((self.args.previous_results / 'result.json').read_text())
        self.check('same-runtime-source', previous_result['source_commit'] == self.args.source_commit,
                   previous_result)
        binding = json.loads((self.args.previous_results / 'binding-before.json').read_text())
        self.source = Path(binding['source'])
        self.check('existing-physical-Home', self.source.is_relative_to(self.root / 'state/node/ownerfs')
                   and not self.source.is_symlink() and self.source.is_dir(), binding)
        self.before_source_identity = [self.source.stat().st_dev, self.source.stat().st_ino]
        self.check('same-archived-physical-directory', self.before_source_identity == binding['source_identity'],
                   self.before_source_identity)
        self.before_proof = proof(self.source / 'epoch-proof')
        self.save('proof-before.json', self.before_proof)
        state_files = {str(p.relative_to(self.root / 'state')): host.checks.sha(p)
                       for p in sorted((self.root / 'state').rglob('*')) if p.is_file()}
        archived = {name.removeprefix('r2/retained-state/'): digest for name, digest in index.items()
                    if name.startswith('r2/retained-state/')}
        self.check('exact-archived-state-before-launch', state_files == archived, state_files)
        self.config_before = immutable_inputs(self.root)
        self.save('immutable-before.json', self.config_before)
        for role in ('meta', 'node'):
            binary = self.root / ('prefix/bin/afs-' + role)
            self.check(role + '-actual-ELF', host.checks.sha(binary) == getattr(self.args, 'afs_' + role + '_sha256'),
                       host.checks.sha(binary))
            libraries = self.command(['ldd', binary])
            self.check(role + '-libraries', 'not found' not in libraries, libraries)
            config = tomllib.loads((self.root / ('etc/' + role + '.toml')).read_text())
            self.command(['openssl', 'verify', '-CAfile', config['tls_ca_certificate'], config['tls_identity_certificate']])
            self.check(role + '-state-isolated', Path(config['data_dir']).is_relative_to(self.root / 'state'), config['data_dir'])
            if role == 'meta':
                self.check('same-local-file-Meta', config['meta_store'] == 'local-file', config['meta_store'])
            else:
                self.check('same-host-ON-container-OFF', config['fs'] == 'ownerfs'
                    and config['experimental_ownerfs_workspace_bind'] is True
                    and not config.get('experimental_native_workspace', False)
                    and config['ownerfs_workspace_bind']['workspace'] == 'workspace'
                    and config['data_mode'] == 'grpc' and config['allow_volatile_meta'] is False, config)
            self.command([binary, '--config', self.root / ('etc/' + role + '.toml'), '--print-config'])
        self.check('same-processctl', host.checks.sha(self.root / 'prefix/bin/afs-processctl') == self.args.processctl_sha256,
                   host.checks.sha(self.root / 'prefix/bin/afs-processctl'))
        old = dict(node=self.previous_identity['node'], meta=self.previous_identity['meta'])
        self.replacement = self.root / 'replacement'
        receipts = {role: self.receipt(role, self.root / 'run', observed, negative=role == 'node')
                    for role, observed in old.items()}
        self.save('previous-actual-receipts.json', receipts)
        self.protected = host.checks.inventory()
        self.save('protected-before.json', self.protected)
        owned = host.checks.inventory(self.root)
        self.check('no-owned-product-process', not owned['processes'], owned)
        for path in (self.root / 'mount/ownerfs/workspace', self.root / 'mount/ownerfs'):
            self.command(['findmnt', '-rn', '--mountpoint', path], allowed=(1,))
        filesystem = json.loads(self.command(['findmnt', '-J', '-T', self.source,
            '-o', 'TARGET,SOURCE,FSTYPE,OPTIONS,UUID']))
        self.check('same-guest-ext4', filesystem['filesystems'][0]['fstype'] == 'ext4'
                   and bool(filesystem['filesystems'][0]['uuid']), filesystem)
        self.save('filesystem.json', filesystem)
        self.check('FUSE-device', stat.S_ISCHR(os.stat('/dev/fuse').st_mode), '/dev/fuse')
        for port in (22400, 22401, 22500, 22501):
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', port))
        self.check('ports-free', True, [22400, 22401, 22500, 22501])
        self.save('ram-admission.json', host.checks.ram_observation())
        self.budget('admission', initial=True)
        self.save('contract.json', dict(source_commit=self.args.source_commit, bytes=4096,
            backend='local-file', host_bind=True, payload_recreation=False,
            fault_scope='process restart after prior epoch-error closure; not VM/crash/power-loss',
            expected_new_session='fresh epoch greater than prior2/3', final_actual_waits=[0, 0]))

    def execute(self):
        result = dict(status='BLOCKED', source_commit=self.args.source_commit,
                      scope='one existing host workspace process recovery; pre-existing4096B')
        try:
            self.preflight()
            result['status'] = 'FAIL'
            self.started = True
            self.ctl('start', 'all')
            current = self.identity()
            self.save('recovered-identity.json', current)
            health = self.health(22501)
            self.save('recovered-health.json', health)
            verify_session(self.previous, self.replaced, health)
            self.check('fresh-recovered-session', True, health['checks']['node_registration'])
            for role in ('meta', 'node'):
                self.check(role + '-new-incarnation', current[role]['pid'] != self.previous_identity[role]['pid']
                    and current[role]['starttick'] > self.previous_identity[role]['starttick'], current[role])
            source, target, binding = self.binding(current)
            self.save('binding-recovered.json', binding)
            self.check('same-physical-Home', source == self.source
                       and binding['source_identity'] == self.before_source_identity, binding)
            after = proof(target / 'epoch-proof')
            verify_proof(self.before_proof, after)
            self.save('proof-recovered.json', after)
            self.check('confirmed-proof-recovered', True, after)
            self.stop('recovered', current)
            verify_proof(self.before_proof, proof(self.source / 'epoch-proof'))
            self.check('config-prefix-unchanged', self.config_before == immutable_inputs(self.root), self.config_before)
            self.inputs()
            self.budget('final')
            after_inventory = host.checks.inventory()
            host.checks.verify_protected(self.protected, after_inventory)
            self.save('protected-after.json', after_inventory)
            owned = host.checks.inventory(self.root)
            self.check('no-owned-product-process-final', not owned['processes'], owned)
            result['status'] = 'PASS'
        except Exception as error:
            result['error'] = str(error)
        finally:
            if self.started:
                self.close_remaining(result)
            if (self.root / 'logs').exists():
                shutil.copytree(self.root / 'logs', self.out / 'service-logs')
            self.save('checks.json', self.checks)
            self.save('result.json', result)
        print(json.dumps(result))
        return 0 if result['status'] == 'PASS' else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('root', 'out', 'transport', 'inputs', 'previous-results', 'previous-index'):
        parser.add_argument('--' + name, type=Path, required=True)
    for name in ('source-commit', 'afs-meta-sha256', 'afs-node-sha256', 'processctl-sha256',
                 'inputs-sha256', 'previous-index-sha256'):
        parser.add_argument('--' + name, required=True)
    return Run(parser.parse_args()).execute()


if __name__ == '__main__':
    raise SystemExit(main())
