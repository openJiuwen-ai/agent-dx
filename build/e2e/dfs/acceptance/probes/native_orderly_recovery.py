"""Two orderly phases on unchanged local-file services and a trusted template."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import stat

ATOM = b'0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-'
PAYLOAD = ATOM * 64
PROOF = 'orderly-recovery-proof'
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()


def load_identity():
    spec = importlib.util.spec_from_file_location('recovery_identity',
        Path(__file__).with_name('native_source_rejection.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def trusted_tree(root):
    """Observe the complete immutable template, including an unexpected dev tree."""
    result = {}
    for path in [root, *sorted(root.rglob('*'))]:
        st = path.lstat()
        if (stat.S_ISLNK(st.st_mode) or not (stat.S_ISREG(st.st_mode) or stat.S_ISDIR(st.st_mode))
                or st.st_uid != 0 or stat.S_IMODE(st.st_mode) & 0o022):
            raise ValueError('untrusted template entry: ' + str(path))
        entry = {'dev': st.st_dev, 'ino': st.st_ino, 'mode': st.st_mode,
                 'uid': st.st_uid, 'gid': st.st_gid, 'size': st.st_size,
                 'mtime_ns': st.st_mtime_ns, 'ctime_ns': st.st_ctime_ns}
        if path.is_file():
            entry['sha256'] = digest(path)
        result[str(path.relative_to(root))] = entry
    return result


def frozen_inputs(run):
    template = run.root / 'rootfs'
    files = {str(p.relative_to(run.root)): digest(p) for p in
             [*sorted((run.root / 'etc').rglob('*')), *sorted((run.root / 'prefix').rglob('*'))]
             if p.is_file()}
    state = run.root / 'state/meta'
    st = state.stat()
    return {'template': trusted_tree(template), 'config_prefix': files,
            'meta_state_directory': {'path': str(state), 'dev': st.st_dev, 'ino': st.st_ino}}


def verify_inputs(before, after):
    if (not before.get('template') or not before.get('config_prefix')
            or not before.get('meta_state_directory') or before != after):
        raise ValueError('template/config/prefix/local-file state identity changed')


def verify_content(value):
    expected = {'bytes_hex': PAYLOAD.hex(), 'size': 4096, 'sha256': DIGEST,
                'eof': True, 'mode': 0o600, 'uid': 501, 'gid': 501}
    if value != expected or type(value.get('eof')) is not bool:
        raise ValueError('complete bytes/EOF/hash/owner/mode mismatch')


def fuse_read(path):
    with path.open('rb', buffering=0) as stream:
        data, eof = stream.read(len(PAYLOAD)), stream.read(1) == b''
        st = os.fstat(stream.fileno())
    value = {'bytes_hex': data.hex(), 'size': st.st_size,
             'sha256': hashlib.sha256(data).hexdigest(), 'eof': eof,
             'mode': stat.S_IMODE(st.st_mode), 'uid': st.st_uid, 'gid': st.st_gid}
    verify_content(value)
    return value


def read_shell():
    p = '/workspace/' + PROOF
    return (f'/bin/busybox cat {p}; printf "\\nRECOVERY_META "; '
            f'/bin/busybox stat -c "%s %a %u %g" {p}; /bin/busybox sha256sum {p}; '
            f'printf "RECOVERY_EOF "; /bin/busybox tail -c +4097 {p} | /bin/busybox wc -c')


def parse_container_content(output):
    expected_suffix = f'\nRECOVERY_META 4096 600 501 501\n{DIGEST}  /workspace/{PROOF}\nRECOVERY_EOF 0\n'
    if output != PAYLOAD.decode('ascii') + expected_suffix:
        raise ValueError('container full read/EOF/stat/hash output mismatch')
    value = {'bytes_hex': PAYLOAD.hex(), 'size': 4096, 'sha256': DIGEST,
             'eof': True, 'mode': 0o600, 'uid': 501, 'gid': 501}
    verify_content(value)
    return value


def container_read(run, label):
    shell = read_shell()
    prior = {p.name: digest(p) for p in (run.root / 'control').glob('command-*.command.json')}
    response = run.native(label, 'exec', '--', '/bin/sh', '-ec', shell)
    if response.get('status') != 'Executed':
        raise ValueError('container read did not execute')
    matching = []
    for path in (run.root / 'control').glob('command-*.command.json'):
        command = json.loads(path.read_text())
        if (command.get('argv', [])[-3:] == ['/bin/sh', '-ec', shell]
                and prior.get(path.name) != digest(path)):
            matching.append(path)
    if len(matching) != 1:
        raise ValueError('container read must have one exact runtime command')
    command = matching[0]
    exit_path = command.with_name(command.name.replace('.command.json', '.exit.json'))
    if json.loads(exit_path.read_text()) != {'code': 0, 'reason': None, 'success': True}:
        raise ValueError('container read actual exit mismatch')
    value = parse_container_content(command.with_name(command.name.replace('.command.json', '.stdout')).read_text())
    run.save(label + '-content.json', {'content': value, 'command': str(command),
                                    'command_sha256': digest(command), 'exit': json.loads(exit_path.read_text())})
    return value


def verify_private_root(runtime, config, template):
    bundle = Path(runtime.get('bundle', ''))
    root = Path(runtime.get('rootfs', ''))
    configured = Path(config.get('root', {}).get('path', ''))
    if (not bundle.is_absolute() or not root.is_absolute() or configured != root
            or root == template or root == bundle or '..' in root.parts
            or bundle.parent != template.parent / 'control'
            or bundle.name != 'bundle-' + runtime.get('id', '')
            or root.is_relative_to(bundle) is False
            or config.get('root', {}).get('readonly') is not True):
        raise ValueError('OCI root is not the private owned bundle root')


def physical_identity(run, label, container):
    identity = load_identity()
    value = identity.snapshot(run, label, container)
    identity.verify_unchanged(value, value)
    config_path = Path(value['runtime']['bundle']) / 'config.json'
    config = json.loads(config_path.read_text())
    verify_private_root(value['runtime'], config, run.root / 'rootfs')
    roots = getattr(run, 'recovery_runtime_roots', [])
    root = Path(value['runtime']['rootfs'])
    if root not in roots:
        roots.append(root)
    run.recovery_runtime_roots = roots
    run.save(label + '-private-root.json', {'runtime': value['runtime'],
                                         'oci_config': config, 'config_sha256': digest(config_path)})
    return value


def archive(run, name, destination, label):
    """Keep runtime-mutated special files in place; archive exact control text separately."""
    roots = list(getattr(run, 'recovery_runtime_roots', []))
    excluded = {}
    if name == 'control':
        for config_path in sorted((run.root / 'control').glob('bundle-*/config.json')):
            config = json.loads(config_path.read_text())
            runtime = {'id': config_path.parent.name.removeprefix('bundle-'),
                       'bundle': str(config_path.parent), 'rootfs': config['root']['path']}
            verify_private_root(runtime, config, run.root / 'rootfs')
            root = Path(runtime['rootfs'])
            if root not in roots:
                roots.append(root)
        for root in roots:
            for directory in [root, *root.parents]:
                st = directory.lstat()
                if not stat.S_ISDIR(st.st_mode) or stat.S_ISLNK(st.st_mode):
                    raise ValueError('private root has a nondirectory or linked ancestor')
                if directory == run.root / 'control':
                    break
            entries = {}
            for path in [root, *sorted(root.rglob('*'))]:
                st = path.lstat()
                value = {'mode': st.st_mode, 'uid': st.st_uid, 'gid': st.st_gid,
                         'dev': st.st_dev, 'ino': st.st_ino, 'rdev': st.st_rdev,
                         'size': st.st_size, 'allocated_bytes': st.st_blocks * 512}
                if stat.S_ISREG(st.st_mode):
                    value['sha256'] = digest(path)
                elif stat.S_ISLNK(st.st_mode):
                    value['target'] = os.readlink(path)
                entries[str(path.relative_to(root))] = value
            excluded[str(root)] = entries
        run.save('orderly-' + label + '-runtime-roots-local-only.json', excluded)
    for path in [run.root / name, *sorted((run.root / name).rglob('*'))]:
        if any(path == root or path.is_relative_to(root) for root in roots):
            continue
        st = path.lstat()
        if not (stat.S_ISREG(st.st_mode) or stat.S_ISDIR(st.st_mode)):
            raise ValueError('unknown linked/nonregular archive input: ' + str(path))
    def ignore(directory, names):
        return [n for n in names if Path(directory) / n in roots]
    shutil.copytree(run.root / name, destination, ignore=ignore, dirs_exist_ok=True)


def verify_restart(old, new, first, second):
    for role in ('meta', 'node'):
        a, b = old[role], new[role]
        if (type(b['pid']) is not int or type(b['starttick']) is not int
                or b['pid'] <= 0 or b['pid'] == a['pid'] or b['starttick'] <= a['starttick']
                or a['sha256'] != b['sha256'] or not a.get('installed')
                or b.get('installed') != a['installed']):
            raise ValueError('service incarnation/ELF mismatch: ' + role)
    if (first['status']['root'] != second['status']['root']
            or first['status']['home_node'] != second['status']['home_node']
            or first['source'] != second['source']
            or first['status']['container'] == second['status']['container']
            or first['process']['pid'] == second['process']['pid']
            or second['process']['starttick'] <= first['process']['starttick']):
        raise ValueError('workspace/root/source or fresh container incarnation mismatch')
    # Namespace inode values may be reused; physical_identity validates each actual binding.


def fields(path):
    return dict(line.split('=', 1) for line in path.read_text().splitlines() if '=' in line)


def verify_receipt(receipt, child, ready, observed, gone):
    keys = ('pid', 'exe', 'config', 'start_ticks', 'boot_id', 'lifecycle', 'supervisor_pid')
    executable = Path(observed['installed']['path'])
    config = executable.parents[2] / 'etc' / (executable.name.removeprefix('afs-') + '.toml')
    if (any(not child.get(k) or receipt.get(k) != child[k] for k in keys)
            or receipt.get('exit_code') != '0' or ready.get('supervisor_pid') != child['supervisor_pid']
            or child['pid'] != str(observed['pid']) or child['start_ticks'] != str(observed['starttick'])
            or child['exe'] != str(executable) or child['config'] != str(config)
            or child['boot_id'] != observed['boot_id']
            or gone is not True):
        raise ValueError('exact actual wait0/incarnation/child-supervisor closure missing')


def record_closure(run, process, label):
    receipts = {}
    for role in ('meta', 'node'):
        candidates = []
        for lifecycle in sorted((run.root / 'run').glob(role + '.lifecycle.*')):
            if (lifecycle / 'child').is_file() and fields(lifecycle / 'child').get('pid') == str(process[role]['pid']):
                candidates.append(lifecycle)
        if len(candidates) != 1:
            raise ValueError('missing unique ' + role + ' lifecycle')
        path = candidates[0]
        child, receipt, ready = fields(path / 'child'), fields(path / 'exit'), fields(path / 'ready')
        gone = all(not Path('/proc/' + child[k]).exists() for k in ('pid', 'supervisor_pid'))
        verify_receipt(receipt, child, ready, process[role], gone)
        receipts[role] = {'path': str(path), 'child': child, 'exit': receipt, 'ready': ready, 'both_gone': gone}
    if (run.root / 'control/control.sock').exists() or (run.root / 'control/controller.lock').exists():
        raise ValueError('controller socket/lock remains after orderly service stop')
    run.command(['findmnt', '-rn', '--mountpoint', run.root / 'mount/ownerfs'], allowed=(1,))
    run.save('orderly-' + label + '-actual-waits.json', receipts)
    run.check('orderly-' + label + '-actual-wait0', True, receipts)
    verify_inputs(run.recovery_inputs_before, frozen_inputs(run))
    return receipts


def stop_container(run, label, value):
    response = run.native(label + '-stop', 'stop')
    run.check(label + '-stopped', response.get('status') == 'Stopped', response)
    run.check(label + '-idle', run.native(label + '-idle', 'status').get('state') == 'Idle', 'Idle')
    run.check(label + '-container-gone', not Path('/proc/' + str(value['process']['pid'])).exists(), value['process'])
    empty = json.loads(run.command(['/usr/local/sbin/runc', '--root', run.root / 'control/runtime-state',
                                   'list', '--format', 'json']))
    run.check(label + '-runtime-empty', empty in (None, []), empty)


def execute(run, start, workspace, process):
    before = run.recovery_inputs_before
    verify_inputs(before, frozen_inputs(run))
    run.save('orderly-inputs-before.json', before)
    first = physical_identity(run, 'orderly-phase1', start['container'])
    proof = workspace / PROOF
    if os.path.lexists(proof):
        raise ValueError('confirmed proof must be fresh')
    shell = ("set -C; umask 077; i=0; while [ \"$i\" -lt 64 ]; do printf '%s' '"
             + ATOM.decode() + "'; i=$((i+1)); done > /workspace/" + PROOF
             + '; /bin/busybox sync -f /workspace/' + PROOF)
    response = run.native('orderly-confirm-write', 'exec', '--', '/bin/sh', '-ec', shell)
    run.check('orderly-confirmed-write', response.get('status') == 'Executed', response)
    first_fuse = fuse_read(proof)
    first_container = container_read(run, 'orderly-phase1-read')
    run.save('orderly-phase1-content.json', {'fuse': first_fuse, 'container': first_container})
    stop_container(run, 'orderly-phase1', first)
    run.ctl('stop', 'all')
    phase1_waits = record_closure(run, process, 'phase1')
    verify_inputs(before, frozen_inputs(run))
    phase1 = run.out / 'phase1'
    phase1.mkdir()
    for name in ('control', 'logs', 'run'):
        archive(run, name, phase1 / name, 'phase1')
    for name in ('running-identity.json', 'final-identity.json', 'commands.json'):
        shutil.copyfile(run.out / name, phase1 / name)
    run.ctl('start', 'all')
    second_process = run.identity()
    verify_inputs(before, frozen_inputs(run))
    second_start = run.native('orderly-second', 'start', 'workspace')
    run.check('orderly-second-final-verified', second_start.get('state') == 'FinalVerified', second_start)
    second = physical_identity(run, 'orderly-phase2', second_start['container'])
    verify_restart(process, second_process, first, second)
    second_container = container_read(run, 'orderly-phase2-read')
    second_fuse = fuse_read(proof)
    verify_inputs(before, frozen_inputs(run))
    run.save('orderly-inputs-after-phase2.json', frozen_inputs(run))
    run.save('orderly-recovery-result.json', {'status': 'PASS', 'phase1': {'identity': first,
        'process': process, 'content': {'fuse': first_fuse, 'container': first_container}, 'actual_waits': phase1_waits},
        'phase2': {'identity': second, 'process': second_process,
                   'content': {'fuse': second_fuse, 'container': second_container}},
        'inputs_unchanged': True, 'scope': 'orderly recovery only; final closure by parent driver'})
    return second_process, second['process']['pid']
