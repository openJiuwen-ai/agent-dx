"""Read only owned container resources, tolerating normal concurrent file retirement."""

import argparse
import importlib.util
import json
import logging
import sys
import os
from pathlib import Path
import shutil
import stat

NAMES = ('afs-meta', 'afs-node', 'adxlet', 'adx-coordinator', 'adx-apiserver', 'adx-ingress', 'sandboxd', 'envd')
STORAGE_SCOPE = 'allocated-backing-fs-v2'
_SPEC = importlib.util.spec_from_file_location('soak_fd_probe', Path(__file__).with_name('soak_fd.py'))
fd = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(fd)
_MEMORY_SPEC = importlib.util.spec_from_file_location('soak_memory_probe', Path(__file__).with_name('soak_memory.py'))
memory = importlib.util.module_from_spec(_MEMORY_SPEC)
_MEMORY_SPEC.loader.exec_module(memory)


def mount_paths(text):
    def decode(path):
        for escaped, plain in (('\\040', ' '), ('\\011', '\t'), ('\\012', '\n'), ('\\134', '\\')):
            path = path.replace(escaped, plain)
        return path
    return {decode(line.split(' - ', 1)[0].split()[4]) for line in text.splitlines()}


def backing_storage(directory, mounts):
    """Allocated blocks on the owning filesystem; prune RO/merged/tmpfs/bind views."""
    directory = Path(directory)
    if directory.is_symlink():
        raise ValueError('backing measurement root cannot be a symlink')
    if not directory.exists():
        return {'bytes': 0, 'device': None, 'excluded_mounts': [], 'entries': 0}
    device = directory.stat().st_dev
    excluded = {path for path in mounts if path.startswith(str(directory) + '/')}
    total, seen = 0, set()

    def fail(error):
        if not isinstance(error, FileNotFoundError):
            raise error
    for root, dirs, files in os.walk(directory, followlinks=False, onerror=fail):
        retained = []
        for name in dirs:
            path = Path(root) / name
            try:
                value = path.lstat()
            except FileNotFoundError:
                continue
            if str(path) not in excluded and value.st_dev == device:
                retained.append(name)
        dirs[:] = retained
        entries = [Path(root), *(Path(root) / name for name in files),
                   *(Path(root) / name for name in dirs if (Path(root) / name).is_symlink())]
        for path in entries:
            if str(path) in excluded:
                continue
            try:
                value = path.lstat()
            except FileNotFoundError:
                continue
            identity = (value.st_dev, value.st_ino)
            if value.st_dev != device or identity in seen:
                continue
            seen.add(identity)
            total += value.st_blocks * 512
    return {'bytes': total, 'device': device, 'excluded_mounts': sorted(excluded), 'entries': len(seen)}


def process_info(pid):
    path = Path('/proc') / str(pid)
    status = (path / 'status').read_text().splitlines()
    return {'pid': pid, 'start_ticks': int((path / 'stat').read_text().rsplit(')', 1)[1].split()[19]),
            'rss_bytes': next((int(line.split()[1]) * 1024 for line in status if line.startswith('VmRSS:')), 0),
            'fds': len(list((path / 'fd').iterdir()))}


def persistent_process(pid, name, mode=None, cache_root=None):
    process = process_info(pid)
    if mode and name == 'afs-node':
        census = fd.inventory.census(pid, cache_root)
        if any(census[key] != process[key] for key in ('pid', 'start_ticks')):
            raise ValueError('AFS process changed during observation')
        process.update(fd_census=census, fds=len(census['entries']))
    return process


def anchor_runtime(environment_id):
    from node import P, command
    lines = command(['sbox', '-a', P / 'sandboxd/sandboxd.sock', 'list',
                     '--label', 'adx.environment_id=' + environment_id], timeout=5).splitlines()
    identities = [line.split()[0] for line in lines[1:] if line.strip()]
    if len(identities) != 1:
        raise ValueError('anchor must have exactly one physical backend')
    state = json.loads(command(['runc', '--root', P / 'sandboxd/runc', 'state', identities[0]], timeout=5))
    if (state.get('id') != identities[0]
            or state.get('status') != 'running'
            or not isinstance(state.get('pid'), int)):
        raise ValueError('anchor runc identity is not running')
    if (isinstance(state['pid'], bool)
            or state['pid'] <= 1):
        raise ValueError('anchor runc identity is not running')
    return dict(process_info(state['pid']), runtime_id=identities[0], environment_id=environment_id)


def storage(directory):
    """Apparent bytes and log bytes from one walk; never follow external symlinks."""
    total = logs = 0
    seen = set()

    def fail(error):
        if not isinstance(error, FileNotFoundError):
            raise error

    for root, dirs, files in os.walk(directory, followlinks=False, onerror=fail):
        entries = [Path(root), *(Path(root) / name for name in files),
                   *(Path(root) / name for name in dirs if (Path(root) / name).is_symlink())]
        for path in entries:
            try:
                value = path.lstat()
            except FileNotFoundError:
                continue
            identity = (value.st_dev, value.st_ino)
            if identity in seen:
                continue
            seen.add(identity)
            total += value.st_size
            fixture_log = (path.parent.parent == Path(directory) and path.parent.name.startswith('afs-test-')
                           and (path.name in ('M.log', 'A.log', 'B.log', 'C.log')
                                or (path.name.startswith('helper-') and path.name.endswith('.log'))))
            if stat.S_ISREG(value.st_mode) and fixture_log:
                logs += value.st_size
    return total, logs


def observe(environment_id=None, *, mode=None, cache_root=None, full_memory=False, sdk_worker=None):
    processes = {}
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            name = (path / 'comm').read_text().strip()
            if name not in (NAMES + memory.EXTRA_NAMES if full_memory else NAMES):
                continue
            processes[name + ':' + path.name] = persistent_process(int(path.name), name, mode, cache_root)
        except FileNotFoundError:
            continue
    total, logs = storage('/a')
    runtime = Path('/tmp/adx-e2e/sandboxd')
    streams = storage('/opt/adx/logs/runtime')[0]
    mountinfo = Path('/proc/self/mountinfo').read_text()
    mounts = mount_paths(mountinfo)
    backing = backing_storage(runtime, mounts)
    stream_backing = backing_storage('/opt/adx/logs/runtime', mounts)
    anchor = anchor_runtime(environment_id) if environment_id else None
    census = memory.enrich(processes, anchor, sdk_worker=sdk_worker) if full_memory else None
    observed = {'processes': processes, 'anchor_runtime': anchor,
            'fds': sum(p['fds'] for p in processes.values()) + (anchor['fds'] if anchor else 0),
            'rss_bytes': sum(p['rss_bytes'] for p in processes.values()) + (anchor['rss_bytes'] if anchor else 0),
            'mounts': len(Path('/proc/self/mountinfo').read_text().splitlines()),
            'disk_bytes': total, 'log_bytes': logs, 'disk_data_bytes': total - logs,
            'runtime_disk_bytes': storage(runtime)[0] + streams, 'runtime_stream_bytes': streams,
            'runtime_backing_bytes': backing['bytes'] + stream_backing['bytes'],
            'runtime_backing': backing, 'runtime_stream_backing': stream_backing, 'storage_scope': STORAGE_SCOPE,
            'runtime_disk_present': runtime.is_dir(), 'free_bytes': shutil.disk_usage('/a').free}
    if full_memory:
        observed.update(observation_scope=memory.SCOPE, process_census=census)
    return fd.attach(observed, mode) if mode else observed


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('environment_id', nargs='?')
    parser.add_argument('--mode', choices=('ownerfs', 'dfs'))
    parser.add_argument('--cache-root')
    parser.add_argument('--full-memory', action='store_true')
    parser.add_argument('--sdk-worker')
    args = parser.parse_args()
    if args.cache_root and args.mode != 'dfs':
        parser.error('cache root is valid only for DFS')
    logging.basicConfig(level=logging.INFO, format='%(message)s', stream=sys.stdout, force=True)
    logging.info('%s', json.dumps(observe(args.environment_id, mode=args.mode, cache_root=args.cache_root,
                                         full_memory=args.full_memory, sdk_worker=args.sdk_worker)))
