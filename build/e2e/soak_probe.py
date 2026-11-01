"""Read only owned container resources, tolerating normal concurrent file retirement."""

import json
import os
from pathlib import Path
import shutil
import stat
import sys

NAMES = ('afs-meta', 'afs-node', 'adxlet', 'adx-coordinator', 'adx-apiserver', 'adx-ingress', 'sandboxd', 'envd')


def process_info(pid):
    path = Path('/proc') / str(pid)
    status = (path / 'status').read_text().splitlines()
    return {'pid': pid, 'start_ticks': int((path / 'stat').read_text().rsplit(')', 1)[1].split()[19]),
            'rss_bytes': next((int(line.split()[1]) * 1024 for line in status if line.startswith('VmRSS:')), 0),
            'fds': len(list((path / 'fd').iterdir()))}


def anchor_runtime(environment_id):
    from node import P, command
    lines = command(['sbox', '-a', P / 'sandboxd/sandboxd.sock', 'list',
                     '--label', 'adx.environment_id=' + environment_id], timeout=5).splitlines()
    identities = [line.split()[0] for line in lines[1:] if line.strip()]
    if len(identities) != 1:
        raise ValueError('anchor must have exactly one physical backend')
    state = json.loads(command(['runc', '--root', P / 'sandboxd/runc', 'state', identities[0]], timeout=5))
    if (state.get('id') != identities[0] or state.get('status') != 'running'
            or not isinstance(state.get('pid'), int) or isinstance(state['pid'], bool) or state['pid'] <= 1):
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


def observe(environment_id=None):
    processes = {}
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            name = (path / 'comm').read_text().strip()
            if name not in NAMES:
                continue
            processes[name + ':' + path.name] = process_info(int(path.name))
        except FileNotFoundError:
            continue
    total, logs = storage('/a')
    runtime = Path('/tmp/adx-e2e/sandboxd')
    streams = storage('/opt/adx/logs/runtime')[0]
    anchor = anchor_runtime(environment_id) if environment_id else None
    return {'processes': processes, 'anchor_runtime': anchor,
            'fds': sum(p['fds'] for p in processes.values()) + (anchor['fds'] if anchor else 0),
            'rss_bytes': sum(p['rss_bytes'] for p in processes.values()) + (anchor['rss_bytes'] if anchor else 0),
            'mounts': len(Path('/proc/self/mountinfo').read_text().splitlines()),
            'disk_bytes': total, 'log_bytes': logs, 'disk_data_bytes': total - logs,
            'runtime_disk_bytes': storage(runtime)[0] + streams, 'runtime_stream_bytes': streams,
            'runtime_disk_present': runtime.is_dir(), 'free_bytes': shutil.disk_usage('/a').free}


if __name__ == '__main__':
    print(json.dumps(observe(sys.argv[1] if len(sys.argv) > 1 else None)))
