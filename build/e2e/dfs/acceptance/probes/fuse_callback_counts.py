"""Exact cumulative first-party callback snapshots; never infer missing counters as zero."""
import errno
import hashlib
import os
from pathlib import Path
import re

PREFIX = 'afs_fuse_callbacks_total'
METADATA_MINIMUM = {'create': 12, 'unlink': 12, 'mkdir': 1, 'rmdir': 1}


def parse_counts(text, filesystem='ownerfs'):
    counts = {}
    for line in text.splitlines():
        if not line.startswith(PREFIX + '{'):
            continue
        match = re.fullmatch(PREFIX + r'\{([^}]+)\} ([0-9]+)', line)
        if not match:
            raise ValueError('malformed callback counter sample')
        fields = re.findall(r'(\w+)="([^"]*)"', match[1])
        labels = dict(fields)
        if len(fields) != 2 or len(labels) != 2 or set(labels) != {'filesystem', 'operation'}:
            raise ValueError('unexpected callback labels')
        if labels['filesystem'] != filesystem:
            continue
        operation = labels['operation']
        if operation in counts:
            raise ValueError('duplicate callback counter')
        counts[operation] = int(match[2])
    if not {'read', 'write'} <= counts.keys():
        raise ValueError('read/write callback series not observed')
    return counts


def delta(before, after):
    if not before or before.keys() != after.keys():
        raise ValueError('counter series changed or missing')
    if any(type(v) is not int or v < 0 for v in [*before.values(), *after.values()]):
        raise ValueError('invalid callback count')
    if any(after[k] < before[k] for k in before):
        raise ValueError('callback counter rolled back')
    return {k: after[k] - before[k] for k in before}


def verify_metadata_window(before, after, native):
    observed = delta(before, after)
    if not METADATA_MINIMUM.keys() <= observed.keys():
        raise ValueError('metadata callback series not observed')
    if native:
        if any(observed[k] != 0 for k in METADATA_MINIMUM):
            raise ValueError('native metadata entered FUSE callbacks')
    elif any(observed[k] < minimum for k, minimum in METADATA_MINIMUM.items()):
        raise ValueError('metadata positive control insufficient')
    return observed


def snapshot(run, label):
    text = run.command(['curl', '-fsS', 'http://127.0.0.1:24501/metrics'])
    record = {'command': run.commands[-1], 'counts': parse_counts(text)}
    run.save('callback-' + label + '.json', record)
    return record['counts']


def same_node(run):
    process = run.current_process['node']
    proc = Path('/proc') / str(process['pid'])
    tick = int((proc / 'stat').read_text().rsplit(')', 1)[1].split()[19])
    digest = hashlib.sha256((proc / 'exe').read_bytes()).hexdigest()
    run.check('callback-same-node-incarnation', tick == process['starttick'] and digest == process['sha256'], process)
    return process


def execute_metadata(run, workspace):
    before = snapshot(run, 'before-fuse')
    directory = workspace / 'callback-fuse-metadata'
    directory.mkdir()
    names = [f'file-{i}' for i in range(12)]
    for name in names:
        path = directory / name
        with path.open('xb'):
            pass
        run.check('metadata-empty-' + name, path.is_file() and path.stat().st_size == 0, name)
    run.check('metadata-directory-entries', sorted(p.name for p in directory.iterdir()) == sorted(names), names)
    for name in names:
        path = directory / name
        path.unlink()
        try:
            path.stat()
        except FileNotFoundError as error:
            run.check('metadata-unlinked-' + name, error.errno == errno.ENOENT, error.errno)
        else:
            raise ValueError('unlinked file remains visible')
    run.check('metadata-directory-empty', list(directory.iterdir()) == [], 'empty')
    directory.rmdir()
    run.check('metadata-directory-gone', not directory.exists(), 'absent')
    fuse = verify_metadata_window(before, snapshot(run, 'after-fuse'), native=False)
    run.check('callback-fuse-metadata-positive', True, fuse)
    before_native = snapshot(run, 'before-native')
    response = run.native('callback-native-metadata', 'exec', '--', '/bin/sh', '-ec',
        'd=/workspace/callback-native-metadata; /bin/busybox mkdir "$d"; '
        'i=0; while test "$i" -lt 12; do : > "$d/file-$i"; '
        'test -f "$d/file-$i"; test ! -s "$d/file-$i"; i=$((i+1)); done; '
        'set -- "$d"/file-*; test "$#" -eq 12; '
        'i=0; while test "$i" -lt 12; do /bin/busybox rm "$d/file-$i"; '
        'test ! -e "$d/file-$i"; i=$((i+1)); done; '
        '/bin/busybox rmdir "$d"; test ! -e "$d"; printf "METADATA_OK files=12 dirs=1\\n"')
    run.check('callback-native-metadata-executed', response.get('status') == 'Executed'
              and response.get('stdout_bytes') == len(b'METADATA_OK files=12 dirs=1\n'), response)
    native = verify_metadata_window(before_native, snapshot(run, 'after-native'), native=True)
    run.check('callback-native-metadata-bypass', True, native)
    process = same_node(run)
    run.save('callback-result.json', {'status': 'PASS', 'case': 'metadata', 'files': 12, 'directories': 1,
        'scope': 'create/unlink/mkdir/rmdir windows within one experimental-ON Node; not OFF/ON timing qualification',
        'fuse_delta': fuse, 'native_delta': native, 'node': process})


def execute(run, workspace, seed_sha):
    before = snapshot(run, 'before-fuse')
    path = workspace / 'callback-fuse-positive'
    data = b'FUSE callback witness\n' * 3072
    with path.open('wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    with path.open('rb') as stream:
        # Hint only; the actual positive callback delta below is the evidence.
        os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        actual = stream.read()
    run.check('callback-fuse-content', actual == data, {'bytes': len(data), 'sha256': hashlib.sha256(actual).hexdigest()})
    after_fuse = snapshot(run, 'after-fuse')
    fuse = delta(before, after_fuse)
    run.check('callback-fuse-positive', fuse['read'] > 0 and fuse['write'] > 0, fuse)
    before_native = snapshot(run, 'before-native')
    response = run.native('callback-native', 'exec', '--', '/bin/sh', '-ec',
        '/bin/busybox cp /workspace/seed /workspace/callback-native; '
        '/bin/busybox sync -f /workspace/callback-native; '
        f'test "$(/bin/busybox sha256sum /workspace/callback-native | /bin/busybox cut -d " " -f 1)" = {seed_sha}')
    run.check('callback-native-executed', response.get('status') == 'Executed', response)
    after_native = snapshot(run, 'after-native')
    native = delta(before_native, after_native)
    run.check('callback-native-data-bypass', native['read'] == 0 and native['write'] == 0, native)
    process = same_node(run)
    run.save('callback-result.json', {'status': 'PASS', 'scope': 'ordinary FUSE path positive control and managed native path within one experimental-ON Node; not configuration OFF/ON timing qualification', 'fuse_delta': fuse, 'native_delta': native, 'cache_residency': 'unobserved', 'node': process})
