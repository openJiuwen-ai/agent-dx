"""Bounded Linux procfs FD census; cache accounting never reads file contents."""

import argparse
import json
import logging
import sys
import os
from pathlib import Path
import stat
import time

SCOPE = 'afs-fd-census-v1'
MAX_ENTRIES = 4096


def integer(value, minimum=0):
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ValueError('invalid integer in FD evidence')
    return value


def root_identity(directory):
    path = Path(directory)
    if not path.is_absolute() or path.is_symlink() or path.resolve() != path:
        raise ValueError('cache root must be an explicit canonical owned directory')
    value = path.stat()
    if not stat.S_ISDIR(value.st_mode):
        raise ValueError('cache root is not a directory')
    return {'path': str(path), 'device': value.st_dev, 'inode': value.st_ino}


def classify(snapshot, capacity):
    """Only one FD per live inode directly in the declared chunk directory is exempt."""
    integer(capacity)
    if snapshot.get('scope') != SCOPE or snapshot.get('comm') != 'afs-node':
        raise ValueError('missing owned AFS FD census')
    integer(snapshot.get('pid'), 2)
    integer(snapshot.get('start_ticks'), 1)
    root = snapshot.get('cache_root')
    if root is not None:
        path = root.get('path')
        if not isinstance(path, str) or not path.startswith('/'):
            raise ValueError('invalid cache root identity')
        if str(Path(path)) != path or '..' in Path(path).parts:
            raise ValueError('invalid cache root identity')
        integer(root.get('device'))
        integer(root.get('inode'), 1)
    entries = snapshot.get('entries')
    if not isinstance(entries, list) or len(entries) > MAX_ENTRIES:
        raise ValueError('missing or oversized FD census')
    descriptors, objects, targets = set(), set(), []
    matching = 0
    for entry in entries:
        descriptor = integer(entry.get('fd'))
        if descriptor in descriptors:
            raise ValueError('duplicate FD number in census')
        descriptors.add(descriptor)
        device, inode = integer(entry.get('device')), integer(entry.get('inode'))
        kind, target = integer(entry.get('kind')), entry.get('target')
        if not isinstance(target, str):
            raise ValueError('missing FD target')
        live_regular = kind == stat.S_IFREG and inode > 0 and not target.endswith(' (deleted)')
        in_root = root is not None and str(Path(target).parent) == root['path']
        if live_regular and in_root and device == root['device']:
            matching += 1
            identity = (device, inode)
            if identity not in objects:
                objects.add(identity)
                targets.append(target)
    eligible = len(objects)
    accounted = min(eligible, capacity)
    return {'total': len(entries), 'eligible': eligible, 'accounted': accounted,
            'noncache': len(entries) - accounted, 'excess': max(0, eligible - capacity),
            'duplicates': matching - eligible, 'targets': sorted(targets)}


def census(pid, cache_root=None):
    """Retry a racing FD table at most three times; PID replacement always fails."""
    integer(pid, 2)
    process = Path('/proc') / str(pid)

    def identity():
        return int((process / 'stat').read_text().rsplit(')', 1)[1].split()[19])

    started = identity()
    comm = (process / 'comm').read_text().strip()
    if comm != 'afs-node':
        raise ValueError('FD observer PID is not the owned AFS Node')
    root = root_identity(cache_root) if cache_root is not None else None
    raced = []
    for attempt in range(3):
        if identity() != started:
            raise ValueError('FD observer process identity changed')
        names = sorted(os.listdir(process / 'fd'), key=int)
        if len(names) > MAX_ENTRIES:
            raise ValueError('FD census exceeds bounded observer budget')
        entries = []
        try:
            for name in names:
                path = process / 'fd' / name
                target = os.readlink(path)
                value = path.stat()
                entries.append({'fd': int(name), 'target': target, 'kind': stat.S_IFMT(value.st_mode),
                                'device': value.st_dev, 'inode': value.st_ino})
            if sorted(os.listdir(process / 'fd'), key=int) != names:
                raise FileNotFoundError('FD table changed during observation')
            if identity() != started or (root and root_identity(cache_root) != root):
                raise ValueError('FD observer process or cache root identity changed')
            return {'scope': SCOPE, 'pid': pid, 'start_ticks': started, 'comm': comm,
                    'cache_root': root, 'entries': entries, 'raced_attempts': raced}
        except FileNotFoundError as error:
            raced.append(str(error))
            if attempt < 2:
                time.sleep(0.02)
    raise ValueError('FD table could not be observed consistently: ' + repr(raced))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pid', required=True, type=int)
    parser.add_argument('--cache-root')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s', stream=sys.stdout, force=True)
    logging.info('%s', json.dumps(census(args.pid, args.cache_root)))


if __name__ == '__main__':
    main()
