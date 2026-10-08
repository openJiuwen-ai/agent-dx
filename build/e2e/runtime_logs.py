#!/usr/bin/env python3
"""Inspect sandboxd host stream redirects after the public SDK scenario."""
import json
from pathlib import Path
import sys


def inspect(directory, instance_ids):
    streams = {}
    for path in directory.iterdir():
        if not path.is_file() or path.suffix not in ('.out', '.err'):
            continue
        runtime_id = path.stem
        if not any(runtime_id.startswith(instance_id + '-') for instance_id in instance_ids):
            continue
        streams.setdefault(runtime_id, set()).add(path.suffix)
    if any(extensions != {'.out', '.err'} for extensions in streams.values()):
        raise AssertionError('runtime stdout/stderr files must be paired')
    return sorted(streams)


if __name__ == '__main__':
    runtime_ids = inspect(Path('/opt/adx/logs/runtime'), sys.argv[1:])
    print(json.dumps({'runtime_ids': runtime_ids}))
