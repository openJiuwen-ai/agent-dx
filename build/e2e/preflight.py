#!/usr/bin/env python3
"""Read-only host prerequisites for the sandboxd runc acceptance fixture."""
from pathlib import Path
import json


def check(proc=Path('/proc')):
    filesystems = {line.split()[-1] for line in (proc / 'filesystems').read_text().splitlines() if line.strip()}
    bridge = proc / 'sys/net/bridge/bridge-nf-call-iptables'
    result = {'erofs': 'erofs' in filesystems,
              'bridge_netfilter': bridge.is_file() and bridge.read_text().strip() == '1'}
    missing = [name for name, available in result.items() if not available]
    if missing:
        raise RuntimeError('sandboxd host prerequisites unavailable: ' + ', '.join(missing))
    return result


if __name__ == '__main__':
    print(json.dumps(check()), flush=True)
