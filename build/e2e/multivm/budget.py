#!/usr/bin/env python3
"""Start the non-resettable three-VM acceptance wall-clock budget."""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import time

if __package__:
    from .contract import inventory_digest, verify_inventory
    from .suite import MAX_BUDGET_SECONDS
else:
    from contract import inventory_digest, verify_inventory
    from suite import MAX_BUDGET_SECONDS


def start_budget(inventory, output, budget_seconds=MAX_BUDGET_SECONDS, now=time.time):
    verify_inventory(inventory)
    if type(budget_seconds) is not int or not 0 < budget_seconds <= MAX_BUDGET_SECONDS:
        raise ValueError('suite budget must be at most three hours')
    started_at = now()
    if not isinstance(started_at, (int, float)) or not math.isfinite(started_at) \
            or started_at <= 0:
        raise ValueError('budget start time must be a positive Unix timestamp')
    state = {
        'schema_version': 2,
        'inventory_sha256': inventory_digest(inventory),
        'budget_seconds': budget_seconds,
        'scope': 'deployment-and-cases',
        'started_at': started_at,
        'finished_at': None,
        'runtime_seconds': 0,
        'cases': [],
        'active': None,
    }
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    path = output / 'budget-state.json'
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise FileExistsError(f'budget-state already exists at {path}; reuse it') from None
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(state, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--budget-seconds', type=int, default=MAX_BUDGET_SECONDS)
    args = parser.parse_args()
    state = start_budget(json.loads(args.inventory.read_text()), args.output,
                         args.budget_seconds)
    print(json.dumps({'budget_state': str(args.output / 'budget-state.json'),
                      'started_at': state['started_at'],
                      'deadline_at': state['started_at'] + state['budget_seconds']}),
          flush=True)


if __name__ == '__main__':
    main()
