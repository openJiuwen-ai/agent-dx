"""Scrape running services and compare actual allocations across both nodes."""

import json
import re
import time
import urllib.error
import urllib.request
from pathlib import Path


def values(text, metric, **wanted):
    found = []
    for line in text.splitlines():
        match = re.fullmatch(r'([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+(\S+)', line)
        if not match or match[1] != metric:
            continue
        labels = {k: json.loads('"' + v + '"') for k, v in re.findall(r'(\w+)="((?:\\.|[^"\\])*)"', match[2] or '')}
        if all(labels.get(k) == v for k, v in wanted.items()):
            found.append(float(match[3]))
    if not (found):
        raise AssertionError((metric, wanted, 'missing samples'))
    return found


def one(text, metric, **labels):
    result = values(text, metric, **labels)
    if not (len(result) == 1):
        raise AssertionError((metric, labels, result))
    return result[0]


def check(label, running, reserved, pending):
    from node import nodes

    end = time.monotonic() + 10
    while True:
        try:
            with urllib.request.urlopen('http://127.0.0.1:17090/metrics', timeout=2) as r:
                coordinator = r.read().decode()
            if not (sum(values(coordinator, 'adx_coordinator_environments', state='Running')) == running):
                raise AssertionError()
            if not (sum(values(coordinator, 'adx_coordinator_node_reserved_cpu_millis')) == reserved):
                raise AssertionError()
            if not (sum(values(coordinator, 'adx_coordinator_queued_requests')) == pending):
                raise AssertionError()
            snapshots = {'coordinator': coordinator}
            for node in nodes():
                nid = node['node']['id']
                host = node['address'].rsplit(':', 1)[0]
                with urllib.request.urlopen(f'http://{host}:17091/metrics', timeout=2) as r:
                    local = r.read().decode()
                for resource in ('cpu_millis', 'memory_bytes', 'disk_bytes'):
                    for measure in ('capacity', 'reserved', 'available', 'overcommitted'):
                        if not (
                            one(coordinator, f'adx_coordinator_node_{measure}_{resource}', node_id=nid)
                            == one(local, f'adx_node_{measure}_{resource}')
                        ):
                            raise AssertionError((nid, measure, resource))
                snapshots[nid] = local
            (Path('/evidence') / f'metrics-{label}.json').write_text(
                json.dumps(
                    {
                        'status': 'passed',
                        'running': running,
                        'reserved_cpu_millis': reserved,
                        'queued': pending,
                        'scrapes': snapshots,
                    },
                    indent=2,
                )
            )
            print(
                (
                    f'[METRICS PASS] '
                    f'{label}'
                    f': running='
                    f'{running}'
                    f', reserved_cpu_millis='
                    f'{reserved}'
                    f', queued='
                    f'{pending}'
                    f'; Coordinator/Node ledgers agree'
                ),
                flush=True,
            )
            return
        except (AssertionError, OSError):
            if time.monotonic() >= end:
                raise
            time.sleep(0.1)
