"""Versioned FD accounting: bounded DFS cache credit, all other FDs remain charged."""
import importlib.util
from pathlib import Path

_SOURCE = Path(__file__).with_name('afs-tests') / 'tools/fd_inventory.py'
if not _SOURCE.is_file():
    _SOURCE = Path(__file__).resolve().parents[2] / 'afs/tests/tools/fd_inventory.py'
_SPEC = importlib.util.spec_from_file_location('soak_fd_inventory', _SOURCE)
inventory = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(inventory)
CAPACITY = 256
SCOPE = 'dfs-reader-accounting-v3'


def calculated(observed, mode):
    if mode not in ('ownerfs', 'dfs'):
        raise ValueError('FD accounting mode missing')
    processes, total, noncache = {}, 0, 0
    for name, process in observed['processes'].items():
        raw = inventory.integer(process.get('fds'))
        is_node = name == 'afs-node:' + str(process.get('pid'))
        if name.startswith('afs-node:') and not is_node:
            raise ValueError('AFS process name/PID binding mismatch')
        if is_node:
            census = process.get('fd_census')
            if (not isinstance(census, dict)
                    or any(census.get(key) != process.get(key) for key in ('pid', 'start_ticks'))):
                raise ValueError('FD census process binding missing')
            if (census.get('cache_root') is not None) != (mode == 'dfs'):
                raise ValueError('FD cache root does not match workload mode')
            accounting = inventory.classify(census, CAPACITY if mode == 'dfs' else 0)
            if accounting['total'] != raw:
                raise ValueError('raw FD count differs from census')
        else:
            if 'fd_census' in process:
                raise ValueError('non-AFS process cannot claim cache credit')
            accounting = {'total': raw, 'eligible': 0, 'accounted': 0, 'noncache': raw,
                          'excess': 0, 'duplicates': 0, 'targets': []}
        processes[name] = accounting
        total += raw
        noncache += accounting['noncache']
    anchor = observed.get('anchor_runtime')
    anchor_fds = inventory.integer(anchor.get('fds')) if anchor else 0
    total += anchor_fds
    noncache += anchor_fds
    if inventory.integer(observed.get('fds')) != total:
        raise ValueError('node FD total does not equal persistent processes plus anchor')
    return processes, noncache


def attach(observed, mode):
    processes, noncache = calculated(observed, mode)
    for name, accounting in processes.items():
        observed['processes'][name]['fd_accounting'] = accounting
    observed.update(fds_noncache=noncache, fd_accounting_scope=SCOPE, fd_mode=mode)
    return observed


def verify(observed, initial, mode, maximum):
    for item in (initial, observed):
        processes, noncache = calculated(item, mode)
        if (item.get('fds_noncache') != noncache or item.get('fd_accounting_scope') != SCOPE
                or item.get('fd_mode') != mode):
            raise ValueError('FD aggregate accounting mismatch')
        if any(item['processes'][name].get('fd_accounting') != accounting for name, accounting in processes.items()):
            raise ValueError('FD process accounting mismatch')
    if set(observed['processes']) != set(initial['processes']):
        raise ValueError('FD persistent process set changed')
    allowances = 0
    for name, process in observed['processes'].items():
        first = initial['processes'][name]
        if any(process.get(key) != first.get(key) for key in ('pid', 'start_ticks')):
            raise ValueError('FD process identity changed')
        node = name.startswith('afs-node:')
        if node and process['fd_census']['cache_root'] != first['fd_census']['cache_root']:
            raise ValueError('FD cache root identity changed')
        allowance = CAPACITY if mode == 'dfs' and node else 0
        allowances += allowance
        if process['fd_accounting']['noncache'] - first['fd_accounting']['noncache'] > maximum:
            raise ValueError('FD process peak noncache growth: ' + name)
        if process['fds'] - first['fds'] > maximum + allowance:
            raise ValueError('FD process peak raw growth: ' + name)
    if observed['fds'] - initial['fds'] > maximum + allowances:
        raise ValueError('FD node peak raw growth')
