"""Read-only Linux memory profiles in the owning Docker PID namespace (v4 only)."""
import json
import os
from pathlib import Path

SCOPE = 'persistent-smaps-v4'
EXTRA_NAMES = ('otelcol-contrib', 'redis-server', 'adxctl')


def stat_fields(raw):
    fields = raw.rsplit(')', 1)[1].split()
    return {'pid': int(raw.split(' ', 1)[0]), 'ppid': int(fields[1]),
            'start_ticks': int(fields[19]), 'state': fields[0]}


def smaps(raw):
    result = {}
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) == 3 and fields[2] == 'kB':
            result[fields[0].rstrip(':')] = int(fields[1]) * 1024
    return result


def profile(pid, proc=Path('/proc')):
    directory = proc / str(pid)
    before = stat_fields((directory / 'stat').read_text())
    raw = (directory / 'smaps_rollup').read_text()
    values = smaps(raw)
    executable = (directory / 'exe').stat()
    identity = {'exe': os.readlink(directory / 'exe'), 'exe_device': executable.st_dev,
                'exe_inode': executable.st_ino, 'pid_namespace': os.readlink(directory / 'ns/pid'),
                'cgroup': (directory / 'cgroup').read_text(), 'ppid': before['ppid']}
    after = stat_fields((directory / 'stat').read_text())
    changed = any(before[k] != after[k] for k in ('pid', 'start_ticks', 'ppid'))
    zombie = 'Z' in (before['state'], after['state'])
    if changed or zombie or identity['cgroup'] != (directory / 'cgroup').read_text():
        raise ValueError('process identity changed while reading smaps')
    required = ('Rss', 'Pss_Anon', 'Swap', 'SwapPss')
    if any(k not in values for k in required):
        raise ValueError('incomplete smaps rollup')
    return {**before, 'identity': identity, 'smaps_rollup_raw': raw, 'rss_bytes': values['Rss'],
            'pss_anon_bytes': values['Pss_Anon'], 'swap_bytes': values['Swap'],
            'swap_pss_bytes': values['SwapPss']}


def census(proc=Path('/proc')):
    rows = []
    for directory in sorted(proc.iterdir()):
        if not directory.name.isdigit():
            continue
        row = {'pid': int(directory.name)}
        try:
            row.update(stat_fields((directory / 'stat').read_text()))
            row['comm'] = (directory / 'comm').read_text().strip()
            if row['state'] == 'Z':
                row['race'] = 'zombie'
            else:
                row.update(profile(row['pid'], proc))
        except (FileNotFoundError, ProcessLookupError, ValueError) as error:
            row.update(race='exited_or_changed_during_census', error=repr(error))
        rows.append(row)
    return rows


def service_child(anchor, rows):
    matches = [p for p in rows if not p.get('race') and p.get('ppid') == anchor['pid']
               and p.get('identity', {}).get('pid_namespace') == anchor['identity']['pid_namespace']
               and all(p.get('identity', {}).get(k) == anchor['identity'][k]
                       for k in ('exe_device', 'exe_inode'))]
    if len(matches) != 1:
        raise ValueError('actual Execd service child missing or ambiguous')
    return matches[0]


def enrich(processes, anchor, *, sdk_worker=None, proc=Path('/proc')):
    rows = census(proc)
    by_pid = {r['pid']: r for r in rows if not r.get('race')}
    for name, process in processes.items():
        record = by_pid.get(process['pid'])
        if record is None or process['start_ticks'] != record['start_ticks']:
            raise ValueError('required persistent process changed: ' + name)
        process.update({k: v for k, v in record.items() if k not in ('comm', 'state')})
    if anchor:
        record = by_pid.get(anchor['pid'])
        if not record or record['start_ticks'] != anchor['start_ticks']:
            raise ValueError('anchor init changed during memory observation')
        anchor.update({k: v for k, v in record.items() if k not in ('comm', 'state')})
        child = service_child(anchor, rows)
        processes['execd-service:' + str(child['pid'])] = {
            **child, 'fds': len(list((proc / str(child['pid']) / 'fd').iterdir()))}
    if sdk_worker:
        worker = json.loads(Path(sdk_worker).read_text())
        record = by_pid.get(worker['pid'])
        if (not record or record['start_ticks'] != worker['start_ticks']
                or 'python' not in Path(record['identity']['exe']).name):
            raise ValueError('installed SDK worker identity missing or changed')
        if b'/opt/adx/e2e/soak_sdk.py' not in (proc / str(worker['pid']) / 'cmdline').read_bytes().split(b'\0'):
            raise ValueError('SDK worker is not the declared soak script')
        processes['sdk-worker:' + str(worker['pid'])] = {
            **record, 'fds': len(list((proc / str(worker['pid']) / 'fd').iterdir()))}
    return rows
