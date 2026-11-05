"""Frozen one-hour acceptance, distinct from short entry qualification."""

import hashlib
import importlib.util
import json
import math
from pathlib import Path
import statistics
from copy import deepcopy

NODES = ('node1', 'node2', 'afs-m')
CONTRACT_PATH = Path(__file__).with_name('soak-contracts') / 'one-hour-v3.json'
V4_CONTRACT_PATH = CONTRACT_PATH.with_name('warmup-hour-v4.json')

_SPEC = importlib.util.spec_from_file_location('soak_fd_acceptance', Path(__file__).with_name('soak_fd.py'))
fd = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(fd)


def contract_path(policy=None):
    return V4_CONTRACT_PATH if policy and policy.get('observation') else CONTRACT_PATH


def contract(kind='one-hour'):
    return json.loads((V4_CONTRACT_PATH if kind == 'warmup-hour' else CONTRACT_PATH).read_text())


def contract_sha(policy=None):
    return hashlib.sha256(contract_path(policy).read_bytes()).hexdigest()


def number(value, label):
    if (not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)):
        raise ValueError('missing/invalid acceptance counter: ' + label)
    if (value < 0):
        raise ValueError('missing/invalid acceptance counter: ' + label)
    return value


def check_sample(sample, baseline, policy, mode):
    full = bool(policy.get('observation'))
    if set(sample.get('nodes', {})) != set(NODES):
        raise ValueError('acceptance resource role missing')
    if sample.get('instances') != baseline['instances'] or set(sample['instances']) != {'A', 'B'}:
        raise ValueError('acceptance anchor identity changed')
    if len(set(sample['instances'].values())) != 2:
        raise ValueError('acceptance anchor identities are ambiguous')
    for field in ('cycles', 'command', 'file', 'create', 'delete'):
        value = sample.get(field) if field == 'cycles' else sample.get('operations', {}).get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError('invalid sample work counter: ' + field)
    for node, observed in sample['nodes'].items():
        initial = baseline['nodes'][node]
        if observed.get('container_id') != initial['container_id']:
            raise ValueError('acceptance container identity changed: ' + node)
        if not observed.get('memory_cgroup') or (node != 'afs-m' and not observed.get('runtime_cgroup')):
            raise ValueError(node + ' cgroup memory coverage missing')
        if any(observed.get(key) != initial.get(key) for key in ('memory_cgroup', 'runtime_cgroup')):
            raise ValueError(node + ' cgroup observation identity changed')
        if node != 'afs-m' and observed.get('runtime_disk_present') is not True:
            raise ValueError(node + ' runtime disk coverage missing')
        if observed.get('storage_scope') != policy['measurement']['storage_scope']:
            raise ValueError(node + ' backing storage measurement scope missing')
        expected = 'afs-meta:' if node == 'afs-m' else 'afs-node:'
        if sum(name.startswith(expected) for name in observed.get('processes', {})) != 1:
            raise ValueError(node + ' exactly one owned AFS process is required')
        if node == 'afs-m' and any(name.startswith('afs-node:') for name in observed['processes']):
            raise ValueError('Meta role cannot claim DFS reader credit')
        fd_initial = initial
        if full and sample.get('active') is not True:
            fd_initial = deepcopy(initial)
            fd_initial['processes'] = {name: p for name, p in fd_initial['processes'].items()
                                       if not name.startswith(('execd-service:', 'sdk-worker:'))}
            fd_initial['anchor_runtime'] = None
            fd_initial['fds'] = sum(p['fds'] for p in fd_initial['processes'].values())
            fd.attach(fd_initial, mode)
        fd.verify(observed, fd_initial, mode, policy['peak_growth']['fds'])
        for field, maximum in policy['peak_growth'].items():
            metric = 'fds_noncache' if field == 'fds' else field
            current = number(observed.get(metric), node + '.' + metric)
            if current - number(initial.get(metric), node + '.baseline.' + metric) > maximum:
                raise ValueError(node + ' peak resource growth: ' + field)
        if number(observed.get('free_bytes'), 'free_bytes') < policy['min_free_bytes']:
            raise ValueError(node + ' disk reserve exhausted')
        for field in ('memory_events', 'runtime_memory_events'):
            for counter in ('oom', 'oom_kill'):
                if number(observed.get(field, {}).get(counter), field + '.' + counter) != 0:
                    raise ValueError(node + ' OOM event: ' + field)
        processes = observed.get('processes', {})
        terminal = full and sample.get('active') is not True
        expected_processes = {name: p for name, p in initial['processes'].items()
                              if not terminal or not name.startswith(('execd-service:', 'sdk-worker:'))}
        if set(processes) != set(expected_processes):
            raise ValueError(node + ' persistent process set changed')
        if full:
            if observed.get('observation_scope') != policy['observation']['scope']:
                raise ValueError(node + ' full memory observation missing')
            census = observed.get('process_census', [])
            by_pid = {p['pid']: p for p in census}
            if not census or len(by_pid) != len(census):
                raise ValueError(node + ' complete unique PID census missing')
            required = (['afs-meta'] if node == 'afs-m' else policy['observation']['required_node_processes'])
            if node == 'node1':
                required = [*required, *policy['observation']['required_node1_processes']]
            if terminal:
                required = [n for n in required if n != 'execd-service']
            if node == policy['observation']['required_sdk_node'] and not terminal:
                required = [*required, 'sdk-worker']
            if any(sum(name.startswith(role + ':') for name in processes) != 1 for role in required):
                raise ValueError(node + ' required persistent role missing/ambiguous')
        for name, process in processes.items():
            if any(process.get(key) != initial['processes'][name][key] for key in ('pid', 'start_ticks')):
                raise ValueError(node + ' process identity changed: ' + name)
            number(process.get('rss_bytes'), node + '.' + name + '.rss_bytes')
            if full:
                verify_memory(process, initial['processes'][name])
                raw_process = by_pid.get(process['pid'], {})
                if any(process.get(k) != raw_process.get(k)
                       for k in ('start_ticks', 'identity', 'smaps_rollup_raw')):
                    raise ValueError(node + ' persistent profile differs from raw PID census')
                if process['rss_bytes'] - initial['processes'][name]['rss_bytes'] > policy['peak_growth']['rss_bytes']:
                    raise ValueError(node + ' persistent process RSS peak growth: ' + name)
        if sample.get('active') is True and node != 'afs-m':
            anchor, initial_anchor = observed.get('anchor_runtime'), initial.get('anchor_runtime')
            if (not anchor or not initial_anchor or any(anchor.get(key) != initial_anchor.get(key)
                    for key in ('environment_id', 'runtime_id', 'pid', 'start_ticks'))):
                raise ValueError(node + ' physical anchor runtime identity changed or missing')
            if anchor['environment_id'] != sample['instances']['A' if node == 'node1' else 'B']:
                raise ValueError(node + ' physical backend is not the declared anchor')
            for metric in ('rss_bytes', 'fds'):
                if (number(anchor.get(metric), 'anchor.' + metric) - initial_anchor[metric]
                        > policy['peak_growth'][metric]):
                    raise ValueError(node + ' anchor peak resource growth: ' + metric)
            if full:
                verify_memory(anchor, initial_anchor)
                service = next(p for name, p in processes.items() if name.startswith('execd-service:'))
                if (service['identity']['ppid'] != anchor['pid'] or any(
                        service['identity'][k] != anchor['identity'][k]
                        for k in ('exe_device', 'exe_inode', 'pid_namespace'))):
                    raise ValueError(node + ' Execd is not the pinned init service child')


def trend(times, values, bounds, label):
    center = statistics.mean(times)
    mean = statistics.mean(values)
    denominator = sum((value - center) ** 2 for value in times)
    slope = sum((x - center) * (y - mean) for x, y in zip(times, values)) / denominator * 3600
    delta = values[-1] - values[0]
    result = {'window_medians': values, 'end_delta': delta, 'slope_per_hour': slope, 'bounds': bounds}
    if delta > bounds['maximum_end_delta'] or slope > bounds['maximum_slope_per_hour']:
        raise ValueError(label + ' trend exceeded: ' + json.dumps(result))
    return result


def verify_memory(process, initial):
    raw = {}
    for line in process.get('smaps_rollup_raw', '').splitlines():
        fields = line.split()
        if len(fields) == 3 and fields[2] == 'kB':
            raw[fields[0].rstrip(':')] = int(fields[1]) * 1024
    for key, source in [('rss_bytes', 'Rss'), ('pss_anon_bytes', 'Pss_Anon'),
                        ('swap_bytes', 'Swap'), ('swap_pss_bytes', 'SwapPss')]:
        if source not in raw or number(process.get(key), key) != raw[source]:
            raise ValueError('raw smaps differs: ' + key)
    identity = process.get('identity')
    if not identity or identity != initial.get('identity'):
        raise ValueError('persistent executable/namespace/cgroup/parent identity differs')
    if process['swap_bytes'] != 0 or process['swap_pss_bytes'] != 0:
        raise ValueError('Swap invalidates resident memory measurement')


def evaluate(samples, work, mode, policy):
    """Every required window and raw sample participates; never select best intervals."""
    full = bool(policy.get('observation'))
    if full and mode not in policy['modes']:
        raise ValueError('mode has not qualified this warmup contract')
    if full and any(work.get(k) != policy[k] for k in ('warmup_seconds', 'measurement_seconds')):
        raise ValueError('SDK phase duration binding differs from frozen contract')
    counts = work.get('operations', {})
    cycles = number(work.get('cycles'), 'cycles')
    if (work.get('status') != 'passed'
            or work.get('reason') != 'duration-complete'
            or work.get('qualification_only') is not False):
        raise ValueError('one-hour workload duration/content/counts/error/cleanup contract incomplete')
    if (work.get('requested_seconds') != policy['seconds']
            or number(work.get('elapsed_seconds'), 'duration') < policy['seconds']
            or not isinstance(cycles, int)):
        raise ValueError('one-hour workload duration/content/counts/error/cleanup contract incomplete')
    if (cycles < policy['minimum_total_cycles']
            or work.get('verified_bytes') != cycles * 16384
            or work.get('contract_sha256') != contract_sha(policy)):
        raise ValueError('one-hour workload duration/content/counts/error/cleanup contract incomplete')
    if (work.get('payload_transport') != policy['measurement']['payload_transport']
            or not 0 < number(work.get('maximum_publish_command_bytes'), 'control command bytes') <= 2048
            or work.get('errors') != []):
        raise ValueError('one-hour workload duration/content/counts/error/cleanup contract incomplete')
    if (work.get('cleanup_errors') != []
            or work.get('remaining') != []
            or work.get('injected_failure_after_cycles') != 0):
        raise ValueError('one-hour workload duration/content/counts/error/cleanup contract incomplete')
    if (number(counts.get('create'), 'create') - 2 < policy['minimum_total_churn']
            or counts.get('create') != counts.get('delete')
            or counts.get('command') != cycles + counts['create'] - 2):
        raise ValueError('one-hour workload duration/content/counts/error/cleanup contract incomplete')
    if (counts.get('file') != counts.get('command')):
        raise ValueError('one-hour workload duration/content/counts/error/cleanup contract incomplete')
    if (len(samples) < 2
            or samples[0]['elapsed_seconds'] > policy['maximum_boundary_gap_seconds']
            or samples[-1]['elapsed_seconds'] < policy['seconds']):
        raise ValueError('one-hour resource coverage incomplete')
    if (samples[-1]['elapsed_seconds'] != work['elapsed_seconds']
            or samples[-1]['cycles'] != cycles
            or samples[-1].get('operations') != counts):
        raise ValueError('one-hour resource coverage incomplete')
    baseline = samples[0]
    last_progress_time = baseline['observed_monotonic_seconds']
    last_cycles = baseline['cycles']
    previous = baseline
    for sample in samples:
        check_sample(sample, baseline, policy, mode)
        now = number(sample.get('observed_monotonic_seconds'), 'observer monotonic time')
        elapsed = number(sample.get('elapsed_seconds'), 'workload elapsed')
        if (not 0 <= now - previous['observed_monotonic_seconds'] <= policy['maximum_sample_gap_seconds']
                or elapsed < previous['elapsed_seconds'] or sample['cycles'] < last_cycles):
            raise ValueError('one-hour sampling gap or regressed workload progress')
        if sample['cycles'] > last_cycles:
            last_cycles, last_progress_time = sample['cycles'], now
        if sample.get('active') is True and now - last_progress_time > policy['maximum_progress_stall_seconds']:
            raise ValueError('one-hour workload stalled')
        previous = sample
    if baseline['instances'] != work.get('instances'):
        raise ValueError('final anchor identities differ')
    windows = []
    for start in range(0 if full else policy['warmup_seconds'], policy['seconds'], policy['window_seconds']):
        end = start + policy['window_seconds']
        selected = [s for s in samples if s.get('active') is True and start <= s['elapsed_seconds'] < end]
        if (len(selected) < policy['minimum_window_samples']
                or selected[0]['elapsed_seconds'] - start > policy['maximum_sample_gap_seconds']
                or end - selected[-1]['elapsed_seconds'] > policy['maximum_sample_gap_seconds']):
            raise ValueError('missing or incomplete five-minute acceptance window: ' + str(start))
        left, right = selected[0], selected[-1]
        if (right['cycles'] - left['cycles'] < policy['minimum_window_cycles']
                or any(right['operations'][name] - left['operations'][name] < policy['minimum_window_churn']
                       for name in ('create', 'delete'))):
            raise ValueError('five-minute window did not sustain declared work: ' + str(start))
        if (mode == 'dfs'
                and number(right.get('peer_reads'), 'peer_reads') <= number(left.get('peer_reads'), 'peer_reads')):
            raise ValueError('DFS peer path made no progress in window: ' + str(start))
        for node in NODES:
            for name in baseline['nodes'][node]['processes']:
                if statistics.median(s['nodes'][node]['processes'][name]['fd_accounting']['excess']
                                     for s in selected) > policy['fd_accounting']['steady_excess_median']:
                    raise ValueError(node + '.' + name + ' steady cache capacity excess')
        windows.append({'start_seconds': start, 'end_seconds': end, 'samples': len(selected),
                        'cycles': right['cycles'] - left['cycles'],
                        'churn': right['operations']['delete'] - left['operations']['delete'],
                        'nodes': {node: {field: statistics.median(
                            s['nodes'][node]['fds_noncache' if field == 'fds' else field] for s in selected)
                                         for field in policy['trends']} for node in NODES},
                        'processes': {node: {name: statistics.median(s['nodes'][node]['processes'][name]['rss_bytes']
                                                                   for s in selected)
                                             for name in baseline['nodes'][node]['processes']} for node in NODES},
                        'process_fds': {node: {name: statistics.median(
                            s['nodes'][node]['processes'][name]['fd_accounting']['noncache'] for s in selected)
                            for name in baseline['nodes'][node]['processes']} for node in NODES},
                        'raw_fds': {node: statistics.median(s['nodes'][node]['fds'] for s in selected)
                                    for node in NODES},
                        'anchors': {node: {field: statistics.median(s['nodes'][node]['anchor_runtime'][field]
                                                                   for s in selected) for field in ('rss_bytes', 'fds')}
                                    for node in ('node1', 'node2')}})
    warmup_windows = []
    if full:
        warmup_windows = [w for w in windows if w['start_seconds'] < policy['warmup_seconds']]
        windows = [w for w in windows if w['start_seconds'] >= policy['warmup_seconds']]
        measured = [s for s in samples if s.get('active') is True
                    and policy['warmup_seconds'] <= s['elapsed_seconds'] < policy['seconds']]
        if (measured[-1]['cycles'] - measured[0]['cycles'] < policy['minimum_measurement_cycles']
                or any(measured[-1]['operations'][k] - measured[0]['operations'][k]
                       < policy['minimum_measurement_churn'] for k in ('create', 'delete'))):
            raise ValueError('measurement-only work insufficient; warmup cannot supply counts')
    times = [(window['start_seconds'] + window['end_seconds']) / 2 for window in windows]
    trends = {}
    for node in NODES:
        trends[node] = {field: trend(times, [w['nodes'][node][field] for w in windows], bounds, node + '.' + field)
                        for field, bounds in policy['trends'].items()}
        trends[node]['processes'] = {
            name: trend(times, [w['processes'][node][name] for w in windows],
                        policy['process_rss_trend'], node + '.' + name)
            for name in baseline['nodes'][node]['processes']}
        if full:
            trends[node]['process_pss_anon'] = {
                name: trend(times, [statistics.median(s['nodes'][node]['processes'][name]['pss_anon_bytes']
                    for s in samples if s.get('active') is True
                    and w['start_seconds'] <= s['elapsed_seconds'] < w['end_seconds']) for w in windows],
                    policy['process_pss_anon_trend'], node + '.' + name + '.pss_anon')
                for name in baseline['nodes'][node]['processes']}
        trends[node]['process_noncache_fds'] = {
            name: trend(times, [w['process_fds'][node][name] for w in windows],
                        policy['trends']['fds'], node + '.' + name + '.noncache_fds')
            for name in baseline['nodes'][node]['processes']}
        raw = [w['raw_fds'][node] for w in windows]
        center = statistics.mean(times)
        trends[node]['raw_fds_diagnostic'] = {
            'window_medians': raw, 'end_delta': raw[-1] - raw[0],
            'slope_per_hour': sum((x - center) * (y - statistics.mean(raw)) for x, y in zip(times, raw))
                / sum((x - center)**2 for x in times) * 3600, 'gated': False}
        if node != 'afs-m':
            trends[node]['anchor_runtime_rss'] = trend(
                times, [w['anchors'][node]['rss_bytes'] for w in windows],
                policy['process_rss_trend'], node + '.anchor.rss')
            trends[node]['anchor_runtime_fds'] = trend(
                times, [w['anchors'][node]['fds'] for w in windows], policy['trends']['fds'], node + '.anchor.fds')
    phases = ({'warmup_seconds': policy['warmup_seconds'], 'measurement_seconds': policy['measurement_seconds'],
               'warmup_windows': warmup_windows} if full else {})
    return {'status': 'passed', 'contract_id': policy['id'], 'qualification_only': False, **phases,
            'actual_seconds': work['elapsed_seconds'], 'windows': windows, 'trends': trends,
            'fault_recovery_claimed': False, 'performance_acceptance': False}
