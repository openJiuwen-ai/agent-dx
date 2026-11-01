"""Frozen one-hour acceptance, distinct from short entry qualification."""

import hashlib
import json
import math
from pathlib import Path
import statistics

NODES = ('node1', 'node2', 'afs-m')
CONTRACT_PATH = Path(__file__).with_name('soak-contracts') / 'one-hour-v1.json'


def contract():
    return json.loads(CONTRACT_PATH.read_text())


def contract_sha():
    return hashlib.sha256(CONTRACT_PATH.read_bytes()).hexdigest()


def number(value, label):
    if (not isinstance(value, (int, float)) or isinstance(value, bool)
            or not math.isfinite(value) or value < 0):
        raise ValueError('missing/invalid acceptance counter: ' + label)
    return value


def check_sample(sample, baseline, policy):
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
        for field, maximum in policy['peak_growth'].items():
            current = number(observed.get(field), node + '.' + field)
            if current - number(initial.get(field), node + '.baseline.' + field) > maximum:
                raise ValueError(node + ' peak resource growth: ' + field)
        if number(observed.get('free_bytes'), 'free_bytes') < policy['min_free_bytes']:
            raise ValueError(node + ' disk reserve exhausted')
        for field in ('memory_events', 'runtime_memory_events'):
            for counter in ('oom', 'oom_kill'):
                if number(observed.get(field, {}).get(counter), field + '.' + counter) != 0:
                    raise ValueError(node + ' OOM event: ' + field)
        processes = observed.get('processes', {})
        if set(processes) != set(initial['processes']):
            raise ValueError(node + ' persistent process set changed')
        expected = 'afs-meta:' if node == 'afs-m' else 'afs-node:'
        if not any(name.startswith(expected) for name in processes):
            raise ValueError(node + ' AFS process observation missing')
        for name, process in processes.items():
            if any(process.get(key) != initial['processes'][name][key] for key in ('pid', 'start_ticks')):
                raise ValueError(node + ' process identity changed: ' + name)
            number(process.get('rss_bytes'), node + '.' + name + '.rss_bytes')


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


def evaluate(samples, work, mode, policy):
    """Every required window and raw sample participates; never select best intervals."""
    counts = work.get('operations', {})
    cycles = number(work.get('cycles'), 'cycles')
    if (work.get('status') != 'passed' or work.get('reason') != 'duration-complete'
            or work.get('qualification_only') is not False or work.get('requested_seconds') != policy['seconds']
            or number(work.get('elapsed_seconds'), 'duration') < policy['seconds']
            or not isinstance(cycles, int) or cycles < policy['minimum_total_cycles']
            or work.get('verified_bytes') != cycles * 16384
            or work.get('contract_sha256') != contract_sha()
            or work.get('errors') != [] or work.get('cleanup_errors') != [] or work.get('remaining') != []
            or work.get('injected_failure_after_cycles') != 0
            or number(counts.get('create'), 'create') - 2 < policy['minimum_total_churn']
            or counts.get('create') != counts.get('delete')
            or counts.get('command') != cycles + counts['create'] - 2
            or counts.get('file') != counts.get('command')):
        raise ValueError('one-hour workload duration/content/counts/error/cleanup contract incomplete')
    if (len(samples) < 2 or samples[0]['elapsed_seconds'] > policy['maximum_boundary_gap_seconds']
            or samples[-1]['elapsed_seconds'] < policy['seconds']
            or samples[-1]['elapsed_seconds'] != work['elapsed_seconds']
            or samples[-1]['cycles'] != cycles or samples[-1].get('operations') != counts):
        raise ValueError('one-hour resource coverage incomplete')
    baseline = samples[0]
    last_progress_time = baseline['observed_monotonic_seconds']
    last_cycles = baseline['cycles']
    previous = baseline
    for sample in samples:
        check_sample(sample, baseline, policy)
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
    for start in range(policy['warmup_seconds'], policy['seconds'], policy['window_seconds']):
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
        windows.append({'start_seconds': start, 'end_seconds': end, 'samples': len(selected),
                        'cycles': right['cycles'] - left['cycles'],
                        'churn': right['operations']['delete'] - left['operations']['delete'],
                        'nodes': {node: {field: statistics.median(s['nodes'][node][field] for s in selected)
                                         for field in policy['trends']} for node in NODES},
                        'processes': {node: {name: statistics.median(s['nodes'][node]['processes'][name]['rss_bytes']
                                                                   for s in selected)
                                             for name in baseline['nodes'][node]['processes']} for node in NODES}})
    times = [(window['start_seconds'] + window['end_seconds']) / 2 for window in windows]
    trends = {}
    for node in NODES:
        trends[node] = {field: trend(times, [w['nodes'][node][field] for w in windows], bounds, node + '.' + field)
                        for field, bounds in policy['trends'].items()}
        trends[node]['processes'] = {
            name: trend(times, [w['processes'][node][name] for w in windows],
                        policy['process_rss_trend'], node + '.' + name)
            for name in baseline['nodes'][node]['processes']}
    return {'status': 'passed', 'contract_id': policy['id'], 'qualification_only': False,
            'actual_seconds': work['elapsed_seconds'], 'windows': windows, 'trends': trends,
            'fault_recovery_claimed': False, 'performance_acceptance': False}
