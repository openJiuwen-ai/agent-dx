"""Explicit fixed-package continuous soak; qualification is not a 24-hour result."""

import argparse
import importlib.util
import json
import math
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid

_spec = importlib.util.spec_from_file_location('soak_docker_adapter', Path(__file__).with_name('docker_run.py'))
docker_run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(docker_run)
_acceptance_spec = importlib.util.spec_from_file_location('soak_formal_acceptance',
                                                       Path(__file__).with_name('soak_acceptance.py'))
acceptance = importlib.util.module_from_spec(_acceptance_spec)
_acceptance_spec.loader.exec_module(acceptance)

CHECK = 'afs-continuous-soak'
NODES = ('node1', 'node2', 'afs-m')


def validate_options(options):
    if not 1 <= options['seconds'] <= 86400:
        raise ValueError('continuous workload duration must be 1..86400 seconds')
    if not 1 <= options['sample_seconds'] <= 300:
        raise ValueError('sampling interval must be 1..300 seconds')


def validate_workload(report, seconds):
    counts = report.get('operations', {})
    elapsed = report.get('elapsed_seconds')
    if (report.get('status') != 'passed'
            or report.get('reason') != 'duration-complete'
            or report.get('injected_failure_after_cycles', 0) != 0):
        raise ValueError('continuous workload duration/work/counts/cleanup incomplete')
    if (report.get('requested_seconds') != seconds
            or not isinstance(elapsed, (int, float))
            or isinstance(elapsed, bool)):
        raise ValueError('continuous workload duration/work/counts/cleanup incomplete')
    if (not math.isfinite(elapsed)
            or elapsed < seconds
            or report.get('cycles', 0) < 2):
        raise ValueError('continuous workload duration/work/counts/cleanup incomplete')
    if (report.get('errors') != []
            or report.get('cleanup_errors') != []
            or report.get('remaining') != []):
        raise ValueError('continuous workload duration/work/counts/cleanup incomplete')
    if (set(report.get('instances', {})) != {'A', 'B'}
            or len(set(report['instances'].values())) != 2):
        raise ValueError('continuous workload duration/work/counts/cleanup incomplete')
    for name in ('command', 'file', 'create', 'delete'):
        value = counts.get(name)
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError('continuous workload duration/work/counts/cleanup incomplete')
    if (any(counts.get(n, 0) < 2 for n in ('command', 'file'))
            or counts.get('create', 0) < 3
            or counts.get('create') != counts.get('delete')):
        raise ValueError('continuous workload duration/work/counts/cleanup incomplete')


def validate_samples(samples, limits, interval, *, baseline=None, require_progress=True, fd_metric='fds',
                     allow_retired_workers=False):
    if len(samples) < 2 or (require_progress and samples[-1]['cycles'] <= samples[0]['cycles']):
        raise ValueError('resource samples or sustained workload progress missing')
    last_progress = samples[0].get('observed_monotonic_seconds', samples[0]['elapsed_seconds'])
    last_cycles = samples[0]['cycles']
    for index, sample in enumerate(samples):
        now = sample.get('observed_monotonic_seconds', sample['elapsed_seconds'])
        if set(sample['nodes']) != set(NODES):
            raise ValueError('resource sample is missing a deployment role')
        previous = samples[index - 1] if index else sample
        if not 0 <= now - previous.get('observed_monotonic_seconds', previous['elapsed_seconds']) <= interval * 3 + 30:
            raise ValueError('resource sampling gap exceeded frozen bound')
        if sample['cycles'] < last_cycles:
            raise ValueError('workload progress regressed')
        if sample['cycles'] > last_cycles:
            last_progress, last_cycles = now, sample['cycles']
        if sample.get('active', True) and now - last_progress > max(30, interval * 3):
            raise ValueError('workload heartbeat stalled')
        for node, observed in sample['nodes'].items():
            initial = (baseline or samples[0])['nodes'][node]
            for field in ('memory_bytes', 'disk_bytes', 'fds', 'mounts', 'free_bytes'):
                metric = fd_metric if field == 'fds' else field
                value = observed.get(metric)
                if (not isinstance(value, (int, float))
                        or isinstance(value, bool)
                        or not math.isfinite(value)):
                    raise ValueError('invalid resource counter: ' + field)
                if (value < 0):
                    raise ValueError('invalid resource counter: ' + field)
                if field != 'free_bytes' and value - initial[metric] > limits[field]:
                    raise ValueError(node + ' resource growth exceeded: ' + field)
            if observed['free_bytes'] < limits['min_free_bytes']:
                raise ValueError(node + ' free disk below frozen reserve')
            if not observed['processes']:
                raise ValueError(node + ' has no monitored persistent processes')
            for name, process in initial['processes'].items():
                if (allow_retired_workers and sample.get('active') is not True
                        and name.startswith(('execd-service:', 'sdk-worker:'))):
                    continue
                current = observed['processes'].get(name, {})
                if any(current.get(k) != process[k] for k in ('pid', 'start_ticks')):
                    raise ValueError(node + ' persistent process identity changed: ' + name)



def cgroup_resources(path):
    stat = dict(line.split() for line in (path / 'memory.stat').read_text().splitlines())
    events = {}
    for line in (path / 'memory.events').read_text().splitlines():
        key, value = line.split()
        events[key] = int(value)
    return {'bytes': int((path / 'memory.current').read_text()), 'anon_bytes': int(stat['anon']),
            'file_bytes': int(stat['file']), 'events': events, 'path': str(path)}


def observe_resources(run, progress, fd_roots=None, *, full_memory=False):
    nodes = {}
    for node in NODES:
        arguments = [progress['instances']['A' if node == 'node1' else 'B']] if (
            node != 'afs-m' and progress['status'] == 'running') else []
        if fd_roots is not None:
            arguments += ['--mode', fd_roots['mode']]
            if node in fd_roots:
                arguments += ['--cache-root', fd_roots[node]]
        if full_memory:
            arguments += ['--full-memory']
            if node == 'node1' and progress['status'] == 'running':
                arguments += ['--sdk-worker', '/evidence/soak-sdk/worker.json']
        observed = json.loads(run.execute(node, 'python3', '/opt/adx/e2e/soak_probe.py', *arguments, timeout=30))
        item = json.loads(run.docker('inspect', run.owned[node], stream=False))[0]
        pid = item['State']['Pid']
        group = next(line.split(':', 2)[2] for line in Path(f'/proc/{pid}/cgroup').read_text().splitlines()
                     if line.startswith('0::'))
        memory = cgroup_resources(Path('/sys/fs/cgroup') / group.lstrip('/'))
        observed.update(memory_bytes=memory['bytes'], memory_anon_bytes=memory['anon_bytes'],
                        memory_file_bytes=memory['file_bytes'], memory_events=memory['events'],
                        memory_cgroup=memory['path'])
        if node != 'afs-m':
            runtime = cgroup_resources(Path('/sys/fs/cgroup') / (run.id + '-' + node))
            observed.update(runtime_memory_bytes=runtime['bytes'], runtime_anon_bytes=runtime['anon_bytes'],
                            runtime_file_bytes=runtime['file_bytes'], runtime_memory_events=runtime['events'],
                            runtime_cgroup=runtime['path'])
        else:
            observed.update(runtime_memory_bytes=0, runtime_anon_bytes=0, runtime_file_bytes=0,
                            runtime_memory_events={'oom': 0, 'oom_kill': 0}, runtime_cgroup=None)
        observed['container_id'] = item['Id']
        nodes[node] = observed
    return {'elapsed_seconds': progress['elapsed_seconds'], 'observed_monotonic_seconds': time.monotonic(),
            'cycles': progress['cycles'], 'operations': progress['operations'], 'instances': progress['instances'],
            'active': progress['status'] == 'running', 'nodes': nodes}


def worker_gone(run, receipt):
    result = run.execute('node1', 'python3', '-c',
        'from pathlib import Path;import sys;p=Path("/proc")/sys.argv[1]/"stat";'
        'v=p.read_text().rsplit(")",1)[1].split() if p.exists() else [];'
        'print("gone" if not v or v[0]=="Z" or int(v[19])!=int(sys.argv[2]) else "running")',
        receipt['pid'], receipt['start_ticks'], timeout=30)
    return result.strip() == 'gone'


def stop_worker(run, directory, receipt):
    (directory / 'stop.json').write_text(json.dumps({'owner': run.id}))
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if (directory / 'ended.json').exists() and worker_gone(run, receipt):
            ended = json.loads((directory / 'ended.json').read_text())
            if ended.get('owner') != run.id or ended.get('pid') != receipt['pid']:
                raise ValueError('SDK exit receipt ownership mismatch')
            return
        time.sleep(0.25)
    # Recovery only: an unresponsive owned SDK worker is a cleanup failure, never PASS.
    run.execute('node1', 'python3', '-c',
        'from pathlib import Path;import os,signal,sys;pid=int(sys.argv[1]);p=Path(f"/proc/{pid}");'
        'v=(p/"stat").read_text().rsplit(")",1)[1].split() if p.exists() else [];'
        'owned=v and int(v[19])==int(sys.argv[2]) and b"soak_sdk.py" in (p/"cmdline").read_bytes();'
        'os.kill(pid,signal.SIGKILL) if owned else None', receipt['pid'], receipt['start_ticks'])
    raise ValueError('SDK worker failed to terminate normally within 180 seconds')


def scenarios(run, checks, options, limits, policy=None):
    product = run.afs_product
    observation = policy if policy and policy.get('observation') else (
        acceptance.contract('warmup-hour') if options.get('qualify_v4_observer') else None)
    if observation:
        (run.output / 'soak-observer-contract.json').write_bytes(acceptance.contract_path(observation).read_bytes())
    if policy:
        (run.output / 'soak-contract.json').write_bytes(acceptance.contract_path(policy).read_bytes())
    if observation:
        for node in ('node1', 'node2'):
            deployment = json.loads((run.output / ('deployment-' + node + '.json')).read_text())
            configs = [s['config'] for s in deployment['services']
                       if isinstance(s.get('config'), dict) and 'execd_env' in s['config']]
            if len(configs) != 1 or any(k in configs[0]['execd_env'] for k in (
                    'EXECD_COMMAND_RESULT_TTL_SECS', 'EXECD_COMMAND_REGISTRY_MAX_RECORDS')):
                raise ValueError('v4 requires unchanged Execd default TTL/capacity')
    product.sdk({'mode': 'warm'}, run.output / 'afs-sdk-warm.json')
    sys.path.append(str(Path(__file__).resolve().parents[2] / 'afs/tests'))
    try:
        from fixtures.common import Case
        from fixtures.multivm import Cluster
        from suites.operations import peer_counter
        with tempfile.TemporaryDirectory(prefix='adx-soak-inputs-') as temp, run.case(CHECK, checks) as record:
            inputs = Path(temp)
            for name in ('afs-meta', 'afs-node', 'artifact-identity.json'):
                base = '/opt/adx/e2e/afs-tests/inputs/' if name.endswith('.json') else '/opt/adx/package/bin/'
                product.copy_from('node1', base + name, inputs / name)
                if not name.endswith('.json'):
                    (inputs / name).chmod(0o755)
            output = run.output / 'soak-afs'
            output.mkdir()
            directory = run.output / 'soak-sdk'
            directory.mkdir()
            mode = options['mode']
            cluster = Cluster(product.config, inputs, output,
                              Case('afs.sdk.soak.' + mode, ('cluster-sdk',), mode, (), None),
                              'afs-test-' + uuid.uuid4().hex)
            product.active = cluster
            samples, receipt = [], None
            report = {'status': 'failed', 'options': options, 'limits': limits, 'samples': 0,
                      'qualification_only': policy is None, 'cleanup_errors': [], 'mode': mode,
                      'contract_id': policy['id'] if policy else None,
                      'contract_sha256': options.get('contract_sha256'),
                      'observer_contract_sha256': acceptance.contract_sha(observation) if observation else None}
            try:
                cluster.prepare()
                fd_roots = ({'mode': mode, **({
                    node: str(Path(cluster.role_dir(role)) / 'state' / 'dfs' / 'chunks')
                    for node, role in (('node1', 'A'), ('node2', 'B'))} if mode == 'dfs' else {})}
                    if policy or observation else None)
                peer_before = peer_counter(cluster, 'B') if mode == 'dfs' else None
                config = dict(options, owner=run.id, special='soak', paths={}, identities={})
                for role in ('A', 'B'):
                    config['paths'][role] = cluster.workspace(role)
                    value = cluster.python(role,
                        'from pathlib import Path\npid=int(Path("/tmp/adx-e2e/sandboxd.pid").read_text());'
                        'assert os.readlink("/proc/self/ns/mnt")==os.readlink(f"/proc/{pid}/ns/mnt");'
                        's=os.stat(sys.argv[1]);print(json.dumps([s.st_dev,s.st_ino]))',
                        cluster.workspace(role), owner=False)
                    config['identities'][role] = json.loads(value['stdout'])
                (directory / 'input.json').write_text(json.dumps(config, indent=2))
                raw = run.execute('node1', 'python3', '-c',
                    'import json,subprocess;from pathlib import Path;'
                    'd=Path("/evidence/soak-sdk");'
                    'f=(d/"console.log").open("x");'
                    'p=subprocess.Popen(["/opt/adx/client/bin/python","-B","/opt/adx/e2e/soak_sdk.py",'
                    '"--input",str(d/"input.json")],stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT,'
                    'start_new_session=True);'
                    'pstat=Path("/proc")/str(p.pid)/"stat";'
                    'v={"pid":p.pid,"start_ticks":int(pstat.read_text().rsplit(")",1)[1].split()[19])};'
                    '(d/"worker.json").write_text(json.dumps(v));print(json.dumps(v))')
                receipt = json.loads(raw)
                # Preparation and bounded final SDK call have separate reserves, never counted as load duration.
                deadline = time.monotonic() + options['seconds'] + 600
                next_sample = 0
                last_cycles, last_progress_at = 0, time.monotonic()
                retiring_at = None
                while not (directory / 'ended.json').exists():
                    if time.monotonic() >= deadline:
                        raise subprocess.TimeoutExpired('continuous SDK workload', options['seconds'] + 600)
                    if (directory / 'progress.json').exists() and time.monotonic() >= next_sample:
                        progress = json.loads((directory / 'progress.json').read_text())
                        if progress['cycles'] > 0:
                            if progress['cycles'] > last_cycles:
                                last_cycles, last_progress_at = progress['cycles'], time.monotonic()
                            elif (progress['status'] == 'running'
                                  and time.monotonic() - last_progress_at > max(30, options['sample_seconds'] * 3)):
                                raise ValueError('workload heartbeat stalled')
                            if progress['status'] != 'running':
                                retiring_at = retiring_at or time.monotonic()
                                if time.monotonic() - retiring_at > 180:
                                    raise ValueError('SDK terminal cleanup exceeded 180 seconds')
                            observed = observe_resources(run, progress, fd_roots,
                                                         full_memory=bool(observation))
                            if policy and mode == 'dfs':
                                observed['peer_reads'] = peer_counter(cluster, 'B')
                            samples.append(observed)
                            with (run.output / 'soak-samples.jsonl').open('a') as stream:
                                stream.write(json.dumps(observed) + '\n')
                            if len(samples) > 1:
                                validate_samples(samples[-2:], limits, options['sample_seconds'],
                                                 baseline=samples[0], require_progress=False,
                                                 fd_metric='fds_noncache' if policy or observation else 'fds',
                                                 allow_retired_workers=bool(observation))
                            if policy or observation:
                                sample_policy = policy or observation
                                acceptance.check_sample(observed, samples[0], sample_policy, mode)
                                if (len(samples) > 1
                                        and observed['observed_monotonic_seconds']
                                        - samples[-2]['observed_monotonic_seconds']
                                        > sample_policy['maximum_sample_gap_seconds']):
                                    raise ValueError('one-hour resource sampling gap exceeded 45 seconds')
                            run.event(f'SOAK {mode} elapsed={progress["elapsed_seconds"]:.1f}s '
                                      f'cycles={progress["cycles"]}')
                            next_sample = time.monotonic() + options['sample_seconds']
                    time.sleep(0.25)
                workload = json.loads((directory / 'workload.json').read_text())
                report['workload'] = workload
                final_sample = observe_resources(run, workload, fd_roots,
                                                 full_memory=bool(observation))
                if policy and mode == 'dfs':
                    final_sample['peer_reads'] = peer_counter(cluster, 'B')
                samples.append(final_sample)
                with (run.output / 'soak-samples.jsonl').open('a') as stream:
                    stream.write(json.dumps(final_sample) + '\n')
                if workload.get('owner') != run.id or workload.get('mode') != mode:
                    raise ValueError('workload result identity mismatch')
                validate_workload(workload, options['seconds'])
                validate_samples(samples, limits, options['sample_seconds'],
                                 fd_metric='fds_noncache' if policy or observation else 'fds',
                                 allow_retired_workers=bool(observation))
                if policy:
                    report['acceptance'] = acceptance.evaluate(samples, workload, mode, policy)
                if mode == 'dfs':
                    report['peer_reads'] = {'before': peer_before, 'after': peer_counter(cluster, 'B')}
                    if report['peer_reads']['after'] <= peer_before:
                        raise ValueError('DFS soak did not prove remote peer reads')
                report['status'] = 'passed'
            except Exception as exc:
                report['error'] = type(exc).__name__ + ': ' + str(exc)
                report['reason'] = 'cancelled' if isinstance(exc, InterruptedError) else 'error'
                raise
            finally:
                if receipt is not None:
                    try:
                        stop_worker(run, directory, receipt)
                    except Exception as exc:
                        product.cleanup_errors.append(str(exc))
                for node in run.nodes:
                    try:
                        run.helper(node, 'empty', node)
                    except Exception as exc:
                        product.cleanup_errors.append(node + ' Environment/backend cleanup: ' + str(exc))
                        try:
                            run.helper(node, 'cleanup', node, timeout=180)
                        except Exception as error:
                            product.cleanup_errors.append(str(error))
                cleanup = cluster.cleanup()
                product.active = None
                if cleanup['status'] != 'PASS':
                    product.cleanup_errors += cleanup['errors'] or ['AFS normal cleanup unconfirmed']
                report['samples'] = len(samples)
                if (directory / 'workload.json').exists():
                    report['workload'] = json.loads((directory / 'workload.json').read_text())
                report['cleanup_errors'] = list(product.cleanup_errors)
                if product.cleanup_errors:
                    report['status'] = 'failed'
                if policy:
                    report.setdefault('acceptance', {'status': 'failed', 'error': report.get('error'),
                                                     'qualification_only': False})
                    report['acceptance'].update(contract_id=policy['id'],
                                                contract_sha256=options['contract_sha256'],
                                                cleanup_errors=list(product.cleanup_errors))
                    if product.cleanup_errors:
                        report['acceptance']['status'] = 'failed'
                    (run.output / 'soak-acceptance.json').write_text(
                        json.dumps(report['acceptance'], indent=2) + '\n')
                (run.output / 'soak-result.json').write_text(json.dumps(report, indent=2) + '\n')
                assertions = (
                    'continuous-confirmed-content', 'command-file-and-churn', 'duration',
                    'resource-sampling-and-bounds', 'stable-identities', 'normal-cleanup')
                if policy:
                    assertions += ('steady-resource-trends', 'per-window-work-and-peer-progress', 'no-oom')
                record['subcases'] = [{'id': n, 'status': report['status']} for n in assertions]
                if report['status'] != 'passed' and 'error' not in report:
                    raise ValueError('continuous soak cleanup failed')
    finally:
        sys.path.pop(0)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('bundle', 'registry-images', 'image-export', 'artifact-commit', 'output'):
        parser.add_argument('--' + flag, required=True)
    parser.add_argument('--mode', choices=('ownerfs', 'dfs'), required=True)
    parser.add_argument('--acceptance', choices=('qualification', 'one-hour', 'warmup-hour'), default='qualification')
    parser.add_argument('--seconds', type=int, required=True)
    parser.add_argument('--qualify-v4-observer', action='store_true',
                        help='OwnerFs qualification only; exercise v4 probes without formal duration claims')
    parser.add_argument('--sample-seconds', type=int)
    parser.add_argument('--max-memory-growth-mib', type=int, default=512)
    parser.add_argument('--max-disk-growth-mib', type=int, default=256)
    parser.add_argument('--max-fd-growth', type=int, default=128)
    parser.add_argument('--max-mount-growth', type=int, default=8)
    parser.add_argument('--min-free-mib', type=int, default=2048)
    parser.add_argument('--qualify-fail-after-cycles', type=int, default=0,
                        help='deliberately fail the test worker; never eligible for PASS')
    args = parser.parse_args(argv)
    policy = acceptance.contract(args.acceptance) if args.acceptance != 'qualification' else None
    options = {'mode': args.mode, 'seconds': args.seconds,
               'sample_seconds': args.sample_seconds if args.sample_seconds is not None else (15 if policy else 10),
               'fail_after_cycles': args.qualify_fail_after_cycles, 'acceptance': args.acceptance,
               'qualify_v4_observer': args.qualify_v4_observer,
               'contract_sha256': acceptance.contract_sha(policy) if policy else None}
    if policy and policy.get('observation'):
        options.update(warmup_seconds=policy['warmup_seconds'], measurement_seconds=policy['measurement_seconds'])
    try:
        validate_options(options)
        if args.qualify_v4_observer and (args.acceptance != 'qualification' or args.mode != 'ownerfs'):
            raise ValueError('v4 observer qualification is restricted to OwnerFs qualification mode')
        limits = {'memory_bytes': args.max_memory_growth_mib * 1024**2,
                  'disk_bytes': args.max_disk_growth_mib * 1024**2, 'fds': args.max_fd_growth,
                  'mounts': args.max_mount_growth, 'min_free_bytes': args.min_free_mib * 1024**2}
        if any(value < 0 for value in limits.values()) or options['fail_after_cycles'] < 0:
            raise ValueError('resource limits and failure cycle count must be nonnegative')
        if policy:
            label = 'warmup-hour' if args.acceptance == 'warmup-hour' else 'one-hour'
            if policy.get('modes') and args.mode not in policy['modes']:
                raise ValueError('mode has not qualified the selected warmup contract')
            expected = {key: policy['peak_growth'][key] for key in limits if key != 'min_free_bytes'}
            expected['min_free_bytes'] = policy['min_free_bytes']
            if (options['seconds'] != policy['seconds']
                    or options['sample_seconds'] != policy['sample_seconds']
                    or options['fail_after_cycles'] != 0):
                raise ValueError(label + ' acceptance requires the exact frozen duration, sampling and resource bounds')
            if (limits != expected):
                raise ValueError(label + ' acceptance requires the exact frozen duration, sampling and resource bounds')
    except ValueError as exc:
        parser.error(str(exc))

    class ContinuousRun(docker_run.DockerRun):
        def scenarios(self, checks, required=None):
            return scenarios(self, checks, options, limits, policy)

    forwarded = [value for flag in ('bundle', 'registry-images', 'image-export', 'artifact-commit', 'output')
                 for value in ('--' + flag, getattr(args, flag.replace('-', '_')))]
    forwarded += ['--profile', 'standalone', '--case', 'afs-sdk-soak', '--cgroupns', 'host']
    return docker_run.common.main(forwarded, native_factory=ContinuousRun, required_checks=(CHECK,))


if __name__ == '__main__':
    raise SystemExit(main())
