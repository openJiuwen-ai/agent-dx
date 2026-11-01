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
    if (report.get('status') != 'passed' or report.get('reason') != 'duration-complete'
            or report.get('injected_failure_after_cycles', 0) != 0
            or report.get('requested_seconds') != seconds
            or not isinstance(elapsed, (int, float)) or isinstance(elapsed, bool)
            or not math.isfinite(elapsed) or elapsed < seconds
            or report.get('cycles', 0) < 2 or report.get('errors') != [] or report.get('cleanup_errors') != []
            or report.get('remaining') != [] or set(report.get('instances', {})) != {'A', 'B'}
            or len(set(report['instances'].values())) != 2
            or any(not isinstance(counts.get(n), int) or isinstance(counts[n], bool) for n in
                   ('command', 'file', 'create', 'delete'))
            or any(counts.get(n, 0) < 2 for n in ('command', 'file'))
            or counts.get('create', 0) < 3 or counts.get('create') != counts.get('delete')):
        raise ValueError('continuous workload duration/work/counts/cleanup incomplete')


def validate_samples(samples, limits, interval, *, baseline=None, require_progress=True):
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
        if now - last_progress > max(30, interval * 3):
            raise ValueError('workload heartbeat stalled')
        for node, observed in sample['nodes'].items():
            initial = (baseline or samples[0])['nodes'][node]
            for field in ('memory_bytes', 'disk_bytes', 'fds', 'mounts', 'free_bytes'):
                value = observed.get(field)
                if (not isinstance(value, (int, float)) or isinstance(value, bool)
                        or not math.isfinite(value) or value < 0):
                    raise ValueError('invalid resource counter: ' + field)
                if field != 'free_bytes' and value - initial[field] > limits[field]:
                    raise ValueError(node + ' resource growth exceeded: ' + field)
            if observed['free_bytes'] < limits['min_free_bytes']:
                raise ValueError(node + ' free disk below frozen reserve')
            if not observed['processes']:
                raise ValueError(node + ' has no monitored persistent processes')
            for name, process in initial['processes'].items():
                current = observed['processes'].get(name, {})
                if any(current.get(k) != process[k] for k in ('pid', 'start_ticks')):
                    raise ValueError(node + ' persistent process identity changed: ' + name)


PROBE = '''from pathlib import Path
import json, os, shutil, subprocess
processes={};fds=0
for path in Path('/proc').iterdir():
 if not path.name.isdigit(): continue
 try:
  command=(path/'comm').read_text().strip()
  names=('afs-meta','afs-node','adxlet','adx-coordinator','adx-apiserver','adx-ingress','sandboxd','envd')
  if command not in names: continue
  status=(path/'status').read_text().splitlines()
  rss=next((int(s.split()[1])*1024 for s in status if s.startswith('VmRSS:')),0)
  count=len(list((path/'fd').iterdir()));fds+=count
  processes[command+':'+path.name]={'pid':int(path.name),'start_ticks':int((path/'stat').read_text().rsplit(')',1)[1].split()[19]),'rss_bytes':rss,'fds':count}
 except FileNotFoundError: continue
print(json.dumps({'processes':processes,'fds':fds,'mounts':len(Path('/proc/self/mountinfo').read_text().splitlines()),'disk_bytes':int(subprocess.check_output(['du','-sb','/a'],text=True).split()[0]),'free_bytes':shutil.disk_usage('/a').free}))
'''


def observe_resources(run, progress):
    nodes = {}
    for node in NODES:
        observed = json.loads(run.execute(node, 'python3', '-c', PROBE, timeout=30))
        item = json.loads(run.docker('inspect', run.owned[node], stream=False))[0]
        pid = item['State']['Pid']
        group = next(line.split(':', 2)[2] for line in Path(f'/proc/{pid}/cgroup').read_text().splitlines()
                     if line.startswith('0::'))
        observed['memory_bytes'] = int((Path('/sys/fs/cgroup') / group.lstrip('/') / 'memory.current').read_text())
        observed['container_id'] = item['Id']
        nodes[node] = observed
    return {'elapsed_seconds': progress['elapsed_seconds'], 'observed_monotonic_seconds': time.monotonic(),
            'cycles': progress['cycles'], 'nodes': nodes}


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


def scenarios(run, checks, options, limits):
    product = run.afs_product
    product.sdk({'mode': 'warm'}, run.output / 'afs-sdk-warm.json')
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'afs/tests'))
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
                      'qualification_only': True, 'cleanup_errors': [], 'mode': mode}
            try:
                cluster.prepare()
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
                    'v={"pid":p.pid,"start_ticks":int(Path(f"/proc/{p.pid}/stat").read_text().rsplit(")",1)[1].split()[19])};'
                    '(d/"worker.json").write_text(json.dumps(v));print(json.dumps(v))')
                receipt = json.loads(raw)
                # Preparation and bounded final SDK call have separate reserves, never counted as load duration.
                deadline = time.monotonic() + options['seconds'] + 600
                next_sample = 0
                last_cycles, last_progress_at = 0, time.monotonic()
                while not (directory / 'ended.json').exists():
                    if time.monotonic() >= deadline:
                        raise subprocess.TimeoutExpired('continuous SDK workload', options['seconds'] + 600)
                    if (directory / 'progress.json').exists() and time.monotonic() >= next_sample:
                        progress = json.loads((directory / 'progress.json').read_text())
                        if progress['cycles'] > 0:
                            if progress['cycles'] > last_cycles:
                                last_cycles, last_progress_at = progress['cycles'], time.monotonic()
                            elif time.monotonic() - last_progress_at > max(30, options['sample_seconds'] * 3):
                                raise ValueError('workload heartbeat stalled')
                            observed = observe_resources(run, progress)
                            samples.append(observed)
                            with (run.output / 'soak-samples.jsonl').open('a') as stream:
                                stream.write(json.dumps(observed) + '\n')
                            if len(samples) > 1:
                                validate_samples(samples[-2:], limits, options['sample_seconds'],
                                                 baseline=samples[0], require_progress=False)
                            run.event(f'SOAK {mode} elapsed={progress["elapsed_seconds"]:.1f}s '
                                      f'cycles={progress["cycles"]}')
                            next_sample = time.monotonic() + options['sample_seconds']
                    time.sleep(0.25)
                workload = json.loads((directory / 'workload.json').read_text())
                report['workload'] = workload
                final_sample = observe_resources(run, workload)
                samples.append(final_sample)
                with (run.output / 'soak-samples.jsonl').open('a') as stream:
                    stream.write(json.dumps(final_sample) + '\n')
                if workload.get('owner') != run.id or workload.get('mode') != mode:
                    raise ValueError('workload result identity mismatch')
                validate_workload(workload, options['seconds'])
                validate_samples(samples, limits, options['sample_seconds'])
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
                (run.output / 'soak-result.json').write_text(json.dumps(report, indent=2) + '\n')
                record['subcases'] = [{'id': n, 'status': report['status']} for n in (
                    'continuous-confirmed-content', 'command-file-and-churn', 'duration',
                    'resource-sampling-and-bounds', 'stable-identities', 'normal-cleanup')]
                if report['status'] != 'passed' and 'error' not in report:
                    raise ValueError('continuous soak cleanup failed')
    finally:
        sys.path.pop(0)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('bundle', 'registry-images', 'image-export', 'artifact-commit', 'output'):
        parser.add_argument('--' + flag, required=True)
    parser.add_argument('--mode', choices=('ownerfs', 'dfs'), required=True)
    parser.add_argument('--seconds', type=int, required=True)
    parser.add_argument('--sample-seconds', type=int, default=10)
    parser.add_argument('--max-memory-growth-mib', type=int, default=512)
    parser.add_argument('--max-disk-growth-mib', type=int, default=256)
    parser.add_argument('--max-fd-growth', type=int, default=128)
    parser.add_argument('--max-mount-growth', type=int, default=8)
    parser.add_argument('--min-free-mib', type=int, default=2048)
    parser.add_argument('--qualify-fail-after-cycles', type=int, default=0,
                        help='deliberately fail the test worker; never eligible for PASS')
    args = parser.parse_args(argv)
    options = {'mode': args.mode, 'seconds': args.seconds, 'sample_seconds': args.sample_seconds,
               'fail_after_cycles': args.qualify_fail_after_cycles}
    try:
        validate_options(options)
        limits = {'memory_bytes': args.max_memory_growth_mib * 1024**2,
                  'disk_bytes': args.max_disk_growth_mib * 1024**2, 'fds': args.max_fd_growth,
                  'mounts': args.max_mount_growth, 'min_free_bytes': args.min_free_mib * 1024**2}
        if any(value < 0 for value in limits.values()) or options['fail_after_cycles'] < 0:
            raise ValueError('resource limits and failure cycle count must be nonnegative')
    except ValueError as exc:
        parser.error(str(exc))

    class ContinuousRun(docker_run.DockerRun):
        def scenarios(self, checks, required=None):
            return scenarios(self, checks, options, limits)

    forwarded = [value for flag in ('bundle', 'registry-images', 'image-export', 'artifact-commit', 'output')
                 for value in ('--' + flag, getattr(args, flag.replace('-', '_')))]
    return docker_run.common.main([*forwarded, '--profile', 'standalone', '--case', 'afs-sdk-soak', '--cgroupns', 'host'],
                                  native_factory=ContinuousRun, required_checks=(CHECK,))


if __name__ == '__main__':
    raise SystemExit(main())
