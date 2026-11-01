"""Deploy AFS beside the existing ADX fixture and exercise the installed SDK."""

from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
from afs_component import resources as component_resources
SDK_FULL_EXTRA = ('namespace-identity', 'truncate-visibility')
SDK_CASES = ('mount-identity', 'bidirectional-publish', 'permissions', 'readonly', 'remove', 'environment-cleanup')


def resources(objects, namespace, image, architecture, registry_auth=False, node_names=()):
    objects = deepcopy(objects)
    for pod in (p for p in objects if p['kind'] == 'Pod' and p['metadata']['name'] in ('node1', 'node2')):
        pod['metadata']['labels']['afs.e2e.data'] = 'true'
        spec = pod['spec']
        spec['volumes'].append({'name': 'fuse', 'hostPath': {'path': '/dev/fuse', 'type': 'CharDevice'}})
        container = spec['containers'][0]
        container['volumeMounts'] += [{'name': 'state', 'mountPath': '/a'},
                                     {'name': 'fuse', 'mountPath': '/dev/fuse'}]
        container['env'] += [{'name': 'AFS_TEST_POD_UID', 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.uid'}}},
                             {'name': 'PYTHONDONTWRITEBYTECODE', 'value': '1'}]
        spec['affinity']['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution'] = [{
            'labelSelector': {'matchLabels': {'adx.e2e.run': namespace, 'afs.e2e.data': 'true'}},
            'topologyKey': 'kubernetes.io/hostname',
        }]
    objects.append(component_resources(namespace, image, architecture, registry_auth, node_names)[0])
    return objects


def validate_sdk_result(report, mode, full=False):
    expected = SDK_CASES + SDK_FULL_EXTRA if full else SDK_CASES
    cases = report.get('cases', [])
    if (report.get('status') != 'passed' or report.get('mode') != mode or report.get('cleanup_errors')
            or len(cases) != len(expected) or {c.get('id') for c in cases} != set(expected)
            or any(c.get('status') != 'passed' for c in cases)):
        raise ValueError('SDK AFS assertions or Environment cleanup did not pass completely')
    if full and report.get('full') is not True:
        raise ValueError('SDK Full evidence is required')


class Product:
    def __init__(self, run, full=False):
        self.run = run
        self.full = full
        self.config = None
        self.active = None
        self.cleanup_errors = []

    def configure(self, package, pods, harness_commit):
        run = self.run
        if not package.get('with_afs'):
            raise ValueError('SDK-on-AFS requires an AFS ON candidate')
        run.sync_harness(harness_commit, package['commit'], package, nodes=['afs-m'])
        run.execute('afs-m', 'python3', '-B', '-m', 'unittest', 'discover',
                    '-s', '/opt/adx/e2e/afs-tests/runner_tests', '-v', timeout=180)
        self.config = {'kind': 'kubernetes', 'uid': 10000, 'gid': 10000, 'other_uid': 10001, 'other_gid': 10001,
                       'roles': {}, 'hosts': {}}
        for role, name in (('M', 'afs-m'), ('A', 'node1'), ('B', 'node2')):
            pod = next(p for p in pods if p['metadata']['name'] == name)
            ip = pod['status']['podIP']
            self.config['roles'][role] = {'host': role.lower(), 'address': ip}
            self.config['hosts'][role.lower()] = {
                'transport': 'kubernetes', 'base': '/a', 'kubectl': run.kubectl,
                'namespace': run.id, 'namespace_uid': run.namespace_uid, 'pod': name,
                'pod_uid': pod['metadata']['uid'], 'pod_ip': ip, 'container': 'platform',
                'mount_namespace': 'private' if role == 'M' else 'container',
            }
        (run.output / 'afs-sdk-environment.json').write_text(json.dumps(self.config, indent=2))

    def configure_runtime(self, node):
        # Only these disposable SDK-test Pods use identity-switch capabilities.
        # Reuse sandboxd's existing basic_spec deployment option, before it starts.
        self.run.execute(node, 'python3', '-c',
                         'from pathlib import Path;import sys;'
                         'p=Path(sys.argv[1]);s=p.read_text();old=sys.argv[2];'
                         'assert s.count(old)==1,"unexpected sandboxd config";'
                         'p.write_text(s.replace(old,sys.argv[3]));',
                         '/tmp/adx-e2e/sandboxd/config.toml',
                         '[plugin.runtime.basic_spec]\nrunc = ""',
                         '[plugin.runtime.basic_spec]\nrunc = "/opt/adx/e2e/afs_runc_spec.json"')

    def sdk(self, config, destination):
        run = self.run
        run.execute('node1', 'python3', '-c',
                    'import pathlib,sys;pathlib.Path("/evidence/afs-sdk-input.json").write_text(sys.argv[1])',
                    json.dumps(config))
        remote = '/evidence/afs-sdk-' + config['mode'] + '.json'
        run.execute('node1', '/opt/adx/client/bin/python', '-B', '/opt/adx/e2e/afs_sdk.py',
                    '--input', '/evidence/afs-sdk-input.json', '--output', remote, timeout=600)
        report = json.loads(run.execute('node1', 'cat', remote))
        destination.write_text(json.dumps(report, indent=2))
        for node in run.nodes:
            run.helper(node, 'empty', node)
        return report

    def scenarios(self, checks):
        run = self.run
        # Populate the runtime image cache before the AFS worker takes its mount baseline.
        self.sdk({'mode': 'warm'}, run.output / 'afs-sdk-warm.json')
        path = str(ROOT / 'afs/tests')
        sys.path.insert(0, path)
        try:
            from fixtures.common import Case
            from fixtures.multivm import Cluster
            from suites.operations import peer_counter

            with tempfile.TemporaryDirectory(prefix='adx-sdk-afs-') as temp:
                inputs = Path(temp)
                for name in ('afs-meta', 'afs-node', 'artifact-identity.json'):
                    source = '/opt/adx/e2e/afs-tests/inputs/' if name.endswith('.json') else '/opt/adx/package/bin/'
                    run.kube('-n', run.id, 'cp', 'node1:' + source + name,
                             str(inputs / name), '-c', 'platform', timeout=120)
                    if name != 'artifact-identity.json':
                        (inputs / name).chmod(0o755)
                for mode in ('ownerfs', 'dfs'):
                    output = run.output / 'afs-sdk' / mode
                    output.mkdir(parents=True)
                    case = Case('afs.sdk.' + mode, ('cluster-sdk',), mode, (), None)
                    cluster = Cluster(self.config, inputs, output, case, 'afs-test-' + uuid.uuid4().hex)
                    self.active = cluster
                    with run.case('afs-sdk-' + mode + ('-full' if self.full else ''), checks) as record:
                        try:
                            cluster.prepare()
                            identities = {}
                            for role in ('A', 'B'):
                                visible = cluster.python(role,
                                    'from pathlib import Path\npid=int(Path("/tmp/adx-e2e/sandboxd.pid").read_text());'
                                    'assert os.readlink("/proc/self/ns/mnt")==os.readlink(f"/proc/{pid}/ns/mnt");'
                                    's=os.stat(sys.argv[1]);print(json.dumps([s.st_dev,s.st_ino]))',
                                    cluster.workspace(role), owner=False)
                                identities[role] = json.loads(visible['stdout'])
                            cluster.record('sandboxd.mount_namespace_visible', identities)
                            before = peer_counter(cluster, 'B') if mode == 'dfs' else None
                            report = self.sdk({'mode': mode, 'full': self.full,
                                               'paths': {r: cluster.workspace(r) for r in ('A', 'B')},
                                               'identities': identities}, output / 'sdk-result.json')
                            validate_sdk_result(report, mode, full=self.full)
                            if mode == 'dfs':
                                after = peer_counter(cluster, 'B')
                                assert after > before, (before, after)
                                cluster.record('sdk.peer_reads', {'before': before, 'after': after})
                            record['subcases'] = report['cases']
                        except Exception:
                            # Retire runtimes before attempting normal AFS unmount after a failed SDK operation.
                            for node in reversed(run.nodes):
                                try:
                                    run.helper(node, 'cleanup', node, timeout=180)
                                except Exception as exc:
                                    self.cleanup_errors.append('runtime cleanup: ' + str(exc))
                            raise
                        finally:
                            cleanup = cluster.cleanup()
                            self.active = None
                            if cleanup['status'] != 'PASS':
                                self.cleanup_errors += cleanup['errors'] or ['AFS cleanup unconfirmed']
                            if self.cleanup_errors:
                                raise ValueError('; '.join(self.cleanup_errors))
        finally:
            sys.path.remove(path)

    def cleanup(self):
        if self.active is not None:
            result = self.active.cleanup()
            self.cleanup_errors += result['errors']
            self.active = None
        (self.run.output / 'afs-sdk-cleanup.json').write_text(json.dumps({
            'status': 'FAIL' if self.cleanup_errors else 'PASS', 'errors': self.cleanup_errors,
        }, indent=2))
        return list(self.cleanup_errors)
