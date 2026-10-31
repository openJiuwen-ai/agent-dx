"""AFS SDK fixtures and required gate contracts, without product execution."""

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
import sys
from types import ModuleType, SimpleNamespace

HERE = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ProductContracts(unittest.TestCase):
    def test_sdk_topology_keeps_public_nodes_and_adds_only_owned_fuse_and_meta(self):
        product = load('afs_product_test', HERE / 'kubernetes/afs_product.py')
        manifest = load('afs_manifest_test', HERE / 'kubernetes/manifest.py')
        image = 'registry/node@sha256:' + 'a' * 64
        base = manifest.resources('adx-e2e-fixture', image, 'amd64')
        objects = product.resources(base, 'adx-e2e-fixture', image, 'amd64')
        pods = [p for p in objects if p['kind'] == 'Pod']
        self.assertEqual([p['metadata']['name'] for p in pods], ['node1', 'node2', 'afs-m'])
        for pod in pods:
            self.assertFalse(pod['spec']['automountServiceAccountToken'])
            hosts = [v['hostPath'] for v in pod['spec']['volumes'] if 'hostPath' in v]
            self.assertEqual(hosts, [{'path': '/dev/fuse', 'type': 'CharDevice'}])
            platform = pod['spec']['containers'][0]
            self.assertIn({'name': 'state', 'mountPath': '/a'}, platform['volumeMounts'])
        self.assertFalse(any(v.get('hostPath') for p in base if p['kind'] == 'Pod'
                             for v in p['spec']['volumes']))

    def test_sdk_fixture_separates_privileged_file_api_from_unprivileged_commands(self):
        spec = json.loads((HERE / 'afs_runc_spec.json').read_text())
        expected = {'CAP_AUDIT_WRITE', 'CAP_KILL', 'CAP_NET_BIND_SERVICE', 'CAP_SETUID',
                    'CAP_SETGID', 'CAP_DAC_OVERRIDE'}
        for capabilities in spec['process']['capabilities'].values():
            self.assertEqual(set(capabilities), expected)
        self.assertFalse(any(ns['type'] == 'user' for ns in spec['linux']['namespaces']))

    def test_sdk_runtime_configuration_changes_only_existing_basic_spec(self):
        product = load('afs_runtime_config_test', HERE / 'kubernetes/afs_product.py')
        run = Mock()
        product.Product(run).configure_runtime('node1')
        args = run.execute.call_args.args
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'sandboxd.toml'
            path.write_text('[plugin.runtime.basic_spec]\nrunc = ""\n'
                            '[plugin.runtime.runtime_binary]\nrunc = "/usr/local/bin/runc"\n')
            subprocess.run([sys.executable, '-c', args[3], str(path), *args[5:]], check=True)
            self.assertIn('runc = "/opt/adx/e2e/afs_runc_spec.json"', path.read_text())
            self.assertIn('runc = "/usr/local/bin/runc"', path.read_text())

    def test_sdk_success_requires_exact_cases_real_identity_and_confirmed_cleanup(self):
        product = load('afs_product_result_test', HERE / 'kubernetes/afs_product.py')
        report = {'status': 'passed', 'mode': 'dfs', 'cleanup_errors': [],
                  'cases': [{'id': name, 'status': 'passed'} for name in product.SDK_CASES]}
        product.validate_sdk_result(report, 'dfs')
        for bad in (dict(report, cases=report['cases'][:-1]), dict(report, mode='ownerfs'),
                    dict(report, cleanup_errors=['environment still running'])):
            with self.assertRaises(ValueError):
                product.validate_sdk_result(bad, 'dfs')

    def test_default_afs_gate_cannot_pass_from_zero_exit_or_missing_sdk_results(self):
        gate = load('afs_gate_test', HERE / 'kubernetes/afs_gate.py')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'result.json').write_text(json.dumps({
                'status': 'passed', 'checks': [], 'required_checks': [],
                'missing_checks': [], 'cleanup_errors': [], 'cases': [],
            }))
            with self.assertRaises(ValueError):
                gate.verify_part(root, ('afs-sdk-ownerfs', 'afs-sdk-dfs'))

    def test_gate_keeps_zero_exit_missing_required_work_red(self):
        gate = load('afs_gate_main_test', HERE / 'kubernetes/afs_gate.py')
        kubernetes = load('afs_gate_kube_test', HERE / 'kubernetes/run.py')
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'gate'

            def child(command):
                part = Path(command[command.index('--output') + 1])
                part.mkdir()
                (part / 'result.json').write_text(json.dumps({'status': 'passed', 'cases': []}))
                return Mock(wait=Mock(return_value=0), poll=Mock(return_value=0))

            argv = ['gate', '--bundle', '/fixture/bundle.json', '--registry-images', '/fixture/images.json',
                    '--kubeconfig', '/fixture/kubeconfig', '--output', str(output), '--profile', 'l0']
            with patch.object(sys, 'argv', argv), patch.dict(sys.modules, {'run': kubernetes}), \
                    patch.object(kubernetes, 'source_commit', return_value='a' * 40), \
                    patch.object(gate.subprocess, 'Popen', side_effect=child):
                self.assertEqual(gate.main(), 1)
            result = json.loads((output / 'result.json').read_text())
            self.assertEqual(result['status'], 'failed')
            self.assertIn('afs-sdk', result['missing_checks'])

    def test_complete_gate_writes_success_junit_with_no_error(self):
        gate = load('afs_gate_success_test', HERE / 'kubernetes/afs_gate.py')
        kubernetes = load('afs_gate_success_kube_test', HERE / 'kubernetes/run.py')
        summary = load('afs_gate_ci_summary_test', HERE.parents[1] / '.buildkite/summary.py')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'acceptance'
            (root / 'bundle').mkdir()
            (root / 'bundle/bundle.json').write_text(json.dumps({'package': {'commit': 'b' * 40}}))
            (root / 'summaries').mkdir()
            (root / 'summaries/images.json').write_text(json.dumps({
                'commit': 'c' * 40, 'product_commit': 'b' * 40, 'stages': {},
                'images': {'references': {}, 'backend': 'pinned-backend'},
            }))
            harness = {'schema_version': 1, 'commit': kubernetes.source_commit(),
                       'product_commit': 'b' * 40, 'files': {'fixture.py': 'd' * 64}}
            argv = ['gate', '--bundle', '/fixture/bundle.json', '--registry-images', '/fixture/images.json',
                    '--kubeconfig', '/fixture/kubeconfig', '--output', str(output), '--profile', 'l0']
            child = Mock(wait=Mock(return_value=0), poll=Mock(return_value=0))
            with patch.object(sys, 'argv', argv), patch.dict(sys.modules, {'run': kubernetes}), \
                    patch.object(kubernetes, 'source_commit', return_value=harness['commit']), \
                    patch.object(gate, 'verify_part', return_value={'harness': harness}), \
                    patch.object(gate.subprocess, 'Popen', return_value=child):
                self.assertEqual(gate.main(), 0)
            result = json.loads((output / 'result.json').read_text())
            self.assertIsNone(result['error'])
            self.assertEqual(result['checks'], ['public', 'afs-component', 'afs-sdk'])
            self.assertEqual(result['missing_checks'], [])
            self.assertTrue((output / 'junit.xml').is_file())
            collected = summary.collect(root, 'e2e', 0, harness['commit'], artifact_build='fixed-images')
            self.assertEqual(collected['e2e']['report']['harness'], harness)
            self.assertIn('public, afs-component, afs-sdk', summary.render(collected))

    def test_gate_rejects_missing_or_inconsistent_child_identity(self):
        gate = load('afs_gate_identity_test', HERE / 'kubernetes/afs_gate.py')
        harness = {'schema_version': 1, 'commit': 'a' * 40, 'product_commit': 'b' * 40,
                   'files': {'fixture.py': 'd' * 64}}
        report = {}
        gate.accept_harness(report, {'harness': harness}, 'a' * 40)
        gate.accept_harness(report, {'harness': harness}, 'a' * 40)
        for bad in ({}, dict(harness, commit='c' * 40), dict(harness, product_commit='c' * 40),
                    dict(harness, files={}), dict(harness, files={'fixture.py': 'e' * 64})):
            with self.assertRaises(ValueError):
                gate.accept_harness(report, {'harness': bad}, 'a' * 40)

    def test_sdk_target_is_explicit_and_cannot_become_an_off_or_default_pass(self):
        kubernetes = load('afs_sdk_selection_test', HERE / 'kubernetes/run.py')
        self.assertEqual(kubernetes.selected_checks('full', 'afs-sdk'), ('afs-sdk-ownerfs', 'afs-sdk-dfs'))
        with self.assertRaises(ValueError):
            kubernetes.selected_checks('l0', 'afs-sdk')
        with tempfile.TemporaryDirectory() as directory:
            run = kubernetes.KubernetesRun(Path(directory), Path('/fixture/kubeconfig'),
                                           profile='full', selected_case='afs-sdk')
            with patch.object(run, 'kube') as kube:
                with self.assertRaises(ValueError):
                    run.deploy({'package': {'with_afs': False}}, {}, {})
                kube.assert_not_called()

    def test_sdk_uses_supported_creation_and_checks_workload_identity_separately(self):
        sdk = load('afs_sdk_installed_test', HERE / 'afs_sdk.py')
        records, calls = {}, []

        class Environment:
            def __init__(self, **options):
                calls.append(options)
                assert 'user' not in options and 'group' not in options
                self.id = 'fixture-' + str(len(calls))
                records['environment:' + self.id] = json.dumps({'assignment': {'node_id': options['node_id']}})
                self.commands = Mock()
                self.commands.run.side_effect = lambda cmd, **_kw: SimpleNamespace(
                    exit_code=0, stdout='0' if cmd == 'id -u' else ('SDK-WARM' if 'SDK-WARM' in cmd else '10000'),
                    stderr='',
                )

            def kill(self):
                records.pop('environment:' + self.id, None)

            def close(self):
                pass

        module = ModuleType('adx_environment')
        module.Environment, module.Mount = Environment, Mock()
        module.__file__ = '/opt/adx/client/lib/python3.12/site-packages/adx_environment/__init__.py'
        node = ModuleType('node')
        node.catalog = lambda: records
        with tempfile.TemporaryDirectory() as directory, \
                patch.dict(sys.modules, {'adx_environment': module, 'node': node}), \
                patch.object(sdk.importlib.metadata, 'version', return_value='candidate'):
            report = sdk.run(None, 'fixture/image', {'mode': 'warm'}, Path(directory) / 'result.json')
        self.assertEqual(report['status'], 'passed', report)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(p['backend_uid'] == 0 and p['workload_uid'] == 10000 for p in report['placements']))


if __name__ == '__main__':
    unittest.main()
