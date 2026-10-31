"""AFS K8s component orchestration contracts, without deployment or mounting."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

HERE = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AfsKubernetesTests(unittest.TestCase):
    def setUp(self):
        self.module = load('afs_k8s_contract', HERE / 'kubernetes/afs_component.py')

    def test_component_resources_are_three_isolated_roles_without_host_data_or_credentials(self):
        resources = self.module.resources('adx-e2e-fixture', 'registry/node@sha256:' + 'a' * 64, 'amd64')
        self.assertEqual([p['metadata']['name'] for p in resources], ['afs-m', 'afs-a', 'afs-b'])
        for pod in resources:
            spec = pod['spec']
            self.assertFalse(spec.get('hostNetwork', False))
            self.assertFalse(spec.get('hostPID', False))
            self.assertFalse(spec['automountServiceAccountToken'])
            self.assertEqual(len(spec['containers']), 1)
            self.assertTrue(spec['containers'][0]['securityContext']['privileged'])
            host_paths = [v['hostPath'] for v in spec['volumes'] if 'hostPath' in v]
            self.assertEqual(host_paths, [{'path': '/dev/fuse', 'type': 'CharDevice'}])
            self.assertFalse(any('secret' in v for v in spec['volumes']))

    def test_off_candidate_stops_before_creating_cluster_resources(self):
        run = Mock()
        component = self.module.Component(run)
        with self.assertRaisesRegex(ValueError, 'AFS ON'):
            component.deploy({'package': {'with_afs': False}}, {}, None, (), False, 'b' * 40)
        run.kube.assert_not_called()
        run.apply.assert_not_called()

    def test_deployment_builds_environment_from_actual_pod_uids_and_addresses(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Mock(id='adx-e2e-fixture', output=Path(directory), nodes=[], kubectl=['/fixture/kubectl'])
            pods = [{'metadata': {'name': 'afs-' + role.lower(), 'uid': role},
                     'spec': {'nodeName': 'worker-' + role}, 'status': {'podIP': '10.1.0.' + str(i)}}
                    for i, role in enumerate('MAB', 1)]

            def kube(*args, **_kwargs):
                if args[:2] == ('get', 'namespace'):
                    return json.dumps({'metadata': {'uid': 'namespace-uid'}})
                if 'pods' in args:
                    return json.dumps({'items': pods})
                return '{}'

            run.kube.side_effect = kube
            package = {'with_afs': True, 'commit': 'a' * 40}
            component = self.module.Component(run)
            component.deploy({'package': package, 'architecture': 'amd64'},
                             {'node': 'registry/node@sha256:' + 'a' * 64}, None, (), True, 'b' * 40)
            config = json.loads((Path(directory) / 'afs-environment.json').read_text())
            self.assertEqual(config['kind'], 'kubernetes')
            self.assertEqual(config['hosts']['a']['pod_uid'], 'A')
            self.assertEqual(config['roles']['B']['address'], '10.1.0.3')
            run.sync_harness.assert_called_once_with('b' * 40, 'a' * 40, package)
            self.assertEqual(len([call for call in run.apply.call_args_list if call.args[0]['kind'] == 'Pod']), 3)

    def test_pod_deletion_cannot_hide_missing_normal_afs_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Mock(id='adx-e2e-fixture', output=Path(directory), namespace_attempted=True)
            run.kube.return_value = ''
            component = self.module.Component(run)
            component.attempted_cases = True
            self.assertTrue(component.cleanup())
            report = json.loads((Path(directory) / 'kubernetes-cleanup.json').read_text())
            self.assertEqual(report['status'], 'FAIL')

    def test_only_the_five_declared_cases_and_complete_cleanup_count_as_pass(self):
        records = [{'id': name, 'status': 'PASS'} for name in self.module.CASES]
        value = {'status': 'PASS', 'selected_cases': list(self.module.CASES), 'cases': records, 'cleanup_errors': []}
        self.module.validate_result(value, {'status': 'PASS'})
        for bad in (
            dict(value, cases=records[:-1]), dict(value, cleanup_errors=['unmount failed']),
            dict(value, cases=records + [records[0]]), dict(value, selected_cases=['other']),
        ):
            with self.assertRaises(ValueError):
                self.module.validate_result(bad, {'status': 'PASS'})
        with self.assertRaises(ValueError):
            self.module.validate_result(value, {'status': 'FAIL'})

    def test_namespace_replacement_prevents_destructive_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Mock(id='adx-e2e-fixture', output=Path(directory), namespace_uid='original', namespace_attempted=True)
            run.kube.return_value = json.dumps({'metadata': {
                'uid': 'different', 'labels': {'adx.e2e.run': 'adx-e2e-fixture'},
            }})
            errors = self.module.Component(run).cleanup()
            self.assertTrue(errors)
            self.assertFalse(any(call.args[0] == 'delete' for call in run.kube.call_args_list))

    def test_target_is_explicit_and_does_not_expand_default_smoke_or_full(self):
        runner = load('afs_selected_check', HERE / 'kubernetes/run.py')
        self.assertEqual(runner.selected_checks('full', 'afs-component'), ('afs-component',))
        self.assertNotIn('afs-component', runner.selected_checks('full', None))
        self.assertNotIn('afs-component', runner.selected_checks('l0', None))


if __name__ == '__main__':
    unittest.main()
