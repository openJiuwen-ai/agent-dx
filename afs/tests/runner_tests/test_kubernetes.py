"""Kubernetes transport identities; tooling tests never enter a real cluster."""

import importlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

TESTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TESTS))


class KubernetesTransportTests(unittest.TestCase):
    def setUp(self):
        self.module = importlib.import_module('fixtures.kubernetes')
        self.config = {
            'transport': 'kubernetes', 'base': '/a',
            'kubectl': ['/fixture/kubectl', '--kubeconfig', '/fixture/kubeconfig'],
            'namespace': 'adx-e2e-fixture', 'namespace_uid': 'namespace-identity',
            'pod': 'afs-a', 'pod_uid': 'pod-identity', 'pod_ip': '10.1.0.2', 'container': 'platform',
        }
        self.namespace = {'metadata': {'uid': 'namespace-identity', 'labels': {'adx.e2e.run': 'adx-e2e-fixture'}}}
        self.pod = {
            'metadata': {'uid': 'pod-identity', 'labels': {'adx.e2e.run': 'adx-e2e-fixture'}},
            'spec': {'nodeName': 'worker-a', 'containers': [{'name': 'platform'}]},
            'status': {'phase': 'Running', 'podIP': '10.1.0.2'},
        }

    def connect(self):
        with patch.object(self.module.subprocess, 'check_output', side_effect=[
            json.dumps(self.namespace), json.dumps(self.pod),
        ]), patch.object(self.module, 'Channel', return_value=Mock()) as factory:
            channel = self.module.connect(self.config)
            return channel, factory

    def test_immutable_namespace_and_pod_identity_are_checked_before_worker_start(self):
        channel, factory = self.connect()
        self.assertEqual(channel.kubernetes_identity['pod_uid'], 'pod-identity')
        self.assertEqual(channel.kubernetes_identity['node'], 'worker-a')
        command = factory.call_args.args[0]
        self.assertIn('-i', command)
        self.assertIn('unshare', command)
        self.assertNotIn('-t', command)
        self.assertIn('HTTP_PROXY', command)

    def test_replaced_pod_changed_ip_or_namespace_cannot_be_used(self):
        from fixtures.common import Blocked
        for key, value in (('pod_uid', 'replacement'), ('pod_ip', '10.1.0.9'), ('namespace_uid', 'replacement')):
            with self.subTest(key=key):
                self.config[key] = value
                with self.assertRaises(Blocked):
                    self.connect()
                self.setUp()

    def test_sdk_fixture_can_share_only_its_frozen_container_mount_namespace(self):
        self.config['mount_namespace'] = 'container'
        _, factory = self.connect()
        self.assertNotIn('unshare', factory.call_args.args[0])
        self.config['mount_namespace'] = 'host'
        with self.assertRaises(ValueError):
            self.connect()

    def test_sdk_public_node_requires_explicit_shared_namespace_and_data_label(self):
        from fixtures.common import Blocked
        self.config['pod'] = 'node1'
        with self.assertRaises(ValueError):
            self.connect()
        self.config['mount_namespace'] = 'container'
        with self.assertRaises(Blocked):
            self.connect()
        self.pod['metadata']['labels']['afs.e2e.data'] = 'true'
        _, factory = self.connect()
        self.assertNotIn('unshare', factory.call_args.args[0])
        self.config['pod_uid'] = 'replaced'
        with self.assertRaises(Blocked):
            self.connect()

    def test_kubernetes_topology_accepts_two_kernels_without_claiming_distinct_vms(self):
        from fixtures.common import Case
        from fixtures.multivm import Cluster, validate
        config = {
            'kind': 'kubernetes', 'uid': 10000, 'gid': 10000, 'other_uid': 10001, 'other_gid': 10001,
            'roles': {r: {'host': r.lower(), 'address': '10.1.0.' + str(i)} for i, r in enumerate('MAB', 1)},
            'hosts': {r.lower(): dict(self.config, pod='afs-' + r.lower(), pod_uid=r) for r in 'MAB'},
        }
        case = Case('ownerfs.cluster.read_write', ('cluster-basic',), 'ownerfs', (), None)
        self.assertEqual(validate(config, [case]), ['a', 'b', 'm'])
        channels = []
        for i, role in enumerate(('a', 'b', 'm')):
            channel = Mock()
            channel.kubernetes_identity = {'pod_uid': role, 'namespace_uid': 'namespace-identity'}
            channel.call.return_value = {
                'system': 'Linux', 'uid': 0, 'fuse': True, 'filesystem': 'ext4', 'free': 2 * 1024**3,
                'tools': {'unshare': '/bin/unshare'}, 'boot_id': 'kernel-' + str(i % 2),
                'pod_uid': role.upper(),
                'network_namespace': 'net-' + role, 'pid_namespace': 'pid-' + role, 'mount_namespace': 'mnt-' + role,
            }
            channels.append(channel)
        import tempfile
        with tempfile.TemporaryDirectory() as directory, patch.object(
            self.module, 'connect', side_effect=channels,
        ):
            cluster = Cluster(config, '/inputs', directory, case, 'test')
            info = cluster.connect()
            self.assertEqual(info['a']['kubernetes']['pod_uid'], 'a')
        channels[1].kubernetes_identity['pod_uid'] = 'a'
        with tempfile.TemporaryDirectory() as directory, patch.object(
            self.module, 'connect', side_effect=channels,
        ):
            from fixtures.common import Blocked
            with self.assertRaises(Blocked):
                Cluster(config, '/inputs', directory, case, 'test').connect()

    def test_worker_rejects_pod_replaced_between_metadata_read_and_exec(self):
        # The worker-side Downward API identity must agree with the frozen Pod UID.
        from fixtures.common import Case, Blocked
        from fixtures.multivm import Cluster
        import tempfile
        config = {
            'kind': 'kubernetes', 'uid': 10000, 'gid': 10000,
            'roles': {r: {'host': r.lower(), 'address': '10.1.0.' + str(i)} for i, r in enumerate('MAB', 1)},
            'hosts': {r.lower(): dict(self.config, pod='afs-' + r.lower(), pod_uid=r) for r in 'MAB'},
        }
        channels = []
        for role in ('a', 'b', 'm'):
            channel = Mock(kubernetes_identity={'pod_uid': role, 'namespace_uid': 'namespace-identity'})
            channel.call.return_value = {
                'system': 'Linux', 'uid': 0, 'fuse': True, 'filesystem': 'ext4', 'free': 2 * 1024**3,
                'tools': {'unshare': '/bin/unshare'}, 'boot_id': 'kernel',
                'pod_uid': 'new-pod' if role == 'a' else role.upper(),
                'network_namespace': 'net-' + role, 'pid_namespace': 'pid-' + role, 'mount_namespace': 'mnt-' + role,
            }
            channels.append(channel)
        with tempfile.TemporaryDirectory() as directory, patch.object(self.module, 'connect', side_effect=channels):
            with self.assertRaises(Blocked):
                Cluster(config, '/inputs', directory, Case('probe', (), 'ownerfs', (), None), 'test').connect()


class ControllerCompatibilityTests(unittest.TestCase):
    def test_path_ownership_does_not_require_python39_path_api(self):
        from fixtures.worker import Worker
        import tempfile
        worker = object.__new__(Worker)
        with tempfile.TemporaryDirectory() as directory:
            worker.root = Path(directory) / 'owned'
            worker.root.mkdir()
            outside = Path(directory) / 'outside'
            outside.mkdir()
            (worker.root / 'escape').symlink_to(outside, target_is_directory=True)
            with patch.object(Path, 'is_relative_to', create=True, side_effect=AttributeError('Python 3.8')):
                self.assertEqual(worker.owned(str(worker.root / 'ok')), worker.root / 'ok')
                for value in (outside, worker.root / '..' / 'outside', worker.root / 'escape' / 'file'):
                    with self.assertRaises(ValueError):
                        worker.owned(str(value))


if __name__ == '__main__':
    unittest.main()
