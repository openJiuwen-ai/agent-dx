"""Fixed-package admission must finish before any cluster resources are created."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('fixed_candidate_run', ROOT / 'kubernetes/run.py')
RUN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUN)
COMMIT = 'a' * 40


class FixedCandidateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bundle = {
            'schema_version': 1, 'architecture': 'amd64',
            'image_ids': {name: name + '-image' for name in ('node', 'execd', 'entrypoint')},
            'package': {'target': 'x86_64-unknown-linux-gnu', 'commit': COMMIT, 'dirty': False},
            'sdk': {'schema_version': 1, 'commit': COMMIT,
                    'files': {'adx_environment-fixture-py3-none-any.whl': 'f' * 64}},
            'backend': {'target': 'x86_64-unknown-linux-gnu',
                        'sandboxd_revision': json.loads((ROOT.parents[1] / 'third_party/sandboxd/source.json')
                                                       .read_text())['revision'],
                        'files': {'sandboxd': 'f' * 64, 'runc': 'e' * 64}},
        }
        self.bundle_path = self.root / 'bundle.json'
        self.registry_path = self.root / 'registry-images.json'
        self.write_inputs()

    def write_inputs(self):
        self.bundle_path.write_text(json.dumps(self.bundle))
        self.registry_path.write_text(json.dumps({
            'schema_version': 1, 'bundle_sha256': RUN.common.sha(self.bundle_path),
            'image_ids': self.bundle['image_ids'],
            'references': {name: 'registry.example/' + name + '@sha256:' + 'b' * 64
                           for name in self.bundle['image_ids']},
        }))

    def test_fixed_commit_is_checked_outside_buildkite(self):
        loaded, _ = RUN.fixed_candidate(self.bundle_path, self.registry_path, COMMIT)
        self.assertEqual(loaded, self.bundle)
        with self.assertRaisesRegex(ValueError, 'fixed candidate'):
            RUN.fixed_candidate(self.bundle_path, self.registry_path, 'c' * 40)

    def test_dirty_and_missing_dirty_marker_are_rejected(self):
        for value in (True, None):
            self.bundle['package']['dirty'] = value
            self.write_inputs()
            with self.assertRaisesRegex(ValueError, 'fixed candidate'):
                RUN.fixed_candidate(self.bundle_path, self.registry_path, COMMIT)

    def test_commit_must_be_a_full_identity(self):
        with self.assertRaisesRegex(ValueError, '40-character'):
            RUN.fixed_candidate(self.bundle_path, self.registry_path, 'master')

    def test_registry_cannot_bind_a_tampered_bundle(self):
        self.bundle['package']['commit'] = 'c' * 40
        self.bundle_path.write_text(json.dumps(self.bundle))
        with self.assertRaisesRegex(ValueError, 'do not match'):
            RUN.fixed_candidate(self.bundle_path, self.registry_path, COMMIT)

    def test_wrong_architecture_and_afs_off_stop_at_admission(self):
        with self.assertRaisesRegex(ValueError, 'AFS candidate mode'):
            RUN.fixed_candidate(self.bundle_path, self.registry_path, COMMIT, afs_required=True)
        self.bundle['architecture'] = 'arm64'
        self.write_inputs()
        with self.assertRaisesRegex(ValueError, 'architecture'):
            RUN.fixed_candidate(self.bundle_path, self.registry_path, COMMIT)

    def test_missing_afs_sidecar_is_rejected(self):
        self.bundle['package']['with_afs'] = True
        self.write_inputs()
        with self.assertRaisesRegex(ValueError, 'handoff'):
            RUN.fixed_candidate(self.bundle_path, self.registry_path, COMMIT, afs_required=True)

    def test_missing_sdk_or_different_backend_is_rejected(self):
        original = self.bundle['sdk']
        self.bundle['sdk'] = {}
        self.write_inputs()
        with self.assertRaisesRegex(ValueError, 'SDK'):
            RUN.fixed_candidate(self.bundle_path, self.registry_path, COMMIT)
        self.bundle['sdk'] = original
        self.bundle['backend']['target'] = 'aarch64-unknown-linux-gnu'
        self.write_inputs()
        with self.assertRaisesRegex(ValueError, 'backend'):
            RUN.fixed_candidate(self.bundle_path, self.registry_path, COMMIT)

    def afs_inputs(self):
        source = self.root / 'test-source'
        for name in ('run.py', 'fixtures/common.py', 'fixtures/worker.py', 'suites/basic.py'):
            path = source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('# test fixture\n')
        self.bundle['package'].update(with_afs=True, files={'bin/afs-meta': 'd' * 64, 'bin/afs-node': 'e' * 64})
        assets = self.root / 'assets'
        RUN.common.afs_handoff.stage(source, assets, self.bundle['package'], COMMIT)
        manifest = self.root / 'afs-tests.json'
        manifest.write_bytes((assets / 'manifest.json').read_bytes())
        self.bundle['afs_tests'] = {'manifest': manifest.name, 'sha256': RUN.common.sha(manifest)}
        self.write_inputs()

    def test_valid_on_candidate_and_missing_full_assets_are_distinct(self):
        self.afs_inputs()
        loaded, _ = RUN.fixed_candidate(self.bundle_path, self.registry_path, COMMIT, afs_required=True)
        self.assertTrue(loaded['package']['with_afs'])
        with self.assertRaisesRegex(ValueError, 'frozen fuse_contract assets'):
            RUN.fixed_candidate(self.bundle_path, self.registry_path, COMMIT,
                                afs_required=True, contracts_required=True)

    def test_gate_rejects_off_candidate_before_starting_public_child(self):
        spec = importlib.util.spec_from_file_location('fixed_candidate_gate', ROOT / 'kubernetes/afs_gate.py')
        gate = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gate)
        command = ['afs_gate.py', '--bundle', str(self.bundle_path), '--registry-images', str(self.registry_path),
                   '--artifact-commit', COMMIT, '--kubeconfig', str(self.root / 'unused-kubeconfig'),
                   '--profile', 'l0', '--output', str(self.root / 'gate')]
        with patch.object(sys, 'argv', command), patch.object(gate.subprocess, 'Popen') as child:
            self.assertEqual(gate.main(), 1)
        child.assert_not_called()
        report = json.loads((self.root / 'gate/result.json').read_text())
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['checks'], [])
        self.assertEqual(report['missing_checks'], report['required_checks'])

    def test_verify_only_does_not_construct_cluster_runner(self):
        output = self.root / 'admission'
        command = ['run.py', '--bundle', str(self.bundle_path), '--registry-images', str(self.registry_path),
                   '--artifact-commit', COMMIT, '--verify-only', '--output', str(output)]
        with patch.object(sys, 'argv', command), patch.object(RUN, 'KubernetesRun') as cluster:
            self.assertEqual(RUN.main(), 0)
        cluster.assert_not_called()
        report = json.loads((output / 'inputs.json').read_text())
        self.assertEqual(report['scope'], 'fixed-candidate-inputs')
        self.assertFalse(report['deployment_performed'])
        self.assertEqual(report['product_commit'], COMMIT)
        self.assertFalse((output / 'result.json').exists())

    def test_verify_only_bad_commit_records_failure_without_deployment(self):
        output = self.root / 'wrong-candidate'
        command = ['run.py', '--bundle', str(self.bundle_path), '--registry-images', str(self.registry_path),
                   '--artifact-commit', 'c' * 40, '--verify-only', '--output', str(output)]
        with patch.object(sys, 'argv', command), patch.object(RUN, 'KubernetesRun') as cluster:
            self.assertEqual(RUN.main(), 1)
        cluster.assert_not_called()
        self.assertEqual(json.loads((output / 'inputs.json').read_text())['status'], 'failed')


if __name__ == '__main__':
    unittest.main()
