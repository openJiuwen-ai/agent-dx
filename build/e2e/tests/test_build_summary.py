import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('ci_summary', ROOT / '.buildkite/summary.py')
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)
COMMIT = 'a' * 40


def write(root, name, value):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class BuildSummaryTests(unittest.TestCase):
    def test_independent_pipeline_summaries_and_actual_placement(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write(root, 'release-manifest.json', {'target': 'linux-test', 'profile': 'release',
                                                 'files': {'bin/adx-coordinator': 'digest'}})
            write(root, 'build-manifest.json', {'commit': COMMIT, 'components': {}})
            (root / 'adx-release.tar.gz').write_bytes(b'archive fixture')
            (root / 'sdk').mkdir()
            (root / 'sdk/adx_sandbox-test.whl').write_bytes(b'wheel fixture')
            release = summary.collect(root, 'release', 0, COMMIT)
            self.assertEqual(set(release['stages']), {'release'})
            self.assertIn('artifact://out/buildkite/adx-release.tar.gz', summary.render(release))
            self.assertIn('artifact://out/buildkite/build-manifest.json', summary.render(release))
            write(root, 'bundle/bundle.json', {'base_images': {}, 'backend': {'sandboxd_revision': 'b' * 40}})
            write(root, 'bundle/registry-images.json', {'references': {'node': 'registry/node@sha256:' + 'c' * 64}})
            images = summary.collect(root, 'images', 0, COMMIT)
            self.assertEqual(set(images['stages']), {'images'})
            write(root, 'summaries/images.json', images)
            write(root, 'acceptance/result.json', {'status': 'passed', 'checks': ['sdk', 'auth', 'capacity', 'restart', 'stop'],
                                                  'cleanup_errors': [], 'missing_checks': [], 'error': None})
            write(root, 'acceptance/placement.json', [{'pod': n, 'host': 'worker-a', 'ip': '10.0.0.1'} for n in ['node1', 'node2']])
            final = summary.collect(root, 'e2e', 0, COMMIT)
            text = summary.render(final)
            self.assertEqual(set(final['stages']), {'images', 'e2e'})
            self.assertIn('registry/node@sha256:', text)
            self.assertIn('同一宿主节点', text)
            self.assertIn('auth, capacity, restart, stop', text)
            with self.assertRaisesRegex(ValueError, 'different commit'):
                summary.collect(root, 'e2e', 0, 'd' * 40)
            (root / 'acceptance/result.json').unlink()
            with self.assertRaisesRegex(ValueError, 'evidence missing'):
                summary.collect(root, 'e2e', 0, COMMIT)

    def test_base_image_stage_preserves_release_artifact_summary(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            release = {'commit': COMMIT, 'stages': {'release': {'status': 'passed', 'exit_code': 0}},
                       'release': {'fixture': True}}
            write(root, 'summaries/release.json', release)
            write(root, 'bundle/bundle.json', {'base_images': {}, 'backend': {'sandboxd_revision': 'b' * 40}})
            write(root, 'bundle/registry-images.json', {'references': {}})
            result = summary.collect(root, 'images', 0, COMMIT)
            self.assertEqual(result['release'], release['release'])
            self.assertEqual(set(result['stages']), {'release', 'images'})
            with self.assertRaisesRegex(ValueError, 'different commit'):
                summary.collect(root, 'images', 0, 'd' * 40)

    def test_e2e_still_requires_image_summary(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write(root, 'acceptance/result.json', {'status': 'passed', 'checks': [],
                                                   'cleanup_errors': [], 'missing_checks': []})
            with self.assertRaisesRegex(ValueError, 'previous stage summary missing'):
                summary.collect(root, 'e2e', 0, COMMIT)

    def test_collector_summary_requires_both_nodes(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            write(root, 'summaries/images.json', {'commit':COMMIT,'stages':{},'images':{'collector':{'version':'test'}}})
            write(root, 'acceptance/result.json', {'status':'passed','cleanup_errors':[],'missing_checks':[]})
            with self.assertRaisesRegex(ValueError,'Collector'):
                summary.collect(root,'e2e',0,COMMIT)
            for node in ('node1','node2'):
                for kind in ('collection','gateway-metrics','traces'):
                    write(root,f'acceptance/{node}/{kind}-{node}.json',{'status':'passed'})
            self.assertEqual(summary.collect(root,'e2e',0,COMMIT)['e2e']['collection']['node2']['collection']['status'],'passed')

    def test_l0_requires_business_and_cleanup_but_not_collector_fault_injection(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write(root, 'summaries/images.json', {'commit': COMMIT, 'stages': {},
                  'images': {'collector': {'version': 'test'}}})
            report = {'profile': 'l0', 'status': 'passed', 'checks': ['l0', 'auth'],
                      'cleanup_errors': [], 'missing_checks': []}
            write(root, 'acceptance/result.json', report)
            self.assertEqual(summary.collect(root, 'e2e', 0, COMMIT)['e2e']['report'], report)
            report['cleanup_errors'] = ['backend remains']
            write(root, 'acceptance/result.json', report)
            with self.assertRaisesRegex(ValueError, 'acceptance evidence'):
                summary.collect(root, 'e2e', 0, COMMIT)

    def test_streaming_preserves_failure_and_publishes_failure_summary(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / '.buildkite').mkdir()
            for name in ['step.sh', 'summary.py']:
                shutil.copyfile(ROOT / '.buildkite' / name, root / '.buildkite' / name)
            bin_dir = root / 'bin'
            bin_dir.mkdir()
            agent = bin_dir / 'buildkite-agent'
            agent.write_text('#!/bin/sh\nif [ "$1" = annotate ]; then cat > annotation.md; fi\n')
            agent.chmod(0o755)
            command = [sys.executable, '-u', '-c', 'import sys; print("compiler fixture output"); sys.exit(37)']
            process = subprocess.run(['bash', '.buildkite/step.sh', 'release', *command], cwd=root,
                                     env={**os.environ, 'PATH': str(bin_dir) + os.pathsep + os.environ['PATH'],
                                          'BUILDKITE_COMMIT': COMMIT}, text=True, capture_output=True)
            self.assertEqual(process.returncode, 37, process.stderr)
            self.assertIn('compiler fixture output', process.stdout)
            self.assertIn('compiler fixture output', (root / 'out/buildkite/logs/step-release.log').read_text())
            self.assertIn('failed（exit 37）', (root / 'annotation.md').read_text())

    def test_sourced_setup_streams_without_losing_exports(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / 'setup.sh').write_text('export ADX_TEST_CACHE=/fixture/cache\necho setup-output\n')
            result = subprocess.run(['bash', '-euo', 'pipefail', '-c',
                                     'source setup.sh > >(tee setup.log) 2>&1; test "$ADX_TEST_CACHE" = /fixture/cache; wait'],
                                    cwd=root, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('setup-output', result.stdout)
            self.assertIn('setup-output', (root / 'setup.log').read_text())
