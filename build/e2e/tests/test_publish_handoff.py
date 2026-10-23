"""Publication consumes the selected immutable artifact transport."""

import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]


class PublishHandoffTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / '.buildkite').mkdir()
        for name in ('publish-amd-package.sh', 'obs-python.sh'):
            shutil.copy2(ROOT / '.buildkite' / name, self.root / '.buildkite' / name)
        (self.root / '.buildkite/upload-obs.sh').write_text('touch "$PUBLICATION_MARKER"\n')
        self.fixtures = self.root / 'fixtures'
        (self.fixtures / 'release').mkdir(parents=True)
        (self.fixtures / 'execd').mkdir()
        self.manifest = b'{"schema_version": 1, "files": {}}\n'
        archive = self.fixtures / 'release/adx-release.tar.gz'
        with tarfile.open(archive, 'w:gz') as stream:
            info = tarfile.TarInfo('./manifest.json')
            info.size = len(self.manifest)
            stream.addfile(info, io.BytesIO(self.manifest))
        (self.fixtures / 'release/build-manifest.json').write_text('{}\n')
        (self.fixtures / 'release/backend.tar.gz').write_bytes(b'backend fixture')
        (self.fixtures / 'release/adx-release.tar.gz.sha256').write_text(
            hashlib.sha256(archive.read_bytes()).hexdigest() + '  adx-release.tar.gz\n'
        )
        (self.fixtures / 'execd/execd.tar.gz').write_bytes(b'exact tested execd component')
        self.calls = self.root / 'calls.jsonl'
        self.marker = self.root / 'published'
        binary = self.root / 'bin'
        binary.mkdir()
        stub = f'#!{sys.executable}\n' + '''
import json, os
from pathlib import Path
import shutil, sys
tool = Path(sys.argv[0]).name
with Path(os.environ['HANDOFF_CALLS']).open('a') as stream:
    stream.write(json.dumps({'tool': tool, 'args': sys.argv[1:]}) + '\\n')
if tool == 'obs-python':
    if sys.argv[1] == '-c':
        raise SystemExit(0)
    if sys.argv[2] != 'download':
        raise SystemExit('unexpected OBS operation')
    group, output = sys.argv[3:5]
    if os.environ.get('FAIL_OBS_GROUP') == group:
        raise SystemExit(23)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    for item in (Path(os.environ['HANDOFF_FIXTURES']) / group).iterdir():
        shutil.copy2(item, output / item.name)
'''
        for name in ('obs-python', 'buildkite-agent'):
            path = binary / name
            path.write_text(stub)
            path.chmod(0o755)
        self.env = {
            **{key: value for key, value in os.environ.items() if key != 'ADX_ARTIFACT_TRANSPORT'},
            'OBS_PYTHON': str(binary / 'obs-python'),
            'PATH': str(binary) + os.pathsep + os.environ.get('PATH', ''),
            'HANDOFF_CALLS': str(self.calls),
            'HANDOFF_FIXTURES': str(self.fixtures),
            'PUBLICATION_MARKER': str(self.marker),
        }

    def run_publisher(self, **environment):
        result = subprocess.run(
            ['bash', '.buildkite/publish-amd-package.sh'], cwd=self.root,
            env={**self.env, **environment}, capture_output=True, text=True,
        )
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()] if self.calls.exists() else []
        return result, calls

    def test_default_obs_handoff_keeps_exact_archives_and_only_fetches_small_candidates_from_buildkite(self):
        result, calls = self.run_publisher()
        self.assertEqual(result.returncode, 0, result.stderr)
        groups = [call['args'][2] for call in calls if call['tool'] == 'obs-python' and call['args'][0] != '-c']
        self.assertEqual(groups, ['release', 'execd'])
        patterns = [call['args'][2] for call in calls if call['tool'] == 'buildkite-agent']
        self.assertEqual(patterns, ['out/buildkite/sdk/*', 'out/buildkite/admin/*'])
        for original, output in (
            ('release/adx-release.tar.gz', 'adx-release.tar.gz'),
            ('execd/execd.tar.gz', 'adx-execd.tar.gz'),
        ):
            self.assertEqual(
                (self.fixtures / original).read_bytes(),
                (self.root / 'out/buildkite' / output).read_bytes(),
            )
        self.assertEqual((self.root / 'out/buildkite/release-manifest.json').read_bytes(), self.manifest)
        checksum = (self.root / 'out/buildkite/adx-execd.tar.gz.sha256').read_text().split()[0]
        self.assertEqual(checksum, hashlib.sha256((self.fixtures / 'execd/execd.tar.gz').read_bytes()).hexdigest())
        self.assertTrue(self.marker.exists())

    def test_explicit_buildkite_transport_keeps_the_full_handoff(self):
        result, calls = self.run_publisher(ADX_ARTIFACT_TRANSPORT='buildkite')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any(call['tool'] == 'obs-python' for call in calls))
        patterns = [call['args'][2] for call in calls]
        self.assertIn('out/buildkite/adx-release.tar.gz*', patterns)
        self.assertIn('out/buildkite/adx-execd.tar.gz*', patterns)
        self.assertTrue(self.marker.exists())

    def test_obs_failure_never_publishes_or_falls_back(self):
        for group in ('release', 'execd'):
            with self.subTest(group=group):
                self.calls.unlink(missing_ok=True)
                result, calls = self.run_publisher(FAIL_OBS_GROUP=group)
                self.assertEqual(result.returncode, 23, result.stderr)
                self.assertFalse(any(call['tool'] == 'buildkite-agent' for call in calls))
                self.assertFalse(self.marker.exists())

    def test_unknown_transport_is_rejected(self):
        result, calls = self.run_publisher(ADX_ARTIFACT_TRANSPORT='unknown')
        self.assertEqual(result.returncode, 2)
        self.assertEqual(calls, [])
        self.assertFalse(self.marker.exists())


if __name__ == '__main__':
    unittest.main()
