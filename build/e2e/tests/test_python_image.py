import os
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]


class PythonImageTests(unittest.TestCase):
    def test_package_and_publish_jobs_share_verified_wheelhouse_digest(self):
        import yaml
        config = json.loads((ROOT / 'build/images/python-environment.json').read_text())
        image = config['ci_image']
        self.assertTrue(image.startswith(config['repository'] + '@sha256:'))
        for name in ('package', 'sdk', 'admin'):
            pipeline = yaml.safe_load((ROOT / f'.buildkite/pipeline-{name}.yml').read_text())
            for step in pipeline['steps']:
                if step.get('key') in ('sdk-package', 'admin-package'):
                    self.assertEqual(step['env']['ADX_SDK_TEST_IMAGE'], image)
                if step.get('key') in ('sdk-pypi', 'admin-pypi'):
                    containers = step['plugins'][0]['kubernetes']['podSpec']['containers']
                    self.assertEqual(containers[0]['image'], image)

    def test_offline_wheelhouse_is_selected_and_stale_recipe_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            wheels = Path(tmp)
            required = ROOT / 'build/images/python-requirements.txt'
            (wheels / 'requirements.txt').write_bytes(required.read_bytes())
            command = 'source .buildkite/python-env.sh; test "$PIP_NO_INDEX" = 1; test "$PIP_FIND_LINKS" = "$ADX_PYTHON_WHEELHOUSE"'
            env = dict(os.environ, ADX_PYTHON_WHEELHOUSE=tmp)
            run = subprocess.run(['bash','-c',command], cwd=ROOT, env=env, capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stderr)
            (wheels / 'requirements.txt').write_text('stale')
            run = subprocess.run(['bash','-c',command], cwd=ROOT, env=env, capture_output=True, text=True)
            self.assertNotEqual(run.returncode, 0)
            self.assertIn('rebuild', run.stderr)

    def test_cache_path_and_index_variables_reach_python_container(self):
        script = (ROOT / '.buildkite/build-sdk.sh').read_text()
        self.assertIn('PIP_CACHE_DIR=/root/.cache/pip', script)
        self.assertIn('PIP_INDEX_URL', script)
        self.assertIn('PIP_EXTRA_INDEX_URL', script)
