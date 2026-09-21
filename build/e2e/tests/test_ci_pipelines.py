import importlib.util
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]


class IndependentPipelineTests(unittest.TestCase):
    def test_product_pipelines_have_separate_configs_and_steps(self):
        package = (ROOT / '.buildkite/pipeline-package.yml').read_text()
        sdk = (ROOT / '.buildkite/pipeline-sdk.yml').read_text()
        full = (ROOT / '.buildkite/pipeline-full.yml').read_text()

        self.assertIn('key: platform-build', package)
        self.assertNotIn('key: platform-e2e', package)
        self.assertNotIn('key: sdk-package', package)

        self.assertIn('key: sdk-package', sdk)
        self.assertNotIn('key: platform-build', sdk)
        self.assertNotIn('key: platform-e2e', sdk)

        self.assertIn('key: platform-images', full)
        self.assertIn('key: platform-e2e', full)
        self.assertIn('ADX_E2E_PROFILE: full', full)
        self.assertNotIn('key: platform-build', full)
        self.assertNotIn('key: sdk-package', full)

    def test_default_entrypoint_dispatches_by_buildkite_pipeline_slug(self):
        pipeline = (ROOT / '.buildkite/pipeline.yml').read_text()
        selector = (ROOT / '.buildkite/select-pipeline.sh').read_text()
        self.assertIn('.buildkite/select-pipeline.sh', pipeline)
        self.assertIn('agent-dx-python-sdk', selector)
        self.assertIn('pipeline-sdk.yml', selector)
        self.assertIn('agent-dx-full-test', selector)
        self.assertIn('pipeline-full.yml', selector)
        self.assertIn('pipeline-package.yml', selector)

    def test_full_image_build_requires_explicit_base_and_sdk_builds(self):
        script = (ROOT / '.buildkite/package-e2e.sh').read_text()
        self.assertIn('ADX_BASE_PACKAGE_BUILD_ID', script)
        self.assertIn('ADX_SDK_BUILD_ID', script)
        self.assertIn('--sdk-wheel', script)
        self.assertIn('--sdk-candidate', script)

    def test_e2e_bundle_records_independent_sdk_candidate(self):
        spec = importlib.util.spec_from_file_location(
            'e2e_prepare', ROOT / 'build/e2e/prepare.py'
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheel = root / 'adx_sandbox-0.1.0-py3-none-any.whl'
            wheel.write_bytes(b'wheel')
            candidate = root / 'sdk-candidate.json'
            candidate.write_text(
                '{"schema_version":1,"commit":"' + 'a' * 40
                + '","version":"0.1.0","files":{"adx_sandbox-0.1.0-py3-none-any.whl":"'
                + module.sha(wheel) + '"}}'
            )
            value = module.verify_sdk(wheel, candidate, 'a' * 40)
            self.assertEqual(value['version'], '0.1.0')
            wheel.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'SDK artifact'):
                module.verify_sdk(wheel, candidate, 'a' * 40)


if __name__ == '__main__':
    unittest.main()
