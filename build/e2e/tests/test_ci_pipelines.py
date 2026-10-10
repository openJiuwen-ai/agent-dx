import importlib.util
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]


class IndependentPipelineTests(unittest.TestCase):
    def test_product_pipelines_have_separate_configs_and_steps(self):
        package = (ROOT / '.buildkite/pipeline-package.yml').read_text()
        sdk = (ROOT / '.buildkite/pipeline-sdk.yml').read_text()
        admin = (ROOT / '.buildkite/pipeline-admin.yml').read_text()
        full = (ROOT / '.buildkite/pipeline-full.yml').read_text()

        self.assertIn('key: platform-build', package)
        for key in ('build-platform', 'build-gateway', 'build-execd', 'build-afs', 'source-gate'):
            self.assertIn(f'key: {key}', package)
        self.assertNotIn('key: afs-gate', package)
        steps = {step['key']: step for step in yaml.safe_load(package)['steps']}
        self.assertEqual(
            set(steps['platform-build']['depends_on']),
            {'build-platform', 'build-gateway', 'build-execd', 'build-afs', 'source-gate', 'admin-package', 'sdk-package'},
        )
        self.assertIn('key: platform-e2e', package)
        self.assertIn('key: sdk-package', package)

        self.assertIn('key: sdk-package', sdk)
        self.assertIn('key: sdk-pypi', sdk)
        self.assertIn('ADX_SDK_PYPI_UPLOAD', sdk)
        self.assertNotIn('key: platform-build', sdk)
        self.assertNotIn('key: platform-e2e', sdk)

        self.assertIn('key: admin-package', admin)
        self.assertIn('key: admin-pypi', admin)
        self.assertIn('ADX_ADMIN_PYPI_UPLOAD', admin)
        self.assertNotIn('key: sdk-package', admin)

        self.assertIn('key: platform-images', full)
        self.assertIn('key: platform-e2e', full)
        self.assertIn('ADX_E2E_PROFILE: full', full)
        self.assertNotIn('key: platform-build', full)
        self.assertNotIn('key: sdk-package', full)

    def test_admin_package_uses_sdk_runner_and_blocks_assembly(self):
        package = yaml.safe_load((ROOT / '.buildkite/pipeline-package.yml').read_text())
        steps = {step['key']: step for step in package['steps']}
        sdk = yaml.safe_load((ROOT / '.buildkite/pipeline-sdk.yml').read_text())
        python_image = sdk['steps'][0]['env']['ADX_SDK_TEST_IMAGE']
        admin = steps['admin-package']
        self.assertEqual(admin['env']['ADX_SDK_TEST_IMAGE'], python_image)
        self.assertIn('admin-package', steps['platform-build']['depends_on'])
        self.assertIn('.buildkite/build-sdk.sh admin', admin['command'])
        self.assertNotIn('admin-gate', steps)
        self.assertNotIn('platform-obs', steps)
        self.assertEqual(steps['admin-pypi']['depends_on'], 'platform-e2e')
        self.assertNotIn('artifact download', (ROOT / '.buildkite/upload-obs.sh').read_text())
        self.assertIn('bash .buildkite/upload-obs.sh', (ROOT / '.buildkite/package-components.sh').read_text())
        self.assertIn('ADX_WITH_AFS', (ROOT / '.buildkite/select-pipeline.sh').read_text())
        self.assertIn('components+=(afs)', (ROOT / '.buildkite/package-components.sh').read_text())

    def test_default_entrypoint_dispatches_by_buildkite_pipeline_slug(self):
        pipeline = (ROOT / '.buildkite/pipeline.yml').read_text()
        selector = (ROOT / '.buildkite/select-pipeline.sh').read_text()
        self.assertIn('.buildkite/select-pipeline.sh', pipeline)
        self.assertIn('agent-dx-python-sdk', selector)
        self.assertIn('pipeline-sdk.yml', selector)
        self.assertIn('agent-dx-full-test', selector)
        self.assertIn('pipeline-full.yml', selector)
        self.assertIn('agent-dx-admin', selector)
        self.assertTrue((ROOT / '.buildkite/pipeline-admin.yml').exists())
        self.assertIn('pipeline-package.yml', selector)

    def test_admin_publish_is_explicit_and_verifies_the_index(self):
        pipeline = (ROOT / '.buildkite/pipeline-package.yml').read_text()
        publisher = (ROOT / '.buildkite/publish-admin-pypi.sh').read_text()
        self.assertIn('build.env("ADX_ADMIN_PYPI_UPLOAD") == "1"', pipeline)
        self.assertIn('build.tag != null', pipeline)
        self.assertIn('adxadmin-v${version}', publisher)
        self.assertIn('TWINE_PASSWORD', publisher)
        self.assertIn('build/python/verify_index.py', publisher)
        self.assertNotIn('--skip-existing', publisher)

    def test_sdk_publish_is_explicit_and_verifies_the_index(self):
        pipeline = (ROOT / '.buildkite/pipeline-sdk.yml').read_text()
        publisher = (ROOT / '.buildkite/publish-sdk-pypi.sh').read_text()
        self.assertIn('build.env("ADX_SDK_PYPI_UPLOAD") == "1"', pipeline)
        self.assertIn('build.tag != null', pipeline)
        self.assertIn('sdk-v${version}', publisher)
        self.assertIn('TWINE_PASSWORD', publisher)
        self.assertIn('build/python/verify_index.py', publisher)
        self.assertNotIn('--skip-existing', publisher)

    def test_full_image_build_requires_explicit_base_and_sdk_builds(self):
        script = (ROOT / '.buildkite/package-e2e.sh').read_text()
        self.assertIn('ADX_BASE_PACKAGE_BUILD_ID', script)
        self.assertIn('ADX_SDK_BUILD_ID', script)
        self.assertIn('out/buildkite/build-manifest.json', script)
        self.assertIn('out/buildkite/backend.tar.gz', script)
        self.assertIn('--sdk-wheel', script)
        self.assertIn('--sdk-candidate', script)
        self.assertIn('ADX_E2E_ARTIFACT_COMMIT:-$BUILDKITE_COMMIT', script)
        self.assertIn('ADX_E2E_RUNSC_URL', script)
        self.assertIn('ADX_E2E_RUNSC_IMAGE', script)
        self.assertIn('build/e2e/runsc_image.py', script)
        self.assertIn('sha512sum --check', script)

    def test_e2e_handoff_forwards_afs_mode_and_preserves_verifier_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / 'tools'
            tools.mkdir()
            scripts = {
                'git': '#!/bin/sh\n[ "$1" = rev-parse ] && echo fixture-commit\nexit 0\n',
                'buildkite-agent': '#!/bin/sh\nmkdir -p out/buildkite/package/sdk\ntouch out/buildkite/package/sdk/adx_sandbox-fixture.whl\n',
                'sha256sum': '#!/bin/sh\nexit 0\n',
                'tar': '#!/bin/sh\nexit 0\n',
                'python3': '#!/bin/sh\nif [ "$2" = verify-build ]; then echo "$*"; exit 31; fi\n',
            }
            for name, script in scripts.items():
                path = tools / name
                path.write_text(script)
                path.chmod(0o755)
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(('ADX_', 'BUILDKITE_'))}
            env.update(PATH=str(tools) + os.pathsep + env['PATH'],
                       ADX_E2E_IMAGE_REPOSITORY='fixture.invalid/adx',
                       ADX_BASE_PACKAGE_BUILD_ID='fixture-base', ADX_SDK_BUILD_ID='fixture-sdk',
                       BUILDKITE_COMMIT='fixture-commit')
            for mode in (None, '0', '1', 'invalid'):
                with self.subTest(mode=mode):
                    child_env = dict(env)
                    if mode is not None:
                        child_env['ADX_WITH_AFS'] = mode
                    result = subprocess.run(['bash', str(ROOT / '.buildkite/package-e2e.sh')],
                                            cwd=root, env=child_env, capture_output=True, text=True)
                    if mode == 'invalid':
                        self.assertEqual(result.returncode, 2)
                        self.assertIn('ADX_WITH_AFS must be 0 or 1', result.stderr)
                    else:
                        # Stop at the real handoff boundary: do not start Docker after failure.
                        self.assertEqual(result.returncode, 31, result.stderr)
                        self.assertIn('component.py verify-build', result.stdout)
                        self.assertEqual('--with-afs' in result.stdout, mode == '1')

    def test_e2e_bundle_records_independent_sdk_candidate(self):
        spec = importlib.util.spec_from_file_location('e2e_prepare', ROOT / 'build/e2e/prepare.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheel = root / 'adx_sandbox-0.1.0-py3-none-any.whl'
            wheel.write_bytes(b'wheel')
            candidate = root / 'sdk-candidate.json'
            candidate.write_text(
                '{"schema_version":1,"commit":"'
                + 'a' * 40
                + '","version":"0.1.0","files":{"adx_sandbox-0.1.0-py3-none-any.whl":"'
                + module.sha(wheel)
                + '"}}'
            )
            value = module.verify_sdk(wheel, candidate, 'a' * 40)
            self.assertEqual(value['version'], '0.1.0')
            wheel.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'SDK artifact'):
                module.verify_sdk(wheel, candidate, 'a' * 40)

    def test_optional_runsc_binary_must_match_release_architecture(self):
        spec = importlib.util.spec_from_file_location('e2e_prepare', ROOT / 'build/e2e/prepare.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            runsc = Path(directory) / 'runsc'
            elf = bytearray(64)
            elf[:6] = b'\x7fELF\x02\x01'
            elf[18:20] = (62).to_bytes(2, 'little')
            runsc.write_bytes(elf)
            runsc.chmod(0o755)
            self.assertEqual(module.verify_runsc(runsc, 'x86_64-unknown-linux-gnu'), module.sha(runsc))
            with self.assertRaisesRegex(ValueError, 'architecture'):
                module.verify_runsc(runsc, 'aarch64-unknown-linux-gnu')
            runsc.write_bytes(b'not-an-ELF')
            with self.assertRaisesRegex(ValueError, 'ELF'):
                module.verify_runsc(runsc, 'x86_64-unknown-linux-gnu')


if __name__ == '__main__':
    unittest.main()
