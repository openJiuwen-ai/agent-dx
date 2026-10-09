"""Execution checks for pipeline controls and the test partition."""

import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


def _executable(name, environment=None, cwd=None):
    """Resolve an external command using the child's execution environment."""
    directory = os.getcwd() if cwd is None else os.path.abspath(cwd)
    search_path = os.pathsep.join(
        os.path.abspath(os.path.join(directory, entry)) for entry in os.get_exec_path(environment)
    )
    executable = shutil.which(name, path=search_path)
    if executable is None:
        raise FileNotFoundError(f"required executable not found: {name}")
    return os.path.abspath(executable)


ROOT = Path(__file__).resolve().parents[3]


class PipelineContracts(unittest.TestCase):
    def test_publication_is_disabled_without_explicit_flag(self):
        for script, flag in [
            ('upload-sdk-obs.sh', 'ADX_OBS_UPLOAD'),
            ('publish-admin-pypi.sh', 'ADX_ADMIN_PYPI_UPLOAD'),
            ('publish-sdk-pypi.sh', 'ADX_SDK_PYPI_UPLOAD'),
        ]:
            for value in (None, '0', 'bad'):
                env = {'PATH': os.environ['PATH']}
                if value is not None:
                    env[flag] = value
                result = subprocess.run(
                    [_executable('bash', environment=env), str(ROOT / '.buildkite' / script)],
                    env=env,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 2 if value == 'bad' else 0, result.stderr)

    def test_base_obs_publication_defaults_on_and_index_is_independent_step(self):
        script = ROOT / '.buildkite/upload-obs.sh'
        self.assertIn('${ADX_OBS_UPLOAD:-1}', script.read_text())
        for value, expected in [('0', 0), ('bad', 2)]:
            result = subprocess.run(
                [_executable('bash', environment={'PATH': os.environ['PATH'], 'ADX_OBS_UPLOAD': value}), str(script)],
                env={'PATH': os.environ['PATH'], 'ADX_OBS_UPLOAD': value},
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, expected, result.stderr)

        result = subprocess.run(
            [_executable('bash', environment={'PATH': os.environ['PATH']}), str(script)],
            env={'PATH': os.environ['PATH']},
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Buildkite revision required', result.stderr)

        import yaml

        pipeline = yaml.safe_load((ROOT / '.buildkite/pipeline-package.yml').read_text())
        steps = {step['key']: step for step in pipeline['steps']}
        index = steps['artifact-manifest']
        self.assertEqual(index['depends_on'], 'platform-build')
        self.assertIn('out/buildkite/index.html', index['artifact_paths'])
        self.assertIn('.buildkite/artifact-manifest.sh', index['command'])

    def test_component_test_partition_covers_workspace_once(self):
        workspace = (ROOT / 'Cargo.toml').read_text().split('[workspace.package]')[0]
        members = re.findall(r'^\s+"([^"]+)",', workspace, re.M)
        expected = {
            re.search(r'^name = "([^"]+)"', (ROOT / path / 'Cargo.toml').read_text(), re.M)[1] for path in members
        }
        actual = []
        with tempfile.TemporaryDirectory() as tmp:
            cargo = Path(tmp) / 'cargo'
            cargo.write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
            cargo.chmod(0o755)
            for group in ('platform', 'gateway', 'execd'):
                result = subprocess.run(
                    [
                        _executable('bash', environment=dict(os.environ, PATH=tmp + os.pathsep + os.environ['PATH'])),
                        str(ROOT / '.buildkite/component-tests.sh'),
                        group,
                    ],
                    env=dict(os.environ, PATH=tmp + os.pathsep + os.environ['PATH']),
                    capture_output=True,
                    text=True,
                    check=True,
                )
                args = result.stdout.splitlines()
                actual.extend(args[i + 1] for i, arg in enumerate(args) if arg == '-p')
        self.assertEqual(set(actual), expected)
        self.assertEqual(len(actual), len(expected))

    def test_transport_rejects_unknown_value_before_network(self):
        result = subprocess.run(
            [
                _executable('bash', environment=dict(os.environ, ADX_ARTIFACT_TRANSPORT='invalid')),
                str(ROOT / '.buildkite/component-transfer.sh'),
                'upload',
                'platform',
            ],
            env=dict(os.environ, ADX_ARTIFACT_TRANSPORT='invalid'),
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 2)

    def test_base_retains_sdk_execd_and_real_l0(self):
        import yaml

        pipeline = yaml.safe_load((ROOT / '.buildkite/pipeline-package.yml').read_text())
        steps = {step['key']: step for step in pipeline['steps']}
        self.assertIn('sdk-package', steps)
        self.assertIn('sdk-package', steps['platform-build']['depends_on'])
        self.assertEqual(steps['platform-e2e']['env']['ADX_E2E_PROFILE'], 'l0')
        self.assertEqual(steps['platform-images']['depends_on'], 'platform-build')
        self.assertIn('out/buildkite/adx-execd.tar.gz', steps['platform-build']['artifact_paths'])
        self.assertEqual(steps['admin-pypi']['depends_on'], 'platform-e2e')
        self.assertEqual(steps['sdk-pypi']['depends_on'], 'platform-e2e')
        self.assertTrue((ROOT / '.buildkite/pipeline-admin.yml').exists())
        assemble = (ROOT / '.buildkite/package-components.sh').read_text()
        self.assertNotIn('platform/sdk/sandbox/python/build.sh', assemble)
        self.assertIn('--step sdk-package', assemble)

    def test_python_build_commands_are_shared_by_base_and_independent_pipelines(self):
        import yaml

        base = {s['key']: s for s in yaml.safe_load((ROOT / '.buildkite/pipeline-package.yml').read_text())['steps']}
        for package in ('sdk', 'admin'):
            single = yaml.safe_load((ROOT / f'.buildkite/pipeline-{package}.yml').read_text())['steps'][0]
            self.assertEqual(base[package + '-package']['command'], single['command'])
