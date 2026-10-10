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


def _without_afs_environment():
    environment = dict(os.environ)
    for key in ('ADX_WITH_AFS', 'ADX_AFS_ALL_FEATURES', 'ADX_WITH_DFS', 'ADX_DFS_ALL_FEATURES'):
        environment.pop(key, None)
    return environment


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
        self.assertEqual(index['depends_on'], 'publish-amd64')
        self.assertIn('out/buildkite/index.html', index['artifact_paths'])
        self.assertIn('.buildkite/artifact-manifest.sh', index['command'])

    def test_component_test_partition_covers_workspace_once(self):
        workspace = (ROOT / 'Cargo.toml').read_text()
        default_members = workspace.split('default-members = [', 1)[1].split(']', 1)[0]
        members = re.findall(r'^\s+"([^"]+)",', default_members, re.M)
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

    def test_default_make_gates_exclude_afs_packages(self):
        makefile = (ROOT / 'Makefile').read_text()
        for package in (
            'afs',
            'afs-client',
            'afs-error',
            'afs-logging',
            'afs-metrics',
            'afs-protocol',
            'afs-tracing',
            'afs-transport',
        ):
            self.assertIn(f'--exclude {package}', makefile)
        self.assertIn('ADX_WITH_AFS ?= 0', makefile)
        self.assertIn('ADX_AFS_ALL_FEATURES ?= 0', makefile)
        self.assertIn('afs-check: afs-build afs-lint afs-test', makefile)
        self.assertIn('afs-all-features-lint:', makefile)
        self.assertIn('pkg-config --exists libibverbs', makefile)
        self.assertIn('afs-build:\n\t$(CARGO) build --locked -p afs --bins', makefile)
        self.assertNotIn('--bins --examples', makefile)


    def test_platform_release_passes_make_afs_mode_to_release_builder(self):
        makefile = (ROOT / 'Makefile').read_text()
        self.assertIn('ADX_WITH_AFS=$(ADX_WITH_AFS) bash build/release/build.sh', makefile)

    def test_release_build_validates_afs_mode_before_rust_probe(self):
        script = (ROOT / 'build/release/build.sh').read_text()
        validation = script.index('ADX_WITH_AFS must be 0 or 1')
        root_resolution = script.index('root=$(cd')
        rust_probe = script.index('rustc -vV')
        afs_compile = script.index('Compile Agent FS (AFS)')
        linux_guard = script.index('ADX_WITH_AFS=1 requires a Linux release builder')
        self.assertLess(validation, root_resolution)
        self.assertLess(validation, rust_probe)
        self.assertLess(linux_guard, afs_compile)

    def test_ci_runner_has_explicit_afs_suite(self):
        result = subprocess.run(
            [
                _executable('python3'),
                str(ROOT / 'build/ci/run.py'),
                'afs',
                '--list',
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertIn('ADX_WITH_AFS=1', result.stdout)
        self.assertIn('ADX_AFS_ALL_FEATURES=1', result.stdout)
        self.assertIn('afs-check', result.stdout)

    def test_source_gate_owns_optional_afs_lint(self):
        script = (ROOT / '.buildkite/source-gate.sh').read_text()
        self.assertIn('ADX_WITH_AFS must be 0 or 1', script)
        self.assertIn('ADX_AFS_ALL_FEATURES must be 0 or 1', script)
        self.assertIn('make rust-check', script)
        self.assertNotIn('make afs-check', script)
        self.assertFalse((ROOT / '.buildkite/afs-gate.sh').exists())

    def test_source_gate_routes_afs_lint_by_switch(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            (repo / '.buildkite').mkdir()
            (repo / 'build/e2e/tests').mkdir(parents=True)
            (repo / 'build/release/tests').mkdir(parents=True)
            (repo / 'build/e2e/tests/test_stub.py').write_text(
                'import unittest\n\nclass Stub(unittest.TestCase):\n    def test_ok(self):\n        pass\n'
            )
            (repo / 'build/release/tests/test_stub.py').write_text(
                'import unittest\n\nclass Stub(unittest.TestCase):\n    def test_ok(self):\n        pass\n'
            )
            (repo / '.gitignore').write_text('gate-ran\nout/\n__pycache__/\n')
            (repo / '.buildkite/source-gate.sh').write_text((ROOT / '.buildkite/source-gate.sh').read_text())
            (repo / '.buildkite/bootstrap-build.sh').write_text('#!/usr/bin/env bash\n')
            (repo / 'Makefile').write_text(
                'rust-check:\n\t@echo rust >> gate-ran\n'
                '\t@if [ "$${ADX_WITH_AFS}" = "1" ]; then '
                'echo "afs:$${ADX_WITH_AFS}:$${ADX_AFS_ALL_FEATURES}" >> gate-ran; fi\n'
            )
            subprocess.run(['git', 'init'], cwd=repo, capture_output=True, text=True, check=True)
            subprocess.run(['git', 'config', 'user.email', 'ci@example.invalid'], cwd=repo, check=True)
            subprocess.run(['git', 'config', 'user.name', 'CI'], cwd=repo, check=True)
            subprocess.run(['git', 'add', '.'], cwd=repo, check=True)
            subprocess.run(['git', 'commit', '-m', 'initial'], cwd=repo, capture_output=True, text=True, check=True)
            commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
            base_env = dict(_without_afs_environment(), BUILDKITE_COMMIT=commit, PATH=os.environ['PATH'])
            for value, all_features, expected in [
                (None, None, 'rust'),
                ('0', None, 'rust'),
                ('1', None, 'rust\nafs:1:0'),
                ('1', '1', 'rust\nafs:1:1'),
            ]:
                with self.subTest(value=value, all_features=all_features):
                    (repo / 'gate-ran').unlink(missing_ok=True)
                    env = dict(base_env)
                    if value is not None:
                        env['ADX_WITH_AFS'] = value
                    if all_features is not None:
                        env['ADX_AFS_ALL_FEATURES'] = all_features
                    subprocess.run(['bash', '.buildkite/source-gate.sh'], cwd=repo, env=env,
                                   capture_output=True, text=True, check=True)
                    self.assertEqual((repo / 'gate-ran').read_text().strip(), expected)

    def test_source_gate_rejects_invalid_afs_switches_before_bootstrap(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            (repo / '.buildkite').mkdir()
            (repo / 'build/e2e/tests').mkdir(parents=True)
            (repo / 'build/release/tests').mkdir(parents=True)
            (repo / 'build/e2e/tests/test_stub.py').write_text(
                'import unittest\n\nclass Stub(unittest.TestCase):\n    def test_ok(self):\n        pass\n'
            )
            (repo / 'build/release/tests/test_stub.py').write_text(
                'import unittest\n\nclass Stub(unittest.TestCase):\n    def test_ok(self):\n        pass\n'
            )
            (repo / '.gitignore').write_text('bootstrap-ran\nout/\n__pycache__/\n')
            (repo / '.buildkite/source-gate.sh').write_text((ROOT / '.buildkite/source-gate.sh').read_text())
            (repo / '.buildkite/bootstrap-build.sh').write_text('echo bootstrap-ran > bootstrap-ran\n')
            (repo / 'Makefile').write_text('rust-check:\n\t@true\n')
            subprocess.run(['git', 'init'], cwd=repo, capture_output=True, text=True, check=True)
            subprocess.run(['git', 'config', 'user.email', 'ci@example.invalid'], cwd=repo, check=True)
            subprocess.run(['git', 'config', 'user.name', 'CI'], cwd=repo, check=True)
            subprocess.run(['git', 'add', '.'], cwd=repo, check=True)
            subprocess.run(['git', 'commit', '-m', 'initial'], cwd=repo, capture_output=True, text=True, check=True)
            commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
            for key in ('ADX_WITH_AFS', 'ADX_AFS_ALL_FEATURES'):
                env = dict(_without_afs_environment(), BUILDKITE_COMMIT=commit, PATH=os.environ['PATH'], **{key: 'bad'})
                result = subprocess.run(['bash', '.buildkite/source-gate.sh'], cwd=repo, env=env,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn(f'{key} must be 0 or 1', result.stderr)
                self.assertFalse((repo / 'bootstrap-ran').exists())

    def test_source_gate_fails_when_afs_lint_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            (repo / '.buildkite').mkdir()
            (repo / 'build/e2e/tests').mkdir(parents=True)
            (repo / 'build/release/tests').mkdir(parents=True)
            (repo / 'build/e2e/tests/test_stub.py').write_text(
                'import unittest\n\nclass Stub(unittest.TestCase):\n    def test_ok(self):\n        pass\n'
            )
            (repo / 'build/release/tests/test_stub.py').write_text(
                'import unittest\n\nclass Stub(unittest.TestCase):\n    def test_ok(self):\n        pass\n'
            )
            (repo / '.gitignore').write_text('gate-ran\nout/\n__pycache__/\n')
            (repo / '.buildkite/source-gate.sh').write_text((ROOT / '.buildkite/source-gate.sh').read_text())
            (repo / '.buildkite/bootstrap-build.sh').write_text('#!/usr/bin/env bash\n')
            (repo / 'Makefile').write_text(
                'rust-check:\n\t@echo rust >> gate-ran\n'
                '\t@if [ "$${ADX_WITH_AFS}" = "1" ]; then exit 7; fi\n'
            )
            subprocess.run(['git', 'init'], cwd=repo, capture_output=True, text=True, check=True)
            subprocess.run(['git', 'config', 'user.email', 'ci@example.invalid'], cwd=repo, check=True)
            subprocess.run(['git', 'config', 'user.name', 'CI'], cwd=repo, check=True)
            subprocess.run(['git', 'add', '.'], cwd=repo, check=True)
            subprocess.run(['git', 'commit', '-m', 'initial'], cwd=repo, capture_output=True, text=True, check=True)
            commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
            env = dict(_without_afs_environment(), BUILDKITE_COMMIT=commit, ADX_WITH_AFS='1', PATH=os.environ['PATH'])
            result = subprocess.run(
                ['bash', '.buildkite/source-gate.sh'],
                cwd=repo,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual((repo / 'gate-ran').read_text().strip(), 'rust')

    def test_legacy_flags_fail_before_build_or_network(self):
        scripts = (
            'build/release/build.sh', '.buildkite/select-pipeline.sh',
            '.buildkite/build-component.sh', '.buildkite/package-components.sh',
            '.buildkite/upload-obs.sh', '.buildkite/source-gate.sh',
        )
        for legacy in ('ADX_WITH_DFS', 'ADX_DFS_ALL_FEATURES'):
            environment = dict(os.environ, **{legacy: '1', 'ADX_WITH_AFS': '1'})
            for script in scripts:
                with self.subTest(legacy=legacy, entry=script):
                    result = subprocess.run(['bash', str(ROOT / script)], env=environment,
                                            capture_output=True, text=True)
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertIn(legacy + ' was replaced by', result.stderr)
            result = subprocess.run(['make', 'help'], cwd=ROOT, env=environment,
                                    capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(legacy + ' was replaced by', result.stderr)

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
        self.assertNotIn('afs-gate', steps)
        self.assertIn('build-afs', steps)
        self.assertEqual(steps['build-afs']['if'], 'build.env("ADX_WITH_AFS") == "1"')
        self.assertEqual(steps['build-afs']['env']['ADX_WITH_AFS'], '1')
        self.assertIn('sdk-package', steps)
        self.assertIn('sdk-package', steps['platform-build']['depends_on'])
        self.assertNotIn('afs-gate', steps['platform-build']['depends_on'])
        self.assertIn('build-afs', steps['platform-build']['depends_on'])
        self.assertEqual(steps['platform-e2e']['env']['ADX_E2E_PROFILE'], 'l0')
        self.assertEqual(steps['platform-images']['depends_on'], ['platform-build', 'publish-amd64'])
        self.assertIn('out/buildkite/adx-execd.tar.gz', steps['platform-build']['artifact_paths'])
        self.assertEqual(steps['admin-pypi']['depends_on'], 'platform-e2e')
        self.assertEqual(steps['sdk-pypi']['depends_on'], 'platform-e2e')
        self.assertTrue((ROOT / '.buildkite/pipeline-admin.yml').exists())
        assemble = (ROOT / '.buildkite/package-components.sh').read_text()
        self.assertNotIn('platform/sdk/sandbox/python/build.sh', assemble)
        self.assertIn('--step sdk-package', assemble)
        self.assertIn('components=(platform gateway execd)', assemble)
        self.assertIn('components+=(afs)', assemble)
        self.assertIn('package_args+=(--with-afs)', assemble)
        self.assertIn('"${package_args[@]}"', assemble)
        self.assertIn('component_args+=(--with-afs)', (ROOT / '.buildkite/upload-obs.sh').read_text())

    def test_python_build_commands_are_shared_by_base_and_independent_pipelines(self):
        import yaml

        base = {s['key']: s for s in yaml.safe_load((ROOT / '.buildkite/pipeline-package.yml').read_text())['steps']}
        for package in ('sdk', 'admin'):
            single = yaml.safe_load((ROOT / f'.buildkite/pipeline-{package}.yml').read_text())['steps'][0]
            self.assertEqual(base[package + '-package']['command'], single['command'])
