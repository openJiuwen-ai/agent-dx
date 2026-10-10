"""ARM packaging contracts: native execution and isolated publication."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[3]


class ArmPackageTests(unittest.TestCase):
    def test_arm_is_parallel_and_uses_available_native_worker(self):
        pipeline = yaml.safe_load((ROOT / '.buildkite/pipeline-package.yml').read_text())
        steps = {step['key']: step for step in pipeline['steps']}
        arm = steps['platform-build-arm64']
        self.assertEqual(arm['agents']['arch'], 'arm64')
        self.assertEqual(arm['agents']['os'], 'macos')
        self.assertNotIn('platform-build', arm['depends_on'])
        self.assertEqual(set(arm['depends_on']), {'build-platform-arm64', 'build-gateway-arm64', 'build-execd-arm64', 'build-afs-arm64', 'afs-gate', 'sdk-package', 'admin-package', 'source-gate'})
        compiler = steps['build-platform-arm64']
        self.assertNotIn('depends_on', compiler)
        self.assertIn('build-arm-package.sh build platform', compiler['command'])
        self.assertIn('build-arm-package.sh package', arm['command'])
        self.assertIn('out/buildkite/arm64/components/*.tar.gz', compiler['artifact_paths'])
        self.assertIn('out/buildkite/arm64/*.tar.gz', arm['artifact_paths'])
        self.assertIn('out/buildkite/arm64/logs/**/*', arm['artifact_paths'])
        self.assertEqual(arm['secrets'], {'SWR_DOCKER_CONFIG_JSON': 'ADX_SWR_PULL_CONFIG'})
        self.assertIn('with_registry.py --docker', arm['command'])
        self.assertEqual(steps['artifact-manifest-arm64']['depends_on'], 'publish-arm64')
        self.assertEqual(steps['artifact-manifest']['depends_on'], 'publish-amd64')
        for component in ('platform', 'gateway', 'execd'):
            self.assertNotIn('depends_on', steps[f'build-{component}'])
            self.assertNotIn('depends_on', steps[f'build-{component}-arm64'])
        self.assertTrue(all(not step['label'].startswith(':') for step in steps.values()))
        publisher = steps['publish-arm64']
        self.assertEqual(publisher['depends_on'], 'platform-build-arm64')
        self.assertEqual(publisher['agents']['arch'], 'amd64')
        self.assertIn('out/buildkite/arm64/obs/*', publisher['artifact_paths'])
        publish = (ROOT / '.buildkite/publish-arm-package.sh').read_text()
        self.assertIn('--step platform-build-arm64', publish)
        self.assertIn('ADX_BUILD_ARCH=arm64', publish)
        self.assertNotIn('OBS_ACCESS_KEY_ID', (ROOT / '.buildkite/build-arm-package.sh').read_text())

    def test_afs_on_uses_the_native_arm_component_and_gate(self):
        steps = {step['key']: step for step in yaml.safe_load(
            (ROOT / '.buildkite/pipeline-package.yml').read_text())['steps']}
        afs = steps['build-afs-arm64']
        self.assertEqual(afs['if'], 'build.env("ADX_WITH_AFS") == "1"')
        self.assertEqual(afs['env']['ADX_WITH_AFS'], '1')
        self.assertEqual(afs['agents']['arch'], 'arm64')
        self.assertIn('build-arm-package.sh build afs', afs['command'])
        runner = (ROOT / '.buildkite/build-arm-package.sh').read_text()
        self.assertIn('parts+=(afs)', runner)
        self.assertIn('ADX_ARM_TESTS ADX_WITH_AFS', runner)
        self.assertIn('build:afs', runner)
        self.assertIn('"${backend_args[@]}"', (ROOT / '.buildkite/package-components.sh').read_text())

    def test_architecture_resolves_native_targets_and_rejects_unknown(self):
        script = ROOT / '.buildkite/build-architecture.sh'
        command = 'source "$1"; printf "%s %s" "$ADX_RELEASE_TARGET" "$ADX_MUSL_TARGET"'
        for arch, target in [('amd64', 'x86_64'), ('arm64', 'aarch64')]:
            result = subprocess.run(
                ['bash', '-c', command, 'test', str(script)],
                env=dict(os.environ, ADX_BUILD_ARCH=arch), capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, f'{target}-unknown-linux-gnu {target}-unknown-linux-musl')
        result = subprocess.run(
            ['bash', str(script)], env=dict(os.environ, ADX_BUILD_ARCH='invalid'),
            capture_output=True, text=True,
        )
        self.assertNotEqual(result.returncode, 0)

    def test_arm_image_and_container_cache_are_isolated(self):
        config = json.loads((ROOT / 'build/images/build-environment-arm64.json').read_text())
        self.assertEqual(config['platform'], 'linux/arm64')
        self.assertRegex(config['source_image'], r'@sha256:[0-9a-f]{64}$')
        self.assertRegex(config['ci_image'], r'@sha256:[0-9a-f]{64}$')
        script = (ROOT / '.buildkite/build-arm-package.sh').read_text()
        for term in [
            '--platform linux/arm64', 'adx-arm64-cargo-home', 'adx-arm64-cargo-target',
            'ADX_BUILD_ARCH=arm64', '--step sdk-package', '--step admin-package',
        ]:
            self.assertIn(term, script)
        native = (ROOT / '.buildkite/package-arm-native.sh').read_text()
        self.assertNotIn('install.sh', native)
        self.assertNotIn('redis.sock', native)
        self.assertIn('.buildkite/package-components.sh', native)
        self.assertIn('ADX_ARM_TESTS:-0', native)
        self.assertIn('ADX_COMPONENT_TESTS=0', native)
        self.assertIn('chmod -R a+rwX out/buildkite', script)
        self.assertIn('build/release/package.py verify', (ROOT / '.buildkite/package-components.sh').read_text())
        self.assertNotIn('build_backend.py', native)


class ComponentTestSwitch(unittest.TestCase):
    def test_arm_can_skip_tests_without_skipping_compilation(self):
        for part, flag in (('platform', '0'), ('platform', '1'), ('afs', '0'), ('afs', '1')):
            with self.subTest(component=part, flag=flag), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / '.buildkite').mkdir()
                (root / 'build/release').mkdir(parents=True)
                for file in ('build-component.sh', 'build-architecture.sh'):
                    shutil.copy(ROOT / '.buildkite' / file, root / '.buildkite' / file)
                shutil.copy(ROOT / 'build/release/component.py', root / 'build/release/component.py')
                (root / '.buildkite/bootstrap-build.sh').write_text('export CARGO_TARGET_DIR="$PWD/target"\n')
                (root / '.buildkite/component-tests.sh').write_text('echo tests-ran > tests-ran\n')
                commands = root / 'commands'
                commands.mkdir()
                fixtures = {
                    'git': '#!/bin/bash\nif [[ $1 == rev-parse ]]; then echo "$BUILDKITE_COMMIT"; fi\n',
                    'rustc': '#!/bin/bash\necho "host: aarch64-unknown-linux-gnu"\n',
                    'cargo': '#!/bin/bash\necho "$@" > compile-args\nmkdir -p "$CARGO_TARGET_DIR/release"\nfor bin in adxctl adx-inspect adx-coordinator adxlet afs-meta afs-node; do echo binary > "$CARGO_TARGET_DIR/release/$bin"; done\n',
                }
                for name, content in fixtures.items():
                    file = commands / name
                    file.write_text(content)
                    file.chmod(0o755)
                result = subprocess.run(
                    [shutil.which('bash'), '.buildkite/build-component.sh', part], cwd=root,
                    env=dict(os.environ, PATH=str(commands) + os.pathsep + os.environ['PATH'],
                             BUILDKITE_COMMIT='b' * 40, ADX_BUILD_ARCH='arm64',
                             ADX_COMPONENT_LOCAL='1', ADX_COMPONENT_TESTS=flag, ADX_WITH_AFS='1' if part == 'afs' else '0'),
                    capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertTrue((root / 'compile-args').read_text().startswith('build --locked --release'))
                self.assertEqual((root / 'tests-ran').exists(), flag == '1')
                self.assertTrue((root / f'out/buildkite/components/{part}.tar.gz').is_file())


class ArmRunnerTests(unittest.TestCase):
    def test_afs_component_requires_explicit_on_before_builder_access(self):
        result = subprocess.run(
            [shutil.which('bash'), str(ROOT / '.buildkite/build-arm-package.sh'), 'build', 'afs'],
            env=dict(os.environ, ADX_WITH_AFS='0'), capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('AFS component requires ADX_WITH_AFS=1', result.stderr)

    def test_container_failure_retains_logs_and_archives_in_arm_directory(self):
        for part in ('execd', 'afs'):
            with self.subTest(component=part), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                (root / 'build/images').mkdir(parents=True)
                (root / 'build/images/build-environment-arm64.json').write_text(json.dumps({
                    'ci_image': 'registry/builder@sha256:' + 'a' * 64,
                }))
                commands = root / 'commands'
                commands.mkdir()
                fixtures = {
                    'uname': '#!/bin/bash\necho arm64\n',
                    'git': '#!/bin/bash\nif [[ $1 == rev-parse ]]; then echo "$BUILDKITE_COMMIT"; fi\n',
                    'buildkite-agent': '#!/bin/bash\nexit 0\n',
                    'docker': '''#!/bin/bash
if [[ $1 == run ]]; then
  printf '%s\\n' "$@" > docker-args
  python3 -c 'from pathlib import Path; assert Path("out/buildkite/logs").stat().st_mode & 2'
  if [[ $? != 0 ]]; then exit 18; fi
  mkdir -p out/buildkite/components
  echo evidence > out/buildkite/components/execd.tar.gz
  echo deliberate-container-failure
  exit 17
fi
''',
                }
                for name, contents in fixtures.items():
                    file = commands / name
                    file.write_text(contents)
                    file.chmod(0o755)
                # Avoid selecting the host's installed Docker in this failure fixture.
                script = (ROOT / '.buildkite/build-arm-package.sh').read_text().replace(
                    'export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"', ':',
                )
                runner = root / 'runner.sh'
                runner.write_text(script)
                result = subprocess.run(
                    [shutil.which('bash'), str(runner), 'build', part], cwd=root,
                    env=dict(
                        os.environ, PATH=str(commands) + os.pathsep + os.environ['PATH'],
                        BUILDKITE_COMMIT='b' * 40, BUILDKITE_BUILD_ID='build-test', ADX_OBS_UPLOAD='0', ADX_WITH_AFS='1' if part == 'afs' else '0',
                    ),
                    capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 17, result.stderr)
                evidence = root / 'out/buildkite/arm64'
                self.assertEqual((evidence / 'components/execd.tar.gz').read_text(), 'evidence\n')
                self.assertIn(
                    'deliberate-container-failure', (evidence / 'logs/step-release-arm64.log').read_text(),
                )
                args = (root / 'docker-args').read_text().splitlines()
                self.assertIn('ADX_WITH_AFS', args)
