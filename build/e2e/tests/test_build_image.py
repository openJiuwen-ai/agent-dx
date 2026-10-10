import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[3]


class BuildImageContractTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / 'build/images/build-environment.json').read_text())
        self.dockerfile = (ROOT / 'build/images/Dockerfile.ci').read_text()
        self.sync = (ROOT / '.buildkite/sync-build-image.sh').read_text()
        self.verify = (ROOT / '.buildkite/verify-build-image.sh').read_text()
        self.bootstrap = (ROOT / '.buildkite/bootstrap-build.sh').read_text()

    def test_ci_steps_use_the_repository_pinned_toolchain(self):
        expected = re.search(r'^channel = "([^"]+)"',
                             (ROOT / 'rust-toolchain.toml').read_text(), re.M)[1]
        steps = yaml.safe_load((ROOT / '.buildkite/pipeline-package.yml').read_text())['steps']
        configured = [step['env']['RUSTUP_TOOLCHAIN'] for step in steps
                      if 'RUSTUP_TOOLCHAIN' in step.get('env', {})]
        self.assertTrue(configured)
        self.assertEqual(set(configured), {expected})

    def test_cargo_bootstrap_uses_pinned_components_and_fails_on_drift(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'rust-toolchain.toml').write_text('[toolchain]\nchannel = "1.95.0"\n')
            (root / 'rustc').write_text('#!/bin/sh\necho "rustc ${FAKE_RUST_VERSION:-1.95.0} (fixture)"\n')
            (root / 'rustup').write_text(
                '#!/bin/sh\n'
                '[ "$*" = "component list --installed --toolchain 1.95.0" ] || exit 9\n'
                '[ "${FAKE_MISSING_COMPONENT:-0}" = 1 ] && exit 0\n'
                'printf "rustfmt-x86_64-unknown-linux-gnu\\nclippy-x86_64-unknown-linux-gnu\\n"\n')
            for name in ('rustc', 'rustup'):
                (root / name).chmod(0o755)
            environment = {key: value for key, value in os.environ.items()
                           if key not in ('RUSTUP_TOOLCHAIN', 'RUSTC_WRAPPER')}
            environment.update(PATH=str(root) + os.pathsep + os.environ['PATH'],
                               ADX_CARGO_HOME=str(root / 'cargo'),
                               CARGO_TARGET_DIR=str(root / 'target'))
            script = ROOT / '.buildkite/setup-cargo.sh'
            for overrides, error in (({}, None), ({'FAKE_MISSING_COMPONENT': '1'}, 'missing rustfmt'),
                                     ({'FAKE_RUST_VERSION': '1.94.0'}, 'version mismatch')):
                with self.subTest(overrides=overrides):
                    result = subprocess.run(['bash', '-c', 'source "$1"', 'test', str(script)],
                                            cwd=root, env=dict(environment, **overrides),
                                            capture_output=True, text=True)
                    if error:
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn(error, result.stderr)
                    else:
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertIn('Rust=1.95.0', result.stdout)

    def test_source_is_digest_pinned_ubuntu_2004_amd64(self):
        self.assertEqual(self.config['schema_version'], 1)
        self.assertEqual(self.config['platform'], 'linux/amd64')
        self.assertEqual(self.config['distribution'], {'id': 'ubuntu', 'version': '20.04'})
        self.assertIn('@sha256:', self.config['source_image'])
        self.assertEqual(self.config['rust'], '1.95.0')
        self.assertEqual(self.config['go'], '1.25.5')
        self.assertEqual(self.config['erofs_utils'], '1.8.10')

    def test_recipe_contains_every_offline_build_prerequisite(self):
        for package in ('autoconf', 'automake', 'libtool', 'pkg-config', 'binutils',
                        'busybox-static', 'musl-tools', 'strace'):
            self.assertIn(package, self.dockerfile)
        self.assertIn('target=x86_64-unknown-linux-musl', self.dockerfile)
        self.assertIn('target=aarch64-unknown-linux-musl', self.dockerfile)
        self.assertIn('rustup target add "$target"', self.dockerfile)
        self.assertIn('rustup component add rustfmt clippy', self.dockerfile)
        self.assertIn('GOROOT=/usr/local/go', self.dockerfile)
        self.assertIn('go env GOROOT', self.verify)
        self.assertIn('python3 -m venv /opt/adx-build-tools/python', self.dockerfile)
        self.assertNotIn('--break-system-packages', self.dockerfile)
        self.assertIn('/opt/adx-build-tools/python', self.verify)
        self.assertIn('build/runtime/erofs-tools.sh', self.dockerfile)
        self.assertIn('strace', self.verify)
        self.assertIn('$VERSION_ID == 20.04', self.verify)

    def test_sync_rechecks_the_pushed_digest(self):
        self.assertIn('dockerd --host=', self.sync)
        self.assertIn("trap cleanup EXIT", self.sync)
        self.assertIn("docker info >/dev/null", self.sync)
        self.assertIn('docker push', self.sync)
        self.assertIn('docker pull "$published"', self.sync)
        self.assertIn('verify_image "$published"', self.sync)
        self.assertIn('[[ $repository != *:latest ]]', self.sync)
        self.assertIn('tag="$repository:${BUILDKITE_COMMIT:0:12}$suffix"', self.sync)
        self.assertIn('cache_tag="$repository:buildcache$suffix"', self.sync)
        self.assertIn('BUILDKIT_INLINE_CACHE=1', self.sync)

    def test_pipeline_and_local_build_use_published_image(self):
        image = self.config['ci_image']
        self.assertRegex(image, r'@sha256:[0-9a-f]{64}$')
        pipeline = (ROOT / '.buildkite/pipeline-package.yml').read_text()
        steps = yaml.safe_load(pipeline)['steps']
        build_image_steps = []
        for step in steps:
            plugins = step.get('plugins', [])
            if plugins:
                container = plugins[0]['kubernetes']['podSpec']['containers'][0]
                if container['image'] == image:
                    build_image_steps.append(step['key'])
        self.assertTrue({
            'build-platform', 'build-gateway', 'build-execd', 'build-afs', 'source-gate',
            'platform-build',
        }.issubset(build_image_steps))
        for key in ('build-platform', 'build-gateway', 'build-execd', 'build-afs',
                    'source-gate', 'platform-build'):
            self.assertIn('key: ' + key, pipeline)
        assembly = next(step for step in steps if step['key'] == 'platform-build')
        self.assertIn('admin-package', assembly['depends_on'])
        self.assertIn('out/buildkite/build-manifest.json', pipeline)
        for component in ('platform', 'gateway', 'execd', 'afs'):
            self.assertIn(f'key: build-{component}', pipeline)
        component_build = (ROOT / '.buildkite/build-component.sh').read_text()
        component_package = (ROOT / '.buildkite/package-components.sh').read_text()
        self.assertIn('tar -czf "out/buildkite/components/$component.tar.gz"', component_build)
        self.assertIn('download_component()', component_package)
        self.assertIn('download_component "$component" &', component_package)
        self.assertIn('out/buildkite/backend.tar.gz', pipeline)
        self.assertIn('tar -czf out/buildkite/backend.tar.gz', component_package)
        obs = (ROOT / '.buildkite/upload-obs.sh').read_text()
        self.assertNotIn("artifact download", obs)
        self.assertIn('out/buildkite/build-manifest.json', obs)
        self.assertIn('out/buildkite/backend.tar.gz', obs)
        local = (ROOT / '.buildkite/run-build-container.sh').read_text()
        self.assertIn("config['ci_image']", local)
        self.assertIn('--platform "$platform"', local)

    def test_bootstrap_only_verifies_baked_tools(self):
        for forbidden in ('apt-get', 'curl ', 'pip install', 'rustup target add',
                          'build/runtime/erofs-tools.sh'):
            self.assertNotIn(forbidden, self.bootstrap)
        self.assertIn('adx-verify-build-image', self.bootstrap)
        self.assertIn('ADX_REDIS_SERVER=/usr/local/bin/redis-server', self.bootstrap)
        self.assertIn(':$PATH"', self.bootstrap)

    def test_build_image_sync_is_an_isolated_pipeline_mode(self):
        maintenance = (ROOT / '.buildkite/pipeline-maintenance.yml').read_text()
        selector = (ROOT / '.buildkite/select-pipeline.sh').read_text()
        package = (ROOT / '.buildkite/pipeline-package.yml').read_text()
        self.assertIn('key: build-image-sync', maintenance)
        self.assertIn('build.env("ADX_BUILD_IMAGE_SYNC_ONLY") == "1"', maintenance)
        self.assertIn('ADX_BUILD_IMAGE_SYNC_ONLY', selector)
        self.assertNotIn('key: build-image-sync', package)

    def test_arm_build_image_sync_uses_native_worker_without_changing_amd64(self):
        pipeline = yaml.safe_load((ROOT / '.buildkite/pipeline-maintenance.yml').read_text())
        steps = {step['key']: step for step in pipeline['steps']}
        amd64 = steps['build-image-sync']
        arm64 = steps['build-image-sync-arm64']

        self.assertEqual(amd64['agents'], {'queue': 'default', 'os': 'linux', 'arch': 'amd64'})
        self.assertIn('ADX_BUILD_IMAGE_CONFIG") != "build/images/build-environment-arm64.json"', amd64['if'])
        self.assertIn('kubernetes', amd64['plugins'][0])

        self.assertEqual(arm64['agents'], {'queue': 'default', 'os': 'macos', 'arch': 'arm64'})
        self.assertEqual(arm64['concurrency_group'], 'adx/native-arm64')
        self.assertEqual(arm64['secrets'], {'SWR_DOCKER_CONFIG_JSON': 'ADX_SWR_PULL_CONFIG'})
        self.assertNotIn('plugins', arm64)
        self.assertIn('ADX_BUILD_IMAGE_CONFIG") == "build/images/build-environment-arm64.json"', arm64['if'])
        self.assertEqual(arm64['command'], amd64['command'])

    def test_sync_build_image_is_portable_on_native_arm_worker_hosts(self):
        self.assertNotIn('readarray', self.sync)
        self.assertNotIn('sha256sum', self.sync)
        self.assertIn('non-Linux build image maintenance requires a native Docker service', self.sync)

    def test_sync_build_image_runs_with_stub_docker_and_without_gnu_sha256sum(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '.buildkite').mkdir()
            shutil.copy(ROOT / '.buildkite/sync-build-image.sh', root / '.buildkite/sync-build-image.sh')
            (root / 'build/images').mkdir(parents=True)
            (root / 'build/images/build-environment-arm64.json').write_text(json.dumps({
                'source_image': 'registry.example/base@sha256:' + 'a' * 64,
                'repository': 'registry.example/adx-build',
                'platform': 'linux/arm64',
            }))
            (root / 'build/images/Dockerfile.ci').write_text('FROM scratch\n')
            commands = root / 'commands'
            commands.mkdir()
            published = 'registry.example/adx-build@sha256:' + 'b' * 64
            docker = commands / 'docker'
            docker.write_text(f'''#!/bin/bash
set -euo pipefail
printf '%s\\n' "$*" >> docker.log
case "$1" in
  info|pull|build|push|tag|run) exit 0 ;;
  image)
    if [[ "$2" == inspect ]]; then
      printf '%s\\n' '{published}'
      exit 0
    fi
    if [[ "$2" == rm ]]; then
      exit 0
    fi
    ;;
esac
exit 17
''')
            sha256sum = commands / 'sha256sum'
            sha256sum.write_text('#!/bin/sh\necho sha256sum must not be called >&2\nexit 99\n')
            for file in (docker, sha256sum):
                file.chmod(0o755)
            environment = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ['PATH'],
                               BUILDKITE_COMMIT='c' * 40,
                               ADX_BUILD_IMAGE_CONFIG='build/images/build-environment-arm64.json')
            bash = Path('/bin/bash') if Path('/bin/bash').exists() else Path(shutil.which('bash'))
            result = subprocess.run([str(bash), '.buildkite/sync-build-image.sh'],
                                    cwd=root, env=environment, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            result_json = json.loads((root / 'out/buildkite/build-image/result.json').read_text())
            self.assertEqual(result_json['platform'], 'linux/arm64')
            self.assertEqual(result_json['reference'], published)
            docker_log = (root / 'docker.log').read_text()
            self.assertIn('--platform linux/arm64', docker_log)
            self.assertIn('GO_SHA256=b00b694903d126c588c378e72d3545549935d3982635ba3f7a964c9fa23fe3b9', docker_log)

    def test_sync_build_image_stops_before_publish_when_verifier_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '.buildkite').mkdir()
            shutil.copy(ROOT / '.buildkite/sync-build-image.sh', root / '.buildkite/sync-build-image.sh')
            (root / 'build/images').mkdir(parents=True)
            (root / 'build/images/build-environment-arm64.json').write_text(json.dumps({
                'source_image': 'registry.example/base@sha256:' + 'a' * 64,
                'repository': 'registry.example/adx-build',
                'platform': 'linux/arm64',
            }))
            (root / 'build/images/Dockerfile.ci').write_text('FROM scratch\n')
            commands = root / 'commands'
            commands.mkdir()
            docker = commands / 'docker'
            docker.write_text('''#!/bin/bash
set -euo pipefail
printf '%s\\n' "$*" >> docker.log
case "$1" in
  info|pull|build) exit 0 ;;
  run) exit 23 ;;
  push) echo unexpected-push >&2; exit 99 ;;
esac
exit 17
''')
            docker.chmod(0o755)
            environment = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ['PATH'],
                               BUILDKITE_COMMIT='c' * 40,
                               ADX_BUILD_IMAGE_CONFIG='build/images/build-environment-arm64.json')
            bash = Path('/bin/bash') if Path('/bin/bash').exists() else Path(shutil.which('bash'))
            result = subprocess.run([str(bash), '.buildkite/sync-build-image.sh'],
                                    cwd=root, env=environment, capture_output=True, text=True)
            self.assertEqual(result.returncode, 23, result.stderr)
            self.assertFalse((root / 'out/buildkite/build-image/result.json').exists())
            docker_log = (root / 'docker.log').read_text()
            self.assertIn('run --rm --platform linux/arm64', docker_log)
            self.assertNotIn('push', docker_log)

    def test_sync_build_image_rejects_bad_config_before_building(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '.buildkite').mkdir()
            shutil.copy(ROOT / '.buildkite/sync-build-image.sh', root / '.buildkite/sync-build-image.sh')
            (root / 'build/images').mkdir(parents=True)
            (root / 'build/images/build-environment-arm64.json').write_text('{bad-json')
            (root / 'build/images/Dockerfile.ci').write_text('FROM scratch\n')
            commands = root / 'commands'
            commands.mkdir()
            docker = commands / 'docker'
            docker.write_text('''#!/bin/bash
set -euo pipefail
printf '%s\\n' "$*" >> docker.log
case "$1" in
  info) exit 0 ;;
  *) echo unexpected-docker-command >&2; exit 99 ;;
esac
''')
            docker.chmod(0o755)
            environment = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ['PATH'],
                               BUILDKITE_COMMIT='c' * 40,
                               ADX_BUILD_IMAGE_CONFIG='build/images/build-environment-arm64.json')
            bash = Path('/bin/bash') if Path('/bin/bash').exists() else Path(shutil.which('bash'))
            result = subprocess.run([str(bash), '.buildkite/sync-build-image.sh'],
                                    cwd=root, env=environment, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / 'out/buildkite/build-image/result.json').exists())
            self.assertEqual((root / 'docker.log').read_text(), 'info\n')


if __name__ == '__main__':
    unittest.main()
