import json
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
                        'busybox-static', 'musl-tools'):
            self.assertIn(package, self.dockerfile)
        self.assertIn('rustup target add x86_64-unknown-linux-musl', self.dockerfile)
        self.assertIn('rustup component add rustfmt clippy', self.dockerfile)
        self.assertIn('GOROOT=/usr/local/go', self.dockerfile)
        self.assertIn('go env GOROOT', self.verify)
        self.assertIn('python3 -m venv /opt/adx-build-tools/python', self.dockerfile)
        self.assertNotIn('--break-system-packages', self.dockerfile)
        self.assertIn('/opt/adx-build-tools/python', self.verify)
        self.assertIn('build/runtime/erofs-tools.sh', self.dockerfile)
        self.assertIn('$VERSION_ID == 20.04', self.verify)

    def test_sync_rechecks_the_pushed_digest(self):
        self.assertIn('dockerd --host=', self.sync)
        self.assertIn("trap cleanup EXIT", self.sync)
        self.assertIn("docker info >/dev/null", self.sync)
        self.assertIn('docker push', self.sync)
        self.assertIn('docker pull "$published"', self.sync)
        self.assertIn('verify_image "$published"', self.sync)
        self.assertIn('[[ $repository != *:latest ]]', self.sync)
        self.assertIn('tag="$repository:${BUILDKITE_COMMIT:0:12}"', self.sync)
        self.assertIn('cache_tag="$repository:buildcache"', self.sync)
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
            'build-platform', 'build-gateway', 'build-execd', 'source-gate',
            'platform-build', 'platform-obs',
        }.issubset(build_image_steps))
        for key in ('build-platform', 'build-gateway', 'build-execd',
                    'source-gate', 'platform-build'):
            self.assertIn('key: ' + key, pipeline)
        self.assertIn('depends_on: [build-platform, build-gateway, build-execd, source-gate]', pipeline)
        self.assertIn('out/buildkite/build-manifest.json', pipeline)
        for component in ('platform', 'gateway', 'execd'):
            self.assertIn(f'out/buildkite/components/{component}.tar.gz', pipeline)
        component_build = (ROOT / '.buildkite/build-component.sh').read_text()
        component_package = (ROOT / '.buildkite/package-components.sh').read_text()
        self.assertIn('tar -czf "out/buildkite/components/$component.tar.gz"', component_build)
        self.assertIn('download_component()', component_package)
        self.assertIn('download_component "$component" &', component_package)
        self.assertIn('out/buildkite/backend.tar.gz', pipeline)
        self.assertIn('tar -czf out/buildkite/backend.tar.gz', component_package)
        obs = (ROOT / '.buildkite/upload-obs.sh').read_text()
        self.assertIn("artifact download 'out/buildkite/build-manifest.json'", obs)
        self.assertIn('out/buildkite/build-manifest.json', obs)
        self.assertIn("artifact download 'out/buildkite/backend.tar.gz'", obs)
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


if __name__ == '__main__':
    unittest.main()
