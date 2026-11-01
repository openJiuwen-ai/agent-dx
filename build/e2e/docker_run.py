"""Offline fixed-image Docker adapter for the existing public and AFS assertions."""

import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
import tempfile

HERE = Path(__file__).resolve().parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


common = load('docker_common_driver', HERE / 'run.py')
artifacts = load('docker_fixed_candidate', HERE / 'candidate.py')
harness = load('docker_shared_harness', HERE / 'harness.py')
afs = load('docker_afs_adapters', HERE / 'docker_afs.py')


def selected_checks(profile, case=None):
    if case in ('afs-component', 'afs-component-full', 'afs-sdk', 'afs-sdk-full'):
        if profile != 'standalone':
            raise ValueError('AFS Docker targets require the standalone profile')
        if case.startswith('afs-component'):
            return (case,)
        suffix = '-full' if case.endswith('-full') else ''
        return ('afs-sdk-ownerfs' + suffix, 'afs-sdk-dfs' + suffix)
    return common.selected_checks(profile, case)


def verify_export(bundle, registry, export, commit, contracts_required=False):
    manifest, published = artifacts.fixed_candidate(bundle, registry, commit, afs_required=True,
                                                     contracts_required=contracts_required)
    record = json.loads(export.read_text())
    if record.get('schema_version') != 1 or record.get('product_commit') != commit:
        raise ValueError('offline image export product identity differs')
    if record.get('architecture') != manifest['architecture']:
        raise ValueError('offline image export architecture differs')
    if record.get('bundle_sha256') != artifacts.sha(bundle) or record.get('registry_sha256') != artifacts.sha(registry):
        raise ValueError('offline image export is not bound to these exact manifests')
    if record.get('image_ids') != manifest['image_ids'] or record.get('references') != published['references']:
        raise ValueError('offline image export image identities differ')
    archive = record['archive']
    if archive.get('name') != 'images.tar.gz':
        raise ValueError('unexpected offline image archive name')
    if not isinstance(archive.get('bytes'), int) or not 0 < archive['bytes'] <= 16 * 1024**3:
        raise ValueError('offline image archive size is invalid')
    path = export.parent / archive['name']
    if path.is_symlink() or path.stat().st_size != archive['bytes'] or artifacts.sha(path) != archive['sha256']:
        raise ValueError('offline image archive size/SHA mismatch')
    return manifest, record, path


class DockerRun(common.Run):
    def __init__(self, output, cgroupns='host', selected_case=None, registry=None, image_archive=None):
        super().__init__(output, cgroupns=cgroupns, selected_case=selected_case)
        executable = shutil.which('docker')
        self.docker_argv = [os.path.abspath(executable)] if executable else ['docker']
        self.registry = registry
        self.image_archive = image_archive
        self.owned = {}
        self.network_id = None
        self.harness = None
        self.bundle_manifest = None
        self.private = None
        self.state = output / 'native-state'
        self.initialized = []
        full = bool(selected_case and selected_case.endswith('-full'))
        self.afs_component = afs.Component(self, full=full) if selected_case and 'component' in selected_case else None
        self.afs_product = afs.Product(self, full=full) if selected_case and 'afs-sdk' in selected_case else None

    def docker(self, *args, timeout=180, stream=True):
        if not os.path.isabs(self.docker_argv[0]):
            raise FileNotFoundError('Docker executable is required on the native controller')
        output = self.command([*self.docker_argv, *args], timeout=timeout, stream=stream)
        if args and args[0] == 'run':
            name = str(args[args.index('--name') + 1])
            ids = [line for line in output.splitlines() if re.fullmatch(r'[0-9a-f]{64}', line)]
            if not name.startswith(self.id + '-') or len(ids) != 1:
                raise ValueError('new Docker container identity is ambiguous')
            self.owned[name.removeprefix(self.id + '-')] = ids[0]
        return output

    def execute(self, node, *args, timeout=180):
        return self.docker('exec', self.owned.get(node, self.id + '-' + node), *args, timeout=timeout)

    def copy_from(self, node, source, destination, timeout=120):
        return self.docker('cp', self.owned[node] + ':' + source, str(destination), timeout=timeout)

    def load_images(self, manifest, bundle):
        if self.image_archive is None:
            raise ValueError('verified offline image archive required')
        self.docker('load', '-i', self.image_archive, timeout=600)
        for identity in manifest['image_ids'].values():
            image = json.loads(self.docker('image', 'inspect', identity, stream=False))[0]
            if image['Id'] != identity or image['Architecture'] != manifest['architecture']:
                raise ValueError('offline loaded image identity/architecture differs')
        # Derive only the transport archive consumed by the existing private test registry.
        self.docker('save', '-o', bundle / 'execd.tar', manifest['image_ids']['execd'], timeout=300)
        (self.output / 'derived-image-transport.json').write_text(json.dumps({
            'source_image_id': manifest['image_ids']['execd'],
            'execd_archive_sha256': common.sha(bundle / 'execd.tar'),
            'original_bundle_unchanged': True,
        }, indent=2))

    def container_args(self, node):
        if not self.afs_product:
            return []
        data = self.state / node
        data.mkdir(parents=True)
        return ['--label', 'afs.e2e.data=true', '--device', '/dev/fuse', '-v', str(data) + ':/a']

    def role_container(self, node, manifest, bdi=False):
        data = self.state / node
        data.mkdir(parents=True)
        args = ['run', '-d', '--name', self.id + '-' + node, '--label', 'adx.e2e.run=' + self.id,
                '--network', self.id, '--privileged', '--cgroupns=' + self.cgroupns,
                '--cpus=' + ('4' if bdi else '2'), '--memory=' + ('4g' if bdi else '2g'),
                '--device', '/dev/fuse', '-v', str(data) + ':/a',
                '-e', 'PYTHONDONTWRITEBYTECODE=1']
        if bdi:
            args += ['-v', '/sys/devices/virtual/bdi:/sys/devices/virtual/bdi']
        self.docker(*args, manifest['image_ids']['node'])

    def sync_harness(self, commit, package, nodes=None):
        with tempfile.TemporaryDirectory(prefix='adx-docker-harness-') as temporary:
            root = Path(temporary) / 'e2e'
            files = harness.stage_harness(root, package, commit)
            self.harness = {'schema_version': 1, 'commit': commit, 'product_commit': package['commit'], 'files': files}
            text = json.dumps(self.harness, indent=2) + '\n'
            (root / 'harness.json').write_text(text)
            (self.output / 'harness.json').write_text(text)
            shutil.copyfile(root / 'afs-tests/manifest.json', self.output / 'afs-tests.json')
            verify = ('import hashlib,json,pathlib;root=pathlib.Path("/opt/adx/e2e");'
                      'm=json.loads((root/"harness.json").read_text());'
                      'assert all(hashlib.sha256((root/n).read_bytes()).hexdigest()==s for n,s in m["files"].items())')
            for node in nodes if nodes is not None else list(self.owned):
                self.execute(node, 'rm', '-rf', '/opt/adx/e2e/afs-tests')
                self.docker('cp', str(root) + '/.', self.owned[node] + ':/opt/adx/e2e')
                self.execute(node, 'python3', '-c', verify)
                self.execute(node, 'python3', '/opt/adx/e2e/afs_handoff.py', 'materialize',
                             '--assets', '/opt/adx/e2e/afs-tests', '--package', '/opt/adx/package',
                             '--harness-commit', commit, '--product-commit', package['commit'],
                             '--target', package['target'])

    def before_setup(self, manifest, _bundle, _secrets):
        self.private = _secrets
        self.sync_harness(harness.source_commit(), manifest['package'])
        if self.afs_product:
            self.afs_product.configure(manifest['package'], harness_commit=harness.source_commit())
        for node in self.nodes:
            self.execute(node, 'python3', '/opt/adx/e2e/preflight.py')

    def after_setup(self, node):
        self.initialized.append(node)
        for name in ('api-key', 'other-key', 'admin-key', 'redis-key'):
            path = self.private / name
            if path.is_file():
                self.redactions.add(path.read_text().strip())
        if self.afs_product:
            self.afs_product.configure_runtime(node)

    def deploy(self, manifest, bundle, secrets):
        if not manifest['package'].get('with_afs'):
            raise ValueError('AFS Docker fixed-package acceptance requires an AFS ON candidate')
        self.bundle_manifest = manifest
        if platform.system() != 'Linux':
            raise ValueError('native Linux Docker host required')
        self.state.mkdir()
        filesystem = self.command(['/usr/bin/findmnt', '-n', '-o', 'FSTYPE', '-T', self.state], stream=False).strip()
        if filesystem != 'ext4':
            raise ValueError('native ext4 state required for AFS Docker acceptance')
        transport = self.output / 'runtime-inputs'
        transport.mkdir()
        if self.afs_component:
            self.load_images(manifest, transport)
            self.docker('network', 'create', '--label', 'adx.e2e.run=' + self.id, self.id)
            self.network = True
            self.afs_component.deploy(manifest, harness_commit=harness.source_commit())
        else:
            super().deploy(manifest, transport, secrets)
        self.network_id = json.loads(self.docker('network', 'inspect', self.id, stream=False))[0]['Id']

    def scenarios(self, checks, required=None):
        if self.afs_component:
            return self.afs_component.scenarios(checks)
        if self.afs_product:
            return self.afs_product.scenarios(checks)
        return super().scenarios(checks, required)

    def cleanup(self):
        errors = []
        if self.afs_component:
            errors += self.afs_component.cleanup()
        if self.afs_product:
            errors += self.afs_product.cleanup()
        for node in reversed(self.initialized):
            try:
                self.execute(node, 'sh', '-c', 'test -f /evidence/stop-' + node + '.json || '
                             'python3 /opt/adx/e2e/node.py cleanup ' + node, timeout=180)
            except Exception as exc:
                errors.append(node + ': normal runtime stop: ' + str(exc))
        for node, identity in reversed(list(self.owned.items())):
            try:
                item = json.loads(self.docker('inspect', self.id + '-' + node, stream=False))[0]
                if item['Id'] != identity or item['Config']['Labels'].get('adx.e2e.run') != self.id:
                    raise ValueError('container ownership changed; refusing removal')
                self.docker('rm', '-f', identity, timeout=60)
                if self.cgroupns == 'host':
                    common.cleanup_cgroup(Path('/sys/fs/cgroup') / (self.id + '-' + node))
            except Exception as exc:
                errors.append(node + ': removal: ' + str(exc))
        if self.network:
            try:
                item = json.loads(self.docker('network', 'inspect', self.id, stream=False))[0]
                if item['Labels'].get('adx.e2e.run') != self.id:
                    raise ValueError('network ownership changed; refusing removal')
                self.docker('network', 'rm', item['Id'])
            except Exception as exc:
                errors.append('network cleanup: ' + str(exc))
        (self.output / 'docker-cleanup.json').write_text(json.dumps({
            'status': 'FAIL' if errors else 'PASS', 'errors': errors, 'owned_containers': self.owned,
        }, indent=2))
        return errors
