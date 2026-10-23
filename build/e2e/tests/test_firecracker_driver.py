import ast
import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'firecracker'))


def assigned_to(node, name):
    if not isinstance(node, ast.Assign):
        return False
    for target in node.targets:
        if isinstance(target, ast.Name) and target.id == name:
            return True
    return False


def creates_environment(node):
    if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
        return False
    return isinstance(node.value.func, ast.Name) and node.value.func.id == 'Environment'


def has_labels(node, expected):
    for keyword in node.value.keywords:
        if keyword.arg == 'labels' and isinstance(keyword.value, ast.Dict):
            if ast.literal_eval(keyword.value) == expected:
                return True
    return False


def contains_expression(tree, expression):
    expected = ast.dump(ast.parse(expression, mode='eval').body)
    return any(ast.dump(node) == expected for node in ast.walk(tree))


class FirecrackerEvidenceTests(unittest.TestCase):
    def test_rootfs_build_applies_envd_config_and_keeps_plain_rootfs(self):
        spec = importlib.util.spec_from_file_location('runtime_rootfs', ROOT.parent / 'runtime/rootfs.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        images = []

        def capture_rootfs(command, **_kwargs):
            if command[0] != '/mock/mkfs.erofs':
                return None
            root = Path(command[-1])
            manifest = root / 'etc/adx-envd-manifest.json'
            images.append(
                {
                    'manifest': json.loads(manifest.read_text()) if manifest.exists() else None,
                    'passwd': (root / 'etc/passwd').read_text(),
                    'group': (root / 'etc/group').read_text(),
                    'envd': (root / 'usr/local/bin/envd').read_bytes() if manifest.exists() else None,
                    'bash': (root / 'bin/bash').read_bytes() if manifest.exists() else None,
                }
            )
            return None

        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            execd = base / 'adx-execd'
            busybox = base / 'busybox'
            envd_binary = base / 'envd'
            bash = base / 'bash'
            for path, contents in ((execd, b'execd'), (busybox, b'busybox'), (envd_binary, b'envd'), (bash, b'bash')):
                path.write_bytes(contents)
            with (
                mock.patch.object(module, '_executable', side_effect=lambda name: '/mock/' + name),
                mock.patch.object(module, 'static_executable'),
                mock.patch.object(module, 'shell_dependencies', return_value=[]),
                mock.patch.object(module.os, 'chown') as chown,
                mock.patch.object(module.subprocess, 'check_output', return_value='busybox\nsh\n'),
                mock.patch.object(module.subprocess, 'run', side_effect=capture_rootfs),
            ):
                module.build(
                    execd,
                    base / 'with-envd.img',
                    busybox,
                    module.EnvdConfig(envd_binary, bash, 'e2e'),
                )
                module.build(execd, base / 'plain.img', busybox)

        self.assertEqual(images[0]['envd'], b'envd')
        self.assertEqual(images[0]['bash'], b'bash')
        self.assertEqual(images[0]['manifest']['default_user'], 'e2e')
        self.assertEqual(images[0]['manifest']['sha256'], hashlib.sha256(b'envd').hexdigest())
        self.assertIn('e2e:x:1000:1000:E2B user:/home/e2e:/bin/bash\n', images[0]['passwd'])
        self.assertIn('e2e:x:1000:\n', images[0]['group'])
        chown.assert_called_once()
        self.assertEqual(images[1]['manifest'], None)
        self.assertEqual(images[1]['envd'], None)
        self.assertEqual(images[1]['bash'], None)
        self.assertEqual(images[1]['passwd'], 'root:x:0:0:root:/root:/bin/sh\n')
        self.assertEqual(images[1]['group'], 'root:x:0:\n')

    def test_fc_bootstrap_has_separate_admin_and_tenant_credentials(self):
        import os
        import subprocess

        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            artifact = base / 'package/runtime/adx-runtime-rootfs.img'
            artifact.parent.mkdir(parents=True)
            artifact.write_bytes(b'test rootfs')
            environment = {**os.environ, 'ADX_FC_BASE': str(base), 'ADX_FC_RUN_ROOT': str(base / 'run')}
            # Supply the same helper layout used by the deployed fixture.
            (base / 'e2e').mkdir()
            import shutil

            shutil.copy2(ROOT.parent / 'ci/rpc_certificates.py', base / 'e2e/rpc_certificates.py')
            subprocess.run(
                [sys.executable, str(ROOT / 'firecracker/configure.py'), 'node1'],
                env=environment,
                check=True,
                capture_output=True,
            )
            deployment = json.loads((base / 'run/deployment.yaml').read_text())
            coordinator = next(s for s in deployment['services'] if s['role'] == 'coordinator')
            credentials = coordinator['config']['bootstrap_credentials']
            administrators = [c for c in credentials if c['administrator']]
            tenants = [c for c in credentials if not c['administrator']]
            self.assertEqual(len(administrators), 1)
            self.assertEqual({c['tenant_id'] for c in tenants}, {'e2e', 'e2e-other'})
            self.assertNotIn(administrators[0]['key_file'], [c['key_file'] for c in tenants])
            self.assertEqual(Path(administrators[0]['key_file']).stat().st_mode & 0o777, 0o600)

    def test_fault_oracles_read_the_nested_runtime_record(self):
        from runtime_record import runtime_id, runtime_ip

        record = {'state': 'Running', 'runtime': {'id': 'backend-1', 'ip': '10.0.0.8'}}
        self.assertEqual(runtime_id(record), 'backend-1')
        self.assertEqual(runtime_ip(record), '10.0.0.8')
        with self.assertRaises(KeyError):
            runtime_id({'state': 'Running', 'runtime_id': 'legacy-backend'})

    def test_entrypoint_case_uses_the_execd_terminal_status_contract(self):
        tree = ast.parse((ROOT / 'firecracker/sdk_checkpoint.py').read_text())
        self.assertTrue(contains_expression(tree, "entrypoint_info['status_kind'] == 'exited'"))
        self.assertTrue(contains_expression(tree, "entrypoint_info['exit_code'] == 7"))
        self.assertFalse(contains_expression(tree, "entrypoint_info['status_kind'] == 'exit_code'"))

    def test_paused_restart_targets_the_selected_instance(self):
        checkpoint = (ROOT / 'firecracker/sdk_checkpoint.py').read_text()
        restart = (ROOT / 'firecracker/restart_paused.py').read_text()
        self.assertIn('[*a.restart_command, environment.id]', checkpoint)
        tree = ast.parse(restart)
        key = next(node.value for node in tree.body if assigned_to(node, 'key'))
        self.assertEqual(ast.dump(key), ast.dump(ast.parse("'environment:' + INSTANCE_ID", mode='eval').body))
        self.assertFalse(
            any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == 'len'
                and node.args
                and isinstance(node.args[0], ast.Name)
                and node.args[0].id == 'records'
                for node in ast.walk(tree)
            )
        )

    def test_runtime_network_policy_uses_a_fresh_execution(self):
        source = (ROOT / 'firecracker/sdk_checkpoint.py').read_text()
        tree = ast.parse(source)
        assignments = [node for node in ast.walk(tree) if creates_environment(node)]
        network_case = next(
            node.lineno
            for node in assignments
            if any(isinstance(target, ast.Name) and target.id == 'networked' for target in node.targets)
        )
        checkpoint_case = next(node.lineno for node in assignments if has_labels(node, {'app': 'checkpoint-source'}))
        self.assertLess(network_case, checkpoint_case)
        self.assertTrue(contains_expression(tree, 'networked.update_network_policy(NetworkPolicy.block())'))
        self.assertTrue(contains_expression(tree, 'networked.update_network_policy(None)'))
        self.assertTrue(contains_expression(tree, "os.environ['ADX_FC_EGRESS_PROBE_HOST']"))
        strings = '\n'.join(
            node.value for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        )
        self.assertIn('/bin/bash', strings)
        self.assertIn('/dev/tcp/', strings)
        self.assertIn('[[ "$line" == HTTP/* ]]', strings)
        self.assertNotIn('/bin/busybox', strings)
        self.assertNotIn('nc -z', strings)
        fixture = (ROOT / 'firecracker/node.py').read_text()
        canonical = ast.unparse(ast.parse(fixture))
        self.assertIn("probe_namespace = 'adx-probe-' + suffix", canonical)
        self.assertTrue(contains_expression(ast.parse(fixture), "'198.18.0.2/30'"))
        self.assertIn("_executable('ip'), 'netns', 'delete', probe_namespace", canonical)

    def test_runtime_profile_uses_local_erofs_for_vm_and_pinned_oci_for_kubernetes(self):
        import fc_runtime_profile

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            package = root / 'package'
            artifact = package / 'runtime/adx-runtime-rootfs.img'
            artifact.parent.mkdir(parents=True)
            artifact.write_bytes(b'erofs')
            custom = '/etc/adx/custom-image-process.json'
            local = fc_runtime_profile.resolve(package, False, root / 'missing', custom)
            self.assertEqual(local['rootfs']['runtime_class'], 'firecracker')
            self.assertEqual(local['rootfs']['path'], str(artifact.resolve()))
            self.assertEqual(local['bootstrap']['root'], str(artifact.resolve()))
            self.assertEqual(local['bootstrap']['image_process_config'], custom)
            image = root / 'runtime-image'
            image.write_text('registry.example/adx-runtime@sha256:' + 'a' * 64)
            remote = fc_runtime_profile.resolve(package, True, image, custom)
            self.assertEqual(remote['rootfs']['image'], image.read_text())
            self.assertEqual(remote['bootstrap']['image'], image.read_text())
            self.assertEqual(remote['bootstrap']['image_process_config'], custom)
            with self.assertRaisesRegex(RuntimeError, 'must be absolute'):
                fc_runtime_profile.resolve(package, False, root / 'missing', 'etc/relative.json')
            image.write_text('registry.example/adx-runtime:latest')
            with self.assertRaisesRegex(RuntimeError, 'digest pinned'):
                fc_runtime_profile.resolve(package, True, image)

    def test_missing_or_duplicate_cases_cannot_pass(self):
        import acceptance

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / 'sdk').mkdir()
            (root / 'lifecycle').mkdir()
            (root / 'result.json').write_text(json.dumps({'status': 'passed'}))
            for group in ('sdk', 'lifecycle'):
                (root / group / 'result.json').write_text(
                    json.dumps({'status': 'passed', 'cases': [{'name': 'duplicate', 'passed': True}] * 5})
                )
            with self.assertRaises(ValueError):
                acceptance.verify(root)

    def test_manifest_requires_kvm_and_pinned_images_and_selected_node(self):
        import acceptance

        image = 'registry.example/node@sha256:' + 'a' * 64
        pod = acceptance.pod('adx-e2e-test', image, 'amd64', 'worker-a', True)
        spec = pod['spec']
        self.assertFalse(spec['automountServiceAccountToken'])
        self.assertNotIn('hostPID', spec)
        self.assertNotIn('hostNetwork', spec)
        self.assertEqual(spec['nodeSelector']['kubernetes.io/hostname'], 'worker-a')
        self.assertIn({'name': 'kvm', 'hostPath': {'path': '/dev/kvm', 'type': 'CharDevice'}}, spec['volumes'])
        with self.assertRaises(ValueError):
            acceptance.pod('adx-e2e-test', 'node:latest', 'amd64', 'worker-a', True)

    def test_kit_rejects_altered_files_and_backend_identity(self):
        import hashlib

        import kit

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            files = {}
            for name in kit.REQUIRED:
                p = root / name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(name.encode())
                files[name] = hashlib.sha256(p.read_bytes()).hexdigest()
            patches = kit.expected_sandboxd_patches()
            backend = {
                'target': 'aarch64-unknown-linux-gnu',
                'sandboxd_revision': kit.expected_sandboxd_revision(),
                'sandboxd_patches': patches,
                'files': {
                    'sandboxd': files.get('bin/sandboxd'),
                    'sbox': files.get('bin/sbox'),
                    'redis-cli': files.get('tools/redis-cli'),
                },
            }
            manifest = {
                'schema_version': 1,
                'target': backend['target'],
                'sandboxd_revision': backend['sandboxd_revision'],
                'sandboxd_patches': patches,
                'initrd_provenance': {
                    'source_revision': backend['sandboxd_revision'],
                    'patches': patches,
                    'sha256': files.get('artifacts/initrd.img'),
                },
                'files': files,
            }
            (root / 'manifest.json').write_text(json.dumps(manifest))
            kit.verify(root, backend)
            (root / 'artifacts/Image').write_bytes(b'altered')
            with self.assertRaises(ValueError):
                kit.verify(root, backend)

    def test_kit_rejects_missing_or_stale_patch_and_initrd_provenance(self):
        import hashlib

        import kit

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            files = {}
            for name in kit.REQUIRED:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(name.encode())
                files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
            patches = kit.expected_sandboxd_patches()
            backend = {
                'target': 'aarch64-unknown-linux-gnu',
                'sandboxd_revision': kit.expected_sandboxd_revision(),
                'sandboxd_patches': patches,
                'files': {
                    'sandboxd': files.get('bin/sandboxd'),
                    'sbox': files.get('bin/sbox'),
                    'redis-cli': files.get('tools/redis-cli'),
                },
            }
            manifest = {
                'schema_version': 1,
                'target': backend['target'],
                'sandboxd_revision': backend['sandboxd_revision'],
                'sandboxd_patches': patches,
                'initrd_provenance': {
                    'source_revision': backend['sandboxd_revision'],
                    'patches': patches,
                    'sha256': files.get('artifacts/initrd.img'),
                },
                'files': files,
            }
            (root / 'manifest.json').write_text(json.dumps(manifest))
            kit.verify(root, backend)
            manifest.pop('sandboxd_patches')
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'patch identity'):
                kit.verify(root, backend)
            manifest['sandboxd_patches'] = list(reversed(patches))
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'patch identity'):
                kit.verify(root, backend)
            manifest['sandboxd_patches'] = patches
            manifest.pop('initrd_provenance')
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'initrd provenance'):
                kit.verify(root, backend)
            manifest['initrd_provenance'] = {
                'source_revision': backend['sandboxd_revision'],
                'patches': patches,
                'sha256': '0' * 64,
            }
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'initrd provenance'):
                kit.verify(root, backend)

    def test_complete_evidence_passes_but_cleanup_error_fails(self):
        import acceptance

        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for group, names in acceptance.CASES.items():
                (root / group).mkdir()
                (root / group / 'result.json').write_text(
                    json.dumps({'status': 'passed', 'cases': [{'name': name, 'passed': True} for name in names]})
                )
            (root / 'result.json').write_text(json.dumps({'status': 'passed'}))
            (root / 'sdk/snapshot-collected-before-clone-resume.json').write_text(
                json.dumps({'snapshot_id': 'saved', 'state': 'Deleted', 'references': []})
            )
            orphan_checks = (
                'passed',
                'coordinator_collected',
                'foreign_preserved',
                'unmarked_preserved',
            )
            (root / 'orphan-gc.json').write_text(
                json.dumps(
                    {
                        **{k: True for k in orphan_checks},
                        'registered_checkpoint_preserved': 'saved',
                    }
                )
            )
            (root / 'snapshots-final.json').write_text(json.dumps({'saved': {'state': 'Deleted', 'references': []}}))
            (root / 'catalog-final.json').write_text('{}')
            (root / 's3-final.xml').write_text('<ListBucketResult />')
            (root / 'inventory-final.txt').write_text('ID STATUS\n')
            self.assertEqual(len(acceptance.verify(root)), sum(map(len, acceptance.CASES.values())))
            (root / 'result.json').write_text(json.dumps({'status': 'passed', 'stop_error': 'failed'}))
            with self.assertRaises(ValueError):
                acceptance.verify(root)

    def test_fc_cleanup_refuses_replaced_namespace(self):
        spec = importlib.util.spec_from_file_location('fc_kube', ROOT / 'firecracker/kubernetes.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as d:
            run = module.FirecrackerRun(Path(d), Path('/test/kubeconfig'))
            run.namespace_attempted = True
            run.namespace_uid = 'original'
            calls = []

            def command(*args, **kwargs):
                calls.append(args)
                return json.dumps({'metadata': {'uid': 'replacement', 'labels': {'adx.e2e.run': run.id}}})

            run.kube = command
            self.assertTrue(run.cleanup())
            self.assertFalse(any('delete' in c for c in calls))

    def test_ci_summary_does_not_pass_missing_or_partial_result(self):
        import subprocess

        script = ROOT.parents[1] / '.buildkite/fc-summary.py'
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d) / 'out/buildkite/firecracker'
            result = subprocess.run(
                [sys.executable, str(script), '--exit-code', '0'], cwd=d, capture_output=True, text=True
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('FAIL', (directory / 'summary.md').read_text())
            (directory / 'result.json').write_text(
                json.dumps({'status': 'passed', 'cases': [{'name': str(n)} for n in range(9)]})
            )
            result = subprocess.run(
                [sys.executable, str(script), '--exit-code', '0'], cwd=d, capture_output=True, text=True
            )
            self.assertNotEqual(result.returncode, 0)
