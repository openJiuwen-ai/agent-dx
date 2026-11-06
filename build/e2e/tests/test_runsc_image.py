"""Pinned OCI carriers must supply the complete architecture-matched runtime kit."""

import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from e2e import runsc_image


IMAGE = 'registry.example/adx-runsc@sha256:' + 'a' * 64
CONTAINER = 'b' * 64
TARGETS = {'x86_64-unknown-linux-gnu': (62, 'amd64'), 'aarch64-unknown-linux-gnu': (183, 'arm64')}
HELPERS = ('checkpointgofer', 'gvisor-sentry-prewarmer', 'gvisor_sentry', 'runsc-fd-parking', 'runsc-metric-server')


class DockerFixture:
    def __init__(self, source, fail=None):
        self.source = source
        self.fail = fail
        self.calls = []

    def __call__(self, args, **_kwargs):
        self.calls.append(args)
        if args[1] == 'create':
            if '--cidfile' in args:
                Path(args[args.index('--cidfile') + 1]).write_text(CONTAINER + '\n')
        failures = (self.fail,) if isinstance(self.fail, str) else self.fail or ()
        if args[1] in failures:
            raise subprocess.CalledProcessError(1, args)
        if args[1] == 'create':
            return subprocess.CompletedProcess(args, 0, stdout='carrier create progress\n')
        if args[1] == 'cp':
            member = args[2].split(':', 1)[1].lstrip('/')
            source = self.source / member
            destination = Path(args[3])
            if source.is_dir():
                shutil.copytree(source, destination, symlinks=True)
            else:
                shutil.copy2(source, destination, follow_symlinks=False)
        return subprocess.CompletedProcess(args, 0)


class RunscImageTests(unittest.TestCase):
    def fixture(self, root, target='x86_64-unknown-linux-gnu'):
        source = root / 'source'
        (source / 'gvisor-bin').mkdir(parents=True)
        files = {}
        machine = TARGETS[target][0]
        for name in ('runsc', *(f'gvisor-bin/{helper}' for helper in HELPERS)):
            path = source / name
            path.write_bytes(b'\x7fELF\x02\x01' + bytes(12) + machine.to_bytes(2, 'little') + name.encode())
            path.chmod(0o755)
            files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        artifact = {target: {'machine': machine, 'files': files}}
        return source, artifact

    def invoke(self, source, artifact, destination, expected=None, **kwargs):
        docker = kwargs.pop('run', DockerFixture(source))
        with mock.patch.object(runsc_image.runsc_release, 'ARTIFACTS', artifact), \
                mock.patch.object(runsc_image.shutil, 'which', return_value='/usr/bin/docker'):
            result = runsc_image.stage(IMAGE, expected, destination, run=docker, **kwargs)
        return result, docker

    def test_both_architectures_verify_and_publish_only_six_pinned_files(self):
        for target, (_, platform) in TARGETS.items():
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, artifact = self.fixture(root, target)
                (source / 'gvisor-bin/untrusted-extra').write_bytes(b'do not publish')
                destination = root / 'runtime/custom-launcher'
                digest, docker = self.invoke(source, artifact, destination, target=target)
                self.assertEqual(digest, artifact[target]['files']['runsc'])
                with mock.patch.object(runsc_image.runsc_release, 'ARTIFACTS', artifact):
                    verified = runsc_image.runsc_release.verify_runtime(destination, target)
                self.assertEqual(verified, artifact[target]['files'])
                self.assertEqual(
                    set(path.name for path in destination.parent.iterdir()), {'custom-launcher', 'gvisor-bin'}
                )
                self.assertEqual(set(path.name for path in (destination.parent / 'gvisor-bin').iterdir()), set(HELPERS))
                self.assertEqual([call[1] for call in docker.calls], ['pull', 'create', 'cp', 'cp', 'rm'])
                self.assertEqual(docker.calls[0], ['/usr/bin/docker', 'pull', '--platform', 'linux/' + platform, IMAGE])
                self.assertEqual(docker.calls[1][-2:], ['/runsc', '--version'])
                self.assertTrue(all(Path(call[0]).is_absolute() for call in docker.calls))

    def test_default_target_and_optional_sha512(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, artifact = self.fixture(root)
            expected = hashlib.sha512((source / 'runsc').read_bytes()).hexdigest()
            self.invoke(source, artifact, root / 'runtime/runsc', expected=expected)
            self.assertTrue((root / 'runtime/runsc').exists())

    def test_invalid_kits_leave_no_runtime_or_temporary_residue(self):
        cases = ('wrong-helper-hash', 'missing-helper', 'helper-mode', 'helper-symlink', 'wrong-architecture', 'sha512')
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, artifact = self.fixture(root)
                helper = source / 'gvisor-bin/gvisor_sentry'
                if case == 'wrong-helper-hash':
                    helper.write_bytes(b'corrupt')
                elif case == 'missing-helper':
                    helper.unlink()
                elif case == 'helper-mode':
                    helper.chmod(0o644)
                elif case == 'helper-symlink':
                    helper.unlink()
                    helper.symlink_to(source / 'runsc')
                elif case == 'wrong-architecture':
                    helper.write_bytes(b'\x7fELF\x02\x01' + bytes(12) + (183).to_bytes(2, 'little'))
                    artifact['x86_64-unknown-linux-gnu']['files']['gvisor-bin/gvisor_sentry'] = (
                        hashlib.sha256(helper.read_bytes()).hexdigest()
                    )
                docker = DockerFixture(source)
                destination = root / 'runtime/runsc'
                with self.assertRaises(ValueError):
                    self.invoke(
                        source, artifact, destination, expected='0' * 128 if case == 'sha512' else None, run=docker
                    )
                self.assertEqual(list(destination.parent.iterdir()), [])
                self.assertEqual(docker.calls[-1][1], 'rm')

    def test_copy_or_container_cleanup_failure_does_not_publish(self):
        for failure in ('cp', 'rm'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, artifact = self.fixture(root)
                destination = root / 'runtime/runsc'
                docker = DockerFixture(source, fail=failure)
                with self.assertRaises(subprocess.CalledProcessError):
                    self.invoke(source, artifact, destination, run=docker)
                self.assertEqual(list(destination.parent.iterdir()), [])
                self.assertEqual(docker.calls[-1][1], 'rm')

    def test_create_failure_after_writing_cid_still_removes_container(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, artifact = self.fixture(root)
            destination = root / 'runtime/runsc'
            docker = DockerFixture(source, fail='create')
            with self.assertRaises(subprocess.CalledProcessError) as raised:
                self.invoke(source, artifact, destination, run=docker)
            self.assertEqual(raised.exception.cmd[1], 'create')
            self.assertEqual(docker.calls[-1][1:], ['rm', '-f', CONTAINER])
            self.assertEqual(list(destination.parent.iterdir()), [])

    def test_copy_and_cleanup_failure_preserves_copy_error_and_logs_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, artifact = self.fixture(root)
            destination = root / 'runtime/runsc'
            docker = DockerFixture(source, fail=('cp', 'rm'))
            with self.assertLogs(level='ERROR') as logs:
                with self.assertRaises(subprocess.CalledProcessError) as raised:
                    self.invoke(source, artifact, destination, run=docker)
            self.assertEqual(raised.exception.cmd[1], 'cp')
            self.assertIn(CONTAINER, '\n'.join(logs.output))
            self.assertIn('cleanup', '\n'.join(logs.output).lower())
            self.assertEqual(list(destination.parent.iterdir()), [])

    def test_existing_destinations_are_never_modified(self):
        for existing in ('runsc', 'gvisor-bin', 'dangling-helper-directory'):
            with self.subTest(existing=existing), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, artifact = self.fixture(root)
                destination = root / 'runtime/runsc'
                destination.parent.mkdir()
                if existing == 'runsc':
                    destination.write_bytes(b'keep launcher')
                elif existing == 'gvisor-bin':
                    (destination.parent / existing).mkdir()
                    (destination.parent / existing / 'old-helper').write_bytes(b'keep helper')
                else:
                    (destination.parent / 'gvisor-bin').symlink_to(root / 'missing', target_is_directory=True)
                before = sorted(path.name for path in destination.parent.iterdir())
                docker = DockerFixture(source)
                with self.assertRaisesRegex(ValueError, 'exist'):
                    self.invoke(source, artifact, destination, run=docker)
                self.assertEqual(sorted(path.name for path in destination.parent.iterdir()), before)
                self.assertEqual(docker.calls, [])

    def test_unpinned_image_or_malformed_sha512_is_rejected_before_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            docker = DockerFixture(Path(directory))
            for image, checksum in (('registry.example/adx-runsc:latest', None), (IMAGE, 'bad')):
                with self.subTest(image=image), self.assertRaises(ValueError):
                    runsc_image.stage(image, checksum, Path(directory) / 'runsc', run=docker)
            self.assertEqual(docker.calls, [])

    def test_publication_failure_rolls_back_only_new_helpers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, artifact = self.fixture(root)
            destination = root / 'runtime/runsc'
            original_link = runsc_image.os.link

            def fail_launcher(source_path, destination_path):
                if Path(destination_path) == destination:
                    destination.write_bytes(b'concurrent owner')
                    raise FileExistsError('concurrent output')
                return original_link(source_path, destination_path)

            with mock.patch.object(runsc_image.os, 'link', side_effect=fail_launcher):
                with self.assertRaises(FileExistsError):
                    self.invoke(source, artifact, destination)
            self.assertEqual(destination.read_bytes(), b'concurrent owner')
            self.assertEqual(list(destination.parent.iterdir()), [destination])

    def test_cli_accepts_optional_sha512_and_architecture(self):
        for extra, checksum, target in (
            ([], None, 'x86_64-unknown-linux-gnu'),
            (['--target', 'aarch64-unknown-linux-gnu', '--sha512', 'a' * 128],
             'a' * 128, 'aarch64-unknown-linux-gnu'),
        ):
            with self.subTest(target=target), mock.patch('sys.argv', [
                'runsc_image.py', '--image', IMAGE, '--output', '/runtime/runsc', *extra,
            ]), mock.patch.object(runsc_image, 'stage', return_value='verified-hash') as staged, \
                    mock.patch('sys.stdout', new_callable=io.StringIO) as stdout:
                runsc_image.main()
                staged.assert_called_once_with(IMAGE, checksum, Path('/runtime/runsc'), target=target)
                self.assertEqual(stdout.getvalue(), 'verified-hash\n')

    def test_cli_stdout_is_only_digest_when_docker_emits_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, artifact = self.fixture(root)
            commands = root / 'commands'
            commands.mkdir()
            docker = commands / 'docker'
            docker.write_text(
                f'#!{sys.executable}\n'
                'from pathlib import Path\n'
                'import shutil, sys\n'
                'command = sys.argv[1]\n'
                'if command == "create":\n'
                '    if "--cidfile" in sys.argv:\n'
                f'        Path(sys.argv[sys.argv.index("--cidfile") + 1]).write_text({CONTAINER!r})\n'
                '    print("docker create progress")\n'
                '    print("docker create diagnostic", file=sys.stderr)\n'
                'elif command == "pull":\n'
                '    print("docker pull progress")\n'
                'elif command == "cp":\n'
                '    print("docker cp progress")\n'
                '    member = sys.argv[2].split(":", 1)[1].lstrip("/")\n'
                f'    source = Path({str(source)!r}) / member\n'
                '    if source.is_dir():\n'
                '        shutil.copytree(source, sys.argv[3])\n'
                '    else:\n'
                '        shutil.copy2(source, sys.argv[3])\n',
                encoding='utf-8',
            )
            docker.chmod(0o755)
            # Keep the real CLI and Docker subprocess I/O; replace only the pinned
            # release data with the same ELF fixture used by the verifier tests.
            launcher = (
                'import importlib.util, json, sys; '
                f'spec = importlib.util.spec_from_file_location("image_cli", {runsc_image.__file__!r}); '
                'module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module); '
                'module.runsc_release.ARTIFACTS = json.loads(sys.argv.pop(1)); module.main()'
            )
            result = subprocess.run(
                [sys.executable, '-c', launcher, json.dumps(artifact), '--image', IMAGE,
                 '--output', str(root / 'runtime/runsc')],
                check=False, text=True, capture_output=True,
                env={**os.environ, 'PATH': str(commands) + os.pathsep + os.defpath},
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, artifact['x86_64-unknown-linux-gnu']['files']['runsc'] + '\n')
            self.assertIn('docker pull progress', result.stderr)
            self.assertEqual(result.stderr.count('docker cp progress'), 2)
            self.assertIn('docker create progress', result.stderr)
            self.assertIn('docker create diagnostic', result.stderr)


if __name__ == '__main__':
    unittest.main()
