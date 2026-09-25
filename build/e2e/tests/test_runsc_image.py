"""Pinned region-local runsc image handoff for heterogeneous E2E."""

import hashlib
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from e2e.runsc_image import stage


IMAGE = 'registry.example/adx-runsc@sha256:' + 'a' * 64
CONTAINER = 'b' * 64


class DockerFixture:
    def __init__(self, binary):
        self.binary = binary
        self.calls = []

    def __call__(self, args, **_kwargs):
        self.calls.append(args)
        if args[1] == 'create':
            return subprocess.CompletedProcess(args, 0, stdout=CONTAINER + '\n')
        if args[1] == 'cp':
            shutil.copyfile(self.binary, args[3])
        return subprocess.CompletedProcess(args, 0)


class RunscImageTests(unittest.TestCase):
    def test_pinned_image_is_copied_verified_and_container_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'source'
            binary.write_bytes(b'runsc fixture')
            docker = DockerFixture(binary)
            destination = root / 'runtime/runsc'
            expected = hashlib.sha512(binary.read_bytes()).hexdigest()
            digest = stage(IMAGE, expected, destination, docker)
            self.assertEqual(digest, hashlib.sha256(binary.read_bytes()).hexdigest())
            self.assertEqual(destination.read_bytes(), binary.read_bytes())
            self.assertTrue(destination.stat().st_mode & 0o111)
            self.assertEqual([call[1] for call in docker.calls], ['pull', 'create', 'cp', 'rm'])
            self.assertEqual(docker.calls[0][2:4], ['--platform', 'linux/amd64'])
            self.assertEqual(docker.calls[1][-2:], ['/runsc', '--version'])

    def test_wrong_digest_rejects_copy_and_still_removes_container(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'source'
            binary.write_bytes(b'runsc fixture')
            docker = DockerFixture(binary)
            destination = root / 'runtime/runsc'
            with self.assertRaisesRegex(ValueError, 'SHA512'):
                stage(IMAGE, '0' * 128, destination, docker)
            self.assertFalse(destination.exists())
            self.assertEqual(docker.calls[-1][1], 'rm')
            with self.assertRaisesRegex(ValueError, 'digest'):
                stage('registry.example/adx-runsc:latest', '0' * 128, destination, docker)


if __name__ == '__main__':
    unittest.main()
