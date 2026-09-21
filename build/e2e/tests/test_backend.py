import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
spec=importlib.util.spec_from_file_location('backend_builder',Path(__file__).resolve().parents[1]/'build_backend.py')
backend=importlib.util.module_from_spec(spec);spec.loader.exec_module(backend)

class ReleaseChecksumTests(unittest.TestCase):
    def test_firecracker_guest_agent_is_a_content_addressed_backend_artifact(self):
        self.assertIn('firecracker-initrd', backend.SANDBOXD_BUILD_TARGETS)
        self.assertEqual(
            backend.SANDBOXD_OUTPUTS['firecracker-initrd.img'],
            'initrd.img',
        )

    def test_blank_lines_comments_and_other_architectures(self):
        digest='a'*64
        manifest='\n# release checksums\n\n'+'b'*64+'  runc.arm64\n\n'+digest+' *runc.amd64\n'
        self.assertEqual(backend.release_checksum(manifest,'runc.amd64'),digest)
    def test_missing_malformed_or_duplicate_checksum_is_rejected(self):
        for manifest in ('\n','bad  runc.amd64\n',('a'*64+'  runc.amd64\n')*2):
            with self.subTest(manifest=manifest), self.assertRaises(ValueError):
                backend.release_checksum(manifest,'runc.amd64')

    def test_pinned_patches_are_content_addressed(self):
        pinned=json.loads((backend.ROOT/'third_party/sandboxd/source.json').read_text())
        self.assertEqual(
            backend.pinned_patches(pinned),
            {entry['path']:entry['sha256'] for entry in pinned['patches']},
        )

    def test_pinned_source_fetch_retries_from_a_clean_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'source'
            attempts=[]
            sleeps=[]

            def command(args):
                attempts.append(tuple(map(str,args)))
                if args[1]=='init':
                    source.mkdir(parents=True)
                    (source/'partial-pack').write_text('stale')
                if len(args)>3 and args[3]=='fetch' and sum(len(cmd)>3 and cmd[3]=='fetch' for cmd in attempts)<3:
                    raise subprocess.CalledProcessError(128,args)

            backend.fetch_pinned_source(
                source,
                'https://example.invalid/sandboxd.git',
                'a'*40,
                command=command,
                sleeper=sleeps.append,
            )

            fetches=[cmd for cmd in attempts if len(cmd)>3 and cmd[3]=='fetch']
            checkouts=[cmd for cmd in attempts if len(cmd)>3 and cmd[3]=='checkout']
            self.assertEqual(len(fetches),3)
            self.assertEqual(len(checkouts),1)
            self.assertEqual(sleeps,[1,2])

    def test_pinned_source_fetch_stops_after_three_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'source'
            fetches=[]

            def command(args):
                if args[1]=='init':
                    source.mkdir(parents=True)
                if len(args)>3 and args[3]=='fetch':
                    fetches.append(tuple(map(str,args)))
                    raise subprocess.CalledProcessError(128,args)

            with self.assertRaises(subprocess.CalledProcessError):
                backend.fetch_pinned_source(
                    source,
                    'https://example.invalid/sandboxd.git',
                    'b'*40,
                    command=command,
                    sleeper=lambda _: None,
                )
            self.assertEqual(len(fetches),3)
